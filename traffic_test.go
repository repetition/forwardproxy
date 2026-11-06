package forwardproxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type mockCloseWriteConn struct {
	net.Conn
	closeWriteCalled bool
}

func (m *mockCloseWriteConn) CloseWrite() error {
	m.closeWriteCalled = true
	return nil
}

func resetTrafficStatsForTest() {
	trafficStatsMap.Range(func(key, _ any) bool {
		trafficStatsMap.Delete(key)
		return true
	})
}

// AI-code-start lines:9
func domainSnapshot(ut *UserTraffic, host string) DomainTrafficSnapshot {
	ut.domainsMu.Lock()
	defer ut.domainsMu.Unlock()
	dt := ut.domains[normalizeDomain(host)]
	if dt == nil {
		return DomainTrafficSnapshot{}
	}
	return DomainTrafficSnapshot{Upload: dt.Upload.Load(), Download: dt.Download.Load()}
}

// AI-code-start lines:5
func domainKeyCount(ut *UserTraffic) int {
	ut.domainsMu.Lock()
	defer ut.domainsMu.Unlock()
	return len(ut.domains)
}

func TestWrapTargetConn(t *testing.T) {
	resetTrafficStatsForTest()

	c1, c2 := net.Pipe()
	defer c2.Close()

	mockConn := &mockCloseWriteConn{Conn: c1}
	host := "example.com:443"
	wrapped := wrapTargetConn(mockConn, "alice", host)

	ut := getOrCreateUserTraffic("alice")
	if got := ut.Connections.Load(); got != 1 {
		t.Fatalf("expected Connections=1 after wrapTargetConn, got %d", got)
	}

	// Test Write -> Upload
	uploadPayload := []byte("hello target server")
	go func() {
		buf := make([]byte, len(uploadPayload))
		_, _ = c2.Read(buf)
	}()
	n, err := wrapped.Write(uploadPayload)
	if err != nil || n != len(uploadPayload) {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}
	if got := ut.Upload.Load(); got != int64(len(uploadPayload)) {
		t.Fatalf("expected Upload=%d, got %d", len(uploadPayload), got)
	}
	if ds := domainSnapshot(ut, host); ds.Upload != int64(len(uploadPayload)) {
		t.Fatalf("expected domains[%s].up=%d, got %d", host, len(uploadPayload), ds.Upload)
	}

	// Test Read -> Download
	downloadPayload := []byte("response from target server")
	go func() {
		_, _ = c2.Write(downloadPayload)
	}()
	readBuf := make([]byte, 64)
	n, err = wrapped.Read(readBuf)
	if err != nil || n != len(downloadPayload) {
		t.Fatalf("Read failed: n=%d, err=%v", n, err)
	}
	if got := ut.Download.Load(); got != int64(len(downloadPayload)) {
		t.Fatalf("expected Download=%d, got %d", len(downloadPayload), got)
	}
	if ds := domainSnapshot(ut, host); ds.Download != int64(len(downloadPayload)) {
		t.Fatalf("expected domains[%s].down=%d, got %d", host, len(downloadPayload), ds.Download)
	}
	if got := ut.LastActive.Load(); got <= 0 {
		t.Fatalf("expected LastActive > 0, got %d", got)
	}

	// Test CloseWrite forwarding
	if cw, ok := wrapped.(closeWriter); ok {
		if err := cw.CloseWrite(); err != nil {
			t.Fatalf("CloseWrite returned error: %v", err)
		}
		if !mockConn.closeWriteCalled {
			t.Fatalf("expected underlying CloseWrite to be called")
		}
	} else {
		t.Fatalf("wrapped conn does not implement closeWriter")
	}

	// Test Close -> decrements Connections exactly once
	_ = wrapped.Close()
	_ = wrapped.Close()
	if got := ut.Connections.Load(); got != 0 {
		t.Fatalf("expected Connections=0 after Close, got %d", got)
	}
}

func TestHandleTrafficQueryAndReset(t *testing.T) {
	resetTrafficStatsForTest()

	ut := getOrCreateUserTraffic("bob")
	ut.Upload.Store(1024)
	ut.Download.Store(4096)
	ut.Connections.Store(2)
	ut.LastActive.Store(1700000000)
	dt := getOrCreateDomainTraffic(ut, "example.com:443")
	dt.Upload.Store(100)
	dt.Download.Store(200)

	// Query without reset
	req1 := httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=false", nil)
	rec1 := httptest.NewRecorder()
	handleTrafficQuery(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec1.Code)
	}
	var snap1 map[string]UserTrafficSnapshot
	if err := json.Unmarshal(rec1.Body.Bytes(), &snap1); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	bob1, ok := snap1["bob"]
	if !ok {
		t.Fatalf("expected bob in traffic snapshot")
	}
	if bob1.Upload != 1024 || bob1.Download != 4096 || bob1.Connections != 2 || bob1.LastActive != 1700000000 {
		t.Fatalf("unexpected snapshot before reset: %+v", bob1)
	}
	if d, ok := bob1.Domains["example.com"]; !ok || d.Upload != 100 || d.Download != 200 {
		t.Fatalf("unexpected domain snapshot before reset: %+v", bob1.Domains)
	}
	// Values should remain unchanged after reset=false
	if ut.Upload.Load() != 1024 || ut.Download.Load() != 4096 {
		t.Fatalf("counters were unexpectedly modified on reset=false")
	}

	// Query with reset=true
	req2 := httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=true", nil)
	rec2 := httptest.NewRecorder()
	handleTrafficQuery(rec2, req2)

	var snap2 map[string]UserTrafficSnapshot
	if err := json.Unmarshal(rec2.Body.Bytes(), &snap2); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	bob2 := snap2["bob"]
	if bob2.Upload != 1024 || bob2.Download != 4096 || bob2.Connections != 2 {
		t.Fatalf("unexpected snapshot returned by reset=true: %+v", bob2)
	}
	if d, ok := bob2.Domains["example.com"]; !ok || d.Upload != 100 || d.Download != 200 {
		t.Fatalf("unexpected domain snapshot returned by reset=true: %+v", bob2.Domains)
	}
	// Values should now be 0 in atomic counters
	if ut.Upload.Load() != 0 || ut.Download.Load() != 0 {
		t.Fatalf("expected Upload=0 and Download=0 after reset=true, got up=%d down=%d", ut.Upload.Load(), ut.Download.Load())
	}
	if ds := domainSnapshot(ut, "example.com:443"); ds.Upload != 0 || ds.Download != 0 {
		t.Fatalf("expected domain counters zeroed after reset, got %+v", ds)
	}

	// Subsequent reset=true evicts idle domain keys
	req3 := httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=true", nil)
	rec3 := httptest.NewRecorder()
	handleTrafficQuery(rec3, req3)
	if domainKeyCount(ut) != 0 {
		t.Fatalf("expected idle domains evicted after second reset, remaining=%d", domainKeyCount(ut))
	}
	var snap3 map[string]UserTrafficSnapshot
	if err := json.Unmarshal(rec3.Body.Bytes(), &snap3); err != nil {
		t.Fatalf("failed to unmarshal second reset response: %v", err)
	}
	if d, ok := snap3["bob"].Domains["example.com"]; ok {
		t.Fatalf("expected evicted domain absent from snapshot, got %+v", d)
	}
}

// AI-code-start lines:62
func TestMultipleDomainsIndependent(t *testing.T) {
	resetTrafficStatsForTest()

	c1a, c2a := net.Pipe()
	c1b, c2b := net.Pipe()
	defer c2a.Close()
	defer c2b.Close()

	w1 := wrapTargetConn(c1a, "alice", "example.com:443")
	w2 := wrapTargetConn(c1b, "alice", "github.com:443")
	defer w1.Close()
	defer w2.Close()

	up1 := []byte("aaaa")
	up2 := []byte("bbbbbb")
	go func() {
		buf := make([]byte, len(up1))
		_, _ = c2a.Read(buf)
	}()
	if _, err := w1.Write(up1); err != nil {
		t.Fatalf("write example.com: %v", err)
	}
	go func() {
		buf := make([]byte, len(up2))
		_, _ = c2b.Read(buf)
	}()
	if _, err := w2.Write(up2); err != nil {
		t.Fatalf("write github.com: %v", err)
	}

	down1 := []byte("ccccc")
	down2 := []byte("dd")
	go func() {
		_, _ = c2a.Write(down1)
	}()
	buf := make([]byte, 16)
	if _, err := w1.Read(buf); err != nil {
		t.Fatalf("read example.com: %v", err)
	}
	go func() {
		_, _ = c2b.Write(down2)
	}()
	if _, err := w2.Read(buf); err != nil {
		t.Fatalf("read github.com: %v", err)
	}

	ut := getOrCreateUserTraffic("alice")
	ex := domainSnapshot(ut, "example.com:443")
	gh := domainSnapshot(ut, "github.com:443")
	if ex.Upload != int64(len(up1)) || ex.Download != int64(len(down1)) {
		t.Fatalf("example.com counters mismatch: %+v", ex)
	}
	if gh.Upload != int64(len(up2)) || gh.Download != int64(len(down2)) {
		t.Fatalf("github.com counters mismatch: %+v", gh)
	}
	if ut.Upload.Load() != int64(len(up1)+len(up2)) {
		t.Fatalf("user upload should sum domains, got %d", ut.Upload.Load())
	}
	if ut.Download.Load() != int64(len(down1)+len(down2)) {
		t.Fatalf("user download should sum domains, got %d", ut.Download.Load())
	}
}

// AI-code-start lines:29
func TestExtractUsername(t *testing.T) {
	h := Handler{
		AuthCredentials: [][]byte{
			EncodeAuthCredentials("charlie", "secret123"),
		},
	}

	req := httptest.NewRequest(http.MethodConnect, "https://example.com:443", nil)
	token := base64.StdEncoding.EncodeToString([]byte("charlie:secret123"))
	req.Header.Set("Proxy-Authorization", "Basic "+token)

	if user := h.extractUsername(req); user != "charlie" {
		t.Fatalf("expected username 'charlie', got %q", user)
	}

	// Unmatched credentials should not extract forged username
	forgedReq := httptest.NewRequest(http.MethodConnect, "https://example.com:443", nil)
	forgedToken := base64.StdEncoding.EncodeToString([]byte("attacker:wrongpass"))
	forgedReq.Header.Set("Proxy-Authorization", "Basic "+forgedToken)
	if user := h.extractUsername(forgedReq); user != "" {
		t.Fatalf("expected empty username for unmatched credentials, got %q", user)
	}

	// Test anonymous fallback
	anon := getOrCreateUserTraffic("")
	if anon != getOrCreateUserTraffic("_anonymous_") {
		t.Fatalf("expected empty user to map to _anonymous_")
	}
}

func TestTrackingReaderWriterAndResponseWriter(t *testing.T) {
	ut := &UserTraffic{}
	src := bytes.NewBufferString("123456")
	tr := &trackingReader{r: src, counter: &ut.Download, active: &ut.LastActive}
	buf := make([]byte, 16)
	n, err := tr.Read(buf)
	if err != nil || n != 6 || ut.Download.Load() != 6 {
		t.Fatalf("trackingReader failed: n=%d, down=%d, err=%v", n, ut.Download.Load(), err)
	}

	var dst bytes.Buffer
	tw := &trackingWriter{w: &dst, counter: &ut.Upload, active: &ut.LastActive}
	n, err = tw.Write([]byte("abcdefg"))
	if err != nil || n != 7 || ut.Upload.Load() != 7 {
		t.Fatalf("trackingWriter failed: n=%d, up=%d, err=%v", n, ut.Upload.Load(), err)
	}

	rec := httptest.NewRecorder()
	trw := &trackingResponseWriter{ResponseWriter: rec, counter: &ut.Download, active: &ut.LastActive}
	_, _ = trw.Write([]byte("xyz"))
	trw.Flush()
	if trw.Unwrap() != rec || ut.Download.Load() != 9 {
		t.Fatalf("trackingResponseWriter failed: down=%d", ut.Download.Load())
	}
}

type failResponseWriter struct {
	hdr  http.Header
	code int
}

func (f *failResponseWriter) Header() http.Header {
	if f.hdr == nil {
		f.hdr = make(http.Header)
	}
	return f.hdr
}

func (f *failResponseWriter) WriteHeader(code int) { f.code = code }

func (f *failResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

// AI-code-start lines:53
func TestDomainSurvivesResetWhileConnectionOpen(t *testing.T) {
	resetTrafficStatsForTest()

	c1, c2 := net.Pipe()
	defer c2.Close()
	wrapped := wrapTargetConn(c1, "alice", "www.google.com:443")

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=true", nil)
		rec := httptest.NewRecorder()
		handleTrafficQuery(rec, req)
	}
	ut := getOrCreateUserTraffic("alice")
	if domainKeyCount(ut) != 1 {
		t.Fatalf("expected open connection to keep domain, keys=%d", domainKeyCount(ut))
	}

	payload := bytes.Repeat([]byte("x"), 1024)
	go func() {
		buf := make([]byte, len(payload))
		_, _ = c2.Read(buf)
	}()
	if _, err := wrapped.Write(payload); err != nil {
		t.Fatalf("write after reset: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=false", nil)
	rec := httptest.NewRecorder()
	handleTrafficQuery(rec, req)
	var snap map[string]UserTrafficSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if d := snap["alice"].Domains["www.google.com"]; d.Upload != 1024 {
		t.Fatalf("expected upload 1024 on live domain, got %+v", snap["alice"].Domains)
	}

	if err := wrapped.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	req = httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=true", nil)
	rec = httptest.NewRecorder()
	handleTrafficQuery(rec, req)
	if domainKeyCount(ut) != 1 {
		t.Fatalf("expected domain to stay until a zero reset, keys=%d", domainKeyCount(ut))
	}
	req = httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=true", nil)
	rec = httptest.NewRecorder()
	handleTrafficQuery(rec, req)
	if domainKeyCount(ut) != 0 {
		t.Fatalf("expected idle domain evicted, keys=%d", domainKeyCount(ut))
	}
}

// AI-code-start lines:45
func TestDomainNormalization(t *testing.T) {
	resetTrafficStatsForTest()

	c1, c2 := net.Pipe()
	c3, c4 := net.Pipe()
	defer c2.Close()
	defer c4.Close()
	w1 := wrapTargetConn(c1, "alice", "Example.COM.:443")
	w2 := wrapTargetConn(c3, "alice", "example.com:80")
	defer w1.Close()
	defer w2.Close()

	go func() {
		buf := make([]byte, 4)
		_, _ = c2.Read(buf)
	}()
	go func() {
		buf := make([]byte, 2)
		_, _ = c4.Read(buf)
	}()
	if _, err := w1.Write([]byte("aaaa")); err != nil {
		t.Fatalf("write host with dot: %v", err)
	}
	if _, err := w2.Write([]byte("bb")); err != nil {
		t.Fatalf("write second port: %v", err)
	}

	ut := getOrCreateUserTraffic("alice")
	if domainKeyCount(ut) != 1 {
		t.Fatalf("expected one normalized domain, keys=%d", domainKeyCount(ut))
	}
	if ds := domainSnapshot(ut, "example.com"); ds.Upload != 6 {
		t.Fatalf("expected aggregated upload 6, got %+v", ds)
	}

	if got := normalizeDomain(":443"); got != "_unknown_" {
		t.Fatalf("expected :443 to normalize to _unknown_, got %q", got)
	}
	if got := normalizeDomain("[]:443"); got != "_unknown_" {
		t.Fatalf("expected []:443 to normalize to _unknown_, got %q", got)
	}
	if got1, got2 := normalizeDomain("[2001:DB8::1]:443"), normalizeDomain("[2001:db8::1]"); got1 != "2001:db8::1" || got1 != got2 {
		t.Fatalf("expected IPv6 normalization to match, got %q vs %q", got1, got2)
	}
}

// AI-code-start lines:32
func TestDomainCapOverflow(t *testing.T) {
	resetTrafficStatsForTest()
	ut := getOrCreateUserTraffic("scanner")
	for i := 0; i < maxDomainsPerUser; i++ {
		getOrCreateDomainTraffic(ut, fmt.Sprintf("h%d.example", i))
	}
	first := getOrCreateDomainTraffic(ut, "h0.example")
	other := getOrCreateDomainTraffic(ut, "new.example")
	again := getOrCreateDomainTraffic(ut, "another.example")
	if other != again {
		t.Fatalf("expected overflow hosts to share _others_")
	}
	if first == other {
		t.Fatalf("expected an existing domain to keep its own counter")
	}
	if domainKeyCount(ut) != maxDomainsPerUser+1 {
		t.Fatalf("expected cap plus overflow bucket, keys=%d", domainKeyCount(ut))
	}
	if _, ok := ut.domains["_others_"]; !ok {
		t.Fatalf("expected _others_ bucket")
	}

	// If one regular domain is deleted while _others_ remains, one new regular domain can still be added
	ut.domainsMu.Lock()
	delete(ut.domains, "h0.example")
	ut.domainsMu.Unlock()

	replacement := getOrCreateDomainTraffic(ut, "replacement.example")
	if replacement == other {
		t.Fatalf("expected _others_ not to count toward regular domain cap")
	}
}

// AI-code-start lines:38
func TestResetWriteFailureKeepsCounters(t *testing.T) {
	resetTrafficStatsForTest()
	ut := getOrCreateUserTraffic("dave")
	ut.Upload.Store(50)
	ut.Download.Store(80)
	dt := getOrCreateDomainTraffic(ut, "example.com:443")
	dt.Upload.Store(50)
	dt.Download.Store(80)

	// Non-GET method should be rejected without resetting
	postReq := httptest.NewRequest(http.MethodPost, "/internal/naive/traffic?reset=true", nil)
	postRec := httptest.NewRecorder()
	handleTrafficQuery(postRec, postReq)
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 for POST, got %d", postRec.Code)
	}
	if ut.Upload.Load() != 50 || ut.Download.Load() != 80 {
		t.Fatalf("POST request modified counters")
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/naive/traffic?reset=true", nil)
	handleTrafficQuery(&failResponseWriter{}, req)
	if ut.Upload.Load() != 50 || ut.Download.Load() != 80 {
		t.Fatalf("failed response cleared user counters: up=%d down=%d", ut.Upload.Load(), ut.Download.Load())
	}
	if ds := domainSnapshot(ut, "example.com"); ds.Upload != 50 || ds.Download != 80 {
		t.Fatalf("failed response cleared domain counters: %+v", ds)
	}

	rec := httptest.NewRecorder()
	handleTrafficQuery(rec, req)
	if ut.Upload.Load() != 0 || ut.Download.Load() != 0 {
		t.Fatalf("successful reset left user counters: up=%d down=%d", ut.Upload.Load(), ut.Download.Load())
	}
	if ds := domainSnapshot(ut, "example.com"); ds.Upload != 0 || ds.Download != 0 {
		t.Fatalf("successful reset left domain counters: %+v", ds)
	}
}
