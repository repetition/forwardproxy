package forwardproxy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
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

// AI-code-start lines:8
func domainSnapshot(ut *UserTraffic, host string) DomainTrafficSnapshot {
	val, ok := ut.Domains.Load(host)
	if !ok {
		return DomainTrafficSnapshot{}
	}
	dt := val.(*DomainTraffic)
	return DomainTrafficSnapshot{Upload: dt.Upload.Load(), Download: dt.Download.Load()}
}

// AI-code-start lines:8
func domainKeyCount(ut *UserTraffic) int {
	n := 0
	ut.Domains.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
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
	if d, ok := bob1.Domains["example.com:443"]; !ok || d.Upload != 100 || d.Download != 200 {
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
	if d, ok := bob2.Domains["example.com:443"]; !ok || d.Upload != 100 || d.Download != 200 {
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
	if d, ok := snap3["bob"].Domains["example.com:443"]; ok {
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
