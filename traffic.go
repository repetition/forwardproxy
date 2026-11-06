package forwardproxy

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const maxDomainsPerUser = 2000

// AI-code-start lines:27
type DomainTraffic struct {
	Upload      atomic.Int64
	Download    atomic.Int64
	Connections atomic.Int64
}

type DomainTrafficSnapshot struct {
	Upload   int64 `json:"up"`
	Download int64 `json:"down"`
}

type UserTraffic struct {
	Upload      atomic.Int64
	Download    atomic.Int64
	Connections atomic.Int64
	LastActive  atomic.Int64
	domainsMu   sync.Mutex
	domains     map[string]*DomainTraffic
}

type UserTrafficSnapshot struct {
	Upload      int64                            `json:"upload"`
	Download    int64                            `json:"download"`
	Connections int64                            `json:"connections"`
	LastActive  int64                            `json:"last_active"`
	Domains     map[string]DomainTrafficSnapshot `json:"domains,omitempty"`
}

type pendingDomain struct {
	dt   *DomainTraffic
	up   int64
	down int64
}

type pendingUser struct {
	ut      *UserTraffic
	up      int64
	down    int64
	domains map[string]pendingDomain
}

var (
	trafficStatsMap sync.Map // map[string]*UserTraffic
	serverOnce      sync.Once
	trafficResetMu  sync.Mutex
)

// AI-code-start lines:21
func initTrafficServer() {
	serverOnce.Do(func() {
		addr := os.Getenv("NAIVE_TRAFFIC_ADDR")
		if addr == "" {
			addr = "127.0.0.1:9199"
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/internal/naive/traffic", handleTrafficQuery)
		srv := &http.Server{
			Addr:         addr,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 30 * time.Second,
		}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("naive traffic server (%s) failed: %v", addr, err)
			}
		}()
	})
}

// AI-code-start lines:16
func normalizeDomain(hostPort string) string {
	if hostPort == "" {
		return "_unknown_"
	}
	host := hostPort
	if h, _, err := net.SplitHostPort(hostPort); err == nil {
		host = h
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "" {
		return "_unknown_"
	}
	return host
}

func getOrCreateUserTraffic(user string) *UserTraffic {
	if user == "" {
		user = "_anonymous_"
	}
	if val, ok := trafficStatsMap.Load(user); ok {
		return val.(*UserTraffic)
	}
	actual, _ := trafficStatsMap.LoadOrStore(user, &UserTraffic{})
	return actual.(*UserTraffic)
}

// AI-code-start lines:31
func loadOrCreateDomain(ut *UserTraffic, targetHost string, pin bool) *DomainTraffic {
	domain := normalizeDomain(targetHost)
	ut.domainsMu.Lock()
	defer ut.domainsMu.Unlock()
	if ut.domains == nil {
		ut.domains = make(map[string]*DomainTraffic)
	}
	dt, ok := ut.domains[domain]
	if !ok {
		regularCount := len(ut.domains)
		if _, hasOthers := ut.domains["_others_"]; hasOthers {
			regularCount--
		}
		if regularCount >= maxDomainsPerUser {
			domain = "_others_"
			dt, ok = ut.domains[domain]
		}
	}
	if ok {
		if pin {
			dt.Connections.Add(1)
		}
		return dt
	}
	dt = &DomainTraffic{}
	if pin {
		dt.Connections.Store(1)
	}
	ut.domains[domain] = dt
	return dt
}

func getOrCreateDomainTraffic(ut *UserTraffic, targetHost string) *DomainTraffic {
	return loadOrCreateDomain(ut, targetHost, false)
}

func acquireDomainTraffic(ut *UserTraffic, targetHost string) *DomainTraffic {
	return loadOrCreateDomain(ut, targetHost, true)
}

// AI-code-start lines:26
func handleTrafficQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	reset := r.URL.Query().Get("reset") == "true" || r.URL.Query().Get("reset") == "1"
	if reset {
		trafficResetMu.Lock()
		defer trafficResetMu.Unlock()
	}

	result, pending := collectTraffic(reset)
	buf, err := json.Marshal(result)
	if err != nil {
		http.Error(w, "failed to encode traffic", http.StatusInternalServerError)
		return
	}
	buf = append(buf, '\n')
	w.Header().Set("Content-Type", "application/json")
	if n, werr := w.Write(buf); werr != nil || n != len(buf) {
		return
	}
	if reset {
		commitTrafficReset(pending)
	}
}

// AI-code-start lines:49
func collectTraffic(reset bool) (map[string]UserTrafficSnapshot, []pendingUser) {
	result := make(map[string]UserTrafficSnapshot)
	var pending []pendingUser
	trafficStatsMap.Range(func(key, value any) bool {
		user := key.(string)
		ut := value.(*UserTraffic)
		up := ut.Upload.Load()
		down := ut.Download.Load()
		conns := ut.Connections.Load()
		lastActive := ut.LastActive.Load()

		ut.domainsMu.Lock()
		copied := make(map[string]*DomainTraffic, len(ut.domains))
		for domain, dt := range ut.domains {
			copied[domain] = dt
		}
		ut.domainsMu.Unlock()

		domainsSnap := make(map[string]DomainTrafficSnapshot)
		var domainPend map[string]pendingDomain
		if reset {
			domainPend = make(map[string]pendingDomain, len(copied))
		}
		for domain, dt := range copied {
			dUp := dt.Upload.Load()
			dDown := dt.Download.Load()
			if reset {
				domainPend[domain] = pendingDomain{dt: dt, up: dUp, down: dDown}
			}
			if dUp > 0 || dDown > 0 {
				domainsSnap[domain] = DomainTrafficSnapshot{Upload: dUp, Download: dDown}
			}
		}
		if reset {
			pending = append(pending, pendingUser{ut: ut, up: up, down: down, domains: domainPend})
		}
		if up > 0 || down > 0 || conns > 0 || len(domainsSnap) > 0 || !reset {
			result[user] = UserTrafficSnapshot{
				Upload:      up,
				Download:    down,
				Connections: conns,
				LastActive:  lastActive,
				Domains:     domainsSnap,
			}
		}
		return true
	})
	return result, pending
}

// AI-code-start lines:15
func subReported(v *atomic.Int64, n int64) {
	if n == 0 {
		return
	}
	for {
		cur := v.Load()
		next := cur - n
		if next < 0 {
			next = 0
		}
		if v.CompareAndSwap(cur, next) {
			return
		}
	}
}

// AI-code-start lines:22
func commitTrafficReset(pending []pendingUser) {
	for _, pu := range pending {
		subReported(&pu.ut.Upload, pu.up)
		subReported(&pu.ut.Download, pu.down)
		for domain, pd := range pu.domains {
			subReported(&pd.dt.Upload, pd.up)
			subReported(&pd.dt.Download, pd.down)
			if pd.up != 0 || pd.down != 0 {
				continue
			}
			pu.ut.domainsMu.Lock()
			cur, ok := pu.ut.domains[domain]
			if ok && cur == pd.dt &&
				pd.dt.Connections.Load() <= 0 &&
				pd.dt.Upload.Load() == 0 &&
				pd.dt.Download.Load() == 0 {
				delete(pu.ut.domains, domain)
			}
			pu.ut.domainsMu.Unlock()
		}
	}
}

// AI-code-start lines:18
func (h Handler) extractUsername(r *http.Request) string {
	pa := strings.Split(r.Header.Get("Proxy-Authorization"), " ")
	if len(pa) != 2 || strings.ToLower(pa[0]) != "basic" {
		return ""
	}
	for _, creds := range h.AuthCredentials {
		if subtle.ConstantTimeCompare(creds, []byte(pa[1])) == 1 {
			buf := make([]byte, base64.StdEncoding.DecodedLen(len(creds)))
			n, _ := base64.StdEncoding.Decode(buf, creds)
			cred := string(buf[:n])
			if idx := strings.IndexByte(cred, ':'); idx >= 0 {
				return cred[:idx]
			}
			return cred
		}
	}
	return ""
}

type trackedReadWriteCloser struct {
	net.Conn
	ut       *UserTraffic
	dt       *DomainTraffic
	mu       sync.Mutex
	inflight int
	closed   bool
	released bool
}

// AI-code-start lines:12
func touchActive(ut *UserTraffic) {
	now := time.Now().Unix()
	for {
		last := ut.LastActive.Load()
		if now-last < 1 {
			return
		}
		if ut.LastActive.CompareAndSwap(last, now) {
			return
		}
	}
}

// AI-code-start lines:11
func wrapTargetConn(conn net.Conn, username string, targetHost string) net.Conn {
	ut := getOrCreateUserTraffic(username)
	dt := acquireDomainTraffic(ut, targetHost)
	ut.Connections.Add(1)
	touchActive(ut)
	return &trackedReadWriteCloser{
		Conn: conn,
		ut:   ut,
		dt:   dt,
	}
}

// AI-code-start lines:9
func (t *trackedReadWriteCloser) begin() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return false
	}
	t.inflight++
	return true
}

// AI-code-start lines:12
func (t *trackedReadWriteCloser) end() {
	t.mu.Lock()
	t.inflight--
	release := t.closed && t.inflight == 0 && !t.released
	if release {
		t.released = true
	}
	t.mu.Unlock()
	if release {
		t.releasePins()
	}
}

func (t *trackedReadWriteCloser) releasePins() {
	t.ut.Connections.Add(-1)
	t.dt.Connections.Add(-1)
}

// AI-code-start lines:14
func (t *trackedReadWriteCloser) Read(p []byte) (int, error) {
	if !t.begin() {
		return 0, net.ErrClosed
	}
	defer t.end()
	n, err := t.Conn.Read(p)
	if n > 0 {
		delta := int64(n)
		t.ut.Download.Add(delta)
		t.dt.Download.Add(delta)
		touchActive(t.ut)
	}
	return n, err
}

// AI-code-start lines:14
func (t *trackedReadWriteCloser) Write(p []byte) (int, error) {
	if !t.begin() {
		return 0, net.ErrClosed
	}
	defer t.end()
	n, err := t.Conn.Write(p)
	if n > 0 {
		delta := int64(n)
		t.ut.Upload.Add(delta)
		t.dt.Upload.Add(delta)
		touchActive(t.ut)
	}
	return n, err
}

// AI-code-start lines:18
func (t *trackedReadWriteCloser) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return t.Conn.Close()
	}
	t.closed = true
	release := t.inflight == 0 && !t.released
	if release {
		t.released = true
	}
	t.mu.Unlock()
	err := t.Conn.Close()
	if release {
		t.releasePins()
	}
	return err
}

func (t *trackedReadWriteCloser) CloseWrite() error {
	if cw, ok := t.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}

// trackingReader wraps an io.Reader to count transferred bytes
type trackingReader struct {
	r       io.Reader
	counter *atomic.Int64
	active  *atomic.Int64
}

func (tr *trackingReader) Read(p []byte) (int, error) {
	n, err := tr.r.Read(p)
	if n > 0 {
		tr.counter.Add(int64(n))
		tr.active.Store(time.Now().Unix())
	}
	return n, err
}

// trackingWriter wraps an io.Writer to count transferred bytes
type trackingWriter struct {
	w       io.Writer
	counter *atomic.Int64
	active  *atomic.Int64
}

func (tw *trackingWriter) Write(p []byte) (int, error) {
	n, err := tw.w.Write(p)
	if n > 0 {
		tw.counter.Add(int64(n))
		tw.active.Store(time.Now().Unix())
	}
	return n, err
}

// trackingResponseWriter preserves http.Flusher if the underlying ResponseWriter implements it
type trackingResponseWriter struct {
	http.ResponseWriter
	counter *atomic.Int64
	active  *atomic.Int64
}

func (trw *trackingResponseWriter) Write(p []byte) (int, error) {
	n, err := trw.ResponseWriter.Write(p)
	if n > 0 {
		trw.counter.Add(int64(n))
		trw.active.Store(time.Now().Unix())
	}
	return n, err
}

func (trw *trackingResponseWriter) Flush() {
	if flusher, ok := trw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (trw *trackingResponseWriter) Unwrap() http.ResponseWriter {
	return trw.ResponseWriter
}
