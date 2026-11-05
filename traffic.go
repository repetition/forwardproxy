package forwardproxy

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type UserTraffic struct {
	Upload      atomic.Int64
	Download    atomic.Int64
	Connections atomic.Int64
	LastActive  atomic.Int64
}

type UserTrafficSnapshot struct {
	Upload      int64 `json:"upload"`
	Download    int64 `json:"download"`
	Connections int64 `json:"connections"`
	LastActive  int64 `json:"last_active"`
}

var (
	trafficStatsMap sync.Map // map[string]*UserTraffic
	serverOnce      sync.Once
)

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
			WriteTimeout: 5 * time.Second,
		}
		go func() {
			_ = srv.ListenAndServe()
		}()
	})
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

func handleTrafficQuery(w http.ResponseWriter, r *http.Request) {
	reset := r.URL.Query().Get("reset") == "true" || r.URL.Query().Get("reset") == "1"
	result := make(map[string]UserTrafficSnapshot)

	trafficStatsMap.Range(func(key, value any) bool {
		user := key.(string)
		ut := value.(*UserTraffic)

		var up, down int64
		if reset {
			up = ut.Upload.Swap(0)
			down = ut.Download.Swap(0)
		} else {
			up = ut.Upload.Load()
			down = ut.Download.Load()
		}

		conns := ut.Connections.Load()
		lastActive := ut.LastActive.Load()

		// Only return entries that have activity or active connections
		if up > 0 || down > 0 || conns > 0 || !reset {
			result[user] = UserTrafficSnapshot{
				Upload:      up,
				Download:    down,
				Connections: conns,
				LastActive:  lastActive,
			}
		}
		return true
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

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
	buf, err := base64.StdEncoding.DecodeString(pa[1])
	if err == nil {
		cred := string(buf)
		if idx := strings.IndexByte(cred, ':'); idx >= 0 {
			return cred[:idx]
		}
	}
	return ""
}

type trackedReadWriteCloser struct {
	net.Conn
	ut     *UserTraffic
	closed atomic.Bool
}

func wrapTargetConn(conn net.Conn, username string) net.Conn {
	ut := getOrCreateUserTraffic(username)
	ut.Connections.Add(1)
	ut.LastActive.Store(time.Now().Unix())
	return &trackedReadWriteCloser{
		Conn: conn,
		ut:   ut,
	}
}

func (t *trackedReadWriteCloser) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	if n > 0 {
		t.ut.Download.Add(int64(n))
		t.ut.LastActive.Store(time.Now().Unix())
	}
	return n, err
}

func (t *trackedReadWriteCloser) Write(p []byte) (int, error) {
	n, err := t.Conn.Write(p)
	if n > 0 {
		t.ut.Upload.Add(int64(n))
		t.ut.LastActive.Store(time.Now().Unix())
	}
	return n, err
}

func (t *trackedReadWriteCloser) Close() error {
	if t.closed.CompareAndSwap(false, true) {
		t.ut.Connections.Add(-1)
	}
	return t.Conn.Close()
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
