package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testClient bounds every test request so a liveness regression fails instead of hanging the package.
var testClient = &http.Client{Timeout: 10 * time.Second}

var chunk188 = func() []byte {
	b := make([]byte, 188)
	b[0] = 0x47
	return b
}()

// zeroReader streams zeros without allocating the body in the fixture.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func pathServiceID(path string) int64 {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 3 {
		return 0
	}
	n, _ := strconv.ParseInt(parts[2], 10, 64)
	return n
}

type fakeBackend struct {
	server             *httptest.Server
	mu                 sync.Mutex
	services, programs []object
	tuners             []object
	logo               []byte
	down               bool
	streamStatus       int
	streamStatusFor    map[int64]int
	streamLocation     string
	streamCalls        atomic.Int32
	started            chan http.Header
	canceled           chan struct{}
	stall              bool
	hold               chan struct{} // returns after headers once closed (or on timeout)
	end                bool
	oneChunk           bool
	continuous         bool
	resetAfterChunk    bool
	oversizeServices   bool
	oversizePrograms   bool
	failServices       int
	failPrograms       int
	failTuners         int
	stallTuners        bool
	events             chan object
	failEvents         int
	closeEventsOnce    atomic.Bool // close the next healthy subscription after one event
	eventTried         chan struct{}
	eventCanceled      chan struct{}
}

func fixture(n, s int64, name string) (object, object) {
	id := n*100000 + s
	return object{"id": id, "networkId": n, "serviceId": s, "name": name, "type": 1, "channel": object{"type": "GR", "channel": "27", "name": name}, "hasLogoData": true}, object{"id": id*100000 + 3, "networkId": n, "serviceId": s, "eventId": 3, "name": name + " program", "startAt": 1700000000000, "duration": 60000, "isFree": true, "extended": object{"detail": "preserved"}}
}
func newFake(t *testing.T, n int64, name string) *fakeBackend {
	t.Helper()
	s, p := fixture(n, 101, name)
	f := &fakeBackend{services: []object{s}, programs: []object{p}, started: make(chan http.Header, 10), canceled: make(chan struct{}, 10), eventTried: make(chan struct{}, 1), eventCanceled: make(chan struct{}, 10)}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		down, status, stall, end := f.down, f.streamStatus, f.stall, f.end
		oneChunk, reset := f.oneChunk, f.resetAfterChunk
		continuous, hold := f.continuous, f.hold
		failServices, failPrograms, failTuners := f.failServices, f.failPrograms, f.failTuners
		stallTuners := f.stallTuners
		failEvents := f.failEvents
		oversizeServices, oversizePrograms := f.oversizeServices, f.oversizePrograms
		location, statusFor := f.streamLocation, f.streamStatusFor
		s, p, events := f.services, f.programs, f.events
		f.mu.Unlock()
		if down {
			http.Error(w, "down", 503)
			return
		}
		switch r.URL.Path {
		case "/api/services":
			if failServices != 0 {
				http.Error(w, "down", failServices)
				return
			}
			if oversizeServices {
				_, _ = io.CopyN(w, zeroReader{}, 64<<20+1)
				return
			}
			respond(w, 200, s)
		case "/api/tuners":
			if failTuners != 0 {
				http.Error(w, "down", failTuners)
				return
			}
			if stallTuners {
				<-r.Context().Done()
				return
			}
			list := f.tuners
			if list == nil {
				list = []object{}
			}
			respond(w, 200, list)
		case "/api/programs":
			if failPrograms != 0 {
				http.Error(w, "down", failPrograms)
				return
			}
			if oversizePrograms {
				_, _ = io.CopyN(w, zeroReader{}, 64<<20+1)
				return
			}
			respond(w, 200, p)
		case "/api/events/stream":
			select {
			case f.eventTried <- struct{}{}:
			default:
			}
			if failEvents != 0 {
				http.Error(w, "events unavailable", failEvents)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "[\n")
			w.(http.Flusher).Flush()
			defer func() { f.eventCanceled <- struct{}{} }()
			closeAfterEvent := f.closeEventsOnce.CompareAndSwap(true, false)
			for {
				select {
				case e := <-events:
					b, _ := json.Marshal(e)
					_, _ = w.Write(append(b, '\n', ','))
					w.(http.Flusher).Flush()
					if closeAfterEvent {
						return
					}
				case <-r.Context().Done():
					return
				}
			}
		default:
			if strings.HasSuffix(r.URL.Path, "/logo") {
				if f.logo == nil {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "image/png")
				w.Header().Set("ETag", `"mock-logo"`)
				w.WriteHeader(200)
				if r.Method != http.MethodHead {
					_, _ = w.Write(f.logo)
				}
				return
			}
			if !strings.HasSuffix(r.URL.Path, "/stream") {
				http.NotFound(w, r)
				return
			}
			f.streamCalls.Add(1)
			if v, ok := statusFor[pathServiceID(r.URL.Path)]; ok {
				status = v
			}
			if status != 0 {
				if location != "" {
					w.Header().Set("Location", location)
				}
				w.WriteHeader(status)
				return
			}
			h := r.Header.Clone()
			h.Set("Test-Query", r.URL.RawQuery)
			h.Set("Test-Path", r.URL.Path)
			h.Set("Test-RequestURI", r.RequestURI)
			f.started <- h
			if reset {
				// Hijack so the client sees a truncated fixed-length body, not a clean EOF.
				conn, rw, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: video/MP2T\r\nContent-Length: 376\r\n\r\n")
					_, _ = rw.Write(chunk188)
					_ = rw.Flush()
					_ = conn.Close()
				}
				return
			}
			w.Header().Set("Content-Type", "video/MP2T")
			w.Header().Set("X-Mirakurun-Tuner-User-ID", "fake-user")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			if hold != nil {
				// The test decides when this attempt fails without sending data.
				select {
				case <-hold:
				case <-r.Context().Done():
				}
				return
			}
			payload := []byte("transport-stream")
			if oneChunk {
				payload = chunk188
			}
			if !stall && !continuous {
				_, _ = w.Write(payload)
				w.(http.Flusher).Flush()
			}
			if continuous {
				defer func() { f.canceled <- struct{}{} }()
				for {
					if _, err := w.Write(chunk188); err != nil {
						return
					}
					w.(http.Flusher).Flush()
					select {
					case <-time.After(100 * time.Millisecond):
					case <-r.Context().Done():
						return
					}
				}
			}
			if end {
				return
			}
			<-r.Context().Done()
			f.canceled <- struct{}{}
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}
func gateway(t *testing.T, fs ...*fakeBackend) (*Gateway, *httptest.Server) {
	t.Helper()
	return gatewayWithCatalogTimeout(t, 1, fs...)
}
func gatewayWithCatalogTimeout(t *testing.T, catalogSeconds int, fs ...*fakeBackend) (*Gateway, *httptest.Server) {
	t.Helper()
	return gatewayWithTimeouts(t, catalogSeconds, 1, fs...)
}

// gatewayWithTimeouts builds a gateway with the given catalog and stream timeouts.
func gatewayWithTimeouts(t *testing.T, catalogSeconds, streamSeconds int, fs ...*fakeBackend) (*Gateway, *httptest.Server) {
	t.Helper()
	c := Config{Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: catalogSeconds, MaxStaleSeconds: 60, StreamTimeoutSeconds: streamSeconds}
	for i, f := range fs {
		c.Backends = append(c.Backends, BackendConfig{ID: fmt.Sprintf("region%d", i), URL: f.server.URL + "/api", Priority: 100 - i, ServiceDomain: "confirmed-broadcasts"})
	}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)
	return g, s
}

// changeBackend edits backend i and republishes the catalog the way Sync does,
// so tests can simulate a time or config change without a real sync.
func changeBackend(g *Gateway, i int, fn func(*backend)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	fn(&g.backends[i])
	g.cat = g.buildCatalog(time.Now())
}
func getJSON(t *testing.T, url string, status int, v any) {
	t.Helper()
	resp, err := testClient.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != status {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s: HTTP %d: %s", url, resp.StatusCode, b)
	}
	if v != nil {
		d := json.NewDecoder(resp.Body)
		d.UseNumber()
		if err := d.Decode(v); err != nil {
			t.Fatal(err)
		}
	}
}
func waitCancel(t *testing.T, f *fakeBackend) {
	t.Helper()
	select {
	case <-f.canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream was not canceled")
	}
}
func openStream(t *testing.T, url string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("GET", url+"/api/services/1000101/stream?decode=0", nil)
	req.Header.Set("X-Mirakurun-Priority", "9")
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("stream HTTP %d", resp.StatusCode)
	}
	buf := make([]byte, len("transport-stream"))
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "transport-stream" {
		t.Fatalf("bad body %q", buf)
	}
	return resp
}

func TestRegionalCatalogAndProgramAuthority(t *testing.T) {
	a, b, c := newFake(t, 10, "Tokyo"), newFake(t, 20, "Osaka"), newFake(t, 10, "Backup")
	g, s := gateway(t, a, b, c)
	var services, channels, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 2 || len(channels) != 2 || len(programs) != 2 {
		t.Fatalf("unexpected catalog sizes: %d %d %d", len(services), len(channels), len(programs))
	}
	if channels[0]["channel"] == channels[1]["channel"] {
		t.Fatal("GR/27 collided")
	}
	if programs[0]["name"] != "Tokyo program" || programs[0]["extended"] == nil {
		t.Fatal("wrong authority or extension lost")
	}
	if services[0]["hasLogoData"] != true {
		t.Fatal("upstream logo state lost")
	}
	getJSON(t, s.URL+"/api/programs?networkId=20&serviceId=101", 200, &programs)
	if len(programs) != 1 {
		t.Fatal("filter failed")
	}
	getJSON(t, s.URL+"/api/programs?name=ignored", 400, nil)
	getJSON(t, s.URL+"/api/tuners", 200, new([]object))
	getJSON(t, s.URL+"/api/services/1000101", 200, new(object))
	getJSON(t, s.URL+"/api/programs/100010100003", 200, new(object))
	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g.Sync(context.Background())
	getJSON(t, s.URL+"/api/programs?networkId=10", 200, &programs)
	if programs[0]["name"] != "Backup program" {
		t.Fatal("authority did not fail over")
	}
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 2 {
		t.Fatal("lost independent region")
	}
}
func TestStreamFallbackOptionsAndCancellation(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	a.streamStatus = 503
	_, s := gateway(t, a, b)
	resp := openStream(t, s.URL)
	h := <-b.started
	if h.Get("X-Mirakurun-Priority") != "9" || h.Get("Test-Query") != "decode=0" {
		t.Fatalf("options lost: %v", h)
	}
	if resp.Header.Get("X-Mirakurun-Tuner-User-ID") != "fake-user" || a.streamCalls.Load() != 1 {
		t.Fatal("routing/headers failed")
	}
	resp.Body.Close()
	waitCancel(t, b)
	// The failing source is briefly excluded on subsequent requests.
	resp = openStream(t, s.URL)
	resp.Body.Close()
	waitCancel(t, b)
	if a.streamCalls.Load() != 1 {
		t.Fatal("failure cooldown ignored")
	}
}
func TestNoFailoverAfterBody(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	a.end = true
	_, s := gateway(t, a, b)
	resp := openStream(t, s.URL)
	defer resp.Body.Close()
	_, err := io.ReadAll(resp.Body)
	if err == nil {
		t.Fatal("truncated live stream looked complete")
	}
	if b.streamCalls.Load() != 0 {
		t.Fatal("switched mid-stream")
	}
}
func TestFirstByteTimeoutAndDisconnectBeforeBody(t *testing.T) {
	t.Run("timeout retries", func(t *testing.T) {
		a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
		a.stall = true
		_, s := gateway(t, a, b)
		resp := openStream(t, s.URL)
		resp.Body.Close()
		waitCancel(t, a)
		waitCancel(t, b)
	})
	t.Run("client cancels", func(t *testing.T) {
		a := newFake(t, 10, "primary")
		a.stall = true
		g, s := gateway(t, a)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, "GET", s.URL+"/api/services/1000101/stream", nil)
		done := make(chan struct{})
		go func() {
			defer close(done)
			resp, _ := testClient.Do(req)
			if resp != nil {
				resp.Body.Close()
			}
		}()
		select {
		case <-a.started:
		case <-time.After(2 * time.Second):
			t.Fatal("stream not requested")
		}
		cancel()
		waitCancel(t, a)
		<-done
		g.mu.RLock()
		blocked := !g.backends[0].blockedUntil.IsZero()
		g.mu.RUnlock()
		if blocked {
			t.Fatal("client cancellation penalized a healthy backend")
		}
	})
}
func TestFailureAndRecovery(t *testing.T) {
	a := newFake(t, 10, "region")
	g, s := gateway(t, a)
	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g.Sync(context.Background())
	getJSON(t, s.URL+"/api/services", 200, new([]object))
	getJSON(t, s.URL+"/healthz", 503, nil)
	getJSON(t, s.URL+"/api/services/1000101/stream", 503, nil)
	changeBackend(g, 0, func(b *backend) { b.updated = time.Now().Add(-2 * time.Minute) })
	var services []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 0 {
		t.Fatal("expired data survived")
	}
	a.mu.Lock()
	a.down = false
	a.mu.Unlock()
	g.Sync(context.Background())
	getJSON(t, s.URL+"/healthz", 200, nil)
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 1 {
		t.Fatal("did not recover")
	}
}
func TestIdentityConflict(t *testing.T) {
	a, b := newFake(t, 10, "different"), newFake(t, 10, "broadcast")
	g, s := gateway(t, a, b)
	changeBackend(g, 1, func(b *backend) { b.config.ServiceDomain = "different-domain" })
	var services, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 0 || len(programs) != 0 {
		t.Fatal("ambiguous identity was merged")
	}
	getJSON(t, s.URL+"/healthz", 503, nil)
	getJSON(t, s.URL+"/api/services/1000101/stream", 409, nil)
}
func TestRejectMalformedSnapshotAndConcurrentSync(t *testing.T) {
	a := newFake(t, 10, "good")
	g, s := gateway(t, a)
	a.mu.Lock()
	a.programs = []object{{"id": 1}}
	a.mu.Unlock()
	g.Sync(context.Background())
	var programs []object
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(programs) != 1 {
		t.Fatal("last good catalog replaced by invalid snapshot")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.Sync(context.Background())
			resp, err := testClient.Get(s.URL + "/api/services")
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
}
func TestStreamValidationAndNonRetryable(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	a.streamStatus = 403
	_, s := gateway(t, a, b)
	getJSON(t, s.URL+"/api/services/1000101/stream?decode=2", 400, nil)
	getJSON(t, s.URL+"/api/services/1000101/stream?decode=%zz", 400, nil)
	getJSON(t, s.URL+"/api/services/1000101/stream?priority=9", 400, nil)
	req, _ := http.NewRequest("GET", s.URL+"/api/services/1000101/stream", nil)
	req.Header.Set("X-Mirakurun-Priority", "-1")
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatal("negative priority accepted")
	}
	req, _ = http.NewRequest("HEAD", s.URL+"/api/services/1000101/stream", nil)
	resp, err = testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || a.streamCalls.Load() != 0 {
		t.Fatal("HEAD allocated tuner")
	}
	getJSON(t, s.URL+"/api/services/1000101/stream", 502, nil)
	if b.streamCalls.Load() != 0 {
		t.Fatal("retried permission failure")
	}
}

func TestConnectionFailureFallback(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	_, s := gateway(t, a, b)
	// Lose the TCP endpoint after catalog sync, before stream allocation.
	a.server.Close()
	resp := openStream(t, s.URL)
	resp.Body.Close()
	waitCancel(t, b)
}

func TestBackgroundSyncAndColdOutage(t *testing.T) {
	a := newFake(t, 10, "region")
	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g, s := gateway(t, a)
	var services []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 0 {
		t.Fatal("cold outage must be empty")
	}
	getJSON(t, s.URL+"/healthz", 503, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { g.Run(ctx); close(done) }()
	a.mu.Lock()
	a.down = false
	a.mu.Unlock()
	deadline := time.Now().Add(3 * time.Second)
	for len(services) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		getJSON(t, s.URL+"/api/services", 200, &services)
	}
	if len(services) != 1 {
		t.Fatal("background sync did not recover")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sync loop failed to stop")
	}
}
func TestConfig(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.json")
	for _, raw := range []string{`{"backends":[]}`, `{"unknown":true}`, `{"backends":[{"id":"a","url":"file:///tmp"}]}`, `{"backends":[{"id":"a","url":"http://localhost/api"}]} {}`} {
		if err := os.WriteFile(p, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(p); err == nil {
			t.Fatalf("invalid config accepted: %s", raw)
		}
	}
	if err := os.WriteFile(p, []byte(`{"backends":[{"id":"a","url":"http://localhost/api"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	// readConfig validates while filling these defaults, so this also covers
	// max_stale_seconds >= sync_seconds + catalog_timeout_seconds.
	c, err := loadConfig(p)
	if err != nil || c.SyncSeconds != 30 || c.CatalogTimeoutSeconds != 20 {
		t.Fatalf("default config failed: %+v, %v", c, err)
	}
}
