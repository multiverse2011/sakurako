package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// logCapture is a log destination safe to read while stream goroutines write.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// captureLog swaps the standard logger's output until the test ends, so it must
// not be used by tests that run in parallel with anything that logs.
func captureLog(t *testing.T) *logCapture {
	t.Helper()
	c := &logCapture{}
	old := log.Writer()
	log.SetOutput(c)
	t.Cleanup(func() { log.SetOutput(old) })
	return c
}

// waitLog returns the captured output once want has been logged. The line can be
// written by a stream goroutine, so poll instead of reading once.
func waitLog(t *testing.T, c *logCapture, want string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := c.String()
		if strings.Contains(got, want) {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("log line %q not found in:\n%s", want, got)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func requireNotLogged(t *testing.T, got string, unwanted ...string) {
	t.Helper()
	for _, s := range unwanted {
		if strings.Contains(got, s) {
			t.Fatalf("unexpected log line %q in:\n%s", s, got)
		}
	}
}

func TestSharedStreamLifecycleAndOptions(t *testing.T) {
	var calls, active atomic.Int32
	packet := bytes.Repeat([]byte{0xff}, 188)
	packet[0] = 0x47
	svc, prog := fixture(10, 101, "shared")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/services":
			respond(w, 200, []object{svc})
			return
		case "/api/programs":
			respond(w, 200, []object{prog})
			return
		}
		calls.Add(1)
		active.Add(1)
		defer active.Add(-1)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-tick.C:
				if _, err := w.Write(packet); err != nil {
					return
				}
				w.(http.Flusher).Flush()
			}
		}
	}))
	defer upstream.Close()
	g, err := NewGateway(Config{Listen: ":1", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1, Backends: []BackendConfig{{ID: "a", URL: upstream.URL + "/api"}}})
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	server := httptest.NewServer(g)
	defer server.Close()
	open := func(query, priority string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest("GET", server.URL+"/api/services/1000101/stream"+query, nil)
		if priority != "" {
			req.Header.Set("X-Mirakurun-Priority", priority)
		}
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("status %d", resp.StatusCode)
		}
		return resp
	}
	read := func(resp *http.Response) {
		t.Helper()
		got := make([]byte, 188)
		if _, err := io.ReadFull(resp.Body, got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, packet) {
			t.Fatalf("unaligned/corrupt packet: %x", got[:8])
		}
	}
	first := open("?decode=0", "2")
	defer first.Body.Close()
	read(first)
	second := open("?decode=0", "02")
	defer second.Body.Close()
	read(second)
	if calls.Load() != 1 {
		t.Fatalf("same request opened %d upstreams", calls.Load())
	}
	first.Body.Close()
	read(second)
	if active.Load() != 1 {
		t.Fatal("first subscriber canceled shared upstream")
	}
	third := open("?decode=1", "2")
	defer third.Body.Close()
	read(third)
	fourth := open("?decode=0", "3")
	defer fourth.Body.Close()
	read(fourth)
	if calls.Load() != 2 {
		t.Fatalf("different priority was isolated: %d", calls.Load())
	}
	second.Body.Close()
	third.Body.Close()
	fourth.Body.Close()
	deadline := time.Now().Add(time.Second)
	for active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 0 {
		t.Fatal("last subscriber did not cancel upstream")
	}
	fifth := open("?decode=0", "2")
	read(fifth)
	fifth.Body.Close()
	if calls.Load() != 3 {
		t.Fatal("new subscriber reused closed stream")
	}
}

func TestSharedFanoutSlowSubscriberAndAlignment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fast := newSubscriber()
	slow := newSubscriber()
	s := newSharedStream(nil, "s", streamKey{}, "", cancel)
	s.subscribers[fast] = true
	s.subscribers[slow] = true
	for i := 0; i < subscriberQueueSize+1; i++ {
		if _, err := s.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		<-fast.chunks
	}
	select {
	case <-slow.dropped:
	default:
		t.Fatal("slow subscriber not disconnected")
	}
	if ctx.Err() != nil {
		t.Fatal("slow subscriber canceled healthy stream")
	}
	// Upstream first byte is packet-aligned; a join after 17 bytes skips 171 bytes.
	late := newSubscriber()
	late.skip = int((188 - s.offset%188) % 188)
	s.subscribers[late] = true
	data := make([]byte, 200)
	data[171] = 0x47
	s.Write(data)
	got := <-late.chunks
	if len(got) != 29 || got[0] != 0x47 {
		t.Fatalf("late join alignment: %d bytes, first %x", len(got), got[0])
	}
}

func TestSharedStartingRequestSurvivesFirstCancellation(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Write(bytes.Repeat([]byte{0x47}, 188))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	g, err := NewGateway(Config{Listen: ":1", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 5, Backends: []BackendConfig{{ID: "a", URL: upstream.URL}}})
	if err != nil {
		t.Fatal(err)
	}
	g.backends[0].healthy = true
	g.backends[0].updated = time.Now()
	sources := []source{{backend: 0, service: object{"channel": map[string]any{"type": "GR"}}}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.sharedStream(w, r, 1, sources, streamOptions{})
	}))
	defer server.Close()
	firstCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		req, _ := http.NewRequestWithContext(firstCtx, "GET", server.URL, nil)
		resp, _ := testClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	}()
	waitSubscribers := func(n int) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			g.streamsMu.Lock()
			count := 0
			for _, s := range g.streams {
				s.mu.Lock()
				count += len(s.subscribers)
				s.mu.Unlock()
			}
			g.streamsMu.Unlock()
			if count == n {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatalf("did not reach %d subscribers", n)
	}
	waitSubscribers(1)
	secondDone := make(chan *http.Response, 1)
	errDone := make(chan error, 1)
	go func() {
		resp, err := testClient.Get(server.URL)
		if err != nil {
			errDone <- err
			return
		}
		secondDone <- resp
	}()
	waitSubscribers(2)
	cancel()
	<-firstDone
	waitSubscribers(1)
	close(release)
	select {
	case err := <-errDone:
		t.Fatal(err)
	case resp := <-secondDone:
		defer resp.Body.Close()
		buf := make([]byte, 188)
		if _, err := io.ReadFull(resp.Body, buf); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("remaining subscriber stalled")
	}
	if calls.Load() != 1 {
		t.Fatalf("startup was not shared: %d upstream requests", calls.Load())
	}
}

func TestSharedIdentityPartition(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write(bytes.Repeat([]byte{0x47}, 188))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer upstream.Close()
	g, err := NewGateway(Config{Listen: ":1", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 5, Backends: []BackendConfig{{ID: "a", URL: upstream.URL, ServiceDomain: "region-a"}, {ID: "b", URL: upstream.URL, ServiceDomain: "region-b"}}})
	if err != nil {
		t.Fatal(err)
	}
	g.backends[0].healthy = true
	g.backends[0].updated = time.Now()
	g.backends[1].healthy = true
	g.backends[1].updated = time.Now()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backend, typ := 0, "GR"
		if r.URL.Path == "/domain" {
			backend = 1
		}
		if r.URL.Path == "/type" {
			typ = "BS"
		}
		g.sharedStream(w, r, 1, []source{{backend: backend, service: object{"channel": map[string]any{"type": typ}}}}, streamOptions{})
	}))
	defer server.Close()
	for _, path := range []string{"/", "/domain", "/type"} {
		resp, err := testClient.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatal(resp.Status)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("different identities shared: %d", calls.Load())
	}
}

func TestPriorityChangesShareExistingStream(t *testing.T) {
	a := newFake(t, 10, "A")
	a.mu.Lock()
	a.continuous = true
	a.mu.Unlock()
	g, s := gateway(t, a)

	open := func(priority string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest("GET", streamURL(s.URL, 1000101)+"?decode=0", nil)
		if priority != "" {
			req.Header.Set("X-Mirakurun-Priority", priority)
		}
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("priority %q: HTTP %d", priority, resp.StatusCode)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	groups := func() int {
		g.streamsMu.Lock()
		defer g.streamsMu.Unlock()
		return len(g.streams)
	}

	high := open("3")
	if got := (<-a.started).Get("X-Mirakurun-Priority"); got != "3" {
		t.Fatalf("upstream priority %q", got)
	}
	low := open("1") // same service, decode and domain; joins priority 3
	if n := groups(); n != 1 {
		t.Fatalf("lower priority did not share: %d upstream streams", n)
	}
	higher := open("9") // also joins; the upstream allocation stays 3
	if n := groups(); n != 1 {
		t.Fatalf("priority change opened another stream: %d upstream streams", n)
	}
	if n := a.streamCalls.Load(); n != 1 {
		t.Fatalf("upstream calls %d", n)
	}
	high.Body.Close()
	low.Body.Close()
	higher.Body.Close()
}

func TestOmittedPrioritySharesExplicitStream(t *testing.T) {
	a := newFake(t, 10, "A")
	a.mu.Lock()
	a.continuous = true
	a.mu.Unlock()
	g, s := gateway(t, a)

	open := func(header bool) *http.Response {
		t.Helper()
		req, _ := http.NewRequest("GET", streamURL(s.URL, 1000101)+"?decode=0", nil)
		if header {
			req.Header.Set("X-Mirakurun-Priority", "0")
		}
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	first := open(false)
	<-a.started
	second := open(true)
	g.streamsMu.Lock()
	n := len(g.streams)
	g.streamsMu.Unlock()
	if n != 1 {
		t.Fatalf("omitted and explicit priority did not share: %d upstream streams", n)
	}
	first.Body.Close()
	second.Body.Close()
}

func TestDecodeZeroRidesDecodedStream(t *testing.T) {
	a := newFake(t, 10, "A")
	a.mu.Lock()
	a.continuous = true
	a.mu.Unlock()
	g, s := gateway(t, a)

	open := func(query string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest("GET", streamURL(s.URL, 1000101)+query, nil)
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	groups := func() int {
		g.streamsMu.Lock()
		defer g.streamsMu.Unlock()
		return len(g.streams)
	}

	decoded := open("?decode=1")
	<-a.started
	undecoded := open("?decode=0") // joins the decoded stream
	if n := groups(); n != 1 {
		t.Fatalf("decode=0 did not ride decoded stream: %d streams", n)
	}
	if n := a.streamCalls.Load(); n != 1 {
		t.Fatalf("upstream calls %d", n)
	}
	decoded.Body.Close()
	undecoded.Body.Close()
}

func TestDecodeOneDoesNotJoinUndecodedStream(t *testing.T) {
	a := newFake(t, 10, "A")
	a.mu.Lock()
	a.continuous = true
	a.mu.Unlock()
	g, s := gateway(t, a)

	open := func(query string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest("GET", streamURL(s.URL, 1000101)+query, nil)
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	undecoded := open("?decode=0")
	<-a.started
	decoded := open("?decode=1")
	<-a.started
	g.streamsMu.Lock()
	n := len(g.streams)
	g.streamsMu.Unlock()
	if n != 2 {
		t.Fatalf("decode=1 joined undecoded stream: %d streams", n)
	}
	undecoded.Body.Close()
	decoded.Body.Close()
}

func TestUpstreamChannelPathEscapedOnce(t *testing.T) {
	a := newFake(t, 10, "A")
	a.services[0]["channel"].(object)["channel"] = "BS 01"
	_, s := configGateway(t, Config{}, a)
	name := channelByName(t, s, 10)

	resp, err := testClient.Get(s.URL + "/api/channels/GR/" + name + "/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	h := <-a.started
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	raw := h.Get("Test-RequestURI")
	if !strings.Contains(raw, "/api/channels/GR/BS%2001/stream") {
		t.Fatalf("upstream request target %q not escaped once", raw)
	}
	if strings.Contains(raw, "%2520") {
		t.Fatalf("channel name double-escaped: %q", raw)
	}
	if got := h.Get("Test-Path"); got != "/api/channels/GR/BS 01/stream" {
		t.Fatalf("decoded upstream path %q", got)
	}
}

func TestChannelStreamCooldownAndSharing(t *testing.T) {
	t.Run("503 cools only the channel", func(t *testing.T) {
		a := newFake(t, 10, "A")
		svcY, progY := fixture(10, 202, "Y")
		svcY["channel"].(object)["channel"] = "28"
		a.services = append(a.services, svcY)
		a.programs = append(a.programs, progY)
		// Channel paths parse to service id 0, so this makes every channel stream 503.
		a.streamStatusFor = map[int64]int{0: 503}
		g, s := gateway(t, a)

		name := g.channelName(g.config.Backends[0], "27")
		resp, err := testClient.Get(s.URL + "/api/channels/GR/" + name + "/stream")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 503 {
			t.Fatalf("cooled channel stream HTTP %d", resp.StatusCode)
		}
		g.mu.RLock()
		blocked := g.backends[0].blockedChannels
		channelBlocked := blocked["GR/"+name] != (time.Time{})
		otherBlocked := blocked["GR/"+g.channelName(g.config.Backends[0], "28")] != (time.Time{})
		backendWide := !g.backends[0].blockedUntil.IsZero()
		g.mu.RUnlock()
		if !channelBlocked || otherBlocked || backendWide {
			t.Fatalf("cooldown scope wrong: channel=%v other=%v backend-wide=%v", channelBlocked, otherBlocked, backendWide)
		}

		// A different channel on the same backend still streams.
		a.mu.Lock()
		a.streamStatusFor = nil
		a.mu.Unlock()
		nameY := g.channelName(g.config.Backends[0], "28")
		resp, err = testClient.Get(s.URL + "/api/channels/GR/" + nameY + "/stream")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
			t.Fatalf("X-Accel-Buffering = %q", got)
		}
		if got := resp.Header.Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control = %q", got)
		}
		if body := readStreamBody(t, resp); body != "transport-stream" {
			t.Fatalf("second channel body %q", body)
		}
	})

	t.Run("concurrent requests share upstream", func(t *testing.T) {
		a := newFake(t, 10, "A")
		a.mu.Lock()
		a.continuous = true
		a.mu.Unlock()
		g, s := gateway(t, a)
		url := s.URL + "/api/channels/GR/" + g.channelName(g.config.Backends[0], "27") + "/stream"

		first, err := testClient.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer first.Body.Close()
		waitStarted(t, a)
		second, err := testClient.Get(url)
		if err != nil {
			t.Fatal(err)
		}
		defer second.Body.Close()

		g.streamsMu.Lock()
		streams, subscribers := len(g.streams), 0
		for _, st := range g.streams {
			st.mu.Lock()
			subscribers += len(st.subscribers)
			st.mu.Unlock()
		}
		g.streamsMu.Unlock()
		if streams != 1 || subscribers != 2 {
			t.Fatalf("shared channel stream: %d streams, %d subscribers", streams, subscribers)
		}
		if n := a.streamCalls.Load(); n != 1 {
			t.Fatalf("upstream /stream calls %d", n)
		}
		first.Body.Close()
		second.Body.Close()
		waitCancel(t, a)
	})
}

func TestDecodeUnsetIsSeparateGroup(t *testing.T) {
	a := newFake(t, 10, "A")
	a.mu.Lock()
	a.continuous = true
	a.mu.Unlock()
	g, s := gateway(t, a)

	open := func(query string) *http.Response {
		t.Helper()
		resp, err := testClient.Get(streamURL(s.URL, 1000101) + query)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatal(err)
		}
		return resp
	}
	unset := open("")
	defer unset.Body.Close()
	<-a.started
	zero := open("?decode=0")
	defer zero.Body.Close()

	g.streamsMu.Lock()
	n := len(g.streams)
	g.streamsMu.Unlock()
	if n != 2 {
		t.Fatalf("omitted decode and decode=0 shared a group: %d streams", n)
	}
	if calls := a.streamCalls.Load(); calls != 2 {
		t.Fatalf("upstream /stream calls %d", calls)
	}
}

// holdWriter blocks the subscriber's first write so its queue overflows without
// depending on socket buffers draining at some speed.
type holdWriter struct {
	header  http.Header
	release chan struct{}
}

func (w *holdWriter) Header() http.Header              { return w.header }
func (w *holdWriter) WriteHeader(int)                  {}
func (w *holdWriter) Flush()                           {}
func (w *holdWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *holdWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

func TestStreamEndLogsNormalClose(t *testing.T) {
	a := newFake(t, 10, "A")
	g, s := gateway(t, a)
	channel := g.channelName(g.config.Backends[0], "27")
	for _, tc := range []struct{ name, path, label string }{
		{"service", streamURL(s.URL, 1000101), "service 1000101"},
		{"channel", s.URL + "/api/channels/GR/" + channel + "/stream", "channel GR/" + channel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLog(t)
			resp, err := testClient.Get(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			// The fake sends exactly this one body, which fixes the byte count.
			if body := readStreamBody(t, resp); body != "transport-stream" {
				t.Fatalf("bad body %q", body)
			}
			resp.Body.Close()
			waitCancel(t, a)

			got := waitLog(t, logs, tc.label+" via region0: ended after")
			if !strings.Contains(got, ", 16 B") {
				t.Fatalf("summary lacks the transferred size:\n%s", got)
			}
			if n := strings.Count(got, tc.label+" via region0: ended after"); n != 1 {
				t.Fatalf("end summary logged %d times:\n%s", n, got)
			}
			requireNotLogged(t, got, "client disconnected", "context canceled", "aborted", "dropped")
		})
	}
}

func TestStreamEndLogsSlowDrop(t *testing.T) {
	svc, prog := fixture(10, 101, "slow")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/services":
			respond(w, 200, []object{svc})
			return
		case "/api/programs":
			respond(w, 200, []object{prog})
			return
		}
		w.Header().Set("Content-Type", "video/MP2T")
		w.WriteHeader(200)
		chunk := bytes.Repeat([]byte{0x47}, 32*1024)
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
	}))
	defer upstream.Close()
	g, err := NewGateway(Config{Listen: ":1", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 30, Backends: []BackendConfig{{ID: "a", URL: upstream.URL + "/api"}}})
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	channel := g.channelName(g.config.Backends[0], "27")
	for _, tc := range []struct{ name, path, label string }{
		{"service", "/api/services/1000101/stream", "service 1000101"},
		{"channel", "/api/channels/GR/" + channel + "/stream", "channel GR/" + channel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLog(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rw := &holdWriter{header: http.Header{}, release: make(chan struct{})}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() {
					// The handler aborts the response once the stream ends.
					if v := recover(); v != nil && v != http.ErrAbortHandler {
						t.Errorf("stream handler panicked: %v", v)
					}
				}()
				g.ServeHTTP(rw, httptest.NewRequest("GET", tc.path, nil).WithContext(ctx))
			}()

			got := waitLog(t, logs, "("+tc.label+" via a): dropping slow subscriber")
			// The blocked handler cannot clean up, so an empty stream map means
			// upstreamStream made its end-of-stream decision without a second line.
			waitStreamsDrained(t, g)
			close(rw.release)
			<-done
			got = logs.String()
			if n := strings.Count(got, "("+tc.label+" via a): dropping slow subscriber"); n != 1 {
				t.Fatalf("slow drop logged %d times:\n%s", n, got)
			}
			requireNotLogged(t, got, "client disconnected", "ended after", "aborted", "closed pipe", tc.label+" via a:")
		})
	}
}

func TestStreamEndLogsUpstreamFailure(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*fakeBackend)
	}{
		{"eof", func(f *fakeBackend) { f.end = true }},
		{"mid-stream reset", func(f *fakeBackend) { f.resetAfterChunk = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLog(t)
			a := newFake(t, 10, "A")
			a.mu.Lock()
			tc.setup(a)
			a.mu.Unlock()
			_, s := gateway(t, a)

			resp, err := testClient.Get(streamURL(s.URL, 1000101))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			got := waitLog(t, logs, "service 1000101 via region0 aborted: ")
			if n := strings.Count(got, "service 1000101 via region0 aborted: "); n != 1 {
				t.Fatalf("upstream failure logged %d times:\n%s", n, got)
			}
			requireNotLogged(t, got, "client disconnected", "ended after", "dropped")
		})
	}
}

func TestStreamEndLogsBeforeStart(t *testing.T) {
	logs := captureLog(t)
	a := newFake(t, 10, "A")
	a.mu.Lock()
	a.stall = true
	a.mu.Unlock()
	g, s := gateway(t, a)

	// The response headers wait for the first byte, so the client must leave
	// while the request is still in flight.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequestWithContext(ctx, "GET", streamURL(s.URL, 1000101), nil)
		if resp, err := testClient.Do(req); resp != nil {
			resp.Body.Close()
		} else if err != nil && ctx.Err() == nil {
			t.Errorf("request failed before the client left: %v", err)
		}
	}()
	waitStarted(t, a)
	cancel()
	<-done
	waitCancel(t, a)
	waitStreamsDrained(t, g)
	// Nothing is logged in this case; let the stream goroutine settle first.
	time.Sleep(50 * time.Millisecond)
	requireNotLogged(t, logs.String(), "client disconnected", "ended after", "aborted", "dropped")
}

func TestFormatLogDuration(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{0, "0m00s"},
		{400 * time.Millisecond, "0m00s"},
		{59*time.Second + 400*time.Millisecond, "0m59s"},
		{59*time.Second + 600*time.Millisecond, "1m00s"},
		{2*time.Minute + 3*time.Second, "2m03s"},
		{time.Hour + 2*time.Minute + 3*time.Second + 400*time.Millisecond, "1h02m03s"},
	} {
		if got := formatLogDuration(tc.d); got != tc.want {
			t.Errorf("formatLogDuration(%v) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestFormatLogBytes(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1 << 20, "1.0 MiB"},
		{3328599654, "3.1 GiB"},
		{2 << 40, "2.0 TiB"},
	} {
		if got := formatLogBytes(tc.n); got != tc.want {
			t.Errorf("formatLogBytes(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}
