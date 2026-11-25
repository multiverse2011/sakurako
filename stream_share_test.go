package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

func TestSharedQueueLimitIsBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fast := newSubscriber()
	slow := newSubscriber()
	s := newSharedStream(nil, "s", streamKey{}, "", cancel)
	s.subscribers[fast] = true
	s.subscribers[slow] = true
	fill := byte(0)
	sent := 0
	write := func(size int) {
		t.Helper()
		data := bytes.Repeat([]byte{fill}, size)
		fill++
		if _, err := s.Write(data); err != nil {
			t.Fatal(err)
		}
		sent += size
		got, ok := fast.dequeue()
		if !ok {
			t.Fatalf("fast subscriber did not receive the %d byte chunk", size)
		}
		if len(got) != size {
			t.Fatalf("fast subscriber got %d of %d bytes", len(got), size)
		}
		select {
		case <-slow.dropped:
			t.Fatalf("slow subscriber dropped with %d bytes queued, below the %d byte limit", sent, subscriberQueueBytes)
		default:
		}
	}
	// Mixed 8 KiB and 32 KiB reads: the limit counts bytes, not chunks.
	for i := 0; ; i++ {
		size := 32 << 10
		if i%2 == 0 {
			size = 8 << 10
		}
		if sent+size > subscriberQueueBytes {
			break
		}
		write(size)
	}
	if sent < 8<<20 {
		t.Fatalf("test only queued %d bytes", sent)
	}
	if sent < subscriberQueueBytes {
		write(subscriberQueueBytes - sent)
	}
	// One byte over the limit drops only the slow subscriber.
	if _, err := s.Write([]byte{0x47}); err != nil {
		t.Fatal(err)
	}
	if _, ok := fast.dequeue(); !ok {
		t.Fatal("fast subscriber did not receive the overflowing chunk")
	}
	select {
	case <-slow.dropped:
	default:
		t.Fatalf("slow subscriber not dropped above the %d byte limit", subscriberQueueBytes)
	}
	if ctx.Err() != nil {
		t.Fatal("slow subscriber canceled healthy stream")
	}
}

func TestSharedLateJoinAlignment(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	fast := newSubscriber()
	s := newSharedStream(nil, "s", streamKey{}, "", cancel)
	s.subscribers[fast] = true
	for i := 0; i < 17; i++ {
		if _, err := s.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		if _, ok := fast.dequeue(); !ok {
			t.Fatal("fast subscriber did not receive the chunk")
		}
	}
	// Upstream first byte is packet-aligned; a join after 17 bytes skips 171 bytes.
	late := newSubscriber()
	late.skip = int((188 - s.offset%188) % 188)
	s.subscribers[late] = true
	data := make([]byte, 200)
	data[171] = 0x47
	s.Write(data)
	got, _ := late.dequeue()
	if len(got) != 29 || got[0] != 0x47 {
		t.Fatalf("late join alignment: %d bytes, first %x", len(got), got[0])
	}
}

// recordingWriter records the response body. When release is non-nil, Write
// blocks until it is closed, standing in for a subscriber that stalls.
type recordingWriter struct {
	mu      sync.Mutex
	header  http.Header
	body    bytes.Buffer
	updated chan struct{}
	release <-chan struct{}
}

func newRecordingWriter(release <-chan struct{}) *recordingWriter {
	return &recordingWriter{header: make(http.Header), updated: make(chan struct{}, 1), release: release}
}

func (w *recordingWriter) Header() http.Header { return w.header }
func (w *recordingWriter) WriteHeader(int)     {}
func (w *recordingWriter) SetWriteDeadline(time.Time) error {
	return nil
}
func (w *recordingWriter) FlushError() error { return nil }
func (w *recordingWriter) Write(p []byte) (int, error) {
	if w.release != nil {
		<-w.release
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.body.Write(p)
	select {
	case w.updated <- struct{}{}:
	default:
	}
	return n, err
}
func (w *recordingWriter) size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Len()
}
func (w *recordingWriter) contents() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.body.Bytes()...)
}

func TestSharedSubscriberStallResumesWithoutLoss(t *testing.T) {
	data := make([]byte, 1<<20)
	for i := range data {
		data[i] = byte(i)
	}
	started := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-started:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "video/MP2T")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		if _, err := w.Write(data); err != nil {
			return
		}
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
	release := make(chan struct{})
	var releaseOnce sync.Once
	open := func(w *recordingWriter) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() { _ = recover() }() // sharedStream aborts with ErrAbortHandler
			req, _ := http.NewRequestWithContext(ctx, "GET", "/", nil)
			g.sharedStream(w, req, 1000101, sources, streamOptions{})
		}()
		t.Cleanup(func() {
			cancel()
			releaseOnce.Do(func() { close(release) })
			<-done
		})
	}
	waitSubscribers := func(n int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
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
	waitSize := func(w *recordingWriter, want int) {
		t.Helper()
		timeout := time.After(5 * time.Second)
		for w.size() < want {
			select {
			case <-w.updated:
			case <-timeout:
				t.Fatalf("received %d of %d bytes", w.size(), want)
			}
		}
	}

	fast := newRecordingWriter(nil)
	open(fast)
	waitSubscribers(1)
	slow := newRecordingWriter(release)
	open(slow)
	waitSubscribers(2)
	close(started)

	// The stalled subscriber blocks on its first write while the other one
	// keeps receiving the whole stream.
	waitSize(fast, len(data))
	if n := slow.size(); n != 0 {
		t.Fatalf("stalled subscriber wrote %d bytes before resuming", n)
	}
	waitSubscribers(2) // the stalled subscriber is still attached
	releaseOnce.Do(func() { close(release) })
	waitSize(slow, len(data))
	if got := fast.contents(); !bytes.Equal(got, data) {
		t.Fatalf("fast subscriber lost data: %d bytes", len(got))
	}
	if got := slow.contents(); !bytes.Equal(got, data) {
		t.Fatalf("stalled subscriber lost data after resuming: %d bytes", len(got))
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
