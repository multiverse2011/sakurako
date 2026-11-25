package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func streamURL(base string, id int64) string {
	return base + "/api/services/" + strconv.FormatInt(id, 10) + "/stream"
}

// Test-facing wrapper for blockKey; serviceOnly=false cools the whole backend.
func (g *Gateway) block(index int, id int64, serviceOnly bool, reason string) {
	g.blockKey(index, streamKey{id: id}, serviceOnly, reason)
}

func waitStreamsDrained(t *testing.T, g *Gateway) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.streamsMu.Lock()
		n := len(g.streams)
		g.streamsMu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("streams did not drain: %d", n)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitStarted(t *testing.T, f *fakeBackend) http.Header {
	t.Helper()
	select {
	case h := <-f.started:
		return h
	case <-time.After(3 * time.Second):
		t.Fatal("upstream stream was not requested")
		return nil
	}
}

func conflictGateway(t *testing.T) (*Gateway, *httptest.Server) {
	t.Helper()
	a, b := newFake(t, 10, "gr"), newFake(t, 10, "bs")
	b.services[0]["channel"].(object)["type"] = "BS"
	return gateway(t, a, b)
}

// A silent upstream (one chunk, then no bytes) must hit the per-read idle
// deadline, abort the downstream, cool the backend and fail over next time.
func TestMidStreamUpstreamSilenceCoolsBackend(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	a.oneChunk = true
	g, s := gateway(t, a, b)

	start := time.Now()
	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 188)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if first[0] != 0x47 {
		t.Fatalf("bad chunk header: %x", first[0])
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("live stream ended cleanly after upstream went silent")
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("silent upstream was not aborted near StreamTimeoutSeconds: %v", elapsed)
	}

	g.mu.RLock()
	blocked := !g.backends[0].blockedUntil.IsZero()
	g.mu.RUnlock()
	if !blocked {
		t.Fatal("silent upstream was not cooled down")
	}

	waitStreamsDrained(t, g)
	resp2, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	body := make([]byte, len("transport-stream"))
	if _, err := io.ReadFull(resp2.Body, body); err != nil {
		t.Fatalf("backup stream: %v", err)
	}
	if string(body) != "transport-stream" {
		t.Fatalf("unexpected backup body %q", body)
	}
	select {
	case <-b.started:
	default:
		t.Fatal("backup backend was not used after cooldown")
	}
}

func TestMidStreamUpstreamResetVsClientDisconnect(t *testing.T) {
	t.Run("upstream reset truncates and cools", func(t *testing.T) {
		a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
		a.resetAfterChunk = true
		g, s := gateway(t, a, b)
		resp, err := testClient.Get(streamURL(s.URL, 1000101))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatalf("first chunk: %v", err)
		}
		if _, err := io.ReadAll(resp.Body); err == nil {
			t.Fatal("upstream reset looked like a complete recording")
		}
		g.mu.RLock()
		blocked := !g.backends[0].blockedUntil.IsZero()
		g.mu.RUnlock()
		if !blocked {
			t.Fatal("backend read error was not cooled down")
		}
		if b.streamCalls.Load() != 0 {
			t.Fatal("switched backends after body bytes were sent")
		}
	})

	t.Run("client disconnect does not cool", func(t *testing.T) {
		a := newFake(t, 10, "only")
		a.oneChunk = true
		g, s := gateway(t, a)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, "GET", streamURL(s.URL, 1000101), nil)
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatalf("first chunk: %v", err)
		}
		cancel()
		resp.Body.Close()
		select {
		case <-a.canceled:
		case <-time.After(3 * time.Second):
			t.Fatal("upstream was not canceled on client disconnect")
		}
		// A wrong classification blocks immediately; keep watching briefly to catch it.
		deadline := time.Now().Add(500 * time.Millisecond)
		for {
			g.mu.RLock()
			blocked := !g.backends[0].blockedUntil.IsZero() || !g.backends[0].blockedServices[1000101].IsZero()
			g.mu.RUnlock()
			if blocked {
				t.Fatal("client disconnect penalized the backend")
			}
			if time.Now().After(deadline) {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
}

// breakStream reads the first chunk of a mid-stream failure so the backend is
// cooled down, then leaves the request. It returns the cooldown deadline.
func breakStream(t *testing.T, g *Gateway, url string) time.Time {
	t.Helper()
	resp, err := testClient.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("upstream reset looked like a complete stream")
	}
	waitStreamsDrained(t, g)
	g.mu.RLock()
	until := g.backends[0].blockedUntil
	g.mu.RUnlock()
	if until.IsZero() {
		t.Fatal("mid-stream failure did not cool the backend")
	}
	return until
}

// A stalled test process must not let the cooldown expire unnoticed, which would
// let the normal path pass instead of the cooling fallback.
func withinCooldown(t *testing.T, until time.Time, stage string) {
	t.Helper()
	if !time.Now().Before(until) {
		t.Fatalf("cooldown expired %s; the test can no longer prove the cooldown was in effect", stage)
	}
}

// A sole source that failed mid-stream is cooled down, but it must still be
// tried: once it recovered, the next request streams instead of waiting out the
// whole cooldown.
func TestSoleSourceRetriedDuringCooldown(t *testing.T) {
	a := newFake(t, 10, "only")
	a.resetAfterChunk = true
	g, s := gateway(t, a)
	until := breakStream(t, g, streamURL(s.URL, 1000101))

	a.mu.Lock()
	a.resetAfterChunk = false
	a.mu.Unlock()

	withinCooldown(t, until, "before the HEAD request")
	req, _ := http.NewRequest("HEAD", streamURL(s.URL, 1000101), nil)
	head, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	withinCooldown(t, until, "after the HEAD response headers")
	if head.StatusCode != 200 {
		t.Fatalf("HEAD on the cooling sole source: HTTP %d", head.StatusCode)
	}

	withinCooldown(t, until, "before the GET request")
	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	withinCooldown(t, until, "after the GET response headers")
	if resp.StatusCode != 200 {
		t.Fatalf("recovered sole source: HTTP %d", resp.StatusCode)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	resp.Body.Close()
	waitCancel(t, a)
}

// The same rescue applies to channel streams, and HEAD must not open a connection.
func TestSoleChannelSourceRetriedDuringCooldown(t *testing.T) {
	a := newFake(t, 10, "only")
	a.resetAfterChunk = true
	g, s := gateway(t, a)
	name := g.channelName(g.config.Backends[0], "27")
	url := s.URL + "/api/channels/GR/" + name + "/stream"
	until := breakStream(t, g, url)

	a.mu.Lock()
	a.resetAfterChunk = false
	a.mu.Unlock()

	calls := a.streamCalls.Load()
	withinCooldown(t, until, "before the HEAD request")
	req, _ := http.NewRequest("HEAD", url, nil)
	head, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	withinCooldown(t, until, "after the HEAD response headers")
	if head.StatusCode != 200 {
		t.Fatalf("HEAD on the cooling sole channel: HTTP %d", head.StatusCode)
	}
	if got := a.streamCalls.Load(); got != calls {
		t.Fatalf("HEAD opened %d upstream connections", got-calls)
	}

	withinCooldown(t, until, "before the GET request")
	resp, err := testClient.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	withinCooldown(t, until, "after the GET response headers")
	if resp.StatusCode != 200 {
		t.Fatalf("recovered sole channel source: HTTP %d", resp.StatusCode)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	resp.Body.Close()
	waitCancel(t, a)
}

// A rescue attempt on a source that has not actually recovered is still made
// once, and the gateway then reports 503.
func TestSoleSourceRescueFailureReturns503(t *testing.T) {
	a := newFake(t, 10, "only")
	a.resetAfterChunk = true
	g, s := gateway(t, a)
	until := breakStream(t, g, streamURL(s.URL, 1000101))

	a.mu.Lock()
	a.resetAfterChunk = false
	a.streamStatus = 503
	a.mu.Unlock()

	calls := a.streamCalls.Load()
	withinCooldown(t, until, "before the GET request")
	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	withinCooldown(t, until, "after the GET response headers")
	if resp.StatusCode != 503 {
		t.Fatalf("unrecovered sole source: HTTP %d, want 503", resp.StatusCode)
	}
	if got := a.streamCalls.Load(); got != calls+1 {
		t.Fatalf("upstream calls = %d, want %d", got, calls+1)
	}
}

// A cooling source stays a last resort: while another source is usable it is not
// tried, even though that usable source fails.
func TestCoolingSourceIsNotTriedWhileAnotherIsUsable(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	b.streamStatus = 503
	g, s := gateway(t, a, b)
	g.block(0, 1000101, false, "test")
	g.mu.RLock()
	until := g.backends[0].blockedUntil
	g.mu.RUnlock()

	withinCooldown(t, until, "before the GET request")
	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	withinCooldown(t, until, "after the GET response headers")
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 503 {
		t.Fatalf("HTTP %d, want 503", resp.StatusCode)
	}
	if a.streamCalls.Load() != 0 {
		t.Fatal("cooling source was tried while another source was usable")
	}
	if b.streamCalls.Load() != 1 {
		t.Fatalf("usable source calls = %d, want 1", b.streamCalls.Load())
	}
}

// A service cooldown (a tuner shortage, for example) is not rescued even when it
// is the only candidate; only a whole-backend cooldown is.
func TestServiceCooldownIsNotRescued(t *testing.T) {
	a := newFake(t, 10, "only")
	g, s := gateway(t, a)
	g.block(0, 1000101, true, "test")
	g.mu.RLock()
	until := g.backends[0].blockedServices[1000101]
	g.mu.RUnlock()

	withinCooldown(t, until, "before the GET request")
	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	withinCooldown(t, until, "after the GET response headers")
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 503 {
		t.Fatalf("GET HTTP %d, want 503", resp.StatusCode)
	}
	if a.streamCalls.Load() != 0 {
		t.Fatal("service cooldown was rescued")
	}

	req, _ := http.NewRequest("HEAD", streamURL(s.URL, 1000101), nil)
	head, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	if head.StatusCode != 503 {
		t.Fatalf("HEAD HTTP %d, want 503", head.StatusCode)
	}
	if a.streamCalls.Load() != 0 {
		t.Fatal("HEAD opened an upstream connection for a service cooldown")
	}
}

// A source that another stream cools down while this request waits on an earlier
// source must be skipped; the next usable source serves instead.
func TestCandidateCooledMidRequestIsSkipped(t *testing.T) {
	a, b, c := newFake(t, 10, "A"), newFake(t, 10, "B"), newFake(t, 10, "C")
	a.hold = make(chan struct{})
	// The long stream timeout is only a hang guard: A must fail when the test
	// releases it, never on its own, so B is not reached before it is cooled down.
	g, s := gatewayWithTimeouts(t, 1, 30, a, b, c)

	type result struct {
		status int
		body   string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := testClient.Get(streamURL(s.URL, 1000101))
		if err != nil {
			done <- result{err: err}
			return
		}
		defer resp.Body.Close()
		buf := make([]byte, len("transport-stream"))
		_, err = io.ReadFull(resp.Body, buf)
		done <- result{status: resp.StatusCode, body: string(buf), err: err}
	}()

	// A has been tried and now waits on its hold channel; cool B down before
	// letting A fail, so no upstream timeout decides the order.
	waitStarted(t, a)
	g.block(1, 1000101, false, "test")
	g.mu.RLock()
	until := g.backends[1].blockedUntil
	g.mu.RUnlock()
	close(a.hold)

	got := <-done
	if got.err != nil {
		t.Fatal(got.err)
	}
	withinCooldown(t, until, "after the GET response headers")
	if got.status != 200 || got.body != "transport-stream" {
		t.Fatalf("HTTP %d body %q", got.status, got.body)
	}
	if b.streamCalls.Load() != 0 {
		t.Fatal("source cooled down mid-request was still tried")
	}
	if c.streamCalls.Load() != 1 {
		t.Fatalf("third source calls = %d, want 1", c.streamCalls.Load())
	}
	waitCancel(t, c)
}

// attemptSources prefers sources outside their cooldowns and falls back to healthy
// sources that are only cooling as a whole backend when there are no others.
func TestAttemptSourcesSelection(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "A"), newFake(t, 10, "B"), newFake(t, 10, "C"))
	sources := []source{{backend: 0}, {backend: 1}, {backend: 2}}
	key := streamKey{id: 1000101}
	now := time.Now()

	selected := func(at time.Time) ([]int, bool) {
		t.Helper()
		g.mu.RLock()
		defer g.mu.RUnlock()
		got, rescue := g.attemptSources(sources, key, at)
		backends := make([]int, len(got))
		for i, s := range got {
			backends[i] = s.backend
		}
		return backends, rescue
	}
	edit := func(i int, fn func(*backend)) {
		g.mu.Lock()
		defer g.mu.Unlock()
		fn(&g.backends[i])
	}
	cool := func(i int) { edit(i, func(b *backend) { b.blockedUntil = now.Add(time.Second) }) }
	want := func(at time.Time, backends []int, rescue bool) {
		t.Helper()
		got, gotRescue := selected(at)
		if !slices.Equal(got, backends) || gotRescue != rescue {
			t.Fatalf("attemptSources = %v rescue=%v, want %v rescue=%v", got, gotRescue, backends, rescue)
		}
	}

	want(now, []int{0, 1, 2}, false)
	cool(1)
	want(now, []int{0, 2}, false)
	cool(0)
	want(now, []int{2}, false)
	cool(2)
	want(now, []int{0, 1, 2}, true)
	// Expired cooldowns are normal candidates again.
	want(now.Add(2*time.Second), []int{0, 1, 2}, false)
	// A service cooldown is kept even in rescue mode.
	edit(1, func(b *backend) {
		b.blockedServices = map[int64]time.Time{key.id: now.Add(time.Second)}
	})
	want(now, []int{0, 2}, true)
	// An unhealthy source is never a candidate, even as a rescue.
	edit(0, func(b *backend) { b.healthy = false })
	want(now, []int{2}, true)
}

func TestSameDomainDifferentChannelTypeConflicts(t *testing.T) {
	_, s := conflictGateway(t)
	var services, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 0 || len(programs) != 0 {
		t.Fatalf("GR/BS identity conflict leaked into catalog: %d/%d", len(services), len(programs))
	}
	var h struct {
		Ready     bool    `json:"ready"`
		Conflicts []int64 `json:"conflicting_service_ids"`
	}
	getJSON(t, s.URL+"/healthz", 503, &h)
	if h.Ready || len(h.Conflicts) != 1 || h.Conflicts[0] != 1000101 {
		t.Fatalf("health did not report conflict: %+v", h)
	}
	getJSON(t, s.URL+"/api/services/1000101/stream", 409, nil)
}

func TestServiceDetailConflictStatus(t *testing.T) {
	_, s := conflictGateway(t)
	getJSON(t, s.URL+"/api/services/1000101", 409, nil)
}

func TestCatalogBodyCapKeepsPreviousSnapshot(t *testing.T) {
	a := newFake(t, 10, "good")
	// A larger catalog timeout keeps the 64 MiB transfer from racing the deadline.
	g, s := gatewayWithCatalogTimeout(t, 10, a)
	a.mu.Lock()
	a.oversizeServices = true
	a.mu.Unlock()
	g.Sync(context.Background())

	g.mu.RLock()
	healthy, syncErr := g.backends[0].healthy, g.backends[0].err
	g.mu.RUnlock()
	if healthy {
		t.Fatalf("cap accepted the oversized body: err=%q", syncErr)
	}
	if !strings.Contains(syncErr, "64 MiB") {
		t.Fatalf("oversized catalog rejected for a different reason: err=%q", syncErr)
	}
	var services, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 1 || len(programs) != 1 || services[0]["name"] != "good" {
		t.Fatalf("previous snapshot lost: %d services, %d programs", len(services), len(programs))
	}
}

func TestOneSidedFetchFailureIsAtomic(t *testing.T) {
	a := newFake(t, 10, "good")
	g, s := gateway(t, a)

	// New services but a failing /programs must not commit the services alone.
	a.mu.Lock()
	svc := clone(a.services[0])
	svc["name"] = "new"
	a.services = []object{svc}
	a.failPrograms = 503
	a.mu.Unlock()
	g.Sync(context.Background())

	var services, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 1 || services[0]["name"] != "good" {
		t.Fatalf("services committed despite failing programs: %v", services)
	}
	if len(programs) != 1 || programs[0]["name"] != "good program" {
		t.Fatalf("previous programs lost: %v", programs)
	}

	// Reverse: a failing /services must not commit the new programs alone.
	a.mu.Lock()
	svc = clone(a.services[0])
	svc["name"] = "newer"
	a.services = []object{svc}
	prog := clone(a.programs[0])
	prog["name"] = "new program"
	a.programs = []object{prog}
	a.failPrograms = 0
	a.failServices = 503
	a.mu.Unlock()
	g.Sync(context.Background())

	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 1 || services[0]["name"] != "good" || len(programs) != 1 || programs[0]["name"] != "good program" {
		t.Fatalf("one-sided failure was not atomic: %v / %v", services, programs)
	}
}

func TestUpstreamStreamRequestContract(t *testing.T) {
	a := newFake(t, 10, "backend")
	_, s := gateway(t, a)

	req, _ := http.NewRequest("GET", streamURL(s.URL, 1000101), nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Cookie", "session=secret")
	req.Header.Set("X-Custom", "should-not-be-forwarded")
	req.Header.Set("User-Agent", "client-agent")
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("stream HTTP %d", resp.StatusCode)
	}
	h := <-a.started
	if got := h.Get("Test-Path"); got != "/api/services/1000101/stream" {
		t.Fatalf("upstream path %q", got)
	}
	if got := h.Get("User-Agent"); got != "Sakurako" {
		t.Fatalf("upstream User-Agent %q", got)
	}
	for _, name := range []string{"Authorization", "Cookie", "X-Custom"} {
		if v := h.Values(name); len(v) != 0 {
			t.Fatalf("client header %s forwarded: %v", name, v)
		}
	}
	if v := h.Values("X-Mirakurun-Priority"); len(v) != 0 {
		t.Fatalf("omitted priority forwarded: %v", v)
	}
	resp.Body.Close()
	waitCancel(t, a)

	req, _ = http.NewRequest("GET", streamURL(s.URL, 1000101)+"?decode=0", nil)
	req.Header.Set("X-Mirakurun-Priority", "007")
	resp, err = testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	h = <-a.started
	if got := h.Get("X-Mirakurun-Priority"); got != "7" {
		t.Fatalf("priority not normalized upstream: %q", got)
	}
	resp.Body.Close()
	waitCancel(t, a)

	// A redirect must not be followed; the gateway reports 502 to the client.
	a.mu.Lock()
	a.streamStatus = 302
	a.streamLocation = "/api/services/1000101/stream"
	a.mu.Unlock()
	resp, err = testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 502 {
		t.Fatalf("redirect was not treated as a refused stream: HTTP %d", resp.StatusCode)
	}
	if calls := a.streamCalls.Load(); calls != 3 {
		t.Fatalf("upstream redirect was followed: %d stream calls", calls)
	}
}

func TestServiceScoped404ThroughRealStreamPath(t *testing.T) {
	a := newFake(t, 10, "A")
	svcY, progY := fixture(10, 202, "Y")
	a.services = append(a.services, svcY)
	a.programs = append(a.programs, progY)
	a.streamStatusFor = map[int64]int{1000101: 404}
	b := newFake(t, 10, "B")
	g, s := gateway(t, a, b)

	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, len("transport-stream"))
	if _, err := io.ReadFull(resp.Body, body); err != nil {
		t.Fatalf("fallback stream: %v", err)
	}
	select {
	case <-b.started:
	default:
		t.Fatal("404 on service X did not fall back to B")
	}
	resp.Body.Close()
	waitCancel(t, b)

	g.mu.RLock()
	backendWide := !g.backends[0].blockedUntil.IsZero()
	serviceBlocked := !g.backends[0].blockedServices[1000101].IsZero()
	g.mu.RUnlock()
	if backendWide || !serviceBlocked {
		t.Fatalf("404 cooldown scope wrong: backend-wide=%v service=%v", backendWide, serviceBlocked)
	}

	respY, err := testClient.Get(streamURL(s.URL, 1000202))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(respY.Body, body); err != nil {
		t.Fatalf("service Y stream: %v", err)
	}
	select {
	case <-a.started:
	default:
		t.Fatal("service Y skipped A after A 404'd for X")
	}
	respY.Body.Close()
	waitCancel(t, a)
}

func TestEqualPriorityTieBreakByBackendID(t *testing.T) {
	bravo, alpha := newFake(t, 10, "bravo"), newFake(t, 10, "alpha")
	c := Config{Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1, Backends: []BackendConfig{
		{ID: "bravo", URL: bravo.server.URL + "/api", Priority: 5, ServiceDomain: "same"},
		{ID: "alpha", URL: alpha.server.URL + "/api", Priority: 5, ServiceDomain: "same"},
	}}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)

	assertAlphaAuthority := func() {
		t.Helper()
		var services, programs []object
		getJSON(t, s.URL+"/api/services", 200, &services)
		getJSON(t, s.URL+"/api/programs", 200, &programs)
		if len(services) != 1 || services[0]["name"] != "alpha" {
			t.Fatalf("authority not the lexicographically lower ID: %v", services)
		}
		if len(programs) != 1 || programs[0]["name"] != "alpha program" {
			t.Fatalf("program authority mismatch: %v", programs)
		}
	}
	assertAlphaAuthority()
	g.Sync(context.Background())
	assertAlphaAuthority()
}

func TestStreamHeaderValidation(t *testing.T) {
	a := newFake(t, 10, "x")
	_, s := gateway(t, a)
	do := func(method, query string, priorities ...string) int {
		t.Helper()
		req, _ := http.NewRequest(method, streamURL(s.URL, 1000101)+query, nil)
		for _, p := range priorities {
			req.Header.Add("X-Mirakurun-Priority", p)
		}
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	for name, tc := range map[string]struct {
		method, query string
		priorities    []string
	}{
		"duplicate priority":    {"GET", "", []string{"1", "2"}},
		"non-numeric priority":  {"GET", "", []string{"abc"}},
		"priority above int32":  {"GET", "", []string{"2147483648"}},
		"HEAD invalid decode":   {"HEAD", "?decode=2", nil},
		"HEAD invalid priority": {"HEAD", "", []string{"abc"}},
	} {
		if got := do(tc.method, tc.query, tc.priorities...); got != 400 {
			t.Errorf("%s: HTTP %d, want 400", name, got)
		}
	}
	if calls := a.streamCalls.Load(); calls != 0 {
		t.Fatalf("invalid stream request allocated a tuner: %d calls", calls)
	}
}

func TestHealthErrorCategoryIsSanitized(t *testing.T) {
	a := newFake(t, 10, "x")
	g, s := gateway(t, a)
	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g.Sync(context.Background())

	resp, err := testClient.Get(s.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 {
		t.Fatalf("health HTTP %d, want 503", resp.StatusCode)
	}
	var h struct {
		Backends []struct {
			Error any `json:"error"`
		} `json:"backends"`
	}
	if err := json.Unmarshal(raw, &h); err != nil {
		t.Fatal(err)
	}
	if len(h.Backends) != 1 || h.Backends[0].Error != "catalog sync failed" {
		t.Fatalf("health error is not the stable category: %s", raw)
	}
	host := strings.TrimPrefix(a.server.URL, "http://")
	for _, leak := range []string{"http://", "https://", host} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("health leaked %q: %s", leak, raw)
		}
	}
}

func TestErrorEnvelope(t *testing.T) {
	a := newFake(t, 10, "x")
	a.streamStatus = 403
	_, s := gateway(t, a)
	check := func(resp *http.Response, want int) {
		t.Helper()
		defer resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("HTTP %d, want %d", resp.StatusCode, want)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("Content-Type %q", ct)
		}
		var e struct {
			Code   int    `json:"code"`
			Reason string `json:"reason"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			t.Fatal(err)
		}
		if e.Code != want || e.Reason == "" {
			t.Fatalf("error envelope: %+v", e)
		}
	}
	resp, err := testClient.Get(s.URL + "/api/services/abc")
	if err != nil {
		t.Fatal(err)
	}
	check(resp, 400)
	resp, err = testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	check(resp, 502)
}

// A catalog transport failure (connection refused, not an HTTP 5xx) must keep
// the last validated snapshot while marking the backend unhealthy.
func TestCatalogTransportFailureKeepsSnapshot(t *testing.T) {
	a := newFake(t, 10, "good")
	g, s := gateway(t, a)
	a.server.Close()

	g.Sync(context.Background())

	g.mu.RLock()
	b := g.backends[0]
	healthy, syncErr := b.healthy, b.err
	g.mu.RUnlock()
	if healthy {
		t.Fatalf("transport failure left the backend healthy (err=%q)", syncErr)
	}
	if syncErr == "" {
		t.Fatal("transport failure did not record an error")
	}

	var services, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 1 || services[0]["name"] != "good" || len(programs) != 1 || programs[0]["name"] != "good program" {
		t.Fatalf("previous snapshot lost: %v / %v", services, programs)
	}

	var h struct {
		Ready    bool `json:"ready"`
		Backends []struct {
			Healthy          bool `json:"healthy"`
			CatalogAvailable bool `json:"catalog_available"`
			LastSuccess      any  `json:"last_success"`
			Error            any  `json:"error"`
		} `json:"backends"`
	}
	getJSON(t, s.URL+"/healthz", 503, &h)
	if len(h.Backends) != 1 {
		t.Fatalf("unexpected health payload: %+v", h)
	}
	be := h.Backends[0]
	if h.Ready || be.Healthy || !be.CatalogAvailable || be.LastSuccess == nil || be.Error != "catalog sync failed" {
		t.Fatalf("health did not reflect retained snapshot: %+v", be)
	}
}

// A body that ends before its Content-Length must not commit a partial catalog.
func TestCatalogBodyReadErrorKeepsSnapshot(t *testing.T) {
	var truncate atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		svc, prog := fixture(10, 101, "good")
		switch {
		case truncate.Load() && r.URL.Path == "/api/services":
			conn, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4096\r\n\r\n[")
			_ = rw.Flush()
			_ = conn.Close()
		case r.URL.Path == "/api/services":
			respond(w, 200, []object{svc})
		case r.URL.Path == "/api/programs":
			respond(w, 200, []object{prog})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	g, err := NewGateway(Config{Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1, Backends: []BackendConfig{{ID: "region0", URL: srv.URL + "/api", Priority: 100}}})
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	truncate.Store(true)
	g.Sync(context.Background())

	g.mu.RLock()
	b := g.backends[0]
	healthy, syncErr := b.healthy, b.err
	g.mu.RUnlock()
	if healthy {
		t.Fatalf("truncated body was accepted as a catalog (err=%q)", syncErr)
	}
	if !strings.Contains(syncErr, "reading body") {
		t.Fatalf("truncated body failed for a different reason: err=%q", syncErr)
	}

	s := httptest.NewServer(g)
	t.Cleanup(s.Close)
	var services, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 1 || services[0]["name"] != "good" || len(programs) != 1 || programs[0]["name"] != "good program" {
		t.Fatalf("previous snapshot lost: %v / %v", services, programs)
	}
}

// A 503 is a tuner shortage for one service: fall back for that service only,
// and keep using the same backend for its other services.
func TestServiceScoped503ThroughRealStreamPath(t *testing.T) {
	a := newFake(t, 10, "A")
	svcY, progY := fixture(10, 202, "Y")
	a.services = append(a.services, svcY)
	a.programs = append(a.programs, progY)
	a.streamStatusFor = map[int64]int{1000101: 503}
	b := newFake(t, 10, "B")
	g, s := gateway(t, a, b)

	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	body := make([]byte, len("transport-stream"))
	if _, err := io.ReadFull(resp.Body, body); err != nil {
		t.Fatalf("fallback stream: %v", err)
	}
	waitStarted(t, b)
	resp.Body.Close()
	waitCancel(t, b)

	g.mu.RLock()
	backendWide := !g.backends[0].blockedUntil.IsZero()
	serviceBlocked := !g.backends[0].blockedServices[1000101].IsZero()
	g.mu.RUnlock()
	if backendWide || !serviceBlocked {
		t.Fatalf("503 cooldown scope wrong: backend-wide=%v service=%v", backendWide, serviceBlocked)
	}

	respY, err := testClient.Get(streamURL(s.URL, 1000202))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(respY.Body, body); err != nil {
		t.Fatalf("service Y stream: %v", err)
	}
	waitStarted(t, a)
	respY.Body.Close()
	waitCancel(t, a)
}

func TestCooldownExpiresAfterTenSeconds(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "x"))
	g.block(0, 42, false, "test")

	now := time.Now()
	g.mu.RLock()
	blockedNow := g.usable(0, streamKey{id: 42}, now)
	usableLater := g.usable(0, streamKey{id: 42}, now.Add(11*time.Second))
	g.mu.RUnlock()
	if blockedNow {
		t.Fatal("blocked backend reported usable immediately")
	}
	if !usableLater {
		t.Fatal("cooldown did not expire after 11s")
	}
}

// A clean upstream EOF is how a finished recording looks; it must not penalize
// the backend even though the downstream live stream is truncated.
func TestCleanUpstreamEOFDoesNotCoolBackend(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	a.end = true
	g, s := gateway(t, a, b)

	resp := openStream(t, s.URL)
	defer resp.Body.Close()
	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Fatal("clean upstream EOF looked like a complete live stream")
	}
	if b.streamCalls.Load() != 0 {
		t.Fatal("switched backends after body bytes were sent")
	}
	waitStreamsDrained(t, g)
	g.mu.RLock()
	backendWide := !g.backends[0].blockedUntil.IsZero()
	serviceWide := !g.backends[0].blockedServices[1000101].IsZero()
	g.mu.RUnlock()
	if backendWide || serviceWide {
		t.Fatalf("clean upstream EOF cooled the backend: backend-wide=%v service=%v", backendWide, serviceWide)
	}
}

// stream_timeout_seconds bounds the first byte, not the stream lifetime.
func TestLongLivedStreamOutlivesIdleTimeout(t *testing.T) {
	a := newFake(t, 10, "long")
	a.continuous = true
	_, s := gateway(t, a)

	resp, err := testClient.Get(streamURL(s.URL, 1000101))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	waitStarted(t, a)

	start := time.Now()
	chunks := 0
	for time.Since(start) < 2200*time.Millisecond {
		if _, err := io.ReadFull(resp.Body, make([]byte, 188)); err != nil {
			t.Fatalf("stream aborted after %v and %d chunks: %v", time.Since(start), chunks, err)
		}
		chunks++
	}
	if chunks < 5 {
		t.Fatalf("received only %d chunks over %v", chunks, time.Since(start))
	}
	resp.Body.Close()
	waitCancel(t, a)
}

// Sync copies backends out of the slice under RLock while streams mutate
// cooldown state under the write lock; the race detector is the assertion.
func TestConcurrentSyncAndCooldownRace(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "x"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 75; i++ {
			g.Sync(ctx)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 75; i++ {
			g.block(0, 1000101, i%2 == 0, "test")
			g.mu.RLock()
			_ = g.usable(0, streamKey{id: 1000101}, time.Now())
			g.mu.RUnlock()
		}
	}()
	wg.Wait()
}

func TestEmptyPriorityHeaderRejected(t *testing.T) {
	a := newFake(t, 10, "x")
	_, s := gateway(t, a)

	check := func(set func(http.Header)) {
		t.Helper()
		req, _ := http.NewRequest("GET", streamURL(s.URL, 1000101), nil)
		set(req.Header)
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("HTTP %d, want 400", resp.StatusCode)
		}
	}
	check(func(h http.Header) { h.Set("X-Mirakurun-Priority", "") })
	check(func(h http.Header) {
		h.Add("X-Mirakurun-Priority", "")
		h.Add("X-Mirakurun-Priority", "5")
	})
	if calls := a.streamCalls.Load(); calls != 0 {
		t.Fatalf("invalid priority allocated a tuner: %d calls", calls)
	}
}

func TestSyncCancellationIsNotFailure(t *testing.T) {
	a := newFake(t, 10, "good")
	g, s := gateway(t, a)

	g.mu.RLock()
	before := g.backends[0].updated
	g.mu.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g.Sync(ctx)

	g.mu.RLock()
	after := g.backends[0].updated
	healthy, syncErr := g.backends[0].healthy, g.backends[0].err
	g.mu.RUnlock()
	if !healthy || syncErr != "" {
		t.Fatalf("cancellation marked the backend unhealthy: healthy=%v err=%q", healthy, syncErr)
	}
	if !after.Equal(before) {
		t.Fatalf("cancellation changed updated: %v -> %v", before, after)
	}
	var services []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 1 || services[0]["name"] != "good" {
		t.Fatalf("previous snapshot lost: %v", services)
	}
}

func TestLogoFailureDoesNotReport404(t *testing.T) {
	// Redundant sources for one service: any failure that is not a 404 means 503.
	check := func(name string, a, b *fakeBackend, down bool, want int) {
		t.Helper()
		_, s := gateway(t, a, b)
		if down {
			a.mu.Lock()
			a.down = true
			a.mu.Unlock()
		}
		resp, err := testClient.Get(s.URL + "/api/services/1000101/logo")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("%s: HTTP %d, want %d", name, resp.StatusCode, want)
		}
		if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
			t.Fatalf("%s: Cache-Control %q", name, cc)
		}
	}
	check("failure then 404", newFake(t, 10, "A"), newFake(t, 10, "B"), true, 503)
	check("all 404", newFake(t, 10, "C"), newFake(t, 10, "D"), false, 404)
}

func TestExpiredCooldownReportedAsNull(t *testing.T) {
	g, s := gateway(t, newFake(t, 10, "x"))

	g.mu.Lock()
	g.backends[0].blockedUntil = time.Now().Add(-time.Second)
	g.mu.Unlock()

	var h struct {
		Backends []map[string]any `json:"backends"`
	}
	getJSON(t, s.URL+"/healthz", 200, &h)
	if v, ok := h.Backends[0]["stream_retry_after"]; !ok || v != nil {
		t.Fatalf("expired health cooldown = %v", v)
	}
	var st struct {
		Backends []map[string]any `json:"backends"`
	}
	getJSON(t, s.URL+"/-/ui/state", 200, &st)
	if v, ok := st.Backends[0]["retry_after"]; !ok || v != nil {
		t.Fatalf("expired UI retry_after = %v", v)
	}

	g.mu.Lock()
	g.backends[0].blockedUntil = time.Now().Add(time.Minute)
	g.mu.Unlock()

	var futureH struct {
		Backends []map[string]any `json:"backends"`
	}
	getJSON(t, s.URL+"/healthz", 503, &futureH)
	if futureH.Backends[0]["stream_retry_after"] == nil {
		t.Fatal("future health cooldown not reported")
	}
	var futureST struct {
		Backends []map[string]any `json:"backends"`
	}
	getJSON(t, s.URL+"/-/ui/state", 200, &futureST)
	if futureST.Backends[0]["retry_after"] == nil {
		t.Fatal("future UI cooldown not reported")
	}
}

func TestEventsRejectsDuplicateResource(t *testing.T) {
	_, s := gateway(t, newFake(t, 10, "x"))
	for _, path := range []string{
		"/api/events/stream?resource=tuner&resource=service",
		"/api/events/stream?resource=service",
		"/api/events/stream?resource=tuner&x=1",
	} {
		getJSON(t, s.URL+path, 400, nil)
	}
}

func TestUILogsRejectsBadAfter(t *testing.T) {
	_, s := gateway(t, newFake(t, 10, "x"))
	getJSON(t, s.URL+"/-/ui/logs?after=abc", 400, nil)
	getJSON(t, s.URL+"/-/ui/logs?after=", 200, nil)
	getJSON(t, s.URL+"/-/ui/logs?after=0", 200, nil)
}
