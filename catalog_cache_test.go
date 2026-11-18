package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

// Requests read the catalog Sync published, so changing backend state without a
// sync must not change what any catalog endpoint returns.
func TestRequestsDoNotRebuildCatalog(t *testing.T) {
	a := newFake(t, 10, "A")
	g, s := gateway(t, a)

	g.mu.Lock()
	g.backends[0].services = []object{}
	g.mu.Unlock()

	var services, channels []object
	var state struct{ Services []object }
	var one object
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	getJSON(t, s.URL+"/-/ui/state", 200, &state)
	getJSON(t, s.URL+"/api/services/1000101", 200, &one)
	if len(services) != 1 || len(channels) != 1 || len(state.Services) != 1 || one["name"] != "A" {
		t.Fatalf("a request rebuilt the catalog: %v / %v / %v / %v", services, channels, state.Services, one)
	}
}

// A catalog change becomes visible on the first request after Sync returns.
func TestCatalogChangeVisibleAfterSync(t *testing.T) {
	a := newFake(t, 10, "A")
	g, s := gateway(t, a)
	setName := func(name string) {
		svc, prg := fixture(10, 101, name)
		a.mu.Lock()
		a.services, a.programs = []object{svc}, []object{prg}
		a.mu.Unlock()
		g.Sync(context.Background())
	}

	var services, programs []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 1 || services[0]["name"] != "A" {
		t.Fatalf("unexpected initial catalog: %v", services)
	}
	setName("B")
	getJSON(t, s.URL+"/api/services", 200, &services)
	getJSON(t, s.URL+"/api/programs", 200, &programs)
	if len(services) != 1 || services[0]["name"] != "B" || len(programs) != 1 || programs[0]["name"] != "B program" {
		t.Fatalf("stale catalog after sync: %v / %v", services, programs)
	}
}

// Concurrent requests keep getting complete, successful responses while syncs
// replace the catalog. This does not assert that a response never straddles two
// syncs: a rebuild while the cached catalog is expired can include backends the
// in-flight sync already committed, which is the pre-cache behaviour too.
func TestCatalogServesDuringConcurrentSync(t *testing.T) {
	a := newFake(t, 10, "A")
	g, s := gateway(t, a)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	defer func() {
		close(stop)
		wg.Wait()
	}()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, path := range []string{"/api/services", "/api/programs", "/-/ui/state", "/healthz"} {
					resp, err := testClient.Get(s.URL + path)
					if err != nil {
						t.Error(err)
						return
					}
					resp.Body.Close()
					if resp.StatusCode != 200 {
						t.Errorf("%s: HTTP %d", path, resp.StatusCode)
						return
					}
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		name := fmt.Sprintf("generation-%d", i)
		svc, prg := fixture(10, 101, name)
		a.mu.Lock()
		a.services, a.programs = []object{svc}, []object{prg}
		a.mu.Unlock()
		g.Sync(context.Background())

		var services []object
		getJSON(t, s.URL+"/api/services", 200, &services)
		if len(services) != 1 || services[0]["name"] != name {
			t.Fatalf("sync %d not visible: %v", i, services)
		}
	}
}

// The freshness cutoff is the only time-dependent part of the catalog: a backend
// that crosses max_stale_seconds between syncs stops being served. The clock is
// injected so the test never sleeps.
func TestCatalogExpiresBetweenSyncs(t *testing.T) {
	a := newFake(t, 10, "A")
	c := Config{Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 2, StreamTimeoutSeconds: 1,
		Backends: []BackendConfig{{ID: "a", URL: a.server.URL + "/api"}}}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())

	base := time.Now()
	changeBackend(g, 0, func(b *backend) { b.updated = base })
	if got := g.catalogNow(base.Add(time.Second)); len(got.services) != 1 {
		t.Fatalf("fresh catalog dropped early: %v", got.services)
	}
	if got := g.catalogNow(base.Add(3 * time.Second)); len(got.services) != 0 {
		t.Fatalf("expired catalog still served: %v", got.services)
	}
}

// At exactly expires a backend is still fresh (buildCatalog drops it only once it
// is older than max_stale), so that instant must not trigger a rebuild.
func TestCatalogIsNotRebuiltAtExpiryBoundary(t *testing.T) {
	a := newFake(t, 10, "A")
	c := Config{Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 2, StreamTimeoutSeconds: 1,
		Backends: []BackendConfig{{ID: "a", URL: a.server.URL + "/api"}}}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	base := time.Now()
	changeBackend(g, 0, func(b *backend) { b.updated = base })

	// Removing the backend's data without republishing makes a rebuild visible.
	g.mu.Lock()
	g.backends[0].services = []object{}
	g.mu.Unlock()
	if got := g.catalogNow(base.Add(2 * time.Second)); len(got.services) != 1 {
		t.Fatalf("rebuilt at the expiry boundary: %v", got.services)
	}
}

// The same expiry, with a conflicting channel name: without a sync, an expired
// winner must keep the conflict and must not fail over to another broadcast.
// catalogNow publishes the catalog rebuilt at the injected instant, so the
// requests below see exactly what a real expiry would have published.
func TestExpiredWinnerKeepsConflictWithoutSync(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	g, s := sharedDomainGateway(t, "upstream", a, b)

	cat := g.catalogNow(time.Now().Add(2 * time.Hour))
	if len(cat.channelConflicts) != 1 || cat.channelConflicts[0].key != "GR/27" {
		t.Fatalf("conflict dropped after expiry: %v", cat.channelConflicts)
	}
	if sources := cat.channelSources["GR/27"]; len(sources) != 0 {
		t.Fatalf("expired sources still served: %v", sources)
	}
	var h healthResponse
	getJSON(t, s.URL+"/healthz", 503, &h)
	if len(h.ChannelConflicts) != 1 || h.ChannelConflicts[0] != "GR/27" {
		t.Fatalf("health lost the conflict: %+v", h)
	}
	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	if len(channels) != 0 {
		t.Fatalf("expired winner still listed: %v", channels)
	}
	req, _ := http.NewRequest("HEAD", s.URL+"/api/channels/GR/27/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 || b.streamCalls.Load() != 0 {
		t.Fatalf("failed over to another broadcast after expiry: %d", resp.StatusCode)
	}
}
