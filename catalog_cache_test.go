package main

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// Requests must read the catalog Sync published, not rebuild it from backend state.
func TestRequestsDoNotRebuildCatalog(t *testing.T) {
	a := newFake(t, 10, "A")
	g, s := gateway(t, a)

	g.mu.Lock()
	g.backends[0].services = []object{}
	g.mu.Unlock()
	var services []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 1 {
		t.Fatalf("request rebuilt the catalog: %v", services)
	}
}

// A catalog change becomes visible on the first request after Sync returns.
func TestCatalogChangeVisibleAfterSync(t *testing.T) {
	a := newFake(t, 10, "A")
	g, s := gateway(t, a)
	setName := func(name string) {
		s, p := fixture(10, 101, name)
		a.mu.Lock()
		a.services, a.programs = []object{s}, []object{p}
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

// Requests during a sync keep getting one complete catalog, never a mixture.
func TestCatalogConsistentDuringSync(t *testing.T) {
	a := newFake(t, 10, "A")
	g, s := gateway(t, a)
	var wg sync.WaitGroup
	stop := make(chan struct{})
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
			close(stop)
			wg.Wait()
			t.Fatalf("sync %d not visible: %v", i, services)
		}
	}
	close(stop)
	wg.Wait()
}

// The catalog is the only place freshness is time-dependent: a backend that
// crosses max_stale_seconds between syncs stops being served.
func TestCatalogExpiresBetweenSyncs(t *testing.T) {
	a := newFake(t, 10, "A")
	c := Config{Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 2, StreamTimeoutSeconds: 1,
		Backends: []BackendConfig{{ID: "a", URL: a.server.URL + "/api"}}}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)

	g.mu.Lock()
	g.backends[0].updated = time.Now().Add(-time.Second) // expires in ~1s without a sync
	g.cat = g.buildCatalog()
	g.mu.Unlock()
	var services []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 1 {
		t.Fatalf("fresh catalog dropped early: %v", services)
	}
	time.Sleep(1200 * time.Millisecond)
	getJSON(t, s.URL+"/api/services", 200, &services)
	if len(services) != 0 {
		t.Fatalf("expired catalog still served: %v", services)
	}
}
