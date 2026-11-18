package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type healthResponse struct {
	Ready            bool     `json:"ready"`
	ServiceConflicts []int64  `json:"conflicting_service_ids"`
	ChannelConflicts []string `json:"conflicting_channels"`
}

// sharedDomainGateway builds a gateway with one service_domain and descending
// priorities (IDs a, b, c) so channel-name collisions are exercised.
func sharedDomainGateway(t *testing.T, names string, fs ...*fakeBackend) (*Gateway, *httptest.Server) {
	t.Helper()
	c := Config{ChannelNames: names, Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1}
	ids := []string{"a", "b", "c"}
	if len(fs) > len(ids) {
		t.Fatalf("too many backends")
	}
	for i, f := range fs {
		c.Backends = append(c.Backends, BackendConfig{ID: ids[i], URL: f.server.URL + "/api", Priority: 100 - 10*i, ServiceDomain: "same"})
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

// setNetworkID rewrites the fake's only service and program to report networkID.
func setNetworkID(f *fakeBackend, networkID int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	svc, prog := clone(f.services[0]), clone(f.programs[0])
	id := networkID*100000 + 101
	svc["networkId"], svc["id"] = networkID, id
	prog["networkId"], prog["id"] = networkID, id*100000+3
	f.services, f.programs = []object{svc}, []object{prog}
}

func configGateway(t *testing.T, c Config, fs ...*fakeBackend) (*Gateway, *httptest.Server) {
	t.Helper()
	c.Listen = ":40772"
	c.SyncSeconds = 1
	c.CatalogTimeoutSeconds = 1
	c.MaxStaleSeconds = 60
	c.StreamTimeoutSeconds = 1
	for i, f := range fs {
		c.Backends = append(c.Backends, BackendConfig{ID: fmt.Sprintf("region%d", i), URL: f.server.URL + "/api", Priority: 100 - i})
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

func channelByName(t *testing.T, s *httptest.Server, networkID int64) string {
	t.Helper()
	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	for _, c := range channels {
		for _, svc := range c["services"].([]any) {
			if number(object(svc.(map[string]any)), "networkId") == networkID {
				return c["channel"].(string)
			}
		}
	}
	t.Fatalf("no channel for networkId %d", networkID)
	return ""
}

func readStreamBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	buf := make([]byte, len("transport-stream"))
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatal(err)
	}
	return string(buf)
}

func TestChannelStreamUsesBackendPhysicalChannel(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	_, s := configGateway(t, Config{}, a, b)
	name := channelByName(t, s, 10)

	req, _ := http.NewRequest("HEAD", s.URL+"/api/channels/GR/"+name+"/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "video/MP2T" {
		t.Fatalf("HEAD channel: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if a.streamCalls.Load() != 0 || b.streamCalls.Load() != 0 {
		t.Fatal("HEAD allocated an upstream channel")
	}

	resp, err = testClient.Get(s.URL + "/api/channels/GR/" + name + "/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	h := <-a.started
	if h.Get("Test-Path") != "/api/channels/GR/27/stream" || h.Get("Test-Query") != "decode=0" {
		t.Fatalf("wrong upstream request: %s?%s", h.Get("Test-Path"), h.Get("Test-Query"))
	}
	if b.streamCalls.Load() != 0 {
		t.Fatal("used the wrong backend")
	}
	resp.Body.Close()

	getJSON(t, s.URL+"/api/channels/GR/nope/stream", 404, nil)
	getJSON(t, s.URL+"/api/channels/GR/"+name+"/stream?decode=2", 400, nil)
}

func tunerFixture(index int, name string) object {
	return object{"index": index, "name": name, "types": []string{"GR"}, "pid": nil, "isAvailable": true, "isFault": false,
		"users": []object{{"id": "u1", "priority": 2, "agent": "client", "streamSetting": object{"channel": object{"type": "GR", "channel": "27"}}}}}
}

func TestTunersAggregationAndChannelRewrite(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	a.tuners = []object{tunerFixture(4, "tuner")}
	b.tuners = []object{tunerFixture(4, "tuner")}
	_, s := configGateway(t, Config{}, a, b)

	var tuners []object
	getJSON(t, s.URL+"/api/tuners", 200, &tuners)
	if len(tuners) != 2 {
		t.Fatalf("got %d tuners", len(tuners))
	}
	for i, want := range []string{"region0/tuner", "region1/tuner"} {
		if tuners[i]["name"] != want {
			t.Fatalf("tuner %d name %v", i, tuners[i]["name"])
		}
	}
	if number(tuners[0], "index") != 4 || number(tuners[1], "index") != 10004 {
		t.Fatalf("indexes collide: %v %v", tuners[0]["index"], tuners[1]["index"])
	}
	for i, want := range []string{"region0-3237", "region1-3237"} {
		user := tuners[i]["users"].([]any)[0].(map[string]any)
		got := user["streamSetting"].(map[string]any)["channel"].(map[string]any)["channel"]
		if got != want {
			t.Fatalf("tuner %d channel %v, want %s", i, got, want)
		}
	}

	b.mu.Lock()
	b.down = true
	b.mu.Unlock()
	getJSON(t, s.URL+"/api/tuners", 200, &tuners)
	if len(tuners) != 1 || tuners[0]["name"] != "region0/tuner" {
		t.Fatalf("down backend leaked into tuners: %v", tuners)
	}
}

func TestTunersPartialBackendFailure(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	a.tuners = []object{tunerFixture(4, "tuner")}
	b.tuners = []object{tunerFixture(4, "tuner")}
	_, s := configGateway(t, Config{}, a, b)

	expectOne := func(name string, index int64) {
		t.Helper()
		var tuners []object
		getJSON(t, s.URL+"/api/tuners", 200, &tuners)
		if len(tuners) != 1 || tuners[0]["name"] != name || number(tuners[0], "index") != index {
			t.Fatalf("tuners %v", tuners)
		}
	}

	// A failing first backend must not shift the later backend's index band or name prefix.
	a.mu.Lock()
	a.failTuners = 500
	a.mu.Unlock()
	expectOne("region1/tuner", 10004)

	// An HTTP error on the second backend leaves the first backend's tuners visible.
	a.mu.Lock()
	a.failTuners = 0
	a.mu.Unlock()
	b.mu.Lock()
	b.failTuners = 500
	b.mu.Unlock()
	expectOne("region0/tuner", 4)

	// An unreachable backend is skipped the same way.
	b.server.Close()
	expectOne("region0/tuner", 4)

	// 503 only when every backend fails.
	a.mu.Lock()
	a.failTuners = 500
	a.mu.Unlock()
	var e object
	getJSON(t, s.URL+"/api/tuners", 503, &e)
	if number(e, "code") != 503 || e["reason"] != "tuner state unavailable from all backends" {
		t.Fatalf("error body %v", e)
	}
}

func TestTunersSlowBackend(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	a.tuners = []object{tunerFixture(4, "tuner")}
	b.tuners = []object{tunerFixture(4, "tuner")}
	a.mu.Lock()
	a.stallTuners = true
	a.mu.Unlock()
	_, s := configGateway(t, Config{}, a, b)

	// The stalled backend waits for CatalogTimeoutSeconds; the healthy one must
	// still answer 200 instead of the timeout turning into a 503.
	var tuners []object
	getJSON(t, s.URL+"/api/tuners", 200, &tuners)
	if len(tuners) != 1 || tuners[0]["name"] != "region1/tuner" || number(tuners[0], "index") != 10004 {
		t.Fatalf("tuners %v", tuners)
	}
}

func TestTunersConcurrentSync(t *testing.T) {
	a := newFake(t, 10, "A")
	a.tuners = []object{tunerFixture(4, "tuner")}
	g, s := configGateway(t, Config{}, a)

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				g.Sync(context.Background())
			}
		}
	}()
	for range 20 {
		getJSON(t, s.URL+"/api/tuners", 200, nil)
	}
	close(done)
	wg.Wait()
}

func TestUpstreamChannelNames(t *testing.T) {
	a := newFake(t, 10, "A")
	a.tuners = []object{tunerFixture(4, "tuner")}
	_, s := configGateway(t, Config{ChannelNames: "upstream"}, a)

	name := channelByName(t, s, 10)
	if name != "27" {
		t.Fatalf("channel name %q", name)
	}
	var services []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if services[0]["channel"].(map[string]any)["channel"] != "27" {
		t.Fatalf("service channel %v", services[0]["channel"])
	}
	var tuners []object
	getJSON(t, s.URL+"/api/tuners", 200, &tuners)
	if got := tuners[0]["users"].([]any)[0].(map[string]any)["streamSetting"].(map[string]any)["channel"].(map[string]any)["channel"]; got != "27" {
		t.Fatalf("tuner channel %v", got)
	}

	resp, err := testClient.Get(s.URL + "/api/channels/GR/27/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	resp.Body.Close()

	if _, err := NewGateway(Config{Listen: ":1", SyncSeconds: 1, MaxStaleSeconds: 1, CatalogTimeoutSeconds: 1, StreamTimeoutSeconds: 1, ChannelNames: "bogus", Backends: []BackendConfig{{ID: "a", URL: "http://127.0.0.1:1/api"}}}); err == nil {
		t.Fatal("invalid channel_names accepted")
	}
}

func TestSharedChannelNameUsesPriorityNotHealth(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	g, s := configGateway(t, Config{ChannelNames: "upstream"}, a, b)

	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	if len(channels) != 1 || channels[0]["channel"] != "27" {
		t.Fatalf("shared name was not collapsed: %v", channels)
	}
	services := channels[0]["services"].([]any)
	if len(services) != 1 || number(object(services[0].(map[string]any)), "networkId") != 10 {
		t.Fatalf("channel lists the wrong broadcast: %v", services)
	}
	var all []object
	getJSON(t, s.URL+"/api/services", 200, &all)
	if len(all) != 2 {
		t.Fatalf("service streams must stay addressable: %d", len(all))
	}

	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g.Sync(context.Background())
	req, _ := http.NewRequest("HEAD", s.URL+"/api/channels/GR/27/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("fell over to another broadcast: %d", resp.StatusCode)
	}
}

func TestSameDomainChannelNameFailsOver(t *testing.T) {
	// Same networkId: the two backends really carry the same broadcast.
	a, b := newFake(t, 10, "A"), newFake(t, 10, "B")
	c := Config{ChannelNames: "upstream", Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1,
		Backends: []BackendConfig{
			{ID: "a", URL: a.server.URL + "/api", Priority: 100, ServiceDomain: "same"},
			{ID: "b", URL: b.server.URL + "/api", Priority: 50, ServiceDomain: "same"},
		}}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)

	var h struct {
		Ready     bool     `json:"ready"`
		Conflicts []string `json:"conflicting_channels"`
	}
	getJSON(t, s.URL+"/healthz", 200, &h)
	if !h.Ready || len(h.Conflicts) != 0 {
		t.Fatalf("same networkId reported as a conflict: %+v", h)
	}

	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g.Sync(context.Background())

	resp, err := testClient.Get(s.URL + "/api/channels/GR/27/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	<-b.started
	resp.Body.Close()
}

func TestSameDomainChannelNameDifferentNetworkIdConflicts(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	c := Config{ChannelNames: "upstream", Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1,
		Backends: []BackendConfig{
			{ID: "a", URL: a.server.URL + "/api", Priority: 100, ServiceDomain: "same"},
			{ID: "b", URL: b.server.URL + "/api", Priority: 50, ServiceDomain: "same"},
		}}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)

	var h struct {
		Ready     bool     `json:"ready"`
		Conflicts []string `json:"conflicting_channels"`
	}
	getJSON(t, s.URL+"/healthz", 503, &h)
	if h.Ready || len(h.Conflicts) != 1 || h.Conflicts[0] != "GR/27" {
		t.Fatalf("health did not report the channel conflict: %+v", h)
	}
	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	if len(channels) != 1 {
		t.Fatalf("got %d channels", len(channels))
	}
	services := channels[0]["services"].([]any)
	if len(services) != 1 || number(object(services[0].(map[string]any)), "networkId") != 10 {
		t.Fatalf("channel lists the wrong broadcast: %v", services)
	}

	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g.Sync(context.Background())

	req, _ := http.NewRequest("HEAD", s.URL+"/api/channels/GR/27/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 || b.streamCalls.Load() != 0 {
		t.Fatalf("fell over to another broadcast: %d", resp.StatusCode)
	}
}

func TestChannelConflictSurvivesExpiredWinner(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	g, s := sharedDomainGateway(t, "upstream", a, b)
	var h healthResponse
	getJSON(t, s.URL+"/healthz", 503, &h)
	if len(h.ChannelConflicts) != 1 || h.ChannelConflicts[0] != "GR/27" {
		t.Fatalf("conflict not reported: %+v", h)
	}

	// Rewind the winner past max_stale as if no sync had happened; the catalog is
	// republished the way Sync does, and the conflict must not be forgotten.
	changeBackend(g, 0, func(b *backend) { b.updated = time.Now().Add(-2 * time.Minute) })
	getJSON(t, s.URL+"/healthz", 503, &h)
	if len(h.ChannelConflicts) != 1 || h.ChannelConflicts[0] != "GR/27" {
		t.Fatalf("expired winner dropped the conflict: %+v", h)
	}

	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	if len(channels) != 0 {
		t.Fatalf("expired winner left the conflicting channel listed: %v", channels)
	}
	req, _ := http.NewRequest("HEAD", s.URL+"/api/channels/GR/27/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("served another broadcast after the winner expired: %d", resp.StatusCode)
	}
	if b.streamCalls.Load() != 0 {
		t.Fatal("allocated the other broadcast's tuner")
	}
}

func TestExpiredWinnerFailsOverToSameBroadcast(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 10, "B")
	g, s := sharedDomainGateway(t, "upstream", a, b)

	changeBackend(g, 0, func(b *backend) { b.updated = time.Now().Add(-2 * time.Minute) })

	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	if len(channels) != 1 || channels[0]["channel"] != "27" {
		t.Fatalf("same-broadcast channel disappeared: %v", channels)
	}

	resp, err := testClient.Get(s.URL + "/api/channels/GR/27/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		t.Fatalf("same-broadcast backup was not used: %d", resp.StatusCode)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	<-b.started
	resp.Body.Close()

	var h healthResponse
	getJSON(t, s.URL+"/healthz", 200, &h)
	if !h.Ready || len(h.ChannelConflicts) != 0 {
		t.Fatalf("same broadcast reported as a conflict: %+v", h)
	}
}

func TestChannelNameDoesNotFailOverAcrossServiceDomains(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	c := Config{ChannelNames: "upstream", Listen: ":40772", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1,
		Backends: []BackendConfig{
			{ID: "a", URL: a.server.URL + "/api", Priority: 100, ServiceDomain: "region-a"},
			{ID: "b", URL: b.server.URL + "/api", Priority: 50, ServiceDomain: "region-b"},
		}}
	g, err := NewGateway(c)
	if err != nil {
		t.Fatal(err)
	}
	g.Sync(context.Background())
	s := httptest.NewServer(g)
	t.Cleanup(s.Close)

	// The priority winner expires; the other domain's same name must not take over.
	changeBackend(g, 0, func(b *backend) { b.updated = time.Now().Add(-2 * time.Minute) })
	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	if len(channels) != 0 {
		t.Fatalf("fell over to another domain's channel: %v", channels)
	}
	req, _ := http.NewRequest("HEAD", s.URL+"/api/channels/GR/27/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == 200 || b.streamCalls.Load() != 0 {
		t.Fatalf("fell over to another service domain: %d", resp.StatusCode)
	}
}

func TestChannelConflictKeepsSameBroadcastBackup(t *testing.T) {
	a, b, c := newFake(t, 10, "A"), newFake(t, 10, "B"), newFake(t, 20, "C")
	g, s := sharedDomainGateway(t, "upstream", a, b, c)

	a.mu.Lock()
	a.down = true
	a.mu.Unlock()
	g.Sync(context.Background())

	resp, err := testClient.Get(s.URL + "/api/channels/GR/27/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		t.Fatalf("same-broadcast backup was dropped: %d", resp.StatusCode)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	<-b.started
	resp.Body.Close()
	if c.streamCalls.Load() != 0 {
		t.Fatal("fell over to another broadcast")
	}
}

func TestChannelConflictKeepsChannelsWhenWinnerUnhealthy(t *testing.T) {
	a, b, c := newFake(t, 10, "A"), newFake(t, 10, "B"), newFake(t, 20, "C")
	g, s := sharedDomainGateway(t, "upstream", a, b, c)

	// The priority winner keeps a fresh catalog but fails its last sync.
	a.mu.Lock()
	a.failServices = 503
	a.mu.Unlock()
	g.Sync(context.Background())

	var h healthResponse
	getJSON(t, s.URL+"/healthz", 503, &h)
	if len(h.ChannelConflicts) != 1 || h.ChannelConflicts[0] != "GR/27" {
		t.Fatalf("conflict lost: %+v", h)
	}
	var channels []object
	getJSON(t, s.URL+"/api/channels", 200, &channels)
	if len(channels) != 1 || channels[0]["channel"] != "27" {
		t.Fatalf("conflicting channel disappeared: %v", channels)
	}
	services := channels[0]["services"].([]any)
	if len(services) != 1 || number(object(services[0].(map[string]any)), "networkId") != 10 {
		t.Fatalf("channel lists the wrong broadcast: %v", services)
	}

	req, _ := http.NewRequest("HEAD", s.URL+"/api/channels/GR/27/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || c.streamCalls.Load() != 0 {
		t.Fatalf("channel unusable while a same-broadcast backup is up: %d", resp.StatusCode)
	}
}

func TestChannelConflictSourcesAreNotDuplicated(t *testing.T) {
	a, b, c := newFake(t, 10, "A"), newFake(t, 20, "B"), newFake(t, 30, "C")
	g, _ := sharedDomainGateway(t, "upstream", a, b, c)
	g.mu.Lock()
	g.backends[1].config.ServiceDomain = "other"
	g.mu.Unlock()
	g.Sync(context.Background())

	// Map iteration order varies per call; a stale alias would duplicate a source.
	for i := 0; i < 20; i++ {
		g.mu.RLock()
		cat := g.buildCatalog(time.Now())
		g.mu.RUnlock()
		if len(cat.channelConflicts) != 1 {
			t.Fatalf("pass %d: conflicts %v", i, cat.channelConflicts)
		}
		sources := cat.channelSources["GR/27"]
		if len(sources) != 1 || sources[0].backend != 0 {
			t.Fatalf("pass %d: conflicting channel sources %v", i, sources)
		}
	}
}

func TestVirtualChannelNamesDoNotReportConflicts(t *testing.T) {
	// Virtual keys are per backend and dedupe collapses a backend's same-name
	// services, so no conflict can arise here at all. This guards "no conflict
	// under virtual names"; it does not prove the upstream-only gate.
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	_, s := sharedDomainGateway(t, "virtual", a, b)
	var h healthResponse
	getJSON(t, s.URL+"/healthz", 200, &h)
	if !h.Ready || len(h.ChannelConflicts) != 0 {
		t.Fatalf("virtual names reported a conflict: %+v", h)
	}
}

func TestChannelConflictResolvesWhenBroadcastMatches(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 20, "B")
	g, s := sharedDomainGateway(t, "upstream", a, b)
	var h healthResponse
	getJSON(t, s.URL+"/healthz", 503, &h)
	if len(h.ChannelConflicts) != 1 {
		t.Fatalf("conflict not reported: %+v", h)
	}

	setNetworkID(b, 10)
	g.Sync(context.Background())
	getJSON(t, s.URL+"/healthz", 200, &h)
	if !h.Ready || len(h.ChannelConflicts) != 0 {
		t.Fatalf("conflict not resolved: %+v", h)
	}

	resp, err := testClient.Get(s.URL + "/api/channels/GR/27/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("bad body %q", body)
	}
	<-a.started
	resp.Body.Close()
}

func TestEventsStreamKeepsConnectionOpen(t *testing.T) {
	a := newFake(t, 10, "A")
	_, s := configGateway(t, Config{}, a)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", s.URL+"/api/events/stream?resource=tuner", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("events: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if head, err := io.ReadAll(io.LimitReader(resp.Body, 2)); err != nil || string(head) != "[\n" {
		t.Fatalf("events body start: %q %v", head, err)
	}

	getJSON(t, s.URL+"/api/events/stream?resource=service", 400, nil)
	head, _ := http.NewRequest("HEAD", s.URL+"/api/events/stream?resource=tuner", nil)
	headResp, err := testClient.Do(head)
	if err != nil {
		t.Fatal(err)
	}
	headResp.Body.Close()
	if headResp.StatusCode != 200 {
		t.Fatalf("HEAD events: %d", headResp.StatusCode)
	}
}

// openEvents subscribes to the gateway's tuner events and skips the opening bracket.
func openEvents(t *testing.T, base string) (*http.Response, *json.Decoder) {
	t.Helper()
	resp, err := testClient.Get(base + "/api/events/stream?resource=tuner")
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		resp.Body.Close()
		t.Fatalf("event stream start: %v %v", tok, err)
	}
	return resp, dec
}

func TestEventsRelaysRewrittenTunerEvents(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 10, "B")
	b.events = make(chan object, 1)
	_, s := gateway(t, a, b)

	resp, dec := openEvents(t, s.URL)
	defer resp.Body.Close()
	b.events <- object{"resource": "tuner", "type": "update", "time": 1, "data": object{
		"index": 0,
		"name":  "tuner0",
		"users": []any{object{"id": "user", "streamSetting": object{"channel": object{"type": "GR", "channel": "27"}}}},
	}}

	var e object
	if err := dec.Decode(&e); err != nil {
		t.Fatal(err)
	}
	if e["resource"] != "tuner" || e["type"] != "update" {
		t.Fatalf("relayed event: %v", e)
	}
	data, _ := e["data"].(map[string]any)
	if number(data, "index") != 10000 || data["name"] != "region1/tuner0" {
		t.Fatalf("tuner identity not rewritten: %v", data)
	}
	users, _ := data["users"].([]any)
	user, _ := users[0].(map[string]any)
	channel, _ := user["streamSetting"].(map[string]any)["channel"].(map[string]any)
	if channel["channel"] != "region1-3237" {
		t.Fatalf("event channel not rewritten: %v", channel)
	}
}

func TestEventsSurvivesBackendSubscriptionFailure(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 10, "B")
	a.failEvents = 503
	b.events = make(chan object, 1)
	_, s := gateway(t, a, b)

	resp, dec := openEvents(t, s.URL)
	defer resp.Body.Close()
	b.events <- object{"resource": "tuner", "type": "create", "time": 2, "data": object{"index": 0, "name": "tuner0"}}
	var e object
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("event from the healthy backend: %v", err)
	}
	// A failing subscription must not stall the stream.
	b.events <- object{"resource": "tuner", "type": "update", "time": 3, "data": object{"index": 1, "name": "tuner1"}}
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("second event: %v", err)
	}
	if data, _ := e["data"].(map[string]any); number(data, "index") != 10001 {
		t.Fatalf("second event index not rewritten: %v", data)
	}
}

func TestEventsResubscribesAfterFailedSubscription(t *testing.T) {
	shortenEventTimings(t)
	a := newFake(t, 10, "A")
	a.events = make(chan object, 1)
	a.failEvents = 503
	_, s := gateway(t, a)

	resp, dec := openEvents(t, s.URL)
	defer resp.Body.Close()
	select {
	case <-a.eventTried:
	case <-time.After(3 * time.Second):
		t.Fatal("event subscription was not attempted")
	}
	a.mu.Lock()
	a.failEvents = 0
	a.mu.Unlock()
	a.events <- object{"resource": "tuner", "type": "update", "time": 1, "data": object{"index": 0, "name": "tuner0"}}
	var e object
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("event after resubscribe: %v", err)
	}
	if data, _ := e["data"].(map[string]any); number(data, "index") != 0 {
		t.Fatalf("resubscribed event: %v", data)
	}
}

func TestEventsKeepaliveOnlyWhileDisconnected(t *testing.T) {
	shortenEventTimings(t)
	a := newFake(t, 10, "A")
	a.events = make(chan object, 1)
	a.failEvents = 503
	_, s := gateway(t, a)

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(s.URL + "/api/events/stream?resource=tuner")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if start, err := br.ReadString('\n'); err != nil || start != "[\n" {
		t.Fatalf("event stream start: %q %v", start, err)
	}
	// The only subscription is down, so a keepalive newline must arrive.
	if b, err := br.ReadByte(); err != nil || b != '\n' {
		t.Fatalf("keepalive while disconnected: %q %v", b, err)
	}
	a.mu.Lock()
	a.failEvents = 0
	a.mu.Unlock()
	a.events <- object{"resource": "tuner", "type": "update", "time": 1, "data": object{"index": 0, "name": "tuner0"}}
	// Newlines before the event are whitespace in the array, so the event decodes.
	dec := json.NewDecoder(br)
	dec.UseNumber()
	var e object
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("event after keepalive: %v", err)
	}
	if data, _ := e["data"].(map[string]any); number(data, "index") != 0 {
		t.Fatalf("event after keepalive: %v", data)
	}
	// Every subscription is live now, so nothing more may be written.
	got := make(chan byte, 1)
	go func() {
		if b, err := br.ReadByte(); err == nil {
			got <- b
		}
	}()
	select {
	case b := <-got:
		t.Fatalf("unexpected byte %q while all subscriptions are live", b)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestEventsResumesAfterUpstreamDrop(t *testing.T) {
	shortenEventTimings(t)
	// Keep the subscription down longer than the keepalive interval.
	tunerEventRetry = 100 * time.Millisecond
	a := newFake(t, 10, "A")
	a.events = make(chan object, 1)
	a.closeEventsOnce.Store(true)
	_, s := gateway(t, a)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(s.URL + "/api/events/stream?resource=tuner")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	readLine := func() string {
		t.Helper()
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read events stream: %v", err)
		}
		return line
	}
	if line := readLine(); line != "[\n" {
		t.Fatalf("event stream start: %q", line)
	}
	next := func() object {
		t.Helper()
		for {
			line := readLine()
			if line == "\n" || line == ",\n" {
				continue // keepalive or element separator
			}
			dec := json.NewDecoder(strings.NewReader(line))
			dec.UseNumber()
			var e object
			if err := dec.Decode(&e); err != nil {
				t.Fatalf("event line %q: %v", line, err)
			}
			return e
		}
	}
	a.events <- object{"resource": "tuner", "type": "update", "time": 1, "data": object{"index": 0, "name": "tuner0"}}
	if e := next(); e["type"] != "update" {
		t.Fatalf("first event: %v", e)
	}
	// The fake closes the upstream after that event, so the keepalive must resume
	// until the subscription is back.
	if line := readLine(); line != ",\n" {
		t.Fatalf("event separator: %q", line)
	}
	if line := readLine(); line != "\n" {
		t.Fatalf("keepalive after upstream drop: %q", line)
	}
	a.events <- object{"resource": "tuner", "type": "create", "time": 2, "data": object{"index": 0, "name": "tuner1"}}
	if e := next(); e["type"] != "create" {
		t.Fatalf("event after resubscribe: %v", e)
	}
}

// shortenEventTimings makes reconnect and keepalive gaps testable in milliseconds.
func shortenEventTimings(t *testing.T) {
	t.Helper()
	keepalive, retry := tunerEventKeepalive, tunerEventRetry
	tunerEventKeepalive, tunerEventRetry = 20*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { tunerEventKeepalive, tunerEventRetry = keepalive, retry })
}

func TestEventsClientDisconnectClosesUpstream(t *testing.T) {
	a := newFake(t, 10, "A")
	a.events = make(chan object, 1)
	_, s := gateway(t, a)

	resp, dec := openEvents(t, s.URL)
	a.events <- object{"resource": "tuner", "type": "update", "time": 1, "data": object{"index": 0, "name": "tuner0"}}
	var e object
	if err := dec.Decode(&e); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	select {
	case <-a.eventCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("client disconnect did not close the backend subscription")
	}
}

func TestLogoProxyAndFailover(t *testing.T) {
	a, b := newFake(t, 10, "A"), newFake(t, 10, "B")
	logo := []byte("png-bytes")
	b.logo = logo
	_, s := gateway(t, a, b)

	resp, err := testClient.Get(s.URL + "/api/services/1000101/logo")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != string(logo) || resp.Header.Get("Content-Type") != "image/png" || resp.Header.Get("ETag") == "" {
		t.Fatalf("logo proxy: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	req, _ := http.NewRequest("HEAD", s.URL+"/api/services/1000101/logo", nil)
	head, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if head.StatusCode != 200 || head.Header.Get("Content-Type") != "image/png" {
		t.Fatalf("logo HEAD: %d %v", head.StatusCode, head.Header)
	}
	head.Body.Close()

	getJSON(t, s.URL+"/api/services/1000199/logo", 404, nil)
	getJSON(t, s.URL+"/api/services/1000101/logo?x=1", 400, nil)

	var services []object
	getJSON(t, s.URL+"/api/services", 200, &services)
	if services[0]["hasLogoData"] != true {
		t.Fatalf("hasLogoData %v", services[0]["hasLogoData"])
	}
}
