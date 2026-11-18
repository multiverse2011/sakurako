package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

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
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("events: %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
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
