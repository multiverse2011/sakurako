package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestUIStateUsesActualFallbackSource(t *testing.T) {
	a, b := newFake(t, 10, "primary"), newFake(t, 10, "backup")
	a.streamStatus = 503
	g, server := gateway(t, a, b)
	response := openStream(t, server.URL)
	defer response.Body.Close()
	snapshot := g.uiState(time.Now())
	groups := snapshot["streams"].([]object)
	if len(groups) != 1 {
		t.Fatalf("groups: %v", groups)
	}
	group := groups[0]
	if group["backend_id"] != "region1" || group["state"] != "streaming" {
		t.Fatalf("not actual fallback: %v", group)
	}
	clients := group["clients"].([]object)
	if len(clients) != 1 || clients[0]["id"] == "" || clients[0]["bytes"].(int64) == 0 {
		t.Fatalf("missing client counters: %v", clients)
	}
	if group["upstream_bytes"].(int64) == 0 {
		t.Fatal("missing upstream counter")
	}
	for _, backend := range snapshot["backends"].([]object) {
		if backend["ui_url"] == "" {
			t.Fatal("missing Mirakurun UI link")
		}
		if _, ok := backend["url"]; ok {
			t.Fatal("raw API URL exposed")
		}
	}
	var wire map[string]any
	getJSON(t, server.URL+"/-/ui/state", 200, &wire)
	if len(wire["streams"].([]any)) != 1 {
		t.Fatal("state route missing shared group")
	}
}

func TestUIStateStartingAndSharedClients(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	now := time.Now()
	first, second := newSubscriber(), newSubscriber()
	first.id, first.joined = "c1", now
	second.id, second.joined = "c2", now
	first.bytes.Store(20)
	second.bytes.Store(10)
	key := streamKey{id: 1000101}
	shared := newSharedStream(g, "s1", key, "", nil)
	shared.subscribers[first] = true
	shared.subscribers[second] = true
	g.streamsMu.Lock()
	g.streams = map[streamKey]*sharedStream{key: shared}
	g.streamsMu.Unlock()
	snapshot := g.uiState(now)
	group := snapshot["streams"].([]object)[0]
	if group["state"] != "starting" || group["backend_id"] != "" || group["started"] != nil {
		t.Fatalf("starting stream pretends to have a source: %v", group)
	}
	if len(group["clients"].([]object)) != 2 {
		t.Fatal("shared clients lost")
	}
}

func TestUIRoutesAndMethodPolicy(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	for _, path := range []string{"/ui/", "/ui/style.css", "/ui/app.js", "/ui/favicon.svg", "/-/ui/state", "/-/ui/config", "/-/ui/logs"} {
		recorder := httptest.NewRecorder()
		g.ServeHTTP(recorder, httptest.NewRequest("GET", path, nil))
		if recorder.Code != 200 {
			t.Fatalf("%s: %d", path, recorder.Code)
		}
		if strings.HasPrefix(path, "/-/ui/") && recorder.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s can be cached", path)
		}
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{{"POST", "/-/ui/state", 405}, {"GET", "/ui/missing", 404}, {"GET", "/", 307}, {"GET", "/ui", 307}} {
		r := httptest.NewRecorder()
		g.ServeHTTP(r, httptest.NewRequest(tc.method, tc.path, nil))
		if r.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, r.Code)
		}
	}
	r := httptest.NewRecorder()
	g.ServeHTTP(r, httptest.NewRequest(http.MethodHead, "/ui/", nil))
	if r.Body.Len() != 0 {
		t.Fatal("HEAD served body")
	}
}

func TestUIServiceTypeSurvivesVirtualChannel(t *testing.T) {
	g, server := gateway(t, newFake(t, 10, "regional service"))
	snapshot := g.uiState(time.Now())
	services := snapshot["services"].([]object)
	if len(services) != 1 || services[0]["type"] != "GR" {
		t.Fatalf("virtual channel lost broadcast type: %v", services)
	}
	var wire struct {
		Services []struct {
			Type string `json:"type"`
		} `json:"services"`
	}
	getJSON(t, server.URL+"/-/ui/state", 200, &wire)
	if len(wire.Services) != 1 || wire.Services[0].Type != "GR" {
		t.Fatalf("wire broadcast type missing: %+v", wire)
	}
}

func TestUIServicesFilterControls(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "regional service"))
	for _, path := range []string{"/ui/", "/ui/app.js"} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		body := w.Body.String()
		for _, needle := range []string{"service-type", "service-backend", "service-sort", "service-count"} {
			if !strings.Contains(body, needle) {
				t.Fatalf("%s: missing %s", path, needle)
			}
		}
	}
}

func TestMirakurunUIURL(t *testing.T) {
	for input, want := range map[string]string{
		"http://host:40772/api":   "http://host:40772/",
		"https://host/tokyo/api/": "https://host/tokyo/",
		"http://host/":            "http://host/",
		"javascript:alert(1)":     "",
	} {
		u, err := url.Parse(input)
		if err != nil {
			t.Fatal(err)
		}
		if got := mirakurunUIURL(u); got != want {
			t.Errorf("%s: got %s, want %s", input, got, want)
		}
	}
}

func TestEmbeddedAPIDocs(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	for path, contentType := range map[string]string{
		"/openapi.yml":                    "application/yaml; charset=utf-8",
		"/openapi.json":                   "application/json; charset=utf-8",
		"/ui/docs.html":                   "text/html; charset=utf-8",
		"/ui/docs.js":                     "text/javascript; charset=utf-8",
		"/ui/favicon.svg":                 "image/svg+xml",
		"/ui/vendor/scalar-standalone.js": "text/javascript; charset=utf-8",
	} {
		for _, method := range []string{"GET", "HEAD"} {
			r := httptest.NewRecorder()
			g.ServeHTTP(r, httptest.NewRequest(method, path, nil))
			if r.Code != 200 || r.Header().Get("Content-Type") != contentType {
				t.Fatalf("%s %s: %d %s", method, path, r.Code, r.Header().Get("Content-Type"))
			}
			if method == "GET" && r.Body.Len() == 0 {
				t.Fatalf("empty %s", path)
			}
			if method == "HEAD" && r.Body.Len() != 0 {
				t.Fatalf("HEAD body for %s", path)
			}
		}
	}
}

// The Backends table must say counts, not traffic: "Downstream Traffic" is the bandwidth metric.
func TestBackendsColumnIsNotTraffic(t *testing.T) {
	html, err := uiFiles.ReadFile("ui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(html), ">Upstream / Downstream</th>") {
		t.Fatal("ambiguous Upstream / Downstream header")
	}
	if !strings.Contains(string(html), ">Streams / Clients</th>") {
		t.Fatal("missing Streams / Clients header")
	}
}

func TestOpenAPIConditionalRequests(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	for _, path := range []string{"/openapi.yml", "/openapi.json"} {
		w := httptest.NewRecorder()
		g.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		etag := w.Header().Get("ETag")
		if etag == "" {
			t.Fatalf("%s: missing ETag", path)
		}
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("If-None-Match", etag)
		w = httptest.NewRecorder()
		g.ServeHTTP(w, r)
		if w.Code != http.StatusNotModified || w.Body.Len() != 0 || w.Header().Get("ETag") != etag {
			t.Fatalf("%s: %d %q %d", path, w.Code, w.Header().Get("ETag"), w.Body.Len())
		}
	}
}
