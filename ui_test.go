package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strconv"
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
	for _, path := range []string{"/ui/", "/ui/style.css", "/ui/app.js", "/ui/favicon.svg", "/-/ui/state", "/-/ui/tuners", "/-/ui/config", "/-/ui/logs"} {
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
	}{{"POST", "/-/ui/state", 405}, {"POST", "/-/ui/tuners", 405}, {"GET", "/ui/missing", 404}, {"GET", "/", 307}, {"GET", "/ui", 307}} {
		r := httptest.NewRecorder()
		g.ServeHTTP(r, httptest.NewRequest(tc.method, tc.path, nil))
		if r.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, r.Code)
		}
	}
	for _, path := range []string{"/ui/", "/-/ui/tuners"} {
		r := httptest.NewRecorder()
		g.ServeHTTP(r, httptest.NewRequest(http.MethodHead, path, nil))
		if r.Body.Len() != 0 {
			t.Fatalf("HEAD %s served body", path)
		}
	}
}

func TestUITunerCounts(t *testing.T) {
	free := object{"index": 1, "isAvailable": true, "isFault": false, "users": []object{}}
	busy := object{"index": 2, "isAvailable": false, "isFault": false, "users": []object{{"id": "u1"}}}
	faulty := object{"index": 3, "isAvailable": false, "isFault": true, "users": []object{}}
	a, b, c := newFake(t, 10, "primary"), newFake(t, 10, "backup"), newFake(t, 10, "empty")
	a.tuners = []object{free, busy}
	b.tuners = []object{faulty, busy}
	_, s := gateway(t, a, b, c)

	var got struct {
		Available bool `json:"available"`
		InUse     int  `json:"in_use"`
		Total     int  `json:"total"`
		Free      int  `json:"free"`
		Fault     int  `json:"fault"`
		Backends  []struct {
			ID    string `json:"id"`
			InUse int    `json:"in_use"`
			Total int    `json:"total"`
			Free  int    `json:"free"`
			Fault int    `json:"fault"`
		} `json:"backends"`
	}
	getJSON(t, s.URL+"/-/ui/tuners", 200, &got)
	if !got.Available || got.InUse != 2 || got.Total != 4 || got.Free != 1 || got.Fault != 1 {
		t.Fatalf("totals: %+v", got)
	}
	if len(got.Backends) != 3 {
		t.Fatalf("want one row per answering backend: %+v", got.Backends)
	}
	if b := got.Backends[0]; b.ID != "region0" || b.InUse != 1 || b.Total != 2 || b.Free != 1 || b.Fault != 0 {
		t.Fatalf("healthy backend: %+v", b)
	}
	// A faulty tuner is neither in use nor free.
	if b := got.Backends[1]; b.ID != "region1" || b.InUse != 1 || b.Total != 2 || b.Free != 0 || b.Fault != 1 {
		t.Fatalf("faulty backend: %+v", b)
	}
	// An empty list is still a successful answer, not a missing one.
	if b := got.Backends[2]; b.ID != "region2" || b.Total != 0 || b.Free != 0 {
		t.Fatalf("empty backend: %+v", b)
	}

	// One broken backend only drops its own row; the screen keeps the rest.
	b.mu.Lock()
	b.failTuners = 500
	b.mu.Unlock()
	getJSON(t, s.URL+"/-/ui/tuners", 200, &got)
	if !got.Available || got.InUse != 1 || got.Total != 2 || got.Free != 1 || got.Fault != 0 {
		t.Fatalf("partial failure totals: %+v", got)
	}
	if len(got.Backends) != 2 || got.Backends[0].ID != "region0" || got.Backends[1].ID != "region2" {
		t.Fatalf("partial failure rows: %+v", got.Backends)
	}

	a.mu.Lock()
	a.failTuners = 500
	a.mu.Unlock()
	c.mu.Lock()
	c.failTuners = 500
	c.mu.Unlock()
	getJSON(t, s.URL+"/-/ui/tuners", 200, &got)
	if got.Available || len(got.Backends) != 0 || got.InUse != 0 || got.Total != 0 || got.Free != 0 || got.Fault != 0 {
		t.Fatalf("all failed should still answer 200 and say unavailable: %+v", got)
	}
}

func TestUITunerCountsSkipsDisconnectedClient(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := httptest.NewRecorder()
	g.ServeHTTP(recorder, httptest.NewRequest("GET", "/-/ui/tuners", nil).WithContext(ctx))
	if recorder.Body.Len() != 0 {
		t.Fatalf("answered a disconnected client: %q", recorder.Body.String())
	}
}

func TestCountTunersIsUsing(t *testing.T) {
	// Mirakurun may report allocation only through isUsing.
	inUse, total, free, fault := countTuners([]object{{"isUsing": true}, {"isAvailable": true}, {"isFault": true}})
	if inUse != 1 || total != 3 || free != 1 || fault != 1 {
		t.Fatalf("countTuners: %d %d %d %d", inUse, total, free, fault)
	}
}

func TestLogsPanelOffersPauseAndFilters(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	for path, needles := range map[string][]string{
		"/ui/":       {`id="log-pause"`, `id="log-query"`, `id="log-backend"`},
		"/ui/app.js": {`'log-pause'`, `'log-query'`, `'log-backend'`, `logPaused`},
	} {
		r := httptest.NewRecorder()
		g.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
		for _, needle := range needles {
			if !strings.Contains(r.Body.String(), needle) {
				t.Errorf("%s: missing %s", path, needle)
			}
		}
	}
}

// The test assumes every element app.js looks up with $() is present in index.html's static markup.
func TestAppScriptReferencesExistingElements(t *testing.T) {
	markup, err := uiFiles.ReadFile("ui/index.html")
	if err != nil {
		t.Fatal(err)
	}
	script, err := uiFiles.ReadFile("ui/app.js")
	if err != nil {
		t.Fatal(err)
	}
	references := regexp.MustCompile(`\$\(\s*'([^']+)'\s*\)`).FindAllStringSubmatch(string(script), -1)
	if len(references) == 0 {
		t.Fatal("no element lookups found in app.js")
	}
	for _, reference := range references {
		if !strings.Contains(string(markup), `id="`+reference[1]+`"`) {
			t.Errorf("app.js looks up #%s, which index.html does not define", reference[1])
		}
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

func TestUIServicesFilterMarkup(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
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

// The UI has no JS test runner; this pins the refresh wiring and the paused label behind the screenshots.
func TestUIManualRefreshControls(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	for _, tc := range []struct{ path, want string }{
		{"/ui/", `id="updated"`},
		{"/ui/", `id="refresh"`},
		{"/ui/app.js", `$('refresh').addEventListener`},
		{"/ui/app.js", `Auto reload paused`},
		{"/ui/app.js", `pollTuners(true)`},
	} {
		recorder := httptest.NewRecorder()
		g.ServeHTTP(recorder, httptest.NewRequest("GET", tc.path, nil))
		if !strings.Contains(recorder.Body.String(), tc.want) {
			t.Errorf("%s does not contain %s", tc.path, tc.want)
		}
	}
}

// Automatic state polling is gated to Overview/Services, tuner polling and redrawing to the
// visible tab. That wiring is the point of the change but has no JS runner here, so pin it like
// the refresh controls above, ignoring whitespace so reformatting app.js does not break the pins.
func TestUIRendersAndPollsPerVisibleTab(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	recorder := httptest.NewRecorder()
	g.ServeHTTP(recorder, httptest.NewRequest("GET", "/ui/app.js", nil))
	script := strings.Join(strings.Fields(recorder.Body.String()), "")
	for _, want := range []string{
		`document.querySelector('main>section:not(.hidden)')?.id||'overview'`,
		`['overview','services'].includes(currentTab())`,
		`currentTab()==='overview'`,
		`if(currentTab()==='overview')renderOverview();elseif(currentTab()==='services')renderServices()`,
		`if(tabPollsState()){render();poll();}`,
		`&&tabPollsState())timer=setTimeout(poll,`,
		`else{$('connection').textContent='';$('connection').classList.remove('status-error');}`,
		`if(!state)poll(true);elsepoll();`,
		`elsepollTuners();`,
		`||!tabPollsTuners()`,
		`if(state&&tabPollsTuners()){renderTuners();renderBackends();}`,
		`&&tabPollsTuners())tunerTimer=setTimeout(pollTuners,5000)`,
		`setTimeout(()=>controller.abort(),25000)`,
	} {
		if !strings.Contains(script, want) {
			t.Errorf("/ui/app.js does not contain %s", want)
		}
	}
	if strings.Contains(script, `renderOverview();renderServices()`) {
		t.Error("/ui/app.js redraws both tabs on every state poll")
	}
}

func TestUISharedServiceListsEveryCandidate(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"), newFake(t, 10, "service"))
	snapshot := g.uiState(time.Now())
	services := snapshot["services"].([]object)
	if len(services) != 1 {
		t.Fatalf("shared services not merged: %v", services)
	}
	candidates := services[0]["candidates"].([]object)
	if len(candidates) != 2 || candidates[0]["id"] != "region0" || candidates[1]["id"] != "region1" {
		t.Fatalf("candidate list lost a backend: %v", candidates)
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

func uiGet(g *Gateway, method, path, accept, ifNoneMatch string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	if accept != "" {
		r.Header.Set("Accept-Encoding", accept)
	}
	if ifNoneMatch != "" {
		r.Header.Set("If-None-Match", ifNoneMatch)
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}

func TestUIAssetsConditionalRequests(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	for name := range uiAssets {
		path := "/ui/" + name
		etags := map[string]string{}
		for _, accept := range []string{"", "gzip"} {
			w := uiGet(g, "GET", path, accept, "")
			etag := w.Header().Get("ETag")
			if w.Code != 200 || etag == "" {
				t.Fatalf("%s accept %q: %d ETag %q", path, accept, w.Code, etag)
			}
			if w.Header().Get("Vary") != "Accept-Encoding" {
				t.Fatalf("%s accept %q: Vary %q", path, accept, w.Header().Get("Vary"))
			}
			wantEncoding := ""
			if accept == "gzip" {
				wantEncoding = "gzip"
			}
			if got := w.Header().Get("Content-Encoding"); got != wantEncoding {
				t.Fatalf("%s accept %q: Content-Encoding %q", path, accept, got)
			}
			if got, want := w.Header().Get("Content-Length"), strconv.Itoa(w.Body.Len()); got != want {
				t.Fatalf("%s accept %q: Content-Length %q, want %q", path, accept, got, want)
			}
			etags[accept] = etag
			for _, candidate := range []string{etag, "W/" + etag, `"stale", ` + etag} {
				w = uiGet(g, "GET", path, accept, candidate)
				if w.Code != http.StatusNotModified || w.Body.Len() != 0 || w.Header().Get("ETag") != etag {
					t.Fatalf("%s accept %q candidate %q: revalidation %d ETag %q body %d", path, accept, candidate, w.Code, w.Header().Get("ETag"), w.Body.Len())
				}
				if w.Header().Get("Vary") != "Accept-Encoding" {
					t.Fatalf("%s accept %q: 304 Vary %q", path, accept, w.Header().Get("Vary"))
				}
				if got := w.Header().Get("Content-Encoding"); got != wantEncoding {
					t.Fatalf("%s accept %q: 304 Content-Encoding %q", path, accept, got)
				}
			}
		}
		// Whenever two representations are served, their ETags must differ.
		if etags[""] == etags["gzip"] {
			t.Fatalf("%s: encodings share an ETag %q", path, etags[""])
		}
		// A client holding the identity body must not get a 304 for the gzip variant.
		if w := uiGet(g, "GET", path, "gzip", etags[""]); w.Code != 200 {
			t.Fatalf("%s: identity ETag answered gzip request with %d", path, w.Code)
		}
	}
}

func TestUIAssetsGzip(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	plain := uiGet(g, "GET", "/ui/vendor/scalar-standalone.js", "", "")
	compressed := uiGet(g, "GET", "/ui/vendor/scalar-standalone.js", "gzip", "")
	if plain.Header().Get("Content-Encoding") != "" {
		t.Fatalf("identity response compressed: %q", plain.Header().Get("Content-Encoding"))
	}
	if compressed.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("gzip response encoding: %q", compressed.Header().Get("Content-Encoding"))
	}
	zr, err := gzip.NewReader(compressed.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, plain.Body.Bytes()) {
		t.Fatal("gzip body differs from identity body")
	}
	if compressed.Body.Len() >= plain.Body.Len()/2 {
		t.Fatalf("compression unhelpful: %d of %d bytes", compressed.Body.Len(), plain.Body.Len())
	}
}

func TestUIAssetsHead(t *testing.T) {
	g, _ := gateway(t, newFake(t, 10, "service"))
	for _, accept := range []string{"", "gzip"} {
		get := uiGet(g, "GET", "/ui/app.js", accept, "")
		head := uiGet(g, "HEAD", "/ui/app.js", accept, "")
		if head.Body.Len() != 0 {
			t.Fatal("HEAD served body")
		}
		for header, want := range map[string]string{
			"ETag":             get.Header().Get("ETag"),
			"Content-Encoding": get.Header().Get("Content-Encoding"),
			"Content-Type":     get.Header().Get("Content-Type"),
			"Vary":             "Accept-Encoding",
			"Content-Length":   strconv.Itoa(get.Body.Len()),
		} {
			if got := head.Header().Get(header); got != want {
				t.Fatalf("HEAD accept %q %s: %q, want %q", accept, header, got, want)
			}
		}
		if head.Code != 200 {
			t.Fatalf("HEAD: %d", head.Code)
		}
		if w := uiGet(g, "HEAD", "/ui/app.js", accept, get.Header().Get("ETag")); w.Code != http.StatusNotModified || w.Body.Len() != 0 {
			t.Fatalf("HEAD revalidation: %d body %d", w.Code, w.Body.Len())
		}
	}
}

func TestAcceptsGzip(t *testing.T) {
	for header, want := range map[string]bool{
		"":                        false,
		"deflate":                 false,
		"gzip":                    true,
		"GZIP":                    true,
		"x-gzip":                  true,
		"gzip, deflate, br":       true,
		"gzip;q=0":                false,
		"gzip;Q=0":                false,
		"gzip;q=0, identity":      false,
		"gzip;q=0.5":              true,
		"gzip;q=0.001":            true,
		"gzip;q=0, *":             false,
		"br, gzip;q=0.5":          true,
		"identity, *;q=0.5":       true,
		"*":                       true,
		"*;q=0":                   false,
		"deflate;q=0, gzip;q=0.0": false,
	} {
		if got := acceptsGzip(header); got != want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", header, got, want)
		}
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
