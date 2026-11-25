package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func mustFixture() object     { s, _ := fixture(10, 101, "ok"); return s }
func jn(v string) json.Number { return json.Number(v) }

// norm round-trips through JSON so nested values have the types fetch() produces.
func norm(t *testing.T, o object) object {
	t.Helper()
	raw, _ := json.Marshal(o)
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var n object
	if err := d.Decode(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestValidateCatalogRejects(t *testing.T) {
	good := norm(t, mustFixture())
	with := func(k string, v any) object {
		o := clone(good)
		o[k] = v
		return o
	}
	badChannel := func(k string, v any) object {
		o := clone(good)
		ch := clone(channelOf(good))
		ch[k] = v
		o["channel"] = map[string]any(ch)
		return o
	}
	for name, tc := range map[string]struct {
		svc  []object
		want string
	}{
		"id mismatch":   {[]object{with("id", jn("5"))}, "does not match"},
		"out of range":  {[]object{with("networkId", jn("70000"))}, "out of range"},
		"duplicate":     {[]object{good, good}, "duplicate service"},
		"empty channel": {[]object{badChannel("channel", "")}, "missing channel"},
		"bad type":      {[]object{badChannel("type", "XX")}, "unsupported channel.type"},
	} {
		if err := validateCatalog(tc.svc, nil); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", name, err, tc.want)
		}
	}
	if err := validateCatalog([]object{good}, nil); err != nil {
		t.Fatalf("good catalog rejected: %v", err)
	}
	p := object{"id": jn("1"), "networkId": jn("1"), "serviceId": jn("1"), "eventId": jn("1")}
	if err := validateCatalog(nil, []object{p}); err == nil {
		t.Error("bad program id accepted")
	}
}

func TestConfigValidateRejects(t *testing.T) {
	ok := Config{Listen: ":1", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1, Backends: []BackendConfig{{ID: "a", URL: "http://h/api"}}}
	mut := func(f func(*Config)) Config { c := ok; f(&c); return c }
	for name, c := range map[string]Config{
		"zero sync":            mut(func(c *Config) { c.SyncSeconds = 0 }),
		"empty listen":         mut(func(c *Config) { c.Listen = "" }),
		"stale < sync+timeout": mut(func(c *Config) { c.MaxStaleSeconds = c.SyncSeconds + c.CatalogTimeoutSeconds - 1 }),
		"bad id":               mut(func(c *Config) { c.Backends = []BackendConfig{{ID: "a b", URL: "http://h"}} }),
		"duplicate id":         mut(func(c *Config) { c.Backends = append(c.Backends, c.Backends[0]) }),
		"creds in url":         mut(func(c *Config) { c.Backends = []BackendConfig{{ID: "a", URL: "http://u:p@h"}} }),
		"unparsable url":       mut(func(c *Config) { c.Backends = []BackendConfig{{ID: "a", URL: "http://%zz"}} }),
		"negative priority":    mut(func(c *Config) { c.Backends = []BackendConfig{{ID: "a", URL: "http://h/api", Priority: -1}} }),
		"bad port":             mut(func(c *Config) { c.Backends = []BackendConfig{{ID: "a", URL: "http://h:99999/api"}} }),
		"trailing ?":           mut(func(c *Config) { c.Backends = []BackendConfig{{ID: "a", URL: "http://h/api?"}} }),
	} {
		if c.validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := ok.validate(); err != nil {
		t.Fatal(err)
	}
	if c := mut(func(c *Config) { c.ChannelNames = "" }); c.validate() != nil {
		t.Error("empty channel_names rejected")
	}
	if _, err := loadConfig("/nonexistent/x.json"); err == nil || !strings.Contains(err.Error(), "/nonexistent/x.json") {
		t.Errorf("config path missing from error: %v", err)
	}
}

func TestReadConfigDefaultsChannelNames(t *testing.T) {
	dir := t.TempDir()
	write := func(name, raw string) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	c, err := readConfig(write("ok.json", `{"backends":[{"id":"a","url":"http://h/api"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.ChannelNames != "virtual" {
		t.Fatalf("empty channel_names normalized to %q", c.ChannelNames)
	}
	if _, err := readConfig(write("bad.json", `{"channel_names":"bogus","backends":[{"id":"a","url":"http://h/api"}]}`)); err == nil {
		t.Fatal("invalid channel_names accepted")
	}
}

func TestServerWriteTimeout(t *testing.T) {
	// max(2m, backends*(2*stream_timeout)+60s)
	for _, tc := range []struct {
		name     string
		backends int
		stream   int
		want     time.Duration
	}{
		{"no backends clamps to floor", 0, 30, 2 * time.Minute},
		{"one backend equals floor", 1, 30, 2 * time.Minute},
		{"two backends", 2, 30, 3 * time.Minute},
		{"five backends", 5, 30, 6 * time.Minute},
	} {
		c := Config{StreamTimeoutSeconds: tc.stream, Backends: make([]BackendConfig, tc.backends)}
		if got := serverWriteTimeout(c); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
	// The largest timeout the validator accepts must not overflow into a negative or tiny value.
	if got := serverWriteTimeout(Config{StreamTimeoutSeconds: int((1<<63 - 1) / int(time.Second))}); got <= 0 {
		t.Fatalf("overflowed timeout %v", got)
	}
}

func TestFetchRejectsBadBodies(t *testing.T) {
	for body, want := range map[string]string{`null`: "null", `{}`: "JSON array", `[] []`: "trailing"} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
		g, err := NewGateway(Config{Listen: ":1", SyncSeconds: 1, CatalogTimeoutSeconds: 1, MaxStaleSeconds: 60, StreamTimeoutSeconds: 1, Backends: []BackendConfig{{ID: "a", URL: srv.URL}}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = g.fetch(context.Background(), g.backends[0], "/services")
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "a/services") {
			t.Errorf("%s: got %v", body, err)
		}
	}
}

func TestHTTPBoundaries(t *testing.T) {
	_, s := gateway(t, newFake(t, 10, "x"))
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"POST", "/api/services", 405},
		{"GET", "/api/services?%zz", 400},
		{"GET", "/api/services/abc/stream", 400},
		{"GET", "/api/services/abc", 400},
		{"GET", "/api/services/999", 404},
		{"GET", "/api/services/999/stream", 404},
		{"GET", "/api/channels/x", 404},
		{"GET", "/api/foo", 404},
		{"GET", "/api/services?x=1", 400},
		{"GET", "/api/programs?networkId=70000", 400},
		{"GET", "/api/programs?networkId=x", 400},
		{"GET", "/api/programs?bogus=1", 400},
	} {
		req, _ := http.NewRequest(tc.method, s.URL+tc.path, nil)
		resp, err := testClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Errorf("%s %s: got %d want %d", tc.method, tc.path, resp.StatusCode, tc.status)
		}
		if tc.status == 405 && resp.Header.Get("Allow") == "" {
			t.Error("Allow header missing")
		}
	}
}

func TestCooldownScopeAndHEAD(t *testing.T) {
	g, s := gateway(t, newFake(t, 10, "x"))
	g.block(0, 1000101, true, "test")
	g.mu.RLock()
	svcBlocked, otherOK := !g.usable(0, streamKey{id: 1000101}, time.Now()), g.usable(0, streamKey{id: 42}, time.Now())
	g.mu.RUnlock()
	if !svcBlocked || !otherOK {
		t.Fatal("service-only cooldown must not affect other services")
	}
	// A cooling source is still an attempt candidate when nothing else can serve,
	// so HEAD and GET agree.
	req, _ := http.NewRequest("HEAD", s.URL+"/api/services/1000101/stream", nil)
	resp, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("HEAD disagreed with the attempt decision: %d", resp.StatusCode)
	}

	changeBackend(g, 0, func(b *backend) { b.healthy = false })
	resp, err = testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("HEAD claimed playable while unhealthy: %d", resp.StatusCode)
	}
}

func TestServiceDomain(t *testing.T) {
	if serviceDomain(BackendConfig{ID: "a", ServiceDomain: "d"}) != serviceDomain(BackendConfig{ID: "b", ServiceDomain: "d"}) {
		t.Error("shared domain not merged")
	}
	if serviceDomain(BackendConfig{ID: "a"}) == serviceDomain(BackendConfig{ID: "b"}) {
		t.Error("distinct backends share a domain")
	}
}

func TestHealthTimestampsAndConflictStatus(t *testing.T) {
	_, s := gateway(t, newFake(t, 10, "x"))
	var h struct {
		Backends []map[string]any `json:"backends"`
	}
	getJSON(t, s.URL+"/healthz", 200, &h)
	if ts, ok := h.Backends[0]["stream_retry_after"]; !ok || ts != nil {
		t.Fatalf("unset cooldown should be null, got %v", ts)
	}
	if ts := h.Backends[0]["last_success"]; ts == nil {
		t.Fatal("synced backend must report last_success")
	}

	// The conflict half the name promises: differing domains make /healthz 503.
	a, b := newFake(t, 10, "different"), newFake(t, 10, "broadcast")
	g, s := gateway(t, a, b)
	changeBackend(g, 1, func(b *backend) { b.config.ServiceDomain = "different-domain" })
	var conflict struct {
		Ready     bool    `json:"ready"`
		Conflicts []int64 `json:"conflicting_service_ids"`
	}
	getJSON(t, s.URL+"/healthz", 503, &conflict)
	if conflict.Ready || len(conflict.Conflicts) != 1 || conflict.Conflicts[0] != 1000101 {
		t.Fatalf("conflict status not reported: %+v", conflict)
	}
}
