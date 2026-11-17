package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var testClient = &http.Client{Timeout: 10 * time.Second}

func testConfig(t *testing.T, mutate func(*mockConfig)) mockConfig {
	t.Helper()
	c := mockConfig{networkID: 10, channelType: "GR", channel: "27", services: 2, programsPerService: 2, streamRate: 1 << 20}
	if mutate != nil {
		mutate(&c)
	}
	return c
}

func getJSON(t *testing.T, url string, v any) {
	t.Helper()
	resp, err := testClient.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("%s: HTTP %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogAndStreamFile(t *testing.T) {
	data := bytes.Repeat([]byte{0x47, 0x1f, 0xff, 0x10}, 2000)
	path := filepath.Join(t.TempDir(), "sample.ts")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(newHandler(testConfig(t, func(c *mockConfig) { c.streamFile = path })))
	defer srv.Close()

	var services []service
	getJSON(t, srv.URL+"/api/services", &services)
	if len(services) != 2 || services[0].ID != 10*100000+1024 || services[1].ServiceID != 1025 {
		t.Fatalf("unexpected services: %+v", services)
	}
	var programs []program
	getJSON(t, srv.URL+"/api/programs", &programs)
	if len(programs) != 4 {
		t.Fatalf("unexpected program count: %d", len(programs))
	}
	for _, p := range programs {
		if p.ID != (p.NetworkID*100000+p.ServiceID)*100000+p.EventID {
			t.Fatalf("program id mismatch: %+v", p)
		}
	}
	resp, err := testClient.Get(srv.URL + "/api/services/1001024/stream?decode=0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "video/MP2T" {
		t.Fatalf("stream response: %d %s", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	got := make([]byte, 4000)
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[:4000]) {
		t.Fatal("stream did not serve the recording")
	}

	req, _ := http.NewRequest("HEAD", srv.URL+"/api/services/1001024/stream", nil)
	head, err := testClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	if head.StatusCode != 200 {
		t.Fatalf("HEAD: HTTP %d", head.StatusCode)
	}
	resp2, err := testClient.Get(srv.URL + "/api/services/999/stream")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("unknown service: HTTP %d", resp2.StatusCode)
	}
}

func TestFailureFlags(t *testing.T) {
	catalog := httptest.NewServer(newHandler(testConfig(t, func(c *mockConfig) { c.failCatalog = true })))
	defer catalog.Close()
	resp, err := testClient.Get(catalog.URL + "/api/services")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("fail-catalog: HTTP %d", resp.StatusCode)
	}

	stream := httptest.NewServer(newHandler(testConfig(t, func(c *mockConfig) { c.failStream = 403 })))
	defer stream.Close()
	resp, err = testClient.Get(stream.URL + "/api/services/1001024/stream")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("fail-stream: HTTP %d", resp.StatusCode)
	}

	slow := httptest.NewServer(newHandler(testConfig(t, func(c *mockConfig) { c.streamDelay = 100 * time.Millisecond })))
	defer slow.Close()
	start := time.Now()
	resp, err = testClient.Get(slow.URL + "/api/services/1001024/stream")
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("stream-delay ignored: %v", elapsed)
	}
	resp.Body.Close()
}

func TestValidate(t *testing.T) {
	packet := make([]byte, 188)
	short := filepath.Join(t.TempDir(), "short.ts")
	if err := os.WriteFile(short, packet[:187], 0600); err != nil {
		t.Fatal(err)
	}
	validFile := filepath.Join(t.TempDir(), "valid.ts")
	if err := os.WriteFile(validFile, packet, 0600); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]mockConfig{
		"network id":        testConfig(t, func(c *mockConfig) { c.networkID = 70000 }),
		"channel type":      testConfig(t, func(c *mockConfig) { c.channelType = "XX" }),
		"empty channel":     testConfig(t, func(c *mockConfig) { c.channel = "" }),
		"no services":       testConfig(t, func(c *mockConfig) { c.services = 0 }),
		"bad status":        testConfig(t, func(c *mockConfig) { c.failStream = 999 }),
		"missing file":      testConfig(t, func(c *mockConfig) { c.streamFile = filepath.Join(t.TempDir(), "missing.ts") }),
		"short stream file": testConfig(t, func(c *mockConfig) { c.streamFile = short }),
	} {
		if c.validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
	valid := testConfig(t, func(c *mockConfig) { c.streamFile = validFile })
	if err := valid.validate(); err != nil {
		t.Fatalf("valid stream file rejected: %v", err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("newHandler rejected a valid config: %v", r)
		}
	}()
	if newHandler(valid) == nil {
		t.Fatal("newHandler returned nil")
	}
}
