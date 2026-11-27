package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

const releaseBody = `{"tag_name":"v0.5.0","html_url":"https://example.test/releases/v0.5.0"}`

// setUpdateCheck points the check at a test server and restores the package state afterwards.
func setUpdateCheck(t *testing.T, url string, interval time.Duration) {
	t.Helper()
	oldURL, oldInterval := updateCheckURL, updateCheckInterval
	updateCheckURL, updateCheckInterval = url, interval
	t.Cleanup(func() { updateCheckURL, updateCheckInterval = oldURL, oldInterval })
}

func setVersion(t *testing.T, v string) {
	t.Helper()
	old := version
	version = v
	t.Cleanup(func() { version = old })
}

// releaseServer answers every request with status and body, counting requests.
func releaseServer(t *testing.T, status int, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(s.Close)
	return s, &requests
}

func TestUpdateCheckOffersNewerStableRelease(t *testing.T) {
	for _, running := range []string{"0.4.0", "v0.4.0", "0.5.0-rc1"} {
		t.Run(running, func(t *testing.T) {
			setVersion(t, running)
			agent := make(chan string, 1)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				agent <- r.Header.Get("User-Agent")
				io.WriteString(w, releaseBody)
			}))
			t.Cleanup(s.Close)
			setUpdateCheck(t, s.URL, time.Hour)
			g, server := gateway(t, newFake(t, 10, "service"))
			g.checkUpdate(context.Background())
			update := g.update.get()
			if update["version"] != "v0.5.0" || update["url"] != "https://example.test/releases/v0.5.0" {
				t.Fatalf("update: %v", update)
			}
			if got, ok := g.uiState(time.Now())["update"].(object); !ok || got == nil {
				t.Fatal("state has no update")
			}
			var wire struct {
				Update *struct {
					Version string `json:"version"`
					URL     string `json:"url"`
				} `json:"update"`
			}
			getJSON(t, server.URL+"/-/ui/state", 200, &wire)
			if wire.Update == nil || wire.Update.Version != "v0.5.0" || wire.Update.URL != "https://example.test/releases/v0.5.0" {
				t.Fatalf("state update: %+v", wire.Update)
			}
			select {
			case got := <-agent:
				if got != "Sakurako/"+running {
					t.Fatalf("User-Agent: %q", got)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("no request reached the server")
			}
			if got := g.uiState(time.Now())["version"]; got != running {
				t.Fatalf("state version: %v", got)
			}
		})
	}
}

func TestUpdateCheckSilentWithoutNewerStableRelease(t *testing.T) {
	for _, tc := range []struct{ name, running, body string }{
		{"same version", "0.5.0", `{"tag_name":"v0.5.0","html_url":"https://example.test/r"}`},
		{"older version", "0.5.0", `{"tag_name":"v0.4.9","html_url":"https://example.test/r"}`},
		{"dev build", "dev", `{"tag_name":"v0.5.0","html_url":"https://example.test/r"}`},
		{"unparseable tag", "0.4.0", `{"tag_name":"nightly","html_url":"https://example.test/r"}`},
		{"empty tag", "0.4.0", `{"html_url":"https://example.test/r"}`},
		{"empty url", "0.4.0", `{"tag_name":"v0.5.0","html_url":""}`},
		{"non-http url", "0.4.0", `{"tag_name":"v0.5.0","html_url":"javascript:alert(1)"}`},
		{"build metadata with a dash", "0.5.0+build-1", `{"tag_name":"v0.5.0","html_url":"https://example.test/r"}`},
		{"draft", "0.4.0", `{"tag_name":"v0.5.0","html_url":"https://example.test/r","draft":true}`},
		{"prerelease", "0.4.0", `{"tag_name":"v0.5.0","html_url":"https://example.test/r","prerelease":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setVersion(t, tc.running)
			s, _ := releaseServer(t, 200, tc.body)
			setUpdateCheck(t, s.URL, time.Hour)
			g, _ := gateway(t, newFake(t, 10, "service"))
			g.checkUpdate(context.Background())
			if update := g.update.get(); update != nil {
				t.Fatalf("offered %v", update)
			}
		})
	}
}

func TestUpdateCheckUnreachableLeavesNoUpdate(t *testing.T) {
	setVersion(t, "0.4.0")
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	for name, url := range map[string]string{
		"connection refused": closed.URL,
		"non-200":            mustServer(t, http.StatusInternalServerError, ""),
		"malformed JSON":     mustServer(t, http.StatusOK, "{"),
	} {
		t.Run(name, func(t *testing.T) {
			setUpdateCheck(t, url, time.Hour)
			g, _ := gateway(t, newFake(t, 10, "service"))
			g.checkUpdate(context.Background())
			if update := g.update.get(); update != nil {
				t.Fatalf("offered %v", update)
			}
		})
	}
}

func mustServer(t *testing.T, status int, body string) string {
	t.Helper()
	s, _ := releaseServer(t, status, body)
	return s.URL
}

func TestUpdateCheckKeepsLastReleaseOnFailure(t *testing.T) {
	setVersion(t, "0.4.0")
	var failing atomic.Bool
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if failing.Load() {
			http.Error(w, "boom", http.StatusBadGateway)
			return
		}
		io.WriteString(w, releaseBody)
	}))
	t.Cleanup(s.Close)
	setUpdateCheck(t, s.URL, time.Hour)
	g, _ := gateway(t, newFake(t, 10, "service"))
	g.checkUpdate(context.Background())
	if g.update.get() == nil {
		t.Fatal("first check found nothing")
	}
	failing.Store(true)
	g.checkUpdate(context.Background())
	if update := g.update.get(); update == nil {
		t.Fatal("a failed check cleared the last release")
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests: %d, want the failure retried", got)
	}
}

func TestRunUpdateCheckRetriesOnInterval(t *testing.T) {
	t.Setenv("SAKURAKO_UPDATE_CHECK", "")
	setVersion(t, "0.4.0")
	var requests atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "boom", http.StatusBadGateway)
			return
		}
		io.WriteString(w, releaseBody)
	}))
	t.Cleanup(s.Close)
	setUpdateCheck(t, s.URL, 10*time.Millisecond)
	g, _ := gateway(t, newFake(t, 10, "service"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.RunUpdateCheck(ctx)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for g.update.get() == nil {
		if time.Now().After(deadline) {
			t.Fatalf("no release found after %d requests", requests.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunUpdateCheck did not stop after cancel")
	}
	if got := requests.Load(); got < 2 {
		t.Fatalf("requests: %d, want a retry", got)
	}
}

func TestUpdateCheckClearsStaleRelease(t *testing.T) {
	setVersion(t, "0.4.0")
	var stale atomic.Bool
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stale.Load() {
			io.WriteString(w, `{"tag_name":"v0.4.0","html_url":"https://example.test/r"}`)
			return
		}
		io.WriteString(w, releaseBody)
	}))
	t.Cleanup(s.Close)
	setUpdateCheck(t, s.URL, time.Hour)
	g, _ := gateway(t, newFake(t, 10, "service"))
	g.checkUpdate(context.Background())
	if g.update.get() == nil {
		t.Fatal("first check found nothing")
	}
	stale.Store(true)
	g.checkUpdate(context.Background())
	if update := g.update.get(); update != nil {
		t.Fatalf("stale release kept: %v", update)
	}
}

func TestNewerVersion(t *testing.T) {
	for _, tc := range []struct {
		running, tag string
		want         bool
	}{
		{"0.4.0", "v0.5.0", true},
		{"v0.4.0", "v0.5.0", true},
		{"0.9.0", "v0.10.0", true},
		{"0.10.0", "v0.9.0", false},
		{"0.5.0", "v0.5.0", false},
		{"1.0", "v1.1.0", false},
		{"0.4.0", "v1.0", false},
		{"0.4.0", "v0.5.0+build", true},
		{"0.5.0+build", "v0.5.0", false},
		{"0.5.0+build-1", "v0.5.0", false},
		{"0.5.0-rc1", "v0.5.0", true},
		{"nightly", "v0.5.0", false},
		{"0.4.0", "nightly", false},
	} {
		if got := newerVersion(tc.running, tc.tag); got != tc.want {
			t.Errorf("newerVersion(%q, %q) = %v, want %v", tc.running, tc.tag, got, tc.want)
		}
	}
}

func TestUpdateCheckDisabledSkipsGitHub(t *testing.T) {
	t.Setenv("SAKURAKO_UPDATE_CHECK", " FALSE ")
	s, requests := releaseServer(t, 200, releaseBody)
	setUpdateCheck(t, s.URL, time.Millisecond)
	g, server := gateway(t, newFake(t, 10, "service"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.RunUpdateCheck(ctx) // returns at once when disabled
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunUpdateCheck kept running while disabled")
	}
	var wire map[string]any
	getJSON(t, server.URL+"/-/ui/state", 200, &wire)
	if got := requests.Load(); got != 0 {
		t.Fatalf("requests: %d, want none", got)
	}
	if wire["version"] != version {
		t.Fatalf("version: %v, want %v", wire["version"], version)
	}
	if wire["update"] != nil {
		t.Fatalf("update: %v, want null", wire["update"])
	}
}

func TestUpdateCheckIntervalIsSixHours(t *testing.T) {
	if updateCheckInterval != 6*time.Hour {
		t.Fatalf("updateCheckInterval: %v, want 6h", updateCheckInterval)
	}
}

func TestUpdateCheckEnabled(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"", true}, {"true", true}, {" TRUE ", true}, {"1", true},
		{"false", false}, {" False ", false},
	} {
		t.Setenv("SAKURAKO_UPDATE_CHECK", tc.value)
		if got := updateCheckEnabled(); got != tc.want {
			t.Errorf("updateCheckEnabled(%q) = %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestReadConfigUpdateCheck(t *testing.T) {
	for raw, want := range map[string]bool{
		`{"backends":[{"id":"a","url":"http://h/api"}]}`:                      true,
		`{"update_check":false,"backends":[{"id":"a","url":"http://h/api"}]}`: false,
		`{"update_check":true,"backends":[{"id":"a","url":"http://h/api"}]}`:  true,
	} {
		p := filepath.Join(t.TempDir(), "sakurako.json")
		if err := os.WriteFile(p, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		c, err := readConfig(p)
		if err != nil {
			t.Fatal(err)
		}
		if c.UpdateCheck != want {
			t.Errorf("%s: update_check = %v, want %v", raw, c.UpdateCheck, want)
		}
	}
}

// The check runs only when both the config and SAKURAKO_UPDATE_CHECK allow it.
func TestRunUpdateCheckConfigAndEnv(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config bool
		env    string
		want   bool
	}{
		{"both enabled", true, "", true},
		{"env other than false", true, "0", true},
		{"config disabled", false, "", false},
		{"env disabled", true, "false", false},
		{"both disabled", false, " FALSE ", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SAKURAKO_UPDATE_CHECK", tc.env)
			setVersion(t, "0.4.0")
			s, requests := releaseServer(t, 200, releaseBody)
			setUpdateCheck(t, s.URL, time.Hour)
			base, _ := gateway(t, newFake(t, 10, "service"))
			c := base.config
			c.UpdateCheck = tc.config
			g, err := NewGateway(c)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				g.RunUpdateCheck(ctx)
			}()
			if tc.want {
				deadline := time.Now().Add(2 * time.Second)
				for g.update.get() == nil {
					if time.Now().After(deadline) {
						t.Fatalf("no release found after %d requests", requests.Load())
					}
					time.Sleep(5 * time.Millisecond)
				}
				cancel()
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("RunUpdateCheck kept running")
			}
			if !tc.want {
				if got := requests.Load(); got != 0 {
					t.Fatalf("requests: %d, want none", got)
				}
				if update := g.update.get(); update != nil {
					t.Fatalf("update: %v, want nil", update)
				}
			}
		})
	}
}
