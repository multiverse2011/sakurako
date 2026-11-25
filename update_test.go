package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
			g, _ := gateway(t, newFake(t, 10, "service"))
			g.checkUpdate(context.Background())
			update := g.update.get()
			if update["version"] != "v0.5.0" || update["url"] != "https://example.test/releases/v0.5.0" {
				t.Fatalf("update: %v", update)
			}
			if got := <-agent; got != "Sakurako/"+running {
				t.Fatalf("User-Agent: %q", got)
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
	go g.RunUpdateCheck(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for g.update.get() == nil {
		if time.Now().After(deadline) {
			t.Fatalf("no release found after %d requests", requests.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if got := requests.Load(); got < 2 {
		t.Fatalf("requests: %d, want a retry", got)
	}
}

func TestUpdateCheckDisabledSkipsGitHub(t *testing.T) {
	t.Setenv("SAKURAKO_UPDATE_CHECK", " FALSE ")
	s, requests := releaseServer(t, 200, releaseBody)
	setUpdateCheck(t, s.URL, time.Millisecond)
	g, server := gateway(t, newFake(t, 10, "service"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.RunUpdateCheck(ctx) // returns at once when disabled
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
