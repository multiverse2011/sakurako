package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Variables, not constants, so tests can point them at an httptest server.
var (
	updateCheckURL      = "https://api.github.com/repos/multiverse2011/sakurako/releases/latest"
	updateCheckInterval = 6 * time.Hour
)

// updateCache holds the release offered by /-/ui/state. Its lock is separate
// from Gateway.mu so a check never holds it while waiting on GitHub.
type updateCache struct {
	mu     sync.Mutex
	latest object // {"version","url"}; nil until a check finds a newer release, never mutated after set
}

func (c *updateCache) get() object {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.latest
}

func (c *updateCache) set(latest object) {
	c.mu.Lock()
	c.latest = latest
	c.mu.Unlock()
}

// updateCheckEnabled honors SAKURAKO_UPDATE_CHECK: unset or any value other than
// "false" enables the check.
func updateCheckEnabled() bool {
	return !strings.EqualFold(strings.TrimSpace(os.Getenv("SAKURAKO_UPDATE_CHECK")), "false")
}

// RunUpdateCheck checks once at startup and then every updateCheckInterval. It is
// a separate loop from Run so startup never waits on GitHub and tests calling
// Run never reach the network.
func (g *Gateway) RunUpdateCheck(ctx context.Context) {
	if !updateCheckEnabled() {
		return
	}
	t := time.NewTicker(updateCheckInterval)
	defer t.Stop()
	for {
		g.checkUpdate(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// checkUpdate records the newest stable release. A failed check logs one line and
// keeps the previous result, so the next interval retries without the header
// losing what it already showed.
func (g *Gateway) checkUpdate(ctx context.Context) {
	latest, err := fetchLatestRelease(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return // a cancelled check is shutdown, not a failure
		}
		log.Printf("update check failed: %v", err)
		return
	}
	g.update.set(latest)
}

type githubRelease struct {
	TagName    string `json:"tag_name"`
	HTMLURL    string `json:"html_url"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
}

// fetchLatestRelease returns the update to offer, or nil when the latest stable
// release is not newer than this build.
func fetchLatestRelease(ctx context.Context) (object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, updateCheckURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Sakurako/"+version)
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}
	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, err
	}
	// releases/latest excludes drafts and prereleases, but a proxy or a changed
	// API should not be trusted to.
	if release.Draft || release.Prerelease || release.TagName == "" || !newerVersion(version, release.TagName) {
		return nil, nil
	}
	// The link is opened by a browser, so only a real page URL is usable.
	if link, err := url.Parse(release.HTMLURL); err != nil || (link.Scheme != "http" && link.Scheme != "https") || link.Host == "" {
		return nil, nil
	}
	return object{"version": release.TagName, "url": release.HTMLURL}, nil
}

// newerVersion compares numeric cores only; callers exclude prerelease and
// draft candidates themselves.
// shortcut: prerelease and build metadata are ignored, so the same core counts as
// newer only when the running build is a prerelease. Move to a SemVer
// implementation when tag-side prerelease ordering is needed.
func newerVersion(running, tag string) bool {
	current, ok := versionCore(running)
	if !ok {
		return false
	}
	candidate, ok := versionCore(tag)
	if !ok {
		return false
	}
	for i, n := range candidate {
		if n != current[i] {
			return n > current[i]
		}
	}
	core := strings.TrimPrefix(strings.TrimSpace(running), "v")
	if i := strings.Index(core, "+"); i >= 0 {
		core = core[:i]
	}
	return strings.Contains(core, "-")
}

// versionCore parses "v1.2.3[-pre][+build]" into its three numeric parts.
func versionCore(s string) ([3]int, bool) {
	var v [3]int
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return v, false
	}
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil {
			return v, false
		}
		v[i] = n
	}
	return v, true
}
