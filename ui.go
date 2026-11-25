package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/multiverse2011/sakurako/internal/apidocs"
)

//go:embed ui/*
var uiFiles embed.FS

// uiAsset is an embedded file with its compressed form. gzipped stays nil when
// compression does not pay off.
type uiAsset struct {
	data, gzipped []byte
	etag, gzEtag  string
}

// uiAssets compresses each file once at startup; requests then only pick a
// cached representation.
var uiAssets = func() map[string]*uiAsset {
	assets := map[string]*uiAsset{}
	// Files served under /ui/; add new assets here too.
	for _, name := range []string{"theme.js", "theme.css", "index.html", "style.css", "app.js", "docs.html", "docs.js", "favicon.svg", "vendor/scalar-standalone.js"} {
		data, err := uiFiles.ReadFile("ui/" + name)
		if err != nil {
			panic(err)
		}
		asset := &uiAsset{data: data, etag: contentETag(data)}
		if gzipped := gzipBytes(data); len(gzipped) < len(data) {
			asset.gzipped, asset.gzEtag = gzipped, contentETag(gzipped)
		}
		assets[name] = asset
	}
	return assets
}()

func contentETag(data []byte) string {
	return fmt.Sprintf(`"%x"`, sha256.Sum256(data))
}

// gzipWriters recycles writers: one costs about 1 MiB, which is too much to
// allocate for every compressed catalog response.
var gzipWriters = sync.Pool{New: func() any { return gzip.NewWriter(io.Discard) }}

func gzipBytes(data []byte) []byte {
	var buf bytes.Buffer
	zw := gzipWriters.Get().(*gzip.Writer)
	zw.Reset(&buf)
	if _, err := zw.Write(data); err != nil {
		panic(err)
	}
	if err := zw.Close(); err != nil {
		panic(err)
	}
	zw.Reset(io.Discard) // drop the reference to buf while the writer waits in the pool
	gzipWriters.Put(zw)
	return buf.Bytes()
}

// acceptsGzip parses Accept-Encoding, honoring explicit q=0 exclusions over the
// wildcard.
func acceptsGzip(header string) bool {
	gzipQ, wildcardQ := -1.0, -1.0
	for _, entry := range strings.Split(header, ",") {
		fields := strings.Split(strings.TrimSpace(entry), ";")
		q := 1.0
		for _, param := range fields[1:] {
			if name, value, ok := strings.Cut(strings.TrimSpace(param), "="); ok && strings.EqualFold(strings.TrimSpace(name), "q") {
				if parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64); err == nil {
					q = parsed
				}
			}
		}
		switch strings.ToLower(strings.TrimSpace(fields[0])) {
		case "gzip", "x-gzip":
			gzipQ = q
		case "*":
			wildcardQ = q
		}
	}
	if gzipQ >= 0 {
		return gzipQ > 0
	}
	return wildcardQ > 0
}

//go:embed openapi.yml
var openAPISpec []byte

// openAPIJSON is derived from the same declarations as openapi.yml, so there is no second file to keep current.
var openAPIJSON = func() []byte {
	data, err := json.Marshal(apidocs.Document())
	if err != nil {
		panic(err)
	}
	return data
}()

func serveOpenAPI(w http.ResponseWriter, r *http.Request, data []byte, contentType string) {
	etag := contentETag(data)
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && etagMatches(r.Header.Get("If-None-Match"), etag) {
		w.Header().Set("ETag", etag)
		w.Header().Set("Cache-Control", "no-cache")
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("ETag", etag)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-cache")
	if r.Method != http.MethodHead {
		w.Write(data)
	}
}

func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimPrefix(strings.TrimSpace(candidate), "W/")
		if candidate == etag || candidate == "*" {
			return true
		}
	}
	return false
}

func (g *Gateway) serveUI(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path == "/openapi.yml" || r.URL.Path == "/openapi.json" {
		data, contentType := openAPISpec, "application/yaml; charset=utf-8"
		if r.URL.Path == "/openapi.json" {
			data, contentType = openAPIJSON, "application/json; charset=utf-8"
		}
		serveOpenAPI(w, r, data, contentType)
		return true
	}

	if r.URL.Path == "/-/ui/state" {
		w.Header().Set("Cache-Control", "no-store")
		respond(w, 200, g.uiState(time.Now()))
		return true
	}
	if r.URL.Path == "/-/ui/tuners" {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		// HEAD reports these headers without the live per-backend counts.
		if r.Method != "HEAD" {
			if counts := g.tunerCounts(r.Context()); counts != nil {
				respond(w, 200, counts)
			}
		}
		return true
	}
	if r.URL.Path == "/-/ui/config" {
		w.Header().Set("Cache-Control", "no-store")
		respond(w, 200, g.config)
		return true
	}
	if r.URL.Path == "/-/ui/logs" {
		afterParam := r.URL.Query().Get("after")
		after, err := strconv.ParseInt(afterParam, 10, 64)
		if afterParam != "" && err != nil {
			apiError(w, 400, "invalid after")
			return true
		}
		lines, next := recentLogs.since(after)
		w.Header().Set("Cache-Control", "no-store")
		respond(w, 200, object{"lines": lines, "next": next})
		return true
	}
	if r.URL.Path == "/" || r.URL.Path == "/ui" {
		http.Redirect(w, r, "/ui/", http.StatusTemporaryRedirect)
		return true
	}
	if !strings.HasPrefix(r.URL.Path, "/ui/") {
		return false
	}
	name := strings.TrimPrefix(r.URL.Path, "/ui/")
	if name == "" {
		name = "index.html"
	}
	asset := uiAssets[name]
	if asset == nil {
		http.NotFound(w, r)
		return true
	}
	docs := name == "docs.html"
	contentType := map[string]string{"index.html": "text/html; charset=utf-8", "style.css": "text/css; charset=utf-8", "app.js": "text/javascript; charset=utf-8", "favicon.svg": "image/svg+xml"}[name]
	if strings.HasSuffix(name, ".js") {
		contentType = "text/javascript; charset=utf-8"
	}
	if strings.HasSuffix(name, ".css") {
		contentType = "text/css; charset=utf-8"
	}
	if strings.HasSuffix(name, ".html") {
		contentType = "text/html; charset=utf-8"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
	if docs {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'none'")
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-cache")
	serveUIAsset(w, r, asset)
	return true
}

// serveUIAsset picks the identity or gzip representation and revalidates it.
// Each encoding has its own ETag so a cache never answers with the wrong one.
func serveUIAsset(w http.ResponseWriter, r *http.Request, asset *uiAsset) {
	data, etag := asset.data, asset.etag
	if asset.gzipped != nil && acceptsGzip(r.Header.Get("Accept-Encoding")) {
		data, etag = asset.gzipped, asset.gzEtag
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set("ETag", etag)
	w.Header().Set("Vary", "Accept-Encoding")
	if etagMatches(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if r.Method != http.MethodHead {
		w.Write(data)
	}
}

// This is an observational snapshot: catalog and stream state are copied under
// separate locks so catalog refresh never waits for JSON encoding or UI clients.
func (g *Gateway) uiState(now time.Time) object {
	backends, services, groups := []object{}, []object{}, []object{}
	cat := g.catalogNow(now)
	g.mu.RLock()
	maxStale := time.Duration(g.config.MaxStaleSeconds) * time.Second
	for i := range g.backends {
		b := &g.backends[i]
		fresh, healthy := b.status(now, maxStale)
		state := "Healthy"
		if b.updated.IsZero() {
			state = "Not synced"
		} else if !fresh {
			state = "Expired"
		} else if !healthy {
			state = "Using cached catalog"
		}
		backends = append(backends, object{"id": b.config.ID, "ui_url": mirakurunUIURL(b.base), "catalog_state": state, "catalog_available": fresh, "healthy": healthy, "available": healthy && !now.Before(b.blockedUntil), "last_success": nilIfZero(b.updated), "retry_after": nilIfExpired(now, b.blockedUntil), "domain": serviceDomain(b.config), "priority": b.config.Priority})
	}
	for _, svc := range cat.services {
		id := number(svc, "id")
		candidates := []object{}
		preferred := ""
		for _, source := range cat.sources[id] {
			b := g.backends[source.backend]
			available := g.usable(source.backend, id, now)
			candidates = append(candidates, object{"id": b.config.ID, "available": available})
			if preferred == "" && available {
				preferred = b.config.ID
			}
		}
		services = append(services, object{"id": id, "name": svc["name"], "type": channelOf(svc)["type"], "candidates": candidates, "preferred": preferred})
	}
	g.mu.RUnlock()
	g.streamsMu.Lock()
	for _, s := range g.streams {
		s.mu.Lock()
		if s.status != 0 && s.status != 200 {
			s.mu.Unlock()
			continue
		}
		clients := []object{}
		for sub := range s.subscribers {
			clients = append(clients, object{"id": sub.id, "remote": sub.remote, "user_agent": sub.userAgent, "joined": sub.joined, "bytes": sub.bytes.Load()})
		}
		sort.Slice(clients, func(i, j int) bool { return clients[i]["id"].(string) < clients[j]["id"].(string) })
		state := "starting"
		if s.status == 200 {
			state = "streaming"
		}
		groups = append(groups, object{"id": s.id, "service_id": s.key.id, "service_name": s.serviceName, "channel": s.key.channel, "channel_type": s.key.channelType, "domain": s.key.domain, "decode": s.key.decode, "priority": s.key.priority, "state": state, "backend_id": s.backendID, "created": s.created, "started": nilIfZero(s.started), "upstream_bytes": s.offset, "clients": clients})
		s.mu.Unlock()
	}
	g.streamsMu.Unlock()
	sort.Slice(groups, func(i, j int) bool { return groups[i]["id"].(string) < groups[j]["id"].(string) })
	return object{"downstream_bytes": g.downstreamBytes.Load(), "observed_at": now, "backends": backends, "services": services, "streams": groups, "conflicting_service_ids": cat.conflicts, "sync_seconds": g.config.SyncSeconds}
}

// tunerCounts summarizes live tuner state for the UI. It has its own route
// because every call queries the backends; the 1-second /-/ui/state poll must not.
// A backend that fails is left out so the UI can render just that row as unknown;
// failures are not logged here because the UI polls this every few seconds.
func (g *Gateway) tunerCounts(ctx context.Context) object {
	ids, lists, errs := g.fetchTuners(ctx)
	// A disconnected client is not a backend failure; Sync skips shutdown
	// cancellations the same way.
	if ctx.Err() != nil {
		return nil
	}
	backends := []object{}
	all := tunerCount{byHolder: map[string]int{}, freeByType: map[string]int{}}
	for i, list := range lists {
		if errs[i] != nil {
			continue
		}
		c := countTuners(list)
		all.add(c)
		row := c.object()
		row["id"] = ids[i]
		backends = append(backends, row)
	}
	out := all.object()
	out["available"] = len(backends) > 0
	out["backends"] = backends
	return out
}

// tunerCount summarizes a backend's tuners. byHolder splits the in-use tuners by
// who holds them, so a backend using its own tuners for EPG does not look like
// an unexplained allocation. freeByType counts free tuners per supported type;
// a combined tuner is free for each of its types.
type tunerCount struct {
	inUse, total, free, fault int
	byHolder                  map[string]int
	freeByType                map[string]int
}

// countTuners splits a backend's tuners into in use, free and faulty, so a faulty
// tuner is never counted as an allocation or as something that can be allocated.
// isUsing is checked as well as users because not every Mirakurun reports both.
func countTuners(list []object) tunerCount {
	c := tunerCount{byHolder: map[string]int{}, freeByType: map[string]int{}}
	for _, t := range list {
		c.total++
		users, _ := t["users"].([]any)
		types, _ := t["types"].([]any)
		for _, typ := range types {
			if name, ok := typ.(string); ok {
				if _, seen := c.freeByType[name]; !seen {
					c.freeByType[name] = 0 // keep a type visible when none of it is free
				}
			}
		}
		switch {
		case t["isFault"] == true:
			c.fault++
		case t["isUsing"] == true || len(users) > 0:
			c.inUse++
			c.byHolder[tunerHolder(users)]++
		default:
			c.free++
			for _, typ := range types {
				if name, ok := typ.(string); ok {
					c.freeByType[name]++
				}
			}
		}
	}
	return c
}

// tunerHolder names who holds an in-use tuner. Mirakurun records a stream's
// User-Agent as the user's agent, and sakurako always sends "Sakurako", so its
// own allocations are recognised without matching the id, which is IP:port.
// An id prefixed "Mirakurun:" is the backend itself (EPG or another internal job).
func tunerHolder(users []any) string {
	kind := "other"
	for _, u := range users {
		user, _ := u.(map[string]any)
		id, _ := user["id"].(string)
		switch {
		case user["agent"] == "Sakurako":
			return "sakurako" // a tuner shared with sakurako is counted once, as sakurako's
		case id == "Mirakurun:getEPG()":
			kind = "epg"
		case strings.HasPrefix(id, "Mirakurun:") && kind != "epg":
			kind = "backend"
		}
	}
	return kind
}

func (c *tunerCount) add(o tunerCount) {
	c.inUse, c.total, c.free, c.fault = c.inUse+o.inUse, c.total+o.total, c.free+o.free, c.fault+o.fault
	for k, n := range o.byHolder {
		c.byHolder[k] += n
	}
	for k, n := range o.freeByType {
		if _, seen := c.freeByType[k]; !seen {
			c.freeByType[k] = 0
		}
		c.freeByType[k] += n
	}
}

// object renders the counts in the shape /-/ui/tuners returns.
func (c tunerCount) object() object {
	return object{
		"in_use": c.inUse, "total": c.total, "free": c.free, "fault": c.fault,
		"in_use_by": object{
			"sakurako": c.byHolder["sakurako"], "epg": c.byHolder["epg"],
			"backend": c.byHolder["backend"], "other": c.byHolder["other"],
		},
		"free_by_type": freeByTypeList(c.freeByType),
	}
}

// freeByTypeList keeps broadcast types in their usual order and sorts the rest.
func freeByTypeList(counts map[string]int) []object {
	out, seen := []object{}, map[string]bool{}
	for _, typ := range []string{"GR", "BS", "CS"} {
		if n, ok := counts[typ]; ok {
			out, seen[typ] = append(out, object{"type": typ, "free": n}), true
		}
	}
	rest := []string{}
	for typ := range counts {
		if !seen[typ] {
			rest = append(rest, typ)
		}
	}
	sort.Strings(rest)
	for _, typ := range rest {
		out = append(out, object{"type": typ, "free": counts[typ]})
	}
	return out
}

// mirakurunUIURL preserves reverse-proxy prefixes while removing the Mirakurun API suffix.
func mirakurunUIURL(base *url.URL) string {
	if base == nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return ""
	}
	u := *base
	u.User = nil
	u.RawQuery = ""
	u.Fragment = ""
	u.Path = strings.TrimSuffix(strings.TrimRight(u.Path, "/"), "/api") + "/"
	u.RawPath = ""
	return u.String()
}
