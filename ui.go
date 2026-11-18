package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/multiverse2011/sakurako/internal/apidocs"
)

//go:embed ui/*
var uiFiles embed.FS

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
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(data))
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
		respond(w, 200, g.tunerCounts(r.Context()))
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
	switch name {
	case "theme.js", "theme.css", "index.html", "style.css", "app.js", "docs.html", "docs.js", "favicon.svg", "vendor/scalar-standalone.js":
	default:
		http.NotFound(w, r)
		return true
	}
	data, err := uiFiles.ReadFile("ui/" + name)
	if err != nil {
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
	if r.Method != "HEAD" {
		w.Write(data)
	}
	return true
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
// A backend that fails is left out so the UI can render just that row as unknown.
func (g *Gateway) tunerCounts(ctx context.Context) object {
	lists, errs := g.fetchTuners(ctx)
	backends := []object{}
	inUse, total, fault := 0, 0, 0
	for i, list := range lists {
		if errs[i] != nil {
			log.Printf("ui tuners: %v", errs[i])
			continue
		}
		used, all, broken := countTuners(list)
		inUse, total, fault = inUse+used, total+all, fault+broken
		backends = append(backends, object{"id": g.backends[i].config.ID, "in_use": used, "total": all, "free": all - used, "fault": broken})
	}
	return object{"available": len(backends) > 0, "in_use": inUse, "total": total, "free": total - inUse, "fault": fault, "backends": backends}
}

// countTuners returns how many tuners are allocated, how many exist, and how many are faulty.
func countTuners(list []object) (inUse, total, fault int) {
	for _, t := range list {
		total++
		if t["isFault"] == true {
			fault++
		}
		users, _ := t["users"].([]any)
		if t["isUsing"] == true || len(users) > 0 {
			inUse++
		}
	}
	return inUse, total, fault
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
