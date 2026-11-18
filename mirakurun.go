package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// tuners aggregates live tuner state. Unlike catalog endpoints it cannot use a
// snapshot: Mirakurun clients use it to see who holds each tuner right now.
// Backends are queried in parallel so one slow backend does not use up the whole
// timeout, and a failed backend is logged instead of failing the response.
// Only when every backend fails is the response a 503.
func (g *Gateway) tuners(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(g.config.CatalogTimeoutSeconds)*time.Second)
	defer cancel()
	g.mu.RLock()
	backends := append([]backend(nil), g.backends...)
	g.mu.RUnlock()
	lists := make([][]object, len(backends))
	errs := make([]error, len(backends))
	var wg sync.WaitGroup
	for i := range backends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lists[i], errs[i] = g.fetch(ctx, backends[i], "/tuners")
		}()
	}
	wg.Wait()
	// A disconnected client is not a backend failure; Sync skips shutdown
	// cancellations the same way.
	if r.Context().Err() != nil {
		return
	}
	out := []object{}
	ok := 0
	for i, list := range lists {
		if errs[i] != nil {
			log.Printf("tuners: %v", errs[i])
			continue
		}
		ok++
		for _, t := range list {
			g.exposeTuner(t, i)
			out = append(out, t)
		}
	}
	if ok == 0 {
		apiError(w, 503, "tuner state unavailable from all backends")
		return
	}
	respond(w, 200, out)
}

// exposeTuner rewrites what differs between a backend and this gateway: channel
// names, and with multiple backends, index/name uniqueness.
func (g *Gateway) exposeTuner(t object, index int) {
	b := g.backends[index].config
	if len(g.backends) > 1 {
		// shortcut: 10000-index bands, upgrade if one backend can expose >=10000 tuners.
		t["index"] = json.Number(strconv.FormatInt(number(t, "index")+int64(index)*10000, 10))
		if name, ok := t["name"].(string); ok {
			t["name"] = b.ID + "/" + name
		}
	}
	users, _ := t["users"].([]any)
	for _, u := range users {
		user, _ := u.(map[string]any)
		settings, _ := user["streamSetting"].(map[string]any)
		channel, _ := settings["channel"].(map[string]any)
		if name, ok := channel["channel"].(string); ok {
			channel["channel"] = g.channelName(b, name)
		}
	}
}

// events keeps a Mirakurun client connected. Tuner changes are not pushed:
// channel-based clients re-read /api/tuners on their own schedule.
// shortcut: keepalive only, upgrade when instant tuner push is needed.
func (g *Gateway) events(w http.ResponseWriter, r *http.Request) {
	if q := r.URL.Query(); len(q) != 1 || len(q["resource"]) != 1 || q["resource"][0] != "tuner" {
		apiError(w, 400, "only resource=tuner is supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	if r.Method == "HEAD" {
		return
	}
	controller := http.NewResponseController(w)
	// Override the server-wide WriteTimeout; SSE must outlive it.
	refresh := func() error { return controller.SetWriteDeadline(time.Now().Add(30 * time.Second)) }
	if err := refresh(); err != nil {
		log.Printf("events: SSE setup failed: %v", err)
		return
	}
	if err := controller.Flush(); err != nil {
		log.Printf("events: SSE setup failed: %v", err)
		return
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := refresh(); err != nil {
				log.Printf("events: keepalive deadline failed: %v", err)
				return
			}
			if _, err := io.WriteString(w, ": keepalive\n\n"); err != nil {
				return
			}
			if err := controller.Flush(); err != nil {
				return
			}
		}
	}
}

// logo proxies the preferred redundant source's logo, falling back to the others.
// Logos are small and cached by clients, so they are fetched on demand instead of
// being stored in the catalog.
func (g *Gateway) logo(w http.ResponseWriter, r *http.Request, id int64, sources []source) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(g.config.CatalogTimeoutSeconds)*time.Second)
	defer cancel()
	notFound := false
	sawFailure := false
	for _, s := range sources {
		backendID := g.backends[s.backend].config.ID
		u := apiURL(g.backends[s.backend].base, "/services/"+strconv.FormatInt(id, 10)+"/logo", "")
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			sawFailure = true
			log.Printf("logo %d via %s: invalid request: %v", id, backendID, err)
			continue
		}
		req.Header.Set("User-Agent", "Sakurako")
		resp, err := g.catalogClient.Do(req)
		if err != nil {
			sawFailure = true
			log.Printf("logo %d via %s: %v", id, backendID, err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if resp.StatusCode == http.StatusNotFound {
				notFound = true
			} else {
				sawFailure = true
				log.Printf("logo %d via %s: upstream HTTP %d", id, backendID, resp.StatusCode)
			}
			continue
		}
		for _, h := range []string{"Content-Type", "Content-Length", "Cache-Control", "ETag", "Last-Modified"} {
			if v := resp.Header.Get(h); v != "" {
				w.Header().Set(h, v)
			}
		}
		if w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", "image/png")
		}
		w.WriteHeader(http.StatusOK)
		if r.Method != "HEAD" {
			if _, err := io.Copy(w, resp.Body); err != nil {
				log.Printf("logo %d via %s: copy failed: %v", id, backendID, err)
			}
		}
		resp.Body.Close()
		return
	}
	if notFound && !sawFailure {
		w.Header().Set("Cache-Control", "no-store")
		apiError(w, 404, "logo not found")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	apiError(w, 503, "no healthy source could provide the logo")
}
