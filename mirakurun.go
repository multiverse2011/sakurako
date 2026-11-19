package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// fetchTuners queries every backend in parallel, so one slow backend does not
// use up the whole timeout. ids, lists and errs stay aligned by backend index.
func (g *Gateway) fetchTuners(parent context.Context) (ids []string, lists [][]object, errs []error) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(g.config.CatalogTimeoutSeconds)*time.Second)
	defer cancel()
	g.mu.RLock()
	backends := append([]backend(nil), g.backends...)
	g.mu.RUnlock()
	ids = make([]string, len(backends))
	lists = make([][]object, len(backends))
	errs = make([]error, len(backends))
	var wg sync.WaitGroup
	for i := range backends {
		ids[i] = backends[i].config.ID
		wg.Add(1)
		go func() {
			defer wg.Done()
			lists[i], errs[i] = g.fetch(ctx, backends[i], "/tuners")
		}()
	}
	wg.Wait()
	return ids, lists, errs
}

// tuners aggregates live tuner state. Unlike catalog endpoints it cannot use a
// snapshot: Mirakurun clients use it to see who holds each tuner right now.
// A failed backend is logged instead of failing the response; only when every
// backend fails is the response a 503.
func (g *Gateway) tuners(w http.ResponseWriter, r *http.Request) {
	_, lists, errs := g.fetchTuners(r.Context())
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

// Tuner event timings, as package variables so tests can shorten them.
var (
	tunerEventKeepalive = 15 * time.Second
	tunerEventRetry     = 5 * time.Second
)

// events relays tuner events from every backend to one client, in Mirakurun's
// format: an application/json response holding an unclosed array. Each client
// keeps its own upstream subscriptions, which end when the client disconnects.
func (g *Gateway) events(w http.ResponseWriter, r *http.Request) {
	if q := r.URL.Query(); len(q) != 1 || len(q["resource"]) != 1 || q["resource"][0] != "tuner" {
		apiError(w, 400, "only resource=tuner is supported")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	if r.Method == "HEAD" {
		return
	}
	controller := http.NewResponseController(w)
	// Override the server-wide WriteTimeout. A write deadline only affects writes,
	// so an idle response survives it; refreshing just before each write is enough.
	write := func(s string) bool {
		if err := controller.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
			log.Printf("events: write deadline failed: %v", err)
			return false
		}
		if _, err := io.WriteString(w, s); err != nil {
			return false
		}
		return controller.Flush() == nil
	}
	if !write("[\n") {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	out := make(chan object) // unbuffered: a slow client backpressures its subscriptions
	var live atomic.Int32    // established upstream subscriptions
	var wg sync.WaitGroup
	defer func() { cancel(); wg.Wait() }() // no subscription outlives its client
	for i := range g.backends {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			g.relayTunerEvents(ctx, index, out, &live)
		}(i)
	}
	ticker := time.NewTicker(tunerEventKeepalive)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-out:
			data, err := json.Marshal(e)
			if err != nil {
				log.Printf("events: %v", err)
				continue
			}
			if !write(string(append(data, '\n', ',', '\n'))) {
				return
			}
		case <-ticker.C:
			// Mirakurun never writes while connected. Keepalive only bridges a
			// subscription that is down, so byte-driven clients such as the denpa
			// bridge do not re-read /api/tuners while nothing is happening.
			if live.Load() != int32(len(g.backends)) && !write("\n") {
				return
			}
		}
	}
}

// relayTunerEvents subscribes to one backend until ctx ends, reconnecting after
// failures so a restarted backend is picked up again.
func (g *Gateway) relayTunerEvents(ctx context.Context, index int, out chan<- object, live *atomic.Int32) {
	for {
		err := g.followTunerEvents(ctx, index, out, live)
		if ctx.Err() != nil {
			return
		}
		log.Printf("events: %s: %v (reconnecting in %v)", g.backends[index].config.ID, err, tunerEventRetry)
		select {
		case <-ctx.Done():
			return
		case <-time.After(tunerEventRetry):
		}
	}
}

// followTunerEvents reads Mirakurun's JSON array one event at a time and rewrites
// tuner data exactly like /api/tuners before handing it on.
func (g *Gateway) followTunerEvents(ctx context.Context, index int, out chan<- object, live *atomic.Int32) error {
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL(g.backends[index].base, "/events/stream", "resource=tuner"), nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Sakurako")
	resp, err := g.eventsClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// Drain a bounded error body so repeated failures can reuse the connection.
	drain := func() { _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) }
	if resp.StatusCode != 200 {
		drain()
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if tok, err := dec.Token(); err != nil {
		return err
	} else if tok != json.Delim('[') {
		drain()
		return fmt.Errorf("not an event stream: starts with %v", tok)
	}
	live.Add(1)
	defer live.Add(-1)
	for {
		var e object
		if err := dec.Decode(&e); err != nil {
			return err
		}
		if data, ok := e["data"].(map[string]any); ok {
			g.exposeTuner(data, index)
		}
		select {
		case out <- e:
		case <-ctx.Done():
			return ctx.Err()
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
