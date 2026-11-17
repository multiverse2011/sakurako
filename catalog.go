package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// object preserves upstream extension fields. Integer precision survives only
// because fetch decodes with json.Decoder.UseNumber; other decoders yield float64.
type object map[string]any
type backend struct {
	config             BackendConfig
	base               *url.URL // parsed config.URL, fixed after NewGateway
	services, programs []object
	updated            time.Time
	healthy            bool
	blockedUntil       time.Time            // backend-wide stream cooldown (500/502/other 5xx, mid-stream read failure)
	blockedServices    map[int64]time.Time  // per-service cooldown (pre-body 404/408/429/503, empty/first-byte timeout)
	blockedChannels    map[string]time.Time // per-channel cooldown for channel streams (same failure classes)
	err                string
}

// status reports whether the catalog is fresh and healthy; callers apply stream cooldowns separately.
func (b *backend) status(now time.Time, maxStale time.Duration) (fresh, healthy bool) {
	fresh = !b.updated.IsZero() && now.Sub(b.updated) <= maxStale
	return fresh, b.healthy && fresh
}

type source struct {
	backend int
	service object
}
type catalog struct {
	services, programs, channels []object
	sources                      map[int64][]source
	channelSources               map[string][]source // exposed channel type + "/" + channel name
	conflicts                    []int64
}

// channelEntry is one service's view of an exposed channel, filtered after the
// preferred backend for that channel name is known.
type channelEntry struct {
	key, domain      string
	channel, service object
}

// Gateway aggregates Mirakurun backends into one catalog and shares their streams.
type Gateway struct {
	downstreamBytes atomic.Int64

	nextConnectionID uint64 // guarded by streamsMu

	streamsMu sync.Mutex
	streams   map[streamKey]*sharedStream

	config Config
	// mu guards all mutable backend state. config and base are fixed after
	// NewGateway (tests only mutate them between requests); services, programs,
	// updated, healthy, cooldowns and err are read and written under mu.
	// len(backends) is fixed. Catalog objects handed out by snapshot are read-only.
	mu                          sync.RWMutex
	syncMu                      sync.Mutex
	backends                    []backend
	catalogClient, streamClient *http.Client
}

func number(o object, key string) int64 {
	n, ok := o[key].(json.Number)
	if !ok {
		return -1
	}
	v, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil {
		return -1
	}
	return v
}
func clone(o object) object {
	n := object{}
	for k, v := range o {
		n[k] = v
	}
	return n
}
func channelOf(o object) object {
	switch ch := o["channel"].(type) {
	case object:
		return ch
	case map[string]any:
		return object(ch)
	default:
		return nil
	}
}

// channelName maps an upstream physical channel name to the name this gateway exposes.
// "upstream" keeps Mirakurun's names so channel-based clients can map
// their own channel list; it is unsafe when backends reuse a name for different broadcasts.
func (g *Gateway) channelName(b BackendConfig, upstream string) string {
	if g.config.ChannelNames == "upstream" {
		return upstream
	}
	return b.ID + "-" + hex.EncodeToString([]byte(upstream))
}
func (g *Gateway) virtualChannel(b BackendConfig, ch object) object {
	n := clone(ch)
	delete(n, "services")
	n["channel"] = g.channelName(b, fmt.Sprint(ch["channel"]))
	return n
}
func serviceDomain(b BackendConfig) string {
	if b.ServiceDomain != "" {
		return "shared:" + b.ServiceDomain
	}
	return "backend:" + b.ID
}

// apiURL joins a backend API base with an escaped path, preserving any reverse-proxy
// prefix. RawPath keeps pre-escaped segments (including %2F) from being decoded.
func apiURL(base *url.URL, path, rawQuery string) string {
	u := *base
	escaped := strings.TrimRight(base.EscapedPath(), "/") + path
	if p, err := url.PathUnescape(escaped); err == nil {
		u.Path, u.RawPath = p, escaped
	} else {
		u.Path, u.RawPath = strings.TrimRight(base.Path, "/")+path, ""
	}
	u.RawQuery = rawQuery
	return u.String()
}

func (g *Gateway) fetch(ctx context.Context, b backend, path string) ([]object, error) {
	id := b.config.ID
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL(b.base, path, ""), nil)
	if err != nil {
		return nil, fmt.Errorf("GET %s%s: %w", id, path, err)
	}
	resp, err := g.catalogClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s%s: %w", id, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("GET %s%s: HTTP %d", id, path, resp.StatusCode)
	}
	const limit = 64 << 20
	raw, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("GET %s%s: reading body: %w", id, path, err)
	}
	if len(raw) > limit {
		return nil, fmt.Errorf("GET %s%s: body larger than 64 MiB", id, path)
	}
	var items []object
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&items); err != nil {
		return nil, fmt.Errorf("GET %s%s: must be a JSON array of objects: %w", id, path, err)
	}
	if items == nil {
		return nil, fmt.Errorf("GET %s%s: must be a JSON array, got null", id, path)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("GET %s%s: invalid trailing data after JSON array", id, path)
	}
	return items, nil
}
func validateCatalog(services, programs []object) error {
	known := map[int64]bool{}
	for _, s := range services {
		n, sid, id := number(s, "networkId"), number(s, "serviceId"), number(s, "id")
		ch := channelOf(s)
		typ, _ := ch["type"].(string)
		physical, _ := ch["channel"].(string)
		switch {
		case n < 0 || n > 65535 || sid < 0 || sid > 65535:
			return fmt.Errorf("service id=%d: networkId=%d/serviceId=%d out of range", id, n, sid)
		case id != n*100000+sid:
			return fmt.Errorf("service id=%d does not match networkId=%d serviceId=%d", id, n, sid)
		case known[id]:
			return fmt.Errorf("duplicate service id=%d", id)
		case physical == "":
			return fmt.Errorf("service id=%d: missing channel.channel", id)
		case typ != "GR" && typ != "BS" && typ != "CS" && typ != "SKY":
			return fmt.Errorf("service id=%d: unsupported channel.type %q", id, typ)
		}
		known[id] = true
	}
	seen := map[int64]bool{}
	for _, p := range programs {
		n, s, e, id := number(p, "networkId"), number(p, "serviceId"), number(p, "eventId"), number(p, "id")
		switch {
		case n < 0 || n > 65535 || s < 0 || s > 65535 || e < 0 || e > 65535:
			return fmt.Errorf("program id=%d: networkId=%d/serviceId=%d/eventId=%d out of range", id, n, s, e)
		case id != (n*100000+s)*100000+e:
			return fmt.Errorf("program id=%d does not match networkId=%d serviceId=%d eventId=%d", id, n, s, e)
		case seen[id]:
			return fmt.Errorf("duplicate program id=%d", id)
		}
		seen[id] = true
	}
	return nil
}

// Sync fetches every backend's services and programs, validates them together,
// and commits them only when both fetch and validation succeed. On failure the
// backend keeps its last validated catalog, is marked unhealthy, and serves
// nothing once that data is older than MaxStaleSeconds.
func (g *Gateway) Sync(ctx context.Context) {
	g.syncMu.Lock()
	defer g.syncMu.Unlock()
	var wg sync.WaitGroup
	for i := range g.backends {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g.mu.RLock()
			b := g.backends[i]
			g.mu.RUnlock()
			c, cancel := context.WithTimeout(ctx, time.Duration(g.config.CatalogTimeoutSeconds)*time.Second)
			defer cancel()
			s, err := g.fetch(c, b, "/services")
			var p []object
			if err == nil {
				p, err = g.fetch(c, b, "/programs")
			}
			if err == nil {
				err = validateCatalog(s, p)
			}
			// Only an explicit cancellation caused by shutdown is not a failure.
			if err != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return
			}
			g.mu.Lock()
			state := &g.backends[i]
			prev := state.err
			state.healthy = err == nil
			var message string
			if err != nil {
				state.err = err.Error()
				if prev != state.err {
					if state.updated.IsZero() {
						message = fmt.Sprintf("sync %s failed: %v", b.config.ID, err)
					} else {
						message = fmt.Sprintf("sync %s failed (serving data from %s): %v", b.config.ID, state.updated.Format(time.RFC3339), err)
					}
				}
			} else {
				if prev != "" {
					message = fmt.Sprintf("sync %s recovered", b.config.ID)
				}
				state.services, state.programs, state.updated, state.err = s, p, time.Now(), ""
			}
			g.mu.Unlock()
			if message != "" {
				log.Print(message)
			}
		}(i)
	}
	wg.Wait()
}

// Run refreshes backend catalogs every SyncSeconds until ctx is canceled.
func (g *Gateway) Run(ctx context.Context) {
	t := time.NewTicker(time.Duration(g.config.SyncSeconds) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.Sync(ctx)
		}
	}
}

// Called under mu. Responses depend only on local snapshots, never live backend requests.
func (g *Gateway) snapshot(now time.Time) catalog {
	c := catalog{services: []object{}, programs: []object{}, channels: []object{}, sources: map[int64][]source{}, channelSources: map[string][]source{}, conflicts: []int64{}}
	for i, b := range g.backends {
		if b.updated.IsZero() || now.Sub(b.updated) > time.Duration(g.config.MaxStaleSeconds)*time.Second {
			continue
		}
		for _, s := range b.services {
			id := number(s, "id")
			c.sources[id] = append(c.sources[id], source{i, s})
		}
	}
	entries := []channelEntry{}
	authority := map[int64]int{}
	seenChannelSource := map[string]bool{}
	for id, sources := range c.sources {
		domain := serviceDomain(g.backends[sources[0].backend].config)
		conflict := false
		for _, s := range sources {
			t, _ := channelOf(s.service)["type"].(string)
			t0, _ := channelOf(sources[0].service)["type"].(string)
			if serviceDomain(g.backends[s.backend].config) != domain || t != t0 {
				conflict = true
			}
		}
		if conflict {
			c.conflicts = append(c.conflicts, id)
			delete(c.sources, id)
			continue
		}
		g.sortSources(sources)
		// Every source's physical channel is a possible channel stream target.
		for _, src := range sources {
			upstream := channelOf(src.service)
			typ, upstreamName := fmt.Sprint(upstream["type"]), fmt.Sprint(upstream["channel"])
			key := typ + "/" + g.channelName(g.backends[src.backend].config, upstreamName)
			dedupe := fmt.Sprint(key, "\x00", src.backend, "\x00", typ, "\x00", upstreamName)
			if seenChannelSource[dedupe] {
				continue
			}
			seenChannelSource[dedupe] = true
			c.channelSources[key] = append(c.channelSources[key], src)
		}
		a := sources[0]
		authority[id] = a.backend
		s := clone(a.service)
		ch := g.virtualChannel(g.backends[a.backend].config, channelOf(s))
		s["channel"] = ch
		c.services = append(c.services, s)
		summary := clone(s)
		delete(summary, "channel")
		entries = append(entries, channelEntry{key: fmt.Sprint(ch["type"], "/", ch["channel"]), domain: serviceDomain(g.backends[a.backend].config), channel: ch, service: summary})
	}
	for i, b := range g.backends {
		for _, p := range b.programs {
			id := number(p, "networkId")*100000 + number(p, "serviceId")
			if a, ok := authority[id]; ok && a == i {
				c.programs = append(c.programs, p)
			}
		}
	}
	// A shared channel name means one broadcast: configured priority (not health)
	// picks it, and only sources with the same identity domain may fail over.
	channelDomain := map[string]string{}
	for key, sources := range c.channelSources {
		best := sources[0]
		for _, s := range sources[1:] {
			a, b := g.backends[s.backend].config, g.backends[best.backend].config
			if a.Priority > b.Priority || (a.Priority == b.Priority && a.ID < b.ID) {
				best = s
			}
		}
		domain := serviceDomain(g.backends[best.backend].config)
		kept := sources[:0]
		for _, s := range sources {
			if serviceDomain(g.backends[s.backend].config) == domain {
				kept = append(kept, s)
			}
		}
		g.sortSources(kept)
		c.channelSources[key] = kept
		channelDomain[key] = domain
	}
	channels := map[string]object{}
	for _, e := range entries {
		if e.domain != channelDomain[e.key] {
			continue
		}
		if channels[e.key] == nil {
			channels[e.key] = clone(e.channel)
			channels[e.key]["services"] = []object{}
		}
		channels[e.key]["services"] = append(channels[e.key]["services"].([]object), e.service)
	}
	for _, ch := range channels {
		sortObjects(ch["services"].([]object))
		c.channels = append(c.channels, ch)
	}
	sortObjects(c.services)
	sortObjects(c.programs)
	sort.Slice(c.conflicts, func(i, j int) bool { return c.conflicts[i] < c.conflicts[j] })
	sort.Slice(c.channels, func(i, j int) bool {
		return fmt.Sprint(c.channels[i]["type"], c.channels[i]["channel"]) < fmt.Sprint(c.channels[j]["type"], c.channels[j]["channel"])
	})
	return c
}
func sortObjects(items []object) {
	sort.Slice(items, func(i, j int) bool { return number(items[i], "id") < number(items[j], "id") })
}

// sortSources orders redundant sources: healthy first, then configured priority, then ID.
func (g *Gateway) sortSources(sources []source) {
	sort.SliceStable(sources, func(i, j int) bool {
		a, b := g.backends[sources[i].backend], g.backends[sources[j].backend]
		if a.healthy != b.healthy {
			return a.healthy
		}
		if a.config.Priority != b.config.Priority {
			return a.config.Priority > b.config.Priority
		}
		return a.config.ID < b.config.ID
	})
}

func (g *Gateway) availableBackends() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	now := time.Now()
	maxStale := time.Duration(g.config.MaxStaleSeconds) * time.Second
	n := 0
	for i := range g.backends {
		if fresh, _ := g.backends[i].status(now, maxStale); fresh {
			n++
		}
	}
	return n
}
