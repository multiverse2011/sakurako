package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type idleConn struct {
	net.Conn
	timeout time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

// gatewaySeq separates gateways created within the same clock tick.
var gatewaySeq atomic.Uint64

// NewGateway validates c and parses each backend URL once into a Gateway.
func NewGateway(c Config) (*Gateway, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	timeout := time.Duration(c.StreamTimeoutSeconds) * time.Second
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = timeout
	transport.DisableCompression = true
	// Per-connection idle deadlines require a dedicated HTTP/1 connection per live stream.
	transport.ForceAttemptHTTP2 = false
	transport.MaxResponseHeaderBytes = 64 << 10
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &idleConn{conn, timeout}, nil
	}
	// Redirects could change backend identity or leak stream options to another host.
	noRedirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	// Event subscriptions are idle for minutes at a time, so they cannot use the
	// stream transport's per-read idle deadline; only the wait for response
	// headers is bounded.
	eventsTransport := http.DefaultTransport.(*http.Transport).Clone()
	eventsTransport.ResponseHeaderTimeout = timeout
	eventsTransport.DisableCompression = true
	eventsTransport.MaxResponseHeaderBytes = 64 << 10
	g := &Gateway{config: c, catalogClient: &http.Client{Timeout: time.Duration(c.CatalogTimeoutSeconds) * time.Second, CheckRedirect: noRedirect}, streamClient: &http.Client{Transport: transport, CheckRedirect: noRedirect}, eventsClient: &http.Client{Transport: eventsTransport, CheckRedirect: noRedirect}}
	// Copy so later changes to the caller's slice cannot desync the gateway.
	g.config.Backends = append([]BackendConfig(nil), c.Backends...)
	for _, b := range g.config.Backends {
		base, err := url.Parse(b.URL)
		if err != nil {
			return nil, fmt.Errorf("backend %s URL: %w", b.ID, err)
		}
		g.backends = append(g.backends, backend{config: b, base: base, blockedServices: map[int64]time.Time{}})
	}
	// Start the generation counter at the process's clock, so an ETag from a
	// previous run can never match this one.
	g.gen = uint64(time.Now().UnixNano()) + gatewaySeq.Add(1)
	g.cat = g.buildCatalog(time.Now()) // empty until the first sync
	return g, nil
}
func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logWriteError(err)
	}
}

// logWriteError reports a failed response write; a client that simply went away
// is not an error.
func logWriteError(err error) {
	if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, context.Canceled) {
		return
	}
	log.Printf("response write failed: %v", err)
}
func apiError(w http.ResponseWriter, status int, reason string) {
	if status >= 500 {
		log.Printf("HTTP %d: %s", status, reason)
	}
	respond(w, status, object{"code": status, "reason": reason})
}

// minGzipBytes is where compression starts to be attempted: a body this small
// cannot shrink enough to pay for the attempt, and shipping it uncompressed
// keeps the response a straight copy.
const minGzipBytes = 1 << 10

// serveCatalog writes a catalog response with an ETag taken from the snapshot
// generation, the request URI and the requested representation (gzip or
// identity, decided from Accept-Encoding), a 304 when it still matches, and
// gzip when the client accepts it and compression pays off. cached, when set,
// is the snapshot's encoded unfiltered program list, which must not be
// re-encoded per request.
func serveCatalog(w http.ResponseWriter, r *http.Request, gen uint64, v any, cached *jsonBody) {
	// The encoding follows from the request headers alone, so a 304 still needs
	// no body. Each representation gets its own ETag, as the UI assets do.
	variant := "identity"
	if acceptsGzip(r.Header.Get("Accept-Encoding")) {
		variant = "gzip"
	}
	etag := contentETag([]byte(fmt.Sprint(gen, " ", variant, " ", r.URL.RequestURI())))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", etag)
	w.Header().Set("Vary", "Accept-Encoding")
	if etagMatches(r.Header.Get("If-None-Match"), etag) {
		// A match needs no encoding and no compression.
		w.WriteHeader(http.StatusNotModified)
		return
	}
	wantGzip := variant == "gzip"
	var plain, gz []byte
	var err error
	if cached != nil {
		plain, gz, err = cached.body(v)
	} else if plain, err = encodeJSON(v); err == nil && wantGzip && len(plain) >= minGzipBytes {
		// Only compress when the result is actually smaller, as the UI assets do.
		if z := gzipBytes(plain); len(z) < len(plain) {
			gz = z
		}
	}
	if err != nil {
		log.Printf("catalog response encoding failed: %v", err)
		apiError(w, 500, "response encoding failed")
		return
	}
	data := plain
	if gz != nil && wantGzip {
		data = gz
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	if r.Method == http.MethodHead {
		w.WriteHeader(200)
		return
	}
	if _, err := w.Write(data); err != nil {
		logWriteError(err)
	}
}

// ServeHTTP routes catalog, stream, health, UI and OpenAPI requests.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		w.Header().Set("Allow", "GET, HEAD")
		apiError(w, 405, "method not supported")
		return
	}
	if _, err := url.ParseQuery(r.URL.RawQuery); err != nil {
		apiError(w, 400, "malformed query")
		return
	}
	if g.serveUI(w, r) {
		return
	}
	if r.URL.Path == "/healthz" {
		g.health(w)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "api" {
		apiError(w, 404, "not found")
		return
	}
	// Live endpoints answer from the backends, not the catalog snapshot.
	if parts[1] == "tuners" {
		if len(parts) != 2 {
			apiError(w, 404, "not found")
			return
		}
		if r.URL.RawQuery != "" {
			apiError(w, 400, "query options are not supported here")
			return
		}
		g.tuners(w, r)
		return
	}
	if parts[1] == "events" {
		if len(parts) != 3 || parts[2] != "stream" {
			apiError(w, 404, "not found")
			return
		}
		g.events(w, r)
		return
	}
	// Reject unknown endpoints before reading the catalog.
	switch parts[1] {
	case "services", "programs", "channels":
	default:
		apiError(w, 404, "endpoint not supported")
		return
	}
	cat := g.catalogNow(time.Now())
	if len(parts) == 4 && parts[1] == "services" && parts[3] == "logo" {
		id, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			apiError(w, 400, "invalid service id")
			return
		}
		if r.URL.RawQuery != "" {
			apiError(w, 400, "query options are not supported here")
			return
		}
		sources := cat.sources[id]
		if len(sources) == 0 {
			for _, c := range cat.conflicts {
				if c == id {
					apiError(w, 409, "service identity conflict between backends")
					return
				}
			}
			apiError(w, 404, "service not found")
			return
		}
		g.logo(w, r, id, sources)
		return
	}
	if len(parts) == 5 && parts[1] == "channels" && parts[4] == "stream" {
		typ, name := parts[2], parts[3]
		sources := cat.channelSources[typ+"/"+name]
		if len(sources) == 0 {
			apiError(w, 404, "channel not found")
			return
		}
		g.channelStream(w, r, typ, name, sources)
		return
	}
	if len(parts) == 4 && parts[1] == "services" && parts[3] == "stream" {
		id, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			apiError(w, 400, "invalid service id")
			return
		}
		sources := cat.sources[id]
		if len(sources) == 0 {
			for _, c := range cat.conflicts {
				if c == id {
					apiError(w, 409, "service identity conflict between backends")
					return
				}
			}
			apiError(w, 404, "service not found")
			return
		}
		g.stream(w, r, id, sources)
		return
	}
	var items []object
	switch parts[1] {
	case "services":
		items = cat.services
	case "programs":
		items = cat.programs
	case "channels":
		items = cat.channels
	default:
		apiError(w, 404, "endpoint not supported")
		return
	}
	if parts[1] == "programs" {
		for key, vals := range r.URL.Query() {
			if (key != "networkId" && key != "serviceId" && key != "eventId") || len(vals) != 1 {
				apiError(w, 400, "only numeric networkId/serviceId/eventId filters are supported")
				return
			}
			value, err := strconv.ParseInt(vals[0], 10, 64)
			if err != nil || value < 0 || value > 65535 {
				apiError(w, 400, "invalid program filter")
				return
			}
			filtered := []object{}
			for _, p := range items {
				if number(p, key) == value {
					filtered = append(filtered, p)
				}
			}
			items = filtered
		}
	} else if r.URL.RawQuery != "" {
		apiError(w, 400, "query options are not supported here")
		return
	}
	if len(parts) == 2 {
		var cached *jsonBody
		if parts[1] == "programs" && r.URL.RawQuery == "" {
			cached = cat.programsJSON
		}
		serveCatalog(w, r, cat.gen, items, cached)
		return
	}
	if parts[1] == "channels" {
		apiError(w, 404, "channel detail is not supported")
		return
	}
	if len(parts) == 3 {
		id, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			apiError(w, 400, "invalid id")
			return
		}
		for _, item := range items {
			if number(item, "id") == id {
				serveCatalog(w, r, cat.gen, item, nil)
				return
			}
		}
		if parts[1] == "services" {
			for _, c := range cat.conflicts {
				if c == id {
					apiError(w, 409, "service identity conflict between backends")
					return
				}
			}
		}
	}
	apiError(w, 404, "not found")
}

// nilIfZero renders zero timestamps as JSON null instead of the year-1 time.
func nilIfZero(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nilIfExpired(now, t time.Time) any {
	if t.IsZero() || !now.Before(t) {
		return nil
	}
	return t
}

func (g *Gateway) health(w http.ResponseWriter) {
	now := time.Now()
	cat := g.catalogNow(now)
	g.mu.RLock()
	maxStale := time.Duration(g.config.MaxStaleSeconds) * time.Second
	states := []object{}
	available := 0
	for i := range g.backends {
		b := &g.backends[i]
		fresh, healthy := b.status(now, maxStale)
		usable := healthy && !now.Before(b.blockedUntil)
		if usable {
			available++
		}
		// Never leak upstream URLs or internal addresses; details stay in logs.
		var errCategory any
		if b.err != "" {
			errCategory = "catalog sync failed"
		}
		states = append(states, object{"id": b.config.ID, "healthy": usable, "catalog_available": fresh, "last_success": nilIfZero(b.updated), "error": errCategory, "stream_retry_after": nilIfExpired(now, b.blockedUntil)})
	}
	status := 200
	if available == 0 || len(cat.conflicts) > 0 || len(cat.channelConflicts) > 0 {
		status = 503
	}
	channelConflicts := make([]string, 0, len(cat.channelConflicts))
	for _, conflict := range cat.channelConflicts {
		channelConflicts = append(channelConflicts, conflict.key)
	}
	g.mu.RUnlock()
	respond(w, status, object{"ready": status == 200, "degraded": available < len(g.backends), "backends": states, "conflicting_service_ids": cat.conflicts, "conflicting_channels": channelConflicts})
}

// streamOptions is the validated per-request stream identity, parsed once.
type streamOptions struct {
	decode      string
	priority    string // normalized; empty when omitted
	hasPriority bool
}

func parseStreamOptions(w http.ResponseWriter, r *http.Request) (streamOptions, bool) {
	opts := streamOptions{decode: r.URL.Query().Get("decode")}
	for key, vals := range r.URL.Query() {
		if key != "decode" || len(vals) != 1 || (vals[0] != "0" && vals[0] != "1") {
			apiError(w, 400, "only decode=0 or decode=1 is supported")
			return opts, false
		}
	}
	if vals := r.Header.Values("X-Mirakurun-Priority"); len(vals) > 0 {
		n, err := strconv.ParseInt(vals[0], 10, 32)
		if err != nil || n < 0 || len(vals) != 1 {
			apiError(w, 400, "invalid X-Mirakurun-Priority")
			return opts, false
		}
		opts.priority, opts.hasPriority = strconv.FormatInt(n, 10), true
	}
	return opts, true
}

func (g *Gateway) stream(w http.ResponseWriter, r *http.Request, id int64, sources []source) {
	opts, ok := parseStreamOptions(w, r)
	if !ok {
		return
	}
	if r.Method == "HEAD" {
		// HEAD allocates no tuner, but must not claim a candidate when every source is down.
		g.mu.RLock()
		candidates, _ := g.attemptSources(sources, streamKey{id: id}, time.Now())
		g.mu.RUnlock()
		if len(candidates) == 0 {
			apiError(w, 503, "no healthy source could start the stream")
			return
		}
		w.Header().Set("Content-Type", "video/MP2T")
		w.WriteHeader(200)
		return
	}
	g.sharedStream(w, r, id, sources, opts)
}

// channelStream proxies a whole-transponder stream, which is what Mirakurun clients
// use for tuning, EPG and scans.
func (g *Gateway) channelStream(w http.ResponseWriter, r *http.Request, typ, name string, sources []source) {
	opts, ok := parseStreamOptions(w, r)
	if !ok {
		return
	}
	if r.Method == "HEAD" {
		g.mu.RLock()
		key := streamKey{channel: name, channelType: typ}
		candidates, _ := g.attemptSources(sources, key, time.Now())
		g.mu.RUnlock()
		if len(candidates) == 0 {
			apiError(w, 503, "no healthy source could start the stream")
			return
		}
		w.Header().Set("Content-Type", "video/MP2T")
		w.WriteHeader(200)
		return
	}
	g.sharedChannelStream(w, r, typ, name, sources, opts)
}

func (g *Gateway) upstreamStream(w *sharedStream, r *http.Request, sources []source, opts streamOptions) {
	timeout := time.Duration(g.config.StreamTimeoutSeconds) * time.Second
	// Decide the candidates once per request: the rescue applies only when no
	// source was usable at the start, not when the last one fails mid-request.
	g.mu.RLock()
	candidates, rescue := g.attemptSources(sources, w.key, time.Now())
	g.mu.RUnlock()
	for _, s := range candidates {
		if r.Context().Err() != nil {
			return
		}
		// A normal candidate may have been cooled down by another stream while an
		// earlier source was being tried. A rescue candidate is cooling by design.
		now := time.Now()
		g.mu.RLock()
		ok := g.catalogHealthy(s.backend, now)
		if !rescue {
			ok = ok && g.keyAvailable(s.backend, w.key, now)
		}
		g.mu.RUnlock()
		if !ok {
			continue
		}
		cfg := g.backends[s.backend].config
		ctx, cancel := context.WithCancel(r.Context())
		// Bounds connection and response headers; idleConn.Read bounds the first body
		// byte separately. There is no lifetime limit on a healthy stream.
		timer := time.AfterFunc(timeout, cancel)
		u := apiURL(g.backends[s.backend].base, w.upstreamPath(s), r.URL.RawQuery)
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			timer.Stop()
			cancel()
			log.Printf("%s via %s: invalid upstream request: %v", w.key.label(), cfg.ID, err)
			apiError(w, 502, "invalid upstream request")
			return
		}
		if opts.hasPriority {
			req.Header.Set("X-Mirakurun-Priority", opts.priority)
		}
		req.Header.Set("User-Agent", "Sakurako")
		resp, err := g.streamClient.Do(req)
		if !timer.Stop() {
			// The timer fired while the request was in flight and canceled ctx.
			if resp != nil {
				resp.Body.Close()
			}
			cancel()
			if r.Context().Err() != nil {
				return
			}
			g.blockKey(s.backend, w.key, true, "no stream data: first-byte timeout")
			continue
		}
		if err == nil && resp.StatusCode != 200 {
			status := resp.StatusCode
			resp.Body.Close()
			cancel()
			// Tuner shortage (503) and similar codes are per-service; plain 5xx is backend-wide.
			serviceOnly := status == 404 || status == 408 || status == 429 || status == 503
			if serviceOnly || status >= 500 {
				g.blockKey(s.backend, w.key, serviceOnly, fmt.Sprintf("upstream HTTP %d", status))
				continue
			}
			log.Printf("%s via %s: upstream refused with HTTP %d", w.key.label(), cfg.ID, status)
			apiError(w, 502, fmt.Sprintf("upstream refused stream (HTTP %d)", status))
			return
		}
		buf := make([]byte, 32*1024)
		n := 0
		if err == nil {
			n, err = resp.Body.Read(buf)
		}
		if n == 0 {
			if resp != nil {
				resp.Body.Close()
			}
			cancel()
			if r.Context().Err() != nil {
				return
			}
			if err == nil {
				err = io.ErrNoProgress
			}
			g.blockKey(s.backend, w.key, true, "no stream data: "+err.Error())
			continue
		}
		w.selectBackend(cfg.ID)
		// This is the commit point: never switch broadcasts after any body byte is sent.
		w.Header().Set("Content-Type", "video/MP2T")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Cache-Control", "no-store")
		if v := resp.Header.Get("X-Mirakurun-Tuner-User-ID"); v != "" {
			w.Header().Set("X-Mirakurun-Tuner-User-ID", v)
		}
		controller := http.NewResponseController(w)
		var cause error
		clientGone := false
		for {
			if _, werr := w.Write(buf[:n]); werr != nil {
				cause, clientGone = werr, true
				break
			}
			if ferr := controller.Flush(); ferr != nil {
				cause, clientGone = ferr, true
				break
			}
			if err != nil {
				cause = err
				break
			}
			n, err = resp.Body.Read(buf)
			if n == 0 {
				cause = err
				break
			}
		}
		resp.Body.Close()
		cancel()
		if cause == nil {
			cause = io.ErrNoProgress
		}
		if r.Context().Err() != nil {
			clientGone = true // client disconnect surfaces as a canceled upstream read
		}
		if clientGone {
			log.Printf("%s via %s: client disconnected: %v", w.key.label(), cfg.ID, cause)
		} else {
			log.Printf("%s via %s aborted: %v", w.key.label(), cfg.ID, cause)
		}
		// Only an upstream read error is the backend's fault; EOF and client disconnects are not.
		if !clientGone && cause != io.EOF && r.Context().Err() == nil {
			g.blockKey(s.backend, w.key, false, "mid-stream failure: "+cause.Error())
		}
		// A truncated live stream must not look like a successfully completed recording.
		panic(http.ErrAbortHandler)
	}
	apiError(w, 503, "no healthy source could start the stream")
}

// usable reports whether a source is healthy and outside its stream cooldown.
// This is the normal candidate test; attemptSources may still try a cooling
// source when nothing else is available. Called under mu.
func (g *Gateway) usable(index int, key streamKey, now time.Time) bool {
	return g.catalogHealthy(index, now) && g.keyAvailable(index, key, now)
}

// catalogHealthy reports whether a source's catalog is fresh and healthy enough
// to stream from. Called under mu.
func (g *Gateway) catalogHealthy(index int, now time.Time) bool {
	_, healthy := g.backends[index].status(now, time.Duration(g.config.MaxStaleSeconds)*time.Second)
	return healthy
}

// keyAvailable reports whether a service or channel stream is outside its
// cooldown. Called under mu.
func (g *Gateway) keyAvailable(index int, key streamKey, now time.Time) bool {
	b := &g.backends[index]
	if now.Before(b.blockedUntil) {
		return false
	}
	if key.channel != "" {
		return !now.Before(b.blockedChannels[key.channelType+"/"+key.channel])
	}
	return !now.Before(b.blockedServices[key.id])
}

// attemptSources picks the sources a stream request may try and reports whether
// they are only cooling down (rescue attempts). Normally it returns the sources
// outside their cooldown; when there are none, it returns the healthy sources
// that are only cooling down, so a sole upstream that recovered is not locked out
// for the whole cooldown window. An unhealthy source (stale or failed catalog) is
// never returned. Called under mu.
func (g *Gateway) attemptSources(sources []source, key streamKey, now time.Time) ([]source, bool) {
	var ready, cooling []source
	for _, s := range sources {
		switch {
		case !g.catalogHealthy(s.backend, now):
		case g.keyAvailable(s.backend, key, now):
			ready = append(ready, s)
		default:
			cooling = append(cooling, s)
		}
	}
	if len(ready) > 0 {
		return ready, false
	}
	return cooling, len(cooling) > 0
}

// blockKey cools a backend (or only one service/channel of it) down for 10 seconds.
func (g *Gateway) blockKey(index int, key streamKey, serviceOnly bool, reason string) {
	g.mu.Lock()
	b := &g.backends[index]
	until := time.Now().Add(10 * time.Second)
	if serviceOnly && key.channel != "" {
		if b.blockedChannels == nil {
			b.blockedChannels = map[string]time.Time{}
		}
		b.blockedChannels[key.channelType+"/"+key.channel] = until
	} else if serviceOnly {
		if b.blockedServices == nil {
			b.blockedServices = map[int64]time.Time{}
		}
		b.blockedServices[key.id] = until
	} else {
		b.blockedUntil = until
	}
	backendID := b.config.ID
	g.mu.Unlock()
	log.Printf("%s via %s failed, cooling down 10s (service-only: %v): %s", key.label(), backendID, serviceOnly, reason)
}
