package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const subscriberQueueSize = 16 // At most 512 KiB of pending chunks per subscriber.
type streamKey struct {
	id                                    int64  // service stream
	channel                               string // exposed channel name; empty for service streams
	domain, channelType, decode, priority string
}

func (k streamKey) label() string {
	if k.channel != "" {
		return "channel " + k.channelType + "/" + k.channel
	}
	return "service " + strconv.FormatInt(k.id, 10)
}

type subscriber struct {
	id                string
	remote, userAgent string
	joined            time.Time
	bytes             atomic.Int64

	chunks  chan []byte
	dropped chan struct{}
	skip    int
}
type sharedStream struct {
	id          string
	created     time.Time
	started     time.Time
	backendID   string
	serviceName string

	g           *Gateway
	key         streamKey
	cancel      context.CancelFunc
	mu          sync.Mutex
	subscribers map[*subscriber]bool
	header      http.Header
	ready       chan struct{}
	status      int
	offset      int64
	// droppedSlow records that the last subscriber was dropped for falling
	// behind, so the closing log names that cause instead of a client error.
	droppedSlow bool
}

// endText names why the upstream was canceled: a slow subscriber was dropped,
// or every subscriber left on its own.
func (s *sharedStream) endText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.droppedSlow {
		return "dropped (slow subscriber)"
	}
	return fmt.Sprintf("ended after %s, %s", formatLogDuration(time.Since(s.started)), formatLogBytes(s.offset))
}

func formatLogDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h, m, s := int64(d/time.Hour), int64(d/time.Minute%60), int64(d/time.Second%60)
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	return fmt.Sprintf("%dm%02ds", m, s)
}

// formatLogBytes uses the same binary units and one decimal as the UI.
func formatLogBytes(n int64) string {
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	v, i := float64(n)/1024, 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

func newSubscriber() *subscriber {
	return &subscriber{chunks: make(chan []byte, subscriberQueueSize), dropped: make(chan struct{})}
}

func newSharedStream(g *Gateway, id string, key streamKey, serviceName string, cancel context.CancelFunc) *sharedStream {
	return &sharedStream{
		id: id, created: time.Now(), serviceName: serviceName,
		g: g, key: key, cancel: cancel,
		subscribers: make(map[*subscriber]bool),
		header:      make(http.Header),
		ready:       make(chan struct{}),
	}
}

func (g *Gateway) sharedStream(w http.ResponseWriter, r *http.Request, id int64, sources []source, opts streamOptions) {
	name, _ := sources[0].service["name"].(string)
	key := streamKey{id: id, domain: serviceDomain(g.backends[sources[0].backend].config), channelType: fmtChannelType(sources[0]), decode: opts.decode, priority: opts.priority}
	g.openSharedStream(w, r, key, name, sources, opts)
}

func (g *Gateway) sharedChannelStream(w http.ResponseWriter, r *http.Request, typ, name string, sources []source, opts streamOptions) {
	key := streamKey{domain: serviceDomain(g.backends[sources[0].backend].config), channelType: typ, channel: name, decode: opts.decode, priority: opts.priority}
	g.openSharedStream(w, r, key, name, sources, opts)
}

func (g *Gateway) openSharedStream(w http.ResponseWriter, r *http.Request, key streamKey, name string, sources []source, opts streamOptions) {
	sub := newSubscriber()
	g.streamsMu.Lock()
	g.nextConnectionID++
	sub.id = "c" + strconv.FormatUint(g.nextConnectionID, 10)
	sub.joined = time.Now()
	sub.remote = r.RemoteAddr
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		sub.remote = host
	}
	sub.userAgent = r.UserAgent()
	if len(sub.userAgent) > 128 {
		sub.userAgent = strings.ToValidUTF8(sub.userAgent[:128], "")
	}
	if g.streams == nil {
		g.streams = make(map[streamKey]*sharedStream)
	}
	shared := g.streams[key]
	if shared != nil {
		shared.mu.Lock()
		if len(shared.subscribers) == 0 || (shared.status != 0 && shared.status != 200) {
			shared.mu.Unlock()
			shared = nil
		}
	}
	// A lower-priority request may ride an existing higher-priority stream: the
	// upstream allocation stays at least as strong as it asked for. Called with
	// streamsMu held, so a selected stream cannot finish under us.
	if shared == nil {
		shared = g.shareableStreamLocked(key)
	}
	if shared == nil {
		ctx, cancel := context.WithCancel(context.Background())
		shared = newSharedStream(g, "s"+strconv.FormatUint(g.nextConnectionID, 10), key, name, cancel)
		g.streams[key] = shared
		// upstreamStream builds the upstream request itself and only needs the context.
		req := r.WithContext(ctx)
		shared.subscribers[sub] = true
		go shared.run(req, sources, opts)
	} else {
		if want, ok := priorityValue(key.priority); ok {
			if have, haveOK := priorityValue(shared.key.priority); !haveOK || want > have {
				// The upstream allocation was fixed when this stream started.
				log.Printf("%s: joining the existing stream at priority %q (requested %q)", key.label(), shared.key.priority, key.priority)
			}
		}
		sub.skip = int((188 - shared.offset%188) % 188)
		shared.subscribers[sub] = true
		shared.mu.Unlock()
	}
	g.streamsMu.Unlock()
	stop := context.AfterFunc(r.Context(), func() { shared.unsubscribe(sub) })
	defer stop()
	defer shared.unsubscribe(sub)
	select {
	case <-r.Context().Done():
		return
	case <-shared.ready:
	}
	shared.mu.Lock()
	status, header := shared.status, shared.header.Clone()
	shared.mu.Unlock()
	for k, vv := range header {
		w.Header()[k] = vv
	}
	controller := http.NewResponseController(w)
	defer controller.SetWriteDeadline(time.Time{})
	live := status == 200
	timeout := time.Duration(g.config.StreamTimeoutSeconds) * time.Second
	warned := false
	for {
		select {
		case <-r.Context().Done():
			panic(http.ErrAbortHandler)
		case <-sub.dropped:
			panic(http.ErrAbortHandler)
		case chunk, ok := <-sub.chunks:
			if !ok {
				if status != 0 {
					w.WriteHeader(status)
				}
				if live {
					panic(http.ErrAbortHandler)
				}
				return
			}
			if err := controller.SetWriteDeadline(time.Now().Add(timeout)); err != nil && !warned {
				warned = true
				log.Printf("write deadline unsupported, slow clients are not bounded: %v", err)
			}
			if status != 0 {
				w.WriteHeader(status)
				status = 0
			}
			n, err := w.Write(chunk)
			sub.bytes.Add(int64(n))
			if live {
				g.downstreamBytes.Add(int64(n))
			}
			if err != nil {
				log.Printf("stream %s (%s): subscriber %s write failed: %v", shared.id, shared.key.label(), sub.id, err)
				panic(http.ErrAbortHandler)
			}
			if err := controller.Flush(); err != nil {
				log.Printf("stream %s (%s): subscriber %s flush failed: %v", shared.id, shared.key.label(), sub.id, err)
				panic(http.ErrAbortHandler)
			}
		}
	}
}

// shareableStreamLocked finds an existing stream for the same broadcast and decode.
// Priority is a property of the shared upstream connection, not of its identity: the
// first request's allocation is kept and later requests join it. Among candidates,
// the strongest allocation wins so new subscribers are as protected as possible.
// Called with streamsMu held.
func (g *Gateway) shareableStreamLocked(key streamKey) *sharedStream {
	var best *sharedStream
	var bestPriority int64
	var bestValid bool
	for k, s := range g.streams {
		// A decode=0 request may ride an already decoded stream (extra decrypted
		// bytes are harmless). A decode=1 request must not join an undecoded one.
		sameDecode := k.decode == key.decode
		safeDecode := key.decode == "0" && k.decode == "1"
		if k.id != key.id || k.channel != key.channel || k.domain != key.domain || k.channelType != key.channelType || (!sameDecode && !safeDecode) {
			continue
		}
		priority, valid := priorityValue(k.priority)
		switch {
		case best == nil:
			best, bestPriority, bestValid = s, priority, valid
		case valid != bestValid && valid:
			best, bestPriority, bestValid = s, priority, valid
		case valid == bestValid && valid && priority > bestPriority:
			best, bestPriority, bestValid = s, priority, valid
		case valid == bestValid && (!valid || priority == bestPriority) && s.id < best.id:
			best = s
		}
	}
	if best == nil {
		return nil
	}
	// Return it locked so the caller can atomically add the subscriber.
	best.mu.Lock()
	if len(best.subscribers) == 0 || (best.status != 0 && best.status != 200) {
		best.mu.Unlock()
		return nil
	}
	return best
}

// priorityValue parses an explicit X-Mirakurun-Priority; the second result is false
// when the request omitted it.
func priorityValue(p string) (int64, bool) {
	if p == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(p, 10, 32)
	return n, err == nil
}

func fmtChannelType(s source) string { v, _ := channelOf(s.service)["type"].(string); return v }
func (s *sharedStream) unsubscribe(sub *subscriber) {
	s.g.streamsMu.Lock()
	defer s.g.streamsMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscribers, sub)
	if len(s.subscribers) == 0 {
		if s.g.streams[s.key] == s {
			delete(s.g.streams, s.key)
		}
		s.cancel()
	}
}
func (s *sharedStream) run(r *http.Request, sources []source, opts streamOptions) {
	defer func() {
		value := recover()
		s.g.streamsMu.Lock()
		s.mu.Lock()
		if s.status == 0 {
			s.status = 503
			close(s.ready)
		}
		for sub := range s.subscribers {
			close(sub.chunks)
		}
		if s.g.streams[s.key] == s {
			delete(s.g.streams, s.key)
		}
		s.mu.Unlock()
		s.g.streamsMu.Unlock()
		s.cancel()
		if value != nil && value != http.ErrAbortHandler {
			panic(value)
		}
	}()
	s.g.upstreamStream(s, r, sources, opts)
}

// upstreamPath maps a source to its backend's stream endpoint. The physical channel
// can differ per backend, so it is resolved from the source's own service object.
func (s *sharedStream) upstreamPath(src source) string {
	if s.key.channel == "" {
		return "/services/" + strconv.FormatInt(s.key.id, 10) + "/stream"
	}
	ch := channelOf(src.service)
	return "/channels/" + url.PathEscape(fmt.Sprint(ch["type"])) + "/" + url.PathEscape(fmt.Sprint(ch["channel"])) + "/stream"
}

// Header returns the live map without the mutex: headers are only written before
// ready closes, and readers clone after it.
func (s *sharedStream) Header() http.Header { return s.header }
func (s *sharedStream) WriteHeader(status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.status == 0 {
		s.status = status
		close(s.ready)
	}
}
func (s *sharedStream) Write(p []byte) (int, error) {
	s.WriteHeader(http.StatusOK)
	chunk := append([]byte(nil), p...)
	s.mu.Lock()
	s.offset += int64(len(p))
	var dropped []*subscriber
	for sub := range s.subscribers {
		data := chunk
		if s.status == http.StatusOK && sub.skip > 0 {
			n := min(sub.skip, len(data))
			sub.skip -= n
			data = data[n:]
			if len(data) == 0 {
				continue
			}
		}
		select {
		case sub.chunks <- data:
		default:
			dropped = append(dropped, sub)
			close(sub.dropped)
			delete(s.subscribers, sub)
		}
	}
	empty := len(s.subscribers) == 0
	// Only this call's drop explains an empty map; an unsubscribe may have
	// removed the last subscriber just before this Write took the lock.
	if empty && len(dropped) > 0 {
		s.droppedSlow = true
	}
	s.mu.Unlock()
	for _, sub := range dropped {
		log.Printf("stream %s (%s): dropping slow subscriber %s (%s) after %d bytes", s.id, s.key.label(), sub.id, sub.remote, sub.bytes.Load())
	}
	if empty {
		s.cancel()
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
func (s *sharedStream) Flush()                           {}
func (s *sharedStream) SetWriteDeadline(time.Time) error { return nil }

func (s *sharedStream) selectBackend(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.backendID = id
	s.started = time.Now()
}
