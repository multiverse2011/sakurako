// Command mockmirakurun serves the subset of the Mirakurun API that Sakurako uses,
// so the gateway can be exercised without real tuners.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

type mockConfig struct {
	listen             string
	networkID          int64
	channelType        string
	channel            string
	services           int
	programsPerService int
	failCatalog        bool
	failStream         int
	streamDelay        time.Duration
	streamFile         string
	streamRate         int64
}

type channel struct {
	Type    string `json:"type"`
	Channel string `json:"channel"`
	Name    string `json:"name"`
}

type service struct {
	ID          int64   `json:"id"`
	ServiceID   int64   `json:"serviceId"`
	NetworkID   int64   `json:"networkId"`
	Type        int     `json:"type"`
	Name        string  `json:"name"`
	HasLogoData bool    `json:"hasLogoData"`
	Channel     channel `json:"channel"`
}

type extended struct {
	Heading string `json:"heading,omitempty"`
	Text    string `json:"text"`
}

type program struct {
	ID        int64      `json:"id"`
	EventID   int64      `json:"eventId"`
	ServiceID int64      `json:"serviceId"`
	NetworkID int64      `json:"networkId"`
	StartAt   int64      `json:"startAt"`
	Duration  int64      `json:"duration"`
	IsFree    bool       `json:"isFree"`
	Name      string     `json:"name"`
	Extended  []extended `json:"extended,omitempty"`
}

func (c mockConfig) validate() error {
	switch {
	case c.networkID < 0 || c.networkID > 65535:
		return fmt.Errorf("network-id must be 0..65535")
	case c.channelType != "GR" && c.channelType != "BS" && c.channelType != "CS" && c.channelType != "SKY":
		return fmt.Errorf("channel-type must be GR, BS, CS or SKY")
	case c.channel == "":
		return fmt.Errorf("channel must not be empty")
	case c.services < 1 || c.services > 1000:
		return fmt.Errorf("services must be 1..1000")
	case c.programsPerService < 0:
		return fmt.Errorf("programs must not be negative")
	case c.failStream != 0 && (c.failStream < 400 || c.failStream > 599):
		return fmt.Errorf("fail-stream must be 0 or a 400..599 HTTP status")
	case c.streamRate < 1:
		return fmt.Errorf("stream-rate must be positive")
	}
	if c.streamFile != "" {
		info, err := os.Stat(c.streamFile)
		if err != nil {
			return fmt.Errorf("stream-file: %w", err)
		}
		if !info.Mode().IsRegular() || info.Size() < 188 {
			return fmt.Errorf("stream-file must be a regular file with at least one 188-byte TS packet")
		}
	}
	return nil
}

func main() {
	var c mockConfig
	flag.StringVar(&c.listen, "listen", "127.0.0.1:40773", "HTTP listen address")
	flag.Int64Var(&c.networkID, "network-id", 32736, "network ID")
	flag.StringVar(&c.channelType, "channel-type", "GR", "channel type (GR, BS, CS, SKY)")
	flag.StringVar(&c.channel, "channel", "27", "physical channel")
	flag.IntVar(&c.services, "services", 2, "number of services")
	flag.IntVar(&c.programsPerService, "programs", 4, "programs per service")
	flag.BoolVar(&c.failCatalog, "fail-catalog", false, "return HTTP 503 for services and programs")
	flag.IntVar(&c.failStream, "fail-stream", 0, "return this HTTP status for streams instead of serving")
	flag.DurationVar(&c.streamDelay, "stream-delay", 0, "delay before stream response headers")
	flag.StringVar(&c.streamFile, "stream-file", "", "real TS file to loop as the stream instead of null packets")
	flag.Int64Var(&c.streamRate, "stream-rate", 2_000_000, "stream-file playback rate in bytes per second")
	flag.Parse()
	log.Printf("mock Mirakurun on %s (networkId=%d %s/%s, %d services)", c.listen, c.networkID, c.channelType, c.channel, c.services)
	log.Fatal(http.ListenAndServe(c.listen, newHandler(c)))
}

type mock struct {
	config     mockConfig
	services   []service
	programs   []program
	streamable map[int64]bool
}

func newHandler(c mockConfig) http.Handler {
	if err := c.validate(); err != nil {
		panic(err)
	}
	m := &mock{config: c, streamable: map[int64]bool{}}
	now := time.Now().Truncate(30 * time.Minute)
	for i := 0; i < c.services; i++ {
		sid := int64(1024 + i)
		id := c.networkID*100000 + sid
		name := fmt.Sprintf("Mock %d サービス%d", c.networkID, i+1)
		m.services = append(m.services, service{
			ID: id, ServiceID: sid, NetworkID: c.networkID, Type: 1, Name: name,
			Channel: channel{Type: c.channelType, Channel: c.channel, Name: fmt.Sprintf("Mock %d", c.networkID)},
		})
		m.streamable[id] = true
		for p := 0; p < c.programsPerService; p++ {
			eventID := int64(p + 1)
			m.programs = append(m.programs, program{
				ID: id*100000 + eventID, EventID: eventID, ServiceID: sid, NetworkID: c.networkID,
				StartAt: now.Add(time.Duration(p) * 30 * time.Minute).UnixMilli(), Duration: 30 * 60 * 1000,
				IsFree: true, Name: fmt.Sprintf("%s 番組%d", name, eventID),
				Extended: []extended{{Heading: "概要", Text: "mock program"}},
			})
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/services", m.catalogHandler(m.services))
	mux.HandleFunc("GET /api/programs", m.catalogHandler(m.programs))
	mux.HandleFunc("GET /api/services/{id}/stream", m.stream)
	return mux
}

func (m *mock) catalogHandler(v any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if m.config.failCatalog {
			http.Error(w, "mock catalog unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(v); err != nil {
			log.Printf("write catalog response: %v", err)
		}
	}
}

func (m *mock) stream(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || !m.streamable[id] {
		http.NotFound(w, r)
		return
	}
	if m.config.failStream != 0 {
		http.Error(w, "mock stream failure", m.config.failStream)
		return
	}
	if m.config.streamDelay > 0 {
		select {
		case <-time.After(m.config.streamDelay):
		case <-r.Context().Done():
			return
		}
	}
	log.Printf("stream id=%d decode=%q priority=%q", id, r.URL.Query().Get("decode"), r.Header.Get("X-Mirakurun-Priority"))
	w.Header().Set("Content-Type", "video/MP2T")
	w.Header().Set("X-Mirakurun-Tuner-User-ID", "mock-tuner")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	if m.config.streamFile != "" {
		m.playFile(w, r)
		return
	}
	// shortcut: null TS packets only, not decodable video; use stream-file for playback tests.
	packet := [188]byte{0x47, 0x1f, 0xff, 0x10}
	batch := bytes.Repeat(packet[:], 20)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := w.Write(batch); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		select {
		case <-ticker.C:
		case <-r.Context().Done():
			return
		}
	}
}

// playFile loops a recorded TS at the configured -stream-rate so clients see a live stream.
func (m *mock) playFile(w http.ResponseWriter, r *http.Request) {
	f, err := os.Open(m.config.streamFile)
	if err != nil {
		log.Printf("open stream-file: %v", err)
		panic(http.ErrAbortHandler)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		log.Printf("stat stream-file: %v", err)
		panic(http.ErrAbortHandler)
	}
	size := info.Size() - info.Size()%188 // Only whole TS packets; never replay a split one.
	if size == 0 {
		log.Printf("stream-file has no complete 188-byte packets")
		panic(http.ErrAbortHandler)
	}
	const interval = 20 * time.Millisecond
	perTick := m.config.streamRate / int64(time.Second/interval)
	perTick = perTick / 188 * 188
	if perTick < 188 {
		perTick = 188
	}
	buf := make([]byte, perTick)
	var offset int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if offset >= size {
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				log.Printf("seek stream-file: %v", err)
				panic(http.ErrAbortHandler)
			}
			offset = 0
		}
		chunk := int64(len(buf))
		if remaining := size - offset; chunk > remaining {
			chunk = remaining
		}
		n, err := io.ReadFull(f, buf[:chunk])
		if n > 0 {
			offset += int64(n)
			if _, err := w.Write(buf[:n]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("read stream-file: %v", err)
				panic(http.ErrAbortHandler)
			}
			// Loop the recording; timestamps restart at the file boundary.
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				log.Printf("seek stream-file: %v", err)
				panic(http.ErrAbortHandler)
			}
			offset = 0
		}
		select {
		case <-ticker.C:
		case <-r.Context().Done():
			return
		}
	}
}
