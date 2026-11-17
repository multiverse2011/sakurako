package main

import (
	"strings"
	"sync"
)

const logLimit = 500

type logEntry struct {
	Seq  int64  `json:"seq"`
	Text string `json:"text"`
}

// logRing keeps the newest log lines for the UI. log.Logger writes one line per call.
type logRing struct {
	mu      sync.Mutex
	entries []logEntry
	next    int64
}

var recentLogs = &logRing{}

func (r *logRing) Write(p []byte) (int, error) {
	if text := strings.TrimRight(string(p), "\n"); text != "" {
		r.mu.Lock()
		r.next++
		r.entries = append(r.entries, logEntry{Seq: r.next, Text: text})
		if len(r.entries) > logLimit {
			r.entries = r.entries[len(r.entries)-logLimit:]
		}
		r.mu.Unlock()
	}
	return len(p), nil
}

func (r *logRing) since(after int64) (entries []logEntry, next int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entries = []logEntry{}
	for _, entry := range r.entries {
		if entry.Seq > after {
			entries = append(entries, entry)
		}
	}
	return entries, r.next
}
