package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The benchmark catalog mirrors the measured deployment: two backends, 147
// services and 28,725 programs (about 600 bytes of JSON per program, i.e. the
// 17.7 MB /api/programs response recorded in issue #13).
func benchCatalog(b *testing.B) (*Gateway, *httptest.Server) {
	b.Helper()
	newBackend := func(networkID, services, programsPerService int64) *httptest.Server {
		svcs := []map[string]any{}
		progs := []map[string]any{}
		for i := int64(0); i < services; i++ {
			serviceID := 1024 + i
			id := networkID*100000 + serviceID
			svcs = append(svcs, map[string]any{
				"id": id, "serviceId": serviceID, "networkId": networkID, "type": 1,
				"name":        fmt.Sprintf("サービス %d 第%d", networkID, i+1),
				"hasLogoData": i%2 == 0,
				"channel":     map[string]any{"type": "GR", "channel": fmt.Sprint(10 + i%20), "name": fmt.Sprintf("チャンネル%d", i)},
			})
			for p := int64(0); p < programsPerService; p++ {
				eventID := p + 1
				progs = append(progs, map[string]any{
					"id": id*100000 + eventID, "eventId": eventID, "serviceId": serviceID, "networkId": networkID,
					"startAt": 1700000000000 + p*1800000, "duration": 1800000, "isFree": true,
					"name":        fmt.Sprintf("サンプル番組タイトル 第%d話", eventID),
					"description": strings.Repeat("番組の説明文です。", 30),
					"extended":    map[string]any{"detail": strings.Repeat("詳細な番組情報です。", 20)},
					"genres":      []any{map[string]any{"lv1": 7, "lv2": 0, "un1": 0, "un2": 0}},
				})
			}
		}
		servicesJSON, err := json.Marshal(svcs)
		if err != nil {
			b.Fatal(err)
		}
		programsJSON, err := json.Marshal(progs)
		if err != nil {
			b.Fatal(err)
		}
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/services":
				w.Write(servicesJSON)
			case "/api/programs":
				w.Write(programsJSON)
			default:
				http.NotFound(w, r)
			}
		}))
		b.Cleanup(s.Close)
		return s
	}
	a := newBackend(1, 74, 195)
	c := newBackend(2, 73, 195)
	g, err := NewGateway(Config{Listen: ":40772", SyncSeconds: 30, CatalogTimeoutSeconds: 10, MaxStaleSeconds: 3600, StreamTimeoutSeconds: 30, Backends: []BackendConfig{
		{ID: "a", URL: a.URL + "/api", Priority: 100},
		{ID: "b", URL: c.URL + "/api", Priority: 50},
	}})
	if err != nil {
		b.Fatal(err)
	}
	g.Sync(context.Background())
	s := httptest.NewServer(g)
	b.Cleanup(s.Close)
	b.ResetTimer()
	return g, s
}

func benchGet(b *testing.B, url string) {
	b.Helper()
	for i := 0; i < b.N; i++ {
		resp, err := http.Get(url)
		if err != nil {
			b.Fatal(err)
		}
		if resp.StatusCode != 200 {
			b.Fatalf("HTTP %d", resp.StatusCode)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

func BenchmarkUIState(b *testing.B) {
	_, s := benchCatalog(b)
	benchGet(b, s.URL+"/-/ui/state")
}

func BenchmarkHealth(b *testing.B) {
	_, s := benchCatalog(b)
	benchGet(b, s.URL+"/healthz")
}

func BenchmarkServicesAll(b *testing.B) {
	_, s := benchCatalog(b)
	benchGet(b, s.URL+"/api/services")
}

func BenchmarkServiceOne(b *testing.B) {
	_, s := benchCatalog(b)
	benchGet(b, s.URL+"/api/services/101024")
}

func BenchmarkProgramsAll(b *testing.B) {
	_, s := benchCatalog(b)
	benchGet(b, s.URL+"/api/programs")
}

func BenchmarkChannels(b *testing.B) {
	_, s := benchCatalog(b)
	benchGet(b, s.URL+"/api/channels")
}
