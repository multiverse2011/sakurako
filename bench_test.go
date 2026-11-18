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

// benchCatalog mirrors the measured deployment's counts (issue #13): two
// backends, 147 services and 28,725 programs. The synthetic programs are heavier
// than the real ones: about 1.7 KB each, so /api/programs is about 48 MB instead
// of the measured 17.7 MB. The cost being benchmarked is per-request catalog
// work, which scales with the item counts this fixture gets right.
func benchCatalog(b *testing.B) (*Gateway, *httptest.Server) {
	b.Helper()
	// Build the fixture outside the measured region: -benchtime=1x reports the one
	// measured run of the benchmark function, which otherwise includes all this.
	b.StopTimer()
	var services, programs [2][]map[string]any
	for i := int64(0); i < 147; i++ {
		bi, networkID := 0, int64(1)
		if i >= 74 {
			bi, networkID = 1, 2
		}
		serviceID := 1024 + i
		id := networkID*100000 + serviceID
		services[bi] = append(services[bi], map[string]any{
			"id": id, "serviceId": serviceID, "networkId": networkID, "type": 1,
			"name":        fmt.Sprintf("サービス %d 第%d", networkID, i+1),
			"hasLogoData": i%2 == 0,
			"channel":     map[string]any{"type": "GR", "channel": fmt.Sprint(10 + i%20), "name": fmt.Sprintf("チャンネル%d", i)},
		})
		perService := int64(195)
		if i < 60 {
			perService++ // 147*195 + 60 = 28,725 programs in total
		}
		for p := int64(0); p < perService; p++ {
			eventID := p + 1
			programs[bi] = append(programs[bi], map[string]any{
				"id": id*100000 + eventID, "eventId": eventID, "serviceId": serviceID, "networkId": networkID,
				"startAt": 1700000000000 + p*1800000, "duration": 1800000, "isFree": true,
				"name":        fmt.Sprintf("サンプル番組タイトル 第%d話", eventID),
				"description": strings.Repeat("番組の説明文です。", 30),
				"extended":    map[string]any{"detail": strings.Repeat("詳細な番組情報です。", 20)},
				"genres":      []any{map[string]any{"lv1": 7, "lv2": 0, "un1": 0, "un2": 0}},
			})
		}
	}
	upstream := func(bi int) *httptest.Server {
		servicesJSON, err := json.Marshal(services[bi])
		if err != nil {
			b.Fatal(err)
		}
		programsJSON, err := json.Marshal(programs[bi])
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
	a, c := upstream(0), upstream(1)
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
	b.StartTimer()
	return g, s
}

func benchGet(b *testing.B, url string) {
	b.Helper()
	b.ReportAllocs()
	b.StopTimer()
	// Warm the process's one-time HTTP path, so the first counted iteration under
	// -benchtime=1x is not the one carrying it.
	get(url)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := get(url); err != nil {
			b.Fatal(err)
		}
	}
}

// get reads a whole response body; io.Discard buffers count toward B/op even on
// the first iteration, so every iteration reads identically.
func get(url string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		resp.Body.Close()
		return err
	}
	return resp.Body.Close()
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
