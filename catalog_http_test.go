package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// rawClient disables the transport's own gzip so tests observe the bytes and
// headers the gateway actually sends.
var rawClient = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableCompression: true}}

// rawGet performs a request with Accept-Encoding and If-None-Match under the
// test's control, and reads the whole body as sent.
func rawGet(t *testing.T, method, url, acceptEncoding, ifNoneMatch string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if acceptEncoding != "" {
		req.Header.Set("Accept-Encoding", acceptEncoding)
	}
	if ifNoneMatch != "" {
		req.Header.Set("If-None-Match", ifNoneMatch)
	}
	resp, err := rawClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, body
}

// bigCatalog gives the fake backend enough programs that gzip has something to
// win, unlike newFake's single small record.
func bigCatalog(t *testing.T) *fakeBackend {
	t.Helper()
	f := newFake(t, 10, "a")
	id := int64(1000101)
	f.programs = f.programs[:0]
	for i := int64(1); i <= 200; i++ {
		f.programs = append(f.programs, object{"id": id*100000 + i, "networkId": int64(10), "serviceId": int64(101), "eventId": i, "name": "番組", "startAt": int64(1700000000000), "duration": int64(1800000), "isFree": true, "description": strings.Repeat("番組の説明文です。", 40)})
	}
	return f
}

func TestCatalogGzip(t *testing.T) {
	_, s := gateway(t, bigCatalog(t))
	// The fixture has a single service, so /api/services and /api/channels stay
	// under minGzipBytes and must be served identity to an accepting client.
	for _, path := range []string{"/api/programs", "/api/programs?serviceId=101", "/api/programs/100010100003", "/api/services", "/api/channels"} {
		gzResp, gzBody := rawGet(t, "GET", s.URL+path, "gzip", "")
		plainResp, plainBody := rawGet(t, "GET", s.URL+path, "", "")
		if plainResp.Header.Get("Content-Encoding") != "" {
			t.Fatalf("%s: compressed without Accept-Encoding", path)
		}
		for _, h := range []http.Header{gzResp.Header, plainResp.Header} {
			if !strings.Contains(h.Get("Vary"), "Accept-Encoding") {
				t.Fatalf("%s: Vary %q", path, h.Get("Vary"))
			}
		}
		unzipped := plainBody
		if gzResp.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(bytes.NewReader(gzBody))
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			if unzipped, err = io.ReadAll(zr); err != nil {
				t.Fatalf("%s: %v", path, err)
			}
		} else if path == "/api/programs" || path == "/api/programs?serviceId=101" {
			t.Fatalf("%s: large body was not gzipped", path)
		}
		if !bytes.Equal(unzipped, plainBody) {
			t.Fatalf("%s: gzip body differs from the identity body", path)
		}
	}
	// q=0 excludes gzip even alongside a wildcard, as for the UI assets.
	_, body := rawGet(t, "GET", s.URL+"/api/programs", "gzip;q=0, *", "")
	if bytes.HasPrefix(body, []byte{0x1f, 0x8b}) {
		t.Fatal("compressed a client that refused gzip")
	}
}

func TestCatalogETagAndNotModified(t *testing.T) {
	f := bigCatalog(t)
	g, s := gateway(t, f)
	resp, _ := rawGet(t, "GET", s.URL+"/api/programs", "gzip", "")
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("no ETag")
	}
	if again, _ := rawGet(t, "GET", s.URL+"/api/programs", "gzip", ""); again.Header.Get("ETag") != etag {
		t.Fatal("ETag changed between syncs")
	}
	if other, _ := rawGet(t, "GET", s.URL+"/api/programs?serviceId=101", "gzip", ""); other.Header.Get("ETag") == etag {
		t.Fatal("query is not part of the ETag")
	}

	nm, body := rawGet(t, "GET", s.URL+"/api/programs", "gzip", etag)
	if nm.StatusCode != http.StatusNotModified {
		t.Fatalf("HTTP %d, want 304", nm.StatusCode)
	}
	if len(body) != 0 {
		t.Fatalf("304 carried %d body bytes", len(body))
	}
	if nm.Header.Get("Content-Encoding") != "" {
		t.Fatal("304 was compressed")
	}
	if nm.Header.Get("ETag") != etag || !strings.Contains(nm.Header.Get("Vary"), "Accept-Encoding") {
		t.Fatal("304 lost its validators")
	}

	// The catalog changing must invalidate the ETag.
	f.mu.Lock()
	f.programs = append(f.programs, object{"id": int64(100010100201), "networkId": int64(10), "serviceId": int64(101), "eventId": int64(201), "name": "新しい番組", "startAt": int64(1700000000000)})
	f.mu.Unlock()
	g.Sync(context.Background())
	if after, _ := rawGet(t, "GET", s.URL+"/api/programs", "gzip", ""); after.Header.Get("ETag") == etag {
		t.Fatal("ETag survived a catalog change")
	}
}

func TestCatalogETagPerEncoding(t *testing.T) {
	_, s := gateway(t, bigCatalog(t))
	for _, path := range []string{"/api/programs", "/api/programs?serviceId=101"} {
		etags := map[string]string{}
		for _, accept := range []string{"", "gzip"} {
			resp, _ := rawGet(t, "GET", s.URL+path, accept, "")
			etag := resp.Header.Get("ETag")
			if resp.StatusCode != 200 || etag == "" {
				t.Fatalf("%s accept %q: %d ETag %q", path, accept, resp.StatusCode, etag)
			}
			etags[accept] = etag
			if nm, body := rawGet(t, "GET", s.URL+path, accept, etag); nm.StatusCode != http.StatusNotModified || len(body) != 0 {
				t.Fatalf("%s accept %q: revalidation %d body %d", path, accept, nm.StatusCode, len(body))
			}
		}
		// Whenever two representations are served, their ETags must differ.
		if etags[""] == etags["gzip"] {
			t.Fatalf("%s: encodings share an ETag %q", path, etags[""])
		}
		// A client holding the identity body must not get a 304 for the gzip variant.
		if resp, _ := rawGet(t, "GET", s.URL+path, "gzip", etags[""]); resp.StatusCode != 200 {
			t.Fatalf("%s: identity ETag answered a gzip request with %d", path, resp.StatusCode)
		}
	}
}

// TestCatalogETagDiffersAcrossGateways pins that the generation counter starts
// from an instance-specific seed: two gateways serving the same catalog and URI
// must not agree on an ETag. That is what keeps an ETag handed out by a
// previous process from matching after a restart; the restart itself is not
// observable from inside one process.
func TestCatalogETagDiffersAcrossGateways(t *testing.T) {
	_, first := gateway(t, bigCatalog(t))
	_, second := gateway(t, bigCatalog(t))
	a, _ := rawGet(t, "GET", first.URL+"/api/programs", "gzip", "")
	b, _ := rawGet(t, "GET", second.URL+"/api/programs", "gzip", "")
	if a.Header.Get("ETag") == b.Header.Get("ETag") {
		t.Fatal("two gateways over the same catalog share an ETag")
	}
}

func TestCatalogHead(t *testing.T) {
	_, s := gateway(t, bigCatalog(t))
	get, body := rawGet(t, "GET", s.URL+"/api/programs", "gzip", "")
	head, headBody := rawGet(t, "HEAD", s.URL+"/api/programs", "gzip", "")
	if head.StatusCode != 200 || len(headBody) != 0 {
		t.Fatalf("HEAD HTTP %d with %d body bytes", head.StatusCode, len(headBody))
	}
	if head.Header.Get("Content-Length") != strconv.Itoa(len(body)) {
		t.Fatalf("HEAD Content-Length %q, GET sent %d", head.Header.Get("Content-Length"), len(body))
	}
	if head.Header.Get("ETag") != get.Header.Get("ETag") || head.Header.Get("Content-Encoding") != "gzip" {
		t.Fatal("HEAD headers differ from GET")
	}
}

func TestStreamsAreNotCompressed(t *testing.T) {
	f := newFake(t, 10, "a")
	_, s := gateway(t, f)
	req, _ := http.NewRequest("GET", s.URL+"/api/services/1000101/stream", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := rawClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || resp.Header.Get("Content-Encoding") != "" {
		t.Fatalf("stream HTTP %d, Content-Encoding %q", resp.StatusCode, resp.Header.Get("Content-Encoding"))
	}
	if body := readStreamBody(t, resp); body != "transport-stream" {
		t.Fatalf("stream body %q", body)
	}
	resp.Body.Close()

	// The channel stream that tuning clients use for EPG scans is untouched too.
	req, _ = http.NewRequest("GET", s.URL+"/api/channels/GR/"+channelByName(t, s, 10)+"/stream", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	channel, err := rawClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if channel.StatusCode != 200 || channel.Header.Get("Content-Encoding") != "" {
		t.Fatalf("channel stream HTTP %d, Content-Encoding %q", channel.StatusCode, channel.Header.Get("Content-Encoding"))
	}
	if body := readStreamBody(t, channel); body != "transport-stream" {
		t.Fatalf("channel stream body %q", body)
	}
	channel.Body.Close()

	req, _ = http.NewRequest("GET", s.URL+"/api/events/stream?resource=tuner", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	events, err := rawClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if events.StatusCode != 200 || events.Header.Get("Content-Encoding") != "" {
		t.Fatalf("event stream HTTP %d, Content-Encoding %q", events.StatusCode, events.Header.Get("Content-Encoding"))
	}
	events.Body.Close()
}
