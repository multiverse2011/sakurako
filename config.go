package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"
)

// BackendConfig describes one Mirakurun backend and its authority priority.
type BackendConfig struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Priority int    `json:"priority"`
	// Equal domains explicitly assert equal broadcasts for equal network/service IDs.
	ServiceDomain string `json:"service_domain"`
	// UIURL overrides the admin UI link when the browser cannot reach URL (e.g. a Docker service name).
	UIURL string `json:"ui_url,omitempty"`
}

// Config is the gateway's JSON configuration, loaded from a file.
type Config struct {
	Listen                string `json:"listen"`
	SyncSeconds           int    `json:"sync_seconds"`
	CatalogTimeoutSeconds int    `json:"catalog_timeout_seconds"`
	MaxStaleSeconds       int    `json:"max_stale_seconds"`
	StreamTimeoutSeconds  int    `json:"stream_timeout_seconds"`
	// ChannelNames is "virtual" (backend-prefixed, default) or "upstream" (Mirakurun names).
	ChannelNames string          `json:"channel_names"`
	UpdateCheck  bool            `json:"update_check"`
	Backends     []BackendConfig `json:"backends"`
}

// identifier is the shared syntax for backend IDs and service domains.
var identifier = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

func loadConfig(path string) (Config, error) {
	c, err := readConfig(path)
	if err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	return c, nil
}

func readConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	c := Config{Listen: ":40772", SyncSeconds: 30, CatalogTimeoutSeconds: 20, MaxStaleSeconds: 3600, StreamTimeoutSeconds: 30, ChannelNames: "virtual", UpdateCheck: true}
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return Config{}, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return Config{}, fmt.Errorf("config must contain one JSON object (trailing data: %v)", err)
	}
	if c.ChannelNames == "" {
		c.ChannelNames = "virtual" // empty means virtual
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) validate() error {
	for name, seconds := range map[string]int{"sync_seconds": c.SyncSeconds, "catalog_timeout_seconds": c.CatalogTimeoutSeconds, "max_stale_seconds": c.MaxStaleSeconds, "stream_timeout_seconds": c.StreamTimeoutSeconds} {
		if seconds < 1 || int64(seconds) > int64((1<<63-1)/time.Second) {
			return fmt.Errorf("%s=%d is outside the supported duration range", name, seconds)
		}
	}
	if c.Listen == "" {
		return fmt.Errorf("listen must not be empty")
	}
	if c.ChannelNames != "" && c.ChannelNames != "virtual" && c.ChannelNames != "upstream" {
		return fmt.Errorf("channel_names must be virtual or upstream (got %q)", c.ChannelNames)
	}
	if _, err := net.ResolveTCPAddr("tcp", c.Listen); err != nil {
		return fmt.Errorf("invalid listen address %q: %w", c.Listen, err)
	}
	if required := c.SyncSeconds + c.CatalogTimeoutSeconds; c.MaxStaleSeconds < required {
		return fmt.Errorf("max_stale_seconds=%d must be >= sync_seconds+catalog_timeout_seconds=%d so a slow sync cannot expire the previous snapshot", c.MaxStaleSeconds, required)
	}
	if len(c.Backends) == 0 {
		return fmt.Errorf("at least one backend is required")
	}
	ids := map[string]bool{}
	for _, b := range c.Backends {
		if !identifier.MatchString(b.ID) || ids[b.ID] {
			return fmt.Errorf("invalid or duplicate backend id %q", b.ID)
		}
		ids[b.ID] = true
		if b.Priority < 0 {
			return fmt.Errorf("backend %s: priority must not be negative", b.ID)
		}
		if b.ServiceDomain != "" && !identifier.MatchString(b.ServiceDomain) {
			return fmt.Errorf("invalid service_domain %q for backend %s", b.ServiceDomain, b.ID)
		}
		u, err := url.Parse(b.URL)
		if err != nil {
			return fmt.Errorf("invalid backend URL for %s: %w", b.ID, err)
		}
		if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.User != nil {
			return fmt.Errorf("invalid backend URL for %s (HTTP(S) API base required, without credentials/query/fragment)", b.ID)
		}
		if port := u.Port(); port != "" {
			if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
				return fmt.Errorf("invalid backend URL for %s: port must be 1..65535 (got %q)", b.ID, port)
			}
		}
		if b.UIURL != "" {
			if u, err := url.Parse(b.UIURL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("invalid ui_url for %s (HTTP(S) URL required)", b.ID)
			}
		}
	}
	return nil
}
