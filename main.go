package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// serverWriteTimeout must cover the worst-case stream startup: every backend
// may be tried for connect+headers plus first body (about 2x stream_timeout),
// and the long-lived body keeps extending its own deadline afterwards.
func serverWriteTimeout(c Config) time.Duration {
	seconds := int64(len(c.Backends))*(2*int64(c.StreamTimeoutSeconds)) + 60
	if maxSeconds := int64((1<<63 - 1) / time.Second); seconds > maxSeconds {
		seconds = maxSeconds
	}
	d := time.Duration(seconds) * time.Second
	if d < 2*time.Minute {
		d = 2 * time.Minute
	}
	return d
}

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	path := flag.String("config", "sakurako.json", "configuration JSON path")
	flag.Parse()
	log.SetOutput(io.MultiWriter(os.Stderr, recentLogs))
	c, err := loadConfig(*path)
	if err != nil {
		log.Fatal(err)
	}
	g, err := NewGateway(c)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	g.Sync(ctx)
	if n := g.availableBackends(); n == 0 {
		log.Printf("warning: no backend catalog available at startup; will keep retrying")
	} else {
		log.Printf("%d/%d backends synced at startup", n, len(c.Backends))
	}
	go g.Run(ctx)
	srv := &http.Server{Addr: c.Listen, Handler: g, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: serverWriteTimeout(c), IdleTimeout: 60 * time.Second, MaxHeaderBytes: 32 * 1024, BaseContext: func(_ net.Listener) context.Context { return ctx }}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil {
			log.Printf("graceful shutdown incomplete, closing connections: %v", err)
		}
		_ = srv.Close()
	}()
	log.Printf("sakurako %s listening on %s", version, c.Listen)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-stopped
}
