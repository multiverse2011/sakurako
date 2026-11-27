package main

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var processStarted = time.Now()

// runtimeInfo is what /-/ui/runtime returns: where the gateway runs and how much it uses.
func runtimeInfo() object {
	host, _ := os.Hostname() // in Docker this is the container ID unless hostname: is set
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	return object{
		"environment": runtimeEnvironment(), "hostname": host, "version": version,
		"go_version": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "num_cpu": runtime.NumCPU(),
		"started": processStarted, "goroutines": runtime.NumGoroutine(),
		"rss_bytes": procRSS("/proc/self/status"), "heap_bytes": mem.HeapAlloc, "sys_bytes": mem.Sys,
	}
}

func runtimeEnvironment() string {
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }
	switch {
	case os.Getenv("KUBERNETES_SERVICE_HOST") != "":
		return "Kubernetes"
	case exists("/.dockerenv"):
		return "Docker"
	case exists("/run/.containerenv"):
		return "Podman"
	}
	return "Native"
}

// procRSS reads VmRSS from a Linux /proc status file; nil where there is none (macOS dev runs).
func procRSS(path string) any {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if rest, ok := strings.CutPrefix(s.Text(), "VmRSS:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			if err != nil {
				return nil
			}
			return kb * 1024
		}
	}
	return nil
}
