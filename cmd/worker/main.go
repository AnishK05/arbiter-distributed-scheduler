// Command worker is the Arbiter worker-agent binary that will run on every
// cluster node (simulated as a container in the demo cluster). Phase 0 only
// scaffolds the process (flags, logging, health endpoint); node registration,
// heartbeats, and task execution are added starting Phase 1.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/buildinfo"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/health"
)

func main() {
	schedulerAddr := flag.String("scheduler-addr", "localhost:7000", "gRPC address of the scheduler's ClusterControl service")
	hostname := flag.String("hostname", "", "hostname this node advertises to the scheduler (defaults to os.Hostname())")
	cpuCapacityMillicores := flag.Int64("cpu-capacity-millicores", 1000, "simulated CPU capacity for this node, in millicores")
	memCapacityMB := flag.Int64("mem-capacity-mb", 512, "simulated memory capacity for this node, in megabytes")
	httpAddr := flag.String("http-addr", ":8081", "address for the HTTP server (/healthz, later /metrics)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	resolvedHostname := *hostname
	if resolvedHostname == "" {
		if h, err := os.Hostname(); err == nil {
			resolvedHostname = h
		} else {
			resolvedHostname = "unknown"
		}
	}

	logger.Info("arbiter-worker starting",
		"version", buildinfo.Version,
		"commit", buildinfo.Commit,
		"hostname", resolvedHostname,
		"scheduler_addr", *schedulerAddr,
		"cpu_capacity_millicores", *cpuCapacityMillicores,
		"mem_capacity_mb", *memCapacityMB,
		"http_addr", *httpAddr,
	)
	logger.Info("node registration, heartbeats, and task execution are not yet implemented (see IMPLEMENTATION_PLAN.md Phase 1+)")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpServer := &http.Server{
		Addr:              *httpAddr,
		Handler:           health.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("HTTP server listening", "addr", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server exited with error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("arbiter-worker stopped")
}
