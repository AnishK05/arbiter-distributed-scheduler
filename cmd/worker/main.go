// Command worker is the Arbiter worker-agent binary that will run on every
// cluster node (simulated as a container in the demo cluster). As of
// Phase 1 it registers itself with the scheduler on startup; heartbeats and
// task execution are added starting Phase 2.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/buildinfo"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/health"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	registerMaxAttempts = 15
	registerRetryDelay  = 2 * time.Second
)

func main() {
	schedulerAddr := flag.String("scheduler-addr", envOr("ARBITER_SCHEDULER_ADDR", "localhost:7000"), "gRPC address of the scheduler's ClusterControl service")
	hostname := flag.String("hostname", "", "hostname this node advertises to the scheduler (defaults to os.Hostname())")
	address := flag.String("address", "", "address this node advertises to the scheduler (defaults to '<hostname><http-addr>')")
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
	resolvedAddress := *address
	if resolvedAddress == "" {
		resolvedAddress = fmt.Sprintf("%s%s", resolvedHostname, *httpAddr)
	}

	logger.Info("arbiter-worker starting",
		"version", buildinfo.Version,
		"commit", buildinfo.Commit,
		"hostname", resolvedHostname,
		"address", resolvedAddress,
		"scheduler_addr", *schedulerAddr,
		"cpu_capacity_millicores", *cpuCapacityMillicores,
		"mem_capacity_mb", *memCapacityMB,
		"http_addr", *httpAddr,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("failed to create gRPC client for scheduler", "error", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	client := arbiterv1.NewClusterControlClient(conn)
	regResp, err := registerWithRetry(ctx, logger, client, &arbiterv1.RegisterNodeRequest{
		Hostname: resolvedHostname,
		Address:  resolvedAddress,
		Capacity: &arbiterv1.NodeResources{
			CpuMillicores: *cpuCapacityMillicores,
			MemoryMb:      *memCapacityMB,
		},
		Labels: map[string]string{},
	})
	if err != nil {
		logger.Error("failed to register with scheduler", "error", err)
		os.Exit(1)
	}
	logger.Info("registered with scheduler",
		"node_id", regResp.GetNodeId(),
		"epoch", regResp.GetEpoch(),
		"heartbeat_interval_ms", regResp.GetHeartbeatIntervalMs(),
	)
	logger.Info("heartbeats and task execution are not yet implemented (see IMPLEMENTATION_PLAN.md Phase 2+)")

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

// registerWithRetry guards against startup-ordering races (e.g. the worker
// container starting around the same time as the scheduler in Docker
// Compose) with a bounded retry loop, on top of the healthcheck-based
// `depends_on` ordering already configured in deploy/docker-compose.yml.
func registerWithRetry(ctx context.Context, logger *slog.Logger, client arbiterv1.ClusterControlClient, req *arbiterv1.RegisterNodeRequest) (*arbiterv1.RegisterNodeResponse, error) {
	var lastErr error
	for attempt := 1; attempt <= registerMaxAttempts; attempt++ {
		resp, err := client.RegisterNode(ctx, req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		logger.Warn("scheduler not ready yet, retrying registration", "attempt", attempt, "max_attempts", registerMaxAttempts, "error", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(registerRetryDelay):
		}
	}
	return nil, lastErr
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
