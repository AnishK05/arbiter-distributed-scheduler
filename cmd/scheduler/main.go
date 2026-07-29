// Command scheduler is the Arbiter control-plane binary. As of Phase 7 it
// exposes Prometheus /metrics alongside /healthz, elects a leader via a
// Postgres lease, serves ClusterControl + SchedulerAPI RPCs, and runs the
// failure detector + scheduling loop only while leading.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/buildinfo"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/dockerutil"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/election"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/failuredetector"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/grpcapi"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/health"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/metrics"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/scheduler"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	dbConnectMaxAttempts    = 15
	dbConnectRetryDelay     = 2 * time.Second
	redisConnectMaxAttempts = 15
	redisConnectRetryDelay  = 2 * time.Second
)

func main() {
	grpcAddr := flag.String("grpc-addr", ":7000", "address for the gRPC server (ClusterControl + SchedulerAPI)")
	httpAddr := flag.String("http-addr", ":8080", "address for the HTTP server (/healthz, /metrics)")
	postgresURL := flag.String("postgres-url", envOr("ARBITER_POSTGRES_URL", "postgres://arbiter:arbiter@localhost:5432/arbiter?sslmode=disable"), "PostgreSQL connection string")
	migrationsPath := flag.String("migrations-path", "migrations", "filesystem path to the SQL migrations directory")
	redisAddr := flag.String("redis-addr", envOr("ARBITER_REDIS_ADDR", "localhost:6379"), "Redis address (host:port or redis:// URL)")
	heartbeatIntervalMS := flag.Int("heartbeat-interval-ms", 500, "heartbeat interval advertised to workers; the failure detector's thresholds are derived from this")
	replicaID := flag.String("replica-id", envOr("ARBITER_REPLICA_ID", ""), "unique identity for this scheduler replica (defaults to hostname)")
	advertiseAddr := flag.String("advertise-addr", envOr("ARBITER_ADVERTISE_ADDR", ""), "gRPC host:port returned to clients/workers in NOT_LEADER redirects (defaults to localhost + grpc port)")
	leaseTTL := flag.Duration("lease-ttl", election.DefaultLeaseTTL, "leader lease time-to-live")
	leaseRenew := flag.Duration("lease-renew-interval", election.DefaultRenewInterval, "how often to renew/attempt the leader lease")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	met := metrics.New()

	resolvedReplicaID := *replicaID
	if resolvedReplicaID == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			resolvedReplicaID = h
		} else {
			resolvedReplicaID = fmt.Sprintf("scheduler-%d", os.Getpid())
		}
	}
	resolvedAdvertise := *advertiseAddr
	if resolvedAdvertise == "" {
		_, port, err := net.SplitHostPort(*grpcAddr)
		if err != nil || port == "" {
			port = "7000"
		}
		resolvedAdvertise = net.JoinHostPort("localhost", port)
	}

	logger.Info("arbiter-scheduler starting",
		"version", buildinfo.Version,
		"commit", buildinfo.Commit,
		"grpc_addr", *grpcAddr,
		"http_addr", *httpAddr,
		"heartbeat_interval_ms", *heartbeatIntervalMS,
		"replica_id", resolvedReplicaID,
		"advertise_addr", resolvedAdvertise,
		"lease_ttl", *leaseTTL,
		"lease_renew_interval", *leaseRenew,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := connectPostgresWithRetry(ctx, logger, *postgresURL)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := store.Migrate(*postgresURL, os.DirFS(*migrationsPath)); err != nil {
		logger.Error("failed to run database migrations", "error", err)
		os.Exit(1)
	}
	logger.Info("database migrations applied")

	rdb, err := connectRedisWithRetry(ctx, logger, *redisAddr)
	if err != nil {
		logger.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	defer func() { _ = rdb.Close() }()

	elector := election.New(db, logger, election.Config{
		Identity:      resolvedReplicaID,
		AdvertiseAddr: resolvedAdvertise,
		LeaseTTL:      *leaseTTL,
		RenewInterval: *leaseRenew,
	}, met)
	elector.Bootstrap(ctx)
	go elector.Run(ctx)
	logger.Info("leader election loop started",
		"is_leader", elector.IsLeader(),
		"leader_addr", elector.LeaderAddr(),
		"epoch", elector.Epoch(),
	)

	heartbeatInterval := time.Duration(*heartbeatIntervalMS) * time.Millisecond
	detectorCfg := failuredetector.DefaultConfig(heartbeatInterval)
	logger.Info("failure detector configured",
		"poll_interval", detectorCfg.PollInterval,
		"not_ready_after", detectorCfg.NotReadyAfter,
		"dead_after", detectorCfg.DeadAfter,
	)
	detector := failuredetector.NewWithDocker(db, rdb, dockerutil.New(), logger, detectorCfg, elector, met)
	go detector.Run(ctx)

	sched := scheduler.New(db, logger, elector, met)
	go sched.Run(ctx)
	logger.Info("scheduling loop started", "policy", "filter_score")

	go met.RunClusterGauges(ctx, logger, db, elector, time.Second)

	grpcServer := grpc.NewServer()
	server := grpcapi.New(db, rdb, int32(*heartbeatIntervalMS), elector, met)
	arbiterv1.RegisterClusterControlServer(grpcServer, server)
	arbiterv1.RegisterSchedulerAPIServer(grpcServer, server)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		logger.Error("failed to listen for gRPC", "error", err)
		os.Exit(1)
	}

	go func() {
		logger.Info("gRPC server listening", "addr", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("gRPC server exited with error", "error", err)
		}
	}()

	httpServer := &http.Server{
		Addr:              *httpAddr,
		Handler:           health.Mux(met.Handler()),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("HTTP server listening", "addr", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server exited with error", "error", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received, draining connections")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcServer.GracefulStop()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("arbiter-scheduler stopped")
}

func connectPostgresWithRetry(ctx context.Context, logger *slog.Logger, postgresURL string) (*store.Store, error) {
	var lastErr error
	for attempt := 1; attempt <= dbConnectMaxAttempts; attempt++ {
		db, err := store.Connect(ctx, postgresURL)
		if err == nil {
			return db, nil
		}
		lastErr = err
		logger.Warn("database not ready yet, retrying", "attempt", attempt, "max_attempts", dbConnectMaxAttempts, "error", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(dbConnectRetryDelay):
		}
	}
	return nil, lastErr
}

func connectRedisWithRetry(ctx context.Context, logger *slog.Logger, redisAddr string) (*cache.Client, error) {
	var lastErr error
	for attempt := 1; attempt <= redisConnectMaxAttempts; attempt++ {
		rdb, err := cache.Connect(ctx, redisAddr)
		if err == nil {
			return rdb, nil
		}
		lastErr = err
		logger.Warn("redis not ready yet, retrying", "attempt", attempt, "max_attempts", redisConnectMaxAttempts, "error", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(redisConnectRetryDelay):
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
