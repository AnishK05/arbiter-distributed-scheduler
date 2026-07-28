// Command scheduler is the Arbiter control-plane binary. In later phases it
// hosts the placement engine, failure detector, and leader-election loop; in
// Phase 0 it only proves the gRPC + HTTP scaffolding wires up end to end.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/buildinfo"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/grpcapi"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/health"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	grpcAddr := flag.String("grpc-addr", ":7000", "address for the gRPC server (ClusterControl + SchedulerAPI)")
	httpAddr := flag.String("http-addr", ":8080", "address for the HTTP server (/healthz, later /metrics)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("arbiter-scheduler starting",
		"version", buildinfo.Version,
		"commit", buildinfo.Commit,
		"grpc_addr", *grpcAddr,
		"http_addr", *httpAddr,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	grpcServer := grpc.NewServer()
	server := grpcapi.New()
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
	logger.Info("shutdown signal received, draining connections")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	grpcServer.GracefulStop()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("arbiter-scheduler stopped")
}
