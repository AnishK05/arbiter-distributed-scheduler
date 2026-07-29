// Command worker is the Arbiter worker-agent binary that runs on every
// cluster node (simulated as a container in the demo cluster). As of
// Phase 3 it registers with the scheduler, sends heartbeats, receives task
// assignments, and executes them as Docker containers.
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
	"strings"
	"sync"
	"syscall"
	"time"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/buildinfo"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/executor"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/health"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	registerMaxAttempts      = 15
	registerRetryDelay       = 2 * time.Second
	defaultHeartbeatInterval = 500 * time.Millisecond
)

func main() {
	schedulerAddr := flag.String("scheduler-addr", envOr("ARBITER_SCHEDULER_ADDR", "localhost:7000"), "gRPC address of the scheduler's ClusterControl service")
	hostname := flag.String("hostname", "", "hostname this node advertises to the scheduler (defaults to os.Hostname())")
	address := flag.String("address", "", "address this node advertises to the scheduler (defaults to '<hostname><http-addr>')")
	cpuCapacityMillicores := flag.Int64("cpu-capacity-millicores", 1000, "simulated CPU capacity for this node, in millicores")
	memCapacityMB := flag.Int64("mem-capacity-mb", 512, "simulated memory capacity for this node, in megabytes")
	httpAddr := flag.String("http-addr", ":8081", "address for the HTTP server (/healthz, later /metrics)")
	labelsFlag := flag.String("labels", "", "comma-separated node labels as key=value pairs (e.g. zone=a,gpu=true)")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	labels, err := parseLabels(*labelsFlag)
	if err != nil {
		logger.Error("invalid --labels", "error", err)
		os.Exit(1)
	}

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
		"labels", labels,
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

	agent := &workerAgent{
		logger: logger,
		client: client,
		specs:  make(map[string]executor.TaskSpec),
	}

	exec, err := executor.New(agent.handleTaskDone)
	if err != nil {
		logger.Error("failed to initialize docker executor", "error", err)
		os.Exit(1)
	}
	defer func() { _ = exec.Close() }()
	agent.exec = exec

	registerReq := &arbiterv1.RegisterNodeRequest{
		Hostname: resolvedHostname,
		Address:  resolvedAddress,
		Capacity: &arbiterv1.NodeResources{
			CpuMillicores: *cpuCapacityMillicores,
			MemoryMb:      *memCapacityMB,
		},
		Labels: labels,
	}
	regResp, err := registerWithRetry(ctx, logger, client, registerReq)
	if err != nil {
		logger.Error("failed to register with scheduler", "error", err)
		os.Exit(1)
	}
	logger.Info("registered with scheduler",
		"node_id", regResp.GetNodeId(),
		"epoch", regResp.GetEpoch(),
		"heartbeat_interval_ms", regResp.GetHeartbeatIntervalMs(),
	)

	go agent.runHeartbeatLoop(ctx, registerReq, regResp)

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
	exec.StopAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "error", err)
	}

	logger.Info("arbiter-worker stopped")
}

type workerAgent struct {
	logger *slog.Logger
	client arbiterv1.ClusterControlClient
	exec   *executor.Executor

	mu    sync.Mutex
	specs map[string]executor.TaskSpec
}

func (a *workerAgent) handleTaskDone(result executor.Result) {
	a.mu.Lock()
	delete(a.specs, result.TaskID)
	a.mu.Unlock()

	a.logger.Info("task finished",
		"task_id", result.TaskID,
		"status", result.Status,
		"exit_code", result.ExitCode,
		"error", result.Error,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := a.client.ReportTaskStatus(ctx, &arbiterv1.TaskStatusUpdate{
		TaskId:   result.TaskID,
		Status:   result.Status,
		ExitCode: result.ExitCode,
		Error:    result.Error,
	})
	if err != nil {
		a.logger.Warn("failed to report task status", "task_id", result.TaskID, "error", err)
	}
}

func (a *workerAgent) runHeartbeatLoop(ctx context.Context, registerReq *arbiterv1.RegisterNodeRequest, initial *arbiterv1.RegisterNodeResponse) {
	nodeID := initial.GetNodeId()
	epoch := initial.GetEpoch()

	interval := time.Duration(initial.GetHeartbeatIntervalMs()) * time.Millisecond
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.mu.Lock()
			cpu, mem := a.exec.AllocatedResources(a.specs)
			a.mu.Unlock()

			resp, err := a.client.Heartbeat(ctx, &arbiterv1.HeartbeatRequest{
				NodeId: nodeID,
				Epoch:  epoch,
				Allocated: &arbiterv1.NodeResources{
					CpuMillicores: cpu,
					MemoryMb:      mem,
				},
			})
			if err != nil {
				a.logger.Warn("heartbeat failed, will retry next interval", "node_id", nodeID, "error", err)
				continue
			}

			if resp.GetEpochInvalid() {
				a.logger.Warn("scheduler rejected this node's epoch (marked dead); stopping tasks and re-registering",
					"node_id", nodeID, "epoch", epoch)
				a.exec.StopAll()
				a.mu.Lock()
				a.specs = make(map[string]executor.TaskSpec)
				a.mu.Unlock()

				regResp, err := registerWithRetry(ctx, a.logger, a.client, registerReq)
				if err != nil {
					a.logger.Error("failed to re-register after epoch invalidation", "error", err)
					continue
				}
				nodeID = regResp.GetNodeId()
				epoch = regResp.GetEpoch()
				a.logger.Info("re-registered with scheduler", "node_id", nodeID, "epoch", epoch)
				continue
			}

			a.handleAssignments(ctx, resp.GetNewAssignments())
		}
	}
}

func (a *workerAgent) handleAssignments(ctx context.Context, assignments []*arbiterv1.TaskAssignment) {
	for _, assignment := range assignments {
		if a.exec.IsRunning(assignment.GetTaskId()) {
			continue
		}
		spec := executor.TaskSpec{
			TaskID:               assignment.GetTaskId(),
			Image:                assignment.GetImage(),
			Command:              assignment.GetCommand(),
			CPURequestMillicores: assignment.GetRequest().GetCpuMillicores(),
			MemRequestMB:         assignment.GetRequest().GetMemoryMb(),
		}

		a.mu.Lock()
		a.specs[spec.TaskID] = spec
		a.mu.Unlock()

		a.logger.Info("received task assignment",
			"task_id", spec.TaskID,
			"image", spec.Image,
			"cpu_mc", spec.CPURequestMillicores,
			"mem_mb", spec.MemRequestMB,
		)

		// Launch asynchronously so a slow docker create/start can't stall
		// the heartbeat loop past the failure-detector dead threshold.
		go a.startAssignedTask(spec)
	}
}

func (a *workerAgent) startAssignedTask(spec executor.TaskSpec) {
	// Report running as soon as we accept the assignment (before/while
	// the container starts) so the control plane's resource accounting
	// reflects the reservation promptly.
	reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := a.client.ReportTaskStatus(reportCtx, &arbiterv1.TaskStatusUpdate{
		TaskId: spec.TaskID,
		Status: "running",
	})
	cancel()
	if err != nil {
		a.logger.Warn("failed to report task running", "task_id", spec.TaskID, "error", err)
	}

	if err := a.exec.Start(context.Background(), spec); err != nil {
		a.logger.Error("failed to start task container", "task_id", spec.TaskID, "error", err)
		// onDone already reports failed for start errors.
	}
}

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

func parseLabels(raw string) (map[string]string, error) {
	out := map[string]string{}
	if raw == "" {
		return out, nil
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, val, ok := strings.Cut(part, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid label %q (want key=value)", part)
		}
		out[key] = val
	}
	return out, nil
}
