// Command worker is the Arbiter worker-agent binary that runs on every
// cluster node (simulated as a container in the demo cluster). As of
// Phase 6 it follows NOT_LEADER redirects and rotates across a configured
// list of scheduler addresses when the current leader is unreachable.
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
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/leaderclient"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	registerMaxAttempts      = 15
	registerRetryDelay       = 2 * time.Second
	defaultHeartbeatInterval = 500 * time.Millisecond
)

func main() {
	schedulerAddr := flag.String("scheduler-addr", envOr("ARBITER_SCHEDULER_ADDR", "localhost:7000"), "primary gRPC address of a scheduler replica")
	schedulerAddrs := flag.String("scheduler-addrs", envOr("ARBITER_SCHEDULER_ADDRS", ""), "comma-separated scheduler addresses to rotate through on failover (defaults to --scheduler-addr)")
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

	addrs := parseAddrList(*schedulerAddrs)
	if len(addrs) == 0 {
		addrs = []string{*schedulerAddr}
	} else if !containsAddr(addrs, *schedulerAddr) {
		addrs = append([]string{*schedulerAddr}, addrs...)
	}

	logger.Info("arbiter-worker starting",
		"version", buildinfo.Version,
		"commit", buildinfo.Commit,
		"hostname", resolvedHostname,
		"address", resolvedAddress,
		"scheduler_addrs", addrs,
		"cpu_capacity_millicores", *cpuCapacityMillicores,
		"mem_capacity_mb", *memCapacityMB,
		"http_addr", *httpAddr,
		"labels", labels,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dialer := newSchedulerDialer(logger, addrs)
	defer dialer.Close()

	agent := &workerAgent{
		logger: logger,
		dialer: dialer,
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
	regResp, err := registerWithRetry(ctx, logger, dialer, registerReq)
	if err != nil {
		logger.Error("failed to register with scheduler", "error", err)
		os.Exit(1)
	}
	logger.Info("registered with scheduler",
		"node_id", regResp.GetNodeId(),
		"epoch", regResp.GetEpoch(),
		"heartbeat_interval_ms", regResp.GetHeartbeatIntervalMs(),
		"scheduler_addr", dialer.Addr(),
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

// schedulerDialer owns the current gRPC connection, follows NOT_LEADER
// redirects, and rotates across configured replica addresses on Unavailable.
type schedulerDialer struct {
	logger *slog.Logger
	addrs  []string

	mu     sync.Mutex
	idx    int
	addr   string
	conn   *grpc.ClientConn
	client arbiterv1.ClusterControlClient
}

func newSchedulerDialer(logger *slog.Logger, addrs []string) *schedulerDialer {
	return &schedulerDialer{logger: logger, addrs: addrs, addr: addrs[0]}
}

func (d *schedulerDialer) Addr() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.addr
}

func (d *schedulerDialer) Client() (arbiterv1.ClusterControlClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.client != nil {
		return d.client, nil
	}
	return d.redialLocked(d.addr)
}

func (d *schedulerDialer) FollowRedirect(err error) bool {
	addr, ok := leaderclient.ParseLeaderAddr(err)
	if !ok {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if addr == d.addr && d.client != nil {
		return true
	}
	d.logger.Info("following NOT_LEADER redirect", "from", d.addr, "to", addr)
	if _, dialErr := d.redialLocked(addr); dialErr != nil {
		d.logger.Warn("failed to dial redirected leader", "addr", addr, "error", dialErr)
		return false
	}
	return true
}

// RecoverFromUnavailable rotates to the next configured scheduler address.
func (d *schedulerDialer) RecoverFromUnavailable(err error) bool {
	if err == nil || status.Code(err) != codes.Unavailable {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.addrs) == 0 {
		return false
	}
	d.idx = (d.idx + 1) % len(d.addrs)
	target := d.addrs[d.idx]
	d.logger.Info("current scheduler unreachable; rotating",
		"from", d.addr, "to", target, "error", err)
	if _, dialErr := d.redialLocked(target); dialErr != nil {
		d.logger.Warn("failed to dial rotated scheduler", "addr", target, "error", dialErr)
		return false
	}
	return true
}

func (d *schedulerDialer) redialLocked(addr string) (arbiterv1.ClusterControlClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	if d.conn != nil {
		_ = d.conn.Close()
	}
	d.addr = addr
	d.conn = conn
	d.client = arbiterv1.NewClusterControlClient(conn)
	for i, a := range d.addrs {
		if a == addr {
			d.idx = i
			break
		}
	}
	return d.client, nil
}

func (d *schedulerDialer) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.conn != nil {
		_ = d.conn.Close()
		d.conn = nil
		d.client = nil
	}
}

type workerAgent struct {
	logger *slog.Logger
	dialer *schedulerDialer
	exec   *executor.Executor

	mu     sync.Mutex
	specs  map[string]executor.TaskSpec
	nodeID string
	epoch  int64
}

func (a *workerAgent) setIdentity(nodeID string, epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nodeID = nodeID
	a.epoch = epoch
}

func (a *workerAgent) identity() (nodeID string, epoch int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nodeID, a.epoch
}

func (a *workerAgent) handleTaskDone(result executor.Result) {
	a.mu.Lock()
	delete(a.specs, result.TaskID)
	a.mu.Unlock()

	a.logger.Info("task finished",
		"task_id", result.TaskID,
		"run_id", result.RunID,
		"status", result.Status,
		"exit_code", result.ExitCode,
		"error", result.Error,
	)

	nodeID, epoch := a.identity()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.withLeaderRetry(ctx, func(client arbiterv1.ClusterControlClient) error {
		_, err := client.ReportTaskStatus(ctx, &arbiterv1.TaskStatusUpdate{
			TaskId:   result.TaskID,
			Status:   result.Status,
			ExitCode: result.ExitCode,
			Error:    result.Error,
			NodeId:   nodeID,
			Epoch:    epoch,
		})
		return err
	}); err != nil {
		a.logger.Warn("failed to report task status", "task_id", result.TaskID, "error", err)
	}
}

func (a *workerAgent) withLeaderRetry(ctx context.Context, fn func(arbiterv1.ClusterControlClient) error) error {
	for attempt := 0; attempt < 8; attempt++ {
		client, err := a.dialer.Client()
		if err != nil {
			return err
		}
		err = fn(client)
		if err == nil {
			return nil
		}
		if a.dialer.FollowRedirect(err) {
			continue
		}
		if a.dialer.RecoverFromUnavailable(err) {
			continue
		}
		return err
	}
	client, err := a.dialer.Client()
	if err != nil {
		return err
	}
	return fn(client)
}

func (a *workerAgent) runHeartbeatLoop(ctx context.Context, registerReq *arbiterv1.RegisterNodeRequest, initial *arbiterv1.RegisterNodeResponse) {
	a.setIdentity(initial.GetNodeId(), initial.GetEpoch())

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
			nodeID, epoch := a.identity()
			a.mu.Lock()
			cpu, mem := a.exec.AllocatedResources(a.specs)
			a.mu.Unlock()

			var resp *arbiterv1.HeartbeatResponse
			err := a.withLeaderRetry(ctx, func(client arbiterv1.ClusterControlClient) error {
				var hbErr error
				resp, hbErr = client.Heartbeat(ctx, &arbiterv1.HeartbeatRequest{
					NodeId: nodeID,
					Epoch:  epoch,
					Allocated: &arbiterv1.NodeResources{
						CpuMillicores: cpu,
						MemoryMb:      mem,
					},
				})
				return hbErr
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

				regResp, err := registerWithRetry(ctx, a.logger, a.dialer, registerReq)
				if err != nil {
					a.logger.Error("failed to re-register after epoch invalidation", "error", err)
					continue
				}
				a.setIdentity(regResp.GetNodeId(), regResp.GetEpoch())
				a.logger.Info("re-registered with scheduler", "node_id", regResp.GetNodeId(), "epoch", regResp.GetEpoch())
				continue
			}

			a.handleAssignments(resp.GetNewAssignments())
		}
	}
}

func (a *workerAgent) handleAssignments(assignments []*arbiterv1.TaskAssignment) {
	_, epoch := a.identity()
	for _, assignment := range assignments {
		if a.exec.IsRunning(assignment.GetTaskId()) {
			continue
		}
		if assignment.GetAssignedEpoch() != epoch {
			a.logger.Warn("refusing assignment with mismatched epoch",
				"task_id", assignment.GetTaskId(),
				"assigned_epoch", assignment.GetAssignedEpoch(),
				"node_epoch", epoch,
			)
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
			"assigned_epoch", assignment.GetAssignedEpoch(),
		)

		go a.startAssignedTask(spec)
	}
}

func (a *workerAgent) startAssignedTask(spec executor.TaskSpec) {
	nodeID, epoch := a.identity()
	reportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := a.withLeaderRetry(reportCtx, func(client arbiterv1.ClusterControlClient) error {
		_, err := client.ReportTaskStatus(reportCtx, &arbiterv1.TaskStatusUpdate{
			TaskId: spec.TaskID,
			Status: "running",
			NodeId: nodeID,
			Epoch:  epoch,
		})
		return err
	})
	cancel()
	if err != nil {
		a.logger.Warn("failed to report task running", "task_id", spec.TaskID, "error", err)
	}

	if err := a.exec.Start(context.Background(), spec); err != nil {
		a.logger.Error("failed to start task container", "task_id", spec.TaskID, "error", err)
	}
}

func registerWithRetry(ctx context.Context, logger *slog.Logger, dialer *schedulerDialer, req *arbiterv1.RegisterNodeRequest) (*arbiterv1.RegisterNodeResponse, error) {
	var lastErr error
	for attempt := 1; attempt <= registerMaxAttempts; attempt++ {
		client, err := dialer.Client()
		if err != nil {
			lastErr = err
		} else {
			resp, err := client.RegisterNode(ctx, req)
			if err == nil {
				return resp, nil
			}
			lastErr = err
			if dialer.FollowRedirect(err) || dialer.RecoverFromUnavailable(err) {
				continue
			}
		}
		logger.Warn("scheduler not ready yet, retrying registration", "attempt", attempt, "max_attempts", registerMaxAttempts, "error", lastErr)

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

func parseAddrList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func containsAddr(addrs []string, addr string) bool {
	for _, a := range addrs {
		if a == addr {
			return true
		}
	}
	return false
}
