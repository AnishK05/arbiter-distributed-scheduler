// Package autoscaler implements Phase 9 simulated cluster autoscaling:
// the leader watches pending-queue depth and launches additional Docker
// worker containers when backlog stays elevated; idle autoscaled workers
// are cordoned and removed after a sustained empty window.
package autoscaler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/dockerutil"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/metrics"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

const nodeLabelAutoscaled = "autoscaled"

// LeaderGate reports whether this replica should run autoscaling decisions.
type LeaderGate interface {
	IsLeader() bool
}

// Config tunes scale-up / scale-down thresholds.
type Config struct {
	Enabled bool

	PollInterval time.Duration

	// Scale up when pending tasks stay >= PendingThreshold for SustainWindow.
	PendingThreshold int64
	SustainWindow    time.Duration

	// Scale down an autoscaled node that stays at zero allocation for IdleWindow.
	IdleWindow time.Duration

	// MaxAutoscaledWorkers caps how many Docker-managed workers we launch.
	MaxAutoscaledWorkers int

	// Cooldown between consecutive scale actions.
	Cooldown time.Duration

	WorkerImage        string
	DockerNetwork      string
	SchedulerAddr      string
	SchedulerAddrs     string
	WorkerCPUMillicores int64
	WorkerMemMB         int64
	ExtraHosts          []string
}

// DefaultConfig returns lab-friendly defaults for the Phase 9 DoD.
func DefaultConfig() Config {
	return Config{
		Enabled:              false,
		PollInterval:         2 * time.Second,
		PendingThreshold:     3,
		SustainWindow:        8 * time.Second,
		IdleWindow:           20 * time.Second,
		MaxAutoscaledWorkers: 3,
		Cooldown:             5 * time.Second,
		WorkerImage:          envOr("ARBITER_WORKER_IMAGE", "deploy-worker:latest"),
		DockerNetwork:        strings.TrimSpace(os.Getenv("ARBITER_DOCKER_NETWORK")),
		SchedulerAddr:        envOr("ARBITER_AUTOSCALE_SCHEDULER_ADDR", "scheduler:7000"),
		SchedulerAddrs:       envOr("ARBITER_AUTOSCALE_SCHEDULER_ADDRS", "scheduler:7000,scheduler-2:7000,scheduler-3:7000"),
		WorkerCPUMillicores:  2000,
		WorkerMemMB:          1024,
		ExtraHosts:           []string{"host.docker.internal:host-gateway"},
	}
}

// Runner is the leader-only autoscaling loop.
type Runner struct {
	store   *store.Store
	docker  *dockerutil.Client
	leader  LeaderGate
	logger  *slog.Logger
	metrics *metrics.Registry
	cfg     Config

	pendingSince   time.Time
	idleSince      map[string]time.Time
	lastScaleAction time.Time
	network        string
	seq            int
}

// New constructs a Runner. logger/metrics/leader may be nil.
func New(s *store.Store, docker *dockerutil.Client, leader LeaderGate, logger *slog.Logger, met *metrics.Registry, cfg Config) *Runner {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.MaxAutoscaledWorkers <= 0 {
		cfg.MaxAutoscaledWorkers = 3
	}
	if cfg.PendingThreshold <= 0 {
		cfg.PendingThreshold = 3
	}
	return &Runner{
		store:     s,
		docker:    docker,
		leader:    leader,
		logger:    logger,
		metrics:   met,
		cfg:       cfg,
		idleSince: make(map[string]time.Time),
		network:   cfg.DockerNetwork,
		seq:       int(time.Now().Unix() % 1000),
	}
}

// Run blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) {
	if !r.cfg.Enabled {
		r.logger.Info("autoscaler disabled")
		return
	}
	r.logger.Info("autoscaler started",
		"pending_threshold", r.cfg.PendingThreshold,
		"sustain_window", r.cfg.SustainWindow,
		"idle_window", r.cfg.IdleWindow,
		"max_autoscaled", r.cfg.MaxAutoscaledWorkers,
		"worker_image", r.cfg.WorkerImage,
	)

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.leader != nil && !r.leader.IsLeader() {
				r.pendingSince = time.Time{}
				r.idleSince = make(map[string]time.Time)
				continue
			}
			if err := r.tick(ctx); err != nil {
				r.logger.Warn("autoscaler: tick failed", "error", err)
			}
		}
	}
}

func (r *Runner) tick(ctx context.Context) error {
	if err := r.ensureNetwork(ctx); err != nil {
		return err
	}

	counts, err := r.store.CountTasksByStatus(ctx)
	if err != nil {
		return err
	}
	pending := counts[store.TaskStatusPending]

	nodes, err := r.store.ListNodes(ctx)
	if err != nil {
		return err
	}
	allocs, err := r.store.GetNodeAllocations(ctx)
	if err != nil {
		return err
	}

	autoscaledNodes := filterAutoscaled(nodes)
	if r.metrics != nil {
		r.metrics.AutoscaledWorkers.Set(float64(len(autoscaledNodes)))
	}

	now := time.Now()
	if pending >= r.cfg.PendingThreshold {
		if r.pendingSince.IsZero() {
			r.pendingSince = now
		}
	} else {
		r.pendingSince = time.Time{}
	}

	if r.canScale() && !r.pendingSince.IsZero() && now.Sub(r.pendingSince) >= r.cfg.SustainWindow {
		runningAutoscaled, err := r.docker.ListAutoscaledWorkers(ctx)
		if err != nil {
			return err
		}
		if len(runningAutoscaled) < r.cfg.MaxAutoscaledWorkers {
			if err := r.scaleUp(ctx); err != nil {
				return err
			}
			r.lastScaleAction = time.Now()
			r.pendingSince = time.Time{}
			return nil
		}
	}

	for _, n := range autoscaledNodes {
		if n.Status != store.NodeStatusReady && n.Status != store.NodeStatusCordoned {
			delete(r.idleSince, n.ID)
			continue
		}
		alloc := allocs[n.ID]
		idle := alloc.CPUMillicores == 0 && alloc.MemoryMB == 0
		if !idle {
			delete(r.idleSince, n.ID)
			continue
		}
		if _, ok := r.idleSince[n.ID]; !ok {
			r.idleSince[n.ID] = now
		}
		if !r.canScale() {
			continue
		}
		if now.Sub(r.idleSince[n.ID]) >= r.cfg.IdleWindow {
			if err := r.scaleDown(ctx, n); err != nil {
				return err
			}
			delete(r.idleSince, n.ID)
			r.lastScaleAction = time.Now()
			return nil
		}
	}
	return nil
}

func (r *Runner) canScale() bool {
	if r.cfg.Cooldown <= 0 {
		return true
	}
	if r.lastScaleAction.IsZero() {
		return true
	}
	return time.Since(r.lastScaleAction) >= r.cfg.Cooldown
}

func (r *Runner) ensureNetwork(ctx context.Context) error {
	if r.network != "" {
		return nil
	}
	netName, err := r.docker.DetectComposeNetwork(ctx)
	if err != nil {
		return fmt.Errorf("detect docker network: %w", err)
	}
	r.network = netName
	r.logger.Info("autoscaler: using docker network", "network", r.network)
	return nil
}

func (r *Runner) scaleUp(ctx context.Context) error {
	hostname := r.nextHostname(ctx)
	name := "arbiter-" + hostname

	// Avoid collisions if a previous container still exists.
	_ = r.docker.RemoveContainer(ctx, name, true)

	id, err := r.docker.CreateAndStartWorker(ctx, dockerutil.WorkerSpec{
		Name:           name,
		Hostname:       hostname,
		Image:          r.cfg.WorkerImage,
		Network:        r.network,
		SchedulerAddr:  r.cfg.SchedulerAddr,
		SchedulerAddrs: r.cfg.SchedulerAddrs,
		CPUMillicores:  r.cfg.WorkerCPUMillicores,
		MemMB:          r.cfg.WorkerMemMB,
		ExtraHosts:     r.cfg.ExtraHosts,
	})
	if err != nil {
		return fmt.Errorf("scale up: %w", err)
	}

	msg := fmt.Sprintf("autoscaler scaled up: launched %s (container=%s)", hostname, shortID(id))
	if err := r.store.InsertEvent(ctx, store.EntityTypeNode, hostname, store.EventTypeNodeScaledUp, msg); err != nil {
		r.logger.Warn("autoscaler: insert scale-up event", "error", err)
	}
	if r.metrics != nil {
		r.metrics.ScaleUpTotal.Inc()
	}
	r.logger.Info("autoscaler: scaled up", "hostname", hostname, "container_id", shortID(id))
	return nil
}

func (r *Runner) nextHostname(ctx context.Context) string {
	used := map[string]bool{}
	if workers, err := r.docker.ListAutoscaledWorkers(ctx); err == nil {
		for _, w := range workers {
			if h := w.Labels[dockerutil.LabelWorkerHostname]; h != "" {
				used[h] = true
			}
			used[w.Name] = true
		}
	}
	for i := 1; i < 10000; i++ {
		h := fmt.Sprintf("worker-auto-%d", i)
		if !used[h] && !used["arbiter-"+h] {
			return h
		}
	}
	r.seq++
	return fmt.Sprintf("worker-auto-%d", r.seq)
}

func (r *Runner) scaleDown(ctx context.Context, node store.Node) error {
	if node.Status != store.NodeStatusCordoned {
		if _, err := r.store.UpdateNodeStatus(ctx, node.ID, store.NodeStatusCordoned, store.EventTypeNodeCordoned,
			fmt.Sprintf("autoscaler cordoning idle node %s before reclaim", node.Hostname)); err != nil {
			return err
		}
	}

	// Prefer remove by hostname label; fall back to container name convention.
	removed := false
	workers, err := r.docker.ListAutoscaledWorkers(ctx)
	if err != nil {
		return err
	}
	for _, w := range workers {
		if w.Labels[dockerutil.LabelWorkerHostname] == node.Hostname || w.Name == "arbiter-"+node.Hostname {
			if err := r.docker.RemoveContainer(ctx, w.ID, true); err != nil {
				return err
			}
			removed = true
			break
		}
	}
	if !removed {
		_ = r.docker.RemoveContainer(ctx, "arbiter-"+node.Hostname, true)
	}

	if _, err := r.store.MarkNodeDead(ctx, node.ID); err != nil {
		// Node may already be transitioning; still emit scale-down event.
		r.logger.Warn("autoscaler: mark dead after reclaim", "error", err, "node", node.Hostname)
	}

	msg := fmt.Sprintf("autoscaler scaled down: reclaimed %s", node.Hostname)
	if err := r.store.InsertEvent(ctx, store.EntityTypeNode, node.ID, store.EventTypeNodeScaledDown, msg); err != nil {
		r.logger.Warn("autoscaler: insert scale-down event", "error", err)
	}
	if r.metrics != nil {
		r.metrics.ScaleDownTotal.Inc()
	}
	r.logger.Info("autoscaler: scaled down", "hostname", node.Hostname, "node_id", node.ID)
	return nil
}

func filterAutoscaled(nodes []store.Node) []store.Node {
	out := make([]store.Node, 0)
	for _, n := range nodes {
		if n.Labels != nil && n.Labels[nodeLabelAutoscaled] == "true" {
			out = append(out, n)
		}
	}
	return out
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// ParseBoolEnv is a tiny helper for main flags.
func ParseBoolEnv(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
