// Package failuredetector implements Arbiter's heartbeat-based failure
// detector (IMPLEMENTATION_PLAN.md Section 6.4): a background loop that
// scans active nodes, compares each one's last-seen timestamp (from Redis)
// against configurable thresholds, and transitions status
// ready -> not_ready -> dead accordingly, bumping epoch on the transition
// to dead (see internal/store.MarkNodeDead and Section 6.5's fencing
// rationale). On dead, scheduled/running tasks are orphaned and requeued,
// and leftover DooD task containers are reaped when a Docker client is set.
//
// Only the elected leader runs the detector; followers no-op each tick
// (Phase 6). Pass a nil LeaderGate for always-leader (single-replica tests).
package failuredetector

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/dockerutil"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/metrics"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// LeaderGate reports whether this replica should evaluate node liveness.
type LeaderGate interface {
	IsLeader() bool
}

// Config holds the tunable thresholds, all derived by cmd/scheduler from a
// single --heartbeat-interval-ms flag (plus missed-interval multipliers) so
// the advertised heartbeat interval and the detector's thresholds can never
// drift out of sync with each other.
type Config struct {
	// PollInterval is how often the detector scans active nodes.
	PollInterval time.Duration
	// NotReadyAfter is the last-seen age at which a "ready" node is
	// downgraded to "not_ready" (a soft/suspect state; epoch is untouched).
	NotReadyAfter time.Duration
	// DeadAfter is the last-seen age at which a node is marked "dead" and
	// its epoch is bumped.
	DeadAfter time.Duration
}

// DefaultConfig returns thresholds derived from a heartbeat interval using
// the multipliers described in IMPLEMENTATION_PLAN.md Section 6.4 (poll at
// half the interval; not_ready after 2 missed beats; dead after 3).
func DefaultConfig(heartbeatInterval time.Duration) Config {
	return Config{
		PollInterval:  heartbeatInterval / 2,
		NotReadyAfter: heartbeatInterval * 2,
		DeadAfter:     heartbeatInterval * 3,
	}
}

// Detector runs the polling loop described in the package doc.
type Detector struct {
	store   *store.Store
	cache   *cache.Client
	docker  *dockerutil.Client
	logger  *slog.Logger
	cfg     Config
	leader  LeaderGate
	metrics *metrics.Registry
}

// New constructs a Detector without Docker reaping (tests / hosts without a
// socket). Prefer NewWithDocker in cmd/scheduler.
func New(s *store.Store, c *cache.Client, logger *slog.Logger, cfg Config, leader LeaderGate, met *metrics.Registry) *Detector {
	return NewWithDocker(s, c, nil, logger, cfg, leader, met)
}

// NewWithDocker is like New but installs a Docker client used to force-remove
// orphaned task containers after a node is marked dead.
func NewWithDocker(s *store.Store, c *cache.Client, docker *dockerutil.Client, logger *slog.Logger, cfg Config, leader LeaderGate, met *metrics.Registry) *Detector {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Detector{store: s, cache: c, docker: docker, logger: logger, cfg: cfg, leader: leader, metrics: met}
}

// Run blocks, polling every cfg.PollInterval until ctx is cancelled.
func (d *Detector) Run(ctx context.Context) {
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.tick(ctx)
		}
	}
}

func (d *Detector) tick(ctx context.Context) {
	if d.leader != nil && !d.leader.IsLeader() {
		return
	}
	nodes, err := d.store.ListActiveNodes(ctx)
	if err != nil {
		d.logger.Error("failuredetector: list active nodes", "error", err)
		return
	}

	for _, n := range nodes {
		d.evaluate(ctx, n)
	}
}

func (d *Detector) evaluate(ctx context.Context, n store.Node) {
	lastSeen, ok, err := d.cache.GetLastSeen(ctx, n.ID)
	if err != nil {
		d.logger.Error("failuredetector: get last seen", "node_id", n.ID, "error", err)
		return
	}

	age := time.Duration(1<<63 - 1)
	if ok {
		age = time.Since(lastSeen)
	}

	switch {
	case age >= d.cfg.DeadAfter:
		result, err := d.store.MarkNodeDead(ctx, n.ID)
		if err != nil {
			d.logger.Error("failuredetector: mark node dead", "node_id", n.ID, "error", err)
			return
		}
		if d.metrics != nil {
			d.metrics.HeartbeatMissesTotal.Inc()
			if ok {
				d.metrics.FailoverSeconds.Observe(age.Seconds())
			}
		}
		d.logger.Warn("node marked dead",
			"node_id", n.ID,
			"hostname", n.Hostname,
			"last_seen_age", age,
			"orphaned_tasks", len(result.OrphanedTaskIDs),
			"epoch", result.Node.Epoch,
		)
		// Reap asynchronously: DooD force-removes (especially under the VFS
		// storage driver) can take seconds per batch and would otherwise block
		// the detector tick — delaying dead detection for other nodes under
		// load and blowing the sub-3s failover p95.
		if d.docker != nil && len(result.OrphanedTaskIDs) > 0 {
			taskIDs := append([]string(nil), result.OrphanedTaskIDs...)
			dockerClient := d.docker
			logger := d.logger
			go func() {
				reapCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				if err := dockerClient.KillTaskContainers(reapCtx, taskIDs); err != nil {
					logger.Warn("failuredetector: reap orphan containers", "error", err)
				} else {
					logger.Info("reaped orphan task containers", "count", len(taskIDs))
				}
			}()
		}

	case age >= d.cfg.NotReadyAfter:
		if n.Status != store.NodeStatusReady {
			return
		}
		if _, err := d.store.UpdateNodeStatus(ctx, n.ID, store.NodeStatusNotReady, store.EventTypeNodeNotReady, "missed heartbeats; downgraded to not_ready"); err != nil {
			d.logger.Error("failuredetector: mark node not_ready", "node_id", n.ID, "error", err)
			return
		}
		if d.metrics != nil {
			d.metrics.HeartbeatMissesTotal.Inc()
		}
		d.logger.Warn("node marked not_ready", "node_id", n.ID, "hostname", n.Hostname, "last_seen_age", age)
	}
}
