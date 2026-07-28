// Package failuredetector implements Arbiter's heartbeat-based failure
// detector (IMPLEMENTATION_PLAN.md Section 6.4): a background loop that
// scans active nodes, compares each one's last-seen timestamp (from Redis)
// against configurable thresholds, and transitions status
// ready -> not_ready -> dead accordingly, bumping epoch on the transition
// to dead (see internal/store.MarkNodeDead and Section 6.5's fencing
// rationale).
//
// This runs unconditionally on the sole scheduler replica for now; Phase 6
// will gate it on "is the current leader" once multiple replicas exist.
package failuredetector

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

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
	store  *store.Store
	cache  *cache.Client
	logger *slog.Logger
	cfg    Config
}

// New constructs a Detector. logger may be nil, in which case a no-op
// discard logger is used (convenient for tests).
func New(s *store.Store, c *cache.Client, logger *slog.Logger, cfg Config) *Detector {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Detector{store: s, cache: c, logger: logger, cfg: cfg}
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

// tick evaluates every active node once. Exported indirectly via Run, but
// kept as its own method so tests can call it directly without waiting on a
// real ticker.
func (d *Detector) tick(ctx context.Context) {
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

	// No heartbeat on record at all (shouldn't normally happen —
	// RegisterNode seeds this — but could follow a Redis restart/flush).
	// Treat as maximally stale rather than skipping the node outright.
	age := time.Duration(1<<63 - 1) // effectively "infinite"
	if ok {
		age = time.Since(lastSeen)
	}

	switch {
	case age >= d.cfg.DeadAfter:
		if _, err := d.store.MarkNodeDead(ctx, n.ID); err != nil {
			d.logger.Error("failuredetector: mark node dead", "node_id", n.ID, "error", err)
			return
		}
		d.logger.Warn("node marked dead", "node_id", n.ID, "hostname", n.Hostname, "last_seen_age", age)

	case age >= d.cfg.NotReadyAfter:
		if n.Status != store.NodeStatusReady {
			return // already not_ready; avoid a redundant write+event every tick
		}
		if _, err := d.store.UpdateNodeStatus(ctx, n.ID, store.NodeStatusNotReady, store.EventTypeNodeNotReady, "missed heartbeats; downgraded to not_ready"); err != nil {
			d.logger.Error("failuredetector: mark node not_ready", "node_id", n.ID, "error", err)
			return
		}
		d.logger.Warn("node marked not_ready", "node_id", n.ID, "hostname", n.Hostname, "last_seen_age", age)
	}

	// A node recovering from not_ready back to ready happens immediately in
	// the Heartbeat handler as soon as a fresh heartbeat arrives (see
	// internal/grpcapi.Heartbeat), rather than waiting for this poll loop —
	// no case needed here for that direction.
}
