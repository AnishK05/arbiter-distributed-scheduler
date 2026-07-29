// Package scheduler implements Arbiter's placement engine. Phase 4 replaces
// Phase 3's naive first-fit with a pluggable Filter → Score pipeline
// (bin-pack / spread), selected per job via scheduling_policy
// (IMPLEMENTATION_PLAN.md Section 6.3).
package scheduler

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/metrics"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

const (
	defaultPollInterval = 200 * time.Millisecond
	defaultClaimLimit   = 50
)

// LeaderGate reports whether this replica should place tasks.
// nil means always-leader (single-replica / unit tests).
type LeaderGate interface {
	IsLeader() bool
}

// Engine runs the background scheduling loop.
type Engine struct {
	store   *store.Store
	logger  *slog.Logger
	leader  LeaderGate
	metrics *metrics.Registry

	pollInterval time.Duration
	claimLimit   int
	filters      []Filter
}

// New constructs an Engine. logger, leader, and met may be nil.
func New(s *store.Store, logger *slog.Logger, leader LeaderGate, met *metrics.Registry) *Engine {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Engine{
		store:        s,
		logger:       logger,
		leader:       leader,
		metrics:      met,
		pollInterval: defaultPollInterval,
		claimLimit:   defaultClaimLimit,
		filters:      DefaultFilters(),
	}
}

// Run blocks, claiming and placing pending tasks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.tick(ctx); err != nil {
				e.logger.Error("scheduler: tick failed", "error", err)
			}
		}
	}
}

// tick claims a batch of pending tasks and places each via Filter → Score.
// Followers (Phase 6) skip the tick entirely — only the elected leader places.
func (e *Engine) tick(ctx context.Context) error {
	if e.leader != nil && !e.leader.IsLeader() {
		return nil
	}
	tx, tasks, err := e.store.ClaimPendingTasksForScheduling(ctx, e.claimLimit)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		_ = tx.Rollback(ctx)
		return nil
	}
	defer func() { _ = tx.Rollback(ctx) }()

	nodes, err := e.store.ListActiveNodes(ctx)
	if err != nil {
		return err
	}
	allocs, err := e.store.GetNodeAllocations(ctx)
	if err != nil {
		return err
	}

	// Track allocations claimed earlier in this same tick so two tasks in
	// one batch don't both think a node's leftover capacity is free.
	pendingAlloc := make(map[string]store.NodeAllocation, len(allocs))
	for id, a := range allocs {
		pendingAlloc[id] = a
	}

	placed := 0
	for _, task := range tasks {
		scorer := ScorerForPolicy(task.SchedulingPolicy)
		node := Place(nodes, pendingAlloc, task, e.filters, scorer)
		if node == nil {
			continue // leave pending; free capacity may appear next tick
		}
		if err := e.store.ScheduleTask(ctx, tx, task.ID, node.ID, node.Epoch); err != nil {
			return err
		}
		a := pendingAlloc[node.ID]
		a.CPUMillicores += task.CPURequestMillicores
		a.MemoryMB += task.MemRequestMB
		pendingAlloc[node.ID] = a
		placed++
		if e.metrics != nil {
			e.metrics.ObserveTaskStatus(store.TaskStatusScheduled)
			e.metrics.SchedulingLatency.Observe(time.Since(task.CreatedAt).Seconds())
		}
		e.logger.Info("task scheduled",
			"task_id", task.ID,
			"job_id", task.JobID,
			"node_id", node.ID,
			"node_hostname", node.Hostname,
			"policy", scorer.Name(),
			"cpu_mc", task.CPURequestMillicores,
			"mem_mb", task.MemRequestMB,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if placed > 0 {
		e.logger.Info("scheduling tick complete", "claimed", len(tasks), "placed", placed)
	}
	return nil
}
