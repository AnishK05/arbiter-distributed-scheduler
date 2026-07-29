// Package scheduler implements Arbiter's placement engine. Phase 3 ships a
// naive first-fit loop (first ready node with enough free CPU/memory);
// Phase 4 replaces the placement decision with pluggable Filter/Scorer
// plugins (bin-pack / spread) without changing the claim/assign plumbing.
package scheduler

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

const (
	defaultPollInterval = 200 * time.Millisecond
	defaultClaimLimit   = 50
)

// Engine runs the background scheduling loop.
type Engine struct {
	store  *store.Store
	logger *slog.Logger

	pollInterval time.Duration
	claimLimit   int
}

// New constructs an Engine. logger may be nil (discard).
func New(s *store.Store, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Engine{
		store:        s,
		logger:       logger,
		pollInterval: defaultPollInterval,
		claimLimit:   defaultClaimLimit,
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

// tick claims a batch of pending tasks and places each with first-fit.
// Exported for tests via the unexported method being callable from the
// same package's _test.go; external packages use Run.
func (e *Engine) tick(ctx context.Context) error {
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
		node := firstFit(nodes, pendingAlloc, task.CPURequestMillicores, task.MemRequestMB)
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
		e.logger.Info("task scheduled",
			"task_id", task.ID,
			"job_id", task.JobID,
			"node_id", node.ID,
			"node_hostname", node.Hostname,
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

// firstFit returns the first ready node with enough residual capacity, or
// nil if none fit. Phase 4 replaces this with Filter → Score ranking.
func firstFit(nodes []store.Node, allocs map[string]store.NodeAllocation, cpuNeed, memNeed int64) *store.Node {
	for i := range nodes {
		n := &nodes[i]
		if n.Status != store.NodeStatusReady {
			continue
		}
		alloc := allocs[n.ID]
		freeCPU := n.CPUCapacityMillicores - alloc.CPUMillicores
		freeMem := n.MemCapacityMB - alloc.MemoryMB
		if freeCPU >= cpuNeed && freeMem >= memNeed {
			return n
		}
	}
	return nil
}
