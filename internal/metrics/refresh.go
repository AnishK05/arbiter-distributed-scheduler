package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// LeaderGate reports whether this replica should refresh cluster gauges.
type LeaderGate interface {
	IsLeader() bool
}

// StoreStats is the subset of store used by the gauge refresher.
type StoreStats interface {
	CountTasksByStatus(ctx context.Context) (map[string]int64, error)
	CountNodesByStatus(ctx context.Context) (map[string]int64, error)
	ListNodes(ctx context.Context) ([]store.Node, error)
	GetNodeAllocations(ctx context.Context) (map[string]store.NodeAllocation, error)
}

// RunClusterGauges periodically refreshes queue depth, node counts, task
// running count, and per-node utilization from Postgres. Only the leader
// writes cluster gauges; followers zero is_leader and skip the rest.
func (r *Registry) RunClusterGauges(ctx context.Context, logger *slog.Logger, s StoreStats, leader LeaderGate, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	refresh := func() {
		if leader != nil && !leader.IsLeader() {
			r.IsLeader.Set(0)
			return
		}
		r.IsLeader.Set(1)

		taskCounts, err := s.CountTasksByStatus(ctx)
		if err != nil {
			logger.Warn("metrics: count tasks", "error", err)
		} else {
			r.QueueDepth.Set(float64(taskCounts[store.TaskStatusPending]))
			r.TasksRunning.Set(float64(taskCounts[store.TaskStatusRunning]))
		}

		nodeCounts, err := s.CountNodesByStatus(ctx)
		if err != nil {
			logger.Warn("metrics: count nodes", "error", err)
		} else {
			for _, status := range []string{
				store.NodeStatusReady,
				store.NodeStatusNotReady,
				store.NodeStatusDead,
				store.NodeStatusCordoned,
				store.NodeStatusUnknown,
			} {
				r.NodesTotal.WithLabelValues(status).Set(float64(nodeCounts[status]))
			}
		}

		nodes, err := s.ListNodes(ctx)
		if err != nil {
			logger.Warn("metrics: list nodes", "error", err)
			return
		}
		allocs, err := s.GetNodeAllocations(ctx)
		if err != nil {
			logger.Warn("metrics: node allocations", "error", err)
			return
		}
		r.NodeCPUCapacity.Reset()
		r.NodeCPUAllocated.Reset()
		r.NodeMemCapacity.Reset()
		r.NodeMemAllocated.Reset()
		for _, n := range nodes {
			if n.Status == store.NodeStatusDead {
				continue
			}
			labels := prometheusLabels(n.ID, n.Hostname)
			r.NodeCPUCapacity.WithLabelValues(labels...).Set(float64(n.CPUCapacityMillicores))
			r.NodeMemCapacity.WithLabelValues(labels...).Set(float64(n.MemCapacityMB))
			a := allocs[n.ID]
			r.NodeCPUAllocated.WithLabelValues(labels...).Set(float64(a.CPUMillicores))
			r.NodeMemAllocated.WithLabelValues(labels...).Set(float64(a.MemoryMB))
		}
	}

	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func prometheusLabels(nodeID, hostname string) []string {
	return []string{nodeID, hostname}
}
