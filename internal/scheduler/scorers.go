package scheduler

import "github.com/AnishK05/arbiter-distributed-scheduler/internal/store"

// BinPackScorer prefers nodes that would be fullest after placement —
// consolidating load onto fewer nodes (IMPLEMENTATION_PLAN.md Section 6.3).
// Score is the mean of post-placement CPU and memory utilization.
type BinPackScorer struct{}

func (BinPackScorer) Name() string { return "bin_pack" }

func (BinPackScorer) Score(node NodeView, task store.TaskWithJob) float64 {
	after := NodeView{
		Node: node.Node,
		Alloc: store.NodeAllocation{
			CPUMillicores: node.Alloc.CPUMillicores + task.CPURequestMillicores,
			MemoryMB:      node.Alloc.MemoryMB + task.MemRequestMB,
		},
	}
	return after.Utilization()
}

// SpreadScorer prefers the least-loaded nodes (inverse of current
// utilization) so work fans out evenly across the cluster.
type SpreadScorer struct{}

func (SpreadScorer) Name() string { return "spread" }

func (SpreadScorer) Score(node NodeView, _ store.TaskWithJob) float64 {
	// Higher score = lower current utilization. The "+ epsilon" keeps a
	// fully-empty node from tying with pathological zero-capacity nodes.
	return 1.0 - node.Utilization()
}
