package scheduler

import (
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// NodeView is a node plus its current allocated resources (including any
// reservations already claimed earlier in the same scheduling tick).
type NodeView struct {
	Node  store.Node
	Alloc store.NodeAllocation
}

// FreeCPUMillicores is residual CPU capacity after the current allocation.
func (v NodeView) FreeCPUMillicores() int64 {
	return v.Node.CPUCapacityMillicores - v.Alloc.CPUMillicores
}

// FreeMemMB is residual memory capacity after the current allocation.
func (v NodeView) FreeMemMB() int64 {
	return v.Node.MemCapacityMB - v.Alloc.MemoryMB
}

// CPUUtilization is allocated/capacity in [0, +inf). Capacity 0 → 1.0.
func (v NodeView) CPUUtilization() float64 {
	if v.Node.CPUCapacityMillicores <= 0 {
		return 1
	}
	return float64(v.Alloc.CPUMillicores) / float64(v.Node.CPUCapacityMillicores)
}

// MemUtilization is allocated/capacity in [0, +inf). Capacity 0 → 1.0.
func (v NodeView) MemUtilization() float64 {
	if v.Node.MemCapacityMB <= 0 {
		return 1
	}
	return float64(v.Alloc.MemoryMB) / float64(v.Node.MemCapacityMB)
}

// Utilization is the mean of CPU and memory utilization.
func (v NodeView) Utilization() float64 {
	return (v.CPUUtilization() + v.MemUtilization()) / 2
}

// Filter is a hard constraint: a node either passes or is eliminated
// (IMPLEMENTATION_PLAN.md Section 6.3).
type Filter interface {
	Name() string
	Passes(node NodeView, task store.TaskWithJob) bool
}

// Scorer ranks eligible nodes; higher scores are preferred.
type Scorer interface {
	Name() string
	Score(node NodeView, task store.TaskWithJob) float64
}

// DefaultFilters returns the Phase 4 hard-constraint chain: ready status,
// residual CPU/memory, and job label selectors.
func DefaultFilters() []Filter {
	return []Filter{
		ReadyFilter{},
		ResourceFilter{},
		LabelSelectorFilter{},
	}
}

// ScorerForPolicy returns the scorer selected by a job's scheduling_policy.
// Unknown / empty policies fall back to bin-pack (the default).
func ScorerForPolicy(policy string) Scorer {
	switch policy {
	case store.SchedulingPolicySpread:
		return SpreadScorer{}
	default:
		return BinPackScorer{}
	}
}

// Place picks the best eligible node for task given current allocations.
// Returns nil when no node passes every filter.
func Place(nodes []store.Node, allocs map[string]store.NodeAllocation, task store.TaskWithJob, filters []Filter, scorer Scorer) *store.Node {
	if scorer == nil {
		scorer = BinPackScorer{}
	}
	if filters == nil {
		filters = DefaultFilters()
	}

	var (
		best      *store.Node
		bestScore float64
		found     bool
	)
	for i := range nodes {
		view := NodeView{Node: nodes[i], Alloc: allocs[nodes[i].ID]}
		eligible := true
		for _, f := range filters {
			if !f.Passes(view, task) {
				eligible = false
				break
			}
		}
		if !eligible {
			continue
		}
		score := scorer.Score(view, task)
		if !found || score > bestScore || (score == bestScore && nodes[i].Hostname < best.Hostname) {
			best = &nodes[i]
			bestScore = score
			found = true
		}
	}
	if !found {
		return nil
	}
	return best
}
