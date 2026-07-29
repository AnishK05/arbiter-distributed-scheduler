package scheduler

import "github.com/AnishK05/arbiter-distributed-scheduler/internal/store"

// ReadyFilter admits only nodes in the ready state (IMPLEMENTATION_PLAN.md
// Section 6.3 filter #1).
type ReadyFilter struct{}

func (ReadyFilter) Name() string { return "ready" }

func (ReadyFilter) Passes(node NodeView, _ store.TaskWithJob) bool {
	return node.Node.Status == store.NodeStatusReady
}

// ResourceFilter admits nodes with enough residual CPU and memory for the
// task request (IMPLEMENTATION_PLAN.md Section 6.3 filter #2).
type ResourceFilter struct{}

func (ResourceFilter) Name() string { return "resources" }

func (ResourceFilter) Passes(node NodeView, task store.TaskWithJob) bool {
	return node.FreeCPUMillicores() >= task.CPURequestMillicores &&
		node.FreeMemMB() >= task.MemRequestMB
}

// LabelSelectorFilter requires every key/value in the job's constraints map
// to match the node's labels (AND semantics, like a Kubernetes nodeSelector).
// An empty constraints map always passes.
type LabelSelectorFilter struct{}

func (LabelSelectorFilter) Name() string { return "label_selector" }

func (LabelSelectorFilter) Passes(node NodeView, task store.TaskWithJob) bool {
	if len(task.Constraints) == 0 {
		return true
	}
	labels := node.Node.Labels
	if labels == nil {
		return false
	}
	for k, v := range task.Constraints {
		if labels[k] != v {
			return false
		}
	}
	return true
}
