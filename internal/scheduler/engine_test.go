package scheduler

import (
	"testing"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

func TestFirstFitPicksReadyNodeWithCapacity(t *testing.T) {
	nodes := []store.Node{
		{ID: "dead", Status: store.NodeStatusDead, CPUCapacityMillicores: 4000, MemCapacityMB: 4096},
		{ID: "full", Status: store.NodeStatusReady, CPUCapacityMillicores: 1000, MemCapacityMB: 512},
		{ID: "ok", Status: store.NodeStatusReady, CPUCapacityMillicores: 2000, MemCapacityMB: 1024},
	}
	allocs := map[string]store.NodeAllocation{
		"full": {CPUMillicores: 1000, MemoryMB: 512},
	}

	got := firstFit(nodes, allocs, 100, 64)
	if got == nil || got.ID != "ok" {
		t.Fatalf("expected node ok, got %+v", got)
	}
}

func TestFirstFitReturnsNilWhenNoCapacity(t *testing.T) {
	nodes := []store.Node{
		{ID: "a", Status: store.NodeStatusReady, CPUCapacityMillicores: 100, MemCapacityMB: 64},
	}
	allocs := map[string]store.NodeAllocation{
		"a": {CPUMillicores: 100, MemoryMB: 64},
	}
	if got := firstFit(nodes, allocs, 1, 1); got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestFirstFitSkipsNotReady(t *testing.T) {
	nodes := []store.Node{
		{ID: "nr", Status: store.NodeStatusNotReady, CPUCapacityMillicores: 4000, MemCapacityMB: 4096},
		{ID: "ready", Status: store.NodeStatusReady, CPUCapacityMillicores: 500, MemCapacityMB: 256},
	}
	got := firstFit(nodes, map[string]store.NodeAllocation{}, 100, 64)
	if got == nil || got.ID != "ready" {
		t.Fatalf("expected ready, got %+v", got)
	}
}
