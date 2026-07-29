package scheduler

import (
	"testing"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

func testNodes() []store.Node {
	return []store.Node{
		{ID: "n1", Hostname: "worker-1", Status: store.NodeStatusReady, CPUCapacityMillicores: 2000, MemCapacityMB: 1024, Labels: map[string]string{"zone": "a"}},
		{ID: "n2", Hostname: "worker-2", Status: store.NodeStatusReady, CPUCapacityMillicores: 2000, MemCapacityMB: 1024, Labels: map[string]string{"zone": "b"}},
		{ID: "n3", Hostname: "worker-3", Status: store.NodeStatusReady, CPUCapacityMillicores: 2000, MemCapacityMB: 1024, Labels: map[string]string{"zone": "a"}},
		{ID: "dead", Hostname: "dead", Status: store.NodeStatusDead, CPUCapacityMillicores: 8000, MemCapacityMB: 8192},
		{ID: "nr", Hostname: "not-ready", Status: store.NodeStatusNotReady, CPUCapacityMillicores: 8000, MemCapacityMB: 8192},
	}
}

func taskReq(cpu, mem int64, policy string, constraints map[string]string) store.TaskWithJob {
	return store.TaskWithJob{
		CPURequestMillicores: cpu,
		MemRequestMB:         mem,
		SchedulingPolicy:     policy,
		Constraints:          constraints,
	}
}

func TestReadyFilterRejectsNonReady(t *testing.T) {
	f := ReadyFilter{}
	ready := NodeView{Node: store.Node{Status: store.NodeStatusReady}}
	dead := NodeView{Node: store.Node{Status: store.NodeStatusDead}}
	if !f.Passes(ready, store.TaskWithJob{}) {
		t.Fatal("ready should pass")
	}
	if f.Passes(dead, store.TaskWithJob{}) {
		t.Fatal("dead should fail")
	}
}

func TestResourceFilterRespectsResidualCapacity(t *testing.T) {
	f := ResourceFilter{}
	view := NodeView{
		Node:  store.Node{CPUCapacityMillicores: 1000, MemCapacityMB: 512},
		Alloc: store.NodeAllocation{CPUMillicores: 900, MemoryMB: 400},
	}
	if !f.Passes(view, taskReq(100, 64, "", nil)) {
		t.Fatal("exact fit should pass")
	}
	if f.Passes(view, taskReq(101, 64, "", nil)) {
		t.Fatal("cpu overcommit should fail")
	}
	if f.Passes(view, taskReq(100, 113, "", nil)) {
		t.Fatal("mem overcommit should fail")
	}
}

func TestLabelSelectorFilter(t *testing.T) {
	f := LabelSelectorFilter{}
	view := NodeView{Node: store.Node{Labels: map[string]string{"zone": "a", "gpu": "true"}}}

	if !f.Passes(view, taskReq(1, 1, "", nil)) {
		t.Fatal("empty constraints should pass")
	}
	if !f.Passes(view, taskReq(1, 1, "", map[string]string{"zone": "a"})) {
		t.Fatal("matching zone should pass")
	}
	if f.Passes(view, taskReq(1, 1, "", map[string]string{"zone": "b"})) {
		t.Fatal("mismatched zone should fail")
	}
	if f.Passes(view, taskReq(1, 1, "", map[string]string{"missing": "x"})) {
		t.Fatal("missing label should fail")
	}
}

func TestBinPackPrefersFullerNode(t *testing.T) {
	nodes := testNodes()[:3] // n1, n2, n3
	allocs := map[string]store.NodeAllocation{
		"n1": {CPUMillicores: 1500, MemoryMB: 512},
		"n2": {CPUMillicores: 0, MemoryMB: 0},
		"n3": {CPUMillicores: 500, MemoryMB: 128},
	}
	got := Place(nodes, allocs, taskReq(100, 64, store.SchedulingPolicyBinPack, nil), DefaultFilters(), BinPackScorer{})
	if got == nil || got.ID != "n1" {
		t.Fatalf("bin_pack should prefer fullest feasible node n1, got %+v", got)
	}
}

func TestSpreadPrefersEmptiestNode(t *testing.T) {
	nodes := testNodes()[:3]
	allocs := map[string]store.NodeAllocation{
		"n1": {CPUMillicores: 1500, MemoryMB: 512},
		"n2": {CPUMillicores: 0, MemoryMB: 0},
		"n3": {CPUMillicores: 500, MemoryMB: 128},
	}
	got := Place(nodes, allocs, taskReq(100, 64, store.SchedulingPolicySpread, nil), DefaultFilters(), SpreadScorer{})
	if got == nil || got.ID != "n2" {
		t.Fatalf("spread should prefer emptiest node n2, got %+v", got)
	}
}

func TestBinPackFillsOneNodeBeforeNext(t *testing.T) {
	nodes := testNodes()[:3]
	allocs := map[string]store.NodeAllocation{}
	task := taskReq(500, 128, store.SchedulingPolicyBinPack, nil)
	counts := map[string]int{}
	for i := 0; i < 9; i++ { // 9 * 500 = 4500 of 6000 total CPU
		got := Place(nodes, allocs, task, DefaultFilters(), BinPackScorer{})
		if got == nil {
			t.Fatalf("placement %d failed", i)
		}
		a := allocs[got.ID]
		a.CPUMillicores += task.CPURequestMillicores
		a.MemoryMB += task.MemRequestMB
		allocs[got.ID] = a
		counts[got.Hostname]++
	}
	// Bin-pack should fill worker-1 (4 tasks = 2000m) then worker-2 (4) then worker-3 (1).
	if counts["worker-1"] != 4 || counts["worker-2"] != 4 || counts["worker-3"] != 1 {
		t.Fatalf("unexpected bin_pack distribution: %v", counts)
	}
}

func TestSpreadDistributesEvenly(t *testing.T) {
	nodes := testNodes()[:3]
	allocs := map[string]store.NodeAllocation{}
	task := taskReq(100, 64, store.SchedulingPolicySpread, nil)
	counts := map[string]int{}
	for i := 0; i < 9; i++ {
		got := Place(nodes, allocs, task, DefaultFilters(), SpreadScorer{})
		if got == nil {
			t.Fatalf("placement %d failed", i)
		}
		a := allocs[got.ID]
		a.CPUMillicores += task.CPURequestMillicores
		a.MemoryMB += task.MemRequestMB
		allocs[got.ID] = a
		counts[got.Hostname]++
	}
	for _, host := range []string{"worker-1", "worker-2", "worker-3"} {
		if counts[host] != 3 {
			t.Fatalf("spread should place 3 tasks per node, got %v", counts)
		}
	}
}

func TestPlaceNeverOvercommits(t *testing.T) {
	nodes := []store.Node{
		{ID: "only", Hostname: "only", Status: store.NodeStatusReady, CPUCapacityMillicores: 1000, MemCapacityMB: 512},
	}
	allocs := map[string]store.NodeAllocation{}
	task := taskReq(400, 128, store.SchedulingPolicyBinPack, nil)
	for i := 0; i < 2; i++ {
		got := Place(nodes, allocs, task, DefaultFilters(), BinPackScorer{})
		if got == nil {
			t.Fatalf("expected placement %d to succeed", i)
		}
		a := allocs[got.ID]
		a.CPUMillicores += task.CPURequestMillicores
		a.MemoryMB += task.MemRequestMB
		allocs[got.ID] = a
	}
	if got := Place(nodes, allocs, task, DefaultFilters(), BinPackScorer{}); got != nil {
		t.Fatalf("third 400m task should not fit on 1000m node, got %+v", got)
	}
	a := allocs["only"]
	if a.CPUMillicores > 1000 || a.MemoryMB > 512 {
		t.Fatalf("overcommit detected: %+v", a)
	}
}

func TestPlaceHonorsLabelConstraints(t *testing.T) {
	nodes := testNodes()[:3]
	got := Place(nodes, nil, taskReq(100, 64, store.SchedulingPolicyBinPack, map[string]string{"zone": "b"}), DefaultFilters(), BinPackScorer{})
	if got == nil || got.ID != "n2" {
		t.Fatalf("expected only zone=b node n2, got %+v", got)
	}
}

func TestScorerForPolicy(t *testing.T) {
	if ScorerForPolicy(store.SchedulingPolicySpread).Name() != "spread" {
		t.Fatal("expected spread scorer")
	}
	if ScorerForPolicy(store.SchedulingPolicyBinPack).Name() != "bin_pack" {
		t.Fatal("expected bin_pack scorer")
	}
	if ScorerForPolicy("").Name() != "bin_pack" {
		t.Fatal("empty policy should default to bin_pack")
	}
}
