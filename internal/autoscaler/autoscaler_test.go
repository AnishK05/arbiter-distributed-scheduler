package autoscaler

import (
	"testing"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

func TestFilterAutoscaled(t *testing.T) {
	nodes := []store.Node{
		{ID: "1", Hostname: "worker-1", Labels: map[string]string{}},
		{ID: "2", Hostname: "worker-auto-1", Labels: map[string]string{"autoscaled": "true"}},
		{ID: "3", Hostname: "worker-auto-2", Labels: map[string]string{"autoscaled": "false"}},
		{ID: "4", Hostname: "worker-auto-3", Labels: map[string]string{"autoscaled": "true"}, Status: store.NodeStatusDead},
	}
	got := filterAutoscaled(nodes)
	if len(got) != 2 {
		t.Fatalf("expected 2 autoscaled nodes, got %d", len(got))
	}
	if got[0].Hostname != "worker-auto-1" || got[1].Hostname != "worker-auto-3" {
		t.Fatalf("unexpected filter result: %+v", got)
	}
}

func TestDefaultConfigDisabled(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Enabled {
		t.Fatal("autoscaler should be disabled by default")
	}
	if cfg.PendingThreshold != 3 || cfg.MaxAutoscaledWorkers != 3 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestParseBoolEnv(t *testing.T) {
	t.Setenv("ARBITER_AUTOSCALER_TEST", "true")
	if !ParseBoolEnv("ARBITER_AUTOSCALER_TEST", false) {
		t.Fatal("expected true")
	}
	t.Setenv("ARBITER_AUTOSCALER_TEST", "0")
	if ParseBoolEnv("ARBITER_AUTOSCALER_TEST", true) {
		t.Fatal("expected false")
	}
	if !ParseBoolEnv("ARBITER_AUTOSCALER_MISSING", true) {
		t.Fatal("expected fallback true")
	}
}
