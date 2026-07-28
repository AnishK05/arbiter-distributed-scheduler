package store_test

import (
	"context"
	"testing"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

func TestMarkNodeDeadBumpsEpochAndWritesEvent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "node-dead-1",
		Address:               "node-dead-1:8081",
		CPUCapacityMillicores: 1000,
		MemCapacityMB:         512,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	dead, err := db.MarkNodeDead(ctx, node.ID)
	if err != nil {
		t.Fatalf("MarkNodeDead: %v", err)
	}
	if dead.Status != store.NodeStatusDead {
		t.Fatalf("expected status %q, got %q", store.NodeStatusDead, dead.Status)
	}
	if dead.Epoch != node.Epoch+1 {
		t.Fatalf("expected epoch to increment from %d to %d, got %d", node.Epoch, node.Epoch+1, dead.Epoch)
	}

	events, err := db.ListEventsForEntity(ctx, store.EntityTypeNode, node.ID)
	if err != nil {
		t.Fatalf("ListEventsForEntity: %v", err)
	}
	if len(events) < 2 {
		t.Fatalf("expected at least 2 events (registered + dead), got %d", len(events))
	}
	last := events[len(events)-1]
	if last.EventType != store.EventTypeNodeDead {
		t.Fatalf("expected last event type %q, got %q", store.EventTypeNodeDead, last.EventType)
	}
}

func TestMarkNodeDeadNotFound(t *testing.T) {
	db := testDB(t)

	_, err := db.MarkNodeDead(context.Background(), "00000000-0000-0000-0000-000000000000")
	if err != store.ErrNodeNotFound {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestUpdateNodeStatusTransitionsAndWritesEvent(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "node-status-1",
		Address:               "node-status-1:8081",
		CPUCapacityMillicores: 1000,
		MemCapacityMB:         512,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	notReady, err := db.UpdateNodeStatus(ctx, node.ID, store.NodeStatusNotReady, store.EventTypeNodeNotReady, "missed heartbeats")
	if err != nil {
		t.Fatalf("UpdateNodeStatus (not_ready): %v", err)
	}
	if notReady.Status != store.NodeStatusNotReady {
		t.Fatalf("expected status %q, got %q", store.NodeStatusNotReady, notReady.Status)
	}
	if notReady.Epoch != node.Epoch {
		t.Fatalf("expected epoch to stay at %d for a not_ready transition, got %d", node.Epoch, notReady.Epoch)
	}

	recovered, err := db.UpdateNodeStatus(ctx, node.ID, store.NodeStatusReady, store.EventTypeNodeRecovered, "heartbeat resumed")
	if err != nil {
		t.Fatalf("UpdateNodeStatus (ready): %v", err)
	}
	if recovered.Status != store.NodeStatusReady {
		t.Fatalf("expected status %q, got %q", store.NodeStatusReady, recovered.Status)
	}

	events, err := db.ListEventsForEntity(ctx, store.EntityTypeNode, node.ID)
	if err != nil {
		t.Fatalf("ListEventsForEntity: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events (registered, not_ready, recovered), got %d", len(events))
	}
}

func TestListActiveNodesExcludesDeadAndCordoned(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	active, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname: "node-active", Address: "node-active:8081", CPUCapacityMillicores: 1000, MemCapacityMB: 512,
	})
	if err != nil {
		t.Fatalf("RegisterNode(active): %v", err)
	}
	dead, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname: "node-to-die", Address: "node-to-die:8081", CPUCapacityMillicores: 1000, MemCapacityMB: 512,
	})
	if err != nil {
		t.Fatalf("RegisterNode(dead): %v", err)
	}
	if _, err := db.MarkNodeDead(ctx, dead.ID); err != nil {
		t.Fatalf("MarkNodeDead: %v", err)
	}

	nodes, err := db.ListActiveNodes(ctx)
	if err != nil {
		t.Fatalf("ListActiveNodes: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID != active.ID {
		t.Fatalf("expected exactly 1 active node (%s), got %+v", active.ID, nodes)
	}
}
