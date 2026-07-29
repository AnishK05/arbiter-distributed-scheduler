package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// testDB connects to a real Postgres instance and applies migrations,
// skipping the test entirely if ARBITER_TEST_POSTGRES_URL isn't set (e.g. a
// local `go test ./...` run without Postgres available). CI provides a
// Postgres service container and sets this env var (see
// .github/workflows/ci.yml); `make up` + exporting the same URL used by
// deploy/docker-compose.yml works for local runs too.
func testDB(t *testing.T) *store.Store {
	t.Helper()

	connString := os.Getenv("ARBITER_TEST_POSTGRES_URL")
	if connString == "" {
		t.Skip("ARBITER_TEST_POSTGRES_URL not set, skipping store integration test")
	}

	if err := store.Migrate(connString, os.DirFS("../../migrations")); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Each test in this file registers nodes with unique hostnames, but
	// clear the table up front too so re-runs against a persistent local
	// Postgres (outside of CI's fresh container) start from a clean slate.
	rawPool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		t.Fatalf("connect for truncate: %v", err)
	}
	defer rawPool.Close()
	var truncateErr error
	for attempt := 0; attempt < 8; attempt++ {
		_, truncateErr = rawPool.Exec(context.Background(), "TRUNCATE tasks, jobs, events, nodes CASCADE")
		if truncateErr == nil {
			break
		}
		// Parallel packages share the CI Postgres instance; brief deadlocks on
		// TRUNCATE are expected under `go test ./...`.
		time.Sleep(time.Duration(50*(attempt+1)) * time.Millisecond)
	}
	if truncateErr != nil {
		t.Fatalf("truncate tables: %v", truncateErr)
	}

	db, err := store.Connect(context.Background(), connString)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(db.Close)

	return db
}

func TestRegisterNodeCreatesNewNode(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "node-a",
		Address:               "node-a:8081",
		CPUCapacityMillicores: 2000,
		MemCapacityMB:         1024,
		Labels:                map[string]string{"zone": "us-east"},
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	if node.ID == "" {
		t.Fatal("expected a generated node ID")
	}
	if node.Status != store.NodeStatusReady {
		t.Fatalf("expected status %q, got %q", store.NodeStatusReady, node.Status)
	}
	if node.Epoch != 0 {
		t.Fatalf("expected epoch 0 for a brand new node, got %d", node.Epoch)
	}
	if node.CPUCapacityMillicores != 2000 || node.MemCapacityMB != 1024 {
		t.Fatalf("capacity mismatch: got cpu=%d mem=%d", node.CPUCapacityMillicores, node.MemCapacityMB)
	}
	if node.Labels["zone"] != "us-east" {
		t.Fatalf("expected label zone=us-east, got %v", node.Labels)
	}

	fetched, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if fetched.Hostname != "node-a" {
		t.Fatalf("expected hostname node-a, got %q", fetched.Hostname)
	}
}

func TestRegisterNodeUpsertsOnRestart(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	params := store.RegisterNodeParams{
		Hostname:              "node-b",
		Address:               "node-b:8081",
		CPUCapacityMillicores: 1000,
		MemCapacityMB:         512,
	}

	first, err := db.RegisterNode(ctx, params)
	if err != nil {
		t.Fatalf("first RegisterNode: %v", err)
	}
	if first.Epoch != 0 {
		t.Fatalf("expected epoch 0 on first registration, got %d", first.Epoch)
	}

	// Simulate the worker restarting and re-registering from the same
	// hostname/address, possibly with updated capacity.
	params.CPUCapacityMillicores = 4000
	second, err := db.RegisterNode(ctx, params)
	if err != nil {
		t.Fatalf("second RegisterNode: %v", err)
	}

	if second.ID != first.ID {
		t.Fatalf("expected re-registration to reuse the same node ID: first=%s second=%s", first.ID, second.ID)
	}
	if second.Epoch != first.Epoch+1 {
		t.Fatalf("expected epoch to increment by 1 on re-registration, got first=%d second=%d", first.Epoch, second.Epoch)
	}
	if second.CPUCapacityMillicores != 4000 {
		t.Fatalf("expected updated CPU capacity 4000, got %d", second.CPUCapacityMillicores)
	}

	nodes, err := db.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	count := 0
	for _, n := range nodes {
		if n.Hostname == "node-b" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 row for node-b after re-registration, found %d", count)
	}
}

func TestGetNodeNotFound(t *testing.T) {
	db := testDB(t)

	_, err := db.GetNode(context.Background(), "00000000-0000-0000-0000-000000000000")
	if err != store.ErrNodeNotFound {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestListNodesOrdersByCreationTime(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	for _, hostname := range []string{"node-c1", "node-c2", "node-c3"} {
		if _, err := db.RegisterNode(ctx, store.RegisterNodeParams{
			Hostname:              hostname,
			Address:               hostname + ":8081",
			CPUCapacityMillicores: 1000,
			MemCapacityMB:         512,
		}); err != nil {
			t.Fatalf("RegisterNode(%s): %v", hostname, err)
		}
	}

	nodes, err := db.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}
	for i, want := range []string{"node-c1", "node-c2", "node-c3"} {
		if nodes[i].Hostname != want {
			t.Fatalf("expected nodes[%d].Hostname = %q, got %q", i, want, nodes[i].Hostname)
		}
	}
}
