package failuredetector

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// testDeps connects to real Postgres + Redis, skipping the test if either
// ARBITER_TEST_POSTGRES_URL or ARBITER_TEST_REDIS_ADDR isn't set. See
// internal/store/nodes_test.go and internal/cache/cache_test.go for the same
// pattern used individually; the failure detector needs both at once.
func testDeps(t *testing.T) (*store.Store, *cache.Client) {
	t.Helper()

	pgURL := os.Getenv("ARBITER_TEST_POSTGRES_URL")
	redisAddr := os.Getenv("ARBITER_TEST_REDIS_ADDR")
	if pgURL == "" || redisAddr == "" {
		t.Skip("ARBITER_TEST_POSTGRES_URL and ARBITER_TEST_REDIS_ADDR must both be set, skipping failuredetector integration test")
	}

	if err := store.Migrate(pgURL, os.DirFS("../../migrations")); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	db, err := store.Connect(context.Background(), pgURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(db.Close)

	c, err := cache.Connect(context.Background(), redisAddr)
	if err != nil {
		t.Fatalf("connect redis: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	return db, c
}

// testConfig uses thresholds only large enough to be unambiguous relative
// to each other; actual "staleness" in these tests is injected directly via
// cache.SetLastSeenAt rather than real sleeps, so the absolute durations
// don't matter for test speed.
var testConfig = Config{
	PollInterval:  time.Hour, // tests call tick() directly, never Run()
	NotReadyAfter: 2 * time.Second,
	DeadAfter:     4 * time.Second,
}

func registerTestNode(t *testing.T, db *store.Store, hostname string) store.Node {
	t.Helper()
	node, err := db.RegisterNode(context.Background(), store.RegisterNodeParams{
		Hostname:              hostname,
		Address:               hostname + ":8081",
		CPUCapacityMillicores: 1000,
		MemCapacityMB:         512,
	})
	if err != nil {
		t.Fatalf("RegisterNode(%s): %v", hostname, err)
	}
	return *node
}

func TestTickLeavesFreshNodeReady(t *testing.T) {
	db, c := testDeps(t)
	ctx := context.Background()
	d := New(db, c, nil, testConfig)

	node := registerTestNode(t, db, "fd-fresh")
	if err := c.SetLastSeen(ctx, node.ID); err != nil {
		t.Fatalf("SetLastSeen: %v", err)
	}

	d.tick(ctx)

	got, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Status != store.NodeStatusReady {
		t.Fatalf("expected status %q, got %q", store.NodeStatusReady, got.Status)
	}
	if got.Epoch != node.Epoch {
		t.Fatalf("expected epoch unchanged at %d, got %d", node.Epoch, got.Epoch)
	}
}

func TestTickMarksStaleNodeNotReady(t *testing.T) {
	db, c := testDeps(t)
	ctx := context.Background()
	d := New(db, c, nil, testConfig)

	node := registerTestNode(t, db, "fd-not-ready")
	staleBy := testConfig.NotReadyAfter + time.Second // past not_ready, short of dead
	if err := c.SetLastSeenAt(ctx, node.ID, time.Now().Add(-staleBy)); err != nil {
		t.Fatalf("SetLastSeenAt: %v", err)
	}

	d.tick(ctx)

	got, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Status != store.NodeStatusNotReady {
		t.Fatalf("expected status %q, got %q", store.NodeStatusNotReady, got.Status)
	}
	if got.Epoch != node.Epoch {
		t.Fatalf("expected epoch unchanged (not_ready doesn't fence) at %d, got %d", node.Epoch, got.Epoch)
	}
}

func TestTickMarksVeryStaleNodeDead(t *testing.T) {
	db, c := testDeps(t)
	ctx := context.Background()
	d := New(db, c, nil, testConfig)

	node := registerTestNode(t, db, "fd-dead")
	staleBy := testConfig.DeadAfter + time.Second
	if err := c.SetLastSeenAt(ctx, node.ID, time.Now().Add(-staleBy)); err != nil {
		t.Fatalf("SetLastSeenAt: %v", err)
	}

	d.tick(ctx)

	got, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Status != store.NodeStatusDead {
		t.Fatalf("expected status %q, got %q", store.NodeStatusDead, got.Status)
	}
	if got.Epoch != node.Epoch+1 {
		t.Fatalf("expected epoch to bump from %d to %d, got %d", node.Epoch, node.Epoch+1, got.Epoch)
	}
}

func TestTickTreatsMissingHeartbeatAsMaximallyStale(t *testing.T) {
	db, c := testDeps(t)
	ctx := context.Background()
	d := New(db, c, nil, testConfig)

	// Registering seeds Redis (internal/grpcapi.RegisterNode does this;
	// here we call the store directly, bypassing that, to simulate a node
	// whose heartbeat key was never set / already expired).
	node := registerTestNode(t, db, "fd-no-heartbeat")
	if err := c.DeleteLastSeen(ctx, node.ID); err != nil {
		t.Fatalf("DeleteLastSeen: %v", err)
	}

	d.tick(ctx)

	got, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Status != store.NodeStatusDead {
		t.Fatalf("expected a node with no recorded heartbeat to be treated as dead, got status %q", got.Status)
	}
}

func TestTickDoesNotReprocessAlreadyDeadNode(t *testing.T) {
	db, c := testDeps(t)
	ctx := context.Background()
	d := New(db, c, nil, testConfig)

	node := registerTestNode(t, db, "fd-already-dead")
	dead, err := db.MarkNodeDead(ctx, node.ID)
	if err != nil {
		t.Fatalf("MarkNodeDead: %v", err)
	}

	d.tick(ctx)

	got, err := db.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Epoch != dead.Node.Epoch {
		t.Fatalf("expected epoch to stay at %d for an already-dead node, got %d (ListActiveNodes should have excluded it)", dead.Node.Epoch, got.Epoch)
	}
}
