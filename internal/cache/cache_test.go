package cache_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
)

// testClient connects to a real Redis instance, skipping the test entirely
// if ARBITER_TEST_REDIS_ADDR isn't set. CI provides a Redis service
// container (see .github/workflows/ci.yml); `make up` + exporting
// ARBITER_TEST_REDIS_ADDR=localhost:6379 works for local runs too.
func testClient(t *testing.T) *cache.Client {
	t.Helper()

	addr := os.Getenv("ARBITER_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("ARBITER_TEST_REDIS_ADDR not set, skipping cache integration test")
	}

	c, err := cache.Connect(context.Background(), addr)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestSetGetLastSeenRoundTrip(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	nodeID := "test-node-round-trip"
	t.Cleanup(func() { _ = c.DeleteLastSeen(ctx, nodeID) })

	before := time.Now()
	if err := c.SetLastSeen(ctx, nodeID); err != nil {
		t.Fatalf("SetLastSeen: %v", err)
	}

	got, ok, err := c.GetLastSeen(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetLastSeen: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true after SetLastSeen")
	}
	if got.Before(before.Add(-time.Second)) || got.After(time.Now().Add(time.Second)) {
		t.Fatalf("got timestamp %v outside expected window around %v", got, before)
	}
}

func TestGetLastSeenMissingKey(t *testing.T) {
	c := testClient(t)

	_, ok, err := c.GetLastSeen(context.Background(), "node-that-never-registered")
	if err != nil {
		t.Fatalf("GetLastSeen: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for a node with no recorded heartbeat")
	}
}

func TestSetLastSeenAtExplicitTimestamp(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	nodeID := "test-node-explicit-ts"
	t.Cleanup(func() { _ = c.DeleteLastSeen(ctx, nodeID) })

	stale := time.Now().Add(-10 * time.Minute)
	if err := c.SetLastSeenAt(ctx, nodeID, stale); err != nil {
		t.Fatalf("SetLastSeenAt: %v", err)
	}

	got, ok, err := c.GetLastSeen(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetLastSeen: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.UnixMilli() != stale.UnixMilli() {
		t.Fatalf("expected timestamp %v, got %v", stale, got)
	}
}

func TestDeleteLastSeen(t *testing.T) {
	c := testClient(t)
	ctx := context.Background()
	nodeID := "test-node-delete"

	if err := c.SetLastSeen(ctx, nodeID); err != nil {
		t.Fatalf("SetLastSeen: %v", err)
	}
	if err := c.DeleteLastSeen(ctx, nodeID); err != nil {
		t.Fatalf("DeleteLastSeen: %v", err)
	}

	_, ok, err := c.GetLastSeen(ctx, nodeID)
	if err != nil {
		t.Fatalf("GetLastSeen: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false after DeleteLastSeen")
	}
}
