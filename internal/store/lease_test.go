package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func resetLeaderLease(t *testing.T) {
	t.Helper()
	connString := os.Getenv("ARBITER_TEST_POSTGRES_URL")
	raw, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		t.Fatalf("connect for lease reset: %v", err)
	}
	t.Cleanup(raw.Close)
	_, err = raw.Exec(context.Background(), `
		UPDATE leader_lease
		SET leader_id = '', leader_addr = '', epoch = 0,
		    acquired_at = now(), expires_at = TIMESTAMPTZ '1970-01-01 00:00:00+00'
		WHERE id = 1`)
	if err != nil {
		t.Fatalf("reset lease: %v", err)
	}
}

func expireLeaderLease(t *testing.T) {
	t.Helper()
	connString := os.Getenv("ARBITER_TEST_POSTGRES_URL")
	raw, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		t.Fatalf("connect for lease expire: %v", err)
	}
	defer raw.Close()
	_, err = raw.Exec(context.Background(), `
		UPDATE leader_lease
		SET expires_at = TIMESTAMPTZ '1970-01-01 00:00:00+00'
		WHERE id = 1`)
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func TestTryAcquireOrRenewLease(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	resetLeaderLease(t)

	ttl := 2 * time.Second
	a, err := db.TryAcquireOrRenewLease(ctx, "replica-a", "a:7000", ttl)
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	if !a.Acquired || !a.EpochBumped || a.Lease.Epoch != 1 {
		t.Fatalf("expected acquire with epoch bump to 1, got %+v", a)
	}

	b, err := db.TryAcquireOrRenewLease(ctx, "replica-b", "b:7000", ttl)
	if err != nil {
		t.Fatalf("acquire b while a holds: %v", err)
	}
	if b.Acquired {
		t.Fatal("replica-b should not steal a non-expired lease")
	}
	if b.Lease.LeaderID != "replica-a" || b.Lease.LeaderAddr != "a:7000" {
		t.Fatalf("expected a as holder, got %+v", b.Lease)
	}

	renew, err := db.TryAcquireOrRenewLease(ctx, "replica-a", "a:7000", ttl)
	if err != nil {
		t.Fatalf("renew a: %v", err)
	}
	if !renew.Acquired || renew.EpochBumped || renew.Lease.Epoch != 1 {
		t.Fatalf("renew should keep epoch 1, got %+v", renew)
	}

	expireLeaderLease(t)
	takeover, err := db.TryAcquireOrRenewLease(ctx, "replica-b", "b:7000", ttl)
	if err != nil {
		t.Fatalf("takeover: %v", err)
	}
	if !takeover.Acquired || !takeover.EpochBumped || takeover.Lease.Epoch != 2 {
		t.Fatalf("expected takeover epoch 2, got %+v", takeover)
	}
	if takeover.Lease.LeaderID != "replica-b" {
		t.Fatalf("expected replica-b, got %s", takeover.Lease.LeaderID)
	}

	events, err := db.ListEventsForEntity(ctx, "leader", "replica-b")
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	found := false
	for _, e := range events {
		if e.EventType == "leader_elected" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected leader_elected event for replica-b")
	}

	got, err := db.GetLeaderLease(ctx)
	if err != nil {
		t.Fatalf("GetLeaderLease: %v", err)
	}
	if got.LeaderID != "replica-b" || got.Epoch != 2 {
		t.Fatalf("GetLeaderLease mismatch: %+v", got)
	}
}
