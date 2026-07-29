package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// LeaderLease mirrors the single-row `leader_lease` table
// (IMPLEMENTATION_PLAN.md Section 6.6).
type LeaderLease struct {
	LeaderID   string
	LeaderAddr string
	Epoch      int64
	AcquiredAt time.Time
	ExpiresAt  time.Time
}

// LeaseAcquireResult is returned by TryAcquireOrRenewLease.
type LeaseAcquireResult struct {
	// Acquired is true when this replica holds the lease after the call.
	Acquired bool
	// Lease is the row state after the call (whether or not we hold it).
	Lease LeaderLease
	// EpochBumped is true when we took the lease from another (or empty) holder.
	EpochBumped bool
}

// TryAcquireOrRenewLease implements the Postgres lease algorithm from
// IMPLEMENTATION_PLAN.md Section 6.6: take the lease if it is expired or
// already ours; otherwise leave it and return the current holder's address.
func (s *Store) TryAcquireOrRenewLease(ctx context.Context, identity, advertiseAddr string, ttl time.Duration) (*LeaseAcquireResult, error) {
	if identity == "" {
		return nil, fmt.Errorf("store: lease identity is required")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("store: lease ttl must be positive")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: lease begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lease, err := scanLease(tx.QueryRow(ctx, `
		SELECT leader_id, leader_addr, epoch, acquired_at, expires_at
		FROM leader_lease WHERE id = 1
		FOR UPDATE`))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("store: leader_lease row missing (migrations not applied?)")
	}
	if err != nil {
		return nil, fmt.Errorf("store: lock leader_lease: %w", err)
	}

	now := time.Now().UTC()
	expired := !lease.ExpiresAt.After(now)
	ours := lease.LeaderID == identity

	result := &LeaseAcquireResult{Lease: *lease}
	if !expired && !ours {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return result, nil
	}

	newEpoch := lease.Epoch
	if !ours {
		newEpoch++
		result.EpochBumped = true
	}
	ttlSeconds := ttl.Seconds()
	updated, err := scanLease(tx.QueryRow(ctx, `
		UPDATE leader_lease
		SET leader_id = $1,
		    leader_addr = $2,
		    epoch = $3,
		    acquired_at = $4,
		    expires_at = $4 + ($5 * interval '1 second')
		WHERE id = 1
		RETURNING leader_id, leader_addr, epoch, acquired_at, expires_at`,
		identity, advertiseAddr, newEpoch, now, ttlSeconds))
	if err != nil {
		return nil, fmt.Errorf("store: update leader_lease: %w", err)
	}

	if result.EpochBumped {
		msg := fmt.Sprintf("leader elected: id=%s addr=%s epoch=%d", identity, advertiseAddr, newEpoch)
		if err := insertEvent(ctx, tx, EntityTypeLeader, identity, EventTypeLeaderElected, msg); err != nil {
			return nil, fmt.Errorf("store: leader elected event: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	result.Acquired = true
	result.Lease = *updated
	return result, nil
}

// GetLeaderLease returns the current lease row without locking.
func (s *Store) GetLeaderLease(ctx context.Context) (*LeaderLease, error) {
	lease, err := scanLease(s.pool.QueryRow(ctx, `
		SELECT leader_id, leader_addr, epoch, acquired_at, expires_at
		FROM leader_lease WHERE id = 1`))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("store: leader_lease row missing")
	}
	if err != nil {
		return nil, fmt.Errorf("store: get leader_lease: %w", err)
	}
	return lease, nil
}

func scanLease(row rowScanner) (*LeaderLease, error) {
	var l LeaderLease
	if err := row.Scan(&l.LeaderID, &l.LeaderAddr, &l.Epoch, &l.AcquiredAt, &l.ExpiresAt); err != nil {
		return nil, err
	}
	return &l, nil
}
