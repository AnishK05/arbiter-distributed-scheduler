// Package election implements Postgres-lease leader election for Arbiter
// scheduler replicas (IMPLEMENTATION_PLAN.md Section 6.6).
package election

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

const (
	DefaultLeaseTTL      = 5 * time.Second
	DefaultRenewInterval = 1 * time.Second
)

// Config tunes the lease loop.
type Config struct {
	// Identity uniquely names this replica (stored as leader_id).
	Identity string
	// AdvertiseAddr is the gRPC host:port clients/workers should dial when
	// this replica is leader (returned in NOT_LEADER errors).
	AdvertiseAddr string
	LeaseTTL      time.Duration
	RenewInterval time.Duration
}

// Elector runs the acquire/renew loop and exposes leadership state.
type Elector struct {
	store  *store.Store
	logger *slog.Logger
	cfg    Config

	mu         sync.RWMutex
	isLeader   bool
	epoch      int64
	leaderAddr string
}

// New constructs an Elector. logger may be nil.
func New(s *store.Store, logger *slog.Logger, cfg Config) *Elector {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = DefaultLeaseTTL
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = DefaultRenewInterval
	}
	return &Elector{
		store:  s,
		logger: logger,
		cfg:    cfg,
	}
}

// Bootstrap performs a single lease attempt so leadership state is known
// before the gRPC server starts accepting traffic.
func (e *Elector) Bootstrap(ctx context.Context) {
	e.tick(ctx)
}

// Run blocks, renewing/acquiring the lease until ctx is cancelled.
func (e *Elector) Run(ctx context.Context) {
	ticker := time.NewTicker(e.cfg.RenewInterval)
	defer ticker.Stop()

	// Attempt immediately so startup doesn't wait a full renew interval.
	e.tick(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.tick(ctx)
		}
	}
}

func (e *Elector) tick(ctx context.Context) {
	result, err := e.store.TryAcquireOrRenewLease(ctx, e.cfg.Identity, e.cfg.AdvertiseAddr, e.cfg.LeaseTTL)
	if err != nil {
		e.logger.Error("election: lease tick failed", "error", err)
		return
	}

	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = result.Acquired
	e.epoch = result.Lease.Epoch
	if result.Acquired {
		e.leaderAddr = e.cfg.AdvertiseAddr
	} else {
		e.leaderAddr = result.Lease.LeaderAddr
	}
	e.mu.Unlock()

	switch {
	case result.Acquired && !wasLeader:
		e.logger.Info("became leader",
			"identity", e.cfg.Identity,
			"addr", e.cfg.AdvertiseAddr,
			"epoch", result.Lease.Epoch,
			"epoch_bumped", result.EpochBumped,
		)
	case !result.Acquired && wasLeader:
		e.logger.Warn("lost leadership",
			"identity", e.cfg.Identity,
			"current_leader", result.Lease.LeaderID,
			"current_leader_addr", result.Lease.LeaderAddr,
			"epoch", result.Lease.Epoch,
		)
	case result.Acquired:
		e.logger.Debug("renewed lease", "epoch", result.Lease.Epoch, "expires_at", result.Lease.ExpiresAt)
	}
}

// IsLeader reports whether this replica currently holds the lease.
func (e *Elector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.isLeader
}

// LeaderAddr returns the advertise address of the current leader (this
// replica's address when we hold the lease). Empty if unknown.
func (e *Elector) LeaderAddr() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.leaderAddr
}

// Epoch returns the fencing epoch of the current lease view.
func (e *Elector) Epoch() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.epoch
}

// Identity returns this replica's configured identity.
func (e *Elector) Identity() string {
	return e.cfg.Identity
}
