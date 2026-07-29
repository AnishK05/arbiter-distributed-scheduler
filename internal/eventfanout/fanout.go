// Package eventfanout polls Postgres for new audit events on the leader and
// publishes them to Redis pub/sub for dashboard SSE subscribers (Phase 8).
package eventfanout

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// LeaderGate reports whether this replica should fan out events.
type LeaderGate interface {
	IsLeader() bool
}

// Runner polls store.ListEventsAfter and publishes to Redis.
type Runner struct {
	store    *store.Store
	cache    *cache.Client
	leader   LeaderGate
	logger   *slog.Logger
	interval time.Duration
}

// New constructs a Runner. logger may be nil.
func New(s *store.Store, c *cache.Client, leader LeaderGate, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Runner{store: s, cache: c, leader: leader, logger: logger, interval: 500 * time.Millisecond}
}

// Run blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	var afterID int64
	if recent, err := r.store.ListRecentEvents(ctx, 1); err == nil && len(recent) > 0 {
		afterID = recent[len(recent)-1].ID
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if r.leader != nil && !r.leader.IsLeader() {
				continue
			}
			events, err := r.store.ListEventsAfter(ctx, afterID, 200)
			if err != nil {
				r.logger.Warn("eventfanout: list events", "error", err)
				continue
			}
			for _, ev := range events {
				if err := r.cache.PublishClusterEvent(ctx, cache.ClusterEvent{
					ID:         ev.ID,
					EntityType: ev.EntityType,
					EntityID:   ev.EntityID,
					EventType:  ev.EventType,
					Message:    ev.Message,
					CreatedAt:  ev.CreatedAt,
				}); err != nil {
					r.logger.Warn("eventfanout: publish", "error", err)
					continue
				}
				afterID = ev.ID
			}
		}
	}
}
