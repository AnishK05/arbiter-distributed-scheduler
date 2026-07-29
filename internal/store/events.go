package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Event types written by the node/failure-detector lifecycle (Phase 2) and
// the job/task lifecycle (Phase 3). Leader event types land in Phase 6.
const (
	EventTypeNodeRegistered = "node_registered"
	EventTypeNodeNotReady   = "node_not_ready"
	EventTypeNodeRecovered  = "node_recovered"
	EventTypeNodeDead       = "node_dead"

	EventTypeJobSubmitted  = "job_submitted"
	EventTypeTaskScheduled = "task_scheduled"
	EventTypeTaskRunning   = "task_running"
	EventTypeTaskSucceeded = "task_succeeded"
	EventTypeTaskFailed    = "task_failed"
)

// Entity types used in the events table's entity_type column.
const (
	EntityTypeNode = "node"
	EntityTypeJob  = "job"
	EntityTypeTask = "task"
)

// Event mirrors a row in the `events` audit-trail table
// (IMPLEMENTATION_PLAN.md Section 6.1).
type Event struct {
	ID         int64
	EntityType string
	EntityID   string
	EventType  string
	Message    string
	CreatedAt  time.Time
}

// execer is satisfied by both *pgxpool.Pool and pgx.Tx, letting insertEvent
// be called either standalone or as part of a transaction (e.g. alongside
// the node status update that triggered it) without duplicating the SQL.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func insertEvent(ctx context.Context, q execer, entityType, entityID, eventType, message string) error {
	_, err := q.Exec(ctx,
		`INSERT INTO events (entity_type, entity_id, event_type, message) VALUES ($1, $2, $3, $4)`,
		entityType, entityID, eventType, message,
	)
	return err
}

// ListEventsForEntity returns every event recorded for a given entity,
// oldest first. Used by tests and (starting Phase 8) the dashboard timeline.
func (s *Store) ListEventsForEntity(ctx context.Context, entityType, entityID string) ([]Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, entity_type, entity_id, event_type, message, created_at
		 FROM events
		 WHERE entity_type = $1 AND entity_id = $2
		 ORDER BY created_at ASC`,
		entityType, entityID,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		var message *string
		if err := rows.Scan(&e.ID, &e.EntityType, &e.EntityID, &e.EventType, &message, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("store: scan event row: %w", err)
		}
		if message != nil {
			e.Message = *message
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list events: %w", err)
	}
	return events, nil
}
