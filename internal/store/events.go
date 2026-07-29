package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Event types written by the node/failure-detector lifecycle (Phase 2),
// the job/task lifecycle (Phase 3), and leader election (Phase 6).
const (
	EventTypeNodeRegistered = "node_registered"
	EventTypeNodeNotReady   = "node_not_ready"
	EventTypeNodeRecovered  = "node_recovered"
	EventTypeNodeDead       = "node_dead"

	EventTypeJobSubmitted       = "job_submitted"
	EventTypeTaskScheduled      = "task_scheduled"
	EventTypeTaskRunning        = "task_running"
	EventTypeTaskSucceeded      = "task_succeeded"
	EventTypeTaskFailed         = "task_failed"
	EventTypeTaskOrphaned       = "task_orphaned"
	EventTypeTaskRequeued       = "task_requeued"
	EventTypeTaskRetryScheduled = "task_retry_scheduled"

	EventTypeLeaderElected = "leader_elected"
)

// Entity types used in the events table's entity_type column.
const (
	EntityTypeNode   = "node"
	EntityTypeJob    = "job"
	EntityTypeTask   = "task"
	EntityTypeLeader = "leader"
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
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, *e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list events: %w", err)
	}
	return events, nil
}

// ListRecentEvents returns the newest events (newest last), up to limit.
func (s *Store) ListRecentEvents(ctx context.Context, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, entity_type, entity_id, event_type, message, created_at
		 FROM (
		   SELECT id, entity_type, entity_id, event_type, message, created_at
		   FROM events
		   ORDER BY id DESC
		   LIMIT $1
		 ) recent
		 ORDER BY id ASC`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list recent events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, *e)
	}
	return events, rows.Err()
}

// ListEventsAfter returns events with id > afterID, oldest first, up to limit.
func (s *Store) ListEventsAfter(ctx context.Context, afterID int64, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, entity_type, entity_id, event_type, message, created_at
		 FROM events
		 WHERE id > $1
		 ORDER BY id ASC
		 LIMIT $2`,
		afterID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list events after: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, *e)
	}
	return events, rows.Err()
}

type eventScanner interface {
	Scan(dest ...any) error
}

func scanEvent(row eventScanner) (*Event, error) {
	var e Event
	var message *string
	if err := row.Scan(&e.ID, &e.EntityType, &e.EntityID, &e.EventType, &message, &e.CreatedAt); err != nil {
		return nil, fmt.Errorf("store: scan event row: %w", err)
	}
	if message != nil {
		e.Message = *message
	}
	return &e, nil
}
