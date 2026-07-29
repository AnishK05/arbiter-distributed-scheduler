package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Node statuses. Only "ready" is produced by Phase 1 (RegisterNode); the
// rest are introduced by the failure detector (Phase 2) and cordoning
// support (later phases).
const (
	NodeStatusUnknown  = "unknown"
	NodeStatusReady    = "ready"
	NodeStatusNotReady = "not_ready"
	NodeStatusDead     = "dead"
	NodeStatusCordoned = "cordoned"
)

// ErrNodeNotFound is returned by GetNode when no node exists with the given ID.
var ErrNodeNotFound = errors.New("store: node not found")

// Node mirrors the `nodes` table (IMPLEMENTATION_PLAN.md Section 6.1).
type Node struct {
	ID                    string
	Hostname              string
	Address               string
	CPUCapacityMillicores int64
	MemCapacityMB         int64
	Labels                map[string]string
	Status                string
	Epoch                 int64
	LastHeartbeatAt       *time.Time
	CreatedAt             time.Time
}

// RegisterNodeParams is the input to RegisterNode.
type RegisterNodeParams struct {
	Hostname              string
	Address               string
	CPUCapacityMillicores int64
	MemCapacityMB         int64
	Labels                map[string]string
}

// RegisterNode upserts a node keyed by (hostname, address): a brand new
// worker gets a fresh row, while a worker that restarts and re-registers
// from the same hostname/address updates its existing row (refreshing
// capacity/labels, resetting status to "ready", bumping last_heartbeat_at so
// it isn't immediately flagged stale, and incrementing epoch).
//
// Incrementing epoch on every registration — not just when the failure
// detector declares a node dead — is deliberately conservative: it ensures
// that if a worker process restarts (e.g. crash-looped, or the container was
// recreated), any in-flight task assignments issued to its *previous*
// process incarnation are addressed to a now-stale epoch. Full enforcement
// of that fencing check on the read side lands in Phase 5
// (IMPLEMENTATION_PLAN.md Section 6.5), but establishing the invariant here
// in Phase 1 means later phases don't need a data-model change.
func (s *Store) RegisterNode(ctx context.Context, params RegisterNodeParams) (*Node, error) {
	labels := params.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	labelsJSON, err := json.Marshal(labels)
	if err != nil {
		return nil, fmt.Errorf("store: marshal labels: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: register node: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const q = `
		INSERT INTO nodes (hostname, address, cpu_capacity_mc, mem_capacity_mb, labels, status, epoch, last_heartbeat_at)
		VALUES ($1, $2, $3, $4, $5, $6, 0, now())
		ON CONFLICT (hostname, address) DO UPDATE
		SET cpu_capacity_mc   = EXCLUDED.cpu_capacity_mc,
		    mem_capacity_mb   = EXCLUDED.mem_capacity_mb,
		    labels            = EXCLUDED.labels,
		    status            = EXCLUDED.status,
		    epoch             = nodes.epoch + 1,
		    last_heartbeat_at = now()
		RETURNING id, hostname, address, cpu_capacity_mc, mem_capacity_mb, labels, status, epoch, last_heartbeat_at, created_at
	`

	row := tx.QueryRow(ctx, q,
		params.Hostname, params.Address, params.CPUCapacityMillicores, params.MemCapacityMB, labelsJSON, NodeStatusReady,
	)

	node, err := scanNode(row)
	if err != nil {
		return nil, fmt.Errorf("store: register node: %w", err)
	}

	msg := fmt.Sprintf("node registered: hostname=%s address=%s epoch=%d", node.Hostname, node.Address, node.Epoch)
	if err := insertEvent(ctx, tx, EntityTypeNode, node.ID, EventTypeNodeRegistered, msg); err != nil {
		return nil, fmt.Errorf("store: register node: insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: register node: commit: %w", err)
	}
	return node, nil
}

// UpdateNodeStatus transitions a node to newStatus (without touching epoch)
// and records an audit event, atomically. Used by the Heartbeat handler
// (not_ready -> ready recovery) and the failure detector (ready -> not_ready).
// Dying (-> dead) goes through MarkNodeDead instead, since that also bumps
// epoch.
func (s *Store) UpdateNodeStatus(ctx context.Context, nodeID, newStatus, eventType, message string) (*Node, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: update node status: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const q = `
		UPDATE nodes SET status = $2
		WHERE id = $1
		RETURNING id, hostname, address, cpu_capacity_mc, mem_capacity_mb, labels, status, epoch, last_heartbeat_at, created_at
	`
	node, err := scanNode(tx.QueryRow(ctx, q, nodeID, newStatus))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNodeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: update node status: %w", err)
	}

	if err := insertEvent(ctx, tx, EntityTypeNode, node.ID, eventType, message); err != nil {
		return nil, fmt.Errorf("store: update node status: insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: update node status: commit: %w", err)
	}
	return node, nil
}

// MarkNodeDeadResult is returned by MarkNodeDead: the updated node plus the
// task IDs that were orphaned and requeued for reassignment (so callers can
// reap leftover Docker containers).
type MarkNodeDeadResult struct {
	Node            *Node
	OrphanedTaskIDs []string
}

// MarkNodeDead transitions a node to "dead", increments its epoch, and
// orphans + requeues every scheduled/running task on that node
// (IMPLEMENTATION_PLAN.md Sections 6.4–6.5). Incrementing epoch is the core
// of fencing: if the node was actually alive behind a partition, its next
// heartbeat carries a stale epoch and is rejected.
func (s *Store) MarkNodeDead(ctx context.Context, nodeID string) (*MarkNodeDeadResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: mark node dead: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const q = `
		UPDATE nodes SET status = $2, epoch = epoch + 1
		WHERE id = $1
		RETURNING id, hostname, address, cpu_capacity_mc, mem_capacity_mb, labels, status, epoch, last_heartbeat_at, created_at
	`
	node, err := scanNode(tx.QueryRow(ctx, q, nodeID, NodeStatusDead))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNodeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: mark node dead: %w", err)
	}

	msg := fmt.Sprintf("node marked dead after missed heartbeats, epoch bumped to %d", node.Epoch)
	if err := insertEvent(ctx, tx, EntityTypeNode, node.ID, EventTypeNodeDead, msg); err != nil {
		return nil, fmt.Errorf("store: mark node dead: insert event: %w", err)
	}

	orphanedIDs, err := orphanAndRequeueTasksTx(ctx, tx, nodeID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: mark node dead: commit: %w", err)
	}
	return &MarkNodeDeadResult{Node: node, OrphanedTaskIDs: orphanedIDs}, nil
}

// orphanAndRequeueTasksTx marks scheduled/running tasks on nodeID as
// orphaned (event) then immediately requeues them as pending for
// reassignment. Returns the affected task IDs.
func orphanAndRequeueTasksTx(ctx context.Context, tx pgx.Tx, nodeID string) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT id FROM tasks
		WHERE assigned_node_id = $1 AND status IN ($2, $3)
		FOR UPDATE`,
		nodeID, TaskStatusScheduled, TaskStatusRunning,
	)
	if err != nil {
		return nil, fmt.Errorf("store: list tasks to orphan: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	for _, id := range ids {
		if err := insertEvent(ctx, tx, EntityTypeTask, id, EventTypeTaskOrphaned,
			fmt.Sprintf("orphaned: assigned node %s marked dead", nodeID)); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE tasks
			SET status = $2, assigned_node_id = NULL, assigned_epoch = NULL,
			    scheduled_at = NULL, started_at = NULL, finished_at = NULL,
			    next_retry_at = NULL,
			    last_error = $3
			WHERE id = $1`,
			id, TaskStatusPending, "requeued after node failure"); err != nil {
			return nil, fmt.Errorf("store: requeue orphaned task: %w", err)
		}
		if err := insertEvent(ctx, tx, EntityTypeTask, id, EventTypeTaskRequeued,
			"requeued as pending for reassignment"); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// GetNode fetches a single node by ID.
func (s *Store) GetNode(ctx context.Context, id string) (*Node, error) {
	const q = `
		SELECT id, hostname, address, cpu_capacity_mc, mem_capacity_mb, labels, status, epoch, last_heartbeat_at, created_at
		FROM nodes
		WHERE id = $1
	`
	node, err := scanNode(s.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNodeNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get node: %w", err)
	}
	return node, nil
}

// CountNodesByStatus returns a status → count map for all nodes.
func (s *Store) CountNodesByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM nodes GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("store: count nodes by status: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("store: scan node count: %w", err)
		}
		out[status] = n
	}
	return out, rows.Err()
}

// ListNodes returns every node in the cluster, ordered by creation time.
func (s *Store) ListNodes(ctx context.Context) ([]Node, error) {
	const q = `
		SELECT id, hostname, address, cpu_capacity_mc, mem_capacity_mb, labels, status, epoch, last_heartbeat_at, created_at
		FROM nodes
		ORDER BY created_at ASC
	`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list nodes: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan node row: %w", err)
		}
		nodes = append(nodes, *node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list nodes: %w", err)
	}
	return nodes, nil
}

// ListActiveNodes returns nodes the failure detector should be watching:
// everything except those already "dead" or manually "cordoned" (dead nodes
// stay dead until a fresh RegisterNode call, and cordoned is an explicit
// manual state later phases will add tooling to set/unset).
func (s *Store) ListActiveNodes(ctx context.Context) ([]Node, error) {
	const q = `
		SELECT id, hostname, address, cpu_capacity_mc, mem_capacity_mb, labels, status, epoch, last_heartbeat_at, created_at
		FROM nodes
		WHERE status NOT IN ($1, $2)
		ORDER BY created_at ASC
	`
	rows, err := s.pool.Query(ctx, q, NodeStatusDead, NodeStatusCordoned)
	if err != nil {
		return nil, fmt.Errorf("store: list active nodes: %w", err)
	}
	defer rows.Close()

	var nodes []Node
	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan node row: %w", err)
		}
		nodes = append(nodes, *node)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list active nodes: %w", err)
	}
	return nodes, nil
}

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query),
// letting scanNode be shared by all three methods above.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanNode(row rowScanner) (*Node, error) {
	var n Node
	var labelsRaw []byte
	if err := row.Scan(
		&n.ID, &n.Hostname, &n.Address, &n.CPUCapacityMillicores, &n.MemCapacityMB,
		&labelsRaw, &n.Status, &n.Epoch, &n.LastHeartbeatAt, &n.CreatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(labelsRaw, &n.Labels); err != nil {
		return nil, fmt.Errorf("unmarshal labels: %w", err)
	}
	return &n, nil
}
