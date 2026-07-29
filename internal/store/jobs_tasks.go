package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Task statuses (IMPLEMENTATION_PLAN.md Section 6.8).
const (
	TaskStatusPending   = "pending"
	TaskStatusScheduled = "scheduled"
	TaskStatusRunning   = "running"
	TaskStatusSucceeded = "succeeded"
	TaskStatusFailed    = "failed"
	TaskStatusOrphaned  = "orphaned"
	TaskStatusCancelled = "cancelled"
)

// Scheduling policies. Phase 3 always uses first-fit regardless of the
// stored policy; Phase 4 wires bin_pack/spread scorers to this field.
const (
	SchedulingPolicyBinPack = "bin_pack"
	SchedulingPolicySpread  = "spread"
)

// ErrJobNotFound is returned by GetJob when no job exists with the given ID.
var ErrJobNotFound = errors.New("store: job not found")

// ErrTaskNotFound is returned by GetTask when no task exists with the given ID.
var ErrTaskNotFound = errors.New("store: task not found")

// ErrStaleTaskReport is returned when a worker status update carries a
// node_id/epoch that no longer matches the task's assignment (zombie report
// after fencing — IMPLEMENTATION_PLAN.md Section 6.5).
var ErrStaleTaskReport = errors.New("store: stale task status report")

// Job mirrors the `jobs` table (IMPLEMENTATION_PLAN.md Section 6.1).
type Job struct {
	ID                   string
	Name                 string
	Image                string
	Command              []string
	CPURequestMillicores int64
	MemRequestMB         int64
	Replicas             int32
	RetryLimit           int32
	SchedulingPolicy     string
	Constraints          map[string]string
	CreatedAt            time.Time
}

// Task mirrors the `tasks` table. Resource requests live on the parent Job;
// TaskWithJob joins them for the scheduling/assignment paths that need both.
type Task struct {
	ID             string
	JobID          string
	Status         string
	AssignedNodeID *string
	AssignedEpoch  *int64
	RetriesUsed    int32
	ExitCode       *int32
	LastError      *string
	CreatedAt      time.Time
	ScheduledAt    *time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	NextRetryAt    *time.Time
}

// TaskWithJob is a Task plus the fields from its parent Job that the
// scheduler and worker need to place/run it.
type TaskWithJob struct {
	Task
	Image                string
	Command              []string
	CPURequestMillicores int64
	MemRequestMB         int64
	RetryLimit           int32
	SchedulingPolicy     string
	Constraints          map[string]string
}

// CreateJobParams is the input to CreateJob.
type CreateJobParams struct {
	Name                 string
	Image                string
	Command              []string
	CPURequestMillicores int64
	MemRequestMB         int64
	Replicas             int32
	RetryLimit           int32
	SchedulingPolicy     string
	Constraints          map[string]string
}

// CreateJob inserts a job and expands it into `replicas` pending task rows
// in a single transaction (IMPLEMENTATION_PLAN.md Phase 3).
func (s *Store) CreateJob(ctx context.Context, params CreateJobParams) (*Job, error) {
	if params.Replicas < 1 {
		return nil, fmt.Errorf("store: replicas must be >= 1")
	}
	if params.Command == nil {
		params.Command = []string{}
	}
	if params.Constraints == nil {
		params.Constraints = map[string]string{}
	}
	if params.SchedulingPolicy == "" {
		params.SchedulingPolicy = SchedulingPolicyBinPack
	}
	constraintsJSON, err := json.Marshal(params.Constraints)
	if err != nil {
		return nil, fmt.Errorf("store: marshal constraints: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: create job: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const insertJob = `
		INSERT INTO jobs (name, image, command, cpu_request_mc, mem_request_mb, replicas, retry_limit, scheduling_policy, constraints)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id, name, image, command, cpu_request_mc, mem_request_mb, replicas, retry_limit, scheduling_policy, constraints, created_at
	`
	job, err := scanJob(tx.QueryRow(ctx, insertJob,
		params.Name, params.Image, params.Command,
		params.CPURequestMillicores, params.MemRequestMB,
		params.Replicas, params.RetryLimit, params.SchedulingPolicy, constraintsJSON,
	))
	if err != nil {
		return nil, fmt.Errorf("store: create job: %w", err)
	}

	for i := int32(0); i < params.Replicas; i++ {
		if _, err := tx.Exec(ctx,
			`INSERT INTO tasks (job_id, status) VALUES ($1, $2)`,
			job.ID, TaskStatusPending,
		); err != nil {
			return nil, fmt.Errorf("store: create job: insert task: %w", err)
		}
	}

	msg := fmt.Sprintf("job submitted: name=%s replicas=%d image=%s", job.Name, job.Replicas, job.Image)
	if err := insertEvent(ctx, tx, EntityTypeJob, job.ID, EventTypeJobSubmitted, msg); err != nil {
		return nil, fmt.Errorf("store: create job: insert event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("store: create job: commit: %w", err)
	}
	return job, nil
}

// GetJob fetches a single job by ID.
func (s *Store) GetJob(ctx context.Context, id string) (*Job, error) {
	const q = `
		SELECT id, name, image, command, cpu_request_mc, mem_request_mb, replicas, retry_limit, scheduling_policy, constraints, created_at
		FROM jobs WHERE id = $1
	`
	job, err := scanJob(s.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get job: %w", err)
	}
	return job, nil
}

// ListJobs returns every job, oldest first.
func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	const q = `
		SELECT id, name, image, command, cpu_request_mc, mem_request_mb, replicas, retry_limit, scheduling_policy, constraints, created_at
		FROM jobs ORDER BY created_at ASC
	`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan job row: %w", err)
		}
		jobs = append(jobs, *job)
	}
	return jobs, rows.Err()
}

// GetTask fetches a single task by ID.
func (s *Store) GetTask(ctx context.Context, id string) (*Task, error) {
	const q = `
		SELECT id, job_id, status, assigned_node_id, assigned_epoch, retries_used, exit_code, last_error,
		       created_at, scheduled_at, started_at, finished_at, next_retry_at
		FROM tasks WHERE id = $1
	`
	task, err := scanTask(s.pool.QueryRow(ctx, q, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get task: %w", err)
	}
	return task, nil
}

// CountTasksByStatus returns a status → count map for all tasks.
func (s *Store) CountTasksByStatus(ctx context.Context) (map[string]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM tasks GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("store: count tasks by status: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return nil, fmt.Errorf("store: scan task count: %w", err)
		}
		out[status] = n
	}
	return out, rows.Err()
}

// ListTasks returns tasks, optionally filtered by job ID.
func (s *Store) ListTasks(ctx context.Context, jobID string) ([]Task, error) {
	var (
		rows pgx.Rows
		err  error
	)
	if jobID == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, job_id, status, assigned_node_id, assigned_epoch, retries_used, exit_code, last_error,
			       created_at, scheduled_at, started_at, finished_at, next_retry_at
			FROM tasks ORDER BY created_at ASC`)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, job_id, status, assigned_node_id, assigned_epoch, retries_used, exit_code, last_error,
			       created_at, scheduled_at, started_at, finished_at, next_retry_at
			FROM tasks WHERE job_id = $1 ORDER BY created_at ASC`, jobID)
	}
	if err != nil {
		return nil, fmt.Errorf("store: list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan task row: %w", err)
		}
		tasks = append(tasks, *task)
	}
	return tasks, rows.Err()
}

// ClaimPendingTasksForScheduling locks up to limit pending tasks with
// FOR UPDATE SKIP LOCKED and returns them joined with their parent job
// fields. The caller must invoke ScheduleTask or leave them untouched before
// the returned tx is committed/rolled back. See IMPLEMENTATION_PLAN.md
// Section 6.2.
func (s *Store) ClaimPendingTasksForScheduling(ctx context.Context, limit int) (pgx.Tx, []TaskWithJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("store: claim pending tasks: begin tx: %w", err)
	}

	const q = `
		SELECT t.id, t.job_id, t.status, t.assigned_node_id, t.assigned_epoch, t.retries_used, t.exit_code, t.last_error,
		       t.created_at, t.scheduled_at, t.started_at, t.finished_at, t.next_retry_at,
		       j.image, j.command, j.cpu_request_mc, j.mem_request_mb, j.retry_limit, j.scheduling_policy, j.constraints
		FROM tasks t
		JOIN jobs j ON j.id = t.job_id
		WHERE t.status = $1
		  AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
		ORDER BY t.created_at ASC
		FOR UPDATE OF t SKIP LOCKED
		LIMIT $2
	`
	rows, err := tx.Query(ctx, q, TaskStatusPending, limit)
	if err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, fmt.Errorf("store: claim pending tasks: %w", err)
	}
	defer rows.Close()

	var tasks []TaskWithJob
	for rows.Next() {
		twj, err := scanTaskWithJob(rows)
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, fmt.Errorf("store: scan pending task: %w", err)
		}
		tasks = append(tasks, *twj)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback(ctx)
		return nil, nil, fmt.Errorf("store: claim pending tasks: %w", err)
	}
	return tx, tasks, nil
}

// ScheduleTask assigns a previously-claimed pending task to a node inside
// the caller's transaction. epoch is the node's current fencing epoch at
// assignment time (IMPLEMENTATION_PLAN.md Section 6.5).
func (s *Store) ScheduleTask(ctx context.Context, tx pgx.Tx, taskID, nodeID string, epoch int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = $2, assigned_node_id = $3, assigned_epoch = $4, scheduled_at = now()
		WHERE id = $1 AND status = $5`,
		taskID, TaskStatusScheduled, nodeID, epoch, TaskStatusPending,
	)
	if err != nil {
		return fmt.Errorf("store: schedule task: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("store: schedule task: expected 1 row updated, got %d", tag.RowsAffected())
	}
	msg := fmt.Sprintf("task scheduled onto node %s (epoch %d)", nodeID, epoch)
	if err := insertEvent(ctx, tx, EntityTypeTask, taskID, EventTypeTaskScheduled, msg); err != nil {
		return fmt.Errorf("store: schedule task: insert event: %w", err)
	}
	return nil
}

// NodeAllocation is the sum of resource requests for tasks currently
// consuming capacity on a node (scheduled + running).
type NodeAllocation struct {
	CPUMillicores int64
	MemoryMB      int64
}

// GetNodeAllocation returns how much CPU/memory is currently reserved on
// the given node by scheduled/running tasks.
func (s *Store) GetNodeAllocation(ctx context.Context, nodeID string) (NodeAllocation, error) {
	const q = `
		SELECT COALESCE(SUM(j.cpu_request_mc), 0), COALESCE(SUM(j.mem_request_mb), 0)
		FROM tasks t
		JOIN jobs j ON j.id = t.job_id
		WHERE t.assigned_node_id = $1 AND t.status IN ($2, $3)
	`
	var alloc NodeAllocation
	err := s.pool.QueryRow(ctx, q, nodeID, TaskStatusScheduled, TaskStatusRunning).
		Scan(&alloc.CPUMillicores, &alloc.MemoryMB)
	if err != nil {
		return NodeAllocation{}, fmt.Errorf("store: get node allocation: %w", err)
	}
	return alloc, nil
}

// GetNodeAllocations returns allocations for every node that currently has
// at least one scheduled/running task. Nodes with zero allocation are
// simply absent from the map (callers should treat missing keys as zero).
func (s *Store) GetNodeAllocations(ctx context.Context) (map[string]NodeAllocation, error) {
	const q = `
		SELECT t.assigned_node_id, COALESCE(SUM(j.cpu_request_mc), 0), COALESCE(SUM(j.mem_request_mb), 0)
		FROM tasks t
		JOIN jobs j ON j.id = t.job_id
		WHERE t.assigned_node_id IS NOT NULL AND t.status IN ($1, $2)
		GROUP BY t.assigned_node_id
	`
	rows, err := s.pool.Query(ctx, q, TaskStatusScheduled, TaskStatusRunning)
	if err != nil {
		return nil, fmt.Errorf("store: get node allocations: %w", err)
	}
	defer rows.Close()

	out := make(map[string]NodeAllocation)
	for rows.Next() {
		var nodeID string
		var alloc NodeAllocation
		if err := rows.Scan(&nodeID, &alloc.CPUMillicores, &alloc.MemoryMB); err != nil {
			return nil, fmt.Errorf("store: scan node allocation: %w", err)
		}
		out[nodeID] = alloc
	}
	return out, rows.Err()
}

// ListScheduledTasksForNode returns scheduled tasks assigned to nodeID,
// joined with job fields needed to launch them. The Heartbeat handler uses
// this to push new assignments to the worker.
func (s *Store) ListScheduledTasksForNode(ctx context.Context, nodeID string) ([]TaskWithJob, error) {
	const q = `
		SELECT t.id, t.job_id, t.status, t.assigned_node_id, t.assigned_epoch, t.retries_used, t.exit_code, t.last_error,
		       t.created_at, t.scheduled_at, t.started_at, t.finished_at, t.next_retry_at,
		       j.image, j.command, j.cpu_request_mc, j.mem_request_mb, j.retry_limit, j.scheduling_policy, j.constraints
		FROM tasks t
		JOIN jobs j ON j.id = t.job_id
		WHERE t.assigned_node_id = $1 AND t.status = $2
		ORDER BY t.scheduled_at ASC NULLS FIRST, t.created_at ASC
	`
	rows, err := s.pool.Query(ctx, q, nodeID, TaskStatusScheduled)
	if err != nil {
		return nil, fmt.Errorf("store: list scheduled tasks: %w", err)
	}
	defer rows.Close()

	var tasks []TaskWithJob
	for rows.Next() {
		twj, err := scanTaskWithJob(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan scheduled task: %w", err)
		}
		tasks = append(tasks, *twj)
	}
	return tasks, rows.Err()
}

// UpdateTaskStatusParams is the input to UpdateTaskStatus.
type UpdateTaskStatusParams struct {
	TaskID   string
	Status   string // running|succeeded|failed
	ExitCode *int32
	Error    string
	// Optional fencing fields from the reporting worker (Phase 5). When set,
	// the update is rejected with ErrStaleTaskReport if they don't match the
	// task's current assignment.
	NodeID string
	Epoch  int64
}

// UpdateTaskStatus transitions a task based on a worker status report.
// Only legal worker-driven transitions are accepted (scheduled/running ->
// running/succeeded/failed). Terminal statuses are idempotent no-ops so a
// duplicated report after a reconnect doesn't error. A failed task whose
// retries_used is still below the job's retry_limit is requeued as pending
// with exponential backoff (IMPLEMENTATION_PLAN.md Phase 5).
func (s *Store) UpdateTaskStatus(ctx context.Context, params UpdateTaskStatusParams) (*Task, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("store: update task status: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	task, err := scanTask(tx.QueryRow(ctx, `
		SELECT id, job_id, status, assigned_node_id, assigned_epoch, retries_used, exit_code, last_error,
		       created_at, scheduled_at, started_at, finished_at, next_retry_at
		FROM tasks WHERE id = $1 FOR UPDATE`, params.TaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: update task status: %w", err)
	}

	if err := checkTaskReportFence(task, params.NodeID, params.Epoch); err != nil {
		return nil, err
	}

	// Idempotent: already in the requested terminal state.
	if (params.Status == TaskStatusSucceeded || params.Status == TaskStatusFailed) && task.Status == params.Status {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return task, nil
	}

	switch params.Status {
	case TaskStatusRunning:
		if task.Status != TaskStatusScheduled && task.Status != TaskStatusRunning {
			return nil, fmt.Errorf("store: cannot transition task from %s to running", task.Status)
		}
		updated, err := scanTask(tx.QueryRow(ctx, `
			UPDATE tasks SET status = $2, started_at = COALESCE(started_at, now()), next_retry_at = NULL
			WHERE id = $1
			RETURNING id, job_id, status, assigned_node_id, assigned_epoch, retries_used, exit_code, last_error,
			          created_at, scheduled_at, started_at, finished_at, next_retry_at`,
			params.TaskID, TaskStatusRunning))
		if err != nil {
			return nil, fmt.Errorf("store: mark task running: %w", err)
		}
		if task.Status == TaskStatusScheduled {
			if err := insertEvent(ctx, tx, EntityTypeTask, params.TaskID, EventTypeTaskRunning, "task container started"); err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return updated, nil

	case TaskStatusSucceeded, TaskStatusFailed:
		if task.Status != TaskStatusScheduled && task.Status != TaskStatusRunning {
			return nil, fmt.Errorf("store: cannot transition task from %s to %s", task.Status, params.Status)
		}
		var lastError *string
		if params.Error != "" {
			lastError = &params.Error
		}

		if params.Status == TaskStatusFailed {
			var retryLimit int32
			if err := tx.QueryRow(ctx, `SELECT retry_limit FROM jobs WHERE id = $1`, task.JobID).Scan(&retryLimit); err != nil {
				return nil, fmt.Errorf("store: load retry_limit: %w", err)
			}
			if task.RetriesUsed < retryLimit {
				return requeueFailedTask(ctx, tx, task, params.ExitCode, lastError)
			}
		}

		eventType := EventTypeTaskSucceeded
		if params.Status == TaskStatusFailed {
			eventType = EventTypeTaskFailed
		}
		updated, err := scanTask(tx.QueryRow(ctx, `
			UPDATE tasks
			SET status = $2, exit_code = $3, last_error = $4, finished_at = now(),
			    started_at = COALESCE(started_at, now()), next_retry_at = NULL
			WHERE id = $1
			RETURNING id, job_id, status, assigned_node_id, assigned_epoch, retries_used, exit_code, last_error,
			          created_at, scheduled_at, started_at, finished_at, next_retry_at`,
			params.TaskID, params.Status, params.ExitCode, lastError))
		if err != nil {
			return nil, fmt.Errorf("store: mark task terminal: %w", err)
		}
		msg := fmt.Sprintf("task %s", params.Status)
		if params.ExitCode != nil {
			msg = fmt.Sprintf("%s (exit_code=%d)", msg, *params.ExitCode)
		}
		if err := insertEvent(ctx, tx, EntityTypeTask, params.TaskID, eventType, msg); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return updated, nil

	default:
		return nil, fmt.Errorf("store: unsupported task status update %q", params.Status)
	}
}

func checkTaskReportFence(task *Task, nodeID string, epoch int64) error {
	if nodeID == "" {
		// Older callers / tests without fencing fields — keep permissive.
		return nil
	}
	if task.AssignedNodeID == nil || *task.AssignedNodeID != nodeID {
		return ErrStaleTaskReport
	}
	if task.AssignedEpoch == nil || *task.AssignedEpoch != epoch {
		return ErrStaleTaskReport
	}
	return nil
}

func requeueFailedTask(ctx context.Context, tx pgx.Tx, task *Task, exitCode *int32, lastError *string) (*Task, error) {
	retries := task.RetriesUsed + 1
	backoff := retryBackoff(retries)
	updated, err := scanTask(tx.QueryRow(ctx, `
		UPDATE tasks
		SET status = $2, retries_used = $3, exit_code = $4, last_error = $5,
		    assigned_node_id = NULL, assigned_epoch = NULL,
		    scheduled_at = NULL, started_at = NULL, finished_at = NULL,
		next_retry_at = now() + ($6 * interval '1 second')
		WHERE id = $1
		RETURNING id, job_id, status, assigned_node_id, assigned_epoch, retries_used, exit_code, last_error,
		          created_at, scheduled_at, started_at, finished_at, next_retry_at`,
		task.ID, TaskStatusPending, retries, exitCode, lastError, backoff.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("store: requeue failed task: %w", err)
	}
	msg := fmt.Sprintf("task failed; retry %d scheduled after %s", retries, backoff)
	if err := insertEvent(ctx, tx, EntityTypeTask, task.ID, EventTypeTaskRetryScheduled, msg); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return updated, nil
}

// retryBackoff returns exponential backoff for the Nth retry attempt
// (retriesUsed after increment): 500ms, 1s, 2s, 4s, … capped at 16s.
func retryBackoff(retriesUsed int32) time.Duration {
	shift := retriesUsed - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 5 {
		shift = 5
	}
	return time.Duration(500*(1<<shift)) * time.Millisecond
}

func scanJob(row rowScanner) (*Job, error) {
	var j Job
	var constraintsRaw []byte
	if err := row.Scan(
		&j.ID, &j.Name, &j.Image, &j.Command, &j.CPURequestMillicores, &j.MemRequestMB,
		&j.Replicas, &j.RetryLimit, &j.SchedulingPolicy, &constraintsRaw, &j.CreatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(constraintsRaw, &j.Constraints); err != nil {
		return nil, fmt.Errorf("unmarshal constraints: %w", err)
	}
	if j.Constraints == nil {
		j.Constraints = map[string]string{}
	}
	return &j, nil
}

func scanTask(row rowScanner) (*Task, error) {
	var t Task
	if err := row.Scan(
		&t.ID, &t.JobID, &t.Status, &t.AssignedNodeID, &t.AssignedEpoch, &t.RetriesUsed,
		&t.ExitCode, &t.LastError, &t.CreatedAt, &t.ScheduledAt, &t.StartedAt, &t.FinishedAt,
		&t.NextRetryAt,
	); err != nil {
		return nil, err
	}
	return &t, nil
}

func scanTaskWithJob(row rowScanner) (*TaskWithJob, error) {
	var twj TaskWithJob
	var constraintsRaw []byte
	if err := row.Scan(
		&twj.ID, &twj.JobID, &twj.Status, &twj.AssignedNodeID, &twj.AssignedEpoch, &twj.RetriesUsed,
		&twj.ExitCode, &twj.LastError, &twj.CreatedAt, &twj.ScheduledAt, &twj.StartedAt, &twj.FinishedAt,
		&twj.NextRetryAt,
		&twj.Image, &twj.Command, &twj.CPURequestMillicores, &twj.MemRequestMB, &twj.RetryLimit, &twj.SchedulingPolicy,
		&constraintsRaw,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(constraintsRaw, &twj.Constraints); err != nil {
		return nil, fmt.Errorf("unmarshal constraints: %w", err)
	}
	if twj.Constraints == nil {
		twj.Constraints = map[string]string{}
	}
	return &twj, nil
}
