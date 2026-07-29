package store_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

func rawPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	connString := os.Getenv("ARBITER_TEST_POSTGRES_URL")
	if connString == "" {
		t.Skip("ARBITER_TEST_POSTGRES_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		t.Fatalf("raw pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestMarkNodeDeadOrphansAndRequeuesTasks(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "orphan-node",
		Address:               "orphan-node:8081",
		CPUCapacityMillicores: 2000,
		MemCapacityMB:         1024,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	job, err := db.CreateJob(ctx, store.CreateJobParams{
		Name:                 "orphan-job",
		Image:                "arbiter-workload:latest",
		CPURequestMillicores: 100,
		MemRequestMB:         64,
		Replicas:             2,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	tx, claimed, err := db.ClaimPendingTasksForScheduling(ctx, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, task := range claimed {
		if err := db.ScheduleTask(ctx, tx, task.ID, node.ID, node.Epoch); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("schedule: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
		TaskID: claimed[0].ID,
		Status: store.TaskStatusRunning,
		NodeID: node.ID,
		Epoch:  node.Epoch,
	}); err != nil {
		t.Fatalf("running: %v", err)
	}

	result, err := db.MarkNodeDead(ctx, node.ID)
	if err != nil {
		t.Fatalf("MarkNodeDead: %v", err)
	}
	if len(result.OrphanedTaskIDs) != 2 {
		t.Fatalf("expected 2 orphaned tasks, got %d", len(result.OrphanedTaskIDs))
	}

	tasks, err := db.ListTasks(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	for _, task := range tasks {
		if task.Status != store.TaskStatusPending {
			t.Fatalf("expected pending after orphan requeue, got %s", task.Status)
		}
		if task.AssignedNodeID != nil {
			t.Fatalf("expected cleared assignment, got %v", *task.AssignedNodeID)
		}
	}
	alloc, err := db.GetNodeAllocation(ctx, node.ID)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if alloc.CPUMillicores != 0 || alloc.MemoryMB != 0 {
		t.Fatalf("expected zero allocation after orphan, got %+v", alloc)
	}
}

func TestFailedTaskRetriesThenExhausts(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	pool := rawPool(t)

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "retry-node",
		Address:               "retry-node:8081",
		CPUCapacityMillicores: 1000,
		MemCapacityMB:         512,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if _, err := db.CreateJob(ctx, store.CreateJobParams{
		Name:                 "retry-job",
		Image:                "arbiter-workload:latest",
		CPURequestMillicores: 50,
		MemRequestMB:         32,
		Replicas:             1,
		RetryLimit:           2,
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	tx, claimed, err := db.ClaimPendingTasksForScheduling(ctx, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	taskID := claimed[0].ID
	if err := db.ScheduleTask(ctx, tx, taskID, node.ID, node.Epoch); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("schedule: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	code := int32(1)
	for attempt := 1; attempt <= 2; attempt++ {
		if _, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
			TaskID: taskID, Status: store.TaskStatusRunning, NodeID: node.ID, Epoch: node.Epoch,
		}); err != nil {
			t.Fatalf("running attempt %d: %v", attempt, err)
		}
		updated, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
			TaskID: taskID, Status: store.TaskStatusFailed, ExitCode: &code, Error: "boom",
			NodeID: node.ID, Epoch: node.Epoch,
		})
		if err != nil {
			t.Fatalf("failed attempt %d: %v", attempt, err)
		}
		if updated.Status != store.TaskStatusPending {
			t.Fatalf("attempt %d: expected pending retry, got %s", attempt, updated.Status)
		}
		if updated.RetriesUsed != int32(attempt) {
			t.Fatalf("attempt %d: retries_used=%d", attempt, updated.RetriesUsed)
		}
		if updated.NextRetryAt == nil {
			t.Fatal("expected next_retry_at")
		}
		if _, err := pool.Exec(ctx, `UPDATE tasks SET next_retry_at = NULL WHERE id = $1`, taskID); err != nil {
			t.Fatalf("clear next_retry_at: %v", err)
		}
		tx, claimed, err = db.ClaimPendingTasksForScheduling(ctx, 1)
		if err != nil {
			t.Fatalf("reclaim %d: %v", attempt, err)
		}
		if len(claimed) != 1 {
			_ = tx.Rollback(ctx)
			t.Fatalf("reclaim %d: expected 1 task", attempt)
		}
		if err := db.ScheduleTask(ctx, tx, taskID, node.ID, node.Epoch); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("reschedule %d: %v", attempt, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit %d: %v", attempt, err)
		}
	}

	if _, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
		TaskID: taskID, Status: store.TaskStatusRunning, NodeID: node.ID, Epoch: node.Epoch,
	}); err != nil {
		t.Fatalf("final running: %v", err)
	}
	final, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
		TaskID: taskID, Status: store.TaskStatusFailed, ExitCode: &code, Error: "boom",
		NodeID: node.ID, Epoch: node.Epoch,
	})
	if err != nil {
		t.Fatalf("final failed: %v", err)
	}
	if final.Status != store.TaskStatusFailed {
		t.Fatalf("expected terminal failed after exhausting retries, got %s", final.Status)
	}
}

func TestStaleTaskReportRejected(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "fence-node",
		Address:               "fence-node:8081",
		CPUCapacityMillicores: 1000,
		MemCapacityMB:         512,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if _, err := db.CreateJob(ctx, store.CreateJobParams{
		Name: "fence-job", Image: "img", CPURequestMillicores: 50, MemRequestMB: 32, Replicas: 1,
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	tx, claimed, err := db.ClaimPendingTasksForScheduling(ctx, 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	taskID := claimed[0].ID
	if err := db.ScheduleTask(ctx, tx, taskID, node.ID, node.Epoch); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("schedule: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	_, err = db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
		TaskID: taskID, Status: store.TaskStatusRunning,
		NodeID: node.ID, Epoch: node.Epoch + 1,
	})
	if err != store.ErrStaleTaskReport {
		t.Fatalf("expected ErrStaleTaskReport, got %v", err)
	}
}

func TestClaimPendingRespectsNextRetryAt(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	pool := rawPool(t)

	if _, err := db.CreateJob(ctx, store.CreateJobParams{
		Name: "delay-job", Image: "img", CPURequestMillicores: 50, MemRequestMB: 32, Replicas: 1,
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE tasks SET next_retry_at = now() + interval '1 hour'`); err != nil {
		t.Fatalf("set next_retry_at: %v", err)
	}
	tx, claimed, err := db.ClaimPendingTasksForScheduling(ctx, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	_ = tx.Rollback(ctx)
	if len(claimed) != 0 {
		t.Fatalf("expected no claimable tasks while next_retry_at is in the future, got %d", len(claimed))
	}
}
