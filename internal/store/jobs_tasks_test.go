package store_test

import (
	"context"
	"testing"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

func TestCreateJobExpandsReplicas(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	job, err := db.CreateJob(ctx, store.CreateJobParams{
		Name:                 "demo",
		Image:                "arbiter-workload:latest",
		CPURequestMillicores: 100,
		MemRequestMB:         64,
		Replicas:             5,
		RetryLimit:           3,
		SchedulingPolicy:     store.SchedulingPolicyBinPack,
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	if job.ID == "" {
		t.Fatal("expected job id")
	}
	if job.Replicas != 5 {
		t.Fatalf("expected replicas=5, got %d", job.Replicas)
	}

	tasks, err := db.ListTasks(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 5 {
		t.Fatalf("expected 5 tasks, got %d", len(tasks))
	}
	for _, task := range tasks {
		if task.Status != store.TaskStatusPending {
			t.Fatalf("expected pending, got %s", task.Status)
		}
		if task.JobID != job.ID {
			t.Fatalf("task job_id mismatch")
		}
	}

	events, err := db.ListEventsForEntity(ctx, store.EntityTypeJob, job.ID)
	if err != nil {
		t.Fatalf("ListEventsForEntity: %v", err)
	}
	if len(events) != 1 || events[0].EventType != store.EventTypeJobSubmitted {
		t.Fatalf("expected one job_submitted event, got %+v", events)
	}
}

func TestClaimAndScheduleTask(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "sched-node",
		Address:               "sched-node:8081",
		CPUCapacityMillicores: 2000,
		MemCapacityMB:         1024,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}

	job, err := db.CreateJob(ctx, store.CreateJobParams{
		Name:                 "place-me",
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
		t.Fatalf("ClaimPendingTasksForScheduling: %v", err)
	}
	if len(claimed) != 2 {
		_ = tx.Rollback(ctx)
		t.Fatalf("expected 2 claimed tasks, got %d", len(claimed))
	}
	for _, task := range claimed {
		if err := db.ScheduleTask(ctx, tx, task.ID, node.ID, node.Epoch); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("ScheduleTask: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tasks, err := db.ListTasks(ctx, job.ID)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	for _, task := range tasks {
		if task.Status != store.TaskStatusScheduled {
			t.Fatalf("expected scheduled, got %s", task.Status)
		}
		if task.AssignedNodeID == nil || *task.AssignedNodeID != node.ID {
			t.Fatalf("expected assigned to %s", node.ID)
		}
	}

	alloc, err := db.GetNodeAllocation(ctx, node.ID)
	if err != nil {
		t.Fatalf("GetNodeAllocation: %v", err)
	}
	if alloc.CPUMillicores != 200 || alloc.MemoryMB != 128 {
		t.Fatalf("unexpected allocation: %+v", alloc)
	}

	scheduled, err := db.ListScheduledTasksForNode(ctx, node.ID)
	if err != nil {
		t.Fatalf("ListScheduledTasksForNode: %v", err)
	}
	if len(scheduled) != 2 {
		t.Fatalf("expected 2 scheduled assignments, got %d", len(scheduled))
	}
}

func TestUpdateTaskStatusLifecycle(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()

	node, err := db.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              "run-node",
		Address:               "run-node:8081",
		CPUCapacityMillicores: 1000,
		MemCapacityMB:         512,
	})
	if err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	job, err := db.CreateJob(ctx, store.CreateJobParams{
		Name:                 "lifecycle",
		Image:                "arbiter-workload:latest",
		CPURequestMillicores: 50,
		MemRequestMB:         32,
		Replicas:             1,
	})
	if err != nil {
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

	running, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
		TaskID: taskID,
		Status: store.TaskStatusRunning,
	})
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	if running.Status != store.TaskStatusRunning {
		t.Fatalf("expected running, got %s", running.Status)
	}

	exit := int32(0)
	done, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
		TaskID:   taskID,
		Status:   store.TaskStatusSucceeded,
		ExitCode: &exit,
	})
	if err != nil {
		t.Fatalf("succeeded: %v", err)
	}
	if done.Status != store.TaskStatusSucceeded || done.ExitCode == nil || *done.ExitCode != 0 {
		t.Fatalf("unexpected terminal task: %+v", done)
	}

	// Idempotent re-report.
	again, err := db.UpdateTaskStatus(ctx, store.UpdateTaskStatusParams{
		TaskID:   taskID,
		Status:   store.TaskStatusSucceeded,
		ExitCode: &exit,
	})
	if err != nil {
		t.Fatalf("idempotent succeeded: %v", err)
	}
	if again.Status != store.TaskStatusSucceeded {
		t.Fatalf("expected still succeeded, got %s", again.Status)
	}

	// Capacity released once terminal.
	alloc, err := db.GetNodeAllocation(ctx, node.ID)
	if err != nil {
		t.Fatalf("alloc: %v", err)
	}
	if alloc.CPUMillicores != 0 || alloc.MemoryMB != 0 {
		t.Fatalf("expected zero allocation after success, got %+v", alloc)
	}

	_ = job
}
