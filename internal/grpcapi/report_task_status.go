package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// ReportTaskStatus implements arbiterv1.ClusterControlServer: workers push
// immediate status changes (running/succeeded/failed) here rather than
// waiting for the next heartbeat.
func (s *Server) ReportTaskStatus(ctx context.Context, req *arbiterv1.TaskStatusUpdate) (*arbiterv1.Ack, error) {
	if err := s.applyTaskStatusUpdate(ctx, req); err != nil {
		return nil, err
	}
	return &arbiterv1.Ack{}, nil
}

func (s *Server) applyTaskStatusUpdate(ctx context.Context, req *arbiterv1.TaskStatusUpdate) error {
	if req.GetTaskId() == "" {
		return status.Error(codes.InvalidArgument, "task_id is required")
	}
	switch req.GetStatus() {
	case store.TaskStatusRunning, store.TaskStatusSucceeded, store.TaskStatusFailed:
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported task status %q", req.GetStatus())
	}

	params := store.UpdateTaskStatusParams{
		TaskID: req.GetTaskId(),
		Status: req.GetStatus(),
		Error:  req.GetError(),
	}
	if req.GetStatus() == store.TaskStatusSucceeded || req.GetStatus() == store.TaskStatusFailed {
		code := req.GetExitCode()
		params.ExitCode = &code
	}

	_, err := s.store.UpdateTaskStatus(ctx, params)
	if errors.Is(err, store.ErrTaskNotFound) {
		return status.Error(codes.NotFound, "task not found")
	}
	if err != nil {
		return status.Errorf(codes.Internal, "update task status: %v", err)
	}
	return nil
}
