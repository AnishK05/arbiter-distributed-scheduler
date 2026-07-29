package grpcapi

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
)

func TestSubmitJobValidation(t *testing.T) {
	tests := []struct {
		name string
		req  *arbiterv1.SubmitJobRequest
	}{
		{
			name: "missing name",
			req: &arbiterv1.SubmitJobRequest{
				Image:   "img",
				Request: &arbiterv1.NodeResources{CpuMillicores: 100, MemoryMb: 64},
			},
		},
		{
			name: "missing image",
			req: &arbiterv1.SubmitJobRequest{
				Name:    "job",
				Request: &arbiterv1.NodeResources{CpuMillicores: 100, MemoryMb: 64},
			},
		},
		{
			name: "missing request",
			req: &arbiterv1.SubmitJobRequest{
				Name:  "job",
				Image: "img",
			},
		},
		{
			name: "zero cpu",
			req: &arbiterv1.SubmitJobRequest{
				Name:    "job",
				Image:   "img",
				Request: &arbiterv1.NodeResources{CpuMillicores: 0, MemoryMb: 64},
			},
		},
		{
			name: "bad policy",
			req: &arbiterv1.SubmitJobRequest{
				Name:             "job",
				Image:            "img",
				Request:          &arbiterv1.NodeResources{CpuMillicores: 100, MemoryMb: 64},
				SchedulingPolicy: "random",
			},
		},
	}

	s := New(nil, nil, 500, nil, nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.SubmitJob(context.Background(), tt.req)
			if err == nil {
				t.Fatal("expected error")
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("expected InvalidArgument, got %v (%v)", got, err)
			}
		})
	}
}

func TestReportTaskStatusValidation(t *testing.T) {
	s := New(nil, nil, 500, nil, nil)
	_, err := s.ReportTaskStatus(context.Background(), &arbiterv1.TaskStatusUpdate{
		Status: "running",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for missing task_id, got %v", err)
	}

	_, err = s.ReportTaskStatus(context.Background(), &arbiterv1.TaskStatusUpdate{
		TaskId: "t1",
		Status: "pending",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument for bad status, got %v", err)
	}
}
