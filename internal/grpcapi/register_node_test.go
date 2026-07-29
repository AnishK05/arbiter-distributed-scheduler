package grpcapi

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
)

// These cases exercise only the request-validation branches, which return
// before the (nil) store is ever touched. Success-path behavior (the actual
// upsert semantics) is covered by internal/store's integration tests, since
// it requires a real Postgres connection.
func TestRegisterNodeValidation(t *testing.T) {
	tests := []struct {
		name string
		req  *arbiterv1.RegisterNodeRequest
	}{
		{
			name: "missing hostname",
			req: &arbiterv1.RegisterNodeRequest{
				Address:  "host:1234",
				Capacity: &arbiterv1.NodeResources{CpuMillicores: 1000, MemoryMb: 512},
			},
		},
		{
			name: "missing address",
			req: &arbiterv1.RegisterNodeRequest{
				Hostname: "host",
				Capacity: &arbiterv1.NodeResources{CpuMillicores: 1000, MemoryMb: 512},
			},
		},
		{
			name: "missing capacity",
			req: &arbiterv1.RegisterNodeRequest{
				Hostname: "host",
				Address:  "host:1234",
			},
		},
		{
			name: "zero cpu capacity",
			req: &arbiterv1.RegisterNodeRequest{
				Hostname: "host",
				Address:  "host:1234",
				Capacity: &arbiterv1.NodeResources{CpuMillicores: 0, MemoryMb: 512},
			},
		},
		{
			name: "zero memory capacity",
			req: &arbiterv1.RegisterNodeRequest{
				Hostname: "host",
				Address:  "host:1234",
				Capacity: &arbiterv1.NodeResources{CpuMillicores: 1000, MemoryMb: 0},
			},
		},
	}

	s := New(nil, nil, 500, nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.RegisterNode(context.Background(), tt.req)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("expected codes.InvalidArgument, got %v (%v)", got, err)
			}
		})
	}
}
