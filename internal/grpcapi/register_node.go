package grpcapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// defaultHeartbeatIntervalMS is advertised to workers in RegisterNodeResponse
// so the interval is configured server-side. The failure detector (Phase 2)
// will use the same constant to derive its missed-heartbeat threshold.
const defaultHeartbeatIntervalMS = 1000

// RegisterNode implements arbiterv1.ClusterControlServer. See
// store.Store.RegisterNode for the upsert-by-(hostname,address) semantics.
func (s *Server) RegisterNode(ctx context.Context, req *arbiterv1.RegisterNodeRequest) (*arbiterv1.RegisterNodeResponse, error) {
	if req.GetHostname() == "" {
		return nil, status.Error(codes.InvalidArgument, "hostname is required")
	}
	if req.GetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "address is required")
	}
	capacity := req.GetCapacity()
	if capacity.GetCpuMillicores() <= 0 || capacity.GetMemoryMb() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "capacity.cpu_millicores and capacity.memory_mb must both be positive")
	}

	node, err := s.store.RegisterNode(ctx, store.RegisterNodeParams{
		Hostname:              req.GetHostname(),
		Address:               req.GetAddress(),
		CPUCapacityMillicores: capacity.GetCpuMillicores(),
		MemCapacityMB:         capacity.GetMemoryMb(),
		Labels:                req.GetLabels(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "register node: %v", err)
	}

	return &arbiterv1.RegisterNodeResponse{
		NodeId:              node.ID,
		Epoch:               node.Epoch,
		HeartbeatIntervalMs: defaultHeartbeatIntervalMS,
	}, nil
}
