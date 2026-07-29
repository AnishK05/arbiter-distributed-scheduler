package grpcapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// RegisterNode implements arbiterv1.ClusterControlServer. See
// store.Store.RegisterNode for the upsert-by-(hostname,address) semantics.
func (s *Server) RegisterNode(ctx context.Context, req *arbiterv1.RegisterNodeRequest) (*arbiterv1.RegisterNodeResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
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

	// Seed Redis immediately so the failure detector's very first poll
	// after registration sees a fresh heartbeat rather than a missing key
	// (which it would otherwise have to treat as "maximally stale").
	if err := s.cache.SetLastSeen(ctx, node.ID); err != nil {
		return nil, status.Errorf(codes.Internal, "register node: seed last-seen: %v", err)
	}

	return &arbiterv1.RegisterNodeResponse{
		NodeId:              node.ID,
		Epoch:               node.Epoch,
		HeartbeatIntervalMs: s.heartbeatIntervalMS,
	}, nil
}
