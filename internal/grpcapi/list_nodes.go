package grpcapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// ListNodes implements arbiterv1.SchedulerAPIServer. It's a thin read-only
// wrapper over store.Store.ListNodes, with per-node allocated resources
// filled from currently scheduled/running tasks.
func (s *Server) ListNodes(ctx context.Context, _ *arbiterv1.ListNodesRequest) (*arbiterv1.ListNodesResponse, error) {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list nodes: %v", err)
	}
	allocs, err := s.store.GetNodeAllocations(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list nodes: allocations: %v", err)
	}

	resp := &arbiterv1.ListNodesResponse{Nodes: make([]*arbiterv1.Node, 0, len(nodes))}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, toProtoNode(n, allocs[n.ID]))
	}
	return resp, nil
}

func toProtoNode(n store.Node, alloc store.NodeAllocation) *arbiterv1.Node {
	return &arbiterv1.Node{
		Id:       n.ID,
		Hostname: n.Hostname,
		Address:  n.Address,
		Capacity: &arbiterv1.NodeResources{
			CpuMillicores: n.CPUCapacityMillicores,
			MemoryMb:      n.MemCapacityMB,
		},
		Allocated: &arbiterv1.NodeResources{
			CpuMillicores: alloc.CPUMillicores,
			MemoryMb:      alloc.MemoryMB,
		},
		Labels: n.Labels,
		Status: n.Status,
		Epoch:  n.Epoch,
	}
}
