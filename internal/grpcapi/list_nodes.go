package grpcapi

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// ListNodes implements arbiterv1.SchedulerAPIServer. It's a thin read-only
// wrapper over store.Store.ListNodes, added alongside RegisterNode so the
// Phase 1 "a node row appears with correct capacity" acceptance check can be
// done through the system's own API instead of a direct DB query.
func (s *Server) ListNodes(ctx context.Context, _ *arbiterv1.ListNodesRequest) (*arbiterv1.ListNodesResponse, error) {
	nodes, err := s.store.ListNodes(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list nodes: %v", err)
	}

	resp := &arbiterv1.ListNodesResponse{Nodes: make([]*arbiterv1.Node, 0, len(nodes))}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, toProtoNode(n))
	}
	return resp, nil
}

func toProtoNode(n store.Node) *arbiterv1.Node {
	return &arbiterv1.Node{
		Id:       n.ID,
		Hostname: n.Hostname,
		Address:  n.Address,
		Capacity: &arbiterv1.NodeResources{
			CpuMillicores: n.CPUCapacityMillicores,
			MemoryMb:      n.MemCapacityMB,
		},
		Labels: n.Labels,
		Status: n.Status,
		Epoch:  n.Epoch,
	}
}
