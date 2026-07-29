package grpcapi

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// Heartbeat implements arbiterv1.ClusterControlServer as a simple unary RPC
// (IMPLEMENTATION_PLAN.md Section 7 notes bidirectional streaming as a
// possible later upgrade once this works). Each call:
//  1. Looks up the node; unknown node IDs get NotFound so the worker knows
//     to RegisterNode from scratch.
//  2. Compares epochs. A mismatch means this node was declared dead (its
//     epoch was bumped by MarkNodeDead) since this process last registered
//     — e.g. it was on the wrong side of a network partition. It gets
//     epoch_invalid=true and nothing else happens; the worker is expected to
//     kill any local state and re-register (full fencing of running tasks
//     lands in Phase 5).
//  3. On a matching epoch, records liveness in Redis, applies any bundled
//     task status updates, recovers not_ready → ready if needed, and returns
//     any tasks newly scheduled onto this node as assignments.
func (s *Server) Heartbeat(ctx context.Context, req *arbiterv1.HeartbeatRequest) (*arbiterv1.HeartbeatResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	node, err := s.store.GetNode(ctx, req.GetNodeId())
	if errors.Is(err, store.ErrNodeNotFound) {
		return nil, status.Error(codes.NotFound, "node is not registered; call RegisterNode first")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
	}

	if req.GetEpoch() != node.Epoch {
		return &arbiterv1.HeartbeatResponse{EpochInvalid: true}, nil
	}

	if err := s.cache.SetLastSeen(ctx, node.ID); err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: record last-seen: %v", err)
	}

	for _, update := range req.GetTaskUpdates() {
		if err := s.applyTaskStatusUpdate(ctx, update); err != nil {
			// Don't fail the whole heartbeat for a bad/stale task update —
			// liveness was already recorded. Log-equivalent: surface as a
			// non-OK status code only for unexpected internal errors.
			if st, ok := status.FromError(err); ok && st.Code() == codes.Internal {
				return nil, err
			}
		}
	}

	if node.Status == store.NodeStatusNotReady {
		if _, err := s.store.UpdateNodeStatus(ctx, node.ID, store.NodeStatusReady, store.EventTypeNodeRecovered, "node recovered: heartbeat resumed before the dead threshold"); err != nil {
			return nil, status.Errorf(codes.Internal, "heartbeat: recover node: %v", err)
		}
	}

	scheduled, err := s.store.ListScheduledTasksForNode(ctx, node.ID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: list assignments: %v", err)
	}

	resp := &arbiterv1.HeartbeatResponse{
		NewAssignments: make([]*arbiterv1.TaskAssignment, 0, len(scheduled)),
	}
	for _, t := range scheduled {
		var assignedEpoch int64
		if t.AssignedEpoch != nil {
			assignedEpoch = *t.AssignedEpoch
		}
		resp.NewAssignments = append(resp.NewAssignments, &arbiterv1.TaskAssignment{
			TaskId:  t.ID,
			Image:   t.Image,
			Command: t.Command,
			Request: &arbiterv1.NodeResources{
				CpuMillicores: t.CPURequestMillicores,
				MemoryMb:      t.MemRequestMB,
			},
			AssignedEpoch: assignedEpoch,
		})
	}
	return resp, nil
}
