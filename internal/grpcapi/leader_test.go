package grpcapi

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
)

type staticLeader struct {
	leader bool
	addr   string
}

func (s staticLeader) IsLeader() bool     { return s.leader }
func (s staticLeader) LeaderAddr() string { return s.addr }

func TestRequireLeaderRejectsFollower(t *testing.T) {
	s := New(nil, nil, 500, staticLeader{leader: false, addr: "leader:7000"}, nil)
	_, err := s.SubmitJob(context.Background(), &arbiterv1.SubmitJobRequest{
		Name:    "j",
		Image:   "img",
		Request: &arbiterv1.NodeResources{CpuMillicores: 100, MemoryMb: 64},
	})
	if err == nil {
		t.Fatal("expected NOT_LEADER error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
	if !strings.Contains(st.Message(), "NOT_LEADER: current leader at leader:7000") {
		t.Fatalf("unexpected message: %s", st.Message())
	}
}

func TestRequireLeaderAllowsLeader(t *testing.T) {
	// Validation still runs; missing capacity fails with InvalidArgument,
	// proving we passed the leader gate.
	s := New(nil, nil, 500, staticLeader{leader: true, addr: "leader:7000"}, nil)
	_, err := s.RegisterNode(context.Background(), &arbiterv1.RegisterNodeRequest{
		Hostname: "h",
		Address:  "a",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument after leader gate, got %v", err)
	}
}
