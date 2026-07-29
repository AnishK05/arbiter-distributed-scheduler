// Package grpcapi hosts the gRPC server implementations for the
// ClusterControl (worker<->scheduler) and SchedulerAPI (client-facing)
// services defined in proto/arbiter/v1/arbiter.proto.
package grpcapi

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/election"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/metrics"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// LeaderGate reports whether this replica should accept mutating RPCs.
// *election.Elector implements it; nil means "always leader" (single-replica
// tests / Phase < 6 call sites).
type LeaderGate interface {
	IsLeader() bool
	LeaderAddr() string
}

// Server implements both arbiterv1.ClusterControlServer and
// arbiterv1.SchedulerAPIServer.
type Server struct {
	arbiterv1.UnimplementedClusterControlServer
	arbiterv1.UnimplementedSchedulerAPIServer

	store *store.Store
	cache *cache.Client

	// heartbeatIntervalMS is advertised to workers in RegisterNodeResponse.
	heartbeatIntervalMS int32

	// leader is optional. When set, mutating RPCs on a follower return
	// NOT_LEADER with the current leader's advertise address.
	leader LeaderGate

	metrics *metrics.Registry
}

// New constructs a Server. leader and met may be nil.
func New(s *store.Store, c *cache.Client, heartbeatIntervalMS int32, leader LeaderGate, met *metrics.Registry) *Server {
	return &Server{store: s, cache: c, heartbeatIntervalMS: heartbeatIntervalMS, leader: leader, metrics: met}
}

// requireLeader returns a FailedPrecondition error when this replica is not
// the elected leader. Documented choice: reject with redirect address rather
// than transparently proxy (see docs/design-decisions.md Phase 6).
func (s *Server) requireLeader() error {
	if s.leader == nil || s.leader.IsLeader() {
		return nil
	}
	addr := s.leader.LeaderAddr()
	if addr == "" {
		return status.Error(codes.FailedPrecondition, "NOT_LEADER: leader address unknown; retry shortly")
	}
	return status.Errorf(codes.FailedPrecondition, "NOT_LEADER: current leader at %s", addr)
}

// Ensure *election.Elector satisfies LeaderGate at compile time.
var _ LeaderGate = (*election.Elector)(nil)

// FormatNotLeaderError is exported for tests/clients that parse the message.
func FormatNotLeaderError(addr string) string {
	return fmt.Sprintf("NOT_LEADER: current leader at %s", addr)
}
