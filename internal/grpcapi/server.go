// Package grpcapi hosts the gRPC server implementations for the
// ClusterControl (worker<->scheduler) and SchedulerAPI (client-facing)
// services defined in proto/arbiter/v1/arbiter.proto.
//
// Phase 0 wires up the protobuf/gRPC toolchain end to end (proto -> codegen
// -> a server that registers and serves the generated service descriptors)
// without any real business logic yet. RegisterNode, Heartbeat, SubmitJob,
// etc. are implemented in later phases (see IMPLEMENTATION_PLAN.md).
package grpcapi

import (
	arbiterv1 "github.com/AnishK05/arbiter-distributed-scheduler/gen/arbiter/v1"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// Server implements both arbiterv1.ClusterControlServer and
// arbiterv1.SchedulerAPIServer. Embedding the generated Unimplemented*
// structs means every RPC not yet overridden still correctly returns a
// codes.Unimplemented error, so this struct can grow one real method at a
// time across phases without breaking the build.
type Server struct {
	arbiterv1.UnimplementedClusterControlServer
	arbiterv1.UnimplementedSchedulerAPIServer

	store *store.Store
}

// New constructs a Server backed by the given store. Later phases will add
// further dependencies (e.g. *cache.Client for heartbeats, the placement
// engine, the leader-election handle) as constructor arguments.
func New(s *store.Store) *Server {
	return &Server{store: s}
}
