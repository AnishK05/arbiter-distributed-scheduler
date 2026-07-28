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
)

// Server implements both arbiterv1.ClusterControlServer and
// arbiterv1.SchedulerAPIServer. Embedding the generated Unimplemented*
// structs means every RPC correctly returns a codes.Unimplemented error
// until it's overridden with a real implementation in a later phase.
type Server struct {
	arbiterv1.UnimplementedClusterControlServer
	arbiterv1.UnimplementedSchedulerAPIServer
}

// New constructs a Server with no dependencies wired up yet. Later phases
// will take a *store.Store, *cache.Client, etc. as constructor arguments.
func New() *Server {
	return &Server{}
}
