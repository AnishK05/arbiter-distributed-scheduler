package leaderclient

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseLeaderAddr(t *testing.T) {
	t.Parallel()

	addr, ok := ParseLeaderAddr(status.Error(codes.FailedPrecondition, "NOT_LEADER: current leader at host.docker.internal:7001"))
	if !ok || addr != "host.docker.internal:7001" {
		t.Fatalf("got %q %v", addr, ok)
	}

	if _, ok := ParseLeaderAddr(status.Error(codes.InvalidArgument, "NOT_LEADER: current leader at x:1")); ok {
		t.Fatal("wrong code should not parse")
	}
	if _, ok := ParseLeaderAddr(status.Error(codes.FailedPrecondition, "NOT_LEADER: leader address unknown; retry shortly")); ok {
		t.Fatal("unknown leader should not parse")
	}
	if _, ok := ParseLeaderAddr(nil); ok {
		t.Fatal("nil should not parse")
	}
}
