// Package leaderclient helps workers and arbiterctl follow NOT_LEADER
// redirects returned by follower scheduler replicas (Phase 6).
package leaderclient

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const notLeaderPrefix = "NOT_LEADER: current leader at "

// ParseLeaderAddr extracts the advertise address from a FailedPrecondition
// NOT_LEADER error. Returns ("", false) when err is not a redirect.
func ParseLeaderAddr(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return "", false
	}
	msg := st.Message()
	if !strings.HasPrefix(msg, notLeaderPrefix) {
		return "", false
	}
	addr := strings.TrimSpace(strings.TrimPrefix(msg, notLeaderPrefix))
	if addr == "" || strings.Contains(addr, "unknown") {
		return "", false
	}
	return addr, true
}
