package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/health"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/httpapi"
)

type staticLeader struct {
	leader bool
	addr   string
}

func (s staticLeader) IsLeader() bool    { return s.leader }
func (s staticLeader) LeaderAddr() string { return s.addr }

func TestSubmitJobRejectedOnFollower(t *testing.T) {
	api := httpapi.New(nil, nil, nil, staticLeader{leader: false, addr: "leader:7000"}, nil, health.Handler())
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", nil)
	rr := httptest.NewRecorder()
	api.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rr.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["error"] != "NOT_LEADER" || body["leader_addr"] != "leader:7000" {
		t.Fatalf("unexpected body %#v", body)
	}
}
