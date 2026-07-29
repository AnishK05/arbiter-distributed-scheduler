package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/metrics"
)

func TestMetricsHandlerExposesCollectors(t *testing.T) {
	m := metrics.New()
	m.ObserveTaskStatus("running")
	m.LeaderElectionsTotal.Inc()
	m.QueueDepth.Set(3)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	m.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d", rr.Code)
	}
	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"arbiter_tasks_total",
		"arbiter_leader_elections_total",
		"arbiter_queue_depth",
		"arbiter_scheduling_latency_seconds",
		"arbiter_failover_seconds",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics body missing %q", want)
		}
	}
}
