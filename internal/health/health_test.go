package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandlerHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if body := rec.Body.String(); body != "ok" {
		t.Fatalf("expected body %q, got %q", "ok", body)
	}
}

func TestMuxMountsMetrics(t *testing.T) {
	metrics := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("metric"))
	})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Mux(metrics).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "metric" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}
