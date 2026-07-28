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
