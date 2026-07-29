// Package health provides HTTP handlers shared by the scheduler and worker
// binaries: /healthz always, and optionally /metrics (Phase 7).
package health

import (
	"net/http"
)

// Handler returns an http.Handler that responds 200 OK on /healthz.
func Handler() http.Handler {
	return Mux(nil)
}

// Mux returns a ServeMux with /healthz and, when metrics is non-nil, /metrics.
func Mux(metrics http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if metrics != nil {
		mux.Handle("/metrics", metrics)
	}
	return mux
}
