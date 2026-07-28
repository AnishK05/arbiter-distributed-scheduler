// Package health provides a minimal HTTP health-check handler shared by the
// scheduler and worker binaries. It will grow to report richer status (e.g.
// leader/follower state, DB connectivity) in later phases.
package health

import "net/http"

// Handler returns an http.Handler that responds 200 OK on /healthz. It is
// intentionally trivial in Phase 0 — later phases can wire in real
// dependency checks (Postgres, Redis, leader election state, etc.).
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}
