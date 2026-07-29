// Package httpapi is a thin hand-written JSON REST surface for the Phase 8
// dashboard. Chosen over grpc-gateway to stay within the Go 1.22.2 pin and
// keep SSE custom wiring simple (see docs/design-decisions.md).
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/AnishK05/arbiter-distributed-scheduler/internal/cache"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/dockerutil"
	"github.com/AnishK05/arbiter-distributed-scheduler/internal/store"
)

// LeaderGate mirrors grpcapi.LeaderGate for mutate gating.
type LeaderGate interface {
	IsLeader() bool
	LeaderAddr() string
}

// API hosts REST handlers.
type API struct {
	store   *store.Store
	cache   *cache.Client
	docker  *dockerutil.Client
	leader  LeaderGate
	logger  *slog.Logger
	baseMux *http.ServeMux
}

// New constructs an API. docker and leader may be nil.
func New(s *store.Store, c *cache.Client, docker *dockerutil.Client, leader LeaderGate, logger *slog.Logger, base http.Handler) *API {
	if logger == nil {
		logger = slog.Default()
	}
	mux := http.NewServeMux()
	if base != nil {
		mux.Handle("/", base)
	}
	a := &API{store: s, cache: c, docker: docker, leader: leader, logger: logger, baseMux: mux}
	a.routes()
	return a
}

// Handler returns the combined mux (/healthz, /metrics, /api/...).
func (a *API) Handler() http.Handler {
	return withCORS(a.baseMux)
}

func (a *API) routes() {
	a.baseMux.HandleFunc("GET /api/v1/nodes", a.handleListNodes)
	a.baseMux.HandleFunc("GET /api/v1/jobs", a.handleListJobs)
	a.baseMux.HandleFunc("GET /api/v1/jobs/{id}", a.handleGetJob)
	a.baseMux.HandleFunc("POST /api/v1/jobs", a.handleSubmitJob)
	a.baseMux.HandleFunc("GET /api/v1/tasks", a.handleListTasks)
	a.baseMux.HandleFunc("GET /api/v1/tasks/{id}", a.handleGetTask)
	a.baseMux.HandleFunc("GET /api/v1/tasks/{id}/logs", a.handleTaskLogs)
	a.baseMux.HandleFunc("GET /api/v1/events", a.handleListEvents)
	a.baseMux.HandleFunc("GET /api/v1/events/stream", a.handleEventsStream)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *API) requireLeader(w http.ResponseWriter) bool {
	if a.leader == nil || a.leader.IsLeader() {
		return true
	}
	addr := a.leader.LeaderAddr()
	writeJSON(w, http.StatusConflict, map[string]string{
		"error":       "NOT_LEADER",
		"leader_addr": addr,
	})
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (a *API) handleListNodes(w http.ResponseWriter, r *http.Request) {
	nodes, err := a.store.ListNodes(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	allocs, err := a.store.GetNodeAllocations(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(nodes))
	for _, n := range nodes {
		a := allocs[n.ID]
		out = append(out, map[string]any{
			"id":        n.ID,
			"hostname":  n.Hostname,
			"address":   n.Address,
			"status":    n.Status,
			"epoch":     n.Epoch,
			"labels":    n.Labels,
			"capacity":  map[string]int64{"cpu_millicores": n.CPUCapacityMillicores, "memory_mb": n.MemCapacityMB},
			"allocated": map[string]int64{"cpu_millicores": a.CPUMillicores, "memory_mb": a.MemoryMB},
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": out})
}

func (a *API) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.ListJobs(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (a *API) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := a.store.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrJobNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, job)
}

type submitJobBody struct {
	Name             string            `json:"name"`
	Image            string            `json:"image"`
	Command          []string          `json:"command"`
	CPUMillicores    int64             `json:"cpu_millicores"`
	MemoryMB         int64             `json:"memory_mb"`
	Replicas         int32             `json:"replicas"`
	RetryLimit       int32             `json:"retry_limit"`
	SchedulingPolicy string            `json:"scheduling_policy"`
	Constraints      map[string]string `json:"constraints"`
}

func (a *API) handleSubmitJob(w http.ResponseWriter, r *http.Request) {
	if !a.requireLeader(w) {
		return
	}
	var body submitJobBody
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if body.Name == "" || body.Image == "" || body.CPUMillicores <= 0 || body.MemoryMB <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name, image, cpu_millicores, and memory_mb are required"})
		return
	}
	if body.Replicas <= 0 {
		body.Replicas = 1
	}
	if body.RetryLimit <= 0 {
		body.RetryLimit = 3
	}
	if body.SchedulingPolicy == "" {
		body.SchedulingPolicy = store.SchedulingPolicyBinPack
	}
	job, err := a.store.CreateJob(r.Context(), store.CreateJobParams{
		Name:                 body.Name,
		Image:                body.Image,
		Command:              body.Command,
		CPURequestMillicores: body.CPUMillicores,
		MemRequestMB:         body.MemoryMB,
		Replicas:             body.Replicas,
		RetryLimit:           body.RetryLimit,
		SchedulingPolicy:     body.SchedulingPolicy,
		Constraints:          body.Constraints,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, job)
}

func (a *API) handleListTasks(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	tasks, err := a.store.ListTasks(r.Context(), jobID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (a *API) handleGetTask(w http.ResponseWriter, r *http.Request) {
	task, err := a.store.GetTask(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrTaskNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (a *API) handleTaskLogs(w http.ResponseWriter, r *http.Request) {
	if a.docker == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "docker client unavailable"})
		return
	}
	tail := 200
	if v := r.URL.Query().Get("tail"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			tail = n
		}
	}
	logs, ok, err := a.docker.TaskLogs(r.Context(), r.PathValue("id"), tail)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "no container found for task (may have exited and been auto-removed)",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"logs": logs})
}

func (a *API) handleListEvents(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	events, err := a.store.ListRecentEvents(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (a *API) handleEventsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Snapshot recent history first so the UI paints immediately.
	recent, err := a.store.ListRecentEvents(r.Context(), 30)
	if err == nil {
		for _, ev := range recent {
			writeSSE(w, ev)
		}
		flusher.Flush()
	}

	if a.cache == nil {
		// Fall back to polling Postgres when Redis is unavailable.
		a.pollEventsSSE(w, flusher, r)
		return
	}

	ch, err := a.cache.SubscribeClusterEvents(r.Context())
	if err != nil {
		a.logger.Warn("httpapi: redis subscribe failed; falling back to poll", "error", err)
		a.pollEventsSSE(w, flusher, r)
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			writeSSE(w, store.Event{
				ID:         ev.ID,
				EntityType: ev.EntityType,
				EntityID:   ev.EntityID,
				EventType:  ev.EventType,
				Message:    ev.Message,
				CreatedAt:  ev.CreatedAt,
			})
			flusher.Flush()
		}
	}
}

func (a *API) pollEventsSSE(w http.ResponseWriter, flusher http.Flusher, r *http.Request) {
	var afterID int64
	recent, _ := a.store.ListRecentEvents(r.Context(), 1)
	if len(recent) > 0 {
		afterID = recent[len(recent)-1].ID
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			events, err := a.store.ListEventsAfter(r.Context(), afterID, 100)
			if err != nil {
				continue
			}
			for _, ev := range events {
				writeSSE(w, ev)
				afterID = ev.ID
			}
			if len(events) > 0 {
				flusher.Flush()
			}
		}
	}
}

func writeSSE(w http.ResponseWriter, ev store.Event) {
	payload, _ := json.Marshal(ev)
	_, _ = fmt.Fprintf(w, "id: %d\ndata: %s\n\n", ev.ID, payload)
}
