// Package executor runs assigned tasks as local Docker containers via the
// Engine HTTP API over the unix socket. We deliberately avoid the heavy
// docker/docker Go SDK (and its Go-version-pinned OpenTelemetry transitive
// deps) — the subset we need (create/start/wait/kill/remove) is a handful
// of HTTP calls.
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const dockerSock = "/var/run/docker.sock"

// TaskSpec describes a container to launch for one assigned task.
type TaskSpec struct {
	TaskID               string
	Image                string
	Command              []string
	CPURequestMillicores int64
	MemRequestMB         int64
}

// Result is reported when a container reaches a terminal state.
type Result struct {
	TaskID   string
	Status   string // succeeded | failed
	ExitCode int32
	Error    string
}

// DoneFunc is invoked once per task when its container finishes (or fails
// to start). Callers typically push a ReportTaskStatus RPC from here.
type DoneFunc func(Result)

// Executor tracks in-flight containers and talks to the local Docker Engine.
type Executor struct {
	client *http.Client
	onDone DoneFunc

	mu      sync.Mutex
	running map[string]context.CancelFunc // taskID -> cancel for wait goroutine
}

// New returns an Executor. onDone may be nil (results are discarded).
func New(onDone DoneFunc) (*Executor, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", dockerSock)
		},
	}
	return &Executor{
		client: &http.Client{
			Transport: transport,
			// Wait endpoints block until the container exits; no global timeout.
			Timeout: 0,
		},
		onDone:  onDone,
		running: make(map[string]context.CancelFunc),
	}, nil
}

// Close is a no-op for the HTTP client (kept for call-site symmetry).
func (e *Executor) Close() error { return nil }

// IsRunning reports whether taskID already has a tracked container.
func (e *Executor) IsRunning(taskID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.running[taskID]
	return ok
}

// AllocatedResources sums the resource requests of currently tracked tasks
// using the caller's spec map (the worker owns the authoritative specs).
func (e *Executor) AllocatedResources(specs map[string]TaskSpec) (cpuMillicores, memMB int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.running {
		if spec, ok := specs[id]; ok {
			cpuMillicores += spec.CPURequestMillicores
			memMB += spec.MemRequestMB
		}
	}
	return cpuMillicores, memMB
}

// Start creates and starts a container for spec, then waits for exit in a
// background goroutine. Create/start failures are returned immediately and
// also delivered via onDone as a failed Result so ReportTaskStatus still
// fires if the caller ignores the error.
func (e *Executor) Start(ctx context.Context, spec TaskSpec) error {
	e.mu.Lock()
	if _, ok := e.running[spec.TaskID]; ok {
		e.mu.Unlock()
		return nil
	}
	runCtx, cancel := context.WithCancel(context.Background())
	e.running[spec.TaskID] = cancel
	e.mu.Unlock()

	containerID, err := e.createAndStart(ctx, spec)
	if err != nil {
		e.finish(spec.TaskID, cancel, Result{
			TaskID:   spec.TaskID,
			Status:   "failed",
			ExitCode: 1,
			Error:    err.Error(),
		})
		return err
	}

	go func() {
		res := e.waitForExit(runCtx, containerID, spec.TaskID)
		e.finish(spec.TaskID, cancel, res)
	}()
	return nil
}

func (e *Executor) finish(taskID string, cancel context.CancelFunc, res Result) {
	e.mu.Lock()
	delete(e.running, taskID)
	e.mu.Unlock()
	cancel()
	if e.onDone != nil {
		e.onDone(res)
	}
}

// StopAll cancels every in-flight wait and best-effort kills containers.
func (e *Executor) StopAll() {
	e.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(e.running))
	ids := make([]string, 0, len(e.running))
	for id, cancel := range e.running {
		ids = append(ids, id)
		cancels = append(cancels, cancel)
	}
	e.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	for _, id := range ids {
		_ = e.removeContainer(context.Background(), containerName(id))
	}
}

func (e *Executor) createAndStart(ctx context.Context, spec TaskSpec) (string, error) {
	name := containerName(spec.TaskID)
	_ = e.removeContainer(ctx, name)

	// Resource requests (CPU/memory) are enforced by the scheduler's
	// accounting, not via Docker cgroup limits. Applying NanoCpus/Memory
	// here fails on some nested/cgroup-v2 hosts ("cannot enter cgroupv2 …
	// with domain controllers — it is in threaded mode"), so we deliberately
	// omit them. Capacity is still tracked in Postgres via job requests.
	createBody := map[string]any{
		"Image": spec.Image,
		"Labels": map[string]string{
			"arbiter.task_id":        spec.TaskID,
			"arbiter.managed":        "true",
			"arbiter.cpu_request_mc": fmt.Sprintf("%d", spec.CPURequestMillicores),
			"arbiter.mem_request_mb": fmt.Sprintf("%d", spec.MemRequestMB),
		},
		"HostConfig": map[string]any{
			"AutoRemove": true,
		},
	}
	if len(spec.Command) > 0 {
		createBody["Cmd"] = spec.Command
	}
	raw, err := json.Marshal(createBody)
	if err != nil {
		return "", err
	}

	createURL := "http://localhost/containers/create?name=" + url.QueryEscape(name)
	resp, err := e.doJSON(ctx, http.MethodPost, createURL, raw, 30*time.Second)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("docker create: status %d: %s", resp.StatusCode, string(body))
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(body, &created); err != nil || created.ID == "" {
		return "", fmt.Errorf("docker create: invalid response")
	}

	startURL := "http://localhost/containers/" + created.ID + "/start"
	startResp, err := e.doJSON(ctx, http.MethodPost, startURL, nil, 30*time.Second)
	if err != nil {
		return "", err
	}
	startBody, _ := io.ReadAll(startResp.Body)
	_ = startResp.Body.Close()
	if startResp.StatusCode >= 300 {
		return "", fmt.Errorf("docker start: status %d: %s", startResp.StatusCode, string(startBody))
	}
	return created.ID, nil
}

func (e *Executor) waitForExit(ctx context.Context, containerID, taskID string) Result {
	waitURL := "http://localhost/containers/" + containerID + "/wait"
	waitResp, err := e.doJSON(ctx, http.MethodPost, waitURL, nil, 0)
	if err != nil {
		if ctx.Err() != nil {
			_ = e.killContainer(context.Background(), containerID)
			return Result{TaskID: taskID, Status: "failed", ExitCode: 137, Error: "canceled"}
		}
		return Result{TaskID: taskID, Status: "failed", ExitCode: 1, Error: err.Error()}
	}
	defer func() { _ = waitResp.Body.Close() }()
	waitRaw, _ := io.ReadAll(waitResp.Body)
	var waited struct {
		StatusCode int `json:"StatusCode"`
	}
	_ = json.Unmarshal(waitRaw, &waited)

	exit := int32(waited.StatusCode)
	if exit != 0 {
		return Result{
			TaskID:   taskID,
			Status:   "failed",
			ExitCode: exit,
			Error:    fmt.Sprintf("exit code %d", exit),
		}
	}
	return Result{TaskID: taskID, Status: "succeeded", ExitCode: 0}
}

func containerName(taskID string) string {
	return "arbiter-task-" + taskID
}

func (e *Executor) removeContainer(ctx context.Context, name string) error {
	u := "http://localhost/containers/" + url.PathEscape(name) + "?force=true"
	resp, err := e.doJSON(ctx, http.MethodDelete, u, nil, 15*time.Second)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

func (e *Executor) killContainer(ctx context.Context, id string) error {
	u := "http://localhost/containers/" + id + "/kill"
	resp, err := e.doJSON(ctx, http.MethodPost, u, nil, 15*time.Second)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

func (e *Executor) doJSON(ctx context.Context, method, rawURL string, body []byte, timeout time.Duration) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	reqCtx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(reqCtx, method, rawURL, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return e.client.Do(req)
}
