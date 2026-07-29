// Package dockerutil provides small Docker Engine helpers shared by the
// worker executor and the scheduler's orphan-container reaper (Phase 5).
// Like internal/executor, this talks HTTP over the unix socket and avoids
// the heavy docker/docker SDK.
package dockerutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	dockerSock           = "/var/run/docker.sock"
	LabelAutoscaled      = "arbiter.autoscaled"
	LabelRole            = "arbiter.role"
	LabelManaged         = "arbiter.managed"
	LabelWorkerHostname  = "arbiter.hostname"
	AutoscaledLabelValue = "true"
	RoleWorker           = "worker"
)

// Client is a minimal Engine API client.
type Client struct {
	http *http.Client
}

// New returns a Client dialing the local Docker unix socket.
func New() *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", dockerSock)
		},
	}
	return &Client{http: &http.Client{Transport: transport, Timeout: 60 * time.Second}}
}

// KillTaskContainers force-removes any containers labeled arbiter.task_id
// for the given task IDs. Used when a node is marked dead so sibling DooD
// task containers don't keep running after reassignment (IMPLEMENTATION_PLAN.md
// Section 6.5). Missing containers are ignored.
func (c *Client) KillTaskContainers(ctx context.Context, taskIDs []string) error {
	var firstErr error
	for _, id := range taskIDs {
		if err := c.killByTaskLabel(ctx, id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Client) killByTaskLabel(ctx context.Context, taskID string) error {
	containers, err := c.listByLabels(ctx, map[string]string{"arbiter.task_id": taskID}, true)
	if err != nil {
		return err
	}
	for _, ctr := range containers {
		if err := c.RemoveContainer(ctx, ctr.ID, true); err != nil {
			return err
		}
	}
	return nil
}

// TaskLogs returns recent stdout/stderr for the container labeled with
// arbiter.task_id=<taskID>. Returns ("", false, nil) when no container exists.
func (c *Client) TaskLogs(ctx context.Context, taskID string, tail int) (string, bool, error) {
	if tail <= 0 {
		tail = 200
	}
	containers, err := c.listByLabels(ctx, map[string]string{"arbiter.task_id": taskID}, true)
	if err != nil {
		return "", false, err
	}
	if len(containers) == 0 {
		return "", false, nil
	}
	logsURL := fmt.Sprintf(
		"http://localhost/containers/%s/logs?stdout=1&stderr=1&timestamps=1&tail=%d",
		containers[0].ID, tail,
	)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", dockerSock)
		},
	}
	httpClient := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, logsURL, nil)
	if err != nil {
		return "", false, err
	}
	logsResp, err := httpClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = logsResp.Body.Close() }()
	raw, err := io.ReadAll(logsResp.Body)
	if err != nil {
		return "", false, err
	}
	if logsResp.StatusCode >= 300 {
		return "", false, fmt.Errorf("docker logs: status %d: %s", logsResp.StatusCode, string(raw))
	}
	return decodeDockerLogs(raw), true, nil
}

// WorkerSpec describes an autoscaled worker container to launch (Phase 9).
type WorkerSpec struct {
	Name              string
	Hostname          string
	Image             string
	Network           string
	SchedulerAddr     string
	SchedulerAddrs    string
	CPUMillicores     int64
	MemMB             int64
	HTTPAddr          string
	ExtraHosts        []string
	DockerSockBind    string
}

// ContainerSummary is a subset of Engine list/inspect fields.
type ContainerSummary struct {
	ID     string
	Name   string
	Labels map[string]string
	State  string
}

// CreateAndStartWorker creates and starts a worker container on the host
// Docker daemon (DooD). The container is labeled arbiter.autoscaled=true so
// only the autoscaler reclaims it — never compose-static workers.
func (c *Client) CreateAndStartWorker(ctx context.Context, spec WorkerSpec) (string, error) {
	if spec.Name == "" || spec.Hostname == "" || spec.Image == "" {
		return "", fmt.Errorf("dockerutil: worker name, hostname, and image are required")
	}
	if spec.HTTPAddr == "" {
		spec.HTTPAddr = ":8081"
	}
	if spec.DockerSockBind == "" {
		spec.DockerSockBind = "/var/run/docker.sock:/var/run/docker.sock"
	}
	if spec.SchedulerAddr == "" {
		spec.SchedulerAddr = "scheduler:7000"
	}

	env := []string{
		"ARBITER_SCHEDULER_ADDR=" + spec.SchedulerAddr,
		"DOCKER_HOST=unix:///var/run/docker.sock",
	}
	if spec.SchedulerAddrs != "" {
		env = append(env, "ARBITER_SCHEDULER_ADDRS="+spec.SchedulerAddrs)
	}

	cmd := []string{
		"--hostname=" + spec.Hostname,
		"--address=" + spec.Hostname + spec.HTTPAddr,
		"--http-addr=" + spec.HTTPAddr,
		fmt.Sprintf("--cpu-capacity-millicores=%d", spec.CPUMillicores),
		fmt.Sprintf("--mem-capacity-mb=%d", spec.MemMB),
		"--labels=autoscaled=true",
	}

	hostConfig := map[string]any{
		"Binds":         []string{spec.DockerSockBind},
		"RestartPolicy": map[string]any{"Name": "unless-stopped"},
	}
	if len(spec.ExtraHosts) > 0 {
		hostConfig["ExtraHosts"] = spec.ExtraHosts
	}

	createBody := map[string]any{
		"Image":    spec.Image,
		"Hostname": spec.Hostname,
		"Env":      env,
		"Cmd":      cmd,
		"Labels": map[string]string{
			LabelManaged:        AutoscaledLabelValue,
			LabelRole:           RoleWorker,
			LabelAutoscaled:     AutoscaledLabelValue,
			LabelWorkerHostname: spec.Hostname,
		},
		"HostConfig": hostConfig,
	}
	if spec.Network != "" {
		createBody["NetworkingConfig"] = map[string]any{
			"EndpointsConfig": map[string]any{
				spec.Network: map[string]any{},
			},
		}
	}

	raw, err := json.Marshal(createBody)
	if err != nil {
		return "", err
	}
	createURL := "http://localhost/containers/create?name=" + url.QueryEscape(spec.Name)
	resp, err := c.do(ctx, http.MethodPost, createURL, raw)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("docker create worker: status %d: %s", resp.StatusCode, string(body))
	}
	var created struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(body, &created); err != nil || created.ID == "" {
		return "", fmt.Errorf("docker create worker: invalid response")
	}

	startURL := "http://localhost/containers/" + created.ID + "/start"
	startResp, err := c.do(ctx, http.MethodPost, startURL, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = startResp.Body.Close() }()
	startBody, _ := io.ReadAll(startResp.Body)
	if startResp.StatusCode >= 300 {
		_ = c.RemoveContainer(ctx, created.ID, true)
		return "", fmt.Errorf("docker start worker: status %d: %s", startResp.StatusCode, string(startBody))
	}
	return created.ID, nil
}

// ListAutoscaledWorkers returns running/exited containers with the
// arbiter.autoscaled=true label.
func (c *Client) ListAutoscaledWorkers(ctx context.Context) ([]ContainerSummary, error) {
	return c.listByLabels(ctx, map[string]string{LabelAutoscaled: AutoscaledLabelValue}, true)
}

// RemoveContainer force-removes a container by ID or name.
func (c *Client) RemoveContainer(ctx context.Context, idOrName string, force bool) error {
	delURL := "http://localhost/containers/" + idOrName + "?force=" + fmt.Sprintf("%t", force)
	resp, err := c.do(ctx, http.MethodDelete, delURL, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("docker remove: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// DetectComposeNetwork returns ARBITER_DOCKER_NETWORK if set, otherwise
// inspects this process's container (by hostname) and returns its first
// non-null network name.
func (c *Client) DetectComposeNetwork(ctx context.Context) (string, error) {
	if n := strings.TrimSpace(os.Getenv("ARBITER_DOCKER_NETWORK")); n != "" {
		return n, nil
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "", fmt.Errorf("dockerutil: cannot detect network: no hostname")
	}
	inspectURL := "http://localhost/containers/" + url.PathEscape(hostname) + "/json"
	resp, err := c.do(ctx, http.MethodGet, inspectURL, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("docker inspect self: status %d: %s", resp.StatusCode, string(body))
	}
	var info struct {
		NetworkSettings struct {
			Networks map[string]json.RawMessage `json:"Networks"`
		} `json:"NetworkSettings"`
	}
	if err := json.Unmarshal(body, &info); err != nil {
		return "", err
	}
	for name := range info.NetworkSettings.Networks {
		if name != "" && name != "none" && name != "host" {
			return name, nil
		}
	}
	return "", fmt.Errorf("dockerutil: no usable network on self container")
}

func (c *Client) listByLabels(ctx context.Context, labels map[string]string, all bool) ([]ContainerSummary, error) {
	labelFilters := make([]string, 0, len(labels))
	for k, v := range labels {
		labelFilters = append(labelFilters, k+"="+v)
	}
	filter, err := json.Marshal(map[string][]string{"label": labelFilters})
	if err != nil {
		return nil, err
	}
	listURL := fmt.Sprintf("http://localhost/containers/json?all=%t&filters=%s", all, url.QueryEscape(string(filter)))
	resp, err := c.do(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("docker list: status %d: %s", resp.StatusCode, string(body))
	}
	var raw []struct {
		ID     string            `json:"Id"`
		Names  []string          `json:"Names"`
		Labels map[string]string `json:"Labels"`
		State  string            `json:"State"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	out := make([]ContainerSummary, 0, len(raw))
	for _, ctr := range raw {
		name := ""
		if len(ctr.Names) > 0 {
			name = strings.TrimPrefix(ctr.Names[0], "/")
		}
		out = append(out, ContainerSummary{
			ID:     ctr.ID,
			Name:   name,
			Labels: ctr.Labels,
			State:  ctr.State,
		})
	}
	return out, nil
}

func decodeDockerLogs(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var b strings.Builder
	i := 0
	for i+8 <= len(raw) {
		size := int(raw[i+4])<<24 | int(raw[i+5])<<16 | int(raw[i+6])<<8 | int(raw[i+7])
		i += 8
		if size < 0 || i+size > len(raw) {
			return string(raw)
		}
		b.Write(raw[i : i+size])
		i += size
	}
	if b.Len() == 0 {
		return string(raw)
	}
	return b.String()
}

func (c *Client) do(ctx context.Context, method, rawURL string, body []byte) (*http.Response, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}
