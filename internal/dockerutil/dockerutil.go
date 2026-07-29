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
	"strings"
	"time"
)

const dockerSock = "/var/run/docker.sock"

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
	return &Client{http: &http.Client{Transport: transport, Timeout: 30 * time.Second}}
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
	filter, err := json.Marshal(map[string][]string{
		"label": {"arbiter.task_id=" + taskID},
	})
	if err != nil {
		return err
	}
	listURL := "http://localhost/containers/json?all=true&filters=" + url.QueryEscape(string(filter))
	resp, err := c.do(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("docker list: status %d: %s", resp.StatusCode, string(body))
	}
	var containers []struct {
		ID    string `json:"Id"`
		Names []string
	}
	if err := json.Unmarshal(body, &containers); err != nil {
		return err
	}
	for _, ctr := range containers {
		delURL := "http://localhost/containers/" + ctr.ID + "?force=true"
		delResp, err := c.do(ctx, http.MethodDelete, delURL, nil)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, delResp.Body)
		_ = delResp.Body.Close()
	}
	return nil
}

// TaskLogs returns recent stdout/stderr for the container labeled with
// arbiter.task_id=<taskID>. Returns ("", false, nil) when no container exists.
func (c *Client) TaskLogs(ctx context.Context, taskID string, tail int) (string, bool, error) {
	if tail <= 0 {
		tail = 200
	}
	filter, err := json.Marshal(map[string][]string{
		"label": {"arbiter.task_id=" + taskID},
	})
	if err != nil {
		return "", false, err
	}
	listURL := "http://localhost/containers/json?all=true&filters=" + url.QueryEscape(string(filter))
	resp, err := c.do(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return "", false, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("docker list: status %d: %s", resp.StatusCode, string(body))
	}
	var containers []struct {
		ID string `json:"Id"`
	}
	if err := json.Unmarshal(body, &containers); err != nil {
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
