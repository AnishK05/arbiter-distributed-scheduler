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
