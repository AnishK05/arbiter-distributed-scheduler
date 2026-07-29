// Package cache is the Redis-backed ephemeral/fast-path state layer for
// Arbiter: heartbeat last-seen timestamps and pub/sub for the dashboard's
// live event feed (Phase 8). It is never the source of truth for durable
// cluster state — that's internal/store's job. See IMPLEMENTATION_PLAN.md
// Section 6.1 for the rationale.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClusterEventsChannel is the Redis pub/sub channel the leader fans events
// onto for SSE/WebSocket dashboard subscribers.
const ClusterEventsChannel = "pubsub:cluster-events"

// lastSeenTTL bounds how long a heartbeat timestamp survives in Redis
// without being refreshed. It's set well above any realistic
// dead-node-detection threshold (IMPLEMENTATION_PLAN.md Section 6.4 target:
// a few seconds) so the key's own expiry is never what triggers a dead
// verdict — the failuredetector's age comparison always gets there first.
// A missing key (GetLastSeen's ok=false) is instead treated as "maximally
// stale" by the failure detector, which is a safe fallback either way.
const lastSeenTTL = 5 * time.Minute

// Client wraps a Redis connection.
type Client struct {
	rdb *redis.Client
}

// Connect parses addr (a redis:// URL or bare host:port) and verifies
// connectivity with a ping.
func Connect(ctx context.Context, addr string) (*Client, error) {
	opts, err := parseAddr(addr)
	if err != nil {
		return nil, fmt.Errorf("cache: parse redis address: %w", err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("cache: ping redis: %w", err)
	}
	return &Client{rdb: rdb}, nil
}

func parseAddr(addr string) (*redis.Options, error) {
	if strings.HasPrefix(addr, "redis://") || strings.HasPrefix(addr, "rediss://") {
		return redis.ParseURL(addr)
	}
	return &redis.Options{Addr: addr}, nil
}

// Close releases the underlying connection(s).
func (c *Client) Close() error {
	return c.rdb.Close()
}

func lastSeenKey(nodeID string) string {
	return "hb:" + nodeID
}

// SetLastSeen records that nodeID sent a heartbeat right now.
func (c *Client) SetLastSeen(ctx context.Context, nodeID string) error {
	return c.SetLastSeenAt(ctx, nodeID, time.Now())
}

// SetLastSeenAt records an explicit last-seen timestamp for nodeID. Exposed
// (rather than folded into SetLastSeen) so tests can deterministically
// simulate a stale node without sleeping in wall-clock time.
func (c *Client) SetLastSeenAt(ctx context.Context, nodeID string, t time.Time) error {
	if err := c.rdb.Set(ctx, lastSeenKey(nodeID), strconv.FormatInt(t.UnixMilli(), 10), lastSeenTTL).Err(); err != nil {
		return fmt.Errorf("cache: set last seen: %w", err)
	}
	return nil
}

// GetLastSeen returns the last recorded heartbeat time for nodeID. ok is
// false if no heartbeat has ever been recorded, or the key has expired
// (see lastSeenTTL) — callers should treat that the same as "very stale".
func (c *Client) GetLastSeen(ctx context.Context, nodeID string) (t time.Time, ok bool, err error) {
	val, err := c.rdb.Get(ctx, lastSeenKey(nodeID)).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("cache: get last seen: %w", err)
	}
	ms, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("cache: parse last seen value %q: %w", val, err)
	}
	return time.UnixMilli(ms), true, nil
}

// DeleteLastSeen removes the heartbeat key for nodeID, if present.
func (c *Client) DeleteLastSeen(ctx context.Context, nodeID string) error {
	if err := c.rdb.Del(ctx, lastSeenKey(nodeID)).Err(); err != nil {
		return fmt.Errorf("cache: delete last seen: %w", err)
	}
	return nil
}

// ClusterEvent is the JSON payload published on ClusterEventsChannel.
type ClusterEvent struct {
	ID         int64     `json:"id"`
	EntityType string    `json:"entity_type"`
	EntityID   string    `json:"entity_id"`
	EventType  string    `json:"event_type"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
}

// PublishClusterEvent publishes a durable-store event onto Redis pub/sub.
func (c *Client) PublishClusterEvent(ctx context.Context, ev ClusterEvent) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("cache: marshal cluster event: %w", err)
	}
	if err := c.rdb.Publish(ctx, ClusterEventsChannel, payload).Err(); err != nil {
		return fmt.Errorf("cache: publish cluster event: %w", err)
	}
	return nil
}

// SubscribeClusterEvents subscribes to ClusterEventsChannel. The returned
// channel is closed when ctx is cancelled or the subscription fails.
func (c *Client) SubscribeClusterEvents(ctx context.Context) (<-chan ClusterEvent, error) {
	sub := c.rdb.Subscribe(ctx, ClusterEventsChannel)
	if _, err := sub.Receive(ctx); err != nil {
		_ = sub.Close()
		return nil, fmt.Errorf("cache: subscribe cluster events: %w", err)
	}
	out := make(chan ClusterEvent, 64)
	go func() {
		defer close(out)
		defer func() { _ = sub.Close() }()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var ev ClusterEvent
				if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
					continue
				}
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}
