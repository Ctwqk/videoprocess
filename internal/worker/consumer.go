package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/redis/go-redis/v9"
)

var ErrConfirmedCancellation = errors.New("confirmed cancellation")

// Handler executes a single node's media transform. Each implementation is
// responsible for resolving input/output paths via the Storage backend and
// returning either nil on success or an error. Returning a wrapped
// ffmpeg.ErrCancelled tells the consumer to skip ack and event publication.
type Handler interface {
	NodeType() string
	Execute(ctx context.Context, task TaskMessage) (NodeResult, error)
}

type NodeResult struct {
	OutputArtifactID string
}

// TaskMessage is the decoded Redis Streams payload the Python orchestrator
// writes to `vp:tasks:{worker_type}`. Keys mirror the producer side in
// `backend/app/orchestrator/engine.py`.
type TaskMessage struct {
	JobID              string         `json:"job_id"`
	NodeExecutionID    string         `json:"node_execution_id"`
	NodeID             string         `json:"node_id"`
	NodeType           string         `json:"node_type"`
	Config             map[string]any `json:"config"`
	InputArtifacts     map[string]any `json:"input_artifacts"`
	PreferredHosts     []string       `json:"preferred_hosts"`
	AffinityEnqueuedAt string         `json:"affinity_enqueued_at"`
	AffinityBounces    string         `json:"affinity_bounces"`
}

// Consumer drives the Redis Streams loop for one Go worker instance. The
// design mirrors the Python worker minus the heartbeat/PEL-reclaim helpers,
// which can be added in a follow-up alongside cancellation listeners.
type Consumer struct {
	Redis         *redis.Client
	WorkerType    string
	WorkerID      string
	ConsumerGroup string
	BlockTimeout  time.Duration
	handlers      map[string]Handler
	log           *slog.Logger
}

// NewConsumer wires a consumer with sensible defaults and the handler set
// supplied by the caller. The consumer group name is
// `{worker_type}-workers` to match the Python convention.
func NewConsumer(client *redis.Client, cfg Config, handlers ...Handler) *Consumer {
	registry := make(map[string]Handler, len(handlers))
	for _, h := range handlers {
		registry[h.NodeType()] = h
	}
	return &Consumer{
		Redis:         client,
		WorkerType:    cfg.WorkerType,
		WorkerID:      cfg.WorkerID,
		ConsumerGroup: cfg.WorkerType + "-workers",
		BlockTimeout:  5 * time.Second,
		handlers:      registry,
		log:           slog.With("worker_id", cfg.WorkerID, "worker_type", cfg.WorkerType),
	}
}

// EnsureGroup creates the consumer group if it does not yet exist. The
// `MKSTREAM` flag mirrors the Python implementation: a freshly booted system
// can start its workers before any task has ever been enqueued.
func (c *Consumer) EnsureGroup(ctx context.Context) error {
	stream := redisstream.TaskStream(c.WorkerType)
	if err := c.Redis.XGroupCreateMkStream(ctx, stream, c.ConsumerGroup, "$").Err(); err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("create consumer group %s: %w", c.ConsumerGroup, err)
		}
	}
	return nil
}

// Run blocks until ctx is done, claiming tasks from Redis Streams and
// dispatching them to the registered handlers. Each iteration claims at most
// one task to keep the loop deterministic for tests; production tuning of
// batch size is a follow-up.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.EnsureGroup(ctx); err != nil {
		return err
	}
	stream := redisstream.TaskStream(c.WorkerType)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := c.Redis.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.ConsumerGroup,
			Consumer: c.WorkerID,
			Streams:  []string{stream, ">"},
			Block:    c.BlockTimeout,
			Count:    1,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			c.log.Warn("xreadgroup failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				c.handleMessage(ctx, msg)
			}
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, msg redis.XMessage) {
	task, err := decodeTask(msg.Values)
	if err != nil {
		c.log.Error("invalid task payload", "msg_id", msg.ID, "error", err)
		_ = c.publishFailed(ctx, task, fmt.Sprintf("invalid task payload: %v", err))
		c.ack(ctx, msg.ID)
		return
	}

	handler, ok := c.handlers[task.NodeType]
	if !ok {
		c.log.Error("no handler", "msg_id", msg.ID, "node_type", task.NodeType)
		_ = c.publishFailed(ctx, task, fmt.Sprintf("no handler for node_type %q", task.NodeType))
		c.ack(ctx, msg.ID)
		return
	}

	result, err := handler.Execute(ctx, task)
	switch {
	case err == nil:
		if strings.TrimSpace(result.OutputArtifactID) == "" {
			_ = c.publishFailed(ctx, task, "handler succeeded without output_artifact_id")
			c.ack(ctx, msg.ID)
			return
		}
		_ = c.publishCompleted(ctx, task, result.OutputArtifactID)
		c.ack(ctx, msg.ID)
	case errors.Is(err, ErrConfirmedCancellation):
		c.log.Info("task cancelled by recorded job/node state, acking without event", "msg_id", msg.ID, "node_id", task.NodeID)
		c.ack(ctx, msg.ID)
	case errors.Is(err, context.Canceled):
		c.log.Info("worker context cancelled, leaving message pending", "msg_id", msg.ID, "node_id", task.NodeID)
	default:
		c.log.Error("handler failed", "msg_id", msg.ID, "node_id", task.NodeID, "error", err)
		_ = c.publishFailed(ctx, task, err.Error())
		c.ack(ctx, msg.ID)
	}
}

func (c *Consumer) ack(ctx context.Context, msgID string) {
	stream := redisstream.TaskStream(c.WorkerType)
	if err := c.Redis.XAck(ctx, stream, c.ConsumerGroup, msgID).Err(); err != nil {
		c.log.Warn("xack failed", "msg_id", msgID, "error", err)
	}
}

func (c *Consumer) publishCompleted(ctx context.Context, task TaskMessage, artifactID string) error {
	return redisstream.PublishNodeCompleted(ctx, c.Redis, redisstream.NodeEvent{
		JobID:            task.JobID,
		NodeExecutionID:  task.NodeExecutionID,
		OutputArtifactID: artifactID,
	})
}

func (c *Consumer) publishFailed(ctx context.Context, task TaskMessage, errMsg string) error {
	return redisstream.PublishNodeFailed(ctx, c.Redis, redisstream.NodeEvent{
		JobID:           task.JobID,
		NodeExecutionID: task.NodeExecutionID,
		Error:           errMsg,
	})
}

// decodeTask converts the Redis stream's string-valued map into a structured
// TaskMessage. JSON-valued fields (config, input_artifacts) are unmarshalled
// to preserve the Python orchestrator's payload semantics.
func decodeTask(values map[string]any) (TaskMessage, error) {
	get := func(key string) string {
		v, _ := values[key].(string)
		return v
	}
	task := TaskMessage{
		JobID:              get("job_id"),
		NodeExecutionID:    get("node_execution_id"),
		NodeID:             get("node_id"),
		NodeType:           get("node_type"),
		AffinityEnqueuedAt: get("affinity_enqueued_at"),
		AffinityBounces:    get("affinity_bounces"),
	}
	if raw := get("config"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &task.Config); err != nil {
			return task, fmt.Errorf("decode config: %w", err)
		}
	}
	if raw := get("input_artifacts"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &task.InputArtifacts); err != nil {
			return task, fmt.Errorf("decode input_artifacts: %w", err)
		}
	}
	if raw := get("preferred_hosts"); raw != "" {
		_ = json.Unmarshal([]byte(raw), &task.PreferredHosts)
	}
	return task, nil
}
