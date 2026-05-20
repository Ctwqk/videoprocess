package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultGoOrchestratorGroup    = "orchestrator-go"
	defaultGoOrchestratorConsumer = "orchestrator-go-1"
	defaultGoReclaimMinIdle       = 5 * time.Minute
)

var ErrNonGoEvent = errors.New("non-Go orchestrator event ignored")

type NodeEventHandler interface {
	OnNodeCompleted(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error
	OnNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error
}

type EventListener struct {
	Client         *redis.Client
	Engine         NodeEventHandler
	Stream         string
	Group          string
	Consumer       string
	ReclaimMinIdle time.Duration
	BlockTimeout   time.Duration
	Logger         *slog.Logger
}

func (l *EventListener) EnsureGroup(ctx context.Context) error {
	if l.Client == nil {
		return errors.New("event listener redis client is nil")
	}
	if err := l.Client.XGroupCreateMkStream(ctx, l.stream(), l.group(), "0").Err(); err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("create event consumer group %s: %w", l.group(), err)
		}
	}
	return nil
}

func (l *EventListener) Run(ctx context.Context) error {
	if err := l.EnsureGroup(ctx); err != nil {
		return err
	}
	if err := l.reclaim(ctx); err != nil {
		l.logger().Warn("initial Go event reclaim failed", "error", err)
	}

	reclaimTicker := time.NewTicker(time.Minute)
	defer reclaimTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reclaimTicker.C:
			if err := l.reclaim(ctx); err != nil {
				l.logger().Warn("periodic Go event reclaim failed", "error", err)
			}
			continue
		default:
		}

		res, err := l.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    l.group(),
			Consumer: l.consumer(),
			Streams:  []string{l.stream(), ">"},
			Count:    10,
			Block:    l.blockTimeout(),
		}).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			if errors.Is(err, redis.Nil) {
				continue
			}
			l.logger().Warn("read Go event stream failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				if err := l.handle(ctx, msg); err != nil {
					eventName, _ := eventString(msg.Values, "event")
					observeGoOrchestratorEventFailure(eventName)
					observeGoOrchestratorEvent(eventName, "error")
					l.logger().Warn("handle Go event failed", "msg_id", msg.ID, "error", err)
					continue
				}
				if err := l.ack(ctx, msg.ID); err != nil {
					return err
				}
			}
		}
	}
}

func (l *EventListener) reclaim(ctx context.Context) error {
	if l.Client == nil {
		return errors.New("event listener redis client is nil")
	}
	messages, _, err := l.Client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   l.stream(),
		Group:    l.group(),
		Consumer: l.consumer(),
		MinIdle:  l.reclaimMinIdle(),
		Start:    "0-0",
		Count:    100,
	}).Result()
	if err != nil {
		observeGoOrchestratorPendingReclaim("error", 1)
		return err
	}
	observeGoOrchestratorPendingReclaim("claimed", len(messages))
	for _, msg := range messages {
		if err := l.handle(ctx, msg); err != nil {
			eventName, _ := eventString(msg.Values, "event")
			observeGoOrchestratorEventFailure(eventName)
			observeGoOrchestratorEvent(eventName, "error")
			l.logger().Warn("handle reclaimed Go event failed", "msg_id", msg.ID, "error", err)
			continue
		}
		if err := l.ack(ctx, msg.ID); err != nil {
			return err
		}
		observeGoOrchestratorPendingReclaim("acked", 1)
	}
	return nil
}

func (l *EventListener) handle(ctx context.Context, msg redis.XMessage) error {
	eventName, ok := eventString(msg.Values, "event")
	if !ok {
		observeGoOrchestratorEvent("", "malformed")
		observeGoOrchestratorEventFailure("")
		l.logger().Warn("malformed Go event missing event type", "msg_id", msg.ID)
		return nil
	}
	switch eventName {
	case "node_completed":
		jobID, jobOK := eventString(msg.Values, "job_id")
		nodeExecutionID, nodeOK := eventString(msg.Values, "node_execution_id")
		outputArtifactID, outputOK := eventString(msg.Values, "output_artifact_id")
		if !jobOK || !nodeOK || !outputOK {
			observeGoOrchestratorEvent(eventName, "malformed")
			observeGoOrchestratorEventFailure(eventName)
			l.logger().Warn("malformed node_completed Go event", "msg_id", msg.ID)
			return nil
		}
		if l.Engine == nil {
			return errors.New("event listener engine is nil")
		}
		if err := l.Engine.OnNodeCompleted(ctx, jobID, nodeExecutionID, outputArtifactID); err != nil {
			if errors.Is(err, ErrNonGoEvent) {
				observeGoOrchestratorEvent(eventName, "ignored")
				l.logger().Warn("ignored non-Go node_completed event", "msg_id", msg.ID, "job_id", jobID)
				return nil
			}
			return err
		}
		observeGoOrchestratorEvent(eventName, "handled")
		return nil
	case "node_failed":
		jobID, jobOK := eventString(msg.Values, "job_id")
		nodeExecutionID, nodeOK := eventString(msg.Values, "node_execution_id")
		if !jobOK || !nodeOK {
			observeGoOrchestratorEvent(eventName, "malformed")
			observeGoOrchestratorEventFailure(eventName)
			l.logger().Warn("malformed node_failed Go event", "msg_id", msg.ID)
			return nil
		}
		errorMessage, ok := eventString(msg.Values, "error")
		if !ok || strings.TrimSpace(errorMessage) == "" {
			errorMessage = "Unknown error"
		}
		if l.Engine == nil {
			return errors.New("event listener engine is nil")
		}
		if err := l.Engine.OnNodeFailed(ctx, jobID, nodeExecutionID, errorMessage); err != nil {
			if errors.Is(err, ErrNonGoEvent) {
				observeGoOrchestratorEvent(eventName, "ignored")
				l.logger().Warn("ignored non-Go node_failed event", "msg_id", msg.ID, "job_id", jobID)
				return nil
			}
			return err
		}
		observeGoOrchestratorEvent(eventName, "handled")
		return nil
	default:
		observeGoOrchestratorEvent(eventName, "unknown")
		observeGoOrchestratorEventFailure(eventName)
		l.logger().Warn("unknown Go event type", "msg_id", msg.ID, "event", eventName)
		return nil
	}
}

func (l *EventListener) ack(ctx context.Context, messageID string) error {
	if err := l.Client.XAck(ctx, l.stream(), l.group(), messageID).Err(); err != nil {
		return fmt.Errorf("ack Go event %s: %w", messageID, err)
	}
	return nil
}

func (l *EventListener) stream() string {
	if strings.TrimSpace(l.Stream) != "" {
		return l.Stream
	}
	return defaultGoEventStream
}

func (l *EventListener) group() string {
	if strings.TrimSpace(l.Group) != "" {
		return l.Group
	}
	return defaultGoOrchestratorGroup
}

func (l *EventListener) consumer() string {
	if strings.TrimSpace(l.Consumer) != "" {
		return l.Consumer
	}
	return defaultGoOrchestratorConsumer
}

func (l *EventListener) reclaimMinIdle() time.Duration {
	if l.ReclaimMinIdle > 0 {
		return l.ReclaimMinIdle
	}
	return defaultGoReclaimMinIdle
}

func (l *EventListener) blockTimeout() time.Duration {
	if l.BlockTimeout > 0 {
		return l.BlockTimeout
	}
	return 5 * time.Second
}

func (l *EventListener) logger() *slog.Logger {
	if l.Logger != nil {
		return l.Logger
	}
	return slog.Default()
}

func eventString(values map[string]any, key string) (string, bool) {
	value, ok := values[key]
	if !ok {
		return "", false
	}
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return "", false
	}
	return text, true
}
