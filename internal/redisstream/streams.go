package redisstream

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const EventStream = "vp:events"

func TaskStream(workerType string) string {
	return "vp:tasks:" + workerType
}

type NodeEvent struct {
	Event            string
	EventStream      string
	JobID            string
	NodeExecutionID  string
	OutputArtifactID string
	Error            string
}

func (event NodeEvent) streamOrDefault() string {
	if event.EventStream != "" {
		return event.EventStream
	}
	return EventStream
}

func PublishNodeCompleted(ctx context.Context, client *redis.Client, event NodeEvent) error {
	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: event.streamOrDefault(),
		Values: map[string]any{
			"event":              "node_completed",
			"job_id":             event.JobID,
			"node_execution_id":  event.NodeExecutionID,
			"output_artifact_id": event.OutputArtifactID,
		},
	}).Err()
}

func PublishNodeFailed(ctx context.Context, client *redis.Client, event NodeEvent) error {
	errorText := event.Error
	if len(errorText) > 2000 {
		errorText = errorText[:2000]
	}
	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: event.streamOrDefault(),
		Values: map[string]any{
			"event":             "node_failed",
			"job_id":            event.JobID,
			"node_execution_id": event.NodeExecutionID,
			"error":             errorText,
		},
	}).Err()
}
