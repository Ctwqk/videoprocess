package redisstream

import (
	"context"
	"errors"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"
)

const EventStream = "vp:events"

const idempotentWorkerEventXAddScript = `
local existing = redis.call('GET', KEYS[2])
if existing then
    return existing
end
local message_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV))
redis.call('SET', KEYS[2], message_id)
return message_id
`

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

func PublishIdempotentWorkerEvent(
	ctx context.Context,
	client *redis.Client,
	stream string,
	marker string,
	values map[string]string,
) (string, error) {
	if client == nil ||
		strings.TrimSpace(stream) == "" ||
		strings.TrimSpace(marker) == "" ||
		len(values) == 0 {
		return "", errors.New("worker event publication is invalid")
	}
	keys := make([]string, 0, len(values))
	for key, value := range values {
		if strings.TrimSpace(key) == "" || value == "" {
			return "", errors.New("worker event publication is invalid")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	arguments := make([]any, 0, len(values)*2)
	for _, key := range keys {
		arguments = append(arguments, key, values[key])
	}
	result, err := client.Eval(
		ctx,
		idempotentWorkerEventXAddScript,
		[]string{stream, marker},
		arguments...,
	).Result()
	if err != nil {
		return "", errors.New("worker event publication failed")
	}
	messageID, ok := result.(string)
	if !ok ||
		strings.TrimSpace(messageID) == "" ||
		messageID != strings.TrimSpace(messageID) {
		return "", errors.New("worker event publication result is invalid")
	}
	return messageID, nil
}

func AcknowledgeWorkerTask(
	ctx context.Context,
	client *redis.Client,
	stream string,
	group string,
	messageID string,
) (int64, error) {
	if client == nil ||
		strings.TrimSpace(stream) == "" ||
		strings.TrimSpace(group) == "" ||
		strings.TrimSpace(messageID) == "" {
		return 0, errors.New("worker task acknowledgement is invalid")
	}
	result, err := client.XAck(ctx, stream, group, messageID).Result()
	if err != nil {
		return 0, errors.New("worker task acknowledgement failed")
	}
	if result != 0 && result != 1 {
		return 0, errors.New("worker task acknowledgement result is invalid")
	}
	return result, nil
}
