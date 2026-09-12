package redisstream

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestTaskStream(t *testing.T) {
	if got := TaskStream("ffmpeg_go"); got != "vp:tasks:ffmpeg_go" {
		t.Fatalf("TaskStream = %q", got)
	}
}

func TestPublishNodeCompletedUsesDefaultEventStream(t *testing.T) {
	client := newTestRedis(t)

	if err := PublishNodeCompleted(context.Background(), client, NodeEvent{
		JobID:            "job-1",
		NodeExecutionID:  "ne-1",
		OutputArtifactID: "artifact-1",
	}); err != nil {
		t.Fatalf("PublishNodeCompleted: %v", err)
	}

	events, err := client.XRange(context.Background(), EventStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange default stream: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("default stream events = %d; want 1", len(events))
	}
}

func TestPublishNodeCompletedUsesExplicitEventStream(t *testing.T) {
	client := newTestRedis(t)
	explicitStream := "vp:events:go"

	if err := PublishNodeCompleted(context.Background(), client, NodeEvent{
		EventStream:      explicitStream,
		JobID:            "job-1",
		NodeExecutionID:  "ne-1",
		OutputArtifactID: "artifact-1",
	}); err != nil {
		t.Fatalf("PublishNodeCompleted: %v", err)
	}

	explicitEvents, err := client.XRange(context.Background(), explicitStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange explicit stream: %v", err)
	}
	if len(explicitEvents) != 1 {
		t.Fatalf("explicit stream events = %d; want 1", len(explicitEvents))
	}
	defaultEvents, err := client.XRange(context.Background(), EventStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange default stream: %v", err)
	}
	if len(defaultEvents) != 0 {
		t.Fatalf("default stream events = %d; want 0", len(defaultEvents))
	}
}

func TestRegistrationPublishEventIsIdempotentByDurableMarker(t *testing.T) {
	client := newTestRedis(t)
	ctx := context.Background()
	stream := "vp:test:events"
	marker := "vp:worker-event-emission:00000000-0000-0000-0000-000000000001"
	values := map[string]string{
		"event":             "node_completed",
		"job_id":            "00000000-0000-0000-0000-000000000002",
		"node_execution_id": "00000000-0000-0000-0000-000000000003",
	}
	first, err := PublishIdempotentWorkerEvent(
		ctx,
		client,
		stream,
		marker,
		values,
	)
	if err != nil {
		t.Fatalf("first idempotent publish: %v", err)
	}
	second, err := PublishIdempotentWorkerEvent(
		ctx,
		client,
		stream,
		marker,
		values,
	)
	if err != nil {
		t.Fatalf("replayed idempotent publish: %v", err)
	}
	if first == "" || second != first {
		t.Fatalf("event ids first=%q second=%q; want one stable id", first, second)
	}
	events, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil || len(events) != 1 {
		t.Fatalf("events after replay = %#v, err=%v; want one", events, err)
	}
	if stored, err := client.Get(ctx, marker).Result(); err != nil || stored != first {
		t.Fatalf("durable marker = %q, err=%v; want %q", stored, err, first)
	}
}

func TestRegistrationAcknowledgeTaskReturnsExactRedisResult(t *testing.T) {
	client := newTestRedis(t)
	ctx := context.Background()
	stream := "vp:test:tasks"
	group := "vp-test-workers"
	if err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		t.Fatalf("create task group: %v", err)
	}
	messageID, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"dispatch_key": strings.Repeat("a", 36)},
	}).Result()
	if err != nil {
		t.Fatalf("add task: %v", err)
	}
	if _, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "worker-1",
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result(); err != nil {
		t.Fatalf("deliver task: %v", err)
	}
	first, err := AcknowledgeWorkerTask(ctx, client, stream, group, messageID)
	if err != nil || first != 1 {
		t.Fatalf("first XACK result = %d, err=%v; want 1", first, err)
	}
	second, err := AcknowledgeWorkerTask(ctx, client, stream, group, messageID)
	if err != nil || second != 0 {
		t.Fatalf("replayed XACK result = %d, err=%v; want 0", second, err)
	}
}

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return client
}
