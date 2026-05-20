package redisstream

import (
	"context"
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

func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return client
}
