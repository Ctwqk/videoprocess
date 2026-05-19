package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/Ctwqk/videoprocess/internal/worker/ffmpeg"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeHandler struct {
	node string
	err  error
	seen []TaskMessage
}

func (f *fakeHandler) NodeType() string { return f.node }
func (f *fakeHandler) Execute(ctx context.Context, task TaskMessage) error {
	f.seen = append(f.seen, task)
	return f.err
}

func newRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return client, mr
}

func enqueueTrim(t *testing.T, client *redis.Client, workerType string) string {
	t.Helper()
	stream := redisstream.TaskStream(workerType)
	configJSON, _ := json.Marshal(map[string]any{"start_time": "0", "duration": "1"})
	id, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"job_id":            "job-1",
			"node_execution_id": "ne-1",
			"node_id":           "trim_1",
			"node_type":         "trim",
			"config":            string(configJSON),
			"input_artifacts":   "{}",
			"preferred_hosts":   "[]",
		},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}
	return id
}

// runOneTick drives Run for a short window so it can process whatever the
// caller has already enqueued. EnsureGroup must have been called by the
// caller before enqueuing (the consumer group's `>` cursor only delivers
// messages enqueued AFTER group creation).
func runOneTick(t *testing.T, consumer *Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	_ = consumer.Run(ctx)
}

// withGroup creates the consumer group up-front so the subsequent enqueue is
// visible to the consumer's `>` read cursor.
func withGroup(t *testing.T, consumer *Consumer) {
	t.Helper()
	if err := consumer.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
}

func TestConsumerSuccessAcksAndEmitsCompleted(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-1"}
	handler := &fakeHandler{node: "trim"}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	msgID := enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)

	if len(handler.seen) != 1 {
		t.Fatalf("handler invocations = %d; want 1", len(handler.seen))
	}
	if handler.seen[0].JobID != "job-1" {
		t.Fatalf("job_id = %q", handler.seen[0].JobID)
	}

	stream := redisstream.TaskStream(cfg.WorkerType)
	pending, err := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending entries after ack = %d (msg %s)", pending.Count, msgID)
	}

	events, err := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d; want 1", len(events))
	}
	if events[0].Values["event"] != "node_completed" {
		t.Fatalf("event = %q", events[0].Values["event"])
	}
}

func TestConsumerHandlerFailurePublishesFailed(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-2"}
	handler := &fakeHandler{node: "trim", err: errors.New("ffmpeg failed: boom")}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)

	events, _ := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if len(events) != 1 || events[0].Values["event"] != "node_failed" {
		t.Fatalf("events = %#v", events)
	}
	if got, _ := events[0].Values["error"].(string); got == "" {
		t.Fatal("error field should be populated for failed event")
	}
}

func TestConsumerCancellationLeavesMessagePending(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-3"}
	handler := &fakeHandler{node: "trim", err: ffmpeg.ErrCancelled}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)

	stream := redisstream.TaskStream(cfg.WorkerType)
	pending, err := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("cancelled task must stay pending for PEL reclaim, got %d", pending.Count)
	}

	events, _ := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if len(events) != 0 {
		t.Fatalf("cancellation must not publish events, got %#v", events)
	}
}

func TestConsumerUnknownNodeTypePublishesFailedAndAcks(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-4"}
	consumer := NewConsumer(client, cfg)
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)

	events, _ := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if len(events) != 1 || events[0].Values["event"] != "node_failed" {
		t.Fatalf("events = %#v", events)
	}
	stream := redisstream.TaskStream(cfg.WorkerType)
	pending, _ := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Fatalf("unhandled type must still ack, pending = %d", pending.Count)
	}
}
