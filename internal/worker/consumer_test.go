package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type fakeHandler struct {
	node string
	err  error
	seen []TaskMessage
}

func (f *fakeHandler) NodeType() string { return f.node }
func (f *fakeHandler) Execute(ctx context.Context, task TaskMessage) (NodeResult, error) {
	f.seen = append(f.seen, task)
	if f.err != nil {
		return NodeResult{}, f.err
	}
	return NodeResult{OutputArtifactID: "artifact-1"}, nil
}

type emptyArtifactHandler struct{}

func (h emptyArtifactHandler) NodeType() string { return "trim" }
func (h emptyArtifactHandler) Execute(context.Context, TaskMessage) (NodeResult, error) {
	return NodeResult{}, nil
}

type publishFailHandler struct {
	mr *miniredis.Miniredis
}

func (h publishFailHandler) NodeType() string { return "trim" }
func (h publishFailHandler) Execute(context.Context, TaskMessage) (NodeResult, error) {
	h.mr.SetError("forced redis write failure")
	return NodeResult{OutputArtifactID: "artifact-1"}, nil
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

func TestConsumerSuccessPublishesToTaskEventStream(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-go-event-stream"}
	handler := &fakeHandler{node: "trim"}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 50 * time.Millisecond
	goEventStream := "vp:events:go"

	withGroup(t, consumer)
	stream := redisstream.TaskStream(cfg.WorkerType)
	configJSON, _ := json.Marshal(map[string]any{"start_time": "0", "duration": "1"})
	if _, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"job_id":             "job-1",
			"node_execution_id":  "ne-1",
			"node_id":            "trim_1",
			"node_type":          "trim",
			"config":             string(configJSON),
			"input_artifacts":    "{}",
			"preferred_hosts":    "[]",
			"event_stream":       goEventStream,
			"orchestrator_owner": "go",
		},
	}).Result(); err != nil {
		t.Fatalf("xadd: %v", err)
	}
	runOneTick(t, consumer)

	if len(handler.seen) != 1 {
		t.Fatalf("handler invocations = %d; want 1", len(handler.seen))
	}
	if handler.seen[0].EventStream != goEventStream {
		t.Fatalf("task event stream = %q", handler.seen[0].EventStream)
	}
	if handler.seen[0].OrchestratorOwner != "go" {
		t.Fatalf("task orchestrator owner = %q", handler.seen[0].OrchestratorOwner)
	}
	goEvents, err := client.XRange(context.Background(), goEventStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange go stream: %v", err)
	}
	if len(goEvents) != 1 {
		t.Fatalf("go stream events = %d; want 1", len(goEvents))
	}
	if goEvents[0].Values["event"] != "node_completed" {
		t.Fatalf("event = %q", goEvents[0].Values["event"])
	}
	defaultEvents, err := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange default stream: %v", err)
	}
	if len(defaultEvents) != 0 {
		t.Fatalf("default stream events = %d; want 0", len(defaultEvents))
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

func TestConsumerConfirmedCancellationAcksWithoutEvent(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-3"}
	handler := &fakeHandler{node: "trim", err: ErrConfirmedCancellation}
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
	if pending.Count != 0 {
		t.Fatalf("confirmed cancelled task should be acked, pending = %d", pending.Count)
	}

	events, _ := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if len(events) != 0 {
		t.Fatalf("confirmed cancellation must not publish events, got %#v", events)
	}
}

func TestConsumerRejectsSuccessWithoutOutputArtifactID(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-empty-artifact"}
	consumer := NewConsumer(client, cfg, emptyArtifactHandler{})
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)

	events, _ := client.XRange(context.Background(), redisstream.EventStream, "-", "+").Result()
	if len(events) != 1 || events[0].Values["event"] != "node_failed" {
		t.Fatalf("events = %#v", events)
	}
	if got, _ := events[0].Values["error"].(string); !strings.Contains(got, "output_artifact_id") {
		t.Fatalf("error = %q", got)
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

func TestConsumerLeavesValidTaskPendingWhenEventPublishFails(t *testing.T) {
	client, mr := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-event-failure"}
	handler := publishFailHandler{mr: mr}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 50 * time.Millisecond

	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)
	runOneTick(t, consumer)
	mr.SetError("")

	stream := redisstream.TaskStream(cfg.WorkerType)
	pending, err := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending after publish failure = %d; want 1", pending.Count)
	}
}

func TestReclaimPendingClaimsStaleMessages(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "claimer", PELMinIdle: time.Millisecond}
	consumer := NewConsumer(client, cfg, &fakeHandler{node: "trim"})
	withGroup(t, consumer)
	enqueueTrim(t, client, cfg.WorkerType)

	other := "other-worker"
	stream := redisstream.TaskStream(cfg.WorkerType)
	if _, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: consumer.ConsumerGroup, Consumer: other, Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	claimed, err := consumer.ReclaimPending(context.Background())
	if err != nil {
		t.Fatalf("ReclaimPending: %v", err)
	}
	if claimed == 0 {
		t.Fatal("expected at least one reclaimed message")
	}
}

func TestHeartbeatRefreshesPendingOwnership(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "heartbeat-worker", HeartbeatInterval: time.Millisecond}
	consumer := NewConsumer(client, cfg, &fakeHandler{node: "trim"})
	withGroup(t, consumer)
	msgID := enqueueTrim(t, client, cfg.WorkerType)
	stream := redisstream.TaskStream(cfg.WorkerType)
	if _, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: consumer.ConsumerGroup, Consumer: cfg.WorkerID, Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := consumer.StartHeartbeat(ctx, msgID)
	time.Sleep(5 * time.Millisecond)
	cancel()
	<-done

	pending, err := client.XPendingExt(context.Background(), &redis.XPendingExtArgs{
		Stream: stream, Group: consumer.ConsumerGroup, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatalf("xpendingext: %v", err)
	}
	if len(pending) != 1 || pending[0].Consumer != cfg.WorkerID {
		t.Fatalf("pending = %#v", pending)
	}
}

func TestAffinityDefersAndRequeuesForPreferredHost(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "ffmpeg_go-worker@wrong-host:1", AffinityWait: time.Minute, AffinityMaxBounces: 6}
	consumer := NewConsumer(client, cfg, &fakeHandler{node: "trim"})

	withGroup(t, consumer)
	stream := redisstream.TaskStream(cfg.WorkerType)
	configJSON, _ := json.Marshal(map[string]any{"duration": "1"})
	msgID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			"job_id":               "job-1",
			"node_execution_id":    "ne-1",
			"node_id":              "trim_1",
			"node_type":            "trim",
			"config":               string(configJSON),
			"input_artifacts":      "{}",
			"preferred_hosts":      `["right-host"]`,
			"affinity_enqueued_at": time.Now().UTC().Format(time.RFC3339Nano),
			"affinity_bounces":     "0",
		},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.XReadGroup(context.Background(), &redis.XReadGroupArgs{
		Group: consumer.ConsumerGroup, Consumer: consumer.WorkerID, Streams: []string{stream, ">"}, Count: 1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}

	task := TaskMessage{
		JobID:              "job-1",
		NodeExecutionID:    "ne-1",
		NodeID:             "trim_1",
		NodeType:           "trim",
		Config:             map[string]any{"duration": "1"},
		InputArtifacts:     map[string]any{},
		PreferredHosts:     []string{"right-host"},
		AffinityEnqueuedAt: time.Now().UTC().Format(time.RFC3339Nano),
		AffinityBounces:    "0",
	}
	if !consumer.shouldDeferForAffinity(task, time.Now().UTC()) {
		t.Fatal("expected non-preferred host to defer")
	}
	if err := consumer.deferForAffinity(context.Background(), redis.XMessage{ID: msgID}, task); err != nil {
		t.Fatalf("deferForAffinity: %v", err)
	}

	pending, _ := client.XPending(context.Background(), stream, consumer.ConsumerGroup).Result()
	if pending.Count != 0 {
		t.Fatalf("deferred message must be acked, pending = %d", pending.Count)
	}
	length, _ := client.XLen(context.Background(), stream).Result()
	if length < 2 {
		t.Fatalf("expected re-enqueued message, stream length = %d", length)
	}
}

func TestAffinityRelaxesAfterBounceBudget(t *testing.T) {
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "ffmpeg_go-worker@wrong-host:1", AffinityWait: time.Minute, AffinityMaxBounces: 1}
	consumer := NewConsumer(nil, cfg, &fakeHandler{node: "trim"})
	task := TaskMessage{
		NodeType:           "trim",
		PreferredHosts:     []string{"right-host"},
		AffinityEnqueuedAt: time.Now().UTC().Format(time.RFC3339Nano),
		AffinityBounces:    "1",
	}
	if consumer.shouldDeferForAffinity(task, time.Now().UTC()) {
		t.Fatal("expected worker to process locally after bounce budget is exhausted")
	}
}

type blockingHandler struct {
	node       string
	started    chan struct{}
	release    chan struct{}
	active     atomic.Int32
	maxActive  atomic.Int32
	invocation atomic.Int32
}

func (h *blockingHandler) NodeType() string { return h.node }

func (h *blockingHandler) Execute(ctx context.Context, task TaskMessage) (NodeResult, error) {
	current := h.active.Add(1)
	for {
		old := h.maxActive.Load()
		if current <= old || h.maxActive.CompareAndSwap(old, current) {
			break
		}
	}
	h.invocation.Add(1)
	h.started <- struct{}{}
	select {
	case <-h.release:
	case <-ctx.Done():
		h.active.Add(-1)
		return NodeResult{}, ctx.Err()
	}
	h.active.Add(-1)
	return NodeResult{OutputArtifactID: "artifact-1"}, nil
}

func TestConsumerHonorsConcurrencyLimit(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{WorkerType: "ffmpeg_go", WorkerID: "test-concurrency", Concurrency: 2, HeartbeatInterval: time.Hour}
	handler := &blockingHandler{node: "trim", started: make(chan struct{}, 4), release: make(chan struct{})}
	consumer := NewConsumer(client, cfg, handler)
	consumer.BlockTimeout = 10 * time.Millisecond

	withGroup(t, consumer)
	for i := 0; i < 4; i++ {
		enqueueTrim(t, client, cfg.WorkerType)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = consumer.Run(ctx)
		close(done)
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-handler.started:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for invocation %d", i+1)
		}
	}
	time.Sleep(30 * time.Millisecond)
	if got := handler.maxActive.Load(); got != 2 {
		t.Fatalf("maxActive = %d; want 2", got)
	}
	close(handler.release)
	<-done
}
