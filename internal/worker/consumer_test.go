package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/redisstream"
	"github.com/Ctwqk/videoprocess/internal/store"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
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

type registeredTaskStoreStub struct {
	lease          RegistrationLease
	claim          store.WorkerNodeClaim
	claimErr       error
	claimCalls     int
	prepareCalls   int
	publishCalls   int
	ackCalls       int
	claimedProof   store.WorkerTaskDeliveryProof
	preparedValues map[string]string
}

type registeredRuntimeStore struct {
	*fakeTaskStore
	requiredClaimCalls int
	persistCalls       int
	persistSaveCalls   int
}

func (s *registeredRuntimeStore) RequireWorkerNodeClaim(
	context.Context,
	store.WorkerNodeClaim,
) error {
	s.requiredClaimCalls++
	return nil
}

func (s *registeredRuntimeStore) PersistWorkerArtifact(
	ctx context.Context,
	_ store.WorkerNodeClaim,
	input store.CreateArtifactInput,
	save store.WorkerArtifactSaver,
) (string, error) {
	s.persistCalls++
	if err := save(ctx); err != nil {
		return "", err
	}
	s.persistSaveCalls++
	s.createdInput = input
	return "00000000-0000-0000-0000-000000000777", nil
}

type retryingStorage struct {
	saveAttempts atomic.Int32
	failures     int32
	block        bool
	savedPath    string
}

func (s *retryingStorage) Read(context.Context, string) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (s *retryingStorage) Save(
	ctx context.Context,
	path string,
	_ []byte,
) error {
	attempt := s.saveAttempts.Add(1)
	s.savedPath = path
	if s.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if attempt <= s.failures {
		return errors.New("temporary object-store failure")
	}
	return nil
}

func (s *retryingStorage) Exists(context.Context, string) (bool, error) {
	return false, nil
}

func (s *retryingStorage) Delete(context.Context, string) error {
	return nil
}

func (s *retryingStorage) LocalPath(string) (string, bool) {
	return "", false
}

func (s *registeredTaskStoreStub) ClaimWorkerNode(
	_ context.Context,
	lease store.WorkerRegistrationLease,
	jobID uuid.UUID,
	nodeExecutionID uuid.UUID,
	proof store.WorkerTaskDeliveryProof,
) (store.WorkerNodeClaim, error) {
	s.claimCalls++
	s.claimedProof = proof
	if s.claimErr != nil {
		return store.WorkerNodeClaim{}, s.claimErr
	}
	s.claim = store.WorkerNodeClaim{
		RegistrationID:  lease.RegistrationID,
		LeaseEpoch:      lease.LeaseEpoch,
		WorkerID:        lease.RedisConsumerID,
		WorkerStartedAt: time.Now().UTC(),
		JobID:           jobID,
		NodeExecutionID: nodeExecutionID,
		AttestationID:   uuid.New(),
		Delivery:        proof,
	}
	return s.claim, nil
}

func (s *registeredTaskStoreStub) PrepareWorkerEvent(
	_ context.Context,
	claim store.WorkerNodeClaim,
	_ string,
	_ string,
	values map[string]string,
) (uuid.UUID, error) {
	s.prepareCalls++
	s.preparedValues = values
	s.claim = claim
	return uuid.New(), nil
}

func (s *registeredTaskStoreStub) PublishPreparedWorkerEvent(
	ctx context.Context,
	_ store.WorkerRegistrationLease,
	_ uuid.UUID,
	publish store.WorkerEventPublisher,
) (store.WorkerNodeClaim, error) {
	s.publishCalls++
	if _, err := publish(
		ctx,
		"vp:events",
		"vp:worker-event-emission:"+uuid.NewString(),
		s.preparedValues,
	); err != nil {
		return store.WorkerNodeClaim{}, err
	}
	return s.claim, nil
}

func (s *registeredTaskStoreStub) ListPreparedWorkerEventIDs(
	context.Context,
	store.WorkerRegistrationLease,
	int,
) ([]uuid.UUID, error) {
	return nil, nil
}

func (s *registeredTaskStoreStub) AcknowledgeWorkerTask(
	ctx context.Context,
	claim store.WorkerNodeClaim,
	acknowledge store.WorkerTaskAcknowledger,
) error {
	s.ackCalls++
	_, err := acknowledge(
		ctx,
		claim.Delivery.RedisStream,
		claim.Delivery.ConsumerGroup,
		claim.Delivery.MessageID,
	)
	return err
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

func TestConsumerRegistrationLossReturnsFenceError(t *testing.T) {
	client, _ := newRedis(t)
	cfg := Config{
		WorkerType:  "ffmpeg_go",
		WorkerID:    "ffmpeg_go-worker@host127:1:00000000-0000-0000-0000-000000000001",
		Concurrency: 1,
	}
	consumer := NewConsumer(client, cfg, &fakeHandler{node: "trim"})
	consumer.BlockTimeout = 10 * time.Millisecond

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(ErrRegistrationLost)
	err := consumer.Run(ctx)
	if !errors.Is(err, ErrRegistrationLost) {
		t.Fatalf("Run error = %v; want ErrRegistrationLost", err)
	}
}

func TestRegistrationConsumerClaimsExactDeliveryCompletesAndAcks(t *testing.T) {
	client, _ := newRedis(t)
	instanceID := uuid.New()
	cfg := Config{
		WorkerType:  "ffmpeg_go",
		WorkerID:    "ffmpeg_go-worker@host127:1:" + instanceID.String(),
		WorkerHost:  "host127",
		RedisStream: "vp:test:registered:tasks",
		RedisGroup:  "vp-test-registered-workers",
	}
	lease := RegistrationLease{
		RegistrationID:   uuid.New(),
		WorkerInstanceID: instanceID,
		RedisConsumerID:  cfg.WorkerID,
		LeaseEpoch:       11,
		LeaseExpiresAt:   time.Now().UTC().Add(RegistrationLeaseDuration),
	}
	registration := &Registration{lease: lease}
	authority := &registeredTaskStoreStub{lease: lease}
	handler := &fakeHandler{node: "trim"}
	consumer := NewRegisteredConsumer(
		client,
		cfg,
		authority,
		registration,
		handler,
	)
	consumer.BlockTimeout = 50 * time.Millisecond
	withGroup(t, consumer)

	jobID := uuid.New()
	nodeExecutionID := uuid.New()
	dispatchKey := uuid.New()
	payload := map[string]any{
		"job_id":             jobID.String(),
		"node_execution_id":  nodeExecutionID.String(),
		"node_id":            "trim-1",
		"node_type":          "trim",
		"config":             `{"duration":"1"}`,
		"input_artifacts":    "{}",
		"preferred_hosts":    "[]",
		"event_stream":       "vp:events",
		"orchestrator_owner": "python",
		"dispatch_key":       dispatchKey.String(),
	}
	messageID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.RedisStream,
		Values: payload,
	}).Result()
	if err != nil {
		t.Fatalf("add registered task: %v", err)
	}
	runOneTick(t, consumer)

	stringPayload := make(map[string]string, len(payload))
	for key, value := range payload {
		stringPayload[key] = value.(string)
	}
	payloadHash, err := store.CanonicalRedisPayloadSHA256(stringPayload)
	if err != nil {
		t.Fatalf("hash registered payload: %v", err)
	}
	wantProof := store.WorkerTaskDeliveryProof{
		RedisStream:   cfg.RedisStream,
		ConsumerGroup: cfg.RedisGroup,
		MessageID:     messageID,
		PayloadSHA256: payloadHash,
		DispatchKey:   dispatchKey,
	}
	if authority.claimCalls != 1 ||
		!reflect.DeepEqual(authority.claimedProof, wantProof) {
		t.Fatalf("claim calls/proof = %d/%#v; want 1/%#v",
			authority.claimCalls,
			authority.claimedProof,
			wantProof,
		)
	}
	if len(handler.seen) != 1 ||
		authority.prepareCalls != 1 ||
		authority.publishCalls != 1 ||
		authority.ackCalls != 1 {
		t.Fatalf(
			"handler/prepare/publish/ack = %d/%d/%d/%d; want 1/1/1/1",
			len(handler.seen),
			authority.prepareCalls,
			authority.publishCalls,
			authority.ackCalls,
		)
	}
	for key, want := range map[string]string{
		"task_stream":         wantProof.RedisStream,
		"task_group":          wantProof.ConsumerGroup,
		"task_message_id":     wantProof.MessageID,
		"task_payload_sha256": wantProof.PayloadSHA256,
		"task_dispatch_key":   wantProof.DispatchKey.String(),
	} {
		if got := authority.preparedValues[key]; got != want {
			t.Errorf("completed event %s = %q; want %q", key, got, want)
		}
	}
	pending, err := client.XPending(
		context.Background(),
		cfg.RedisStream,
		cfg.RedisGroup,
	).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending after registered completion = %#v, err=%v", pending, err)
	}
}

func TestRegistrationConsumerFailureCopiesExactProofAndAcks(t *testing.T) {
	client, _ := newRedis(t)
	instanceID := uuid.New()
	cfg := Config{
		WorkerType:  "ffmpeg_go",
		WorkerID:    "ffmpeg_go-worker@host127:1:" + instanceID.String(),
		WorkerHost:  "host127",
		RedisStream: "vp:test:registered:failure:tasks",
		RedisGroup:  "vp-test-registered-failure-workers",
	}
	lease := RegistrationLease{
		RegistrationID:   uuid.New(),
		WorkerInstanceID: instanceID,
		RedisConsumerID:  cfg.WorkerID,
		LeaseEpoch:       12,
		LeaseExpiresAt:   time.Now().UTC().Add(RegistrationLeaseDuration),
	}
	authority := &registeredTaskStoreStub{lease: lease}
	handler := &fakeHandler{
		node: "trim",
		err:  errors.New("ffmpeg integration failure"),
	}
	consumer := NewRegisteredConsumer(
		client,
		cfg,
		authority,
		&Registration{lease: lease},
		handler,
	)
	consumer.BlockTimeout = 50 * time.Millisecond
	withGroup(t, consumer)

	dispatchKey := uuid.New()
	payload := map[string]any{
		"job_id":             uuid.NewString(),
		"node_execution_id":  uuid.NewString(),
		"node_id":            "trim-1",
		"node_type":          "trim",
		"config":             "{}",
		"input_artifacts":    "{}",
		"preferred_hosts":    "[]",
		"event_stream":       "vp:events",
		"orchestrator_owner": "python",
		"dispatch_key":       dispatchKey.String(),
	}
	messageID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.RedisStream,
		Values: payload,
	}).Result()
	if err != nil {
		t.Fatalf("add registered failure task: %v", err)
	}
	runOneTick(t, consumer)

	if authority.claimCalls != 1 ||
		authority.prepareCalls != 1 ||
		authority.publishCalls != 1 ||
		authority.ackCalls != 1 {
		t.Fatalf(
			"claim/prepare/publish/ack = %d/%d/%d/%d; want 1/1/1/1",
			authority.claimCalls,
			authority.prepareCalls,
			authority.publishCalls,
			authority.ackCalls,
		)
	}
	if authority.preparedValues["event"] != "node_failed" ||
		authority.preparedValues["error"] != handler.err.Error() {
		t.Fatalf("failure event = %#v", authority.preparedValues)
	}
	for key, want := range map[string]string{
		"task_stream":         authority.claimedProof.RedisStream,
		"task_group":          authority.claimedProof.ConsumerGroup,
		"task_message_id":     messageID,
		"task_payload_sha256": authority.claimedProof.PayloadSHA256,
		"task_dispatch_key":   dispatchKey.String(),
	} {
		if got := authority.preparedValues[key]; got != want {
			t.Errorf("failed event %s = %q; want %q", key, got, want)
		}
	}
	pending, err := client.XPending(
		context.Background(),
		cfg.RedisStream,
		cfg.RedisGroup,
	).Result()
	if err != nil || pending.Count != 0 {
		t.Fatalf("pending after registered failure = %#v, err=%v", pending, err)
	}
}

func TestRegistrationMediaTaskPersistsRemoteObjectAndPointerUnderClaim(
	t *testing.T,
) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(inputPath, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	jobID := uuid.MustParse("00000000-0000-0000-0000-000000000101")
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000201")
	claim := store.WorkerNodeClaim{
		RegistrationID:  uuid.MustParse("00000000-0000-0000-0000-000000000301"),
		LeaseEpoch:      9,
		WorkerID:        "ffmpeg_go-worker@host127:1:00000000-0000-0000-0000-000000000401",
		WorkerStartedAt: time.Date(2026, 7, 28, 20, 0, 0, 123000000, time.UTC),
		JobID:           jobID,
		NodeExecutionID: nodeID,
		AttestationID:   uuid.MustParse("00000000-0000-0000-0000-000000000501"),
		Delivery: store.WorkerTaskDeliveryProof{
			RedisStream:   "vp:tasks:ffmpeg_go",
			ConsumerGroup: "ffmpeg_go-workers",
			MessageID:     "1-0",
			PayloadSHA256: strings.Repeat("a", 64),
			DispatchKey:   uuid.MustParse("00000000-0000-0000-0000-000000000601"),
		},
	}
	storeFake := &registeredRuntimeStore{
		fakeTaskStore: &fakeTaskStore{
			artifacts: map[string]store.ArtifactRow{
				"input-artifact": {
					ID:             "input-artifact",
					Filename:       "input.mp4",
					StorageBackend: "local",
					StoragePath:    inputPath,
				},
			},
		},
	}
	remote := &retryingStorage{failures: 2}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:                   storeFake,
		Storage:                 remote,
		StorageBackend:          "minio",
		LocalRoot:               root,
		WorkerID:                claim.WorkerID,
		StorageOperationTimeout: 100 * time.Millisecond,
		StorageSaveAttempts:     3,
	}, &fakeMediaHandler{})

	result, err := handler.Execute(context.Background(), TaskMessage{
		JobID:           jobID.String(),
		NodeExecutionID: nodeID.String(),
		NodeType:        "trim",
		Config:          map[string]any{"output_format": "mp4"},
		InputArtifacts:  map[string]any{"input": "input-artifact"},
		WorkerClaim:     &claim,
	})
	if err != nil {
		t.Fatalf("registered Execute: %v", err)
	}
	if result.OutputArtifactID == "" {
		t.Fatal("registered artifact id is empty")
	}
	if storeFake.runningNode != "" {
		t.Fatalf("registered handler directly marked node running: %q", storeFake.runningNode)
	}
	if storeFake.requiredClaimCalls == 0 ||
		storeFake.persistCalls != 1 ||
		storeFake.persistSaveCalls != 1 {
		t.Fatalf(
			"require/persist/save = %d/%d/%d; want >=1/1/1",
			storeFake.requiredClaimCalls,
			storeFake.persistCalls,
			storeFake.persistSaveCalls,
		)
	}
	if attempts := remote.saveAttempts.Load(); attempts != 3 {
		t.Fatalf("remote Save attempts = %d; want 3", attempts)
	}
	const wantGeneration = "55d25999c6240b9c"
	if got := workerClaimGeneration(claim); got != wantGeneration {
		t.Fatalf(
			"claim generation = %q; want Python %q",
			got,
			wantGeneration,
		)
	}
	wantPrefix := "staging/artifacts/" + jobID.String() + "/" +
		nodeID.String() + "-" + wantGeneration
	if !strings.HasPrefix(remote.savedPath, wantPrefix) ||
		storeFake.createdInput.StoragePath != remote.savedPath {
		t.Fatalf(
			"remote/pointer paths = %q/%q; want prefix %q",
			remote.savedPath,
			storeFake.createdInput.StoragePath,
			wantPrefix,
		)
	}
}

func TestRegistrationMediaTaskRemoteSaveHasBoundedDeadline(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(inputPath, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	jobID := uuid.New()
	nodeID := uuid.New()
	claim := store.WorkerNodeClaim{
		RegistrationID:  uuid.New(),
		LeaseEpoch:      3,
		WorkerID:        "ffmpeg_go-worker@host127:1:" + uuid.NewString(),
		WorkerStartedAt: time.Now().UTC(),
		JobID:           jobID,
		NodeExecutionID: nodeID,
		AttestationID:   uuid.New(),
		Delivery: store.WorkerTaskDeliveryProof{
			RedisStream:   "vp:tasks:ffmpeg_go",
			ConsumerGroup: "ffmpeg_go-workers",
			MessageID:     "2-0",
			PayloadSHA256: strings.Repeat("b", 64),
			DispatchKey:   uuid.New(),
		},
	}
	storeFake := &registeredRuntimeStore{
		fakeTaskStore: &fakeTaskStore{
			artifacts: map[string]store.ArtifactRow{
				"input-artifact": {
					ID:             "input-artifact",
					Filename:       "input.mp4",
					StorageBackend: "local",
					StoragePath:    inputPath,
				},
			},
		},
	}
	remote := &retryingStorage{block: true}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:                   storeFake,
		Storage:                 remote,
		StorageBackend:          "minio",
		LocalRoot:               root,
		WorkerID:                claim.WorkerID,
		StorageOperationTimeout: 20 * time.Millisecond,
		StorageSaveAttempts:     2,
	}, &fakeMediaHandler{})

	started := time.Now()
	_, err := handler.Execute(context.Background(), TaskMessage{
		JobID:           jobID.String(),
		NodeExecutionID: nodeID.String(),
		NodeType:        "trim",
		Config:          map[string]any{"output_format": "mp4"},
		InputArtifacts:  map[string]any{"input": "input-artifact"},
		WorkerClaim:     &claim,
	})
	if err == nil {
		t.Fatal("blocking remote Save succeeded")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("blocking remote Save returned after %s; want bounded shutdown", elapsed)
	}
	if attempts := remote.saveAttempts.Load(); attempts != 2 {
		t.Fatalf("blocking remote Save attempts = %d; want 2", attempts)
	}
	if storeFake.persistSaveCalls != 0 {
		t.Fatalf(
			"artifact pointer persisted after timed out Save: %d",
			storeFake.persistSaveCalls,
		)
	}
}

func TestRegistrationConsumerMissingDispatchLeavesOriginalPending(t *testing.T) {
	client, _ := newRedis(t)
	instanceID := uuid.New()
	cfg := Config{
		WorkerType:  "ffmpeg_go",
		WorkerID:    "ffmpeg_go-worker@host127:1:" + instanceID.String(),
		WorkerHost:  "host127",
		RedisStream: "vp:test:registered:missing-dispatch:tasks",
		RedisGroup:  "vp-test-registered-missing-dispatch-workers",
	}
	lease := RegistrationLease{
		RegistrationID:   uuid.New(),
		WorkerInstanceID: instanceID,
		RedisConsumerID:  cfg.WorkerID,
		LeaseEpoch:       13,
		LeaseExpiresAt:   time.Now().UTC().Add(RegistrationLeaseDuration),
	}
	authority := &registeredTaskStoreStub{lease: lease}
	handler := &fakeHandler{node: "trim"}
	consumer := NewRegisteredConsumer(
		client,
		cfg,
		authority,
		&Registration{lease: lease},
		handler,
	)
	consumer.BlockTimeout = 50 * time.Millisecond
	withGroup(t, consumer)
	messageID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.RedisStream,
		Values: map[string]any{
			"job_id":            uuid.NewString(),
			"node_execution_id": uuid.NewString(),
			"node_id":           "trim-1",
			"node_type":         "trim",
			"config":            "{}",
			"input_artifacts":   "{}",
			"preferred_hosts":   "[]",
		},
	}).Result()
	if err != nil {
		t.Fatalf("add missing-dispatch task: %v", err)
	}
	runOneTick(t, consumer)

	if authority.claimCalls != 0 ||
		len(handler.seen) != 0 ||
		authority.prepareCalls != 0 ||
		authority.publishCalls != 0 ||
		authority.ackCalls != 0 {
		t.Fatalf(
			"claim/handler/prepare/publish/ack = %d/%d/%d/%d/%d; want all zero",
			authority.claimCalls,
			len(handler.seen),
			authority.prepareCalls,
			authority.publishCalls,
			authority.ackCalls,
		)
	}
	pending, err := client.XPendingExt(
		context.Background(),
		&redis.XPendingExtArgs{
			Stream: cfg.RedisStream,
			Group:  cfg.RedisGroup,
			Start:  messageID,
			End:    messageID,
			Count:  1,
		},
	).Result()
	if err != nil || len(pending) != 1 || pending[0].ID != messageID {
		t.Fatalf("missing-dispatch pending = %#v, err=%v", pending, err)
	}
}

func TestRegistrationConsumerLostEpochLeavesPendingWithoutFinalWrite(t *testing.T) {
	client, _ := newRedis(t)
	instanceID := uuid.New()
	cfg := Config{
		WorkerType:  "ffmpeg_go",
		WorkerID:    "ffmpeg_go-worker@host127:1:" + instanceID.String(),
		WorkerHost:  "host127",
		RedisStream: "vp:test:lost-registration:tasks",
		RedisGroup:  "vp-test-lost-registration-workers",
	}
	lease := RegistrationLease{
		RegistrationID:   uuid.New(),
		WorkerInstanceID: instanceID,
		RedisConsumerID:  cfg.WorkerID,
		LeaseEpoch:       12,
		LeaseExpiresAt:   time.Now().UTC().Add(RegistrationLeaseDuration),
	}
	authority := &registeredTaskStoreStub{
		lease:    lease,
		claimErr: store.ErrWorkerRegistrationLost,
	}
	handler := &fakeHandler{node: "trim"}
	consumer := NewRegisteredConsumer(
		client,
		cfg,
		authority,
		&Registration{lease: lease},
		handler,
	)
	consumer.BlockTimeout = 50 * time.Millisecond
	withGroup(t, consumer)
	messageID, err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: cfg.RedisStream,
		Values: map[string]any{
			"job_id":            uuid.NewString(),
			"node_execution_id": uuid.NewString(),
			"node_id":           "trim-1",
			"node_type":         "trim",
			"config":            "{}",
			"input_artifacts":   "{}",
			"preferred_hosts":   "[]",
			"dispatch_key":      uuid.NewString(),
		},
	}).Result()
	if err != nil {
		t.Fatalf("add lost-epoch task: %v", err)
	}
	runOneTick(t, consumer)
	if authority.claimCalls != 1 {
		t.Fatalf("claim calls = %d; want 1", authority.claimCalls)
	}
	if len(handler.seen) != 0 ||
		authority.prepareCalls != 0 ||
		authority.publishCalls != 0 ||
		authority.ackCalls != 0 {
		t.Fatalf(
			"handler/prepare/publish/ack = %d/%d/%d/%d; want all zero",
			len(handler.seen),
			authority.prepareCalls,
			authority.publishCalls,
			authority.ackCalls,
		)
	}
	pending, err := client.XPendingExt(
		context.Background(),
		&redis.XPendingExtArgs{
			Stream: cfg.RedisStream,
			Group:  cfg.RedisGroup,
			Start:  messageID,
			End:    messageID,
			Count:  1,
		},
	).Result()
	if err != nil || len(pending) != 1 {
		t.Fatalf("lost-epoch pending = %#v, err=%v; want exact entry", pending, err)
	}
}

func TestRegistrationAffinityDefersWithoutReplacementAndPreferredClaimsExactPELMessage(
	t *testing.T,
) {
	client := newRealWorkerRedisClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	stream := "vp:test:affinity:" + suffix
	group := "vp-test-affinity-" + suffix
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, stream).Err()
	})
	if err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		t.Fatalf("create affinity group: %v", err)
	}

	instanceID := uuid.New()
	payload := map[string]any{
		"job_id":               uuid.NewString(),
		"node_execution_id":    uuid.NewString(),
		"node_id":              "trim-1",
		"node_type":            "trim",
		"config":               "{}",
		"input_artifacts":      "{}",
		"preferred_hosts":      `["host127"]`,
		"affinity_enqueued_at": time.Now().UTC().Format(time.RFC3339Nano),
		"affinity_bounces":     "0",
		"dispatch_key":         uuid.NewString(),
	}
	messageID, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: payload,
	}).Result()
	if err != nil {
		t.Fatalf("add affinity task: %v", err)
	}
	delivered, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "ffmpeg_go-worker@host150:1:" + instanceID.String(),
		Streams:  []string{stream, ">"},
		Count:    1,
	}).Result()
	if err != nil || len(delivered) != 1 || len(delivered[0].Messages) != 1 {
		t.Fatalf("deliver affinity task: streams=%#v err=%v", delivered, err)
	}
	original := delivered[0].Messages[0]
	task, err := decodeTask(original.Values)
	if err != nil {
		t.Fatalf("decode affinity task: %v", err)
	}
	wrongLease := RegistrationLease{
		RegistrationID:   uuid.New(),
		WorkerInstanceID: instanceID,
		RedisConsumerID:  "ffmpeg_go-worker@host150:1:" + instanceID.String(),
		LeaseEpoch:       1,
		LeaseExpiresAt:   time.Now().UTC().Add(RegistrationLeaseDuration),
	}
	wrongAuthority := &registeredTaskStoreStub{lease: wrongLease}
	wrongHost := NewRegisteredConsumer(
		client,
		Config{
			WorkerType:   "ffmpeg_go",
			WorkerID:     wrongLease.RedisConsumerID,
			WorkerHost:   "host150",
			RedisStream:  stream,
			RedisGroup:   group,
			AffinityWait: 20 * time.Second,
		},
		wrongAuthority,
		&Registration{lease: wrongLease},
	)
	wrongHost.handleRegisteredMessage(ctx, original)
	if wrongAuthority.claimCalls != 0 {
		t.Fatalf(
			"registered affinity claim calls = %d; want 0",
			wrongAuthority.claimCalls,
		)
	}
	if length, err := client.XLen(ctx, stream).Result(); err != nil || length != 1 {
		t.Fatalf("stream length after defer = %d, err=%v; want 1", length, err)
	}
	pending, err := client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending after defer = %#v, err=%v; want exact entry", pending, err)
	}
	if pending[0].Consumer != wrongHost.WorkerID {
		t.Fatalf("pending owner = %q; want %q", pending[0].Consumer, wrongHost.WorkerID)
	}
	exact, err := client.XRangeN(ctx, stream, messageID, messageID, 1).Result()
	if err != nil || len(exact) != 1 || !reflect.DeepEqual(exact[0].Values, original.Values) {
		t.Fatalf("exact message changed after defer: message=%#v err=%v", exact, err)
	}

	time.Sleep(600 * time.Millisecond)
	preferredInstance := uuid.New()
	preferred := NewRegisteredConsumer(
		client,
		Config{
			WorkerType:   "ffmpeg_go",
			WorkerID:     "ffmpeg_go-worker@host127:1:" + preferredInstance.String(),
			WorkerHost:   "host127",
			RedisStream:  stream,
			RedisGroup:   group,
			AffinityWait: 20 * time.Second,
		},
		nil,
		&Registration{},
	)
	if preferred.shouldDeferRegisteredForAffinity(
		task,
		time.Now().UTC(),
	) {
		t.Fatal("preferred registered consumer misidentified its process-UUID consumer ID")
	}
	claimed, err := preferred.claimPreferredPending(ctx)
	if err != nil {
		t.Fatalf("claim preferred pending: %v", err)
	}
	if len(claimed) != 1 ||
		claimed[0].ID != messageID ||
		!reflect.DeepEqual(claimed[0].Values, original.Values) {
		t.Fatalf("claimed messages = %#v; want unchanged %s", claimed, messageID)
	}
	pending, err = client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  messageID,
		End:    messageID,
		Count:  1,
	}).Result()
	if err != nil || len(pending) != 1 ||
		pending[0].Consumer != preferred.WorkerID {
		t.Fatalf("preferred pending owner = %#v, err=%v", pending, err)
	}

	expiredPayload := make(map[string]any, len(payload))
	for key, value := range payload {
		expiredPayload[key] = value
	}
	expiredPayload["job_id"] = uuid.NewString()
	expiredPayload["node_execution_id"] = uuid.NewString()
	expiredPayload["dispatch_key"] = uuid.NewString()
	expiredPayload["affinity_enqueued_at"] = time.Now().UTC().
		Add(-21 * time.Second).
		Format(time.RFC3339Nano)
	expiredMessageID, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: expiredPayload,
	}).Result()
	if err != nil {
		t.Fatalf("add expired-affinity task: %v", err)
	}
	expiredDelivery, err := client.XReadGroup(
		ctx,
		&redis.XReadGroupArgs{
			Group:    group,
			Consumer: wrongHost.WorkerID,
			Streams:  []string{stream, ">"},
			Count:    1,
		},
	).Result()
	if err != nil ||
		len(expiredDelivery) != 1 ||
		len(expiredDelivery[0].Messages) != 1 {
		t.Fatalf(
			"deliver expired-affinity task: streams=%#v err=%v",
			expiredDelivery,
			err,
		)
	}
	expiredOriginal := expiredDelivery[0].Messages[0]
	time.Sleep(600 * time.Millisecond)
	expired, err := wrongHost.claimPreferredPending(ctx)
	if err != nil {
		t.Fatalf("claim expired affinity pending: %v", err)
	}
	if len(expired) != 1 ||
		expired[0].ID != expiredMessageID ||
		!reflect.DeepEqual(expired[0].Values, expiredOriginal.Values) {
		t.Fatalf(
			"expired affinity messages = %#v; want unchanged %s",
			expired,
			expiredMessageID,
		)
	}
	expiredPending, err := client.XPendingExt(
		ctx,
		&redis.XPendingExtArgs{
			Stream: stream,
			Group:  group,
			Start:  expiredMessageID,
			End:    expiredMessageID,
			Count:  1,
		},
	).Result()
	if err != nil ||
		len(expiredPending) != 1 ||
		expiredPending[0].Consumer != wrongHost.WorkerID {
		t.Fatalf(
			"expired affinity pending owner = %#v, err=%v",
			expiredPending,
			err,
		)
	}
}

func newRealWorkerRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv("CHANNEL_OPS_GO_REDIS_TEST_URL"))
	if rawURL == "" {
		t.Skip("set CHANNEL_OPS_GO_REDIS_TEST_URL for Redis 7.4 worker integration tests")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse Redis integration URL: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping Redis integration server: %v", err)
	}
	info, err := client.Info(ctx, "server").Result()
	if err != nil {
		t.Fatalf("read Redis server version: %v", err)
	}
	version := ""
	for _, line := range strings.Split(info, "\n") {
		if value, ok := strings.CutPrefix(
			strings.TrimSpace(line),
			"redis_version:",
		); ok {
			version = value
			break
		}
	}
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		t.Fatalf("invalid Redis server version %q", version)
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil ||
		major < 7 || major == 7 && minor < 4 {
		t.Fatalf("Redis version = %q; require 7.4 or newer", version)
	}
	return client
}
