package orchestrator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestEventListenerHandlesCompletionAndAcks(t *testing.T) {
	ctx := context.Background()
	client, _ := newEventRedis(t)
	engine := newFakeEventEngine()
	listener := testEventListener(client, engine)
	listener.BlockTimeout = 25 * time.Millisecond

	if err := listener.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "vp:events:go",
		Values: map[string]any{
			"event":              "node_completed",
			"job_id":             "job-1",
			"node_execution_id":  "node-exec-1",
			"output_artifact_id": "artifact-1",
		},
	}).Result(); err != nil {
		t.Fatalf("xadd: %v", err)
	}

	runEventListenerUntil(t, listener, engine.waitForCalls(1))

	if got := engine.completed; len(got) != 1 {
		t.Fatalf("completed calls = %d; want 1", len(got))
	}
	if got := engine.completed[0]; got != (eventCall{jobID: "job-1", nodeExecutionID: "node-exec-1", outputArtifactID: "artifact-1"}) {
		t.Fatalf("completed call = %#v", got)
	}
	assertNoEventPending(t, client)
}

func TestEventListenerDoesNotAckWhenEngineFails(t *testing.T) {
	ctx := context.Background()
	client, _ := newEventRedis(t)
	engine := newFakeEventEngine()
	engine.err = errors.New("store unavailable")
	listener := testEventListener(client, engine)
	listener.BlockTimeout = 25 * time.Millisecond

	if err := listener.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "vp:events:go",
		Values: map[string]any{
			"event":              "node_completed",
			"job_id":             "job-1",
			"node_execution_id":  "node-exec-1",
			"output_artifact_id": "artifact-1",
		},
	}).Result(); err != nil {
		t.Fatalf("xadd: %v", err)
	}

	runEventListenerUntil(t, listener, engine.waitForCalls(1))

	pending, err := client.XPending(ctx, "vp:events:go", "orchestrator-go").Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 1 {
		t.Fatalf("pending count = %d; want 1", pending.Count)
	}
}

func TestEventListenerAcksPythonOwnedEventAfterGuard(t *testing.T) {
	ctx := context.Background()
	client, _ := newEventRedis(t)
	engine := newFakeEventEngine()
	engine.err = ErrNonGoEvent
	listener := testEventListener(client, engine)
	listener.BlockTimeout = 25 * time.Millisecond

	if err := listener.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	msgID, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "vp:events:go",
		Values: map[string]any{
			"event":              "node_completed",
			"job_id":             "python-owned-job",
			"node_execution_id":  "node-exec-1",
			"output_artifact_id": "artifact-1",
			"orchestrator_owner": "python",
		},
	}).Result()
	if err != nil {
		t.Fatalf("xadd: %v", err)
	}

	runEventListenerUntil(t, listener, func() bool {
		if engine.callCount() != 1 {
			return false
		}
		pending, err := client.XPending(ctx, "vp:events:go", "orchestrator-go").Result()
		if err != nil || pending.Count != 0 {
			return false
		}
		groups, err := client.XInfoGroups(ctx, "vp:events:go").Result()
		if err != nil || len(groups) != 1 {
			return false
		}
		return groups[0].LastDeliveredID == msgID
	})

	if got := engine.callCount(); got != 1 {
		t.Fatalf("engine call count = %d; want 1", got)
	}
	assertNoEventPending(t, client)
}

func TestEventListenerReclaimsPendingEvents(t *testing.T) {
	ctx := context.Background()
	client, _ := newEventRedis(t)
	engine := newFakeEventEngine()
	listener := testEventListener(client, engine)
	listener.ReclaimMinIdle = time.Millisecond

	if err := listener.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "vp:events:go",
		Values: map[string]any{
			"event":             "node_failed",
			"job_id":            "job-1",
			"node_execution_id": "node-exec-1",
			"error":             "ffmpeg failed",
		},
	}).Result(); err != nil {
		t.Fatalf("xadd: %v", err)
	}
	if _, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "orchestrator-go",
		Consumer: "stale-consumer",
		Streams:  []string{"vp:events:go", ">"},
		Count:    1,
	}).Result(); err != nil {
		t.Fatalf("xreadgroup: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	if err := listener.reclaim(ctx); err != nil {
		t.Fatalf("reclaim: %v", err)
	}

	if got := engine.failed; len(got) != 1 {
		t.Fatalf("failed calls = %d; want 1", len(got))
	}
	if got := engine.failed[0]; got != (eventCall{jobID: "job-1", nodeExecutionID: "node-exec-1", errorMessage: "ffmpeg failed"}) {
		t.Fatalf("failed call = %#v", got)
	}
	assertNoEventPending(t, client)
}

func TestEventListenerAcksMalformedOrUnknownEvents(t *testing.T) {
	ctx := context.Background()
	client, _ := newEventRedis(t)
	engine := newFakeEventEngine()
	listener := testEventListener(client, engine)
	listener.BlockTimeout = 25 * time.Millisecond

	if err := listener.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	lastID := ""
	for _, values := range []map[string]any{
		{"event": "node_completed", "job_id": "job-1", "node_execution_id": "node-exec-1"},
		{"event": "node_unknown", "job_id": "job-1", "node_execution_id": "node-exec-1"},
	} {
		id, err := client.XAdd(ctx, &redis.XAddArgs{Stream: "vp:events:go", Values: values}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		lastID = id
	}

	runEventListenerUntil(t, listener, func() bool {
		pending, err := client.XPending(ctx, "vp:events:go", "orchestrator-go").Result()
		if err != nil || pending.Count != 0 || engine.callCount() != 0 {
			return false
		}
		groups, err := client.XInfoGroups(ctx, "vp:events:go").Result()
		if err != nil || len(groups) != 1 {
			return false
		}
		return groups[0].LastDeliveredID == lastID
	})

	if engine.callCount() != 0 {
		t.Fatalf("engine call count = %d; want 0", engine.callCount())
	}
	assertNoEventPending(t, client)
}

func newEventRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

func testEventListener(client *redis.Client, engine *fakeEventEngine) *EventListener {
	return &EventListener{
		Client:   client,
		Engine:   engine,
		Consumer: "test-consumer",
	}
}

func runEventListenerUntil(t *testing.T, listener *EventListener, done func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- listener.Run(ctx)
	}()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			cancel()
			t.Fatalf("listener did not reach expected state")
		case <-ticker.C:
			if done() {
				cancel()
				select {
				case <-errCh:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("listener did not stop after cancel")
				}
				return
			}
		}
	}
}

func assertNoEventPending(t *testing.T, client *redis.Client) {
	t.Helper()
	pending, err := client.XPending(context.Background(), "vp:events:go", "orchestrator-go").Result()
	if err != nil {
		t.Fatalf("xpending: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count = %d; want 0", pending.Count)
	}
}

type eventCall struct {
	jobID            string
	nodeExecutionID  string
	outputArtifactID string
	errorMessage     string
}

type fakeEventEngine struct {
	mu        sync.Mutex
	completed []eventCall
	failed    []eventCall
	err       error
}

func newFakeEventEngine() *fakeEventEngine {
	return &fakeEventEngine{}
}

func (e *fakeEventEngine) OnNodeCompleted(_ context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.completed = append(e.completed, eventCall{jobID: jobID, nodeExecutionID: nodeExecutionID, outputArtifactID: outputArtifactID})
	return e.err
}

func (e *fakeEventEngine) OnNodeFailed(_ context.Context, jobID string, nodeExecutionID string, errorMessage string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failed = append(e.failed, eventCall{jobID: jobID, nodeExecutionID: nodeExecutionID, errorMessage: errorMessage})
	return e.err
}

func (e *fakeEventEngine) waitForCalls(want int) func() bool {
	return func() bool {
		return e.callCount() >= want
	}
}

func (e *fakeEventEngine) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.completed) + len(e.failed)
}
