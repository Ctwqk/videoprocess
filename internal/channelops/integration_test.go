package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestCancellableTestOperationCancelsAndDrains(t *testing.T) {
	observedCancellation := make(chan struct{})
	operation := startCancellableTestOperation(t, nil, func(ctx context.Context) error {
		<-ctx.Done()
		close(observedCancellation)
		return ctx.Err()
	})

	started := time.Now()
	result, err := operation.cancelAndDrain(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("cancel and drain: %v", err)
	}
	if !errors.Is(result, context.Canceled) {
		t.Fatalf("operation result = %v, want context cancellation", result)
	}
	select {
	case <-observedCancellation:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked operation did not observe cancellation")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("cancel and drain took %s, want bounded completion", elapsed)
	}
}

func TestCancellableTestOperationTimeoutCancelsAndDrainsBeforeReturning(t *testing.T) {
	observedCancellation := make(chan struct{})
	operation := startCancellableTestOperation(t, nil, func(ctx context.Context) error {
		<-ctx.Done()
		close(observedCancellation)
		return ctx.Err()
	})

	result, err := operation.waitOrCancelAndDrain(20*time.Millisecond, 100*time.Millisecond)
	if err == nil {
		t.Fatal("timeout wait returned no diagnostic")
	}
	if !errors.Is(result, context.Canceled) {
		t.Fatalf("operation result = %v, want context cancellation", result)
	}
	select {
	case <-observedCancellation:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked operation did not observe cancellation before helper returned")
	}
	if !operation.drained() {
		t.Fatal("operation was not drained before timeout diagnostic")
	}
}

func TestCancellableTestOperationTimeoutMarksFixtureCleanupIneligibleWhenUndrained(t *testing.T) {
	release := make(chan struct{})
	operation := startCancellableTestOperation(t, nil, func(context.Context) error {
		<-release
		return nil
	})

	started := time.Now()
	_, err := operation.waitOrCancelAndDrain(30*time.Millisecond, 50*time.Millisecond)
	if err == nil {
		t.Fatal("undrained timeout returned no diagnostic")
	}
	if elapsed := time.Since(started); elapsed < 70*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Fatalf("timeout cleanup took %s, want two bounded waits", elapsed)
	}
	if fixtureCleanupEligible([]*cancellableTestOperation{operation}) {
		t.Fatal("fixture cleanup remained eligible after operation failed to drain")
	}

	close(release)
	if _, err := operation.wait(100 * time.Millisecond); err != nil {
		t.Fatalf("drain released synthetic operation: %v", err)
	}
	if fixtureCleanupEligible([]*cancellableTestOperation{operation}) {
		t.Fatal("fixture cleanup became eligible after an undrained operation was released")
	}
}

const testOperationCleanupTimeout = 5 * time.Second

type cancellableTestOperation struct {
	cancel  context.CancelFunc
	done    chan error
	release func()

	releaseOnce sync.Once
	mu          sync.Mutex
	finished    bool
	result      error
	drainTimed  bool
}

func startCancellableTestOperation(
	t *testing.T,
	release func(),
	run func(context.Context) error,
) *cancellableTestOperation {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	operation := &cancellableTestOperation{
		cancel:  cancel,
		done:    make(chan error, 1),
		release: release,
	}
	t.Cleanup(func() {
		if _, err := operation.cancelAndDrain(testOperationCleanupTimeout); err != nil {
			t.Errorf("cancel and drain test operation: %v", err)
		}
	})
	go func() { operation.done <- run(ctx) }()
	return operation
}

func (o *cancellableTestOperation) cancelAndDrain(timeout time.Duration) (error, error) {
	o.releaseBlocker()
	o.cancel()
	return o.drain(timeout)
}

func (o *cancellableTestOperation) waitOrCancelAndDrain(waitTimeout, drainTimeout time.Duration) (error, error) {
	result, err := o.wait(waitTimeout)
	if err == nil {
		return result, nil
	}

	drainedResult, drainErr := o.cancelAndDrain(drainTimeout)
	if drainErr != nil {
		return nil, fmt.Errorf("operation did not complete within %s and did not drain within %s", waitTimeout, drainTimeout)
	}
	return drainedResult, fmt.Errorf("operation did not complete within %s", waitTimeout)
}

func (o *cancellableTestOperation) drain(timeout time.Duration) (error, error) {
	result, err := o.wait(timeout)
	if err != nil {
		o.mu.Lock()
		o.drainTimed = true
		o.mu.Unlock()
	}
	return result, err
}

func (o *cancellableTestOperation) releaseBlocker() {
	o.releaseOnce.Do(func() {
		if o.release != nil {
			o.release()
		}
	})
}

func (o *cancellableTestOperation) tryWait() (bool, error) {
	o.mu.Lock()
	if o.finished {
		result := o.result
		o.mu.Unlock()
		return true, result
	}
	o.mu.Unlock()

	select {
	case result := <-o.done:
		o.recordResult(result)
		return true, result
	default:
		return false, nil
	}
}

func (o *cancellableTestOperation) wait(timeout time.Duration) (error, error) {
	if done, result := o.tryWait(); done {
		return result, nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-o.done:
		o.recordResult(result)
		return result, nil
	case <-timer.C:
		return nil, fmt.Errorf("operation did not drain within %s", timeout)
	}
}

func (o *cancellableTestOperation) recordResult(result error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.finished {
		o.finished = true
		o.result = result
	}
}

func (o *cancellableTestOperation) drained() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.finished && !o.drainTimed
}

func fixtureCleanupEligible(operations []*cancellableTestOperation) bool {
	for _, operation := range operations {
		if !operation.drained() {
			return false
		}
	}
	return true
}

func registerBoundedFixtureCleanup(t *testing.T, fixture *ChannelOpsFixture, operations *[]*cancellableTestOperation) {
	t.Helper()
	t.Cleanup(func() {
		if !fixtureCleanupEligible(*operations) {
			t.Errorf("skipping fixture cleanup after an operation failed to drain")
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.Background(), testOperationCleanupTimeout)
		defer cancel()
		fixture.cleanup(cleanupCtx)
		if cleanupCtx.Err() != nil {
			t.Errorf("fixture cleanup exceeded %s: %v", testOperationCleanupTimeout, cleanupCtx.Err())
			return
		}
		fixture.Store.Close()
	})
}

func registerBoundedRollback(t *testing.T, tx pgx.Tx) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), testOperationCleanupTimeout)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	})
}

func TestFakeLiveFlowReachesMeasured(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	bucket := UTCBucket(time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC))

	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, bucket, handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	fixture.ProcessAllQueueItems(ctx, handler)

	task := fixture.RequireSingleTask(ctx)
	if task.State != TaskMeasured {
		t.Fatalf("task state = %s", task.State)
	}
	if got := fixture.CountRows(ctx, "publication_records"); got != 1 {
		t.Fatalf("publication count = %d", got)
	}
	if got := fixture.CountRows(ctx, "feedback_snapshots"); got != 1 {
		t.Fatalf("feedback snapshot count = %d", got)
	}
	if got := fixture.CountRows(ctx, "material_usage_ledger"); got == 0 {
		t.Fatal("material ledger did not grow")
	}
	if got := fixture.CountRows(ctx, "takedown_events"); got != 0 {
		t.Fatalf("takedown event count = %d", got)
	}
}

func TestRunLiveSmokeFreshSmokeCompletesWithDelayedQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})

	result, err := fixture.Store.RunLiveSmoke(ctx, fixture.ChannelID, handler)
	if err != nil {
		t.Fatalf("RunLiveSmoke: %v", err)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("fresh live smoke did not validate: %v; result=%#v", err, result)
	}
}

func TestPromotePublicationUsesConfiguredMetricsDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	handler.Config.MetricsPollDelayMinutes = 7
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	item := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.HandlePromotePublication(ctx, item); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, item); err != nil {
		t.Fatalf("MarkQueueDone: %v", err)
	}

	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	scheduledRaw, _ := item.PayloadJSON["scheduled_at"].(string)
	scheduledAt, err := time.Parse(time.RFC3339, scheduledRaw)
	if err != nil {
		t.Fatalf("parse scheduled_at: %v", err)
	}
	var runAfter time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT run_after
		FROM channel_ops_queue_items
		WHERE kind = $1
		  AND idempotency_key = $2
	`, QueueCollectMetrics, fmt.Sprintf("collect_metrics:%s:poll:0", publicationID)).Scan(&runAfter); err != nil {
		t.Fatalf("select collect_metrics run_after: %v", err)
	}
	if want := scheduledAt.UTC().Add(7 * time.Minute); !runAfter.Equal(want) {
		t.Fatalf("collect_metrics run_after = %s, want %s", runAfter, want)
	}
}

func TestPromotionHandleAndQuarantineSerializeOnChannelFence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	releaseYouTube := make(chan struct{})
	youtube := &blockingPromotionYouTube{
		started: make(chan struct{}, 1),
		release: releaseYouTube,
	}
	handler.YouTube = youtube
	handleDone := make(chan error, 1)
	go func() {
		handleDone <- handler.Handle(ctx, promote)
	}()

	select {
	case <-youtube.started:
	case <-time.After(2 * time.Second):
		close(releaseYouTube)
		t.Fatal("promotion did not reach blocking YouTube client")
	}

	lockAcquired := make(chan struct{})
	quarantineDone := make(chan error, 1)
	go func() {
		tx, err := fixture.Store.Pool.Begin(ctx)
		if err != nil {
			quarantineDone <- err
			return
		}
		defer func() { _ = tx.Rollback(ctx) }()
		var channelID string
		err = tx.QueryRow(ctx, `
			SELECT id::text
			FROM channel_profiles
			WHERE id = $1::uuid
			FOR UPDATE
		`, fixture.ChannelID).Scan(&channelID)
		if err != nil {
			quarantineDone <- err
			return
		}
		close(lockAcquired)
		if err := applyFenceTestQuarantine(ctx, tx, fixture.ChannelID); err != nil {
			quarantineDone <- err
			return
		}
		quarantineDone <- tx.Commit(ctx)
	}()

	select {
	case <-lockAcquired:
		close(releaseYouTube)
		<-handleDone
		<-quarantineDone
		t.Fatal("quarantine acquired the channel lock while promotion dispatch was active")
	case <-time.After(150 * time.Millisecond):
	}

	close(releaseYouTube)
	if err := <-handleDone; err != nil {
		t.Fatalf("Handle promotion: %v", err)
	}
	if err := <-quarantineDone; err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	if youtube.calls.Load() != 1 {
		t.Fatalf("YouTube calls = %d, want 1 before quarantine", youtube.calls.Load())
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote); err != nil {
		t.Fatalf("stale MarkQueueDone: %v", err)
	}
	requireQuarantinedPromotion(t, ctx, fixture, promote, 2)
}

func TestQuarantineFirstPreventsPromotionSideEffects(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	releaseYouTube := make(chan struct{})
	close(releaseYouTube)
	youtube := &blockingPromotionYouTube{
		started: make(chan struct{}, 1),
		release: releaseYouTube,
	}
	handler.YouTube = youtube

	tx, err := fixture.Store.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin quarantine: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var channelID string
	if err := tx.QueryRow(ctx, `
		SELECT id::text
		FROM channel_profiles
		WHERE id = $1::uuid
		FOR UPDATE
	`, fixture.ChannelID).Scan(&channelID); err != nil {
		t.Fatalf("lock channel: %v", err)
	}
	if err := applyFenceTestQuarantine(ctx, tx, fixture.ChannelID); err != nil {
		t.Fatalf("apply quarantine: %v", err)
	}

	handleDone := make(chan error, 1)
	go func() {
		handleDone <- handler.Handle(ctx, promote)
	}()
	calledBeforeCommit := false
	select {
	case <-youtube.started:
		calledBeforeCommit = true
	case <-time.After(150 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit quarantine: %v", err)
	}
	handleErr := <-handleDone
	if calledBeforeCommit {
		t.Fatal("promotion called YouTube while quarantine held the channel lock")
	}
	if youtube.calls.Load() != 0 {
		t.Fatalf("YouTube calls after quarantine = %d, want 0", youtube.calls.Load())
	}
	if handleErr == nil || !strings.Contains(handleErr.Error(), "channel execution blocked") {
		t.Fatalf("Handle error = %v, want channel execution blocked", handleErr)
	}
	if err := fixture.Store.MarkQueueFailedOrRetry(ctx, promote, handleErr.Error()); err != nil {
		t.Fatalf("stale retry completion: %v", err)
	}
	requireQuarantinedPromotion(t, ctx, fixture, promote, 0)
}

func TestPromotionAuthorityFencesReferencedChannelWithNullOrMismatchedMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, metadata := range []string{"null", "mismatched"} {
		t.Run(metadata, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)

			fixture.InsertChannelWithLaneAccountSeed(ctx)
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
				t.Fatalf("RunTick: %v", err)
			}
			promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
			setPromotionMetadataForAuthorityTest(t, ctx, fixture, &promote, metadata)

			releaseYouTube := make(chan struct{})
			youtube := &blockingPromotionYouTube{started: make(chan struct{}, 1), release: releaseYouTube}
			handler.YouTube = youtube
			handleDone := make(chan error, 1)
			go func() { handleDone <- handler.Handle(ctx, promote) }()
			select {
			case handleErr := <-handleDone:
				close(releaseYouTube)
				if !errors.Is(handleErr, ErrQueueAuthorityInvalid) {
					t.Fatalf("Handle error = %v, want invalid queue authority", handleErr)
				}
			case <-time.After(2 * time.Second):
				close(releaseYouTube)
				t.Fatal("invalid promotion authority did not fail closed")
			}
			if youtube.calls.Load() != 0 {
				t.Fatalf("YouTube calls = %d, want 0", youtube.calls.Load())
			}
			if children := countQueueChildren(t, ctx, fixture, promote.ID); children != 0 {
				t.Fatalf("promotion descendant count = %d, want 0", children)
			}
		})
	}
}

func TestPlanRejectFirstPreventsDirectPromotionSideEffects(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	var operations []*cancellableTestOperation
	registerBoundedFixtureCleanup(t, fixture, &operations)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	taskID := taskIDForQueueItem(t, ctx, fixture, promote)
	publicationID := firstString(promote.PayloadJSON, "publication_id")
	setHumanReviewEvidenceForTest(t, ctx, fixture, taskID, "valid", publicationID, "unlisted")
	promote.PayloadJSON["manual_review"] = true

	rejectTx, err := fixture.Store.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin plan rejection: %v", err)
	}
	registerBoundedRollback(t, rejectTx)
	if _, err := rejectTx.Exec(ctx, `
		UPDATE autoflow_plans
		SET status = 'rejected', rejected_reason = 'concurrent reviewer rejection'
		WHERE id = '00000000-0000-0000-0000-000000000101'::uuid
	`); err != nil {
		t.Fatalf("stage plan rejection: %v", err)
	}
	var rejectPID int
	if err := rejectTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&rejectPID); err != nil {
		t.Fatalf("read rejection pid: %v", err)
	}

	releaseYouTube := make(chan struct{})
	close(releaseYouTube)
	youtube := &blockingPromotionYouTube{started: make(chan struct{}, 1), release: releaseYouTube}
	handler := baseHandler
	handler.YouTube = youtube
	handleOperation := startCancellableTestOperation(t, nil, func(operationCtx context.Context) error {
		return handler.HandlePromotePublication(operationCtx, promote)
	})
	operations = append(operations, handleOperation)

	waitForPlanLockOrExternalCall(t, ctx, fixture, rejectPID, youtube, handleOperation.tryWait)
	if err := rejectTx.Commit(ctx); err != nil {
		t.Fatalf("commit plan rejection: %v", err)
	}
	handleErr, err := handleOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if handleErr != nil {
		t.Fatalf("direct promotion after rejection: %v", handleErr)
	}
	if youtube.calls.Load() != 0 {
		t.Fatalf("YouTube calls after rejection = %d, want 0", youtube.calls.Load())
	}
}

func TestDirectPromotionHoldsPlanAuthorityThroughYouTubeAndDurableWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	var operations []*cancellableTestOperation
	registerBoundedFixtureCleanup(t, fixture, &operations)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	taskID := taskIDForQueueItem(t, ctx, fixture, promote)
	publicationID := firstString(promote.PayloadJSON, "publication_id")
	setHumanReviewEvidenceForTest(t, ctx, fixture, taskID, "valid", publicationID, "unlisted")
	promote.PayloadJSON["manual_review"] = true

	releaseYouTube := make(chan struct{})
	youtube := &blockingPromotionYouTube{started: make(chan struct{}, 1), release: releaseYouTube}
	handler := baseHandler
	handler.YouTube = youtube
	handleOperation := startCancellableTestOperation(t, func() { close(releaseYouTube) }, func(operationCtx context.Context) error {
		return handler.HandlePromotePublication(operationCtx, promote)
	})
	operations = append(operations, handleOperation)
	select {
	case <-youtube.started:
	case <-time.After(5 * time.Second):
		t.Fatal("direct promotion did not reach YouTube")
	}

	conn, err := fixture.Store.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire invalidation connection: %v", err)
	}
	var invalidationOperation *cancellableTestOperation
	t.Cleanup(func() {
		for _, operation := range []*cancellableTestOperation{invalidationOperation, handleOperation} {
			if operation == nil {
				continue
			}
			if _, err := operation.cancelAndDrain(testOperationCleanupTimeout); err != nil {
				t.Errorf("cancel and drain operation before connection release: %v", err)
				return
			}
		}
		conn.Release()
	})
	var invalidationPID int
	if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&invalidationPID); err != nil {
		t.Fatalf("read invalidation pid: %v", err)
	}
	invalidationOperation = startCancellableTestOperation(t, nil, func(operationCtx context.Context) error {
		_, err := conn.Exec(operationCtx, `
			UPDATE autoflow_plans
			SET prompt = prompt || ' concurrent canonical patch'
			WHERE id = '00000000-0000-0000-0000-000000000101'::uuid
		`)
		return err
	})
	operations = append(operations, invalidationOperation)
	waitForDatabaseLock(t, ctx, fixture, invalidationPID, invalidationOperation.tryWait)

	handleOperation.releaseBlocker()
	handleErr, err := handleOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if handleErr != nil {
		t.Fatalf("direct promotion: %v", handleErr)
	}
	invalidationErr, err := invalidationOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if invalidationErr != nil {
		t.Fatalf("canonical invalidation: %v", invalidationErr)
	}
	if youtube.calls.Load() != 1 {
		t.Fatalf("YouTube calls = %d, want 1", youtube.calls.Load())
	}
	var approvedRevisionHash *string
	var approvedRevision *int64
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT approved_revision_hash, approved_revision
		FROM autoflow_plans
		WHERE id = '00000000-0000-0000-0000-000000000101'::uuid
	`).Scan(&approvedRevisionHash, &approvedRevision); err != nil {
		t.Fatalf("read invalidated plan authority: %v", err)
	}
	if approvedRevisionHash != nil || approvedRevision != nil {
		t.Fatalf("plan authority survived canonical patch: hash=%v revision=%v", approvedRevisionHash, approvedRevision)
	}
}

func TestAutomaticOwnedPromotionRejectFirstPreventsPDSAndYouTube(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	var operations []*cancellableTestOperation
	registerBoundedFixtureCleanup(t, fixture, &operations)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	taskID := taskIDForQueueItem(t, ctx, fixture, promote)
	setAutomaticOwnedPlanAuthorityForTest(t, ctx, fixture, taskID)
	task, err := fixture.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetProductionTask: %v", err)
	}
	if taskUsesExternalAssets(task) || task.ApprovalMode == ApprovalHuman || boolValue(promote.PayloadJSON["manual_review"]) {
		t.Fatalf("promotion test must use the automatic owned path: task=%#v payload=%#v", task, promote.PayloadJSON)
	}

	rejectTx, err := fixture.Store.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin plan rejection: %v", err)
	}
	registerBoundedRollback(t, rejectTx)
	if _, err := rejectTx.Exec(ctx, `
		UPDATE autoflow_plans
		SET status = 'rejected', rejected_reason = 'automatic promotion rejection wins'
		WHERE id = '00000000-0000-0000-0000-000000000101'::uuid
	`); err != nil {
		t.Fatalf("stage plan rejection: %v", err)
	}
	var rejectPID int
	if err := rejectTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&rejectPID); err != nil {
		t.Fatalf("read rejection pid: %v", err)
	}

	recorder := &externalCallRecorder{}
	releaseYouTube := make(chan struct{})
	close(releaseYouTube)
	youtube := &blockingPromotionYouTube{started: make(chan struct{}, 1), release: releaseYouTube}
	handler := baseHandler
	handler.PDS = &recordingPDS{recorder: recorder}
	handler.YouTube = youtube
	handleOperation := startCancellableTestOperation(t, nil, func(operationCtx context.Context) error {
		return handler.HandlePromotePublication(operationCtx, promote)
	})
	operations = append(operations, handleOperation)

	waitForPlanLockOrExternalCall(t, ctx, fixture, rejectPID, youtube, handleOperation.tryWait)
	if err := rejectTx.Commit(ctx); err != nil {
		t.Fatalf("commit plan rejection: %v", err)
	}
	handleErr, err := handleOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if handleErr != nil {
		t.Fatalf("automatic promotion after rejection: %v", handleErr)
	}
	if recorder.pds.Load() != 0 || youtube.calls.Load() != 0 {
		t.Fatalf("automatic promotion side effects pds/youtube = %d/%d, want 0/0", recorder.pds.Load(), youtube.calls.Load())
	}
	storedTask, err := fixture.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetProductionTask after rejection: %v", err)
	}
	if storedTask.State != TaskHeld {
		t.Fatalf("task state = %s, want held", storedTask.State)
	}
}

func TestAutomaticOwnedPromotionHoldsPlanLockThroughDurableWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	var operations []*cancellableTestOperation
	registerBoundedFixtureCleanup(t, fixture, &operations)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	taskID := taskIDForQueueItem(t, ctx, fixture, promote)
	setAutomaticOwnedPlanAuthorityForTest(t, ctx, fixture, taskID)

	recorder := &externalCallRecorder{}
	releaseYouTube := make(chan struct{})
	youtube := &blockingPromotionYouTube{started: make(chan struct{}, 1), release: releaseYouTube}
	handler := baseHandler
	handler.PDS = &recordingPDS{recorder: recorder}
	handler.YouTube = youtube
	handleOperation := startCancellableTestOperation(t, func() { close(releaseYouTube) }, func(operationCtx context.Context) error {
		return handler.HandlePromotePublication(operationCtx, promote)
	})
	operations = append(operations, handleOperation)
	select {
	case <-youtube.started:
	case <-time.After(5 * time.Second):
		t.Fatal("automatic promotion did not reach YouTube")
	}

	conn, err := fixture.Store.Pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire invalidation connection: %v", err)
	}
	var invalidationOperation *cancellableTestOperation
	t.Cleanup(func() {
		for _, operation := range []*cancellableTestOperation{invalidationOperation, handleOperation} {
			if operation == nil {
				continue
			}
			if _, err := operation.cancelAndDrain(testOperationCleanupTimeout); err != nil {
				t.Errorf("cancel and drain operation before connection release: %v", err)
				return
			}
		}
		conn.Release()
	})
	var invalidationPID int
	if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&invalidationPID); err != nil {
		t.Fatalf("read invalidation pid: %v", err)
	}
	invalidationOperation = startCancellableTestOperation(t, nil, func(operationCtx context.Context) error {
		_, err := conn.Exec(operationCtx, `
			UPDATE autoflow_plans
			SET prompt = prompt || ' automatic concurrent patch'
			WHERE id = '00000000-0000-0000-0000-000000000101'::uuid
		`)
		return err
	})
	operations = append(operations, invalidationOperation)
	waitForDatabaseLock(t, ctx, fixture, invalidationPID, invalidationOperation.tryWait)

	handleOperation.releaseBlocker()
	handleErr, err := handleOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if handleErr != nil {
		t.Fatalf("automatic promotion: %v", handleErr)
	}
	invalidationErr, err := invalidationOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	if invalidationErr != nil {
		t.Fatalf("canonical invalidation: %v", invalidationErr)
	}
	if recorder.pds.Load() != 1 || youtube.calls.Load() != 1 {
		t.Fatalf("automatic promotion side effects pds/youtube = %d/%d, want 1/1", recorder.pds.Load(), youtube.calls.Load())
	}
	publication, err := fixture.Store.GetPublication(ctx, firstString(promote.PayloadJSON, "publication_id"))
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	if publication.PublishStatus != "scheduled" || publication.ScheduledPublishAt == nil {
		t.Fatalf("publication did not durably linearize before invalidation: %#v", publication)
	}
}

func TestPromotionRevalidatesFencedChannelAfterTaskScopeLock(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, haltedB := range []bool{false, true} {
		name := "enabled-b"
		if haltedB {
			name = "halted-b"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			var operations []*cancellableTestOperation
			registerBoundedFixtureCleanup(t, fixture, &operations)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			promote := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
			taskID := taskIDForQueueItem(t, ctx, fixture, promote)
			setAutomaticOwnedPlanAuthorityForTest(t, ctx, fixture, taskID)

			channelB := testUUID(t, name)
			var haltedAt any
			if haltedB {
				haltedAt = fixture.Store.Now().UTC()
			}
			if _, err := fixture.Store.Pool.Exec(ctx, `
				INSERT INTO channel_profiles (
					id, name, positioning, language, default_aspect_ratio, risk_policy_json,
					content_mix_policy_json, cadence_policy_json, alert_policy_json, enabled,
					dry_run, halted_at, halt_reason, config_version, tick_interval_minutes,
					created_at, updated_at
				) VALUES (
					$1::uuid, $2, '', 'en', '9:16', '{}'::json, '{}'::json, '{}'::json,
					'{}'::json, TRUE, FALSE, $3::timestamptz,
					CASE WHEN $3::timestamptz IS NULL THEN NULL ELSE 'halted test channel' END,
					1, 60, NOW(), NOW()
				)
			`, channelB, name, haltedAt); err != nil {
				t.Fatalf("insert channel B: %v", err)
			}
			fixture.AdditionalChannelIDs = append(fixture.AdditionalChannelIDs, channelB)
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), testOperationCleanupTimeout)
				defer cancel()
				_, err := fixture.Store.Pool.Exec(cleanupCtx, `
					UPDATE production_tasks SET channel_profile_id = $2::uuid WHERE id = $1::uuid
				`, taskID, fixture.ChannelID)
				if err != nil {
					t.Errorf("restore task authority: %v", err)
				}
			})

			blocker, err := fixture.Store.Pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin channel A blocker: %v", err)
			}
			registerBoundedRollback(t, blocker)
			var lockedChannel string
			if err := blocker.QueryRow(ctx, `
				SELECT id::text FROM channel_profiles WHERE id = $1::uuid FOR UPDATE
			`, fixture.ChannelID).Scan(&lockedChannel); err != nil {
				t.Fatalf("lock channel A: %v", err)
			}
			var blockerPID int
			if err := blocker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
				t.Fatalf("read channel A blocker pid: %v", err)
			}

			recorder := &externalCallRecorder{}
			releaseYouTube := make(chan struct{})
			close(releaseYouTube)
			youtube := &blockingPromotionYouTube{started: make(chan struct{}, 1), release: releaseYouTube}
			handler := baseHandler
			handler.PDS = &recordingPDS{recorder: recorder}
			handler.YouTube = youtube
			handleOperation := startCancellableTestOperation(t, nil, func(operationCtx context.Context) error {
				return handler.Handle(operationCtx, promote)
			})
			operations = append(operations, handleOperation)
			waitForChannelLockOrExternalCall(t, ctx, fixture, blockerPID, youtube)

			if _, err := blocker.Exec(ctx, `
				UPDATE production_tasks SET channel_profile_id = $2::uuid WHERE id = $1::uuid
			`, taskID, channelB); err != nil {
				t.Fatalf("reassign task authority to channel B: %v", err)
			}
			if err := blocker.Commit(ctx); err != nil {
				t.Fatalf("commit task reassignment: %v", err)
			}
			handleErr, err := handleOperation.waitOrCancelAndDrain(5*time.Second, testOperationCleanupTimeout)
			if err != nil {
				t.Fatal(err)
			}
			if !errors.Is(handleErr, ErrQueueAuthorityInvalid) {
				t.Fatalf("Handle error = %v, want invalid queue authority", handleErr)
			}
			if recorder.pds.Load() != 0 || youtube.calls.Load() != 0 {
				t.Fatalf("reassigned promotion side effects pds/youtube = %d/%d, want 0/0", recorder.pds.Load(), youtube.calls.Load())
			}
			if children := countQueueChildren(t, ctx, fixture, promote.ID); children != 0 {
				t.Fatalf("promotion descendant count = %d, want 0", children)
			}
		})
	}
}

func TestRunnerImmediatelyRejectsInvalidQueueAuthority(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, scenario := range []string{"stored_halted_mismatch", "unresolved_reference"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)

			var itemID string
			switch scenario {
			case "stored_halted_mismatch":
				setupHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
				if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), setupHandler); err != nil {
					t.Fatalf("RunTick: %v", err)
				}
				haltedChannelID := insertAuthorityTestChannel(t, ctx, fixture, "halted-metadata", true)
				if err := fixture.Store.Pool.QueryRow(ctx, `
					UPDATE channel_ops_queue_items
					SET channel_profile_id = $1::uuid
					WHERE kind = $2 AND channel_profile_id = $3::uuid
					RETURNING id::text
				`, haltedChannelID, QueuePlanTask, fixture.ChannelID).Scan(&itemID); err != nil {
					t.Fatalf("store mismatched halted metadata: %v", err)
				}
			case "unresolved_reference":
				var err error
				itemID, err = fixture.Store.Enqueue(ctx, EnqueueOptions{
					Kind:           QueuePlanTask,
					IdempotencyKey: "plan_task:missing-authority:" + testUUID(t, "unresolved-queue-key"),
					Payload: map[string]any{
						"production_task_id": testUUID(t, "missing-production-task"),
					},
				})
				if err != nil {
					t.Fatalf("enqueue unresolved item: %v", err)
				}
				defer func() {
					_, _ = fixture.Store.Pool.Exec(ctx, `DELETE FROM channel_ops_queue_items WHERE id = $1::uuid`, itemID)
				}()
			}

			recorder := &externalCallRecorder{}
			sink := &recordingAlertSink{}
			handler := HandlerService{
				Store:    fixture.Store,
				PDS:      &recordingPDS{recorder: recorder},
				AutoFlow: &recordingAutoFlow{recorder: recorder},
				YouTube:  &recordingYouTube{recorder: recorder},
				Alerts:   sink,
			}
			runner := &Runner{Store: fixture.Store, Handlers: handler}
			runner.SetLastSchedulerRun(fixture.Store.Now())
			if err := runner.runOnce(ctx); err != nil {
				t.Fatalf("runOnce: %v", err)
			}

			var status string
			var attempts int
			var lockedBy *string
			var lockedAt, deadLetterAt *time.Time
			var lastError *string
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, attempt_count, locked_by, locked_at, dead_letter_at, last_error
				FROM channel_ops_queue_items WHERE id = $1::uuid
			`, itemID).Scan(&status, &attempts, &lockedBy, &lockedAt, &deadLetterAt, &lastError); err != nil {
				t.Fatalf("inspect rejected queue item: %v", err)
			}
			if status != QueueStatusDeadLettered || attempts != 1 || lockedBy != nil || lockedAt != nil || deadLetterAt == nil {
				t.Fatalf("rejected lease = status %s attempts %d locked_by %v locked_at %v dead_letter_at %v",
					status, attempts, lockedBy, lockedAt, deadLetterAt)
			}
			if lastError == nil || !strings.Contains(*lastError, "queue authority") {
				t.Fatalf("last_error = %v, want queue authority failure", lastError)
			}
			if recorder.total() != 0 || len(sink.payloads) != 0 {
				t.Fatalf("external calls = %d alerts = %d, want zero", recorder.total(), len(sink.payloads))
			}
			if children := countQueueChildren(t, ctx, fixture, itemID); children != 0 {
				t.Fatalf("descendant count = %d, want zero", children)
			}
		})
	}
}

func TestAlertQueueAuthorityScopesLegacyAndGlobalRows(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)

	t.Run("halted payload channel is not claimed", func(t *testing.T) {
		if _, err := fixture.Store.Pool.Exec(ctx, `
			UPDATE channel_profiles SET halted_at = NOW(), halt_reason = 'alert fence test'
			WHERE id = $1::uuid
		`, fixture.ChannelID); err != nil {
			t.Fatalf("halt channel: %v", err)
		}
		channelID := fixture.ChannelID
		itemID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
			Kind:             QueueSendAlert,
			IdempotencyKey:   "send_alert:halted-payload-channel",
			Payload:          alertQueuePayload(fixture.ChannelID),
			ChannelProfileID: &channelID,
		})
		if err != nil {
			t.Fatalf("enqueue alert: %v", err)
		}
		item, err := fixture.Store.ClaimNextForKinds(ctx, "halted-alert-worker", []string{QueueSendAlert})
		if err != nil {
			t.Fatalf("claim halted alert: %v", err)
		}
		if item != nil {
			t.Fatalf("claimed halted channel alert: %#v", item)
		}
		if _, err := fixture.Store.Pool.Exec(ctx, `DELETE FROM channel_ops_queue_items WHERE id = $1::uuid`, itemID); err != nil {
			t.Fatalf("delete halted alert: %v", err)
		}
		if _, err := fixture.Store.Pool.Exec(ctx, `
			UPDATE channel_profiles SET halted_at = NULL, halt_reason = NULL WHERE id = $1::uuid
		`, fixture.ChannelID); err != nil {
			t.Fatalf("unhalt channel: %v", err)
		}
	})

	t.Run("legacy stored channel is fenced", func(t *testing.T) {
		channelID := fixture.ChannelID
		_, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
			Kind:             QueueSendAlert,
			IdempotencyKey:   "send_alert:legacy-stored-channel",
			Payload:          alertQueuePayload(""),
			ChannelProfileID: &channelID,
		})
		if err != nil {
			t.Fatalf("enqueue legacy alert: %v", err)
		}
		item, err := fixture.Store.ClaimNextForKinds(ctx, "legacy-alert-worker", []string{QueueSendAlert})
		if err != nil || item == nil {
			t.Fatalf("claim legacy alert = %#v, %v", item, err)
		}
		if _, err := fixture.Store.Pool.Exec(ctx, `
			UPDATE channel_profiles SET halted_at = NOW(), halt_reason = 'legacy alert fence'
			WHERE id = $1::uuid
		`, fixture.ChannelID); err != nil {
			t.Fatalf("halt channel after claim: %v", err)
		}
		sink := &recordingAlertSink{}
		handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
		handler.Alerts = sink
		err = handler.Handle(ctx, *item)
		if !errors.Is(err, ErrChannelExecutionBlocked) {
			t.Fatalf("Handle legacy alert error = %v, want channel execution blocked", err)
		}
		if len(sink.payloads) != 0 {
			t.Fatalf("legacy halted alert dispatched %d times", len(sink.payloads))
		}
		if _, err := fixture.Store.Pool.Exec(ctx, `
			UPDATE channel_profiles SET halted_at = NULL, halt_reason = NULL WHERE id = $1::uuid
		`, fixture.ChannelID); err != nil {
			t.Fatalf("unhalt channel: %v", err)
		}
	})

	t.Run("payloadless null metadata is global", func(t *testing.T) {
		itemID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
			Kind:           QueueSendAlert,
			IdempotencyKey: "send_alert:truly-global:" + testUUID(t, "global-alert-key"),
			Payload:        alertQueuePayload(""),
		})
		if err != nil {
			t.Fatalf("enqueue global alert: %v", err)
		}
		defer func() {
			_, _ = fixture.Store.Pool.Exec(ctx, `DELETE FROM channel_ops_queue_items WHERE id = $1::uuid`, itemID)
		}()
		item, err := fixture.Store.ClaimNextForKinds(ctx, "global-alert-worker", []string{QueueSendAlert})
		if err != nil || item == nil {
			t.Fatalf("claim global alert = %#v, %v", item, err)
		}
		sink := &recordingAlertSink{}
		handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
		handler.Alerts = sink
		if err := handler.Handle(ctx, *item); err != nil {
			t.Fatalf("Handle global alert: %v", err)
		}
		if len(sink.payloads) != 1 {
			t.Fatalf("global alert dispatch count = %d, want one", len(sink.payloads))
		}
	})
}

func TestQuarantineFirstBlocksNullOrMismatchedPromotionMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, metadata := range []string{"null", "mismatched"} {
		t.Run(metadata, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)

			fixture.InsertChannelWithLaneAccountSeed(ctx)
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
				t.Fatalf("RunTick: %v", err)
			}
			promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
			setPromotionMetadataForAuthorityTest(t, ctx, fixture, &promote, metadata)

			quarantineTx, err := fixture.Store.Pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin quarantine: %v", err)
			}
			defer func() { _ = quarantineTx.Rollback(ctx) }()
			var channelID string
			if err := quarantineTx.QueryRow(ctx, `
				SELECT id::text FROM channel_profiles WHERE id = $1::uuid FOR UPDATE
			`, fixture.ChannelID).Scan(&channelID); err != nil {
				t.Fatalf("lock channel: %v", err)
			}
			if err := applyFenceTestQuarantine(ctx, quarantineTx, fixture.ChannelID); err != nil {
				t.Fatalf("apply quarantine: %v", err)
			}

			releaseYouTube := make(chan struct{})
			close(releaseYouTube)
			youtube := &blockingPromotionYouTube{started: make(chan struct{}, 1), release: releaseYouTube}
			handler.YouTube = youtube
			handleDone := make(chan error, 1)
			go func() { handleDone <- handler.Handle(ctx, promote) }()
			var handleErr error
			select {
			case handleErr = <-handleDone:
			case <-time.After(2 * time.Second):
				t.Fatal("invalid promotion authority did not fail closed")
			}
			if err := quarantineTx.Commit(ctx); err != nil {
				t.Fatalf("commit quarantine: %v", err)
			}
			if !errors.Is(handleErr, ErrQueueAuthorityInvalid) {
				t.Fatalf("Handle error = %v, want invalid queue authority", handleErr)
			}
			if youtube.calls.Load() != 0 {
				t.Fatalf("YouTube calls after quarantine = %d, want 0", youtube.calls.Load())
			}
			if children := countQueueChildren(t, ctx, fixture, promote.ID); children != 0 {
				t.Fatalf("promotion descendant count = %d, want 0", children)
			}
		})
	}
}

func TestGlobalCleanupAndAlertDispatchWithoutChannelMetadata(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	sink := &recordingAlertSink{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	handler.Alerts = sink

	if err := handler.Handle(ctx, QueueItemRow{
		ID:          testUUID(t, "global-cleanup"),
		Kind:        QueueCleanupExpired,
		PayloadJSON: map[string]any{},
	}); err != nil {
		t.Fatalf("global cleanup: %v", err)
	}
	if err := handler.Handle(ctx, QueueItemRow{
		ID:   testUUID(t, "global-alert"),
		Kind: QueueSendAlert,
		PayloadJSON: map[string]any{
			"kind":    "global_test",
			"message": "global alert",
		},
	}); err != nil {
		t.Fatalf("global alert: %v", err)
	}
	if len(sink.payloads) != 1 || sink.payloads[0].ChannelID != "" {
		t.Fatalf("global alert payloads = %#v", sink.payloads)
	}
}

func TestHeldPromotionIsStaleBeforeExternalSideEffects(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	publicationID, _ := promote.PayloadJSON["publication_id"].(string)
	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE production_tasks
		SET state = $2, blocked_by_guard = 'manual_hold'
		WHERE id = $1::uuid
	`, publication.ProductionTaskID, TaskHeld); err != nil {
		t.Fatalf("hold task: %v", err)
	}
	releaseYouTube := make(chan struct{})
	close(releaseYouTube)
	youtube := &blockingPromotionYouTube{
		started: make(chan struct{}, 1),
		release: releaseYouTube,
	}
	handler.YouTube = youtube

	if err := handler.Handle(ctx, promote); err != nil {
		t.Fatalf("Handle held promotion: %v", err)
	}
	if youtube.calls.Load() != 0 {
		t.Fatalf("held promotion YouTube calls = %d, want 0", youtube.calls.Load())
	}
	var state string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT state FROM production_tasks WHERE id = $1::uuid
	`, publication.ProductionTaskID).Scan(&state); err != nil {
		t.Fatalf("select held task: %v", err)
	}
	if state != TaskHeld {
		t.Fatalf("task state = %s, want held", state)
	}
	if children := countQueueChildren(t, ctx, fixture, promote.ID); children != 0 {
		t.Fatalf("held promotion descendant count = %d, want 0", children)
	}
}

func TestHeldTaskHandlersAreStaleBeforeExternalCallsOrDescendants(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, kind := range []string{
		QueuePlanTask,
		QueueExecuteTask,
		QueueObserveJob,
		QueuePublishTask,
		QueueReconcilePublication,
		QueueCollectMetrics,
	} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)

			fixture.InsertChannelWithLaneAccountSeed(ctx)
			baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			item := prepareQueueKind(t, ctx, fixture, baseHandler, kind)
			taskID := taskIDForQueueItem(t, ctx, fixture, item)
			if _, err := fixture.Store.Pool.Exec(ctx, `
				UPDATE production_tasks
				SET state = $2, blocked_by_guard = 'manual_hold'
				WHERE id = $1::uuid
			`, taskID, TaskHeld); err != nil {
				t.Fatalf("hold task: %v", err)
			}
			recorder := &externalCallRecorder{}
			handler := baseHandler
			handler.PDS = &recordingPDS{recorder: recorder}
			handler.AutoFlow = &recordingAutoFlow{recorder: recorder}
			handler.YouTube = &recordingYouTube{recorder: recorder}

			if err := handler.Handle(ctx, item); err != nil {
				t.Fatalf("Handle held %s: %v", kind, err)
			}
			if calls := recorder.total(); calls != 0 {
				t.Fatalf("held %s external calls = %d, want 0", kind, calls)
			}
			var state string
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT state FROM production_tasks WHERE id = $1::uuid
			`, taskID).Scan(&state); err != nil {
				t.Fatalf("select held task: %v", err)
			}
			if state != TaskHeld {
				t.Fatalf("held %s task state = %s", kind, state)
			}
			if children := countQueueChildren(t, ctx, fixture, item.ID); children != 0 {
				t.Fatalf("held %s descendant count = %d, want 0", kind, children)
			}
		})
	}
}

func TestExternalAssetPlanRequiresHumanReviewBeforeExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	item := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePlanTask)
	taskID := taskIDForQueueItem(t, ctx, fixture, item)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE production_tasks
		SET uses_external_assets = TRUE,
		    source_platforms_json = '["youtube"]'::json,
		    approval_mode = $2
		WHERE id = $1::uuid
	`, taskID, ApprovalAgent); err != nil {
		t.Fatalf("mark task external: %v", err)
	}
	recorder := &externalCallRecorder{}
	handler := baseHandler
	handler.PDS = &recordingPDS{recorder: recorder}
	handler.AutoFlow = &recordingAutoFlow{recorder: recorder}
	handler.YouTube = &recordingYouTube{recorder: recorder}

	if err := handler.Handle(ctx, item); err != nil {
		t.Fatalf("Handle external plan: %v", err)
	}
	if recorder.plan.Load() != 1 {
		t.Fatalf("AutoFlow plan calls = %d, want 1", recorder.plan.Load())
	}
	if calls := recorder.total() - recorder.plan.Load(); calls != 0 {
		t.Fatalf("external calls after planning = %d, want 0", calls)
	}
	var state string
	var guard *string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT state, blocked_by_guard
		FROM production_tasks
		WHERE id = $1::uuid
	`, taskID).Scan(&state, &guard); err != nil {
		t.Fatalf("select external task: %v", err)
	}
	if state != TaskHeld || guard == nil || *guard != "human_approval_required" {
		t.Fatalf("external task state/guard = %s/%v", state, guard)
	}
	if children := countQueueChildren(t, ctx, fixture, item.ID); children != 0 {
		t.Fatalf("external plan descendant count = %d, want 0", children)
	}
}

func TestExternalAssetExecuteRejectsInvalidHumanReviewEvidence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, evidenceCase := range []string{"missing", "agent_only", "stale", "mismatched", "revision_mismatched", "rejected"} {
		t.Run(evidenceCase, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			item := prepareQueueKind(t, ctx, fixture, baseHandler, QueueExecuteTask)
			taskID := taskIDForQueueItem(t, ctx, fixture, item)
			setHumanReviewEvidenceForTest(t, ctx, fixture, taskID, evidenceCase, "", "")
			recorder := &externalCallRecorder{}
			handler := baseHandler
			handler.PDS = &recordingPDS{recorder: recorder}
			handler.AutoFlow = &recordingAutoFlow{recorder: recorder}
			handler.YouTube = &recordingYouTube{recorder: recorder}

			if err := handler.Handle(ctx, item); err != nil {
				t.Fatalf("Handle execute: %v", err)
			}
			if calls := recorder.total(); calls != 0 {
				t.Fatalf("external calls = %d, want 0", calls)
			}
			task, err := fixture.Store.GetProductionTask(ctx, taskID)
			if err != nil {
				t.Fatalf("GetProductionTask: %v", err)
			}
			if task.State != TaskHeld || task.BlockedByGuard == nil || *task.BlockedByGuard != "human_review_evidence_invalid" {
				t.Fatalf("task state/guard = %s/%v", task.State, task.BlockedByGuard)
			}
		})
	}
}

func TestValidHumanReviewEvidenceReachesExecutePublishAndManualPromotion(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, kind := range []string{QueueExecuteTask, QueuePublishTask, QueuePromotePublication} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			item := prepareQueueKind(t, ctx, fixture, baseHandler, kind)
			taskID := taskIDForQueueItem(t, ctx, fixture, item)
			publicationID := firstString(item.PayloadJSON, "publication_id")
			setHumanReviewEvidenceForTest(t, ctx, fixture, taskID, "valid", publicationID, "unlisted")
			if kind == QueuePromotePublication {
				item.PayloadJSON["manual_review"] = true
			}
			recorder := &externalCallRecorder{}
			handler := baseHandler
			handler.PDS = &recordingPDS{recorder: recorder}
			handler.AutoFlow = &recordingAutoFlow{recorder: recorder}
			handler.YouTube = &recordingYouTube{recorder: recorder}

			if err := handler.Handle(ctx, item); err != nil {
				t.Fatalf("Handle %s: %v", kind, err)
			}
			switch kind {
			case QueueExecuteTask:
				if recorder.execute.Load() != 1 {
					t.Fatalf("execute calls = %d, want 1", recorder.execute.Load())
				}
			case QueuePublishTask:
				if recorder.pds.Load() != 1 {
					t.Fatalf("PDS calls = %d, want 1", recorder.pds.Load())
				}
			case QueuePromotePublication:
				if recorder.pds.Load() != 1 || recorder.schedulePublish.Load() != 1 {
					t.Fatalf("promotion PDS/YouTube calls = %d/%d, want 1/1", recorder.pds.Load(), recorder.schedulePublish.Load())
				}
			}
		})
	}
}

func TestExecuteHandlerRetryReusesStableApprovedRevisionKey(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	item := prepareQueueKind(t, ctx, fixture, baseHandler, QueueExecuteTask)
	taskID := taskIDForQueueItem(t, ctx, fixture, item)
	setHumanReviewEvidenceForTest(t, ctx, fixture, taskID, "valid", "", "")

	keys := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode execute request: %v", err)
		}
		keys = append(keys, firstString(payload, "idempotency_key"))
		if len(keys) == 1 {
			panic(http.ErrAbortHandler)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"run_id":"00000000-0000-0000-0000-000000000201",
			"pipeline_id":"00000000-0000-0000-0000-000000000202",
			"job_id":"00000000-0000-0000-0000-000000000301",
			"status":"PENDING"
		}`))
	}))
	defer server.Close()

	handler := baseHandler
	handler.AutoFlow = HTTPAutoFlowClient{BaseURL: server.URL}
	if err := handler.HandleExecuteTask(ctx, item); err == nil {
		t.Fatal("first execute attempt should observe response loss")
	}
	if err := handler.HandleExecuteTask(ctx, item); err != nil {
		t.Fatalf("execute replay: %v", err)
	}
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("execute idempotency keys = %#v, want two identical non-empty keys", keys)
	}
	task, err := fixture.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetProductionTask: %v", err)
	}
	if task.State != TaskProducing || task.AutoFlowRunID == nil || task.JobID == nil {
		t.Fatalf("task execution state = %s run = %v job = %v", task.State, task.AutoFlowRunID, task.JobID)
	}
}

func TestPublishTaskEnqueuesQuotaLowAlert(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	publish := fixture.ProcessUntilQueueKind(ctx, handler, QueuePublishTask)
	handler.YouTube = fakeLowQuotaYouTube{fakeYouTube{}}

	if err := handler.HandlePublishTask(ctx, publish); err != nil {
		t.Fatalf("HandlePublishTask: %v", err)
	}

	var alertKind string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT payload_json ->> 'kind'
		FROM channel_ops_queue_items
		WHERE kind = $1
		  AND channel_profile_id = $2::uuid
		  AND payload_json ->> 'kind' = 'quota_low'
	`, QueueSendAlert, fixture.ChannelID).Scan(&alertKind); err != nil {
		t.Fatalf("select quota alert: %v", err)
	}
	if alertKind != "quota_low" {
		t.Fatalf("quota alert kind = %q", alertKind)
	}
}

func TestCollectMetricsRequeueUsesConfiguredDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.HandlePromotePublication(ctx, promote); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote); err != nil {
		t.Fatalf("MarkQueueDone promote: %v", err)
	}
	collect := fixture.ProcessUntilQueueKind(ctx, handler, QueueCollectMetrics)
	handler.YouTube = fakeNoMetricsYouTube{fakeYouTube{}}
	handler.Config.MetricsPollDelayMinutes = 11
	handler.Config.MetricsPollMaxAttempts = 3

	if err := handler.HandleCollectMetrics(ctx, collect); err != nil {
		t.Fatalf("HandleCollectMetrics: %v", err)
	}

	publicationID, _ := collect.PayloadJSON["publication_id"].(string)
	var runAfter time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT run_after
		FROM channel_ops_queue_items
		WHERE kind = $1
		  AND idempotency_key = $2
	`, QueueCollectMetrics, fmt.Sprintf("collect_metrics:%s:poll:1", publicationID)).Scan(&runAfter); err != nil {
		t.Fatalf("select requeued collect_metrics run_after: %v", err)
	}
	if want := fixture.Store.Now().UTC().Add(11 * time.Minute); !runAfter.Equal(want) {
		t.Fatalf("requeued collect_metrics run_after = %s, want %s", runAfter, want)
	}
}

func TestCollectMetricsUpsertsStagedRewardSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.HandlePromotePublication(ctx, promote); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote); err != nil {
		t.Fatalf("MarkQueueDone promote: %v", err)
	}
	collect := fixture.ProcessUntilQueueKind(ctx, handler, QueueCollectMetrics)
	publicationID, _ := collect.PayloadJSON["publication_id"].(string)
	collect.PayloadJSON["snapshot_stage"] = "6h"
	collect.PayloadJSON["metrics"] = map[string]any{
		"views":                 1000,
		"likes":                 50,
		"comments":              10,
		"avg_view_duration_sec": 18.0,
	}
	if err := handler.HandleCollectMetrics(ctx, collect); err != nil {
		t.Fatalf("HandleCollectMetrics first: %v", err)
	}
	collect.PayloadJSON["metrics"] = map[string]any{
		"views":                 1200,
		"likes":                 75,
		"comments":              15,
		"avg_view_duration_sec": 20.0,
	}
	if err := handler.HandleCollectMetrics(ctx, collect); err != nil {
		t.Fatalf("HandleCollectMetrics second: %v", err)
	}

	var count int
	var stage string
	var likes int
	var hasReward bool
	var hasEngagement bool
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT COUNT(*),
		       COALESCE(MAX(snapshot_stage), ''),
		       COALESCE(MAX(likes), 0),
		       bool_or(reward_score IS NOT NULL),
		       bool_or(reward_components_json::jsonb ? 'engagement_rate')
		FROM feedback_snapshots
		WHERE publication_id = $1::uuid
	`, publicationID).Scan(&count, &stage, &likes, &hasReward, &hasEngagement); err != nil {
		t.Fatalf("select feedback snapshots: %v", err)
	}
	if count != 1 || stage != "6h" || likes != 75 || !hasReward || !hasEngagement {
		t.Fatalf("snapshot summary count/stage/likes/reward/engagement = %d/%s/%d/%v/%v", count, stage, likes, hasReward, hasEngagement)
	}
}

func TestRecomputeLearningStateForSourcesAggregatesReward(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.HandlePromotePublication(ctx, promote); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote); err != nil {
		t.Fatalf("MarkQueueDone promote: %v", err)
	}
	collect := fixture.ProcessUntilQueueKind(ctx, handler, QueueCollectMetrics)
	collect.PayloadJSON["metrics"] = map[string]any{
		"views":                 1000,
		"likes":                 50,
		"comments":              10,
		"avg_view_duration_sec": 18.0,
	}
	if err := handler.HandleCollectMetrics(ctx, collect); err != nil {
		t.Fatalf("HandleCollectMetrics: %v", err)
	}

	if err := fixture.Store.RecomputeLearningState(ctx, fixture.ChannelID, 7); err != nil {
		t.Fatalf("RecomputeLearningState: %v", err)
	}

	var sampleCount int
	var avgReward float64
	var confidence float64
	var recommendation string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT sample_count, avg_reward, confidence, recommendation_json ->> 'action'
		FROM learning_states
		WHERE channel_profile_id = $1::uuid
		  AND dimension_type = 'source'
		  AND dimension_key = $2
		  AND window_days = 7
	`, fixture.ChannelID, SourceLaneSeed).Scan(&sampleCount, &avgReward, &confidence, &recommendation); err != nil {
		t.Fatalf("select learning state: %v", err)
	}
	if sampleCount != 1 || avgReward <= 0 || confidence <= 0 || recommendation != "insufficient_data" {
		t.Fatalf("learning sample/reward/confidence/action = %d/%f/%f/%s", sampleCount, avgReward, confidence, recommendation)
	}
}

func TestPublicationMetricsFailureCategoryPersistsAndClears(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.HandlePromotePublication(ctx, promote); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote); err != nil {
		t.Fatalf("MarkQueueDone promote: %v", err)
	}
	collect := fixture.ProcessUntilQueueKind(ctx, handler, QueueCollectMetrics)
	publicationID, _ := collect.PayloadJSON["publication_id"].(string)
	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}

	if err := fixture.Store.RequeueOrHoldMetrics(ctx, publication, collect, 1, time.Minute); err != nil {
		t.Fatalf("RequeueOrHoldMetrics: %v", err)
	}
	heldTask, err := fixture.Store.GetProductionTask(ctx, publication.ProductionTaskID)
	if err != nil {
		t.Fatalf("GetProductionTask held: %v", err)
	}
	if heldTask.FailureCategory == nil || *heldTask.FailureCategory != FailureMetrics {
		t.Fatalf("held task failure category = %#v, want %q", heldTask.FailureCategory, FailureMetrics)
	}

	if err := fixture.Store.markTaskUploadedPrivate(ctx, publication.ProductionTaskID, fixture.Store.Now()); err != nil {
		t.Fatalf("markTaskUploadedPrivate: %v", err)
	}
	clearedTask, err := fixture.Store.GetProductionTask(ctx, publication.ProductionTaskID)
	if err != nil {
		t.Fatalf("GetProductionTask cleared: %v", err)
	}
	if clearedTask.FailureCategory != nil {
		t.Fatalf("cleared task failure category = %#v, want nil", *clearedTask.FailureCategory)
	}
	if clearedTask.FailureReason != nil {
		t.Fatalf("cleared task failure reason = %#v, want nil", *clearedTask.FailureReason)
	}
}

func TestPublicationYouTubeStatusFailureCategoryPersists(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.HandlePromotePublication(ctx, promote); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote); err != nil {
		t.Fatalf("MarkQueueDone promote: %v", err)
	}
	publicationID, _ := promote.PayloadJSON["publication_id"].(string)
	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	handler.YouTube = fakeSevereStatusYouTube{fakeYouTube{}}

	err = handler.HandleReconcilePublication(ctx, QueueItemRow{
		ID:          testUUID(t, "reconcile-item"),
		Kind:        QueueReconcilePublication,
		PayloadJSON: map[string]any{"publication_id": publicationID},
	})
	if err != nil {
		t.Fatalf("HandleReconcilePublication: %v", err)
	}
	task, err := fixture.Store.GetProductionTask(ctx, publication.ProductionTaskID)
	if err != nil {
		t.Fatalf("GetProductionTask: %v", err)
	}
	if task.FailureCategory == nil || *task.FailureCategory != FailureYouTubeStatus {
		t.Fatalf("youtube status task failure category = %#v, want %q", task.FailureCategory, FailureYouTubeStatus)
	}
	var alertKind string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT payload_json ->> 'kind'
		FROM channel_ops_queue_items
		WHERE kind = $1
		  AND channel_profile_id = $2::uuid
		  AND payload_json ->> 'kind' = 'platform_rejected'
	`, QueueSendAlert, fixture.ChannelID).Scan(&alertKind); err != nil {
		t.Fatalf("select platform alert: %v", err)
	}
	if alertKind != "platform_rejected" {
		t.Fatalf("platform alert kind = %q", alertKind)
	}
}

func TestRunTickWritesDecisionAuditDryRunWithoutTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	fixture.SetDryRun(ctx, true)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}

	if got := fixture.CountProductionTasks(ctx); got != 0 {
		t.Fatalf("production task count = %d, want 0", got)
	}

	row := fixture.RequireSingleDecisionAudit(ctx)
	if !row.Selected {
		t.Fatal("dry-run accepted candidate audit selected = false, want true")
	}
	if row.CreatedTaskID != nil {
		t.Fatalf("dry-run decision audit created_task_id = %v, want nil", *row.CreatedTaskID)
	}
	if row.TargetAccountID == nil || *row.TargetAccountID != fixture.AccountID {
		t.Fatalf("target_account_id = %#v, want %s", row.TargetAccountID, fixture.AccountID)
	}
	if row.CandidateSource != SourceLaneSeed {
		t.Fatalf("candidate source = %q, want %q", row.CandidateSource, SourceLaneSeed)
	}
	if row.RejectionReason != nil {
		t.Fatalf("dry-run accepted decision audit rejection_reason = %v, want nil", *row.RejectionReason)
	}
	guards := decodeDecisionAuditArray(t, "guard_results_json", row.GuardResultsJSON)
	if len(guards) != 0 {
		t.Fatalf("guard_results_json length = %d, want 0: %s", len(guards), row.GuardResultsJSON)
	}
	decodeDecisionAuditObject(t, "score_json", row.ScoreJSON)
	decodeDecisionAuditObject(t, "pds_decision_json", row.PDSDecisionJSON)
	decodeDecisionAuditObject(t, "learning_context_json", row.LearningContextJSON)
}

func TestRunTickWritesDecisionAuditRejectedGuardResults(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	fixture.SetAccountEnabled(ctx, false)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}

	if got := fixture.CountProductionTasks(ctx); got != 0 {
		t.Fatalf("production task count = %d, want 0", got)
	}

	row := fixture.RequireSingleDecisionAudit(ctx)
	if row.Selected {
		t.Fatal("rejected candidate audit selected = true, want false")
	}
	if row.CreatedTaskID != nil {
		t.Fatalf("rejected decision audit created_task_id = %v, want nil", *row.CreatedTaskID)
	}
	if row.CandidateSource != SourceLaneSeed {
		t.Fatalf("candidate source = %q, want %q", row.CandidateSource, SourceLaneSeed)
	}
	if row.RejectionReason == nil || *row.RejectionReason == "" {
		t.Fatalf("rejection_reason = %#v, want non-empty", row.RejectionReason)
	}
	guards := decodeDecisionAuditArray(t, "guard_results_json", row.GuardResultsJSON)
	if len(guards) != 1 {
		t.Fatalf("guard_results_json length = %d, want 1: %s", len(guards), row.GuardResultsJSON)
	}
	if guards[0]["guard"] != "account_unavailable" || guards[0]["verdict"] != "reject" {
		t.Fatalf("guard_results_json = %#v, want account_unavailable reject", guards)
	}
	decodeDecisionAuditObject(t, "score_json", row.ScoreJSON)
	decodeDecisionAuditObject(t, "pds_decision_json", row.PDSDecisionJSON)
	decodeDecisionAuditObject(t, "learning_context_json", row.LearningContextJSON)
}

func TestRunTickBackfillsDecisionAuditCreatedTaskID(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}

	task := fixture.RequireSingleTask(ctx)
	row := fixture.RequireSingleDecisionAudit(ctx)
	if !row.Selected {
		t.Fatal("accepted candidate audit selected = false, want true")
	}
	if row.RejectionReason != nil {
		t.Fatalf("accepted decision audit rejection_reason = %v, want nil", *row.RejectionReason)
	}
	if row.CreatedTaskID == nil || *row.CreatedTaskID != task.ID {
		t.Fatalf("created_task_id = %#v, want %s", row.CreatedTaskID, task.ID)
	}
	if row.TargetAccountID == nil || *row.TargetAccountID != fixture.AccountID {
		t.Fatalf("target_account_id = %#v, want %s", row.TargetAccountID, fixture.AccountID)
	}
	if row.CandidateSource != SourceLaneSeed {
		t.Fatalf("candidate source = %q, want %q", row.CandidateSource, SourceLaneSeed)
	}
	guards := decodeDecisionAuditArray(t, "guard_results_json", row.GuardResultsJSON)
	if len(guards) != 0 {
		t.Fatalf("guard_results_json length = %d, want 0: %s", len(guards), row.GuardResultsJSON)
	}
	score := decodeDecisionAuditObject(t, "score_json", row.ScoreJSON)
	if score["source_kind"] != SourceLaneSeed {
		t.Fatalf("score_json source_kind = %#v, want %q", score["source_kind"], SourceLaneSeed)
	}
	pdsDecision := decodeDecisionAuditObject(t, "pds_decision_json", row.PDSDecisionJSON)
	if pdsDecision["verdict"] != "allow" || pdsDecision["decision_id"] != "allow" {
		t.Fatalf("pds_decision_json = %#v, want verdict and decision_id", pdsDecision)
	}
	decodeDecisionAuditObject(t, "learning_context_json", row.LearningContextJSON)
}

func TestRunTickEnqueuesPDSOutageAlertWhenCandidateDecisionUsesFailPolicy(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{
		Verdict: "allow",
		Metadata: map[string]any{
			"warning":     "pds_unavailable",
			"fail_policy": "allow",
		},
	})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}

	var alertKind string
	var severity string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT payload_json ->> 'kind', payload_json ->> 'severity'
		FROM channel_ops_queue_items
		WHERE channel_profile_id = $1::uuid
		  AND kind = $2
		  AND payload_json ->> 'kind' = 'pds_outage'
	`, fixture.ChannelID, QueueSendAlert).Scan(&alertKind, &severity); err != nil {
		t.Fatalf("select pds outage alert: %v", err)
	}
	if alertKind != "pds_outage" || severity != "warning" {
		t.Fatalf("alert kind/severity = %s/%s", alertKind, severity)
	}
}

func TestRunTickConvertsDiscoverySignalCandidate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	signalID := testUUID(t, "discovery-signal")
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	fixture.InsertDiscoverySignal(ctx, signalID)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}

	task := fixture.RequireSingleTask(ctx)
	if task.Source != SourceTrendYT {
		t.Fatalf("task source = %q, want %q", task.Source, SourceTrendYT)
	}
	if task.DiscoverySignalID == nil || *task.DiscoverySignalID != signalID {
		t.Fatalf("discovery_signal_id = %#v, want %s", task.DiscoverySignalID, signalID)
	}
	if task.ManualSeedID != nil {
		t.Fatalf("manual_seed_id = %#v, want nil", *task.ManualSeedID)
	}
	if got := task.RationaleJSON["discovery_signal_id"]; got != signalID {
		t.Fatalf("rationale discovery_signal_id = %#v, want %s", got, signalID)
	}
	row := fixture.RequireSingleDecisionAudit(ctx)
	if row.CandidateSource != SourceTrendYT {
		t.Fatalf("decision candidate_source = %q, want %q", row.CandidateSource, SourceTrendYT)
	}
	if row.CreatedTaskID == nil || *row.CreatedTaskID != task.ID {
		t.Fatalf("decision created_task_id = %#v, want %s", row.CreatedTaskID, task.ID)
	}
	var status string
	var convertedTaskID string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, converted_task_id::text
		FROM discovery_signals
		WHERE id = $1::uuid
	`, signalID).Scan(&status, &convertedTaskID); err != nil {
		t.Fatalf("select discovery signal: %v", err)
	}
	if status != "converted" || convertedTaskID != task.ID {
		t.Fatalf("discovery signal status/task = %s/%s, want converted/%s", status, convertedTaskID, task.ID)
	}
}

func TestListActiveDiscoverySignalsFiltersSourceAndCapsPerLane(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := fixture.Store.Now().UTC()
	var lowestID string
	for i := 0; i < 51; i++ {
		id := uuid.NewString()
		if i == 0 {
			lowestID = id
		}
		_, err := fixture.Store.Pool.Exec(ctx, `
			INSERT INTO discovery_signals (
				id, channel_profile_id, topic_lane_id, source, source_url, source_external_id,
				title, summary, keywords_json, observed_at, expires_at, trend_score, novelty_score,
				raw_json, status, created_at, updated_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3::uuid, 'youtube_search', '', $4,
				$5, '', '[]'::json, $6::timestamptz, $7::timestamptz, $8, 0,
				'{}'::json, 'active', $6::timestamp, $6::timestamp
			)
		`, id, fixture.ChannelID, fixture.LaneID, fmt.Sprintf("yt-%02d", i), fmt.Sprintf("trend-%02d", i), now, now.Add(24*time.Hour), float64(100+i))
		if err != nil {
			t.Fatalf("insert discovery signal %d: %v", i, err)
		}
	}
	_, err := fixture.Store.Pool.Exec(ctx, `
		INSERT INTO discovery_signals (
			id, channel_profile_id, topic_lane_id, source, source_external_id,
			title, summary, keywords_json, observed_at, expires_at, trend_score, novelty_score,
			raw_json, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'x_search', 'x-1',
			'x trend', '', '[]'::json, $4::timestamptz, $5::timestamptz, 99999, 0,
			'{}'::json, 'active', $4::timestamp, $4::timestamp
		)
	`, uuid.NewString(), fixture.ChannelID, fixture.LaneID, now, now.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("insert non-youtube discovery signal: %v", err)
	}

	signals, err := fixture.Store.ListActiveDiscoverySignals(ctx, fixture.ChannelID, now)
	if err != nil {
		t.Fatalf("ListActiveDiscoverySignals: %v", err)
	}
	if len(signals) != 50 {
		t.Fatalf("signal count = %d, want 50", len(signals))
	}
	for _, signal := range signals {
		if signal.Source != "youtube_search" {
			t.Fatalf("source = %q, want youtube_search", signal.Source)
		}
		if signal.ID == lowestID {
			t.Fatalf("lowest ranked signal %s should have been capped out", lowestID)
		}
	}
}

func TestAttachDecisionAuditTaskErrorsWhenDecisionAuditMissing(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	if err := fixture.Store.AttachDecisionAuditTask(ctx, testUUID(t, "missing-audit"), testUUID(t, "task")); err == nil {
		t.Fatal("AttachDecisionAuditTask returned nil for a missing audit row")
	}
}

type decisionAuditFixtureRow struct {
	TickAuditID         string
	CandidateID         string
	CandidateSource     string
	TopicLaneID         *string
	LaneFormatID        *string
	TargetAccountID     *string
	ScoreJSON           []byte
	GuardResultsJSON    []byte
	PDSDecisionJSON     []byte
	LearningContextJSON []byte
	Selected            bool
	RejectionReason     *string
	CreatedTaskID       *string
	CreatedAt           time.Time
}

type ChannelOpsFixture struct {
	T                    *testing.T
	Store                *Store
	ChannelID            string
	LaneID               string
	FormatID             string
	AccountID            string
	AdditionalChannelIDs []string
}

func NewChannelOpsFixture(t *testing.T) *ChannelOpsFixture {
	t.Helper()
	cfg := LoadConfig()
	store, err := OpenStore(context.Background(), cfg.DatabaseURL)
	if err != nil {
		t.Skipf("ChannelOps integration test requires reachable DATABASE_URL %q: %v", cfg.DatabaseURL, err)
	}
	store.Now = func() time.Time { return time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC) }
	fixture := &ChannelOpsFixture{
		T:         t,
		Store:     store,
		ChannelID: testUUID(t, "channel"),
		LaneID:    testUUID(t, "lane"),
		FormatID:  testUUID(t, "format"),
		AccountID: testUUID(t, "account"),
	}
	fixture.cleanup(context.Background())
	return fixture
}

func (f *ChannelOpsFixture) Close(ctx context.Context) {
	f.cleanup(ctx)
	f.Store.Close()
}

func (f *ChannelOpsFixture) InsertChannelWithLaneAccountSeed(ctx context.Context) {
	f.T.Helper()
	now := f.Store.Now().UTC()
	_, err := f.Store.Pool.Exec(ctx, `
		INSERT INTO channel_profiles (
			id, operator_id, name, positioning, language, default_aspect_ratio,
			risk_policy_json, content_mix_policy_json, cadence_policy_json,
			alert_policy_json, enabled, dry_run, halted_at, halt_reason,
			config_version, tick_interval_minutes, created_at, updated_at
		)
		VALUES (
			$1::uuid, NULL, 'ChannelOps fake live test', 'integration fixture',
			'en', '9:16', '{}'::json, '{}'::json, '{}'::json, '{}'::json,
			TRUE, FALSE, NULL, NULL, 1, 60, $2, $2
		)
	`, f.ChannelID, now)
	if err != nil {
		f.T.Fatalf("insert channel_profiles: %v", err)
	}

	_, err = f.Store.Pool.Exec(ctx, `
		INSERT INTO topic_lanes (
			id, channel_profile_id, name, description, weight, learned_weight,
			keywords_json, negative_keywords_json, min_posts_per_week,
			max_posts_per_day, max_consecutive_streak, cooldown_after_post_minutes,
			enabled, paused_until, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'Go live lane', 'fake integration topic', 1.0, NULL,
			'["go", "channelops"]'::json, '[]'::json, 1, 1, 3, 0,
			TRUE, NULL, $3, $3
		)
	`, f.LaneID, f.ChannelID, now)
	if err != nil {
		f.T.Fatalf("insert topic_lanes: %v", err)
	}

	_, err = f.Store.Pool.Exec(ctx, `
		INSERT INTO lane_format_matrix (
			id, topic_lane_id, format_key, enabled, weight, target_duration_sec,
			template_pool_json, default_publish_visibility, source_platforms_json,
			created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'short', TRUE, 1.0, 45,
			'["channelops-live"]'::json, 'unlisted', '[]'::json, $3, $3
		)
	`, f.FormatID, f.LaneID, now)
	if err != nil {
		f.T.Fatalf("insert lane_format_matrix: %v", err)
	}

	_, err = f.Store.Pool.Exec(ctx, `
		INSERT INTO publishing_accounts (
			id, channel_profile_id, platform, account_label, platform_account_id,
			credential_ref, platform_specific_config_json, default_privacy,
			external_asset_auto_publish, enabled, paused_until, last_token_check_at,
			last_token_check_status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'youtube', 'fixture account', 'fixture-youtube',
			'fixture', '{}'::json, 'unlisted', TRUE, TRUE, NULL, NULL, NULL, $3, $3
		)
	`, f.AccountID, f.ChannelID, now)
	if err != nil {
		f.T.Fatalf("insert publishing_accounts: %v", err)
	}

	_, err = f.Store.Pool.Exec(ctx, `
		INSERT INTO autoflow_plans (
			id, prompt, request_json, intent_json, template_id, pipeline_definition,
			candidates_json, metadata_json, rights_json, validation_json, status,
			execution_revision, review_approved_at, approved_revision_hash,
			approved_revision, created_at, updated_at
		) VALUES (
			'00000000-0000-0000-0000-000000000101'::uuid,
			'ChannelOps fake approved plan', '{}'::json, '{}'::json, 'channelops-live',
			'{"nodes": [], "edges": []}'::json, '[]'::json, '{}'::json,
			'{"status": "allowed"}'::json, '{"valid": true}'::json, 'review_approved',
			1, $1::timestamptz,
			'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
			1, $1::timestamp, $1::timestamp
		)
	`, now)
	if err != nil {
		f.T.Fatalf("insert fake approved AutoFlow plan: %v", err)
	}
}

func (f *ChannelOpsFixture) SetTickInterval(ctx context.Context, intervalMinutes int) {
	f.T.Helper()
	_, err := f.Store.Pool.Exec(ctx, `
		UPDATE channel_profiles
		SET tick_interval_minutes = $2
		WHERE id = $1::uuid
	`, f.ChannelID, intervalMinutes)
	if err != nil {
		f.T.Fatalf("set tick interval: %v", err)
	}
}

func (f *ChannelOpsFixture) SetDryRun(ctx context.Context, dryRun bool) {
	f.T.Helper()
	_, err := f.Store.Pool.Exec(ctx, `
		UPDATE channel_profiles
		SET dry_run = $2
		WHERE id = $1::uuid
	`, f.ChannelID, dryRun)
	if err != nil {
		f.T.Fatalf("set dry_run: %v", err)
	}
}

func (f *ChannelOpsFixture) InsertDiscoverySignal(ctx context.Context, signalID string) {
	f.T.Helper()
	now := f.Store.Now().UTC()
	_, err := f.Store.Pool.Exec(ctx, `
		INSERT INTO discovery_signals (
			id, channel_profile_id, topic_lane_id, source, source_url, source_external_id,
			title, summary, keywords_json, observed_at, expires_at, trend_score, novelty_score,
			raw_json, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'youtube_search', 'https://youtu.be/trend-1',
			'trend-1', 'Trend title', 'Trend summary', '["trend"]'::json, $4::timestamptz,
			$5::timestamptz, 2500, 0, '{"video_id": "trend-1"}'::json, 'active',
			$4::timestamp, $4::timestamp
		)
	`, signalID, f.ChannelID, f.LaneID, now, now.Add(24*time.Hour))
	if err != nil {
		f.T.Fatalf("insert discovery signal: %v", err)
	}
}

func (f *ChannelOpsFixture) SetAccountEnabled(ctx context.Context, enabled bool) {
	f.T.Helper()
	_, err := f.Store.Pool.Exec(ctx, `
		UPDATE publishing_accounts
		SET enabled = $2
		WHERE id = $1::uuid
	`, f.AccountID, enabled)
	if err != nil {
		f.T.Fatalf("set account enabled: %v", err)
	}
}

func (f *ChannelOpsFixture) HandlerService(decision PDSDecision) HandlerService {
	return HandlerService{
		Store:    f.Store,
		PDS:      fakePDS{decision: decision},
		AutoFlow: fakeAutoFlow{},
		YouTube:  fakeYouTube{},
		Config:   Config{MetricsPollMaxAttempts: 3},
	}
}

func (f *ChannelOpsFixture) ProcessAllQueueItems(ctx context.Context, handler HandlerService) {
	f.T.Helper()
	for i := 0; i < 20; i++ {
		f.makeQueuedItemsReady(ctx)
		item, err := f.Store.ClaimNextForChannelAndKinds(ctx, "channelops-integration-test", f.ChannelID, handler.ClaimableKinds())
		if err != nil {
			f.T.Fatalf("ClaimNextForChannelAndKinds: %v", err)
		}
		if item == nil {
			return
		}
		if err := handler.Handle(ctx, *item); err != nil {
			_ = f.Store.MarkQueueFailedOrRetry(ctx, *item, err.Error())
			f.T.Fatalf("Handle %s: %v", item.Kind, err)
		}
		if err := f.Store.MarkQueueDone(ctx, *item); err != nil {
			f.T.Fatalf("MarkQueueDone %s: %v", item.ID, err)
		}
	}
	f.T.Fatal("queue did not drain within 20 items")
}

func (f *ChannelOpsFixture) ProcessUntilQueueKind(ctx context.Context, handler HandlerService, kind string) QueueItemRow {
	f.T.Helper()
	for i := 0; i < 20; i++ {
		f.makeQueuedItemsReady(ctx)
		item, err := f.Store.ClaimNextForChannelAndKinds(ctx, "channelops-integration-test", f.ChannelID, handler.ClaimableKinds())
		if err != nil {
			f.T.Fatalf("ClaimNextForChannelAndKinds: %v", err)
		}
		if item == nil {
			f.T.Fatalf("queue drained before %s", kind)
		}
		if item.Kind == kind {
			return *item
		}
		if err := handler.Handle(ctx, *item); err != nil {
			_ = f.Store.MarkQueueFailedOrRetry(ctx, *item, err.Error())
			f.T.Fatalf("Handle %s: %v", item.Kind, err)
		}
		if err := f.Store.MarkQueueDone(ctx, *item); err != nil {
			f.T.Fatalf("MarkQueueDone %s: %v", item.ID, err)
		}
	}
	f.T.Fatalf("queue did not reach %s within 20 items", kind)
	return QueueItemRow{}
}

func (f *ChannelOpsFixture) RequireSingleTask(ctx context.Context) ProductionTaskRow {
	f.T.Helper()
	var taskID string
	err := f.Store.Pool.QueryRow(ctx, `
		SELECT id
		FROM production_tasks
		WHERE channel_profile_id = $1::uuid
	`, f.ChannelID).Scan(&taskID)
	if err != nil {
		f.T.Fatalf("select production task: %v", err)
	}
	var extra string
	err = f.Store.Pool.QueryRow(ctx, `
		SELECT id
		FROM production_tasks
		WHERE channel_profile_id = $1::uuid AND id <> $2::uuid
		LIMIT 1
	`, f.ChannelID, taskID).Scan(&extra)
	if err == nil {
		f.T.Fatalf("expected one production task, found extra task %s", extra)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		f.T.Fatalf("check single production task: %v", err)
	}
	task, err := f.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		f.T.Fatalf("GetProductionTask: %v", err)
	}
	return task
}

func (f *ChannelOpsFixture) CountProductionTasks(ctx context.Context) int {
	f.T.Helper()
	var count int
	if err := f.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM production_tasks
		WHERE channel_profile_id = $1::uuid
	`, f.ChannelID).Scan(&count); err != nil {
		f.T.Fatalf("count production tasks: %v", err)
	}
	return count
}

func (f *ChannelOpsFixture) RequireSingleDecisionAudit(ctx context.Context) decisionAuditFixtureRow {
	f.T.Helper()
	var row decisionAuditFixtureRow
	err := f.Store.Pool.QueryRow(ctx, `
		SELECT tick_audit_id::text, candidate_id, candidate_source,
		       topic_lane_id::text, lane_format_id::text, target_account_id::text,
		       score_json, guard_results_json, pds_decision_json, learning_context_json,
		       selected, rejection_reason, created_task_id::text, created_at
		FROM decision_audit_entries
		WHERE channel_profile_id = $1::uuid
	`, f.ChannelID).Scan(
		&row.TickAuditID,
		&row.CandidateID,
		&row.CandidateSource,
		&row.TopicLaneID,
		&row.LaneFormatID,
		&row.TargetAccountID,
		&row.ScoreJSON,
		&row.GuardResultsJSON,
		&row.PDSDecisionJSON,
		&row.LearningContextJSON,
		&row.Selected,
		&row.RejectionReason,
		&row.CreatedTaskID,
		&row.CreatedAt,
	)
	if err != nil {
		f.T.Fatalf("select decision audit: %v", err)
	}
	var count int
	if err := f.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM decision_audit_entries
		WHERE channel_profile_id = $1::uuid
	`, f.ChannelID).Scan(&count); err != nil {
		f.T.Fatalf("count decision audit rows: %v", err)
	}
	if count != 1 {
		f.T.Fatalf("decision audit row count = %d, want 1", count)
	}
	if row.TickAuditID == "" {
		f.T.Fatal("decision audit tick_audit_id is empty")
	}
	if row.CandidateID == "" {
		f.T.Fatal("decision audit candidate_id is empty")
	}
	if row.TopicLaneID == nil || *row.TopicLaneID != f.LaneID {
		f.T.Fatalf("topic_lane_id = %#v, want %s", row.TopicLaneID, f.LaneID)
	}
	if row.LaneFormatID == nil || *row.LaneFormatID != f.FormatID {
		f.T.Fatalf("lane_format_id = %#v, want %s", row.LaneFormatID, f.FormatID)
	}
	if row.CreatedAt.IsZero() {
		f.T.Fatal("decision audit created_at is zero")
	}
	return row
}

func (f *ChannelOpsFixture) CountRows(ctx context.Context, table string) int {
	f.T.Helper()
	queries := map[string]string{
		"publication_records": `
			SELECT count(*)
			FROM publication_records p
			JOIN production_tasks t ON t.id = p.production_task_id
			WHERE t.channel_profile_id = $1::uuid
		`,
		"feedback_snapshots": `
			SELECT count(*)
			FROM feedback_snapshots f
			JOIN publication_records p ON p.id = f.publication_id
			JOIN production_tasks t ON t.id = p.production_task_id
			WHERE t.channel_profile_id = $1::uuid
		`,
		"material_usage_ledger": `
			SELECT count(*)
			FROM material_usage_ledger
			WHERE channel_profile_id = $1::uuid
		`,
		"takedown_events": `
			SELECT count(*)
			FROM takedown_events e
			JOIN publication_records p ON p.id = e.publication_id
			JOIN production_tasks t ON t.id = p.production_task_id
			WHERE t.channel_profile_id = $1::uuid
		`,
	}
	query, ok := queries[table]
	if !ok {
		f.T.Fatalf("unsupported CountRows table %q", table)
	}
	var count int
	if err := f.Store.Pool.QueryRow(ctx, query, f.ChannelID).Scan(&count); err != nil {
		f.T.Fatalf("count %s: %v", table, err)
	}
	return count
}

func (f *ChannelOpsFixture) cleanup(ctx context.Context) {
	_, _ = f.Store.Pool.Exec(ctx, `
		WITH fixture_tasks AS (
			SELECT id FROM production_tasks WHERE channel_profile_id = $1::uuid
		), fixture_publications AS (
			SELECT id FROM publication_records WHERE production_task_id IN (SELECT id FROM fixture_tasks)
		), deleted_feedback AS (
			DELETE FROM feedback_snapshots WHERE publication_id IN (SELECT id FROM fixture_publications)
		), deleted_takedowns AS (
			DELETE FROM takedown_events WHERE publication_id IN (SELECT id FROM fixture_publications)
		), deleted_ledger AS (
			DELETE FROM material_usage_ledger
			WHERE channel_profile_id = $1::uuid OR publication_id IN (SELECT id FROM fixture_publications)
		), deleted_publications AS (
			DELETE FROM publication_records WHERE id IN (SELECT id FROM fixture_publications)
		), deleted_queue AS (
			DELETE FROM channel_ops_queue_items
			WHERE channel_profile_id = $1::uuid
			   OR (payload_json ->> 'channel_id') = $1::text
			   OR (payload_json ->> 'production_task_id') IN (SELECT id::text FROM fixture_tasks)
			   OR (payload_json ->> 'publication_id') IN (SELECT id::text FROM fixture_publications)
		), deleted_decisions AS (
			DELETE FROM decision_audit_entries WHERE channel_profile_id = $1::uuid
		), deleted_discovery AS (
			DELETE FROM discovery_signals WHERE channel_profile_id = $1::uuid
		), deleted_learning AS (
			DELETE FROM learning_states WHERE channel_profile_id = $1::uuid
		), deleted_audits AS (
			DELETE FROM agent_tick_audits WHERE channel_profile_id = $1::uuid
		), deleted_scheduler AS (
			DELETE FROM internal_scheduler_runs WHERE channel_profile_id = $1::uuid
		)
		DELETE FROM channel_profiles WHERE id = $1::uuid
	`, f.ChannelID)
	_, _ = f.Store.Pool.Exec(ctx, `
		DELETE FROM autoflow_plans
		WHERE id = '00000000-0000-0000-0000-000000000101'::uuid
	`)
	for _, channelID := range f.AdditionalChannelIDs {
		_, _ = f.Store.Pool.Exec(ctx, `DELETE FROM channel_profiles WHERE id = $1::uuid`, channelID)
	}
}

func testUUID(t *testing.T, label string) string {
	t.Helper()
	id, err := uuid.NewRandom()
	if err != nil {
		t.Fatalf("generate %s uuid: %v", label, err)
	}
	return id.String()
}

func (f *ChannelOpsFixture) makeQueuedItemsReady(ctx context.Context) {
	f.T.Helper()
	_, err := f.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET run_after = NOW()
		WHERE channel_profile_id = $1::uuid AND status = $2
	`, f.ChannelID, QueueStatusQueued)
	if err != nil {
		f.T.Fatalf("make queued items ready: %v", err)
	}
}

func decodeDecisionAuditObject(t *testing.T, field string, raw []byte) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode %s: %v; raw=%s", field, err, raw)
	}
	if value == nil {
		t.Fatalf("%s decoded as nil object; raw=%s", field, raw)
	}
	return value
}

func decodeDecisionAuditArray(t *testing.T, field string, raw []byte) []map[string]any {
	t.Helper()
	var value []map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("decode %s: %v; raw=%s", field, err, raw)
	}
	if value == nil {
		t.Fatalf("%s decoded as nil array; raw=%s", field, raw)
	}
	return value
}

type fakePDS struct {
	decision PDSDecision
}

func (f fakePDS) Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error) {
	decision := f.decision
	if decision.Verdict == "" {
		decision.Verdict = "allow"
	}
	if decision.DecisionID == "" {
		decision.DecisionID = "allow"
	}
	return decision, nil
}

type fakeAutoFlow struct {
	executeObservation AutoFlowExecuteObservation
	getJobObservation  AutoFlowJobObservation
	getJobErr          error
}

type externalCallRecorder struct {
	plan              atomic.Int32
	approve           atomic.Int32
	execute           atomic.Int32
	observe           atomic.Int32
	pds               atomic.Int32
	accountHealth     atomic.Int32
	schedulePublish   atomic.Int32
	publicationStatus atomic.Int32
	fetchMetrics      atomic.Int32
}

func (r *externalCallRecorder) total() int32 {
	return r.plan.Load() + r.approve.Load() + r.execute.Load() + r.observe.Load() +
		r.pds.Load() + r.accountHealth.Load() + r.schedulePublish.Load() +
		r.publicationStatus.Load() + r.fetchMetrics.Load()
}

type recordingAutoFlow struct {
	fakeAutoFlow
	recorder *externalCallRecorder
}

func (f *recordingAutoFlow) PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error) {
	f.recorder.plan.Add(1)
	return f.fakeAutoFlow.PlanTask(ctx, task, request)
}

func (f *recordingAutoFlow) ApprovePlan(ctx context.Context, planID string, evidence map[string]any) (AutoFlowApprovalObservation, error) {
	f.recorder.approve.Add(1)
	return f.fakeAutoFlow.ApprovePlan(ctx, planID, evidence)
}

func (f *recordingAutoFlow) ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error) {
	f.recorder.execute.Add(1)
	return f.fakeAutoFlow.ExecuteTask(ctx, task, request)
}

func (f *recordingAutoFlow) GetJob(ctx context.Context, runID string, jobID string) (AutoFlowJobObservation, error) {
	f.recorder.observe.Add(1)
	return f.fakeAutoFlow.GetJob(ctx, runID, jobID)
}

type recordingPDS struct {
	recorder *externalCallRecorder
}

func (f *recordingPDS) Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error) {
	f.recorder.pds.Add(1)
	return PDSDecision{Verdict: "allow", DecisionID: "allow"}, nil
}

func (fakeAutoFlow) PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error) {
	return AutoFlowPlanObservation{
		PlanID:          "00000000-0000-0000-0000-000000000101",
		UploadNodeCount: 1,
		PlanPayload: map[string]any{
			"clips": []any{map[string]any{"material_id": "mat-1", "asset_id": "00000000-0000-0000-0000-00000000a501"}},
		},
	}, nil
}

func (fakeAutoFlow) ApprovePlan(ctx context.Context, planID string, evidence map[string]any) (AutoFlowApprovalObservation, error) {
	return AutoFlowApprovalObservation{
		PlanID:               planID,
		ApprovedRevisionHash: strings.Repeat("a", 64),
		ApprovedRevision:     1,
	}, nil
}

func (f fakeAutoFlow) ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error) {
	if f.executeObservation.Status != "" || f.executeObservation.ErrorMessage != "" {
		return f.executeObservation, nil
	}
	return AutoFlowExecuteObservation{
		RunID:  "00000000-0000-0000-0000-000000000201",
		JobID:  "00000000-0000-0000-0000-000000000301",
		Status: "running",
	}, nil
}

func (f fakeAutoFlow) GetJob(ctx context.Context, runID string, jobID string) (AutoFlowJobObservation, error) {
	if f.getJobErr != nil {
		return AutoFlowJobObservation{}, f.getJobErr
	}
	if f.getJobObservation.Status != "" {
		return f.getJobObservation, nil
	}
	return AutoFlowJobObservation{
		Status:         "succeeded",
		RunPayload:     map[string]any{"rendered": true},
		UploadMetadata: map[string]any{"video_id": "yt-1"},
	}, nil
}

type fakeYouTube struct{}

type recordingYouTube struct {
	fakeYouTube
	recorder *externalCallRecorder
}

func (f *recordingYouTube) AccountHealth(ctx context.Context, accountID string) (YouTubeAccountHealth, error) {
	f.recorder.accountHealth.Add(1)
	return f.fakeYouTube.AccountHealth(ctx, accountID)
}

func (f *recordingYouTube) SchedulePublish(ctx context.Context, videoID string, scheduledAt time.Time, privacy string) error {
	f.recorder.schedulePublish.Add(1)
	return f.fakeYouTube.SchedulePublish(ctx, videoID, scheduledAt, privacy)
}

func (f *recordingYouTube) PublicationStatus(ctx context.Context, videoID string) (YouTubePublicationStatus, error) {
	f.recorder.publicationStatus.Add(1)
	return f.fakeYouTube.PublicationStatus(ctx, videoID)
}

func (f *recordingYouTube) FetchMetrics(ctx context.Context, videoID string) (map[string]any, error) {
	f.recorder.fetchMetrics.Add(1)
	return f.fakeYouTube.FetchMetrics(ctx, videoID)
}

type blockingPromotionYouTube struct {
	fakeYouTube
	started chan struct{}
	release <-chan struct{}
	calls   atomic.Int32
}

func (f *blockingPromotionYouTube) SchedulePublish(ctx context.Context, videoID string, scheduledAt time.Time, privacy string) error {
	f.calls.Add(1)
	select {
	case f.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.release:
		return f.fakeYouTube.SchedulePublish(ctx, videoID, scheduledAt, privacy)
	}
}

func (fakeYouTube) AccountHealth(ctx context.Context, accountID string) (YouTubeAccountHealth, error) {
	return YouTubeAccountHealth{Authenticated: true, QuotaRemaining: 1000, Raw: map[string]any{"ok": true}}, nil
}

func (fakeYouTube) SchedulePublish(ctx context.Context, videoID string, scheduledAt time.Time, privacy string) error {
	if strings.TrimSpace(videoID) == "" {
		return fmt.Errorf("videoID is required")
	}
	if privacy != "unlisted" && privacy != "private" {
		return fmt.Errorf("unexpected scheduled privacy %q", privacy)
	}
	return nil
}

func (fakeYouTube) PublicationStatus(ctx context.Context, videoID string) (YouTubePublicationStatus, error) {
	return YouTubePublicationStatus{
		VideoID:       videoID,
		PublishStatus: "scheduled",
		Privacy:       "unlisted",
		Permalink:     "https://youtu.be/" + videoID,
		Raw:           map[string]any{"status": "scheduled"},
	}, nil
}

func (fakeYouTube) FetchMetrics(ctx context.Context, videoID string) (map[string]any, error) {
	return map[string]any{"views": 10, "likes": 2, "impressions": 100}, nil
}

func applyFenceTestQuarantine(ctx context.Context, tx pgx.Tx, channelID string) error {
	if _, err := tx.Exec(ctx, `
		UPDATE channel_profiles
		SET halted_at = NOW(), halt_reason = 'fence_test_quarantine'
		WHERE id = $1::uuid
	`, channelID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE production_tasks
		SET state = $2, blocked_by_guard = 'fence_test_quarantine',
		    failure_reason = 'fence_test_quarantine'
		WHERE channel_profile_id = $1::uuid
		  AND state NOT IN ('failed', 'rejected', 'cancelled', 'published', 'measured')
	`, channelID, TaskHeld); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, last_error = 'fence_test_quarantine',
		    dead_letter_at = NOW(), locked_by = NULL, locked_at = NULL
		WHERE channel_profile_id = $1::uuid
		  AND status IN ($3, $4)
	`, channelID, QueueStatusDeadLettered, QueueStatusQueued, QueueStatusRunning)
	return err
}

func insertAuthorityTestChannel(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	label string,
	halted bool,
) string {
	t.Helper()
	channelID := testUUID(t, "authority-"+label)
	var haltedAt *time.Time
	if halted {
		value := fixture.Store.Now()
		haltedAt = &value
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		INSERT INTO channel_profiles (
			id, name, positioning, language, default_aspect_ratio, risk_policy_json,
			content_mix_policy_json, cadence_policy_json, alert_policy_json, enabled,
			dry_run, halted_at, halt_reason, config_version, tick_interval_minutes,
			created_at, updated_at
		) VALUES (
			$1::uuid, $2, '', 'en', '9:16', '{}'::json, '{}'::json, '{}'::json,
			'{}'::json, TRUE, FALSE, $3, CASE WHEN $3::timestamptz IS NULL THEN NULL ELSE 'test halt' END,
			1, 60, NOW(), NOW()
		)
	`, channelID, label, haltedAt); err != nil {
		t.Fatalf("insert authority test channel: %v", err)
	}
	fixture.AdditionalChannelIDs = append(fixture.AdditionalChannelIDs, channelID)
	return channelID
}

func alertQueuePayload(channelID string) map[string]any {
	payload := map[string]any{
		"type":        "authority_test",
		"severity":    "warning",
		"resource_id": "authority-test",
		"message":     "queue authority test alert",
	}
	if channelID != "" {
		payload["channel_id"] = channelID
	}
	return payload
}

func setPromotionMetadataForAuthorityTest(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	promote *QueueItemRow,
	metadata string,
) {
	t.Helper()
	var storedChannelID *string
	if metadata == "mismatched" {
		mismatchedChannelID := testUUID(t, "mismatched-channel")
		if _, err := fixture.Store.Pool.Exec(ctx, `
			INSERT INTO channel_profiles (
				id, name, positioning, language, default_aspect_ratio, risk_policy_json,
				content_mix_policy_json, cadence_policy_json, alert_policy_json, enabled,
				dry_run, config_version, tick_interval_minutes, created_at, updated_at
			) VALUES (
				$1::uuid, 'mismatched queue metadata', '', 'en', '9:16', '{}'::json,
				'{}'::json, '{}'::json, '{}'::json, TRUE, FALSE, 1, 60, NOW(), NOW()
			)
		`, mismatchedChannelID); err != nil {
			t.Fatalf("insert mismatched channel: %v", err)
		}
		fixture.AdditionalChannelIDs = append(fixture.AdditionalChannelIDs, mismatchedChannelID)
		storedChannelID = &mismatchedChannelID
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items SET channel_profile_id = $2::uuid WHERE id = $1::uuid
	`, promote.ID, storedChannelID); err != nil {
		t.Fatalf("update promotion metadata: %v", err)
	}
	promote.ChannelProfileID = storedChannelID
}

func setHumanReviewEvidenceForTest(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	taskID string,
	evidenceCase string,
	publicationID string,
	targetVisibility string,
) {
	t.Helper()
	planID := "00000000-0000-0000-0000-000000000101"
	approvedAt := fixture.Store.Now().UTC().Add(-time.Hour)
	planStatus := "review_approved"
	planToken := approvedAt
	evidencePlanID := planID
	approvedRevisionHash := strings.Repeat("a", 64)
	evidenceRevisionHash := approvedRevisionHash
	approvedRevision := int64(1)
	if evidenceCase == "stale" {
		planToken = approvedAt.Add(-time.Second)
	}
	if evidenceCase == "mismatched" {
		evidencePlanID = "00000000-0000-0000-0000-000000000102"
	}
	if evidenceCase == "revision_mismatched" {
		evidenceRevisionHash = strings.Repeat("b", 64)
	}
	if evidenceCase == "rejected" {
		planStatus = "rejected"
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		INSERT INTO autoflow_plans (
			id, prompt, request_json, intent_json, template_id, pipeline_definition,
			candidates_json, metadata_json, rights_json, validation_json, status,
			execution_revision, review_approved_at, approved_revision_hash,
			approved_revision, created_at, updated_at
		) VALUES (
			$1::uuid, 'review fixture', '{}'::json, '{}'::json, 'material_library_remix',
			'{"nodes": [], "edges": []}'::json, '[]'::json, '{}'::json,
			'{"status": "review_required"}'::json, '{"valid": true}'::json, $2,
			1, $3::timestamptz, $4, $5, NOW(), NOW()
		)
		ON CONFLICT (id) DO UPDATE
		SET status = EXCLUDED.status,
		    review_approved_at = EXCLUDED.review_approved_at,
		    approved_revision_hash = EXCLUDED.approved_revision_hash,
		    approved_revision = autoflow_plans.execution_revision
	`, planID, planStatus, approvedAt, approvedRevisionHash, approvedRevision); err != nil {
		t.Fatalf("upsert AutoFlow plan: %v", err)
	}
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT execution_revision
		FROM autoflow_plans
		WHERE id = $1::uuid
	`, planID).Scan(&approvedRevision); err != nil {
		t.Fatalf("read AutoFlow execution revision: %v", err)
	}
	evidence := map[string]any{}
	if evidenceCase != "missing" && evidenceCase != "agent_only" {
		evidence["pre_upload"] = map[string]any{
			"kind":                        "human_review",
			"scope":                       "external_asset_pre_upload",
			"human_actor":                 "operator@example.com",
			"reviewed_at":                 planToken.Format(time.RFC3339Nano),
			"autoflow_plan_id":            evidencePlanID,
			"plan_review_approved_at":     planToken.Format(time.RFC3339Nano),
			"plan_approved_revision_hash": evidenceRevisionHash,
			"plan_approved_revision":      approvedRevision,
		}
	}
	if evidenceCase == "valid" && publicationID != "" {
		evidence["promotion"] = map[string]any{
			"kind":                        "human_review",
			"scope":                       "publication_promotion",
			"human_actor":                 "operator@example.com",
			"reviewed_at":                 fixture.Store.Now().UTC().Format(time.RFC3339Nano),
			"production_task_id":          taskID,
			"publication_id":              publicationID,
			"target_visibility":           targetVisibility,
			"autoflow_plan_id":            planID,
			"plan_review_approved_at":     approvedAt.Format(time.RFC3339Nano),
			"plan_approved_revision_hash": approvedRevisionHash,
			"plan_approved_revision":      approvedRevision,
		}
	}
	agentEvidence := map[string]any{}
	if evidenceCase == "agent_only" {
		agentEvidence = map[string]any{"approved_by": "channel_agent"}
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE production_tasks
		SET uses_external_assets = TRUE,
		    approval_mode = $2,
		    autoflow_plan_id = $3::uuid,
		    human_review_evidence_json = $4::json,
		    agent_approval_evidence_json = $5::json
		WHERE id = $1::uuid
	`, taskID, ApprovalHuman, planID, mustJSON(evidence), mustJSON(agentEvidence)); err != nil {
		t.Fatalf("set task review evidence: %v", err)
	}
}

func setAutomaticOwnedPlanAuthorityForTest(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	taskID string,
) {
	t.Helper()
	planID := "00000000-0000-0000-0000-000000000101"
	approvedRevisionHash := strings.Repeat("a", 64)
	approvedRevision := int64(1)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		INSERT INTO autoflow_plans (
			id, prompt, request_json, intent_json, template_id, pipeline_definition,
			candidates_json, metadata_json, rights_json, validation_json, status,
			execution_revision, review_approved_at, approved_revision_hash,
			approved_revision, created_at, updated_at
		) VALUES (
			$1::uuid, 'automatic owned fixture', '{}'::json, '{}'::json, 'channelops-live',
			'{"nodes": [], "edges": []}'::json, '[]'::json, '{}'::json,
			'{"status": "allowed"}'::json, '{"valid": true}'::json, 'review_approved',
			1, NOW(), $2, $3, NOW(), NOW()
		)
		ON CONFLICT (id) DO UPDATE
		SET status = 'review_approved',
			rights_json = '{"status": "allowed"}'::json,
			review_approved_at = NOW(),
			approved_revision_hash = $2,
			approved_revision = autoflow_plans.execution_revision
	`, planID, approvedRevisionHash, approvedRevision); err != nil {
		t.Fatalf("upsert automatic owned AutoFlow plan: %v", err)
	}
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT execution_revision FROM autoflow_plans WHERE id = $1::uuid
	`, planID).Scan(&approvedRevision); err != nil {
		t.Fatalf("read automatic owned execution revision: %v", err)
	}
	task, err := fixture.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("read automatic owned task authority: %v", err)
	}
	if task.AutoFlowPlanID == nil || *task.AutoFlowPlanID != planID ||
		task.AutoFlowApprovedRevisionHash == nil || *task.AutoFlowApprovedRevisionHash != approvedRevisionHash ||
		task.AutoFlowApprovedRevision == nil || *task.AutoFlowApprovedRevision != approvedRevision {
		t.Fatalf("automatic owned task did not preserve approval observation: %#v", task)
	}
}

func waitForChannelLockOrExternalCall(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	quarantinePID int,
	youtube *blockingPromotionYouTube,
) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-youtube.started:
			t.Fatal("promotion called YouTube before quarantine committed")
		default:
		}
		var waiting bool
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_locks waiter
				JOIN pg_locks blocker
				  ON blocker.locktype = 'transactionid'
				 AND blocker.transactionid = waiter.transactionid
				 AND blocker.granted
				WHERE waiter.locktype = 'transactionid'
				  AND NOT waiter.granted
				  AND blocker.pid = $1
			)
		`, quarantinePID).Scan(&waiting); err != nil {
			t.Fatalf("inspect channel lock waiter: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("promotion did not wait on quarantined channel lock")
}

func waitForPlanLockOrExternalCall(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	blockerPID int,
	youtube *blockingPromotionYouTube,
	probe testOperationProbe,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-youtube.started:
			t.Fatal("promotion called YouTube before rejected plan authority committed")
		default:
		}
		if done, err := probe(); done {
			t.Fatalf("promotion completed before waiting on plan authority: %v", err)
		}
		var waiting bool
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_locks waiter
				JOIN pg_locks blocker
				  ON blocker.locktype = 'transactionid'
				 AND blocker.transactionid = waiter.transactionid
				 AND blocker.granted
				WHERE waiter.locktype = 'transactionid'
				  AND NOT waiter.granted
				  AND blocker.pid = $1
			)
		`, blockerPID).Scan(&waiting); err != nil {
			t.Fatalf("inspect plan lock waiter: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("promotion did not wait on rejected plan authority")
}

func waitForDatabaseLock(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	pid int,
	probe testOperationProbe,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if done, err := probe(); done {
			t.Fatalf("database writer completed before expected lock: %v", err)
		}
		var waiting bool
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT COALESCE(wait_event_type = 'Lock', FALSE)
			FROM pg_stat_activity
			WHERE pid = $1
		`, pid).Scan(&waiting); err != nil {
			t.Fatalf("inspect database writer wait: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("database writer did not reach expected lock")
}

type testOperationProbe func() (bool, error)

func testOperationChannelProbe(operation <-chan error) testOperationProbe {
	return func() (bool, error) {
		select {
		case err := <-operation:
			return true, err
		default:
			return false, nil
		}
	}
}

func countQueueChildren(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture, parentID string) int {
	t.Helper()
	var count int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_ops_queue_items
		WHERE parent_queue_item_id = $1::uuid
	`, parentID).Scan(&count); err != nil {
		t.Fatalf("count queue children: %v", err)
	}
	return count
}

func requireQuarantinedPromotion(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture, promote QueueItemRow, wantChildren int) {
	t.Helper()
	publicationID, _ := promote.PayloadJSON["publication_id"].(string)
	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication: %v", err)
	}
	var taskState string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT state FROM production_tasks WHERE id = $1::uuid
	`, publication.ProductionTaskID).Scan(&taskState); err != nil {
		t.Fatalf("select task state: %v", err)
	}
	if taskState != TaskHeld {
		t.Fatalf("task state = %s, want held", taskState)
	}
	var queueStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status FROM channel_ops_queue_items WHERE id = $1::uuid
	`, promote.ID).Scan(&queueStatus); err != nil {
		t.Fatalf("select promotion queue status: %v", err)
	}
	if queueStatus != QueueStatusDeadLettered {
		t.Fatalf("promotion queue status = %s, want dead_lettered", queueStatus)
	}
	if children := countQueueChildren(t, ctx, fixture, promote.ID); children != wantChildren {
		t.Fatalf("promotion descendant count = %d, want %d", children, wantChildren)
	}
	var runnableChildren int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_ops_queue_items
		WHERE parent_queue_item_id = $1::uuid
		  AND status IN ($2, $3)
	`, promote.ID, QueueStatusQueued, QueueStatusRunning).Scan(&runnableChildren); err != nil {
		t.Fatalf("count runnable children: %v", err)
	}
	if runnableChildren != 0 {
		t.Fatalf("runnable promotion descendants = %d, want 0", runnableChildren)
	}
}

func prepareQueueKind(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture, handler HandlerService, kind string) QueueItemRow {
	t.Helper()
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	if kind != QueueReconcilePublication && kind != QueueCollectMetrics {
		return fixture.ProcessUntilQueueKind(ctx, handler, kind)
	}
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.Handle(ctx, promote); err != nil {
		t.Fatalf("Handle promotion setup: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote); err != nil {
		t.Fatalf("MarkQueueDone promotion setup: %v", err)
	}
	return fixture.ProcessUntilQueueKind(ctx, handler, kind)
}

func taskIDForQueueItem(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture, item QueueItemRow) string {
	t.Helper()
	if taskID, _ := item.PayloadJSON["production_task_id"].(string); taskID != "" {
		return taskID
	}
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	publication, err := fixture.Store.GetPublication(ctx, publicationID)
	if err != nil {
		t.Fatalf("GetPublication for %s: %v", item.Kind, err)
	}
	return publication.ProductionTaskID
}

type fakeNoMetricsYouTube struct {
	fakeYouTube
}

func (fakeNoMetricsYouTube) FetchMetrics(ctx context.Context, videoID string) (map[string]any, error) {
	return map[string]any{}, nil
}

type fakeLowQuotaYouTube struct {
	fakeYouTube
}

func (fakeLowQuotaYouTube) AccountHealth(ctx context.Context, accountID string) (YouTubeAccountHealth, error) {
	return YouTubeAccountHealth{Authenticated: true, QuotaRemaining: 1200, Raw: map[string]any{"quota": "low"}}, nil
}

type fakeSevereStatusYouTube struct {
	fakeYouTube
}

func (fakeSevereStatusYouTube) PublicationStatus(ctx context.Context, videoID string) (YouTubePublicationStatus, error) {
	return YouTubePublicationStatus{
		VideoID:       videoID,
		PublishStatus: "rejected",
		Privacy:       "private",
		Permalink:     "https://youtu.be/" + videoID,
		Raw:           map[string]any{"status": "rejected"},
	}, nil
}
