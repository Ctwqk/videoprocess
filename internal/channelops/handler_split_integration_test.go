package channelops

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAgentTickExternalWaitAllowsLeaderTakeoverAndRejectsStaleFinalize(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	secondStore := openHandlerTakeoverStore(t, ctx)
	now := fixture.Store.Now()
	oldLease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@tick-split-old:1", now)
	var takeoverLease *LeaderLease
	releaseExternal := make(chan struct{})
	var releaseExternalOnce sync.Once
	defer func() {
		releaseExternalOnce.Do(func() { close(releaseExternal) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(3*time.Second))
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	item := enqueueAndClaimHandlerItem(t, ctx, fixture, oldLease.Authority(), EnqueueOptions{
		Kind:             QueueAgentTick,
		IdempotencyKey:   "agent_tick:split-takeover:" + fixture.ChannelID,
		Payload:          map[string]any{"channel_id": fixture.ChannelID, "bucket": "2026-07-23-18"},
		ChannelProfileID: &fixture.ChannelID,
	})
	pds := &blockingHandlerPDS{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.PDS = pds
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.Handle(ctx, item) }()
	waitHandlerSplitSignal(t, pds.started, "tick PDS call")

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@tick-split-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })

	select {
	case err := <-handleDone:
		if !errors.Is(err, ErrLeaderAuthorityLost) {
			t.Fatalf("stale tick finalizer error = %v, want ErrLeaderAuthorityLost", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for stale tick finalizer: %v", ctx.Err())
	}
	var taskCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM production_tasks WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&taskCount); err != nil {
		t.Fatalf("count stale tick tasks: %v", err)
	}
	if taskCount != 0 {
		t.Fatalf("stale tick finalizer wrote %d production tasks", taskCount)
	}
}

func TestPlanTaskExternalWaitAllowsLeaderTakeoverAndRejectsStaleFinalize(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, "2026-07-23-18", handler); err != nil {
		t.Fatalf("seed selected task: %v", err)
	}
	secondStore := openHandlerTakeoverStore(t, ctx)
	now := fixture.Store.Now()
	oldLease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@plan-split-old:1", now)
	var takeoverLease *LeaderLease
	releaseExternal := make(chan struct{})
	var releaseExternalOnce sync.Once
	defer func() {
		releaseExternalOnce.Do(func() { close(releaseExternal) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(3*time.Second))
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	item, err := fixture.Store.ClaimNextForKinds(
		ctx,
		handlerWorkerID(oldLease.Authority()),
		[]string{QueuePlanTask},
	)
	if err != nil || item == nil {
		t.Fatalf("claim plan task = %#v, %v", item, err)
	}
	taskID := firstString(item.PayloadJSON, "production_task_id")
	recorder := &externalCallRecorder{}
	autoFlow := &blockingPlanAutoFlow{
		fakeAutoFlow: fakeAutoFlow{},
		started:      make(chan struct{}, 1),
		release:      releaseExternal,
	}
	handler.AutoFlow = autoFlow
	handler.PDS = &recordingPDS{recorder: recorder}
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.Handle(ctx, *item) }()
	waitHandlerSplitSignal(t, autoFlow.started, "AutoFlow plan call")

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@plan-split-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })

	select {
	case err := <-handleDone:
		if !errors.Is(err, ErrLeaderAuthorityLost) {
			t.Fatalf("stale plan finalizer error = %v, want ErrLeaderAuthorityLost", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for stale plan finalizer: %v", ctx.Err())
	}
	if recorder.pds.Load() != 0 {
		t.Fatalf("stale plan called PDS %d times after takeover", recorder.pds.Load())
	}
	task, err := fixture.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("read stale plan task: %v", err)
	}
	if task.State != TaskSelected || task.AutoFlowPlanID != nil {
		t.Fatalf("stale plan changed task to state=%s plan=%v", task.State, task.AutoFlowPlanID)
	}
	var executeCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM channel_ops_queue_items
		WHERE parent_queue_item_id = $1::uuid AND kind = $2
	`, item.ID, QueueExecuteTask).Scan(&executeCount); err != nil {
		t.Fatalf("count stale plan descendants: %v", err)
	}
	if executeCount != 0 {
		t.Fatalf("stale plan created %d execute descendants", executeCount)
	}
}

func TestAccountHealthExternalWaitAllowsLeaderTakeoverAndRejectsStaleFinalize(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	secondStore := openHandlerTakeoverStore(t, ctx)
	now := fixture.Store.Now()
	oldLease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@health-split-old:1", now)
	var takeoverLease *LeaderLease
	releaseExternal := make(chan struct{})
	var releaseExternalOnce sync.Once
	defer func() {
		releaseExternalOnce.Do(func() { close(releaseExternal) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(3*time.Second))
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	item := enqueueAndClaimHandlerItem(t, ctx, fixture, oldLease.Authority(), EnqueueOptions{
		Kind:             QueueAccountHealth,
		IdempotencyKey:   "account_health:split-takeover:" + fixture.AccountID,
		Payload:          map[string]any{"account_id": fixture.AccountID},
		ChannelProfileID: &fixture.ChannelID,
	})
	youtube := &blockingAccountHealthYouTube{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.YouTube = youtube
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.Handle(ctx, item) }()
	waitHandlerSplitSignal(t, youtube.started, "YouTube account health call")

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@health-split-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })

	select {
	case err := <-handleDone:
		if !errors.Is(err, ErrLeaderAuthorityLost) {
			t.Fatalf("stale account health finalizer error = %v, want ErrLeaderAuthorityLost", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for stale account health finalizer: %v", ctx.Err())
	}
	var lastTokenCheckAt *time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT last_token_check_at
		FROM publishing_accounts
		WHERE id = $1::uuid
	`, fixture.AccountID).Scan(&lastTokenCheckAt); err != nil {
		t.Fatalf("read stale account health state: %v", err)
	}
	if lastTokenCheckAt != nil {
		t.Fatalf("stale account health wrote last_token_check_at %s", *lastTokenCheckAt)
	}
}

func TestDiscoveryExternalWaitAllowsLeaderTakeoverAndRejectsStaleFinalize(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	secondStore := openHandlerTakeoverStore(t, ctx)
	now := fixture.Store.Now()
	oldLease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@discovery-split-old:1", now)
	var takeoverLease *LeaderLease
	releaseExternal := make(chan struct{})
	var releaseExternalOnce sync.Once
	defer func() {
		releaseExternalOnce.Do(func() { close(releaseExternal) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(3*time.Second))
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	item := enqueueAndClaimHandlerItem(t, ctx, fixture, oldLease.Authority(), EnqueueOptions{
		Kind:           QueueIngestDiscovery,
		IdempotencyKey: "ingest_discovery:split-takeover:" + fixture.ChannelID,
		Payload: map[string]any{
			"channel_id":       fixture.ChannelID,
			"source":           "youtube_search",
			"bucket":           "2026-07-23-18",
			"scheduler_bucket": "2026-07-23-18",
		},
		ChannelProfileID: &fixture.ChannelID,
	})
	discovery := &blockingDiscoveryClient{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = discovery
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.Handle(ctx, item) }()
	waitHandlerSplitSignal(t, discovery.started, "discovery ingestion")

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@discovery-split-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })

	select {
	case err := <-handleDone:
		if !errors.Is(err, ErrLeaderAuthorityLost) {
			t.Fatalf("stale discovery finalizer error = %v, want ErrLeaderAuthorityLost", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for stale discovery finalizer: %v", ctx.Err())
	}
}

func TestExecuteFinalizerRejectsChangedTaskSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(context.Background())
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	item := prepareQueueKind(t, ctx, fixture, baseHandler, QueueExecuteTask)
	taskID := taskIDForQueueItem(t, ctx, fixture, item)
	releaseExternal := make(chan struct{})
	autoFlow := &blockingExecuteAutoFlow{
		fakeAutoFlow: fakeAutoFlow{},
		started:      make(chan struct{}, 1),
		release:      releaseExternal,
	}
	handler := baseHandler
	handler.AutoFlow = autoFlow
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.HandleExecuteTask(ctx, item) }()
	waitHandlerSplitSignal(t, autoFlow.started, "AutoFlow execute call")

	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE production_tasks
		SET prompt = prompt || ' changed-after-prepare'
		WHERE id = $1::uuid
	`, taskID); err != nil {
		t.Fatalf("change prepared execute task: %v", err)
	}
	close(releaseExternal)

	select {
	case err := <-handleDone:
		if !errors.Is(err, ErrHandlerSnapshotStale) {
			t.Fatalf("changed execute finalizer error = %v, want ErrHandlerSnapshotStale", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for changed execute finalizer: %v", ctx.Err())
	}
	var observeCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM channel_ops_queue_items
		WHERE parent_queue_item_id = $1::uuid AND kind = $2
	`, item.ID, QueueObserveJob).Scan(&observeCount); err != nil {
		t.Fatalf("count execute descendants: %v", err)
	}
	if observeCount != 0 {
		t.Fatalf("changed execute finalizer created %d observe descendants", observeCount)
	}
}

func TestPromotionPDSWaitAllowsLeaderTakeoverAndRejectsStaleReservation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	queued := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	if err := fixture.Store.ReleaseQueueClaim(ctx, queued); err != nil {
		t.Fatalf("release promotion setup claim: %v", err)
	}
	fixture.ResetLeaderEpoch(ctx)
	secondStore := openHandlerTakeoverStore(t, ctx)
	now := fixture.Store.Now()
	oldLease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@promotion-pds-old:1", now)
	var takeoverLease *LeaderLease
	releaseExternal := make(chan struct{})
	var releaseExternalOnce sync.Once
	defer func() {
		releaseExternalOnce.Do(func() { close(releaseExternal) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(3*time.Second))
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	item, err := fixture.Store.ClaimNextForKinds(
		ctx,
		handlerWorkerID(oldLease.Authority()),
		[]string{QueuePromotePublication},
	)
	if err != nil || item == nil || item.ID != queued.ID {
		t.Fatalf("claim promotion PDS item = %#v, %v", item, err)
	}
	pds := &blockingHandlerPDS{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := baseHandler
	handler.PDS = pds
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.Handle(ctx, *item) }()
	waitHandlerSplitSignal(t, pds.started, "promotion PDS call")

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@promotion-pds-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })

	select {
	case err := <-handleDone:
		if !errors.Is(err, ErrLeaderAuthorityLost) {
			t.Fatalf("stale promotion PDS finalizer error = %v, want ErrLeaderAuthorityLost", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for stale promotion PDS finalizer: %v", ctx.Err())
	}
	var operationCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM publication_promotion_operations
		WHERE queue_item_id = $1::uuid
	`, item.ID).Scan(&operationCount); err != nil {
		t.Fatalf("count stale promotion reservations: %v", err)
	}
	if operationCount != 0 {
		t.Fatalf("stale promotion PDS reserved %d operations", operationCount)
	}
}

func TestPromotionYouTubeWaitAllowsLeaderTakeoverAndRejectsStaleConfirmation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	queued := prepareQueueKind(t, ctx, fixture, baseHandler, QueuePromotePublication)
	if err := fixture.Store.ReleaseQueueClaim(ctx, queued); err != nil {
		t.Fatalf("release promotion setup claim: %v", err)
	}
	fixture.ResetLeaderEpoch(ctx)
	secondStore := openHandlerTakeoverStore(t, ctx)
	now := fixture.Store.Now()
	oldLease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@promotion-youtube-old:1", now)
	var takeoverLease *LeaderLease
	releaseExternal := make(chan struct{})
	var releaseExternalOnce sync.Once
	defer func() {
		releaseExternalOnce.Do(func() { close(releaseExternal) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(3*time.Second))
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	item, err := fixture.Store.ClaimNextForKinds(
		ctx,
		handlerWorkerID(oldLease.Authority()),
		[]string{QueuePromotePublication},
	)
	if err != nil || item == nil || item.ID != queued.ID {
		t.Fatalf("claim promotion YouTube item = %#v, %v", item, err)
	}
	youtube := &blockingPromotionYouTube{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := baseHandler
	handler.YouTube = youtube
	handleDone := make(chan error, 1)
	go func() { handleDone <- handler.Handle(ctx, *item) }()
	waitHandlerSplitSignal(t, youtube.started, "promotion YouTube schedule call")

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@promotion-youtube-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })

	select {
	case err := <-handleDone:
		if !errors.Is(err, ErrLeaderAuthorityLost) {
			t.Fatalf("stale promotion YouTube finalizer error = %v, want ErrLeaderAuthorityLost", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for stale promotion YouTube finalizer: %v", ctx.Err())
	}
	var operationStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status FROM publication_promotion_operations
		WHERE queue_item_id = $1::uuid
	`, item.ID).Scan(&operationStatus); err != nil {
		t.Fatalf("read stale promotion operation: %v", err)
	}
	if operationStatus != PromotionSubmitting {
		t.Fatalf("stale promotion operation status = %q, want %q", operationStatus, PromotionSubmitting)
	}
}

type blockingHandlerPDS struct {
	started chan struct{}
	release <-chan struct{}
	calls   atomic.Int32
}

func (p *blockingHandlerPDS) Decide(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
	p.calls.Add(1)
	select {
	case p.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return PDSDecision{}, ctx.Err()
	case <-p.release:
		return PDSDecision{Verdict: "allow", DecisionID: "split-allow"}, nil
	}
}

type blockingPlanAutoFlow struct {
	fakeAutoFlow
	started chan struct{}
	release <-chan struct{}
}

func (a *blockingPlanAutoFlow) PlanTask(
	ctx context.Context,
	task ProductionTaskRow,
	request map[string]any,
) (AutoFlowPlanObservation, error) {
	select {
	case a.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return AutoFlowPlanObservation{}, ctx.Err()
	case <-a.release:
		return a.fakeAutoFlow.PlanTask(ctx, task, request)
	}
}

type blockingExecuteAutoFlow struct {
	fakeAutoFlow
	started chan struct{}
	release <-chan struct{}
}

func (a *blockingExecuteAutoFlow) ExecuteTask(
	ctx context.Context,
	task ProductionTaskRow,
	request map[string]any,
) (AutoFlowExecuteObservation, error) {
	select {
	case a.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return AutoFlowExecuteObservation{}, ctx.Err()
	case <-a.release:
		return a.fakeAutoFlow.ExecuteTask(ctx, task, request)
	}
}

type blockingAccountHealthYouTube struct {
	fakeYouTube
	started chan struct{}
	release <-chan struct{}
}

func (y *blockingAccountHealthYouTube) AccountHealth(
	ctx context.Context,
	accountID string,
) (YouTubeAccountHealth, error) {
	select {
	case y.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return YouTubeAccountHealth{}, ctx.Err()
	case <-y.release:
		return y.fakeYouTube.AccountHealth(ctx, accountID)
	}
}

type blockingDiscoveryClient struct {
	started chan struct{}
	release <-chan struct{}
}

func (d *blockingDiscoveryClient) Ingest(
	ctx context.Context,
	request DiscoveryIngestRequest,
) (DiscoveryObservation, error) {
	select {
	case d.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return DiscoveryObservation{}, ctx.Err()
	case <-d.release:
		return discoveryObservationForTest(request), nil
	}
}

func openHandlerTakeoverStore(t *testing.T, ctx context.Context) *Store {
	t.Helper()
	store, err := OpenStore(ctx, LoadConfig().DatabaseURL)
	if err != nil {
		t.Fatalf("open handler takeover store: %v", err)
	}
	return store
}

func enqueueAndClaimHandlerItem(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	authority LeaderAuthority,
	options EnqueueOptions,
) QueueItemRow {
	t.Helper()
	itemID, err := fixture.Store.Enqueue(ctx, options)
	if err != nil {
		t.Fatalf("enqueue %s handler item: %v", options.Kind, err)
	}
	item, err := fixture.Store.ClaimNextForKinds(ctx, handlerWorkerID(authority), []string{options.Kind})
	if err != nil || item == nil || item.ID != itemID {
		t.Fatalf("claim %s handler item = %#v, %v", options.Kind, item, err)
	}
	return *item
}

func handlerWorkerID(authority LeaderAuthority) string {
	return fmt.Sprintf("%s:epoch:%d", authority.HolderID, authority.Epoch)
}

func acquireHandlerTakeoverWhileBlocked(
	t *testing.T,
	ctx context.Context,
	store *Store,
	holderID string,
	now time.Time,
) *LeaderLease {
	t.Helper()
	acquireCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		lease, acquired, err := store.TryAcquireLeader(acquireCtx, holderID, now)
		if err != nil {
			t.Fatalf("handler takeover: %v", err)
		}
		if acquired && lease != nil {
			return lease
		}
		select {
		case <-ticker.C:
		case <-acquireCtx.Done():
			t.Fatalf("replacement leader blocked behind external handler wait: %v", acquireCtx.Err())
		}
	}
}

func waitHandlerSplitSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}
