package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeLeadershipController struct {
	mu          sync.Mutex
	authority   *LeaderAuthority
	status      LeaderStatus
	ensureErr   error
	ensureCalls int
	ensureHook  func(context.Context, time.Time, int) (*LeaderAuthority, error)
	closeCalls  int
	closeHook   func(context.Context, time.Time) error
}

func (f *fakeLeadershipController) EnsureActive(ctx context.Context, now time.Time) (*LeaderAuthority, error) {
	f.mu.Lock()
	f.ensureCalls++
	call := f.ensureCalls
	hook := f.ensureHook
	authority := cloneLeaderAuthorityPointer(f.authority)
	err := f.ensureErr
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, now, call)
	}
	return authority, err
}

func (f *fakeLeadershipController) Status() LeaderStatus {
	f.mu.Lock()
	defer f.mu.Unlock()
	status := f.status
	status.Authority = cloneLeaderAuthorityPointer(status.Authority)
	return status
}

func (f *fakeLeadershipController) Close(ctx context.Context, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	if f.closeHook != nil {
		return f.closeHook(ctx, now)
	}
	return nil
}

func TestRunnerStandbyLeadershipHasZeroSideEffects(t *testing.T) {
	now := time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC)
	leadership := &fakeLeadershipController{
		status: LeaderStatus{Role: LeaderRoleStandby},
	}
	store := &Store{Now: func() time.Time { return now }}
	runner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      store,
		Scheduler:  Scheduler{Store: store},
		Handlers:   HandlerService{Store: store},
		Leadership: leadership,
	}

	if err := runner.runOnce(context.Background()); err != nil {
		t.Fatalf("standby runOnce: %v", err)
	}
	if leadership.ensureCalls != 1 {
		t.Fatalf("EnsureActive calls = %d, want 1", leadership.ensureCalls)
	}
	if !runner.LastSchedulerRun().IsZero() {
		t.Fatalf("standby scheduler timestamp = %s, want zero", runner.LastSchedulerRun())
	}
}

func TestRunnerLeadershipSchedulerRequiresFence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	authority := &LeaderAuthority{
		ServiceName: leaderServiceName,
		HolderID:    "channelops-go@colima-127:1",
		Epoch:       11,
		AcquiredAt:  fixture.Store.Now(),
		HeartbeatAt: fixture.Store.Now(),
	}
	runner := &Runner{
		Config:    Config{SchedulerPollSeconds: 60},
		Store:     fixture.Store,
		Scheduler: Scheduler{Store: fixture.Store},
		Leadership: &fakeLeadershipController{
			authority: authority,
			status:    LeaderStatus{Role: LeaderRoleActive, Authority: authority},
		},
	}

	if err := runner.runOnce(ctx); err != nil {
		t.Fatalf("runOnce without published leader fence: %v", err)
	}

	var schedulerRuns int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM internal_scheduler_runs
		WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&schedulerRuns); err != nil {
		t.Fatalf("count scheduler runs: %v", err)
	}
	var queueItems int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_ops_queue_items
		WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&queueItems); err != nil {
		t.Fatalf("count scheduled queue items: %v", err)
	}
	if schedulerRuns != 0 || queueItems != 0 {
		t.Fatalf("unfenced scheduler side effects = runs %d queue items %d", schedulerRuns, queueItems)
	}
}

func TestRunnerLeadershipSchedulerErrorStopsCycleBeforeRecoveryOrClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := fixture.Store.Now()
	lease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@scheduler-error:1", now)
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(time.Minute))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	authority := lease.Authority()
	staleID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	staleLockedAt := now.Add(-time.Hour)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, attempt_count = 1,
		    locked_by = 'stale-before-scheduler-error', locked_at = $3
		WHERE id = $1::uuid
	`, staleID, QueueStatusRunning, staleLockedAt); err != nil {
		t.Fatalf("seed stale discovery item: %v", err)
	}
	channelID := fixture.ChannelID
	queuedID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueIngestDiscovery,
		IdempotencyKey: "scheduler-error-queued:" + channelID,
		Payload: map[string]any{
			"channel_id": channelID, "source": "youtube_search",
			"bucket": "2026-07-23-20", "scheduler_bucket": "2026-07-23-20",
		},
		Priority: 80, ChannelProfileID: &channelID,
	})
	if err != nil {
		t.Fatalf("enqueue claim candidate: %v", err)
	}
	schedulerErr := errors.New("scheduler callback failed")
	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	runner := &Runner{
		Config:    Config{SchedulerPollSeconds: 60},
		Store:     fixture.Store,
		Scheduler: Scheduler{Store: fixture.Store},
		Handlers:  handler,
		Leadership: &fakeLeadershipController{
			authority: &authority,
			status:    LeaderStatus{Role: LeaderRoleActive, Authority: &authority},
		},
		schedulerRun: func(_ context.Context, scheduler Scheduler, _ time.Time) (int, error) {
			if scheduler.Store == fixture.Store || !scheduler.Store.hasExecutionTransaction() {
				t.Fatal("scheduler did not receive the fenced store")
			}
			return 0, schedulerErr
		},
	}

	err = runner.runOnce(ctx)

	if !errors.Is(err, schedulerErr) {
		t.Fatalf("runOnce error = %v, want scheduler error", err)
	}
	if !runner.LastSchedulerRun().IsZero() {
		t.Fatalf("last scheduler run = %s, want zero", runner.LastSchedulerRun())
	}
	if client.calls != 0 {
		t.Fatalf("handler calls = %d, want 0", client.calls)
	}
	var staleStatus, staleOwner string
	var staleAt time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, locked_by, locked_at
		FROM channel_ops_queue_items WHERE id = $1::uuid
	`, staleID).Scan(&staleStatus, &staleOwner, &staleAt); err != nil {
		t.Fatalf("read stale item: %v", err)
	}
	if staleStatus != QueueStatusRunning || staleOwner != "stale-before-scheduler-error" || !staleAt.Equal(staleLockedAt) {
		t.Fatalf("scheduler error allowed recovery = %s/%s/%s", staleStatus, staleOwner, staleAt)
	}
	var queuedStatus string
	var queuedOwner *string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, locked_by
		FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queuedID).Scan(&queuedStatus, &queuedOwner); err != nil {
		t.Fatalf("read queued item: %v", err)
	}
	if queuedStatus != QueueStatusQueued || queuedOwner != nil {
		t.Fatalf("scheduler error allowed claim = %s/%v", queuedStatus, queuedOwner)
	}
}

func TestRunnerLeadershipEpochClaimOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := fixture.Store.Now()
	lease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@colima-127:1", now)
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(time.Minute))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	authority := lease.Authority()
	if authority.Epoch <= 0 {
		t.Fatalf("leader epoch = %d, want positive", authority.Epoch)
	}

	channelID := fixture.ChannelID
	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueIngestDiscovery,
		IdempotencyKey: "runner-leadership-epoch:" + channelID,
		Payload: map[string]any{
			"channel_id": channelID, "source": "youtube_search",
			"bucket": "2026-07-23-18", "scheduler_bucket": "2026-07-23-18",
		},
		Priority: 80, ChannelProfileID: &channelID,
	})
	if err != nil {
		t.Fatalf("enqueue epoch claim: %v", err)
	}
	wantOwner := fmt.Sprintf("%s:epoch:%d", authority.HolderID, authority.Epoch)
	var claimedOwner string
	client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT locked_by
			FROM channel_ops_queue_items
			WHERE id = $1::uuid
		`, queueID).Scan(&claimedOwner); err != nil {
			return DiscoveryObservation{}, err
		}
		return discoveryObservationForTest(request), nil
	}}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	runner := &Runner{
		Config:   Config{RunnerID: authority.HolderID, SchedulerPollSeconds: 60},
		Store:    fixture.Store,
		Handlers: handler,
		Leadership: &fakeLeadershipController{
			authority: &authority,
			status:    LeaderStatus{Role: LeaderRoleActive, Authority: &authority},
		},
	}
	runner.SetLastSchedulerRun(now)

	if err := runner.runOnce(ctx); err != nil {
		t.Fatalf("active epoch runOnce: %v", err)
	}
	if claimedOwner != wantOwner {
		t.Fatalf("locked_by = %q, want %q", claimedOwner, wantOwner)
	}
}

func TestRunnerRevalidatesLeadershipAfterClaimBeforeHandler(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, tt := range []struct {
		name         string
		second       func(LeaderAuthority) (*LeaderAuthority, error)
		advanceEpoch bool
	}{
		{
			name:   "standby",
			second: func(LeaderAuthority) (*LeaderAuthority, error) { return nil, nil },
		},
		{
			name: "lost",
			second: func(LeaderAuthority) (*LeaderAuthority, error) {
				return nil, ErrLeaderAuthorityLost
			},
		},
		{
			name: "mismatched epoch",
			second: func(authority LeaderAuthority) (*LeaderAuthority, error) {
				authority.Epoch++
				return &authority, nil
			},
		},
		{
			name:         "database epoch advanced",
			advanceEpoch: true,
			second:       func(LeaderAuthority) (*LeaderAuthority, error) { return nil, nil },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			fixture.ResetLeaderEpoch(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			now := fixture.Store.Now()
			lease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@post-claim:1", now)
			defer func() {
				releaseLeaderTestLease(t, ctx, lease, now.Add(time.Minute))
				fixture.ResetLeaderEpoch(ctx)
				fixture.Close(ctx)
			}()
			authority := lease.Authority()
			channelID := fixture.ChannelID
			queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
				Kind:           QueueIngestDiscovery,
				IdempotencyKey: "post-claim-authority:" + tt.name + ":" + channelID,
				Payload: map[string]any{
					"channel_id": channelID, "source": "youtube_search",
					"bucket": "2026-07-23-21", "scheduler_bucket": "2026-07-23-21",
				},
				Priority: 80, ChannelProfileID: &channelID,
			})
			if err != nil {
				t.Fatalf("enqueue post-claim item: %v", err)
			}
			client := &recordingDiscoveryClient{}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			leadership := &fakeLeadershipController{
				status: LeaderStatus{Role: LeaderRoleActive, Authority: &authority},
			}
			leadership.ensureHook = func(_ context.Context, _ time.Time, call int) (*LeaderAuthority, error) {
				if call == 1 {
					return cloneLeaderAuthority(authority), nil
				}
				if call != 2 {
					return nil, fmt.Errorf("unexpected EnsureActive call %d", call)
				}
				if tt.advanceEpoch {
					if _, err := fixture.Store.Pool.Exec(ctx, `
						UPDATE channelops_leader_epochs
						SET epoch = epoch + 1
						WHERE service_name = $1
					`, leaderServiceName); err != nil {
						return nil, err
					}
				}
				return tt.second(authority)
			}
			runner := &Runner{
				Config:     Config{SchedulerPollSeconds: 60},
				Store:      fixture.Store,
				Handlers:   handler,
				Leadership: leadership,
			}
			runner.SetLastSchedulerRun(now)

			if err := runner.runOnce(ctx); err != nil {
				t.Fatalf("runOnce after post-claim authority change: %v", err)
			}
			if leadership.ensureCalls != 2 {
				t.Fatalf("EnsureActive calls = %d, want 2", leadership.ensureCalls)
			}
			if client.calls != 0 {
				t.Fatalf("discovery calls = %d, want 0", client.calls)
			}
			var status string
			var lockedBy *string
			var lockedAt *time.Time
			var lastError *string
			var attemptCount int
			var runAfter time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
					SELECT status, locked_by, locked_at, last_error, attempt_count, run_after
					FROM channel_ops_queue_items WHERE id = $1::uuid
				`, queueID).Scan(
				&status,
				&lockedBy,
				&lockedAt,
				&lastError,
				&attemptCount,
				&runAfter,
			); err != nil {
				t.Fatalf("read released post-claim item: %v", err)
			}
			if status != QueueStatusQueued || lockedBy != nil || lockedAt != nil ||
				lastError != nil || attemptCount != 0 || runAfter.After(now) {
				t.Fatalf(
					"released post-claim item = %s/%v/%v/%v attempt=%d run_after=%s",
					status,
					lockedBy,
					lockedAt,
					lastError,
					attemptCount,
					runAfter,
				)
			}
		})
	}
}

func TestRunnerReleasesClaimWithoutPenaltyWhenHandlerLosesLeader(t *testing.T) {
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
	oldLease := acquireLeaderTestLease(
		t,
		ctx,
		fixture.Store,
		"channelops-go@runner-handler-old:1",
		now,
	)
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

	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueIngestDiscovery,
		IdempotencyKey: "runner-handler-authority-loss:" + fixture.ChannelID,
		Payload: map[string]any{
			"channel_id":       fixture.ChannelID,
			"source":           "youtube_search",
			"bucket":           "2026-07-23-22",
			"scheduler_bucket": "2026-07-23-22",
		},
		Priority: 80, ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue handler authority-loss item: %v", err)
	}
	var originalRunAfter time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT run_after FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&originalRunAfter); err != nil {
		t.Fatalf("read original handler authority-loss run_after: %v", err)
	}

	authority := oldLease.Authority()
	leadership := &fakeLeadershipController{
		authority: &authority,
		status:    LeaderStatus{Role: LeaderRoleActive, Authority: &authority},
	}
	client := &blockingDiscoveryClient{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	runner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      fixture.Store,
		Handlers:   handler,
		Leadership: leadership,
	}
	runner.SetLastSchedulerRun(now)

	runDone := make(chan error, 1)
	go func() { runDone <- runner.runOnce(ctx) }()
	waitHandlerSplitSignal(t, client.started, "runner discovery ingestion")

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@runner-handler-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runOnce after handler authority loss: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for runner handler authority loss: %v", ctx.Err())
	}

	var status string
	var attemptCount int
	var runAfter time.Time
	var lockedBy *string
	var lockedAt *time.Time
	var lastError *string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count, run_after, locked_by, locked_at, last_error
		FROM channel_ops_queue_items
		WHERE id = $1::uuid
	`, queueID).Scan(
		&status,
		&attemptCount,
		&runAfter,
		&lockedBy,
		&lockedAt,
		&lastError,
	); err != nil {
		t.Fatalf("read released handler authority-loss item: %v", err)
	}
	if status != QueueStatusQueued || attemptCount != 0 ||
		!runAfter.Equal(originalRunAfter) || lockedBy != nil ||
		lockedAt != nil || lastError != nil {
		t.Fatalf(
			"handler authority-loss claim = %s attempt=%d run_after=%s locked_by=%v locked_at=%v last_error=%v",
			status,
			attemptCount,
			runAfter,
			lockedBy,
			lockedAt,
			lastError,
		)
	}
}

func TestRunnerCancellationReleasesNonDiscoveryClaimWithoutPenalty(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer testCancel()
	runCtx, cancelRun := context.WithCancel(testCtx)
	defer cancelRun()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(testCtx)
	fixture.InsertChannelWithLaneAccountSeed(testCtx)
	now := fixture.Store.Now()
	oldLease := acquireLeaderTestLease(
		t,
		testCtx,
		fixture.Store,
		"channelops-go@runner-cancel-old:1",
		now,
	)
	defer func() {
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		fixture.Close(context.Background())
	}()

	queueID, err := fixture.Store.Enqueue(testCtx, EnqueueOptions{
		Kind:             QueueAccountHealth,
		IdempotencyKey:   "account_health:runner-cancel:" + fixture.AccountID,
		Payload:          map[string]any{"account_id": fixture.AccountID},
		Priority:         1,
		ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue runner cancellation item: %v", err)
	}
	originalRunAfter := queueRunAfterForTest(t, testCtx, fixture.Store, queueID)

	authority := oldLease.Authority()
	youtube := &blockingAccountHealthYouTube{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.YouTube = youtube
	runner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      fixture.Store,
		Handlers:   handler,
		Leadership: &fakeLeadershipController{authority: &authority},
	}
	runner.SetLastSchedulerRun(now)

	runDone := make(chan error, 1)
	go func() { runDone <- runner.runOnce(runCtx) }()
	waitHandlerSplitSignal(t, youtube.started, "runner account health cancellation")
	cancelRun()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("runOnce cancellation error = %v, want context.Canceled", err)
		}
	case <-testCtx.Done():
		t.Fatalf("wait for canceled runner: %v", testCtx.Err())
	}

	requireQueueClaimReleasedWithoutPenalty(
		t,
		testCtx,
		fixture.Store,
		queueID,
		originalRunAfter,
	)
}

func TestRunnerTakeoverReleasesNonDiscoveryClaimWithoutPenalty(t *testing.T) {
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
	oldLease := acquireLeaderTestLease(
		t,
		ctx,
		fixture.Store,
		"channelops-go@runner-health-old:1",
		now,
	)
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

	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:             QueueAccountHealth,
		IdempotencyKey:   "account_health:runner-takeover:" + fixture.AccountID,
		Payload:          map[string]any{"account_id": fixture.AccountID},
		Priority:         1,
		ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue runner takeover item: %v", err)
	}
	originalRunAfter := queueRunAfterForTest(t, ctx, fixture.Store, queueID)

	authority := oldLease.Authority()
	youtube := &blockingAccountHealthYouTube{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.YouTube = youtube
	runner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      fixture.Store,
		Handlers:   handler,
		Leadership: &fakeLeadershipController{authority: &authority},
	}
	runner.SetLastSchedulerRun(now)

	runDone := make(chan error, 1)
	go func() { runDone <- runner.runOnce(ctx) }()
	waitHandlerSplitSignal(t, youtube.started, "runner account health takeover")
	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@runner-health-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runOnce after account health takeover: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for runner account health takeover: %v", ctx.Err())
	}

	requireQueueClaimReleasedWithoutPenalty(
		t,
		ctx,
		fixture.Store,
		queueID,
		originalRunAfter,
	)
}

func TestRunnerCommittedAgentTickMarksExactLeaseDoneAfterTakeover(t *testing.T) {
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
	oldLease := acquireLeaderTestLease(
		t,
		ctx,
		fixture.Store,
		"channelops-go@committed-tick-old:1",
		now,
	)
	var takeoverLease *LeaderLease
	resumeCompletion := make(chan struct{})
	var resumeOnce sync.Once
	defer func() {
		resumeOnce.Do(func() { close(resumeCompletion) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(3*time.Second))
		_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueAgentTick,
		IdempotencyKey: "agent_tick:committed-takeover:" + fixture.ChannelID,
		Payload: map[string]any{
			"channel_id": fixture.ChannelID,
			"bucket":     "2026-07-23-23",
		},
		Priority: 1, ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue committed tick: %v", err)
	}
	authority := oldLease.Authority()
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	handlerSucceeded := make(chan struct{})
	runner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      fixture.Store,
		Handlers:   handler,
		Leadership: &fakeLeadershipController{authority: &authority},
		afterHandlerSuccess: func(item QueueItemRow) {
			if item.ID != queueID {
				t.Errorf("post-handler queue id = %s, want %s", item.ID, queueID)
			}
			close(handlerSucceeded)
			<-resumeCompletion
		},
	}

	runDone := make(chan error, 1)
	go func() { runDone <- runner.runOnce(ctx) }()
	waitHandlerSplitSignal(t, handlerSucceeded, "committed agent tick")
	if tasks := countProductionTasksForChannel(t, ctx, fixture.Store, fixture.ChannelID); tasks != 1 {
		t.Fatalf("committed agent tick task count = %d, want 1", tasks)
	}

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@committed-tick-new:1",
		now.Add(time.Second),
	)
	resumeOnce.Do(func() { close(resumeCompletion) })
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runOnce committed tick completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for committed tick completion: %v", ctx.Err())
	}

	requireQueueSucceededWithNoLease(t, ctx, fixture.Store, queueID)
	if tasks := countProductionTasksForChannel(t, ctx, fixture.Store, fixture.ChannelID); tasks != 1 {
		t.Fatalf("post-takeover agent tick task count = %d, want 1", tasks)
	}
}

func TestRunnerCommittedAlertMarksExactLeaseDoneWithoutReplayAfterTakeover(t *testing.T) {
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
	oldLease := acquireLeaderTestLease(
		t,
		ctx,
		fixture.Store,
		"channelops-go@committed-alert-old:1",
		now,
	)
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

	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueSendAlert,
		IdempotencyKey: "send_alert:committed-takeover:" + fixture.ChannelID,
		Payload: map[string]any{
			"kind":       "test_alert",
			"severity":   "warning",
			"channel_id": fixture.ChannelID,
			"message":    "committed alert takeover",
		},
		Priority: 1, ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue committed alert: %v", err)
	}
	authority := oldLease.Authority()
	sink := &blockingHandlerAlertSink{
		started: make(chan struct{}, 1),
		release: releaseExternal,
	}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	handler.Alerts = sink
	runner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      fixture.Store,
		Handlers:   handler,
		Leadership: &fakeLeadershipController{authority: &authority},
	}

	runDone := make(chan error, 1)
	go func() { runDone <- runner.runOnce(ctx) }()
	waitHandlerSplitSignal(t, sink.started, "committed alert sink")
	if calls := sink.calls.Load(); calls != 1 {
		t.Fatalf("committed alert calls = %d, want 1", calls)
	}

	dropLeaderTestSession(t, ctx, oldLease)
	takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		secondStore,
		"channelops-go@committed-alert-new:1",
		now.Add(time.Second),
	)
	releaseExternalOnce.Do(func() { close(releaseExternal) })
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("runOnce committed alert completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for committed alert completion: %v", ctx.Err())
	}

	requireQueueSucceededWithNoLease(t, ctx, fixture.Store, queueID)
	var replay *QueueItemRow
	if err := secondStore.WithLeaderExecutionFence(ctx, func(fenced *Store) error {
		var claimErr error
		replay, claimErr = fenced.ClaimNextForKinds(
			ctx,
			handlerWorkerID(takeoverLease.Authority()),
			[]string{QueueSendAlert},
		)
		return claimErr
	}); err != nil {
		t.Fatalf("claim committed alert replay: %v", err)
	}
	if replay != nil {
		t.Fatalf("committed alert was replayable: %#v", replay)
	}
	if calls := sink.calls.Load(); calls != 1 {
		t.Fatalf("post-takeover alert calls = %d, want 1", calls)
	}
}

func TestRunnerExecuteTakeoverReplaysOneDurableRunWithStableIdempotency(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	baseHandler := fixture.HandlerService(PDSDecision{
		Verdict:    "allow",
		DecisionID: "allow",
	})
	preparedItem := prepareQueueKind(t, ctx, fixture, baseHandler, QueueExecuteTask)
	taskID := taskIDForQueueItem(t, ctx, fixture, preparedItem)
	setHumanReviewEvidenceForTest(t, ctx, fixture, taskID, "valid", "", "")
	harness := newPreparedHandlerTakeoverHarness(
		t,
		ctx,
		fixture,
		preparedItem,
		"channelops-go@execute-handoff-old:1",
	)
	if err := fixture.Store.ReleaseQueueClaim(ctx, harness.item); err != nil {
		t.Fatalf("release execute takeover setup claim: %v", err)
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET run_after = NOW() + INTERVAL '1 hour'
		WHERE id <> $1::uuid
		  AND status = $2
	`, harness.item.ID, QueueStatusQueued); err != nil {
		t.Fatalf("defer non-target execute takeover queue rows: %v", err)
	}
	originalRunAfter := queueRunAfterForTest(t, ctx, fixture.Store, harness.item.ID)

	const (
		runID      = "00000000-0000-0000-0000-000000000211"
		pipelineID = "00000000-0000-0000-0000-000000000212"
		jobID      = "00000000-0000-0000-0000-000000000311"
	)
	firstRunCreated := make(chan struct{})
	releaseFirstResponse := make(chan struct{})
	var releaseFirstOnce sync.Once
	defer releaseFirstOnce.Do(func() { close(releaseFirstResponse) })
	var requestMu sync.Mutex
	requestCount := 0
	remoteCreateCount := 0
	idempotencyKeys := map[string]int{}
	serverErrors := make(chan error, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			serverErrors <- fmt.Errorf("decode AutoFlow execute request: %w", err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		idempotencyKey := firstString(payload, "idempotency_key")
		if idempotencyKey == "" {
			serverErrors <- errors.New("AutoFlow execute request missing idempotency key")
			http.Error(w, "missing idempotency key", http.StatusBadRequest)
			return
		}

		requestMu.Lock()
		requestCount++
		call := requestCount
		if idempotencyKeys[idempotencyKey] == 0 {
			remoteCreateCount++
		}
		idempotencyKeys[idempotencyKey]++
		requestMu.Unlock()

		if call == 1 {
			if _, err := fixture.Store.Pool.Exec(ctx, `
				UPDATE production_tasks
				SET autoflow_run_id = $2::uuid,
				    pipeline_id = $3::uuid,
				    job_id = $4::uuid,
				    state = 'producing'
				WHERE id = $1::uuid
			`, taskID, runID, pipelineID, jobID); err != nil {
				serverErrors <- fmt.Errorf("persist durable AutoFlow handoff: %w", err)
				http.Error(w, "durable handoff failed", http.StatusInternalServerError)
				return
			}
			close(firstRunCreated)
			select {
			case <-releaseFirstResponse:
			case <-r.Context().Done():
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"run_id":"00000000-0000-0000-0000-000000000211",
			"pipeline_id":"00000000-0000-0000-0000-000000000212",
			"job_id":"00000000-0000-0000-0000-000000000311",
			"status":"PENDING"
		}`))
	}))
	defer server.Close()

	oldAuthority := harness.oldLease.Authority()
	oldHandler := baseHandler
	oldHandler.AutoFlow = HTTPAutoFlowClient{BaseURL: server.URL}
	oldRunner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      fixture.Store,
		Handlers:   oldHandler,
		Leadership: &fakeLeadershipController{authority: &oldAuthority},
	}
	oldRunDone := make(chan error, 1)
	go func() { oldRunDone <- oldRunner.runOnce(ctx) }()
	waitHandlerSplitSignal(t, firstRunCreated, "durable AutoFlow execute handoff")

	dropLeaderTestSession(t, ctx, harness.oldLease)
	harness.takeoverLease = acquireHandlerTakeoverWhileBlocked(
		t,
		ctx,
		harness.secondStore,
		"channelops-go@execute-handoff-new:1",
		harness.now.Add(time.Second),
	)
	releaseFirstOnce.Do(func() { close(releaseFirstResponse) })
	select {
	case err := <-oldRunDone:
		if err != nil {
			t.Fatalf("old execute runner after takeover: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for old execute runner: %v", ctx.Err())
	}
	requireQueueClaimReleasedWithoutPenalty(
		t,
		ctx,
		fixture.Store,
		harness.item.ID,
		originalRunAfter,
	)

	newAuthority := harness.takeoverLease.Authority()
	newHandler := oldHandler
	newHandler.Store = harness.secondStore
	newRunner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      harness.secondStore,
		Handlers:   newHandler,
		Leadership: &fakeLeadershipController{authority: &newAuthority},
	}
	if err := newRunner.runOnce(ctx); err != nil {
		t.Fatalf("replacement execute runner: %v", err)
	}

	requireQueueSucceededWithNoLease(t, ctx, fixture.Store, harness.item.ID)
	task, err := fixture.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("read durable execute task: %v", err)
	}
	if task.State != TaskProducing ||
		task.AutoFlowRunID == nil || *task.AutoFlowRunID != runID ||
		task.JobID == nil || *task.JobID != jobID {
		t.Fatalf(
			"durable execute task = state %s run %v job %v",
			task.State,
			task.AutoFlowRunID,
			task.JobID,
		)
	}
	var observeCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM channel_ops_queue_items
		WHERE parent_queue_item_id = $1::uuid
		  AND kind = $2
	`, harness.item.ID, QueueObserveJob).Scan(&observeCount); err != nil {
		t.Fatalf("count durable execute observe rows: %v", err)
	}
	if observeCount != 1 {
		t.Fatalf("durable execute observe rows = %d, want 1", observeCount)
	}

	requestMu.Lock()
	gotRequestCount := requestCount
	gotCreateCount := remoteCreateCount
	gotKeyCount := len(idempotencyKeys)
	var gotKeyUses int
	for _, uses := range idempotencyKeys {
		gotKeyUses = uses
	}
	requestMu.Unlock()
	if gotRequestCount != 2 ||
		gotCreateCount != 1 ||
		gotKeyCount != 1 ||
		gotKeyUses != 2 {
		t.Fatalf(
			"AutoFlow replay = requests %d creates %d keys %d uses %d",
			gotRequestCount,
			gotCreateCount,
			gotKeyCount,
			gotKeyUses,
		)
	}
	select {
	case err := <-serverErrors:
		t.Fatal(err)
	default:
	}
}

func requireQueueSucceededWithNoLease(
	t *testing.T,
	ctx context.Context,
	store *Store,
	queueID string,
) {
	t.Helper()
	var status string
	var lockedBy *string
	var lockedAt *time.Time
	if err := store.Pool.QueryRow(ctx, `
		SELECT status, locked_by, locked_at
		FROM channel_ops_queue_items
		WHERE id = $1::uuid
	`, queueID).Scan(&status, &lockedBy, &lockedAt); err != nil {
		t.Fatalf("read completed queue item: %v", err)
	}
	if status != QueueStatusSucceeded || lockedBy != nil || lockedAt != nil {
		t.Fatalf(
			"completed queue item = status %s locked_by=%v locked_at=%v",
			status,
			lockedBy,
			lockedAt,
		)
	}
}

func countProductionTasksForChannel(
	t *testing.T,
	ctx context.Context,
	store *Store,
	channelID string,
) int {
	t.Helper()
	var count int
	if err := store.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM production_tasks
		WHERE channel_profile_id = $1::uuid
	`, channelID).Scan(&count); err != nil {
		t.Fatalf("count production tasks: %v", err)
	}
	return count
}

func queueRunAfterForTest(
	t *testing.T,
	ctx context.Context,
	store *Store,
	queueID string,
) time.Time {
	t.Helper()
	var runAfter time.Time
	if err := store.Pool.QueryRow(ctx, `
		SELECT run_after
		FROM channel_ops_queue_items
		WHERE id = $1::uuid
	`, queueID).Scan(&runAfter); err != nil {
		t.Fatalf("read queue run_after: %v", err)
	}
	return runAfter
}

func requireQueueClaimReleasedWithoutPenalty(
	t *testing.T,
	ctx context.Context,
	store *Store,
	queueID string,
	wantRunAfter time.Time,
) {
	t.Helper()
	var status string
	var attemptCount int
	var runAfter time.Time
	var lockedBy *string
	var lockedAt *time.Time
	var lastError *string
	if err := store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count, run_after, locked_by, locked_at, last_error
		FROM channel_ops_queue_items
		WHERE id = $1::uuid
	`, queueID).Scan(
		&status,
		&attemptCount,
		&runAfter,
		&lockedBy,
		&lockedAt,
		&lastError,
	); err != nil {
		t.Fatalf("read released queue claim: %v", err)
	}
	if status != QueueStatusQueued ||
		attemptCount != 0 ||
		!runAfter.Equal(wantRunAfter) ||
		lockedBy != nil ||
		lockedAt != nil ||
		lastError != nil {
		t.Fatalf(
			"released queue claim = %s attempt=%d run_after=%s locked_by=%v locked_at=%v last_error=%v",
			status,
			attemptCount,
			runAfter,
			lockedBy,
			lockedAt,
			lastError,
		)
	}
}

func TestRunnerLeadershipLossReturnsStandbyWithoutClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	defer func() {
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	now := fixture.Store.Now()
	leadership := newPostgresLeadershipController(fixture.Store, "channelops-go@colima-127:1")
	authority, err := leadership.EnsureActive(ctx, now)
	if err != nil || authority == nil {
		t.Fatalf("initial EnsureActive = authority %#v, err %v", authority, err)
	}

	channelID := fixture.ChannelID
	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueIngestDiscovery,
		IdempotencyKey: "runner-leadership-loss:" + channelID,
		Payload: map[string]any{
			"channel_id": channelID, "source": "youtube_search",
			"bucket": "2026-07-23-18", "scheduler_bucket": "2026-07-23-18",
		},
		Priority: 80, ChannelProfileID: &channelID,
	})
	if err != nil {
		t.Fatalf("enqueue leadership-loss item: %v", err)
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET epoch = epoch + 1
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		t.Fatalf("advance leader epoch: %v", err)
	}
	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	runner := &Runner{
		Config:     Config{SchedulerPollSeconds: 60},
		Store:      fixture.Store,
		Handlers:   handler,
		Leadership: leadership,
	}
	runner.SetLastSchedulerRun(now)

	if err := runner.runOnce(ctx); err != nil {
		t.Fatalf("runOnce after authority loss: %v", err)
	}
	if status := leadership.Status(); status.Role != LeaderRoleStandby {
		t.Fatalf("leader role after authority loss = %q, want standby", status.Role)
	}
	if client.calls != 0 {
		t.Fatalf("handler calls after authority loss = %d, want 0", client.calls)
	}
	var queueStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&queueStatus); err != nil {
		t.Fatalf("read leadership-loss queue item: %v", err)
	}
	if queueStatus != QueueStatusQueued {
		t.Fatalf("queue status after authority loss = %q, want queued", queueStatus)
	}
}

func TestRunnerLeadershipClosePrecedesStoreClose(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	cfg := LoadConfig()
	store, err := OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		if os.Getenv("CHANNELOPS_REQUIRE_DATABASE") == "1" {
			t.Fatalf("required ChannelOps integration DATABASE_URL %q is unreachable: %v", cfg.DatabaseURL, err)
		}
		t.Skipf("ChannelOps integration test requires reachable DATABASE_URL %q: %v", cfg.DatabaseURL, err)
	}
	now := time.Date(2026, 7, 23, 19, 0, 0, 0, time.UTC)
	store.Now = func() time.Time { return now }
	checkedOpen := false
	leadership := &fakeLeadershipController{
		status: LeaderStatus{Role: LeaderRoleStandby},
		closeHook: func(closeCtx context.Context, closeNow time.Time) error {
			if _, ok := closeCtx.Deadline(); !ok {
				return errors.New("leadership close context has no deadline")
			}
			if closeCtx.Err() != nil {
				return fmt.Errorf("leadership close context is already canceled: %w", closeCtx.Err())
			}
			if !closeNow.Equal(now) {
				return fmt.Errorf("leadership close time = %s, want %s", closeNow, now)
			}
			if err := store.Pool.Ping(closeCtx); err != nil {
				return fmt.Errorf("store closed before leadership: %w", err)
			}
			checkedOpen = true
			return nil
		},
	}
	runner := &Runner{Store: store, Leadership: leadership}

	if err := runner.Close(); err != nil {
		t.Fatalf("runner Close: %v", err)
	}

	if leadership.closeCalls != 1 || !checkedOpen {
		t.Fatalf("leadership close = calls %d checked store open %t", leadership.closeCalls, checkedOpen)
	}
}

func TestLeadershipControllerStatusAndCloseAreRaceSafe(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	defer func() {
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	now := fixture.Store.Now()
	leadership := newPostgresLeadershipController(fixture.Store, "channelops-go@race:1")
	authority, err := leadership.EnsureActive(ctx, now)
	if err != nil || authority == nil {
		t.Fatalf("initial EnsureActive = authority %#v, err %v", authority, err)
	}

	start := make(chan struct{})
	errCh := make(chan error, 9)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				status := leadership.Status()
				if status.Role != LeaderRoleActive && status.Role != LeaderRoleStandby && status.Role != LeaderRoleUnavailable {
					errCh <- fmt.Errorf("invalid concurrent leader role %q", status.Role)
					return
				}
				if status.Role == LeaderRoleActive && (status.Authority == nil || status.Authority.Epoch <= 0) {
					errCh <- fmt.Errorf("invalid active status %#v", status)
					return
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_, ensureErr := leadership.EnsureActive(ctx, now.Add(time.Second))
		if ensureErr != nil && !errors.Is(ensureErr, ErrLeaderAuthorityUnavailable) {
			errCh <- fmt.Errorf("concurrent EnsureActive: %w", ensureErr)
		}
	}()
	close(start)
	if err := leadership.Close(ctx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if status := leadership.Status(); status.Role != LeaderRoleStandby {
		t.Fatalf("closed leader role = %q, want standby", status.Role)
	}
}

func TestLeadershipControllerStatusReportsStandbyAfterExecutionFenceLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	defer func() {
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	now := fixture.Store.Now()
	leadership := newPostgresLeadershipController(fixture.Store, "channelops-go@status-fence-loss:1")
	defer func() {
		_ = leadership.Close(ctx, now.Add(2*time.Second))
	}()
	authority, err := leadership.EnsureActive(ctx, now)
	if err != nil || authority == nil {
		t.Fatalf("initial EnsureActive = %#v, %v", authority, err)
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET epoch = epoch + 1
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		t.Fatalf("advance leader epoch: %v", err)
	}
	callbackCalled := false
	err = fixture.Store.WithLeaderExecutionFence(ctx, func(*Store) error {
		callbackCalled = true
		return nil
	})
	if !errors.Is(err, ErrLeaderAuthorityLost) {
		t.Fatalf("execution fence error = %v, want ErrLeaderAuthorityLost", err)
	}
	if callbackCalled {
		t.Fatal("execution fence called callback after authority loss")
	}

	status := leadership.Status()

	if status.Role != LeaderRoleStandby || status.Authority != nil || status.Err != nil {
		t.Fatalf("status after fence loss = %#v, want standby", status)
	}
	if err := leadership.Close(ctx, now.Add(time.Second)); !errors.Is(err, ErrLeaderAuthorityLost) {
		t.Fatalf("Close after fence loss error = %v, want ErrLeaderAuthorityLost", err)
	}
}

func TestLeadershipControllerCloseCancelsAndJoinsBlockedAcquire(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	defer func() {
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	attemptStarted := make(chan struct{})
	fixture.Store.leaderLockAttempt = func(ctx context.Context, _ *pgxpool.Conn) (bool, error) {
		close(attemptStarted)
		<-ctx.Done()
		return false, ctx.Err()
	}
	leadership := newPostgresLeadershipController(fixture.Store, "channelops-go@blocked-acquire:1")
	ensureDone := make(chan error, 1)
	go func() {
		_, err := leadership.EnsureActive(ctx, fixture.Store.Now())
		ensureDone <- err
	}()
	waitRunnerTestSignal(t, attemptStarted, "blocked leader acquisition")

	statusDone := make(chan LeaderStatus, 1)
	go func() { statusDone <- leadership.Status() }()
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	var status LeaderStatus
	select {
	case status = <-statusDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Status blocked behind leader acquisition")
	}
	startedClose := time.Now()
	if err := leadership.Close(closeCtx, fixture.Store.Now()); err != nil {
		t.Fatalf("cancel blocked acquisition: %v", err)
	}
	if elapsed := time.Since(startedClose); elapsed >= time.Second {
		t.Fatalf("blocked acquisition Close took %s", elapsed)
	}
	if err := <-ensureDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked EnsureActive error = %v, want context canceled", err)
	}
	if status.Role != LeaderRoleStandby {
		t.Fatalf("blocked acquisition status role = %q, want standby", status.Role)
	}
	secondStore, err := OpenStore(ctx, LoadConfig().DatabaseURL)
	if err != nil {
		t.Fatalf("open replacement store: %v", err)
	}
	defer secondStore.Close()
	replacement, acquired, err := secondStore.TryAcquireLeader(
		ctx,
		"channelops-go@blocked-acquire-replacement:1",
		fixture.Store.Now().Add(time.Second),
	)
	if err != nil || !acquired || replacement == nil {
		t.Fatalf("replacement after canceled acquisition = %#v, %t, %v", replacement, acquired, err)
	}
	if err := replacement.Release(ctx, fixture.Store.Now().Add(2*time.Second)); err != nil {
		t.Fatalf("release replacement: %v", err)
	}
}

func TestLeadershipControllerCloseCancelsAndJoinsBlockedHeartbeat(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	defer func() {
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	now := fixture.Store.Now()
	leadership := newPostgresLeadershipController(
		fixture.Store,
		"channelops-go@blocked-heartbeat:1",
	).(*postgresLeadershipController)
	authority, err := leadership.EnsureActive(ctx, now)
	if err != nil || authority == nil {
		t.Fatalf("initial EnsureActive = %#v, %v", authority, err)
	}
	blocker, err := fixture.Store.Pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin heartbeat blocker: %v", err)
	}
	if _, err := blocker.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET heartbeat_at = heartbeat_at
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		_ = blocker.Rollback(ctx)
		t.Fatalf("lock leader epoch row: %v", err)
	}
	heartbeatDone := make(chan error, 1)
	go func() {
		_, err := leadership.EnsureActive(ctx, now.Add(time.Second))
		heartbeatDone <- err
	}()
	waitForPostgresLockWait(
		t,
		ctx,
		fixture.Store,
		leadership.lease.conn.Conn().PgConn().PID(),
		"blocked leader heartbeat",
	)

	statusDone := make(chan LeaderStatus, 1)
	go func() { statusDone <- leadership.Status() }()
	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	var status LeaderStatus
	select {
	case status = <-statusDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Status blocked behind leader heartbeat")
	}
	startedClose := time.Now()
	if err := leadership.Close(closeCtx, now.Add(2*time.Second)); err != nil {
		t.Fatalf("cancel blocked heartbeat: %v", err)
	}
	if elapsed := time.Since(startedClose); elapsed >= time.Second {
		t.Fatalf("blocked heartbeat Close took %s", elapsed)
	}
	if err := blocker.Rollback(ctx); err != nil {
		t.Fatalf("release heartbeat blocker: %v", err)
	}
	if err := <-heartbeatDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked heartbeat error = %v, want context canceled", err)
	}
	if status.Role != LeaderRoleActive || status.Authority == nil || status.Authority.Epoch != authority.Epoch {
		t.Fatalf("blocked heartbeat status = %#v", status)
	}
	secondStore, err := OpenStore(ctx, LoadConfig().DatabaseURL)
	if err != nil {
		t.Fatalf("open heartbeat replacement store: %v", err)
	}
	defer secondStore.Close()
	replacement, acquired, err := secondStore.TryAcquireLeader(
		ctx,
		"channelops-go@blocked-heartbeat-replacement:1",
		now.Add(3*time.Second),
	)
	if err != nil || !acquired || replacement == nil {
		t.Fatalf("replacement after canceled heartbeat = %#v, %t, %v", replacement, acquired, err)
	}
	if err := replacement.Release(ctx, now.Add(4*time.Second)); err != nil {
		t.Fatalf("release heartbeat replacement: %v", err)
	}
}

func TestRunnerCloseReturnsBoundedErrorWithoutClosingStoreWhenAcquireWillNotJoin(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	defer func() {
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	attemptStarted := make(chan struct{})
	unblockAttempt := make(chan struct{})
	fixture.Store.leaderLockAttempt = func(context.Context, *pgxpool.Conn) (bool, error) {
		close(attemptStarted)
		<-unblockAttempt
		return false, nil
	}
	leadership := newPostgresLeadershipController(
		fixture.Store,
		"channelops-go@stubborn-acquire:1",
	)
	ensureDone := make(chan error, 1)
	go func() {
		_, err := leadership.EnsureActive(ctx, fixture.Store.Now())
		ensureDone <- err
	}()
	waitRunnerTestSignal(t, attemptStarted, "stubborn leader acquisition")

	storeClosed := false
	runner := &Runner{
		Store:                  fixture.Store,
		Leadership:             leadership,
		leadershipCloseTimeout: 25 * time.Millisecond,
		closeStore: func() {
			storeClosed = true
		},
	}
	startedClose := time.Now()
	closeErr := runner.Close()
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("bounded runner Close error = %v, want context deadline exceeded", closeErr)
	}
	if elapsed := time.Since(startedClose); elapsed >= 250*time.Millisecond {
		t.Fatalf("bounded runner Close took %s", elapsed)
	}
	if storeClosed {
		t.Fatal("runner closed store before stubborn leadership operation joined")
	}

	close(unblockAttempt)
	if err := <-ensureDone; err != nil {
		t.Fatalf("drain stubborn acquisition: %v", err)
	}
	if err := leadership.Close(ctx, fixture.Store.Now().Add(time.Second)); err != nil {
		t.Fatalf("cleanup stubborn leadership: %v", err)
	}
}

func waitForPostgresLockWait(
	t *testing.T,
	ctx context.Context,
	store *Store,
	pid uint32,
	label string,
) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := store.Pool.QueryRow(waitCtx, `
			SELECT COALESCE(wait_event_type = 'Lock', FALSE)
			FROM pg_stat_activity
			WHERE pid = $1
		`, pid).Scan(&waiting)
		if err == nil && waiting {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("timed out observing %s: %v", label, err)
		case <-ticker.C:
		}
	}
}

func waitRunnerTestSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func TestRunnerRunWaitsForCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := &Runner{Config: Config{RunnerPollSeconds: 1}}
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("Run returned before cancellation: %v", err)
	case <-time.After(1100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestRunnerRunPerformsInitialRunBeforeFirstSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &Store{Now: func() time.Time {
		return time.Date(2026, 6, 7, 17, 0, 0, 0, time.UTC)
	}}
	runner := &Runner{
		Config:   Config{RunnerPollSeconds: 60},
		Store:    store,
		Handlers: HandlerService{Store: store, PDS: PDSClient{}},
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "autoflow client is not configured") {
			t.Fatalf("Run returned %v, want initial readiness error", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not perform initial run before sleeping")
	}
}

func TestRunnerRunOnceRejectsMissingHandlerDependencies(t *testing.T) {
	store := &Store{Now: func() time.Time {
		return time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	}}
	runner := &Runner{
		Store:    store,
		Handlers: HandlerService{Store: store, PDS: PDSClient{}},
	}

	err := runner.runOnce(context.Background())
	if err == nil {
		t.Fatal("expected missing handler dependencies to return an error")
	}
	if !strings.Contains(err.Error(), "autoflow client is not configured") {
		t.Fatalf("error = %v", err)
	}
}

func TestNewRunnerHandlerServiceConfiguresAutoFlowClient(t *testing.T) {
	store := &Store{Now: func() time.Time {
		return time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	}}
	handler := newRunnerHandlerService(store, validConfig())
	if handler.AutoFlow == nil {
		t.Fatal("AutoFlow client is nil")
	}
	if handler.Alerts == nil {
		t.Fatal("Alerts sink is nil")
	}
	if handler.Discovery == nil {
		t.Fatal("Discovery client is nil for valid config")
	}
	if err := handler.ReadinessError(); err != nil {
		t.Fatalf("ReadinessError returned error: %v", err)
	}
}

func TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly(t *testing.T) {
	for _, tt := range []struct {
		name          string
		mutate        func(*Config)
		loadMalformed bool
	}{
		{name: "missing base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "  " }},
		{name: "credential base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "http://user:password@api:8080" }},
		{name: "query base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "http://api:8080?credential=secret" }},
		{name: "empty query base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "http://api:8080?" }},
		{name: "invalid scheme", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "ftp://api:8080" }},
		{name: "invalid timeout", mutate: func(cfg *Config) { cfg.DiscoveryTimeout = 29 * time.Second }},
		{name: "malformed timeout", loadMalformed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			if tt.loadMalformed {
				t.Setenv("CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS", "not-an-integer")
				cfg = LoadConfig()
			}
			cfg.LiveMode = false
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			handler := newRunnerHandlerService(&Store{}, cfg)
			if handler.Discovery != nil {
				t.Fatal("Discovery client configured for invalid discovery settings")
			}
			if containsString(handler.ClaimableKinds(), QueueIngestDiscovery) {
				t.Fatal("ClaimableKinds includes discovery for invalid discovery settings")
			}
			if err := handler.ReadinessError(); err != nil {
				t.Fatalf("invalid optional discovery settings changed readiness: %v", err)
			}
		})
	}
}

func TestNewRunnerHandlerServiceDiscoveryIgnoresUnrelatedConfigValidation(t *testing.T) {
	cfg := validConfig()
	cfg.DatabaseURL = ""
	handler := newRunnerHandlerService(&Store{}, cfg)
	if handler.Discovery == nil || !containsString(handler.ClaimableKinds(), QueueIngestDiscovery) {
		t.Fatal("unrelated invalid config disabled valid discovery settings")
	}
}

func TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name       string
		client     *recordingDiscoveryClient
		wantStatus string
		wantError  string
	}{
		{
			name: "retry", client: &recordingDiscoveryClient{err: errors.New("credential=top-secret provider-title=private")},
			wantStatus: QueueStatusQueued, wantError: "discovery ingestion failed",
		},
		{
			name: "done", client: &recordingDiscoveryClient{}, wantStatus: QueueStatusSucceeded,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			channelID := fixture.ChannelID
			queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
				Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner:" + tt.name + ":" + channelID,
				Payload:  map[string]any{"channel_id": channelID, "source": "youtube_search", "bucket": "2026-07-21-18", "scheduler_bucket": "2026-07-21-18"},
				Priority: 80, ChannelProfileID: &channelID,
			})
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			request := DiscoveryIngestRequest{QueueItemID: queueID, ChannelID: channelID, Source: "youtube_search", SchedulerBucket: "2026-07-21-18"}
			if tt.client.observation.RunID == "" && tt.client.err == nil {
				tt.client.observation = discoveryObservationForTest(request)
			}
			handler.Discovery = tt.client
			runner := &Runner{Store: fixture.Store, Handlers: handler}
			if err := runner.runOnce(ctx); err != nil {
				t.Fatalf("runOnce: %v", err)
			}
			var status string
			var lastError *string
			var lockedBy *string
			var lockedAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, last_error, locked_by, locked_at
				FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&status, &lastError, &lockedBy, &lockedAt); err != nil {
				t.Fatalf("select queue: %v", err)
			}
			if status != tt.wantStatus {
				t.Fatalf("status = %q, want %q", status, tt.wantStatus)
			}
			if tt.wantError != "" && (lastError == nil || *lastError != tt.wantError) {
				t.Fatal("last_error was not the fixed discovery failure")
			}
			if tt.wantError == "" && lastError != nil {
				t.Fatalf("last_error = %q, want nil", *lastError)
			}
			if lockedBy != nil || lockedAt != nil {
				t.Fatalf("lease remains locked_by=%v locked_at=%v", lockedBy, lockedAt)
			}
			if tt.client.calls != 1 {
				t.Fatalf("client calls = %d, want 1", tt.client.calls)
			}
		})
	}
}

func TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name        string
		maxAttempts int
		clientError bool
	}{
		{name: "done", maxAttempts: 3},
		{name: "retry", maxAttempts: 3, clientError: true},
		{name: "deadletter", maxAttempts: 1, clientError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			channelID := fixture.ChannelID
			bucket := "2026-07-21-18"
			queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
				Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner-lease-race:" + tt.name + ":" + channelID,
				Payload: map[string]any{
					"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
				},
				Priority: 80, ChannelProfileID: &channelID, MaxAttempts: tt.maxAttempts,
			})
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
				if _, err := fixture.Store.Pool.Exec(ctx, `
					UPDATE channel_ops_queue_items
					SET locked_by = 'replacement-worker', locked_at = locked_at + INTERVAL '1 second'
					WHERE id = $1::uuid AND status = $2
				`, queueID, QueueStatusRunning); err != nil {
					return DiscoveryObservation{}, err
				}
				if tt.clientError {
					return DiscoveryObservation{}, errors.New("credential=top-secret provider-title=private")
				}
				return discoveryObservationForTest(request), nil
			}}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			runner := &Runner{Store: fixture.Store, Handlers: handler}
			if err := runner.runOnce(ctx); !errors.Is(err, ErrQueueLeaseLost) || err.Error() != "queue lease lost" {
				t.Fatal("runOnce did not return the queue lease lost sentinel")
			}

			var status string
			var lockedBy *string
			var lockedAt *time.Time
			var lastError *string
			var deadLetterAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, locked_by, locked_at, last_error, dead_letter_at
				FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&status, &lockedBy, &lockedAt, &lastError, &deadLetterAt); err != nil {
				t.Fatalf("select queue: %v", err)
			}
			if status != QueueStatusRunning || lockedBy == nil || *lockedBy != "replacement-worker" || lockedAt == nil || lastError != nil || deadLetterAt != nil {
				t.Fatal("stale runner changed the replacement lease")
			}
		})
	}
}

func TestRunnerRecoversStaleDiscoveryLeaseBeforeClaim(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	staleLockedAt := fixture.Store.Now().Add(-15 * time.Minute)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, attempt_count = 1, locked_by = 'crashed-runner', locked_at = $3
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, staleLockedAt); err != nil {
		t.Fatalf("seed stale discovery lease: %v", err)
	}

	var replacementLockedAt time.Time
	var recoveredError *string
	client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
		var status, lockedBy string
		var attempts int
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT status, attempt_count, locked_by, locked_at, last_error
			FROM channel_ops_queue_items WHERE id = $1::uuid
		`, queueID).Scan(&status, &attempts, &lockedBy, &replacementLockedAt, &recoveredError); err != nil {
			return DiscoveryObservation{}, err
		}
		if status != QueueStatusRunning || attempts != 2 || lockedBy != "channelops-go-runner" {
			return DiscoveryObservation{}, errors.New("replacement discovery lease was not claimed")
		}
		return discoveryObservationForTest(request), nil
	}}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	runner := &Runner{Store: fixture.Store, Handlers: handler}

	if err := runner.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	if !replacementLockedAt.After(staleLockedAt) {
		t.Fatalf("replacement locked_at = %s, stale locked_at = %s", replacementLockedAt, staleLockedAt)
	}
	if recoveredError == nil || *recoveredError != "discovery_lease_recovered" {
		t.Fatalf("recovery error category = %v", recoveredError)
	}
	var status string
	var attempts int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &attempts); err != nil {
		t.Fatalf("select recovered discovery row: %v", err)
	}
	if status != QueueStatusSucceeded || attempts != 2 || client.calls != 1 {
		t.Fatalf("recovered row = status %s attempts %d calls %d", status, attempts, client.calls)
	}
}

func TestRunnerDiscoveryRecoveryLeavesFreshAndOtherKindsRunning(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	freshID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	otherID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind: QueueLearningRecompute, IdempotencyKey: "recovery-other-kind:" + t.Name(),
		Payload: map[string]any{
			"channel_id": fixture.ChannelID, "bucket": "2026-05-21-18", "window_days": []int{7, 30},
		},
		Priority: 180, ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue other queue kind: %v", err)
	}
	freshLockedAt := fixture.Store.Now().Add(-15*time.Minute + time.Second)
	staleLockedAt := fixture.Store.Now().Add(-time.Hour)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    attempt_count = 1,
		    locked_by = CASE WHEN id = $1::uuid THEN 'fresh-runner' ELSE 'other-runner' END,
		    locked_at = CASE WHEN id = $1::uuid THEN $4::timestamptz ELSE $5::timestamptz END
		WHERE id IN ($1::uuid, $3::uuid)
	`, freshID, QueueStatusRunning, otherID, freshLockedAt, staleLockedAt); err != nil {
		t.Fatalf("seed untouched running leases: %v", err)
	}

	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	for _, tt := range []struct {
		id       string
		owner    string
		lockedAt time.Time
	}{
		{id: freshID, owner: "fresh-runner", lockedAt: freshLockedAt},
		{id: otherID, owner: "other-runner", lockedAt: staleLockedAt},
	} {
		var status, owner string
		var lockedAt time.Time
		var attempts int
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT status, attempt_count, locked_by, locked_at
			FROM channel_ops_queue_items WHERE id = $1::uuid
		`, tt.id).Scan(&status, &attempts, &owner, &lockedAt); err != nil {
			t.Fatalf("select untouched running lease: %v", err)
		}
		if status != QueueStatusRunning || attempts != 1 || owner != tt.owner || !lockedAt.Equal(tt.lockedAt) {
			t.Fatalf("running lease changed = %s/%d/%s/%s", status, attempts, owner, lockedAt)
		}
	}
	if client.calls != 0 {
		t.Fatalf("fresh discovery client calls = %d, want 0", client.calls)
	}
}

func TestRunnerRecoversDiscoveryRowsWithMalformedLeaseOwnership(t *testing.T) {
	for _, tt := range []struct {
		name     string
		lockedBy *string
		lockedAt *time.Time
	}{
		{name: "missing owner", lockedAt: timePointer(time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC))},
		{name: "blank owner", lockedBy: stringPointer("   "), lockedAt: timePointer(time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC))},
		{name: "missing timestamp", lockedBy: stringPointer("crashed-runner")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
			if _, err := fixture.Store.Pool.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2, attempt_count = 1, locked_by = $3, locked_at = $4
				WHERE id = $1::uuid
			`, queueID, QueueStatusRunning, tt.lockedBy, tt.lockedAt); err != nil {
				t.Fatalf("seed malformed discovery lease: %v", err)
			}

			client := &recordingDiscoveryClient{}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			client.ingest = func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
				return discoveryObservationForTest(request), nil
			}
			if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
				t.Fatalf("runOnce: %v", err)
			}
			var status string
			var attempts int
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, attempt_count FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&status, &attempts); err != nil {
				t.Fatalf("select recovered malformed lease: %v", err)
			}
			if status != QueueStatusSucceeded || attempts != 2 || client.calls != 1 {
				t.Fatalf("malformed recovery = status %s attempts %d calls %d", status, attempts, client.calls)
			}
		})
	}
}

func TestRunnerDeadLettersExhaustedStaleDiscoveryLease(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, attempt_count = max_attempts,
		    locked_by = 'crashed-runner', locked_at = $3
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, fixture.Store.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("seed exhausted discovery lease: %v", err)
	}

	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	var status string
	var lastError *string
	var lockedBy *string
	var lockedAt, deadLetterAt *time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, last_error, locked_by, locked_at, dead_letter_at
		FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &lastError, &lockedBy, &lockedAt, &deadLetterAt); err != nil {
		t.Fatalf("select exhausted discovery lease: %v", err)
	}
	if status != QueueStatusDeadLettered || lastError == nil || *lastError != "discovery_lease_recovered" || lockedBy != nil || lockedAt != nil || deadLetterAt == nil {
		t.Fatalf("exhausted recovery = %s/%v/%v/%v/%v", status, lastError, lockedBy, lockedAt, deadLetterAt)
	}
	if client.calls != 0 {
		t.Fatalf("exhausted discovery client calls = %d, want 0", client.calls)
	}
}

func TestRunnerRecoveryFencesStaleDiscoveryOwnerCompletion(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	oldOwner := "crashed-runner"
	oldLockedAt := fixture.Store.Now().Add(-time.Hour)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, attempt_count = 1, locked_by = $3, locked_at = $4
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, oldOwner, oldLockedAt); err != nil {
		t.Fatalf("seed stale discovery owner: %v", err)
	}
	staleItem := QueueItemRow{
		ID: queueID, Status: QueueStatusRunning, AttemptCount: 1, MaxAttempts: 3,
		LockedBy: &oldOwner, LockedAt: &oldLockedAt,
	}

	var staleCompletionErr error
	client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
		staleCompletionErr = fixture.Store.MarkQueueDone(ctx, staleItem)
		return discoveryObservationForTest(request), nil
	}}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	if !errors.Is(staleCompletionErr, ErrQueueLeaseLost) {
		t.Fatalf("stale completion error = %v, want ErrQueueLeaseLost", staleCompletionErr)
	}
	var status string
	var attempts int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &attempts); err != nil {
		t.Fatalf("select stale owner recovery: %v", err)
	}
	if status != QueueStatusSucceeded || attempts != 2 {
		t.Fatalf("stale owner recovery = status %s attempts %d", status, attempts)
	}
}

func TestRunnerRecoveryReconcilesSucceededDiscoveryRunAtMaxAttempts(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 1)
	staleLockedAt := fixture.Store.Now().Add(-time.Hour)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    attempt_count = max_attempts,
		    locked_by = 'lost-response-runner',
		    locked_at = $3,
		    last_error = 'old provider response details',
		    dead_letter_at = $3
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, staleLockedAt); err != nil {
		t.Fatalf("seed stale max-attempt discovery lease: %v", err)
	}
	finishedAt := fixture.Store.Now().Add(-time.Minute)
	runID := insertDiscoveryRecoveryRun(t, ctx, fixture, queueID, "succeeded", 1, &finishedAt)

	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	var status string
	var attempts int
	var lockedBy, lastError *string
	var lockedAt, deadLetterAt *time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count, locked_by, locked_at, last_error, dead_letter_at
		FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &attempts, &lockedBy, &lockedAt, &lastError, &deadLetterAt); err != nil {
		t.Fatalf("select reconciled queue row: %v", err)
	}
	if status != QueueStatusSucceeded || attempts != 1 || lockedBy != nil || lockedAt != nil || lastError != nil || deadLetterAt != nil {
		t.Fatalf("reconciled queue = %s/%d/%v/%v/%v/%v", status, attempts, lockedBy, lockedAt, lastError, deadLetterAt)
	}
	if client.calls != 0 {
		t.Fatalf("succeeded replay client calls = %d, want 0", client.calls)
	}
	var runStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status FROM discovery_ingestion_runs WHERE id = $1::uuid
	`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("select succeeded durable run: %v", err)
	}
	if runStatus != "succeeded" {
		t.Fatalf("durable run status = %q, want succeeded", runStatus)
	}
}

func TestRunnerRecoveryInvalidatesRunningDiscoveryGeneration(t *testing.T) {
	for _, tt := range []struct {
		name         string
		maxAttempts  int
		wantQueue    string
		wantAttempts int
		wantClaim    bool
	}{
		{name: "requeue", maxAttempts: 3, wantQueue: QueueStatusSucceeded, wantAttempts: 2, wantClaim: true},
		{name: "dead letter", maxAttempts: 1, wantQueue: QueueStatusDeadLettered, wantAttempts: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, tt.maxAttempts)
			if _, err := fixture.Store.Pool.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2, attempt_count = 1,
				    locked_by = 'old-python-owner', locked_at = $3
				WHERE id = $1::uuid
			`, queueID, QueueStatusRunning, fixture.Store.Now().Add(-time.Hour)); err != nil {
				t.Fatalf("seed stale discovery queue generation: %v", err)
			}
			const oldGeneration = 7
			runID := insertDiscoveryRecoveryRun(t, ctx, fixture, queueID, "running", oldGeneration, nil)

			var statusBeforeLate string
			var generationBeforeLate int
			var finishedBeforeLate *time.Time
			var errorBeforeLate *string
			var lateRows int64 = -1
			lateTerminalUpdate := func() error {
				if err := fixture.Store.Pool.QueryRow(ctx, `
					SELECT status, attempt_count, finished_at, last_error_code
					FROM discovery_ingestion_runs WHERE id = $1::uuid
				`, runID).Scan(&statusBeforeLate, &generationBeforeLate, &finishedBeforeLate, &errorBeforeLate); err != nil {
					return err
				}
				result, err := fixture.Store.Pool.Exec(ctx, `
					UPDATE discovery_ingestion_runs
					SET status = 'succeeded', finished_at = $3, last_error_code = NULL
					WHERE id = $1::uuid
					  AND status = 'running'
					  AND attempt_count = $2
				`, runID, oldGeneration, fixture.Store.Now().Add(time.Minute))
				if err != nil {
					return err
				}
				lateRows = result.RowsAffected()
				return nil
			}

			client := &recordingDiscoveryClient{}
			if tt.wantClaim {
				client.ingest = func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
					if err := lateTerminalUpdate(); err != nil {
						return DiscoveryObservation{}, err
					}
					return discoveryObservationForTest(request), nil
				}
			}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
				t.Fatalf("runOnce: %v", err)
			}
			if !tt.wantClaim {
				if err := lateTerminalUpdate(); err != nil {
					t.Fatalf("late durable terminal update: %v", err)
				}
			}

			if statusBeforeLate != "failed" || generationBeforeLate != oldGeneration || finishedBeforeLate == nil || !finishedBeforeLate.Equal(fixture.Store.Now()) || errorBeforeLate == nil || *errorBeforeLate != "discovery_lease_recovered" {
				t.Fatalf("durable run before late update = %s/%d/%v/%v", statusBeforeLate, generationBeforeLate, finishedBeforeLate, errorBeforeLate)
			}
			if lateRows != 0 {
				t.Fatalf("late generation terminal rows = %d, want 0", lateRows)
			}
			var runStatus string
			if err := fixture.Store.Pool.QueryRow(ctx, `SELECT status FROM discovery_ingestion_runs WHERE id = $1::uuid`, runID).Scan(&runStatus); err != nil {
				t.Fatalf("select invalidated durable run: %v", err)
			}
			if runStatus != "failed" {
				t.Fatalf("durable run status after late update = %q, want failed", runStatus)
			}

			var queueStatus string
			var attempts int
			var deadLetterAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, attempt_count, dead_letter_at
				FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&queueStatus, &attempts, &deadLetterAt); err != nil {
				t.Fatalf("select recovered queue generation: %v", err)
			}
			if queueStatus != tt.wantQueue || attempts != tt.wantAttempts {
				t.Fatalf("queue recovery = %s/%d, want %s/%d", queueStatus, attempts, tt.wantQueue, tt.wantAttempts)
			}
			if (tt.wantQueue == QueueStatusDeadLettered) != (deadLetterAt != nil) {
				t.Fatalf("queue dead_letter_at = %v for status %s", deadLetterAt, queueStatus)
			}
			wantCalls := 0
			if tt.wantClaim {
				wantCalls = 1
			}
			if client.calls != wantCalls {
				t.Fatalf("discovery client calls = %d, want %d", client.calls, wantCalls)
			}
		})
	}
}

func enqueueDiscoveryRecoveryItem(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture, maxAttempts int) string {
	t.Helper()
	channelID := fixture.ChannelID
	bucket := "2026-05-21-18"
	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-recovery:" + t.Name(),
		Payload: map[string]any{
			"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
		},
		Priority: 80, ChannelProfileID: &channelID, MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("enqueue discovery recovery item: %v", err)
	}
	return queueID
}

func insertDiscoveryRecoveryRun(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	queueID string,
	status string,
	generation int,
	finishedAt *time.Time,
) string {
	t.Helper()
	var runID string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		INSERT INTO discovery_ingestion_runs (
			id, channel_profile_id, queue_item_id, source, scheduler_bucket,
			query_version, status, attempt_count, query_count, created_count,
			refreshed_count, expired_count, quota_units_estimated,
			policy_snapshot_json, started_at, finished_at, last_error_code
		) VALUES (
			gen_random_uuid(), $1::uuid, $2::uuid, 'youtube_search', '2026-05-21-18',
			'youtube-lane-keyword-v1', $3, $4, 0, 0, 0, 0, 0,
			'{}'::json, $5, $6, NULL
		)
		RETURNING id::text
	`, fixture.ChannelID, queueID, status, generation, fixture.Store.Now().Add(-time.Hour), finishedAt).Scan(&runID); err != nil {
		t.Fatalf("insert discovery recovery run: %v", err)
	}
	return runID
}

func stringPointer(value string) *string {
	return &value
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func TestRunnerRunContinuesAfterDiscoveryLeaseLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, tt := range []struct {
		name     string
		seedPoll bool
	}{
		{name: "initial poll"},
		{name: "timer poll", seedPoll: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(context.Background())
			fixture.InsertChannelWithLaneAccountSeed(context.Background())
			channelID := fixture.ChannelID
			bucket := "2026-07-21-18"

			if tt.seedPoll {
				if _, err := fixture.Store.Enqueue(context.Background(), EnqueueOptions{
					Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner-lease-loss-seed:" + channelID,
					Payload: map[string]any{
						"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
					},
					Priority: 70, ChannelProfileID: &channelID,
				}); err != nil {
					t.Fatalf("enqueue seed: %v", err)
				}
			}
			targetID, err := fixture.Store.Enqueue(context.Background(), EnqueueOptions{
				Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner-lease-loss-target:" + tt.name + ":" + channelID,
				Payload: map[string]any{
					"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
				},
				Priority: 80, ChannelProfileID: &channelID,
			})
			if err != nil {
				t.Fatalf("enqueue target: %v", err)
			}

			initialPollCompleted := make(chan struct{})
			leaseReplaced := make(chan struct{}, 1)
			calls := 0
			client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
				calls++
				if tt.seedPoll && calls == 1 {
					close(initialPollCompleted)
					return discoveryObservationForTest(request), nil
				}
				result, err := fixture.Store.Pool.Exec(context.Background(), `
					UPDATE channel_ops_queue_items
					SET locked_by = 'replacement-worker', locked_at = locked_at + INTERVAL '1 second'
					WHERE id = $1::uuid AND status = $2
				`, targetID, QueueStatusRunning)
				if err != nil {
					return DiscoveryObservation{}, err
				}
				if result.RowsAffected() != 1 {
					return DiscoveryObservation{}, errors.New("target discovery lease was not running")
				}
				leaseReplaced <- struct{}{}
				return discoveryObservationForTest(request), nil
			}}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			runner := &Runner{Config: Config{RunnerPollSeconds: 1}, Store: fixture.Store, Handlers: handler}
			errCh := make(chan error, 1)
			go func() { errCh <- runner.Run(ctx) }()

			if tt.seedPoll {
				select {
				case <-initialPollCompleted:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("initial poll did not complete before timer poll")
				}
			}
			select {
			case <-leaseReplaced:
			case <-time.After(2 * time.Second):
				t.Fatal("runner did not reach the discovery lease replacement")
			}
			select {
			case err := <-errCh:
				t.Fatalf("Run returned after lease loss: %v", err)
			case <-time.After(100 * time.Millisecond):
			}

			cancel()
			select {
			case err := <-errCh:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run returned %v, want context.Canceled", err)
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Run did not return after cancellation")
			}
		})
	}
}

func TestShouldRunSchedulerHonorsPollSeconds(t *testing.T) {
	lastRun := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)

	if ShouldRunScheduler(lastRun, lastRun.Add(59*time.Second), 60) {
		t.Fatal("scheduler should not run before configured poll interval")
	}
	if !ShouldRunScheduler(lastRun, lastRun.Add(60*time.Second), 60) {
		t.Fatal("scheduler should run at configured poll interval")
	}
	if !ShouldRunScheduler(time.Time{}, lastRun, 60) {
		t.Fatal("scheduler should run when it has not run yet")
	}
}

func TestConfigEffectivePollSecondsUsesDaytimeThrottleWindow(t *testing.T) {
	cfg := validConfig()
	cfg.ThrottleEnabled = true
	cfg.ThrottleTimeZone = "America/Los_Angeles"
	cfg.ThrottleStartHour = 8
	cfg.ThrottleEndHour = 24
	cfg.ThrottleRunnerPollSeconds = 300
	cfg.ThrottleSchedulerPollSeconds = 1800

	daytimePacific := time.Date(2026, 6, 7, 17, 0, 0, 0, time.UTC) // 10:00 PDT
	if got := cfg.EffectiveRunnerPollSeconds(daytimePacific); got != 300 {
		t.Fatalf("daytime runner poll = %d, want 300", got)
	}
	if got := cfg.EffectiveSchedulerPollSeconds(daytimePacific); got != 1800 {
		t.Fatalf("daytime scheduler poll = %d, want 1800", got)
	}

	overnightPacific := time.Date(2026, 6, 7, 8, 0, 0, 0, time.UTC) // 01:00 PDT
	if got := cfg.EffectiveRunnerPollSeconds(overnightPacific); got != 5 {
		t.Fatalf("overnight runner poll = %d, want 5", got)
	}
	if got := cfg.EffectiveSchedulerPollSeconds(overnightPacific); got != 60 {
		t.Fatalf("overnight scheduler poll = %d, want 60", got)
	}
}

func TestNewRunnerAppliesQueueMaxAttemptsConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	t.Setenv("CHANNELOPS_RUNNER_ID", "channelops-go@test:1")
	cfg := LoadConfig()
	cfg.LiveMode = false
	cfg.MaxQueueAttempts = 6

	runner, err := NewRunner(ctx, cfg)
	if err != nil {
		t.Skipf("ChannelOps runner test requires reachable DATABASE_URL %q: %v", cfg.DatabaseURL, err)
	}
	defer runner.Close()

	if runner.Store.DefaultMaxAttempts != 6 {
		t.Fatalf("DefaultMaxAttempts = %d, want 6", runner.Store.DefaultMaxAttempts)
	}
	if runner.Leadership == nil {
		t.Fatal("NewRunner did not configure leadership")
	}
	if status := runner.Leadership.Status(); status.Role != LeaderRoleStandby {
		t.Fatalf("new runner leader role = %q, want standby", status.Role)
	}
}
