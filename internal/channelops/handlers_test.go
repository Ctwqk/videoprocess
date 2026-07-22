package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPlanDecisionFlagHoldsTask(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-flag", Verdict: "flag"})
	if result.NextState != TaskHeld {
		t.Fatalf("NextState = %s", result.NextState)
	}
	if result.BlockedByGuard != "pds_flagged_for_review" {
		t.Fatalf("BlockedByGuard = %s", result.BlockedByGuard)
	}
	if result.EnqueueExecute {
		t.Fatal("flagged plan must not enqueue execute")
	}
}

func TestPlanDecisionBlockHoldsTask(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-block", Verdict: "block"})
	if result.NextState != TaskHeld || result.BlockedByGuard != "pds_blocked" || result.EnqueueExecute {
		t.Fatalf("result = %#v", result)
	}
}

func TestPlanDecisionAllowEnqueuesExecute(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-allow", Verdict: "allow"})
	if result.NextState != TaskPlanning || !result.EnqueueExecute {
		t.Fatalf("result = %#v", result)
	}
}

func TestExistingExecutionRequiresRunAndJob(t *testing.T) {
	runID := "00000000-0000-0000-0000-000000000201"
	jobID := "00000000-0000-0000-0000-000000000301"
	task := ProductionTaskRow{AutoFlowRunID: &runID, JobID: &jobID}

	gotRunID, gotJobID, ok := ExistingExecution(task)
	if !ok {
		t.Fatal("existing execution should be detected")
	}
	if gotRunID != runID || gotJobID != jobID {
		t.Fatalf("execution = %s/%s", gotRunID, gotJobID)
	}

	task.JobID = nil
	if _, _, ok := ExistingExecution(task); ok {
		t.Fatal("run without job should not count as existing execution")
	}
}

func TestClaimableKindsIncludesOperationalQueueKinds(t *testing.T) {
	handler := HandlerService{
		Store:    &Store{},
		PDS:      fakePDS{decision: PDSDecision{Verdict: "allow"}},
		AutoFlow: fakeAutoFlow{},
		YouTube:  fakeYouTube{},
		Alerts:   &recordingAlertSink{},
	}

	kinds := handler.ClaimableKinds()

	for _, want := range []string{QueueSendAlert, QueueCleanupExpired, QueueLearningRecompute} {
		if !containsString(kinds, want) {
			t.Fatalf("ClaimableKinds() = %#v, missing %s", kinds, want)
		}
	}
}

func TestClaimableKindsIncludesDiscoveryOnlyWhenConfigured(t *testing.T) {
	handler := HandlerService{
		Store:    &Store{},
		PDS:      fakePDS{decision: PDSDecision{Verdict: "allow"}},
		AutoFlow: fakeAutoFlow{},
		YouTube:  fakeYouTube{},
	}
	if handler.ReadinessError() != nil {
		t.Fatalf("optional discovery changed readiness: %v", handler.ReadinessError())
	}
	if containsString(handler.ClaimableKinds(), QueueIngestDiscovery) {
		t.Fatalf("ClaimableKinds = %#v, unexpectedly includes discovery", handler.ClaimableKinds())
	}
	handler.Discovery = &recordingDiscoveryClient{observation: discoveryObservationForTest(discoveryRequestForTest())}
	if !containsString(handler.ClaimableKinds(), QueueIngestDiscovery) {
		t.Fatalf("ClaimableKinds = %#v, missing discovery", handler.ClaimableKinds())
	}
}

func TestHandleIngestDiscoveryRejectsMissingIdentityBeforeClientCall(t *testing.T) {
	request := discoveryRequestForTest()
	tests := []struct {
		name   string
		mutate func(*QueueItemRow)
	}{
		{name: "queued status", mutate: func(item *QueueItemRow) { item.Status = QueueStatusQueued }},
		{name: "missing locked by", mutate: func(item *QueueItemRow) { item.LockedBy = nil }},
		{name: "blank locked by", mutate: func(item *QueueItemRow) { blank := "  "; item.LockedBy = &blank }},
		{name: "long locked by", mutate: func(item *QueueItemRow) { long := strings.Repeat("x", 256); item.LockedBy = &long }},
		{name: "missing locked at", mutate: func(item *QueueItemRow) { item.LockedAt = nil }},
		{name: "zero locked at", mutate: func(item *QueueItemRow) { zero := time.Time{}; item.LockedAt = &zero }},
		{name: "missing attempt", mutate: func(item *QueueItemRow) { item.AttemptCount = 0 }},
		{name: "queue id", mutate: func(item *QueueItemRow) { item.ID = "" }},
		{name: "stored channel", mutate: func(item *QueueItemRow) { item.ChannelProfileID = nil }},
		{name: "payload channel", mutate: func(item *QueueItemRow) { item.PayloadJSON["channel_id"] = "00000000-0000-0000-0000-000000000099" }},
		{name: "source", mutate: func(item *QueueItemRow) { item.PayloadJSON["source"] = "youtube" }},
		{name: "missing bucket", mutate: func(item *QueueItemRow) { delete(item.PayloadJSON, "bucket") }},
		{name: "blank bucket", mutate: func(item *QueueItemRow) { item.PayloadJSON["bucket"] = "  " }},
		{name: "missing scheduler bucket", mutate: func(item *QueueItemRow) { delete(item.PayloadJSON, "scheduler_bucket") }},
		{name: "mismatched bucket", mutate: func(item *QueueItemRow) { item.PayloadJSON["bucket"] = "2026-07-21-19" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &recordingDiscoveryClient{observation: discoveryObservationForTest(request)}
			item := discoveryQueueItemForTest(request)
			tt.mutate(&item)
			err := (HandlerService{Store: &Store{}, Discovery: client}).HandleIngestDiscovery(context.Background(), item)
			if err == nil {
				t.Fatal("HandleIngestDiscovery error = nil")
			}
			if client.calls != 0 {
				t.Fatalf("client calls = %d, want 0", client.calls)
			}
		})
	}
}

func TestDiscoveryRequestFromQueueItemBindsExactLeaseToken(t *testing.T) {
	item := discoveryQueueItemForTest(discoveryRequestForTest())
	item.AttemptCount = 7
	lockedBy := "discovery-exact-owner"
	lockedAt := time.Date(2026, 7, 21, 18, 0, 0, 654321000, time.UTC)
	item.LockedBy = &lockedBy
	item.LockedAt = &lockedAt

	request, err := discoveryRequestFromQueueItem(item)
	if err != nil {
		t.Fatalf("discoveryRequestFromQueueItem: %v", err)
	}
	if request.AttemptCount != item.AttemptCount || request.LockedBy != lockedBy || request.LockedAt != lockedAt {
		t.Fatalf("lease token = %d/%q/%s, want %d/%q/%s", request.AttemptCount, request.LockedBy, request.LockedAt, item.AttemptCount, lockedBy, lockedAt)
	}
}

func TestHandleIngestDiscoveryDefensivelyRejectsClientMismatch(t *testing.T) {
	request := discoveryRequestForTest()
	observation := discoveryObservationForTest(request)
	observation.SchedulerBucket = "mismatch"
	client := &recordingDiscoveryClient{observation: observation}
	err := (HandlerService{Store: &Store{}, Discovery: client}).HandleIngestDiscovery(
		context.Background(), discoveryQueueItemForTest(request),
	)
	if err == nil || !strings.Contains(err.Error(), "scheduler_bucket mismatch") {
		t.Fatalf("HandleIngestDiscovery error = %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("client calls = %d, want 1", client.calls)
	}
}

func TestHandleIngestDiscoverySanitizesClientError(t *testing.T) {
	client := &recordingDiscoveryClient{err: errors.New("credential=top-secret provider-title=private")}
	err := (HandlerService{Store: &Store{}, Discovery: client}).HandleIngestDiscovery(
		context.Background(), discoveryQueueItemForTest(discoveryRequestForTest()),
	)
	if !errors.Is(err, ErrDiscoveryIngestFailed) || err.Error() != "discovery ingestion failed" {
		t.Fatal("HandleIngestDiscovery did not return the fixed discovery error")
	}
}

func TestHandleIngestDiscoveryDoesNotHoldExecutionFenceDuringClientCall(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	channelID := fixture.ChannelID
	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-handler-fence:" + channelID,
		Payload:  map[string]any{"channel_id": channelID, "source": "youtube_search", "bucket": "2026-07-21-18", "scheduler_bucket": "2026-07-21-18"},
		Priority: 80, ChannelProfileID: &channelID,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	item, err := fixture.Store.ClaimNextForKinds(ctx, "discovery-fence-test", []string{QueueIngestDiscovery})
	if err != nil || item == nil {
		t.Fatalf("ClaimNextForKinds = %#v, %v", item, err)
	}
	client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
		tx, err := fixture.Store.Pool.Begin(ctx)
		if err != nil {
			return DiscoveryObservation{}, err
		}
		defer tx.Rollback(ctx)
		var observedChannel string
		if err := tx.QueryRow(ctx, `SELECT id::text FROM channel_profiles WHERE id = $1::uuid FOR UPDATE NOWAIT`, channelID).Scan(&observedChannel); err != nil {
			return DiscoveryObservation{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return DiscoveryObservation{}, err
		}
		return discoveryObservationForTest(request), nil
	}}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := handler.Handle(ctx, *item); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	var status string
	if err := fixture.Store.Pool.QueryRow(ctx, `SELECT status FROM channel_ops_queue_items WHERE id = $1::uuid`, queueID).Scan(&status); err != nil {
		t.Fatalf("select queue status: %v", err)
	}
	if status != QueueStatusRunning {
		t.Fatalf("handler changed queue status to %q", status)
	}
}

type recordingDiscoveryClient struct {
	observation DiscoveryObservation
	err         error
	calls       int
	ingest      func(DiscoveryIngestRequest) (DiscoveryObservation, error)
}

func (c *recordingDiscoveryClient) Ingest(_ context.Context, request DiscoveryIngestRequest) (DiscoveryObservation, error) {
	c.calls++
	if c.ingest != nil {
		return c.ingest(request)
	}
	return c.observation, c.err
}

func discoveryObservationForTest(request DiscoveryIngestRequest) DiscoveryObservation {
	return DiscoveryObservation{
		RunID: "00000000-0000-0000-0000-000000000003", ChannelID: request.ChannelID,
		QueueItemID: request.QueueItemID, Source: request.Source, SchedulerBucket: request.SchedulerBucket,
		Status: "succeeded", QueryCount: 2, CreatedCount: 3, RefreshedCount: 4, ExpiredCount: 5, QuotaUnitsEstimated: 200,
	}
}

func discoveryQueueItemForTest(request DiscoveryIngestRequest) QueueItemRow {
	lockedBy := request.LockedBy
	lockedAt := request.LockedAt
	storedChannel := request.ChannelID
	return QueueItemRow{
		ID: request.QueueItemID, Kind: QueueIngestDiscovery, Status: QueueStatusRunning,
		AttemptCount: request.AttemptCount, LockedBy: &lockedBy, LockedAt: &lockedAt, ChannelProfileID: &storedChannel,
		PayloadJSON: map[string]any{
			"channel_id": request.ChannelID, "source": "youtube_search",
			"bucket": request.SchedulerBucket, "scheduler_bucket": request.SchedulerBucket,
		},
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func TestHandleLearningRecomputeRunsConfiguredWindows(t *testing.T) {
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
	for _, wantStage := range []string{"1h", "6h", "24h"} {
		collect := fixture.ProcessUntilQueueKind(ctx, handler, QueueCollectMetrics)
		if stage := firstString(collect.PayloadJSON, "snapshot_stage"); stage != wantStage {
			t.Fatalf("metric stage = %q, want %q", stage, wantStage)
		}
		collect.PayloadJSON["metrics"] = map[string]any{
			"views":                 1000,
			"likes":                 50,
			"comments":              10,
			"avg_view_duration_sec": 18.0,
		}
		if err := handler.HandleCollectMetrics(ctx, collect); err != nil {
			t.Fatalf("HandleCollectMetrics %s: %v", wantStage, err)
		}
		if err := fixture.Store.MarkQueueDone(ctx, collect); err != nil {
			t.Fatalf("MarkQueueDone %s metrics: %v", wantStage, err)
		}
	}

	err := handler.HandleLearningRecompute(ctx, QueueItemRow{
		Kind:        QueueLearningRecompute,
		PayloadJSON: map[string]any{"channel_id": fixture.ChannelID, "window_days": []any{7.0, 30.0}},
	})

	if err != nil {
		t.Fatalf("HandleLearningRecompute: %v", err)
	}
	var windows []int
	rows, err := fixture.Store.Pool.Query(ctx, `
		SELECT window_days
		FROM learning_states
		WHERE channel_profile_id = $1::uuid
		ORDER BY window_days
	`, fixture.ChannelID)
	if err != nil {
		t.Fatalf("query learning windows: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var window int
		if err := rows.Scan(&window); err != nil {
			t.Fatalf("scan window: %v", err)
		}
		windows = append(windows, window)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("learning windows rows: %v", err)
	}
	if !reflect.DeepEqual(windows, []int{7, 30}) {
		t.Fatalf("learning windows = %#v, want [7 30]", windows)
	}
}

func TestHandleAgentTickReadsSchedulerBucketPayload(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	item := QueueItemRow{
		Kind:        QueueAgentTick,
		PayloadJSON: map[string]any{"channel_id": fixture.ChannelID, "scheduler_bucket": "2026-05-21-18-15"},
	}

	if err := handler.HandleAgentTick(ctx, item); err != nil {
		t.Fatalf("HandleAgentTick: %v", err)
	}

	var tickID string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT tick_id
		FROM agent_tick_audits
		WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&tickID); err != nil {
		t.Fatalf("select tick audit: %v", err)
	}
	if tickID != "tick:"+fixture.ChannelID+":2026-05-21-18-15" {
		t.Fatalf("tick_id = %q", tickID)
	}
}

func TestAgentTickPlanDelaySecondsIsBounded(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    time.Duration
		wantErr bool
	}{
		{name: "absent", value: nil, want: 0},
		{name: "json number", value: float64(300), want: 300 * time.Second},
		{name: "integer", value: 300, want: 300 * time.Second},
		{name: "negative", value: -1, wantErr: true},
		{name: "too large", value: 3601, wantErr: true},
		{name: "fraction", value: 300.5, wantErr: true},
		{name: "boolean", value: true, wantErr: true},
		{name: "string", value: "300", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := map[string]any{}
			if tt.value != nil {
				payload["plan_delay_seconds"] = tt.value
			}
			got, err := agentTickPlanDelay(payload)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("agentTickPlanDelay(%#v) error = nil", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("agentTickPlanDelay(%#v): %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("agentTickPlanDelay(%#v) = %s, want %s", tt.value, got, tt.want)
			}
		})
	}
}

func TestAgentTickOptionsRequireGuardedCanaryAuthority(t *testing.T) {
	runID := "00000000-0000-0000-0000-00000000cafe"
	valid, err := agentTickOptionsFromPayload(map[string]any{
		"plan_delay_seconds":           float64(300),
		"pause_intake_after_selection": true,
		"canary_run_id":                runID,
	})
	if err != nil {
		t.Fatalf("valid guarded options: %v", err)
	}
	if valid.PlanDelay != 300*time.Second || !valid.PauseIntakeAfterSelection || valid.CanaryRunID != runID {
		t.Fatalf("valid guarded options = %#v", valid)
	}

	ordinary, err := agentTickOptionsFromPayload(map[string]any{})
	if err != nil {
		t.Fatalf("ordinary options: %v", err)
	}
	if ordinary.PlanDelay != 0 || ordinary.PauseIntakeAfterSelection || ordinary.CanaryRunID != "" {
		t.Fatalf("ordinary options = %#v", ordinary)
	}

	for _, tt := range []struct {
		name    string
		payload map[string]any
	}{
		{name: "string flag", payload: map[string]any{"pause_intake_after_selection": "true"}},
		{name: "numeric flag", payload: map[string]any{"pause_intake_after_selection": 1}},
		{name: "missing run id", payload: map[string]any{"pause_intake_after_selection": true, "plan_delay_seconds": 300}},
		{name: "invalid run id", payload: map[string]any{"pause_intake_after_selection": true, "plan_delay_seconds": 300, "canary_run_id": "invalid"}},
		{name: "missing delay", payload: map[string]any{"pause_intake_after_selection": true, "canary_run_id": runID}},
		{name: "zero delay", payload: map[string]any{"pause_intake_after_selection": true, "plan_delay_seconds": 0, "canary_run_id": runID}},
		{name: "orphan run id", payload: map[string]any{"canary_run_id": runID}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := agentTickOptionsFromPayload(tt.payload); err == nil {
				t.Fatalf("agentTickOptionsFromPayload(%#v) error = nil", tt.payload)
			}
		})
	}
}

func TestHandleAgentTickAtomicallyPausesIntakeAfterOneTask(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	runID := testUUID(t, "guarded-canary-run")
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	item := QueueItemRow{
		Kind: QueueAgentTick,
		PayloadJSON: map[string]any{
			"channel_id":                   fixture.ChannelID,
			"canary_run_id":                runID,
			"plan_delay_seconds":           float64(300),
			"pause_intake_after_selection": true,
		},
	}

	if err := handler.HandleAgentTick(ctx, item); err != nil {
		t.Fatalf("HandleAgentTick: %v", err)
	}

	var taskCount, planCount int
	var pausedAt, haltedAt *time.Time
	var pauseReason, summaryRunID *string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*) FROM production_tasks WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&taskCount); err != nil {
		t.Fatalf("count canary tasks: %v", err)
	}
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_ops_queue_items
		WHERE channel_profile_id = $1::uuid AND kind = 'plan_task'
		  AND run_after = $2::timestamptz
	`, fixture.ChannelID, fixture.Store.Now().UTC().Add(300*time.Second)).Scan(&planCount); err != nil {
		t.Fatalf("count guarded plan rows: %v", err)
	}
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT intake_paused_at, intake_pause_reason, halted_at
		FROM channel_profiles WHERE id = $1::uuid
	`, fixture.ChannelID).Scan(&pausedAt, &pauseReason, &haltedAt); err != nil {
		t.Fatalf("select intake pause: %v", err)
	}
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT decision_summary_json ->> 'canary_run_id'
		FROM agent_tick_audits WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&summaryRunID); err != nil {
		t.Fatalf("select guarded tick audit: %v", err)
	}
	if taskCount != 1 || planCount != 1 {
		t.Fatalf("guarded tick counts = tasks %d, plans %d; want 1, 1", taskCount, planCount)
	}
	if pausedAt == nil || pauseReason == nil || *pauseReason != CanaryIntakePauseReason || haltedAt != nil {
		t.Fatalf("guarded channel state = paused %v, reason %v, halted %v", pausedAt, pauseReason, haltedAt)
	}
	if summaryRunID == nil || *summaryRunID != runID {
		t.Fatalf("guarded tick canary_run_id = %v, want %s", summaryRunID, runID)
	}
}

func TestDirectTickCannotCrossGuardedIntakePause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(context.Background())
	fixture.InsertChannelWithLaneAccountSeed(ctx)

	guardedEntered := make(chan struct{}, 1)
	releaseGuarded := make(chan struct{})
	guardedHandler := fixture.HandlerService(PDSDecision{})
	guardedHandler.PDS = waitingPDS{entered: guardedEntered, release: releaseGuarded}
	channelID := fixture.ChannelID
	queueItemID := testUUID(t, "guarded-queue-item")
	canaryRunID := testUUID(t, "guarded-canary-run")
	guardedDone := make(chan error, 1)
	go func() {
		guardedDone <- guardedHandler.Handle(ctx, QueueItemRow{
			ID:               queueItemID,
			Kind:             QueueAgentTick,
			ChannelProfileID: &channelID,
			PayloadJSON: map[string]any{
				"channel_id":                   fixture.ChannelID,
				"canary_run_id":                canaryRunID,
				"plan_delay_seconds":           float64(300),
				"pause_intake_after_selection": true,
			},
		})
	}()

	select {
	case <-guardedEntered:
	case <-ctx.Done():
		t.Fatal("guarded tick did not reach policy evaluation")
	}

	ordinaryEntered := make(chan struct{}, 1)
	ordinaryHandler := fixture.HandlerService(PDSDecision{})
	ordinaryHandler.PDS = notifyingPDS{entered: ordinaryEntered}
	ordinaryDone := make(chan error, 1)
	go func() {
		ordinaryDone <- fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), ordinaryHandler)
	}()

	ordinaryCrossedFence := false
	select {
	case <-ordinaryEntered:
		ordinaryCrossedFence = true
	case <-time.After(500 * time.Millisecond):
	}
	close(releaseGuarded)

	if err := <-guardedDone; err != nil {
		t.Fatalf("guarded tick: %v", err)
	}
	ordinaryErr := <-ordinaryDone
	if ordinaryCrossedFence {
		t.Fatal("direct ordinary tick evaluated policy while guarded tick held the channel fence")
	}
	if !errors.Is(ordinaryErr, ErrChannelExecutionBlocked) {
		t.Fatalf("direct ordinary tick error = %v, want ErrChannelExecutionBlocked", ordinaryErr)
	}

	var taskCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*) FROM production_tasks WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks after guarded race: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("tasks after guarded race = %d, want exactly 1", taskCount)
	}
}

func TestDirectTickHonorsHaltedChannelFence(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_profiles
		SET halted_at = NOW(), halt_reason = 'direct tick fence test'
		WHERE id = $1::uuid
	`, fixture.ChannelID); err != nil {
		t.Fatalf("halt channel: %v", err)
	}

	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler)
	if !errors.Is(err, ErrChannelExecutionBlocked) {
		t.Fatalf("direct halted tick error = %v, want ErrChannelExecutionBlocked", err)
	}

	var taskCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*) FROM production_tasks WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&taskCount); err != nil {
		t.Fatalf("count halted tasks: %v", err)
	}
	if taskCount != 0 {
		t.Fatalf("halted direct tick created %d tasks", taskCount)
	}
}

type waitingPDS struct {
	entered chan<- struct{}
	release <-chan struct{}
}

func (p waitingPDS) Decide(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	select {
	case <-p.release:
		return PDSDecision{Verdict: "allow", DecisionID: "allow"}, nil
	case <-ctx.Done():
		return PDSDecision{}, ctx.Err()
	}
}

type notifyingPDS struct {
	entered chan<- struct{}
}

func (p notifyingPDS) Decide(context.Context, PDSDecisionRequest) (PDSDecision, error) {
	select {
	case p.entered <- struct{}{}:
	default:
	}
	return PDSDecision{Verdict: "allow", DecisionID: "allow"}, nil
}

func TestGuardedAgentTickRollsBackUnlessExactlyOneTaskIsSelected(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(context.Context, *ChannelOpsFixture)
	}{
		{
			name: "zero tasks",
			setup: func(ctx context.Context, fixture *ChannelOpsFixture) {
				if _, err := fixture.Store.Pool.Exec(ctx, `
					UPDATE channel_profiles SET dry_run = TRUE WHERE id = $1::uuid
				`, fixture.ChannelID); err != nil {
					t.Fatalf("set dry run: %v", err)
				}
			},
		},
		{
			name: "multiple tasks",
			setup: func(ctx context.Context, fixture *ChannelOpsFixture) {
				if _, err := fixture.Store.Pool.Exec(ctx, `
					UPDATE topic_lanes SET max_posts_per_day = 2 WHERE id = $1::uuid
				`, fixture.LaneID); err != nil {
					t.Fatalf("raise lane budget: %v", err)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			tt.setup(ctx, fixture)
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			err := handler.HandleAgentTick(ctx, QueueItemRow{
				Kind: QueueAgentTick,
				PayloadJSON: map[string]any{
					"channel_id":                   fixture.ChannelID,
					"canary_run_id":                testUUID(t, "guarded-canary-run"),
					"plan_delay_seconds":           float64(300),
					"pause_intake_after_selection": true,
				},
			})
			if err == nil || !strings.Contains(err.Error(), "exactly one task") {
				t.Fatalf("guarded tick error = %v, want exactly-one rejection", err)
			}

			var taskCount, planCount int
			var pausedAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT count(*) FROM production_tasks WHERE channel_profile_id = $1::uuid
			`, fixture.ChannelID).Scan(&taskCount); err != nil {
				t.Fatalf("count rolled back tasks: %v", err)
			}
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT count(*) FROM channel_ops_queue_items
				WHERE channel_profile_id = $1::uuid AND kind = 'plan_task'
			`, fixture.ChannelID).Scan(&planCount); err != nil {
				t.Fatalf("count rolled back plans: %v", err)
			}
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT intake_paused_at FROM channel_profiles WHERE id = $1::uuid
			`, fixture.ChannelID).Scan(&pausedAt); err != nil {
				t.Fatalf("select rolled back pause: %v", err)
			}
			if taskCount != 0 || planCount != 0 || pausedAt != nil {
				t.Fatalf("guarded rollback = tasks %d, plans %d, paused %v", taskCount, planCount, pausedAt)
			}
		})
	}
}

func TestHandleAgentTickDelaysPlanTaskForGuardedPreapproval(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	item := QueueItemRow{
		Kind: QueueAgentTick,
		PayloadJSON: map[string]any{
			"channel_id":         fixture.ChannelID,
			"plan_delay_seconds": float64(300),
		},
	}

	if err := handler.HandleAgentTick(ctx, item); err != nil {
		t.Fatalf("HandleAgentTick: %v", err)
	}

	var runAfter time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT run_after
		FROM channel_ops_queue_items
		WHERE channel_profile_id = $1::uuid AND kind = 'plan_task'
	`, fixture.ChannelID).Scan(&runAfter); err != nil {
		t.Fatalf("select plan_task run_after: %v", err)
	}
	want := fixture.Store.Now().UTC().Add(300 * time.Second)
	if !runAfter.Equal(want) {
		t.Fatalf("plan_task run_after = %s, want %s", runAfter, want)
	}
}

func TestHandleAgentTickPrefersBucketPayload(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	item := QueueItemRow{
		Kind: QueueAgentTick,
		PayloadJSON: map[string]any{
			"channel_id":       fixture.ChannelID,
			"bucket":           "2026-05-21-18-30",
			"scheduler_bucket": "2026-05-21-18-15",
		},
	}

	if err := handler.HandleAgentTick(ctx, item); err != nil {
		t.Fatalf("HandleAgentTick: %v", err)
	}

	var tickID string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT tick_id
		FROM agent_tick_audits
		WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&tickID); err != nil {
		t.Fatalf("select tick audit: %v", err)
	}
	if tickID != "tick:"+fixture.ChannelID+":2026-05-21-18-30" {
		t.Fatalf("tick_id = %q", tickID)
	}
}

func TestAutoFlowRequestForTaskBuildsUploadRequestFromSnapshot(t *testing.T) {
	task := representativeAutoFlowRequestTask()

	request := AutoFlowRequestForTask(task)

	if request["prompt"] != "Make a short" {
		t.Fatalf("prompt = %#v", request["prompt"])
	}
	if request["publish_mode"] != "unlisted_upload" {
		t.Fatalf("publish_mode = %#v", request["publish_mode"])
	}
	if request["publish_mode"] == "preview_only" {
		t.Fatal("publish_mode must not default to preview_only")
	}
	if request["duration_sec"] != 45 {
		t.Fatalf("duration_sec = %#v", request["duration_sec"])
	}
	if request["aspect_ratio"] != "16:9" {
		t.Fatalf("aspect_ratio = %#v", request["aspect_ratio"])
	}
	if request["source_strategy"] != "external_research" {
		t.Fatalf("source_strategy = %#v", request["source_strategy"])
	}
	if request["planning_mode"] != "template" {
		t.Fatalf("planning_mode = %#v", request["planning_mode"])
	}
	if got := stringSliceFromAny(request["target_platforms"]); len(got) != 1 || got[0] != "youtube" {
		t.Fatalf("target_platforms = %#v", request["target_platforms"])
	}
	if got := stringSliceFromAny(request["source_platforms"]); len(got) != 1 || got[0] != "bilibili" {
		t.Fatalf("source_platforms = %#v", request["source_platforms"])
	}
	constraints := mapFromAny(request["constraints"])
	if constraints["lane_id"] != "lane-1" || constraints["lane_format_id"] != "format-1" || constraints["tone"] != "dry" {
		t.Fatalf("constraints = %#v", constraints)
	}
	if got := stringSliceFromAny(constraints["template_pool_json"]); len(got) != 1 || got[0] != "template-a" {
		t.Fatalf("template_pool_json = %#v", constraints["template_pool_json"])
	}
}

func TestAutoFlowRequestForTaskMatchesSharedFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/autoflow_request.json")
	if err != nil {
		t.Fatalf("read shared fixture: %v", err)
	}
	var fixture map[string]any
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode shared fixture: %v", err)
	}

	got := normalizeJSONMap(t, AutoFlowRequestForTask(representativeAutoFlowRequestTask()))

	if !reflect.DeepEqual(got, fixture) {
		gotJSON, _ := json.MarshalIndent(got, "", "  ")
		fixtureJSON, _ := json.MarshalIndent(fixture, "", "  ")
		t.Fatalf("AutoFlowRequestForTask drifted from shared fixture\ngot:\n%s\nfixture:\n%s", gotJSON, fixtureJSON)
	}
}

func TestAutoFlowRequestForTaskExternalAssetsUseReviewPolicyAndPrivateDefault(t *testing.T) {
	task := ProductionTaskRow{
		ID:                 "task-1",
		Prompt:             "Make a short",
		UsesExternalAssets: true,
		ChannelConfigSnapshotJSON: map[string]any{
			"channel":     map[string]any{},
			"lane_format": map[string]any{},
		},
	}

	request := AutoFlowRequestForTask(task)

	if request["source_policy"] != "remix_with_review" {
		t.Fatalf("source_policy = %#v", request["source_policy"])
	}
	if request["publish_mode"] != "private_upload" {
		t.Fatalf("publish_mode = %#v", request["publish_mode"])
	}
	if request["duration_sec"] != 30 {
		t.Fatalf("duration_sec = %#v", request["duration_sec"])
	}
	if request["aspect_ratio"] != "9:16" {
		t.Fatalf("aspect_ratio = %#v", request["aspect_ratio"])
	}
}

func TestAutoFlowRequestForTaskOwnedInputAsset(t *testing.T) {
	task := ProductionTaskRow{
		ID:                  "task-1",
		ChannelProfileID:    "channel-1",
		TargetAccountID:     "account-1",
		Prompt:              "Make a short",
		UsesExternalAssets:  true,
		SourcePlatformsJSON: []string{"youtube", "bilibili"},
		ChannelConfigSnapshotJSON: map[string]any{
			"channel":     map[string]any{},
			"lane_format": map[string]any{},
			"manual_seed": map[string]any{
				"constraints_json": map[string]any{
					"input_asset_id":  "00000000-0000-0000-0000-000000000123",
					"source_strategy": "input_video",
					"planning_mode":   "template",
				},
			},
		},
	}

	request := AutoFlowRequestForTask(task)

	if request["input_asset_id"] != "00000000-0000-0000-0000-000000000123" {
		t.Fatalf("input_asset_id = %#v", request["input_asset_id"])
	}
	if request["source_strategy"] != "input_video" {
		t.Fatalf("source_strategy = %#v", request["source_strategy"])
	}
	if request["planning_mode"] != "template" {
		t.Fatalf("planning_mode = %#v", request["planning_mode"])
	}
	if request["source_policy"] != "owned_only" {
		t.Fatalf("source_policy = %#v", request["source_policy"])
	}
	if got := stringSliceFromAny(request["source_platforms"]); len(got) != 0 {
		t.Fatalf("source_platforms = %#v, want no external platforms", got)
	}
	if _, ok := mapFromAny(request["constraints"])["input_asset_id"]; ok {
		t.Fatalf("constraints must not retain input_asset_id: %#v", request["constraints"])
	}
}

func TestAutoFlowRequestForTaskEmptyInputAssetIDPreservesOrdinaryBehavior(t *testing.T) {
	task := representativeAutoFlowRequestTask()
	task.ChannelConfigSnapshotJSON["manual_seed"] = map[string]any{
		"constraints_json": map[string]any{"input_asset_id": ""},
	}

	request := AutoFlowRequestForTask(task)

	if _, ok := request["input_asset_id"]; ok {
		t.Fatalf("empty input_asset_id forwarded: %#v", request["input_asset_id"])
	}
	if request["source_strategy"] != "external_research" {
		t.Fatalf("source_strategy = %#v", request["source_strategy"])
	}
	if got := stringSliceFromAny(request["source_platforms"]); len(got) != 1 || got[0] != "bilibili" {
		t.Fatalf("source_platforms = %#v", got)
	}
}

func TestAutoFlowRequestForTaskInvalidInputAssetIDFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "short", value: "00000000-0000-0000-0000-00000000012"},
		{name: "whitespace", value: "00000000-0000-0000-0000-000000000123 "},
		{name: "invalid_hex", value: "00000000-0000-0000-0000-00000000012G"},
		{name: "truncated", value: "00000000-0000-0000-0000-000000000123"[:35]},
		{name: "newline", value: "00000000-0000-0000-0000-000000000123\n"},
		{name: "uppercase", value: "ABCDEFAB-1234-4ABC-8DEF-ABCDEFABCDEF"},
		{name: "null", value: nil},
		{name: "object", value: map[string]any{}},
		{name: "array", value: []any{}},
		{name: "number", value: 123},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := representativeAutoFlowRequestTask()
			task.ChannelConfigSnapshotJSON["manual_seed"] = map[string]any{
				"constraints_json": map[string]any{"input_asset_id": test.value},
			}

			request := AutoFlowRequestForTask(task)

			if _, ok := request["input_asset_id"]; ok {
				t.Fatalf("invalid input_asset_id forwarded: %#v", request["input_asset_id"])
			}
			if _, ok := mapFromAny(request["constraints"])["input_asset_id"]; ok {
				t.Fatalf("constraints retained invalid input_asset_id: %#v", request["constraints"])
			}
			if request["source_policy"] != "owned_only" {
				t.Fatalf("source_policy = %#v", request["source_policy"])
			}
			if request["source_strategy"] != "input_video" {
				t.Fatalf("source_strategy = %#v", request["source_strategy"])
			}
			if request["planning_mode"] != "template" {
				t.Fatalf("planning_mode = %#v", request["planning_mode"])
			}
			if got := stringSliceFromAny(request["source_platforms"]); len(got) != 0 {
				t.Fatalf("source_platforms = %#v, want no external platforms", got)
			}
		})
	}
}

func TestAutoFlowRequestForTaskInvalidAspectRatioFallsBack(t *testing.T) {
	task := ProductionTaskRow{
		ID:     "task-1",
		Prompt: "Make a short",
		ChannelConfigSnapshotJSON: map[string]any{
			"channel": map[string]any{
				"default_aspect_ratio": "vertical",
			},
			"lane_format": map[string]any{},
		},
	}

	request := AutoFlowRequestForTask(task)

	if request["aspect_ratio"] != "9:16" {
		t.Fatalf("aspect_ratio = %#v, want 9:16", request["aspect_ratio"])
	}
}

func TestHandleExecuteTaskFailsTaskWhenAutoFlowExecutionFails(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	task := fixture.RequireSingleTask(ctx)
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, testApprovalObservation(), ""); err != nil {
		t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
	}
	handler.AutoFlow = fakeAutoFlow{executeObservation: AutoFlowExecuteObservation{Status: "failed", ErrorMessage: "execute blocked"}}

	err := handler.HandleExecuteTask(ctx, testClaimedExecuteQueueItem(task, testExecuteQueuePayload(task.ID)))
	if err != nil {
		t.Fatalf("HandleExecuteTask returned error: %v", err)
	}
	updated, err := fixture.Store.GetProductionTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetProductionTask: %v", err)
	}
	if updated.State != TaskFailed {
		t.Fatalf("state = %s", updated.State)
	}
	if updated.FailureReason == nil || *updated.FailureReason != "execute blocked" {
		t.Fatalf("failure reason = %#v", updated.FailureReason)
	}
}

func TestHandleExecuteTaskRejectsMissingOrMismatchedDurableAuthorityBeforeAutoFlow(t *testing.T) {
	for _, testCase := range []string{"missing-task-snapshot", "mismatched-queue-snapshot"} {
		t.Run(testCase, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
			if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
				t.Fatalf("RunTick: %v", err)
			}
			task := fixture.RequireSingleTask(ctx)
			if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(
				ctx,
				task.ID,
				"00000000-0000-0000-0000-000000000101",
				map[string]any{},
				testApprovalObservation(),
				"",
			); err != nil {
				t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
			}
			payload := map[string]any{
				"production_task_id":              task.ID,
				"autoflow_plan_id":                testApprovalObservation().PlanID,
				"expected_approved_revision_hash": testApprovalObservation().ApprovedRevisionHash,
				"expected_approved_revision":      testApprovalObservation().ApprovedRevision,
			}
			if testCase == "missing-task-snapshot" {
				if _, err := fixture.Store.Pool.Exec(ctx, `
					UPDATE production_tasks
					SET rationale_json = (COALESCE(rationale_json, '{}'::json)::jsonb - 'autoflow_plan_payload')::json
					WHERE id = $1::uuid
				`, task.ID); err != nil {
					t.Fatalf("clear task authority snapshot: %v", err)
				}
			} else {
				payload["expected_approved_revision_hash"] = strings.Repeat("b", 64)
			}
			recorder := &externalCallRecorder{}
			handler.AutoFlow = &recordingAutoFlow{recorder: recorder}

			err := handler.HandleExecuteTask(ctx, testClaimedExecuteQueueItem(task, payload))

			if !errors.Is(err, ErrQueueAuthorityInvalid) {
				t.Fatalf("HandleExecuteTask error = %v, want invalid queue authority", err)
			}
			if recorder.execute.Load() != 0 {
				t.Fatalf("AutoFlow execute calls = %d, want 0", recorder.execute.Load())
			}
		})
	}
}

func TestHandleExecuteTaskFailsTaskWhenAutoFlowExecutionMissingRunID(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	task := fixture.RequireSingleTask(ctx)
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, testApprovalObservation(), ""); err != nil {
		t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
	}
	handler.AutoFlow = fakeAutoFlow{executeObservation: AutoFlowExecuteObservation{
		Status: "running",
		JobID:  "00000000-0000-0000-0000-000000000301",
	}}

	err := handler.HandleExecuteTask(ctx, testClaimedExecuteQueueItem(task, testExecuteQueuePayload(task.ID)))
	if err != nil {
		t.Fatalf("HandleExecuteTask returned error: %v", err)
	}
	assertTaskFailedWithReason(t, fixture.Store, ctx, task.ID, "autoflow execute response missing run_id")
}

func TestHandleExecuteTaskFailsTaskWhenAutoFlowExecutionMissingJobID(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	task := fixture.RequireSingleTask(ctx)
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, testApprovalObservation(), ""); err != nil {
		t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
	}
	handler.AutoFlow = fakeAutoFlow{executeObservation: AutoFlowExecuteObservation{
		Status: "running",
		RunID:  "00000000-0000-0000-0000-000000000201",
	}}

	err := handler.HandleExecuteTask(ctx, testClaimedExecuteQueueItem(task, testExecuteQueuePayload(task.ID)))
	if err != nil {
		t.Fatalf("HandleExecuteTask returned error: %v", err)
	}
	assertTaskFailedWithReason(t, fixture.Store, ctx, task.ID, "autoflow execute response missing job_id")
}

func TestHandleObserveJobRequiresRunIDPayload(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	task := fixture.RequireSingleTask(ctx)
	runID := "00000000-0000-0000-0000-000000000201"
	jobID := "00000000-0000-0000-0000-000000000301"
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, testApprovalObservation(), ""); err != nil {
		t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
	}
	if err := fixture.Store.MarkTaskProducingAndEnqueueObserve(ctx, task.ID, runID, jobID, ""); err != nil {
		t.Fatalf("MarkTaskProducingAndEnqueueObserve: %v", err)
	}
	handler.AutoFlow = fakeAutoFlow{getJobErr: errors.New("should not observe without run id")}

	err := handler.HandleObserveJob(ctx, QueueItemRow{
		ID:          "00000000-0000-0000-0000-000000000401",
		PayloadJSON: map[string]any{"production_task_id": task.ID, "job_id": jobID},
	})
	if err == nil {
		t.Fatal("expected missing run_id error")
	}
	if err.Error() != "observe_job payload missing run_id" {
		t.Fatalf("error = %v", err)
	}
}

func assertTaskFailedWithReason(t *testing.T, store *Store, ctx context.Context, taskID string, wantReason string) {
	t.Helper()
	updated, err := store.GetProductionTask(ctx, taskID)
	if err != nil {
		t.Fatalf("GetProductionTask: %v", err)
	}
	if updated.State != TaskFailed {
		t.Fatalf("state = %s", updated.State)
	}
	if updated.FailureReason == nil || *updated.FailureReason != wantReason {
		t.Fatalf("failure reason = %#v, want %q", updated.FailureReason, wantReason)
	}
}

func testApprovalObservation() AutoFlowApprovalObservation {
	return AutoFlowApprovalObservation{
		PlanID:               "00000000-0000-0000-0000-000000000101",
		ApprovedRevisionHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ApprovedRevision:     1,
	}
}

func testExecuteQueuePayload(taskID string) map[string]any {
	approval := testApprovalObservation()
	return map[string]any{
		"production_task_id":              taskID,
		"autoflow_plan_id":                approval.PlanID,
		"expected_approved_revision_hash": approval.ApprovedRevisionHash,
		"expected_approved_revision":      approval.ApprovedRevision,
	}
}

func testClaimedExecuteQueueItem(task ProductionTaskRow, payload map[string]any) QueueItemRow {
	lockedBy := "test-execute-worker"
	lockedAt := mustTime("2026-07-21T20:15:16Z")
	return QueueItemRow{
		ID:               "00000000-0000-0000-0000-000000000401",
		Kind:             QueueExecuteTask,
		Status:           QueueStatusRunning,
		AttemptCount:     1,
		LockedBy:         &lockedBy,
		LockedAt:         &lockedAt,
		ChannelProfileID: &task.ChannelProfileID,
		PayloadJSON:      payload,
	}
}

func TestTakedownDedupKeyUsesPublicationEventDay(t *testing.T) {
	key := TakedownDedupKey("pub-1", "rejected", mustTime("2026-05-21T17:15:00Z"))
	if key != "pub-1:rejected:2026-05-21" {
		t.Fatalf("key = %s", key)
	}
}

func stringSliceFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			out = append(out, stringOrFallback(item, ""))
		}
		return out
	default:
		return nil
	}
}

func representativeAutoFlowRequestTask() ProductionTaskRow {
	return ProductionTaskRow{
		ID:                     "task-1",
		ChannelProfileID:       "channel-1",
		TargetAccountID:        "account-1",
		Source:                 SourceManualSeed,
		TitleSeed:              "Title",
		Prompt:                 "Make a short",
		SourcePlatformsJSON:    []string{"bilibili"},
		MaterialLibraryIDsJSON: []string{"library-1"},
		ChannelConfigSnapshotJSON: map[string]any{
			"channel": map[string]any{
				"default_aspect_ratio": "16:9",
				"risk_policy_json": map[string]any{
					"source_strategy": "external_search",
				},
			},
			"lane": map[string]any{"id": "lane-1"},
			"lane_format": map[string]any{
				"id":                         "format-1",
				"default_publish_visibility": "unlisted",
				"target_duration_sec":        45,
				"template_pool_json":         []any{"template-a"},
				"source_platforms_json":      []any{"youtube"},
			},
			"manual_seed": map[string]any{
				"planning_mode": "template",
				"constraints_json": map[string]any{
					"tone": "dry",
				},
			},
		},
	}
}

func normalizeJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal JSON: %v", err)
	}
	var normalized map[string]any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		t.Fatalf("unmarshal normalized JSON: %v", err)
	}
	return normalized
}

func TestPromotionVisibilityDoesNotAllowPublic(t *testing.T) {
	if got := safePromotionVisibility("public"); got != "" {
		t.Fatalf("public promotion visibility = %q", got)
	}
	if got := safePromotionVisibility("unlisted"); got != "unlisted" {
		t.Fatalf("unlisted promotion visibility = %q", got)
	}
}

func TestObservedPrivacyAllowsPublic(t *testing.T) {
	if got := observedPrivacy("public"); got != "public" {
		t.Fatalf("observed privacy = %q", got)
	}
}

func mustTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
