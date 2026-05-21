package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

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
	if err := fixture.Store.MarkQueueDone(ctx, item.ID); err != nil {
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
	if err := fixture.Store.MarkQueueDone(ctx, promote.ID); err != nil {
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
	if err := fixture.Store.MarkQueueDone(ctx, promote.ID); err != nil {
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
	if err := fixture.Store.MarkQueueDone(ctx, promote.ID); err != nil {
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
	decodeDecisionAuditObject(t, "pds_decision_json", row.PDSDecisionJSON)
	decodeDecisionAuditObject(t, "learning_context_json", row.LearningContextJSON)
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
	T         *testing.T
	Store     *Store
	ChannelID string
	LaneID    string
	FormatID  string
	AccountID string
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
		if err := f.Store.MarkQueueDone(ctx, item.ID); err != nil {
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
		if err := f.Store.MarkQueueDone(ctx, item.ID); err != nil {
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
		), deleted_audits AS (
			DELETE FROM agent_tick_audits WHERE channel_profile_id = $1::uuid
		), deleted_scheduler AS (
			DELETE FROM internal_scheduler_runs WHERE channel_profile_id = $1::uuid
		)
		DELETE FROM channel_profiles WHERE id = $1::uuid
	`, f.ChannelID)
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

func (fakeAutoFlow) PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error) {
	return AutoFlowPlanObservation{
		PlanID:          "00000000-0000-0000-0000-000000000101",
		UploadNodeCount: 1,
		PlanPayload: map[string]any{
			"clips": []any{map[string]any{"material_id": "mat-1", "asset_id": "00000000-0000-0000-0000-00000000a501"}},
		},
	}, nil
}

func (fakeAutoFlow) ApprovePlan(ctx context.Context, planID string, evidence map[string]any) error {
	return nil
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

type fakeNoMetricsYouTube struct {
	fakeYouTube
}

func (fakeNoMetricsYouTube) FetchMetrics(ctx context.Context, videoID string) (map[string]any, error) {
	return map[string]any{}, nil
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
