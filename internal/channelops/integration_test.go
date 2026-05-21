package channelops

import (
	"context"
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
		item, err := f.Store.ClaimNextForKinds(ctx, "channelops-integration-test", handler.ClaimableKinds())
		if err != nil {
			f.T.Fatalf("ClaimNextForKinds: %v", err)
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

type fakeAutoFlow struct{}

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

func (fakeAutoFlow) ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error) {
	return AutoFlowExecuteObservation{
		RunID:  "00000000-0000-0000-0000-000000000201",
		JobID:  "00000000-0000-0000-0000-000000000301",
		Status: "running",
	}, nil
}

func (fakeAutoFlow) GetJob(ctx context.Context, jobID string) (AutoFlowJobObservation, error) {
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
