package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
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
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, ""); err != nil {
		t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
	}
	handler.AutoFlow = fakeAutoFlow{executeObservation: AutoFlowExecuteObservation{Status: "failed", ErrorMessage: "execute blocked"}}

	err := handler.HandleExecuteTask(ctx, QueueItemRow{
		ID:          "00000000-0000-0000-0000-000000000401",
		PayloadJSON: map[string]any{"production_task_id": task.ID},
	})
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
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, ""); err != nil {
		t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
	}
	handler.AutoFlow = fakeAutoFlow{executeObservation: AutoFlowExecuteObservation{
		Status: "running",
		JobID:  "00000000-0000-0000-0000-000000000301",
	}}

	err := handler.HandleExecuteTask(ctx, QueueItemRow{
		ID:          "00000000-0000-0000-0000-000000000401",
		PayloadJSON: map[string]any{"production_task_id": task.ID},
	})
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
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, ""); err != nil {
		t.Fatalf("MarkTaskPlanningAndEnqueueExecute: %v", err)
	}
	handler.AutoFlow = fakeAutoFlow{executeObservation: AutoFlowExecuteObservation{
		Status: "running",
		RunID:  "00000000-0000-0000-0000-000000000201",
	}}

	err := handler.HandleExecuteTask(ctx, QueueItemRow{
		ID:          "00000000-0000-0000-0000-000000000401",
		PayloadJSON: map[string]any{"production_task_id": task.ID},
	})
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
	if err := fixture.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, "00000000-0000-0000-0000-000000000101", map[string]any{}, ""); err != nil {
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
