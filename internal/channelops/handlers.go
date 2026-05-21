package channelops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type PDSDecider interface {
	Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error)
}

type HandlerService struct {
	Store    *Store
	PDS      PDSDecider
	AutoFlow AutoFlowClient
	YouTube  YouTubeClient
	Config   Config
}

type PlanResult struct {
	NextState      string
	BlockedByGuard string
	EnqueueExecute bool
}

func PlanDecisionResult(decision PDSDecision) PlanResult {
	switch decision.Verdict {
	case "allow":
		return PlanResult{NextState: TaskPlanning, EnqueueExecute: true}
	case "block":
		return PlanResult{NextState: TaskHeld, BlockedByGuard: "pds_blocked"}
	default:
		return PlanResult{NextState: TaskHeld, BlockedByGuard: "pds_flagged_for_review"}
	}
}

func (h HandlerService) Ready() bool {
	return h.ReadinessError() == nil
}

func (h HandlerService) ReadinessError() error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	if h.PDS == nil {
		return errors.New("pds client is not configured")
	}
	if h.AutoFlow == nil {
		return errors.New("autoflow client is not configured")
	}
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}
	return nil
}

func (h HandlerService) ClaimableKinds() []string {
	if !h.Ready() {
		return []string{}
	}
	return []string{
		QueueAgentTick,
		QueuePlanTask,
		QueueExecuteTask,
		QueueObserveJob,
		QueuePublishTask,
		QueuePromotePublication,
		QueueReconcilePublication,
		QueueCollectMetrics,
		QueueAccountHealth,
	}
}

func (h HandlerService) Handle(ctx context.Context, item QueueItemRow) error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	switch item.Kind {
	case QueueAgentTick:
		return h.HandleAgentTick(ctx, item)
	case QueuePlanTask:
		return h.HandlePlanTask(ctx, item)
	case QueueExecuteTask:
		return h.HandleExecuteTask(ctx, item)
	case QueueObserveJob:
		return h.HandleObserveJob(ctx, item)
	case QueuePublishTask:
		return h.HandlePublishTask(ctx, item)
	case QueuePromotePublication:
		return h.HandlePromotePublication(ctx, item)
	case QueueReconcilePublication:
		return h.HandleReconcilePublication(ctx, item)
	case QueueCollectMetrics:
		return h.HandleCollectMetrics(ctx, item)
	case QueueAccountHealth:
		return h.HandleAccountHealth(ctx, item)
	default:
		return fmt.Errorf("unknown ChannelOps queue kind: %s", item.Kind)
	}
}

func (h HandlerService) HandleAgentTick(ctx context.Context, item QueueItemRow) error {
	channelID, _ := item.PayloadJSON["channel_id"].(string)
	bucket := firstString(item.PayloadJSON, "bucket", "scheduler_bucket")
	if bucket == "" {
		bucket = SchedulerBucket(h.Store.Now(), 60)
	}
	if channelID == "" {
		return errors.New("agent_tick payload missing channel_id")
	}
	return h.Store.RunTick(ctx, channelID, bucket, h)
}

func (h HandlerService) HandlePlanTask(ctx context.Context, item QueueItemRow) error {
	if h.AutoFlow == nil {
		return errors.New("autoflow client is not configured")
	}
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	if taskID == "" {
		return errors.New("plan_task payload missing production_task_id")
	}
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	observation, err := h.AutoFlow.PlanTask(ctx, task, AutoFlowRequestForTask(task))
	if err != nil {
		return err
	}
	if observation.UploadNodeCount != 1 {
		return h.Store.HoldTaskWithPlan(ctx, task.ID, observation.PlanID, "missing_youtube_upload_node", "AutoFlow plan must contain exactly one youtube_upload node", "plan_task")
	}
	if task.ApprovalMode == ApprovalHuman {
		return h.Store.HoldTaskWithPlan(ctx, task.ID, observation.PlanID, "human_approval_required", "AutoFlow plan requires human approval before execution", "plan_task_human_approval")
	}
	if h.PDS == nil {
		return errors.New("pds client is not configured")
	}
	decision, err := h.PDS.Decide(ctx, PDSDecisionRequest{
		ActorID:    task.TargetAccountID,
		ActionType: "plan_approval",
		Platform:   "youtube",
		Content:    map[string]any{"title": task.TitleSeed, "description": task.Prompt},
		Context:    map[string]any{"production_task_id": task.ID, "autoflow_plan_id": observation.PlanID},
	})
	if err != nil {
		return err
	}
	result := PlanDecisionResult(decision)
	if !result.EnqueueExecute {
		return h.Store.HoldTaskWithPlanAndPDS(ctx, task.ID, observation.PlanID, result.BlockedByGuard, decision, "plan_task_pds")
	}
	if err := h.AutoFlow.ApprovePlan(ctx, observation.PlanID, map[string]any{"decision_id": decision.DecisionID, "verdict": decision.Verdict}); err != nil {
		return err
	}
	return h.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, observation.PlanID, observation.PlanPayload, item.ID)
}

func (h HandlerService) HandleExecuteTask(ctx context.Context, item QueueItemRow) error {
	if h.AutoFlow == nil {
		return errors.New("autoflow client is not configured")
	}
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	if taskID == "" {
		return errors.New("execute_task payload missing production_task_id")
	}
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	if runID, jobID, ok := ExistingExecution(task); ok {
		return h.Store.MarkTaskProducingAndEnqueueObserve(ctx, task.ID, runID, jobID, item.ID)
	}
	observation, err := h.AutoFlow.ExecuteTask(ctx, task, AutoFlowRequestForTask(task))
	if err != nil {
		return err
	}
	if observation.Status == "failed" {
		return h.Store.FailTask(ctx, task.ID, observation.ErrorMessage, "execute_task")
	}
	if strings.TrimSpace(observation.RunID) == "" {
		return h.Store.FailTask(ctx, task.ID, "autoflow execute response missing run_id", "execute_task")
	}
	if strings.TrimSpace(observation.JobID) == "" {
		return h.Store.FailTask(ctx, task.ID, "autoflow execute response missing job_id", "execute_task")
	}
	return h.Store.MarkTaskProducingAndEnqueueObserve(ctx, task.ID, observation.RunID, observation.JobID, item.ID)
}

func ExistingExecution(task ProductionTaskRow) (string, string, bool) {
	if task.AutoFlowRunID == nil || task.JobID == nil {
		return "", "", false
	}
	if *task.AutoFlowRunID == "" || *task.JobID == "" {
		return "", "", false
	}
	return *task.AutoFlowRunID, *task.JobID, true
}

func (h HandlerService) HandleObserveJob(ctx context.Context, item QueueItemRow) error {
	if h.AutoFlow == nil {
		return errors.New("autoflow client is not configured")
	}
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	if taskID == "" {
		return errors.New("observe_job payload missing production_task_id")
	}
	runID, _ := item.PayloadJSON["run_id"].(string)
	if strings.TrimSpace(runID) == "" {
		return errors.New("observe_job payload missing run_id")
	}
	jobID, _ := item.PayloadJSON["job_id"].(string)
	if strings.TrimSpace(jobID) == "" {
		return errors.New("observe_job payload missing job_id")
	}
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task.JobID == nil || *task.JobID == "" {
		return fmt.Errorf("task %s has no AutoFlow job id", task.ID)
	}
	if task.AutoFlowRunID == nil || *task.AutoFlowRunID == "" {
		return fmt.Errorf("task %s has no AutoFlow run id", task.ID)
	}
	if *task.AutoFlowRunID != runID {
		return fmt.Errorf("observe_job payload run_id %s does not match task %s run_id %s", runID, task.ID, *task.AutoFlowRunID)
	}
	if *task.JobID != jobID {
		return fmt.Errorf("observe_job payload job_id %s does not match task %s job_id %s", jobID, task.ID, *task.JobID)
	}
	observation, err := h.AutoFlow.GetJob(ctx, runID, jobID)
	if err != nil {
		return err
	}
	switch observation.Status {
	case "running", "queued", "pending":
		return h.Store.ReenqueueObserve(ctx, task.ID, item.ID, time.Minute)
	case "succeeded":
		return h.Store.MarkTaskReadyToPublish(ctx, task, observation, item.ID)
	case "failed":
		return h.Store.FailTask(ctx, task.ID, observation.ErrorMessage, "observe_job")
	default:
		return h.Store.FailTask(ctx, task.ID, fmt.Sprintf("unknown AutoFlow job status: %s", observation.Status), "observe_job")
	}
}

func (h HandlerService) HandlePublishTask(ctx context.Context, item QueueItemRow) error {
	if h.PDS == nil {
		return errors.New("pds client is not configured")
	}
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	if taskID == "" {
		return errors.New("publish_task payload missing production_task_id")
	}
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	decision, err := h.PDS.Decide(ctx, PDSDecisionRequest{
		ActorID:    task.TargetAccountID,
		ActionType: "publish",
		Platform:   "youtube",
		Content:    map[string]any{"title": task.TitleSeed, "description": task.Prompt},
		Context: map[string]any{
			"production_task_id":  task.ID,
			"platform_content_id": uploadVideoID(task.RationaleJSON),
		},
	})
	if err != nil {
		return err
	}
	if decision.Verdict != "allow" {
		guard := "pds_blocked"
		if decision.Verdict == "flag" {
			guard = "pds_flagged_for_review"
		}
		return h.Store.HoldTaskWithPDS(ctx, task.ID, guard, decision, "publish_task_pds")
	}
	return h.Store.CreateOrUpdatePublicationFromTask(ctx, task, item.ID)
}

func (h HandlerService) HandlePromotePublication(ctx context.Context, item QueueItemRow) error {
	if h.PDS == nil {
		return errors.New("pds client is not configured")
	}
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	if publicationID == "" {
		return errors.New("promote_publication payload missing publication_id")
	}
	publication, err := h.Store.GetPublication(ctx, publicationID)
	if err != nil {
		return err
	}
	targetVisibility := safePromotionVisibility(firstString(item.PayloadJSON, "target_visibility"))
	if targetVisibility == "" {
		targetVisibility = safePromotionVisibility(publication.DesiredPrivacy)
	}
	if targetVisibility == "" {
		targetVisibility = "unlisted"
	}
	scheduledAt := h.Store.Now().UTC()
	if raw := firstString(item.PayloadJSON, "scheduled_at"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return fmt.Errorf("promote_publication scheduled_at: %w", err)
		}
		scheduledAt = parsed.UTC()
	}
	decision, err := h.PDS.Decide(ctx, PDSDecisionRequest{
		ActorID:    publication.AccountID,
		ActionType: "publish",
		Platform:   publication.Platform,
		Content:    map[string]any{"title": publication.Title, "description": publication.Description},
		Context: map[string]any{
			"publication_id":     publication.ID,
			"production_task_id": publication.ProductionTaskID,
			"target_visibility":  targetVisibility,
		},
	})
	if err != nil {
		return err
	}
	if decision.Verdict != "allow" {
		guard := "pds_blocked"
		if decision.Verdict == "flag" {
			guard = "pds_flagged_for_review"
		}
		return h.Store.HoldTaskWithPDS(ctx, publication.ProductionTaskID, guard, decision, "promote_publication_pds")
	}
	if err := h.YouTube.SchedulePublish(ctx, publication.PlatformContentID, scheduledAt, targetVisibility); err != nil {
		return err
	}
	return h.Store.PromotePublication(ctx, publication.ID, targetVisibility, scheduledAt, decision, item.ID, metricsPollDelay(h.Config))
}

func (h HandlerService) HandleReconcilePublication(ctx context.Context, item QueueItemRow) error {
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	if publicationID == "" {
		return errors.New("reconcile_publication payload missing publication_id")
	}
	publication, err := h.Store.GetPublication(ctx, publicationID)
	if err != nil {
		return err
	}
	status, err := h.YouTube.PublicationStatus(ctx, publication.PlatformContentID)
	if err != nil {
		return err
	}
	if isSeverePublicationStatus(status.PublishStatus) {
		return h.Store.MarkPublicationSevereDedup(ctx, publication, status, h.Store.Now())
	}
	return h.Store.UpdatePublicationStatus(ctx, publication.ID, status)
}

func (h HandlerService) HandleCollectMetrics(ctx context.Context, item QueueItemRow) error {
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	if publicationID == "" {
		return errors.New("collect_metrics payload missing publication_id")
	}
	publication, err := h.Store.GetPublication(ctx, publicationID)
	if err != nil {
		return err
	}
	metrics := mapFromAny(item.PayloadJSON["metrics"])
	if !HasRecognizedMetrics(metrics) && publication.PlatformContentID != "" && h.YouTube != nil {
		fetched, err := h.YouTube.FetchMetrics(ctx, publication.PlatformContentID)
		if err == nil && HasRecognizedMetrics(fetched) {
			metrics = fetched
		}
	}
	if !HasRecognizedMetrics(metrics) {
		return h.Store.RequeueOrHoldMetrics(ctx, publication, item, h.Config.MetricsPollMaxAttempts, metricsPollDelay(h.Config))
	}
	stage := SnapshotStageFromPayload(item.PayloadJSON)
	score, fields := MetricsCompleteness(metrics)
	reward, components := RewardScore(metrics, PublicationRewardContext{StablePublication: true})
	return h.Store.UpsertFeedbackSnapshot(ctx, publication, metrics, stage, score, fields, reward, components)
}

func (h HandlerService) HandleAccountHealth(ctx context.Context, item QueueItemRow) error {
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}
	accountID, _ := item.PayloadJSON["account_id"].(string)
	if accountID == "" {
		return errors.New("account_health payload missing account_id")
	}
	health, err := h.YouTube.AccountHealth(ctx, accountID)
	if err != nil {
		return err
	}
	return h.Store.UpdateAccountHealth(ctx, accountID, health)
}

func AutoFlowRequestForTask(task ProductionTaskRow) map[string]any {
	snapshot := jsonObject(task.ChannelConfigSnapshotJSON)
	channel := mapFromAny(snapshot["channel"])
	account := mapFromAny(snapshot["account"])
	lane := mapFromAny(snapshot["lane"])
	laneFormat := mapFromAny(snapshot["lane_format"])
	manualSeed := mapFromAny(snapshot["manual_seed"])
	riskPolicy := mapFromAny(channel["risk_policy_json"])
	manualSeedConstraints := mapFromAny(manualSeed["constraints_json"])
	sourcePlatforms := effectiveSourcePlatforms(task, laneFormat)
	constraints := map[string]any{
		"lane_id":            firstString(lane, "id"),
		"lane_format_id":     firstString(laneFormat, "id"),
		"template_pool_json": stringListFromAny(laneFormat["template_pool_json"]),
		"channelops": map[string]any{
			"production_task_id":        task.ID,
			"channel_profile_id":        task.ChannelProfileID,
			"target_account_id":         task.TargetAccountID,
			"title_seed":                task.TitleSeed,
			"source":                    task.Source,
			"rationale":                 jsonObject(task.RationaleJSON),
			"score_breakdown":           jsonObject(task.ScoreBreakdownJSON),
			"channel_config_version":    task.ChannelConfigVersionSnapshot,
			"transition_history_length": len(task.TransitionHistoryJSON),
		},
	}
	for key, value := range manualSeedConstraints {
		constraints[key] = value
	}

	return map[string]any{
		"prompt":               task.Prompt,
		"target_platforms":     []string{"youtube"},
		"source_platforms":     sourcePlatforms,
		"duration_sec":         positiveAnyInt(laneFormat["target_duration_sec"], 30),
		"aspect_ratio":         normalizeAspectRatio(channel["default_aspect_ratio"]),
		"source_policy":        autoflowSourcePolicy(task),
		"publish_mode":         autoflowPublishMode(laneFormat, account),
		"material_library_ids": stringSlice(task.MaterialLibraryIDsJSON),
		"source_strategy":      normalizeSourceStrategy(firstNonBlank(manualSeed["source_strategy"], manualSeedConstraints["source_strategy"], riskPolicy["source_strategy"])),
		"planning_mode":        normalizePlanningMode(firstNonBlank(manualSeed["planning_mode"], manualSeedConstraints["planning_mode"], riskPolicy["planning_mode"])),
		"constraints":          constraints,
	}
}

func effectiveSourcePlatforms(task ProductionTaskRow, laneFormat map[string]any) []string {
	if len(task.SourcePlatformsJSON) > 0 {
		return stringSlice(task.SourcePlatformsJSON)
	}
	return stringListFromAny(laneFormat["source_platforms_json"])
}

func autoflowSourcePolicy(task ProductionTaskRow) string {
	if taskUsesExternalAssets(task) {
		return "remix_with_review"
	}
	return "owned_only"
}

func autoflowPublishMode(laneFormat map[string]any, account map[string]any) string {
	privacy := safePrivacy(firstNonBlank(laneFormat["default_publish_visibility"], account["default_privacy"]))
	if privacy == "unlisted" {
		return "unlisted_upload"
	}
	return "private_upload"
}

func normalizeSourceStrategy(value any) string {
	requested := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	if requested == "" || requested == "<nil>" {
		return "auto"
	}
	if requested == "external_search" {
		requested = "external_research"
	}
	switch requested {
	case "auto", "input_video", "material_library", "external_research", "generate_missing", "hybrid":
		return requested
	default:
		return "auto"
	}
}

func normalizePlanningMode(value any) string {
	requested := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	switch requested {
	case "auto", "template", "storyboard", "ai_graph":
		return requested
	default:
		return "auto"
	}
}

func normalizeAspectRatio(value any) string {
	requested := strings.TrimSpace(fmt.Sprint(value))
	switch requested {
	case "9:16", "16:9", "1:1", "auto":
		return requested
	default:
		return "9:16"
	}
}

func positiveAnyInt(value any, fallback int) int {
	parsed := intOrDefault(value, fallback)
	if parsed <= 0 {
		return fallback
	}
	return parsed
}

func firstNonBlank(values ...any) any {
	for _, value := range values {
		text := strings.TrimSpace(fmt.Sprint(value))
		if text != "" && text != "<nil>" {
			return value
		}
	}
	return ""
}

func stringListFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return stringSlice(typed)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			text := stringOrFallback(item, "")
			if text != "" {
				out = append(out, text)
			}
		}
		return out
	default:
		return []string{}
	}
}

func TakedownDedupKey(publicationID string, eventType string, at time.Time) string {
	return fmt.Sprintf("%s:%s:%s", publicationID, eventType, at.UTC().Format("2006-01-02"))
}

func isSeverePublicationStatus(status string) bool {
	switch normalizedStatus(status) {
	case "rejected", "removed", "failed", "claim", "claimed", "blocked", "takedown":
		return true
	default:
		return false
	}
}
