package channelops

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type PDSDecider interface {
	Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error)
}

type HandlerService struct {
	Store    *Store
	PDS      PDSDecider
	AutoFlow AutoFlowClient
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
	bucket, _ := item.PayloadJSON["bucket"].(string)
	if bucket == "" {
		bucket = UTCBucket(h.Store.Now())
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
	return h.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, observation.PlanID, item.ID)
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
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task.JobID == nil || *task.JobID == "" {
		return fmt.Errorf("task %s has no AutoFlow job id", task.ID)
	}
	observation, err := h.AutoFlow.GetJob(ctx, *task.JobID)
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
	return task10NotImplemented(item.Kind)
}

func (h HandlerService) HandlePromotePublication(ctx context.Context, item QueueItemRow) error {
	return task10NotImplemented(item.Kind)
}

func (h HandlerService) HandleReconcilePublication(ctx context.Context, item QueueItemRow) error {
	return task10NotImplemented(item.Kind)
}

func (h HandlerService) HandleCollectMetrics(ctx context.Context, item QueueItemRow) error {
	return task10NotImplemented(item.Kind)
}

func (h HandlerService) HandleAccountHealth(ctx context.Context, item QueueItemRow) error {
	return task10NotImplemented(item.Kind)
}

func task10NotImplemented(kind string) error {
	return fmt.Errorf("ChannelOps queue kind %s is not implemented until Task 10", kind)
}

func AutoFlowRequestForTask(task ProductionTaskRow) map[string]any {
	request := map[string]any{
		"production_task_id":        task.ID,
		"channel_profile_id":        task.ChannelProfileID,
		"target_account_id":         task.TargetAccountID,
		"title_seed":                task.TitleSeed,
		"prompt":                    task.Prompt,
		"source":                    task.Source,
		"source_platforms":          stringSlice(task.SourcePlatformsJSON),
		"material_library_ids":      stringSlice(task.MaterialLibraryIDsJSON),
		"rationale":                 jsonObject(task.RationaleJSON),
		"score_breakdown":           jsonObject(task.ScoreBreakdownJSON),
		"channel_config_version":    task.ChannelConfigVersionSnapshot,
		"channel_config_snapshot":   jsonObject(task.ChannelConfigSnapshotJSON),
		"transition_history_length": len(task.TransitionHistoryJSON),
	}
	if task.TopicLaneID != nil {
		request["topic_lane_id"] = *task.TopicLaneID
	}
	if task.LaneFormatID != nil {
		request["lane_format_id"] = *task.LaneFormatID
	}
	if task.ManualSeedID != nil {
		request["manual_seed_id"] = *task.ManualSeedID
	}
	if task.AutoFlowPlanID != nil {
		request["autoflow_plan_id"] = *task.AutoFlowPlanID
	}
	return request
}
