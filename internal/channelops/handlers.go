package channelops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type PDSDecider interface {
	Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error)
}

type HandlerService struct {
	Store     *Store
	PDS       PDSDecider
	AutoFlow  AutoFlowClient
	YouTube   YouTubeClient
	Discovery DiscoveryClient
	Alerts    AlertSink
	Config    Config
}

type PlanResult struct {
	NextState      string
	BlockedByGuard string
	EnqueueExecute bool
}

var (
	ErrPromotionOutcomeUncertain = errors.New("promotion outcome uncertain")
	ErrDiscoveryIngestFailed     = errors.New("discovery ingestion failed")
)

type promotionPreparation struct {
	Operation        PromotionOperationRow
	Scope            preparedPublicationSnapshot
	DecisionRequest  PDSDecisionRequest
	TargetVisibility string
	ScheduledAt      time.Time
	NeedsDecision    bool
	Skip             bool
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
	kinds := []string{
		QueueAgentTick,
		QueuePlanTask,
		QueueExecuteTask,
		QueueObserveJob,
		QueuePublishTask,
		QueuePromotePublication,
		QueueReconcilePublication,
		QueueCollectMetrics,
		QueueAccountHealth,
		QueueSendAlert,
		QueueCleanupExpired,
		QueueLearningRecompute,
	}
	if h.Discovery != nil {
		kinds = append(kinds, QueueIngestDiscovery)
	}
	return kinds
}

func (h HandlerService) Handle(ctx context.Context, item QueueItemRow) error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	if networkedQueueKind(item.Kind) {
		return h.dispatch(ctx, item)
	}
	return h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		fencedHandler := h
		fencedHandler.Store = fencedStore
		return fencedHandler.dispatch(ctx, item)
	})
}

func networkedQueueKind(kind string) bool {
	switch kind {
	case QueueAgentTick,
		QueuePlanTask,
		QueueExecuteTask,
		QueueObserveJob,
		QueuePublishTask,
		QueuePromotePublication,
		QueueReconcilePublication,
		QueueCollectMetrics,
		QueueAccountHealth,
		QueueSendAlert,
		QueueIngestDiscovery:
		return true
	default:
		return false
	}
}

func (h HandlerService) dispatch(ctx context.Context, item QueueItemRow) error {
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
	case QueueSendAlert:
		return h.HandleSendAlert(ctx, item)
	case QueueCleanupExpired:
		return h.HandleCleanupExpired(ctx, item)
	case QueueLearningRecompute:
		return h.HandleLearningRecompute(ctx, item)
	case QueueIngestDiscovery:
		return h.HandleIngestDiscovery(ctx, item)
	default:
		return fmt.Errorf("unknown ChannelOps queue kind: %s", item.Kind)
	}
}

func (h HandlerService) HandleIngestDiscovery(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueueIngestDiscovery); err != nil {
		return err
	}
	if h.Discovery == nil {
		return errors.New("discovery client is not configured")
	}
	request, err := discoveryRequestFromQueueItem(item)
	if err != nil {
		return err
	}
	if err := h.withQueueExecutionPhase(ctx, item, func(HandlerService) error {
		return nil
	}); err != nil {
		return err
	}
	observation, err := h.Discovery.Ingest(ctx, request)
	if err != nil {
		return ErrDiscoveryIngestFailed
	}
	if err := validateDiscoveryObservation(request, observation); err != nil {
		return err
	}
	return h.withQueueExecutionPhase(ctx, item, func(HandlerService) error {
		return nil
	})
}

func discoveryRequestFromQueueItem(item QueueItemRow) (DiscoveryIngestRequest, error) {
	if item.Kind != QueueIngestDiscovery {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("kind is invalid")
	}
	if item.Status != QueueStatusRunning || item.AttemptCount < 1 || item.LockedBy == nil || strings.TrimSpace(*item.LockedBy) == "" || len(*item.LockedBy) > 255 || item.LockedAt == nil || item.LockedAt.IsZero() {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("running lease is invalid")
	}
	if !canonicalDiscoveryUUID(item.ID) {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("id is invalid")
	}
	if item.ChannelProfileID == nil || !canonicalDiscoveryUUID(*item.ChannelProfileID) {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("stored channel is invalid")
	}
	payloadChannelID, ok := item.PayloadJSON["channel_id"].(string)
	if !ok || payloadChannelID != *item.ChannelProfileID || !canonicalDiscoveryUUID(payloadChannelID) {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("channel identity is invalid")
	}
	source, ok := item.PayloadJSON["source"].(string)
	if !ok || source != "youtube_search" {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("source is invalid")
	}
	bucket, ok := item.PayloadJSON["scheduler_bucket"].(string)
	if !ok || strings.TrimSpace(bucket) == "" || len(bucket) > 64 {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("scheduler_bucket is invalid")
	}
	payloadBucket, ok := item.PayloadJSON["bucket"].(string)
	if !ok || strings.TrimSpace(payloadBucket) == "" || payloadBucket != bucket {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("bucket identity is invalid")
	}
	return DiscoveryIngestRequest{
		QueueItemID:     item.ID,
		ChannelID:       payloadChannelID,
		Source:          source,
		SchedulerBucket: bucket,
		AttemptCount:    item.AttemptCount,
		LockedBy:        *item.LockedBy,
		LockedAt:        *item.LockedAt,
	}, nil
}

func discoveryQueueAuthorityError(message string) error {
	return fmt.Errorf("%w: discovery queue item %s", ErrQueueAuthorityInvalid, message)
}

func (h HandlerService) HandleAgentTick(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueueAgentTick); err != nil {
		return err
	}
	channelID, _ := item.PayloadJSON["channel_id"].(string)
	bucket := firstString(item.PayloadJSON, "bucket", "scheduler_bucket")
	if bucket == "" {
		bucket = SchedulerBucket(h.Store.Now(), 60)
	}
	if channelID == "" {
		return errors.New("agent_tick payload missing channel_id")
	}
	options, err := agentTickOptionsFromPayload(item.PayloadJSON)
	if err != nil {
		return err
	}
	var preparation tickPreparation
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		prepared, err := fenced.Store.prepareTick(ctx, channelID, bucket, options)
		preparation = prepared
		return err
	}); err != nil {
		return err
	}
	revalidate := func() error {
		return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
			current, err := fenced.Store.prepareTick(ctx, channelID, bucket, options)
			if err != nil {
				return err
			}
			return preparation.validate(current)
		})
	}
	candidates, alerts, err := evaluateTickCandidatePolicyWithRevalidation(
		ctx,
		preparation.Channel,
		preparation.Candidates,
		h,
		revalidate,
	)
	if err != nil {
		return err
	}
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		return fenced.Store.finalizeTick(ctx, preparation, candidates, alerts)
	})
}

type agentTickOptions struct {
	PlanDelay                 time.Duration
	PauseIntakeAfterSelection bool
	CanaryRunID               string
}

func agentTickOptionsFromPayload(payload map[string]any) (agentTickOptions, error) {
	planDelay, err := agentTickPlanDelay(payload)
	if err != nil {
		return agentTickOptions{}, err
	}
	rawPause, hasPause := payload["pause_intake_after_selection"]
	rawRunID, hasRunID := payload["canary_run_id"]
	if !hasPause {
		if hasRunID {
			return agentTickOptions{}, errors.New("agent_tick canary_run_id requires intake pause authority")
		}
		return agentTickOptions{PlanDelay: planDelay}, nil
	}
	pause, ok := rawPause.(bool)
	if !ok {
		return agentTickOptions{}, errors.New("agent_tick pause_intake_after_selection must be boolean")
	}
	if !pause {
		if hasRunID {
			return agentTickOptions{}, errors.New("agent_tick canary_run_id requires intake pause authority")
		}
		return agentTickOptions{PlanDelay: planDelay}, nil
	}
	if planDelay <= 0 {
		return agentTickOptions{}, errors.New("guarded agent_tick requires a positive plan delay")
	}
	runID, ok := rawRunID.(string)
	if !ok {
		return agentTickOptions{}, errors.New("guarded agent_tick requires canary_run_id")
	}
	if err := requireUUID("canary_run_id", runID); err != nil {
		return agentTickOptions{}, fmt.Errorf("guarded agent_tick: %w", err)
	}
	return agentTickOptions{
		PlanDelay:                 planDelay,
		PauseIntakeAfterSelection: true,
		CanaryRunID:               runID,
	}, nil
}

func agentTickPlanDelay(payload map[string]any) (time.Duration, error) {
	raw, ok := payload["plan_delay_seconds"]
	if !ok {
		return 0, nil
	}
	switch raw.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
	default:
		return 0, errors.New("agent_tick plan_delay_seconds must be a numeric integer")
	}
	seconds, ok := intValue(raw)
	if !ok {
		return 0, errors.New("agent_tick plan_delay_seconds must be a numeric integer")
	}
	number, ok := floatValue(raw)
	if !ok || number != float64(seconds) {
		return 0, errors.New("agent_tick plan_delay_seconds must be a numeric integer")
	}
	if seconds < 0 || seconds > 3_600 {
		return 0, errors.New("agent_tick plan_delay_seconds must be from 0 through 3600")
	}
	return time.Duration(seconds) * time.Second, nil
}

func (h HandlerService) HandlePlanTask(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueuePlanTask); err != nil {
		return err
	}
	if h.AutoFlow == nil {
		return errors.New("autoflow client is not configured")
	}
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	if taskID == "" {
		return errors.New("plan_task payload missing production_task_id")
	}
	var prepared preparedTaskSnapshot
	skip := false
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		task, err := fenced.Store.GetProductionTask(ctx, taskID)
		if err != nil {
			return err
		}
		if task.State != TaskSelected {
			skip = true
			return nil
		}
		prepared, err = newPreparedTaskSnapshot(task)
		return err
	}); err != nil {
		return err
	}
	if skip {
		return nil
	}
	observation, err := h.AutoFlow.PlanTask(
		ctx,
		prepared.Task,
		AutoFlowRequestForTask(prepared.Task),
	)
	if err != nil {
		return err
	}
	var decision PDSDecision
	var approval AutoFlowApprovalObservation
	if observation.UploadNodeCount == 1 &&
		prepared.Task.ApprovalMode != ApprovalHuman &&
		!taskUsesExternalAssets(prepared.Task) {
		if h.PDS == nil {
			return errors.New("pds client is not configured")
		}
		if err := h.revalidatePreparedTask(ctx, item, prepared); err != nil {
			return err
		}
		decision, err = h.PDS.Decide(ctx, PDSDecisionRequest{
			ActorID:    prepared.Task.TargetAccountID,
			ActionType: "plan_approval",
			Platform:   "youtube",
			Content: map[string]any{
				"title":       prepared.Task.TitleSeed,
				"description": prepared.Task.Prompt,
			},
			Context: map[string]any{
				"production_task_id": prepared.Task.ID,
				"autoflow_plan_id":   observation.PlanID,
			},
		})
		if err != nil {
			return err
		}
		if PlanDecisionResult(decision).EnqueueExecute {
			if err := h.revalidatePreparedTask(ctx, item, prepared); err != nil {
				return err
			}
			approval, err = h.AutoFlow.ApprovePlan(
				ctx,
				observation.PlanID,
				map[string]any{
					"decision_id": decision.DecisionID,
					"verdict":     decision.Verdict,
				},
			)
			if err != nil {
				return err
			}
		}
	}
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		current, err := fenced.Store.GetProductionTask(ctx, prepared.Task.ID)
		if err != nil {
			return err
		}
		if err := prepared.validate(current); err != nil {
			return err
		}
		if observation.UploadNodeCount != 1 {
			return fenced.Store.HoldTaskWithPlan(
				ctx,
				current.ID,
				observation.PlanID,
				"missing_youtube_upload_node",
				"AutoFlow plan must contain exactly one youtube_upload node",
				"plan_task",
			)
		}
		if current.ApprovalMode == ApprovalHuman || taskUsesExternalAssets(current) {
			return fenced.Store.HoldTaskWithPlan(
				ctx,
				current.ID,
				observation.PlanID,
				"human_approval_required",
				"AutoFlow plan requires human approval before execution",
				"plan_task_human_approval",
			)
		}
		if alert, ok := maybePDSOutageAlert(
			decision,
			current.ChannelProfileID,
			current.ID,
			"plan_approval",
		); ok {
			if _, err := fenced.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
				return err
			}
		}
		result := PlanDecisionResult(decision)
		if !result.EnqueueExecute {
			return fenced.Store.HoldTaskWithPlanAndPDS(
				ctx,
				current.ID,
				observation.PlanID,
				result.BlockedByGuard,
				decision,
				"plan_task_pds",
			)
		}
		return fenced.Store.MarkTaskPlanningAndEnqueueExecute(
			ctx,
			current.ID,
			observation.PlanID,
			observation.PlanPayload,
			approval,
			item.ID,
		)
	})
}

func (h HandlerService) HandleExecuteTask(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueueExecuteTask); err != nil {
		return err
	}
	if h.AutoFlow == nil {
		return errors.New("autoflow client is not configured")
	}
	queueLockedBy, queueLockedAt, err := runningLease(item)
	if err != nil || strings.TrimSpace(queueLockedBy) == "" || queueLockedAt.IsZero() {
		return fmt.Errorf("%w: execute queue item has no valid running lease", ErrQueueAuthorityInvalid)
	}

	var preparedTask ProductionTaskRow
	var preparedSnapshot preparedTaskSnapshot
	shouldExecute := false
	if err := h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		fencedHandler := h
		fencedHandler.Store = fencedStore
		task, execute, err := fencedHandler.prepareExecuteTask(ctx, item)
		if err != nil {
			return err
		}
		preparedTask = task
		shouldExecute = execute
		if !execute {
			return nil
		}
		preparedSnapshot, err = newPreparedTaskSnapshot(task)
		return err
	}); err != nil {
		return err
	}
	if !shouldExecute {
		return nil
	}

	request := AutoFlowRequestForTask(preparedTask)
	request["production_task_id"] = preparedTask.ID
	request["channelops_queue_item_id"] = item.ID
	request["channelops_queue_locked_by"] = queueLockedBy
	request["channelops_queue_locked_at"] = queueLockedAt.UTC().Format(time.RFC3339Nano)
	observation, err := h.AutoFlow.ExecuteTask(ctx, preparedTask, request)
	if err != nil {
		return err
	}
	return h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		fencedHandler := h
		fencedHandler.Store = fencedStore
		return fencedHandler.finalizeExecuteTask(ctx, item, preparedSnapshot, observation)
	})
}

func (h HandlerService) prepareExecuteTask(ctx context.Context, item QueueItemRow) (ProductionTaskRow, bool, error) {
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	if taskID == "" {
		return ProductionTaskRow{}, false, errors.New("execute_task payload missing production_task_id")
	}
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return ProductionTaskRow{}, false, err
	}
	if task.State != TaskPlanning && task.State != TaskProducing {
		return task, false, nil
	}
	if err := validateExecuteTaskAuthority(item, task); err != nil {
		return ProductionTaskRow{}, false, err
	}
	_, _, hasExecution := ExistingExecution(task)
	if task.State != TaskPlanning && !hasExecution {
		return ProductionTaskRow{}, false, fmt.Errorf("%w: producing task has no durable execution", ErrQueueAuthorityInvalid)
	}
	if held, err := h.holdInvalidPreUploadReview(ctx, task, "execute_task_human_review"); held {
		return ProductionTaskRow{}, false, err
	}
	return task, true, nil
}

func (h HandlerService) finalizeExecuteTask(
	ctx context.Context,
	item QueueItemRow,
	prepared preparedTaskSnapshot,
	observation AutoFlowExecuteObservation,
) error {
	taskID := firstString(item.PayloadJSON, "production_task_id")
	if taskID == "" {
		return errors.New("execute_task payload missing production_task_id")
	}
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	if err := validateExecuteTaskAuthority(item, task); err != nil {
		return err
	}
	if err := prepared.validate(task); err != nil && !validDurableExecuteHandoff(prepared, task) {
		return err
	}
	if task.State != TaskPlanning && task.State != TaskProducing {
		return nil
	}
	if runID, jobID, ok := ExistingExecution(task); ok {
		return h.Store.MarkTaskProducingAndEnqueueObserve(ctx, task.ID, runID, jobID, item.ID)
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

func validDurableExecuteHandoff(prepared preparedTaskSnapshot, current ProductionTaskRow) bool {
	if current.State != TaskProducing {
		return false
	}
	if _, _, ok := ExistingExecution(current); !ok {
		return false
	}
	normalized := current
	normalized.State = prepared.Task.State
	normalized.AutoFlowRunID = prepared.Task.AutoFlowRunID
	normalized.JobID = prepared.Task.JobID
	normalized.StateUpdatedAt = prepared.Task.StateUpdatedAt
	normalized.TransitionHistoryJSON = prepared.Task.TransitionHistoryJSON
	return prepared.validate(normalized) == nil
}

func validateExecuteTaskAuthority(item QueueItemRow, task ProductionTaskRow) error {
	if task.AutoFlowPlanID == nil || strings.TrimSpace(*task.AutoFlowPlanID) == "" {
		return fmt.Errorf("%w: execute task has no durable plan id", ErrQueueAuthorityInvalid)
	}
	queuePlanID := strings.TrimSpace(firstString(item.PayloadJSON, "autoflow_plan_id"))
	if queuePlanID == "" || queuePlanID != strings.TrimSpace(*task.AutoFlowPlanID) {
		return fmt.Errorf("%w: execute queue plan does not match task plan", ErrQueueAuthorityInvalid)
	}
	if task.AutoFlowApprovedRevisionHash == nil || task.AutoFlowApprovedRevision == nil {
		return fmt.Errorf("%w: execute task has no durable expected plan authority", ErrQueueAuthorityInvalid)
	}
	queueRevisionHash := strings.TrimSpace(firstString(item.PayloadJSON, "expected_approved_revision_hash"))
	queueRevision, ok := intValue(item.PayloadJSON["expected_approved_revision"])
	if len(queueRevisionHash) != 64 || !ok || queueRevision < 1 {
		return fmt.Errorf("%w: execute queue has no valid expected plan authority", ErrQueueAuthorityInvalid)
	}
	if queueRevisionHash != *task.AutoFlowApprovedRevisionHash || int64(queueRevision) != *task.AutoFlowApprovedRevision {
		return fmt.Errorf("%w: execute queue authority does not match task snapshot", ErrQueueAuthorityInvalid)
	}
	return nil
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
	if err := h.requireExternalPhase(QueueObserveJob); err != nil {
		return err
	}
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
	var prepared preparedTaskSnapshot
	skip := false
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		task, err := fenced.Store.GetProductionTask(ctx, taskID)
		if err != nil {
			return err
		}
		if task.State != TaskProducing {
			skip = true
			return nil
		}
		if task.JobID == nil || *task.JobID == "" {
			return fmt.Errorf("task %s has no AutoFlow job id", task.ID)
		}
		if task.AutoFlowRunID == nil || *task.AutoFlowRunID == "" {
			return fmt.Errorf("task %s has no AutoFlow run id", task.ID)
		}
		if *task.AutoFlowRunID != runID {
			return fmt.Errorf(
				"observe_job payload run_id %s does not match task %s run_id %s",
				runID,
				task.ID,
				*task.AutoFlowRunID,
			)
		}
		if *task.JobID != jobID {
			return fmt.Errorf(
				"observe_job payload job_id %s does not match task %s job_id %s",
				jobID,
				task.ID,
				*task.JobID,
			)
		}
		prepared, err = newPreparedTaskSnapshot(task)
		return err
	}); err != nil {
		return err
	}
	if skip {
		return nil
	}
	observation, err := h.AutoFlow.GetJob(ctx, runID, jobID)
	if err != nil {
		return err
	}
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		task, err := fenced.Store.GetProductionTask(ctx, prepared.Task.ID)
		if err != nil {
			return err
		}
		if err := prepared.validate(task); err != nil {
			return err
		}
		switch observation.Status {
		case "running", "queued", "pending":
			return fenced.Store.ReenqueueObserve(ctx, task.ID, item.ID, time.Minute)
		case "succeeded":
			return fenced.Store.MarkTaskReadyToPublish(ctx, task, observation, item.ID)
		case "failed":
			return fenced.Store.FailTask(ctx, task.ID, observation.ErrorMessage, "observe_job")
		default:
			return fenced.Store.FailTask(
				ctx,
				task.ID,
				fmt.Sprintf("unknown AutoFlow job status: %s", observation.Status),
				"observe_job",
			)
		}
	})
}

func (h HandlerService) HandlePublishTask(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueuePublishTask); err != nil {
		return err
	}
	if h.PDS == nil {
		return errors.New("pds client is not configured")
	}
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	if taskID == "" {
		return errors.New("publish_task payload missing production_task_id")
	}
	var prepared preparedTaskSnapshot
	skip := false
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		task, err := fenced.Store.GetProductionTask(ctx, taskID)
		if err != nil {
			return err
		}
		if task.State != TaskScheduled {
			skip = true
			return nil
		}
		if held, err := fenced.holdInvalidPreUploadReview(
			ctx,
			task,
			"publish_task_human_review",
		); held {
			skip = true
			return err
		}
		prepared, err = newPreparedTaskSnapshot(task)
		return err
	}); err != nil {
		return err
	}
	if skip {
		return nil
	}
	var health *YouTubeAccountHealth
	if h.YouTube != nil {
		observation, err := h.YouTube.AccountHealth(ctx, prepared.Task.TargetAccountID)
		if err == nil {
			health = &observation
		}
		if err := h.revalidatePreparedTask(ctx, item, prepared); err != nil {
			return err
		}
	}
	decision, err := h.PDS.Decide(ctx, PDSDecisionRequest{
		ActorID:    prepared.Task.TargetAccountID,
		ActionType: "publish",
		Platform:   "youtube",
		Content: map[string]any{
			"title":       prepared.Task.TitleSeed,
			"description": prepared.Task.Prompt,
		},
		Context: map[string]any{
			"production_task_id":  prepared.Task.ID,
			"platform_content_id": uploadVideoID(prepared.Task.RationaleJSON),
		},
	})
	if err != nil {
		return err
	}
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		task, err := fenced.Store.GetProductionTask(ctx, prepared.Task.ID)
		if err != nil {
			return err
		}
		if err := prepared.validate(task); err != nil {
			return err
		}
		if health != nil {
			if alert, ok := quotaLowAlert(
				task.ChannelProfileID,
				task.TargetAccountID,
				health.QuotaRemaining,
			); ok {
				if _, err := fenced.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
					return err
				}
			}
		}
		if alert, ok := maybePDSOutageAlert(
			decision,
			task.ChannelProfileID,
			task.ID,
			"publish",
		); ok {
			if _, err := fenced.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
				return err
			}
		}
		if decision.Verdict != "allow" {
			guard := "pds_blocked"
			if decision.Verdict == "flag" {
				guard = "pds_flagged_for_review"
			}
			return fenced.Store.HoldTaskWithPDS(
				ctx,
				task.ID,
				guard,
				decision,
				"publish_task_pds",
			)
		}
		return fenced.Store.CreateOrUpdatePublicationFromTask(ctx, task, item.ID)
	})
}

func (h HandlerService) HandlePromotePublication(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueuePromotePublication); err != nil {
		return err
	}
	if h.PDS == nil {
		return errors.New("pds client is not configured")
	}
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}

	var preparation promotionPreparation
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		prepared, err := fenced.preparePromotion(ctx, item)
		preparation = prepared
		return err
	}); err != nil {
		return err
	}
	if preparation.Skip {
		return nil
	}
	if preparation.NeedsDecision {
		decision, err := h.PDS.Decide(ctx, preparation.DecisionRequest)
		if err != nil {
			return err
		}
		if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
			finalized, err := fenced.finalizePromotionDecision(ctx, item, preparation, decision)
			preparation = finalized
			return err
		}); err != nil {
			return err
		}
		if preparation.Skip {
			return nil
		}
	}
	return h.executePromotionOperation(ctx, item, preparation)
}

func (h HandlerService) preparePromotion(
	ctx context.Context,
	item QueueItemRow,
) (promotionPreparation, error) {
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	if publicationID == "" {
		return promotionPreparation{}, errors.New("promote_publication payload missing publication_id")
	}
	publication, task, err := h.Store.LockPromotionOperatorScope(ctx, publicationID)
	if err != nil {
		return promotionPreparation{}, err
	}
	existingOperation, err := h.Store.GetPromotionOperationForPublication(ctx, publication.ID)
	if err != nil {
		return promotionPreparation{}, err
	}
	if existingOperation != nil {
		if err := validatePromotionOperationAuthority(item, publication, task, *existingOperation); err != nil {
			return promotionPreparation{}, err
		}
		if existingOperation.Status == PromotionFinalized {
			return promotionPreparation{Operation: *existingOperation, Skip: true}, nil
		}
	}
	if task.State != TaskUploadedPrivate && !taskHeldForPromotionUncertainty(task, existingOperation) {
		return promotionPreparation{Skip: true}, nil
	}
	rawTargetVisibility := strings.TrimSpace(firstString(item.PayloadJSON, "target_visibility"))
	targetVisibility := ""
	if rawTargetVisibility != "" {
		targetVisibility = safePromotionVisibility(rawTargetVisibility)
		if targetVisibility == "" {
			return promotionPreparation{}, fmt.Errorf(
				"%w: target visibility must be private or unlisted",
				ErrPromotionOperationConflict,
			)
		}
	} else {
		targetVisibility = safePromotionVisibility(publication.DesiredPrivacy)
		if targetVisibility == "" {
			targetVisibility = "unlisted"
		}
	}
	scheduledAt := h.Store.Now().UTC()
	if raw := firstString(item.PayloadJSON, "scheduled_at"); raw != "" {
		parsed, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return promotionPreparation{}, fmt.Errorf("promote_publication scheduled_at: %w", err)
		}
		scheduledAt = parsed.UTC()
	}
	if existingOperation != nil {
		if targetVisibility != existingOperation.TargetPrivacy {
			return promotionPreparation{}, fmt.Errorf(
				"%w: queued target visibility does not match reserved operation",
				ErrPromotionOperationConflict,
			)
		}
		targetVisibility = existingOperation.TargetPrivacy
		scheduledAt = existingOperation.ScheduledAt
	}
	scope, err := newPreparedPublicationSnapshot(publication, task)
	if err != nil {
		return promotionPreparation{}, err
	}
	if existingOperation != nil {
		switch existingOperation.Status {
		case PromotionReserved, PromotionSubmitting, PromotionUncertain, PromotionConfirmed:
			return promotionPreparation{
				Operation:        *existingOperation,
				Scope:            scope,
				TargetVisibility: targetVisibility,
				ScheduledAt:      scheduledAt,
			}, nil
		default:
			return promotionPreparation{}, fmt.Errorf(
				"%w: unknown promotion operation state %q",
				ErrPromotionOperationConflict,
				existingOperation.Status,
			)
		}
	}
	held, err := h.validatePromotionSafety(ctx, item, publication, task, targetVisibility)
	if held || err != nil {
		return promotionPreparation{Skip: held}, err
	}
	request := PDSDecisionRequest{
		ActorID:    publication.AccountID,
		ActionType: "publish",
		Platform:   publication.Platform,
		Content:    map[string]any{"title": publication.Title, "description": publication.Description},
		Context: map[string]any{
			"publication_id":     publication.ID,
			"production_task_id": publication.ProductionTaskID,
			"target_visibility":  targetVisibility,
		},
	}
	return promotionPreparation{
		Scope:            scope,
		DecisionRequest:  request,
		TargetVisibility: targetVisibility,
		ScheduledAt:      scheduledAt,
		NeedsDecision:    true,
	}, nil
}

func (h HandlerService) finalizePromotionDecision(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
	decision PDSDecision,
) (promotionPreparation, error) {
	publication, task, err := h.Store.LockPromotionOperatorScope(
		ctx,
		prepared.Scope.Publication.ID,
	)
	if err != nil {
		return promotionPreparation{}, err
	}
	if err := prepared.Scope.validate(publication, task); err != nil {
		return promotionPreparation{}, err
	}
	existingOperation, err := h.Store.GetPromotionOperationForPublication(ctx, publication.ID)
	if err != nil {
		return promotionPreparation{}, err
	}
	if existingOperation != nil {
		if err := validatePromotionOperationAuthority(item, publication, task, *existingOperation); err != nil {
			return promotionPreparation{}, err
		}
		prepared.Operation = *existingOperation
		prepared.NeedsDecision = false
		prepared.Skip = existingOperation.Status == PromotionFinalized
		return prepared, nil
	}
	held, err := h.validatePromotionSafety(
		ctx,
		item,
		publication,
		task,
		prepared.TargetVisibility,
	)
	if held || err != nil {
		prepared.Skip = held
		return prepared, err
	}
	channelID := task.ChannelProfileID
	if alert, ok := maybePDSOutageAlert(decision, channelID, publication.ID, "publish"); ok {
		if _, err := h.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
			return promotionPreparation{}, err
		}
	}
	if decision.Verdict != "allow" {
		guard := "pds_blocked"
		if decision.Verdict == "flag" {
			guard = "pds_flagged_for_review"
		}
		prepared.Skip = true
		return prepared, h.Store.HoldTaskWithPDS(
			ctx,
			publication.ProductionTaskID,
			guard,
			decision,
			"promote_publication_pds",
		)
	}
	operation, err := h.Store.ReservePromotionOperation(
		ctx,
		publication,
		item.ID,
		prepared.TargetVisibility,
		prepared.ScheduledAt,
		decision,
	)
	if err != nil {
		return promotionPreparation{}, err
	}
	prepared.Operation = operation
	prepared.NeedsDecision = false
	return prepared, nil
}

func (h HandlerService) validatePromotionSafety(
	ctx context.Context,
	item QueueItemRow,
	publication PublicationRow,
	task ProductionTaskRow,
	targetVisibility string,
) (bool, error) {
	if task.AutoFlowPlanID != nil {
		valid, err := h.Store.ValidPromotionPlanAuthority(ctx, task)
		if err != nil {
			return false, err
		}
		if !valid {
			return true, h.Store.HoldTask(
				ctx,
				task.ID,
				"autoflow_plan_authority_invalid",
				"Publication promotion plan authority is missing, stale, or revoked",
				"promote_publication_plan_authority",
			)
		}
	}
	if held, err := h.holdInvalidPreUploadReview(ctx, task, "promote_publication_human_review"); held {
		return true, err
	}
	if taskUsesExternalAssets(task) || boolValue(item.PayloadJSON["manual_review"]) {
		valid, err := h.Store.ValidPromotionHumanReview(ctx, task, publication, targetVisibility)
		if err != nil {
			return false, err
		}
		if !valid {
			return true, h.Store.HoldTask(
				ctx,
				task.ID,
				"human_review_evidence_invalid",
				"Publication promotion human review evidence is missing or stale",
				"promote_publication_human_review",
			)
		}
	}
	return false, nil
}

func validatePromotionOperationAuthority(
	item QueueItemRow,
	publication PublicationRow,
	task ProductionTaskRow,
	operation PromotionOperationRow,
) error {
	if operation.PublicationID != publication.ID ||
		operation.ProductionTaskID != task.ID ||
		operation.QueueItemID != item.ID ||
		operation.PlatformVideoID != publication.PlatformContentID ||
		safePromotionVisibility(operation.TargetPrivacy) != operation.TargetPrivacy ||
		operation.ScheduledAt.IsZero() {
		return fmt.Errorf("%w: persisted promotion authority changed", ErrPromotionOperationConflict)
	}
	return nil
}

func (h HandlerService) executePromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
) error {
	operation := prepared.Operation
	for {
		switch operation.Status {
		case PromotionFinalized:
			return nil
		case PromotionConfirmed:
			prepared.Operation = operation
			return h.finalizePromotionOperation(ctx, item, prepared)
		case PromotionSubmitting, PromotionUncertain:
			prepared.Operation = operation
			return h.reconcilePromotionOperation(ctx, item, prepared, nil)
		case PromotionReserved:
			claimed, shouldSubmit, skip, err := h.beginPromotionSubmission(
				ctx,
				item,
				prepared,
			)
			if err != nil {
				return err
			}
			if skip {
				return nil
			}
			operation = claimed
			prepared.Operation = operation
			if !shouldSubmit {
				continue
			}
			submitErr := h.YouTube.SchedulePublish(
				ctx,
				operation.PlatformVideoID,
				operation.ScheduledAt,
				operation.TargetPrivacy,
				operation.AttemptKey,
			)
			if submitErr != nil {
				return h.reconcilePromotionOperation(ctx, item, prepared, submitErr)
			}
			confirmed, err := h.confirmPromotionOperation(
				ctx,
				item,
				prepared,
				YouTubePublicationStatus{
					VideoID:       operation.PlatformVideoID,
					PublishStatus: "scheduled",
					Privacy:       operation.TargetPrivacy,
				},
				map[string]any{
					"manager_response": map[string]any{
						"accepted": true,
						"at":       h.Store.Now().UTC().Format(time.RFC3339Nano),
					},
				},
			)
			if err != nil {
				return err
			}
			prepared.Operation = confirmed
			return h.finalizePromotionOperation(ctx, item, prepared)
		default:
			return fmt.Errorf(
				"%w: unknown promotion operation state %q",
				ErrPromotionOperationConflict,
				operation.Status,
			)
		}
	}
}

func (h HandlerService) beginPromotionSubmission(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
) (PromotionOperationRow, bool, bool, error) {
	var operation PromotionOperationRow
	var shouldSubmit bool
	var skip bool
	err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		publication, task, locked, err := fenced.lockPreparedPromotionScope(ctx, item, prepared)
		if err != nil {
			return err
		}
		held, err := fenced.validatePromotionSafety(
			ctx,
			item,
			publication,
			task,
			locked.TargetPrivacy,
		)
		if held || err != nil {
			skip = held
			return err
		}
		operation, shouldSubmit, err = fenced.Store.BeginPromotionSubmission(ctx, locked.ID)
		return err
	})
	return operation, shouldSubmit, skip, err
}

func (h HandlerService) confirmPromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
	status YouTubePublicationStatus,
	evidence map[string]any,
) (PromotionOperationRow, error) {
	var confirmed PromotionOperationRow
	err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		_, _, operation, err := fenced.lockPreparedPromotionScope(ctx, item, prepared)
		if err != nil {
			return err
		}
		confirmed, err = fenced.Store.ConfirmPromotionOperation(
			ctx,
			operation.ID,
			status,
			evidence,
		)
		return err
	})
	return confirmed, err
}

func (h HandlerService) reconcilePromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
	submitErr error,
) error {
	var operation PromotionOperationRow
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		_, _, locked, err := fenced.lockPreparedPromotionScope(ctx, item, prepared)
		operation = locked
		return err
	}); err != nil {
		return err
	}
	prepared.Operation = operation
	if operation.Status == PromotionConfirmed || operation.Status == PromotionFinalized {
		return h.finalizePromotionOperation(ctx, item, prepared)
	}
	status, statusErr := h.YouTube.PublicationStatus(ctx, operation.PlatformVideoID)
	if statusErr == nil &&
		observedPrivacy(status.Privacy) == operation.TargetPrivacy &&
		!isSeverePublicationStatus(status.PublishStatus) {
		confirmed, err := h.confirmPromotionOperation(
			ctx,
			item,
			prepared,
			status,
			map[string]any{
				"status_reconciliation": map[string]any{
					"matched": true,
					"at":      h.Store.Now().UTC().Format(time.RFC3339Nano),
				},
			},
		)
		if err != nil {
			return err
		}
		prepared.Operation = confirmed
		return h.finalizePromotionOperation(ctx, item, prepared)
	}

	reason := "YouTube promotion outcome could not be confirmed"
	if submitErr != nil {
		reason += ": schedule request returned " + submitErr.Error()
	}
	if statusErr != nil {
		reason += "; status unavailable: " + statusErr.Error()
	} else {
		reason += fmt.Sprintf(
			"; observed privacy %q contradicts target %q",
			observedPrivacy(status.Privacy),
			operation.TargetPrivacy,
		)
	}
	reason = boundedPromotionReason(reason)
	var uncertain PromotionOperationRow
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		_, _, locked, err := fenced.lockPreparedPromotionScope(ctx, item, prepared)
		if err != nil {
			return err
		}
		uncertain, err = fenced.Store.MarkPromotionOperationUncertain(
			ctx,
			locked.ID,
			status,
			reason,
		)
		return err
	}); err != nil {
		return err
	}
	prepared.Operation = uncertain
	if uncertain.Status == PromotionConfirmed || uncertain.Status == PromotionFinalized {
		return h.finalizePromotionOperation(ctx, item, prepared)
	}
	if err := h.holdUncertainPromotionOperation(ctx, item, prepared, reason); err != nil {
		return errors.Join(fmt.Errorf("%w: %s", ErrPromotionOutcomeUncertain, reason), err)
	}
	return fmt.Errorf("%w: %s", ErrPromotionOutcomeUncertain, reason)
}

func (h HandlerService) lockPreparedPromotionScope(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
) (PublicationRow, ProductionTaskRow, PromotionOperationRow, error) {
	publication, task, err := h.Store.LockPromotionOperatorScope(
		ctx,
		prepared.Scope.Publication.ID,
	)
	if err != nil {
		return PublicationRow{}, ProductionTaskRow{}, PromotionOperationRow{}, err
	}
	if err := prepared.Scope.validate(publication, task); err != nil {
		return PublicationRow{}, ProductionTaskRow{}, PromotionOperationRow{}, err
	}
	operation, err := h.Store.LockPromotionOperation(ctx, prepared.Operation.ID)
	if err != nil {
		return PublicationRow{}, ProductionTaskRow{}, PromotionOperationRow{}, err
	}
	if operation.ID != prepared.Operation.ID {
		return PublicationRow{}, ProductionTaskRow{}, PromotionOperationRow{}, fmt.Errorf(
			"%w: promotion operation identity changed",
			ErrPromotionOperationConflict,
		)
	}
	if err := validatePromotionOperationAuthority(item, publication, task, operation); err != nil {
		return PublicationRow{}, ProductionTaskRow{}, PromotionOperationRow{}, err
	}
	return publication, task, operation, nil
}

func (h HandlerService) finalizePromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
) error {
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		publication, task, operation, err := fenced.lockPreparedPromotionScope(ctx, item, prepared)
		if err != nil {
			return err
		}
		if operation.Status == PromotionFinalized {
			return nil
		}
		if operation.Status != PromotionConfirmed ||
			operation.PublicationID != publication.ID ||
			operation.ProductionTaskID != task.ID ||
			operation.PlatformVideoID != publication.PlatformContentID ||
			safePromotionVisibility(operation.TargetPrivacy) != operation.TargetPrivacy ||
			operation.ObservedPrivacy == nil ||
			observedPrivacy(*operation.ObservedPrivacy) != operation.TargetPrivacy {
			return fmt.Errorf("%w: finalization authority changed", ErrPromotionOperationConflict)
		}
		if publication.PublishStatus == "rejected" || task.State == TaskRejected {
			return nil
		}
		if task.State != TaskUploadedPrivate && !taskHeldForPromotionUncertainty(task, &operation) {
			return nil
		}
		if task.AutoFlowPlanID != nil {
			valid, err := fenced.Store.ValidPromotionPlanAuthority(ctx, task)
			if err != nil {
				return err
			}
			if !valid {
				return fenced.Store.HoldTask(
					ctx,
					task.ID,
					"autoflow_plan_authority_invalid",
					"Publication promotion plan authority is missing, stale, or revoked",
					"promote_publication_plan_authority",
				)
			}
		}
		if held, err := fenced.holdInvalidPreUploadReview(
			ctx,
			task,
			"promote_publication_human_review",
		); held {
			return err
		}
		if taskUsesExternalAssets(task) || boolValue(item.PayloadJSON["manual_review"]) {
			valid, err := fenced.Store.ValidPromotionHumanReview(
				ctx,
				task,
				publication,
				operation.TargetPrivacy,
			)
			if err != nil {
				return err
			}
			if !valid {
				return fenced.Store.HoldTask(
					ctx,
					task.ID,
					"human_review_evidence_invalid",
					"Publication promotion human review evidence is missing or stale",
					"promote_publication_human_review",
				)
			}
		}
		return fenced.Store.FinalizePromotionOperation(
			ctx,
			operation.ID,
			metricsPollDelay(h.Config),
		)
	})
}

func (h HandlerService) holdUncertainPromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	prepared promotionPreparation,
	reason string,
) error {
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		publication, task, locked, err := fenced.lockPreparedPromotionScope(ctx, item, prepared)
		if err != nil {
			return err
		}
		if locked.Status == PromotionConfirmed || locked.Status == PromotionFinalized {
			return nil
		}
		if locked.PublicationID != publication.ID || locked.ProductionTaskID != task.ID {
			return fmt.Errorf("%w: uncertain operation authority changed", ErrPromotionOperationConflict)
		}
		if task.State != TaskUploadedPrivate && !taskHeldForPromotionUncertainty(task, &locked) {
			return nil
		}
		return fenced.Store.HoldPromotionOperationUncertain(ctx, publication, locked, reason)
	})
}

func (h HandlerService) withStore(store *Store) HandlerService {
	clone := h
	clone.Store = store
	return clone
}

func taskHeldForPromotionUncertainty(
	task ProductionTaskRow,
	operation *PromotionOperationRow,
) bool {
	return operation != nil &&
		task.State == TaskHeld &&
		task.BlockedByGuard != nil &&
		*task.BlockedByGuard == "promotion_outcome_uncertain"
}

func boundedPromotionReason(reason string) string {
	const maxLength = 1_000
	if len(reason) <= maxLength {
		return reason
	}
	return reason[:maxLength]
}

func (h HandlerService) HandleReconcilePublication(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueueReconcilePublication); err != nil {
		return err
	}
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	if publicationID == "" {
		return errors.New("reconcile_publication payload missing publication_id")
	}
	var prepared preparedPublicationSnapshot
	skip := false
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		publication, err := fenced.Store.GetPublication(ctx, publicationID)
		if err != nil {
			return err
		}
		task, err := fenced.Store.GetProductionTask(ctx, publication.ProductionTaskID)
		if err != nil {
			return err
		}
		if task.State == TaskHeld || task.State == TaskFailed || task.State == TaskRejected {
			skip = true
			return nil
		}
		prepared, err = newPreparedPublicationSnapshot(publication, task)
		return err
	}); err != nil {
		return err
	}
	if skip {
		return nil
	}
	status, err := h.YouTube.PublicationStatus(ctx, prepared.Publication.PlatformContentID)
	if err != nil {
		return err
	}
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		publication, err := fenced.Store.GetPublication(ctx, prepared.Publication.ID)
		if err != nil {
			return err
		}
		task, err := fenced.Store.GetProductionTask(ctx, publication.ProductionTaskID)
		if err != nil {
			return err
		}
		if err := prepared.validate(publication, task); err != nil {
			return err
		}
		if isSeverePublicationStatus(status.PublishStatus) {
			if err := fenced.Store.MarkPublicationSevereDedup(
				ctx,
				publication,
				status,
				fenced.Store.Now(),
			); err != nil {
				return err
			}
			_, err := fenced.Store.EnqueueAlert(
				ctx,
				platformRejectedAlert(publication, task.ChannelProfileID, status),
				5,
				item.ID,
			)
			return err
		}
		return fenced.Store.UpdatePublicationStatus(ctx, publication.ID, status)
	})
}

func (h HandlerService) HandleCollectMetrics(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueueCollectMetrics); err != nil {
		return err
	}
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	if publicationID == "" {
		return errors.New("collect_metrics payload missing publication_id")
	}
	var prepared preparedMetricsSnapshot
	skip := false
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		publication, err := fenced.Store.GetPublication(ctx, publicationID)
		if err != nil {
			return err
		}
		task, err := fenced.Store.GetProductionTask(ctx, publication.ProductionTaskID)
		if err != nil {
			return err
		}
		if task.State == TaskHeld || task.State == TaskFailed || task.State == TaskRejected {
			skip = true
			return nil
		}
		var schedule *MetricScheduleRow
		if firstString(item.PayloadJSON, "metric_schedule_id") != "" {
			lockedSchedule, err := fenced.Store.LockMetricScheduleForQueue(ctx, item)
			if err != nil {
				return err
			}
			if lockedSchedule.Status != MetricSchedulePending {
				skip = true
				return nil
			}
			schedule = &lockedSchedule
		}
		prepared, err = newPreparedMetricsSnapshot(publication, task, schedule)
		return err
	}); err != nil {
		return err
	}
	if skip {
		return nil
	}
	metrics := mapFromAny(item.PayloadJSON["metrics"])
	if !HasRecognizedMetrics(metrics) &&
		prepared.Publication.PlatformContentID != "" &&
		h.YouTube != nil {
		fetched, err := h.YouTube.FetchMetrics(ctx, prepared.Publication.PlatformContentID)
		if err == nil && HasRecognizedMetrics(fetched) {
			metrics = fetched
		}
	}
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		publication, err := fenced.Store.GetPublication(ctx, prepared.Publication.ID)
		if err != nil {
			return err
		}
		task, err := fenced.Store.GetProductionTask(ctx, publication.ProductionTaskID)
		if err != nil {
			return err
		}
		var schedule *MetricScheduleRow
		if prepared.Schedule != nil {
			lockedSchedule, err := fenced.Store.LockMetricScheduleForQueue(ctx, item)
			if err != nil {
				return err
			}
			schedule = &lockedSchedule
		}
		if err := prepared.validate(publication, task, schedule); err != nil {
			return err
		}
		if !HasRecognizedMetrics(metrics) {
			if schedule != nil {
				return fenced.Store.RequeueOrExpireMetricSchedule(
					ctx,
					publication,
					*schedule,
					item,
					fenced.Config.MetricsPollMaxAttempts,
					metricsPollDelay(fenced.Config),
				)
			}
			return fenced.Store.RequeueOrHoldMetrics(
				ctx,
				publication,
				item,
				fenced.Config.MetricsPollMaxAttempts,
				metricsPollDelay(fenced.Config),
			)
		}
		score, fields := MetricsCompleteness(metrics)
		reward, components := RewardScore(
			metrics,
			PublicationRewardContext{StablePublication: true},
		)
		if schedule != nil {
			return fenced.Store.CompleteMetricSchedule(
				ctx,
				publication,
				*schedule,
				metrics,
				score,
				fields,
				reward,
				components,
			)
		}
		stage := SnapshotStageFromPayload(item.PayloadJSON)
		return fenced.Store.UpsertFeedbackSnapshot(
			ctx,
			publication,
			metrics,
			stage,
			score,
			fields,
			reward,
			components,
		)
	})
}

func (h HandlerService) HandleAccountHealth(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueueAccountHealth); err != nil {
		return err
	}
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}
	accountID, _ := item.PayloadJSON["account_id"].(string)
	if accountID == "" {
		return errors.New("account_health payload missing account_id")
	}
	var prepared preparedAccountSnapshot
	if err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		account, err := fenced.Store.getPublishingAccount(ctx, accountID)
		if err != nil {
			return err
		}
		prepared, err = newPreparedAccountSnapshot(account)
		return err
	}); err != nil {
		return err
	}
	health, err := h.YouTube.AccountHealth(ctx, accountID)
	if err != nil {
		return err
	}
	return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
		account, err := fenced.Store.getPublishingAccount(ctx, prepared.Account.ID)
		if err != nil {
			return err
		}
		if err := prepared.validate(account); err != nil {
			return err
		}
		if alert, ok := quotaLowAlert(
			account.ChannelProfileID,
			account.ID,
			health.QuotaRemaining,
		); ok {
			if _, err := fenced.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
				return err
			}
		}
		return fenced.Store.UpdateAccountHealth(ctx, account.ID, health)
	})
}

func (h HandlerService) HandleSendAlert(ctx context.Context, item QueueItemRow) error {
	if err := h.requireExternalPhase(QueueSendAlert); err != nil {
		return err
	}
	now := time.Now().UTC()
	if h.Store != nil && h.Store.Now != nil {
		now = h.Store.Now().UTC()
	}
	alert, err := parseAlertPayload(item.PayloadJSON, now)
	if err != nil {
		return err
	}
	if err := h.withQueueExecutionPhase(ctx, item, func(HandlerService) error {
		return nil
	}); err != nil {
		return err
	}
	sink := h.Alerts
	if sink == nil {
		sink = LogAlertSink{}
	}
	if err := sink.Send(ctx, alert); err != nil {
		return err
	}
	return h.withQueueExecutionPhase(ctx, item, func(HandlerService) error {
		return nil
	})
}

func (h HandlerService) HandleCleanupExpired(ctx context.Context, item QueueItemRow) error {
	cfg := RetentionConfig{
		QueueDays:    positiveAnyInt(item.PayloadJSON["queue_days"], h.Config.RetentionQueueDays),
		AuditDays:    positiveAnyInt(item.PayloadJSON["audit_days"], h.Config.RetentionAuditDays),
		FeedbackDays: positiveAnyInt(item.PayloadJSON["feedback_days"], h.Config.RetentionFeedbackDays),
	}
	_, err := h.Store.CleanupExpired(ctx, h.Store.Now().UTC(), cfg)
	return err
}

func (h HandlerService) HandleLearningRecompute(ctx context.Context, item QueueItemRow) error {
	channelID := firstString(item.PayloadJSON, "channel_id")
	if channelID == "" {
		return errors.New("learning_recompute payload missing channel_id")
	}
	for _, windowDays := range learningRecomputeWindows(item.PayloadJSON["window_days"]) {
		if err := h.Store.RecomputeLearningState(ctx, channelID, windowDays); err != nil {
			return err
		}
	}
	return nil
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
	inputAssetID, ownedInputProfile := ownedInputAssetID(manualSeedConstraints)
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
		if key == "input_asset_id" {
			continue
		}
		constraints[key] = value
	}

	request := map[string]any{
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
	if ownedInputProfile {
		if inputAssetID != "" {
			request["input_asset_id"] = inputAssetID
		}
		request["source_platforms"] = []string{}
		request["source_policy"] = "owned_only"
		request["source_strategy"] = "input_video"
		request["planning_mode"] = "template"
	}
	return request
}

func ownedInputAssetID(constraints map[string]any) (string, bool) {
	rawValue, present := constraints["input_asset_id"]
	if !present {
		return "", false
	}
	value, ok := rawValue.(string)
	if ok && value == "" {
		return "", false
	}
	if !ok {
		return "", true
	}
	parsed, err := uuid.Parse(value)
	if err != nil || parsed.String() != value {
		return "", true
	}
	return value, true
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
