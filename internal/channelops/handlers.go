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
	Operation PromotionOperationRow
	Skip      bool
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
	if item.Kind == QueueExecuteTask {
		return h.HandleExecuteTask(ctx, item)
	}
	if item.Kind == QueuePromotePublication {
		return h.HandlePromotePublication(ctx, item)
	}
	if item.Kind == QueueIngestDiscovery {
		return h.HandleIngestDiscovery(ctx, item)
	}
	return h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		fencedHandler := h
		fencedHandler.Store = fencedStore
		return fencedHandler.dispatch(ctx, item)
	})
}

func (h HandlerService) dispatch(ctx context.Context, item QueueItemRow) error {
	switch item.Kind {
	case QueueAgentTick:
		return h.HandleAgentTick(ctx, item)
	case QueuePlanTask:
		return h.HandlePlanTask(ctx, item)
	case QueueExecuteTask:
		return errors.New("execute_task requires split execution fencing")
	case QueueObserveJob:
		return h.HandleObserveJob(ctx, item)
	case QueuePublishTask:
		return h.HandlePublishTask(ctx, item)
	case QueuePromotePublication:
		return errors.New("promote_publication requires split execution fencing")
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
		return errors.New("ingest_discovery requires split execution fencing")
	default:
		return fmt.Errorf("unknown ChannelOps queue kind: %s", item.Kind)
	}
}

func (h HandlerService) HandleIngestDiscovery(ctx context.Context, item QueueItemRow) error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	if h.Store.hasExecutionTransaction() {
		return errors.New("ingest_discovery cannot call discovery API while a database fence is held")
	}
	if h.Discovery == nil {
		return errors.New("discovery client is not configured")
	}
	request, err := discoveryRequestFromQueueItem(item)
	if err != nil {
		return err
	}
	observation, err := h.Discovery.Ingest(ctx, request)
	if err != nil {
		return ErrDiscoveryIngestFailed
	}
	return validateDiscoveryObservation(request, observation)
}

func discoveryRequestFromQueueItem(item QueueItemRow) (DiscoveryIngestRequest, error) {
	if item.Kind != QueueIngestDiscovery {
		return DiscoveryIngestRequest{}, discoveryQueueAuthorityError("kind is invalid")
	}
	if item.Status != QueueStatusRunning || item.LockedBy == nil || strings.TrimSpace(*item.LockedBy) == "" || item.LockedAt == nil {
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
	}, nil
}

func discoveryQueueAuthorityError(message string) error {
	return fmt.Errorf("%w: discovery queue item %s", ErrQueueAuthorityInvalid, message)
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
	planDelay, err := agentTickPlanDelay(item.PayloadJSON)
	if err != nil {
		return err
	}
	return h.Store.RunTickWithPlanDelay(ctx, channelID, bucket, planDelay, h)
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
	if task.State != TaskSelected {
		return nil
	}
	observation, err := h.AutoFlow.PlanTask(ctx, task, AutoFlowRequestForTask(task))
	if err != nil {
		return err
	}
	if observation.UploadNodeCount != 1 {
		return h.Store.HoldTaskWithPlan(ctx, task.ID, observation.PlanID, "missing_youtube_upload_node", "AutoFlow plan must contain exactly one youtube_upload node", "plan_task")
	}
	if task.ApprovalMode == ApprovalHuman || taskUsesExternalAssets(task) {
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
	if alert, ok := maybePDSOutageAlert(decision, task.ChannelProfileID, task.ID, "plan_approval"); ok {
		if _, err := h.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
			return err
		}
	}
	result := PlanDecisionResult(decision)
	if !result.EnqueueExecute {
		return h.Store.HoldTaskWithPlanAndPDS(ctx, task.ID, observation.PlanID, result.BlockedByGuard, decision, "plan_task_pds")
	}
	approval, err := h.AutoFlow.ApprovePlan(ctx, observation.PlanID, map[string]any{"decision_id": decision.DecisionID, "verdict": decision.Verdict})
	if err != nil {
		return err
	}
	return h.Store.MarkTaskPlanningAndEnqueueExecute(
		ctx,
		task.ID,
		observation.PlanID,
		observation.PlanPayload,
		approval,
		item.ID,
	)
}

func (h HandlerService) HandleExecuteTask(ctx context.Context, item QueueItemRow) error {
	if h.AutoFlow == nil {
		return errors.New("autoflow client is not configured")
	}
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	if h.Store.hasExecutionTransaction() {
		return errors.New("execute_task cannot call AutoFlow while a database fence is held")
	}
	queueLockedBy, queueLockedAt, err := runningLease(item)
	if err != nil || strings.TrimSpace(queueLockedBy) == "" || queueLockedAt.IsZero() {
		return fmt.Errorf("%w: execute queue item has no valid running lease", ErrQueueAuthorityInvalid)
	}

	var preparedTask ProductionTaskRow
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
		return nil
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
		return fencedHandler.finalizeExecuteTask(ctx, item, observation)
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
	if task.State != TaskPlanning && task.State != TaskProducing {
		return nil
	}
	if err := validateExecuteTaskAuthority(item, task); err != nil {
		return err
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
	if task.State != TaskProducing {
		return nil
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
	if task.State != TaskScheduled {
		return nil
	}
	if held, err := h.holdInvalidPreUploadReview(ctx, task, "publish_task_human_review"); held {
		return err
	}
	if h.YouTube != nil {
		health, err := h.YouTube.AccountHealth(ctx, task.TargetAccountID)
		if err == nil {
			if alert, ok := quotaLowAlert(task.ChannelProfileID, task.TargetAccountID, health.QuotaRemaining); ok {
				if _, err := h.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
					return err
				}
			}
		}
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
	if alert, ok := maybePDSOutageAlert(decision, task.ChannelProfileID, task.ID, "publish"); ok {
		if _, err := h.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
			return err
		}
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
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	if h.PDS == nil {
		return errors.New("pds client is not configured")
	}
	if h.YouTube == nil {
		return errors.New("youtube client is not configured")
	}
	if h.Store.hasExecutionTransaction() {
		return errors.New("promote_publication cannot hold an execution transaction across submission")
	}

	var preparation promotionPreparation
	if err := h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		fencedHandler := h
		fencedHandler.Store = fencedStore
		prepared, err := fencedHandler.preparePromotion(ctx, item)
		preparation = prepared
		return err
	}); err != nil {
		return err
	}
	if preparation.Skip {
		return nil
	}
	return h.executePromotionOperation(ctx, item, preparation.Operation)
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
	if existingOperation != nil {
		switch existingOperation.Status {
		case PromotionSubmitting, PromotionUncertain, PromotionConfirmed:
			return promotionPreparation{Operation: *existingOperation}, nil
		case PromotionReserved:
		default:
			return promotionPreparation{}, fmt.Errorf(
				"%w: unknown promotion operation state %q",
				ErrPromotionOperationConflict,
				existingOperation.Status,
			)
		}
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
	if task.AutoFlowPlanID != nil {
		valid, err := h.Store.ValidPromotionPlanAuthority(ctx, task)
		if err != nil {
			return promotionPreparation{}, err
		}
		if !valid {
			return promotionPreparation{Skip: true}, h.Store.HoldTask(
				ctx,
				task.ID,
				"autoflow_plan_authority_invalid",
				"Publication promotion plan authority is missing, stale, or revoked",
				"promote_publication_plan_authority",
			)
		}
	}
	if held, err := h.holdInvalidPreUploadReview(ctx, task, "promote_publication_human_review"); held {
		return promotionPreparation{Skip: true}, err
	}
	if taskUsesExternalAssets(task) || boolValue(item.PayloadJSON["manual_review"]) {
		valid, err := h.Store.ValidPromotionHumanReview(ctx, task, publication, targetVisibility)
		if err != nil {
			return promotionPreparation{}, err
		}
		if !valid {
			return promotionPreparation{Skip: true}, h.Store.HoldTask(
				ctx,
				task.ID,
				"human_review_evidence_invalid",
				"Publication promotion human review evidence is missing or stale",
				"promote_publication_human_review",
			)
		}
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
		return promotionPreparation{}, err
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
		return promotionPreparation{Skip: true}, h.Store.HoldTaskWithPDS(
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
		targetVisibility,
		scheduledAt,
		decision,
	)
	if err != nil {
		return promotionPreparation{}, err
	}
	return promotionPreparation{Operation: operation}, nil
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
	operation PromotionOperationRow,
) error {
	for {
		switch operation.Status {
		case PromotionFinalized:
			return nil
		case PromotionConfirmed:
			return h.finalizePromotionOperation(ctx, item, operation.ID)
		case PromotionSubmitting, PromotionUncertain:
			return h.reconcilePromotionOperation(ctx, item, operation, nil)
		case PromotionReserved:
			claimed, shouldSubmit, err := h.Store.BeginPromotionSubmission(ctx, operation.ID)
			if err != nil {
				return err
			}
			operation = claimed
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
				return h.reconcilePromotionOperation(ctx, item, operation, submitErr)
			}
			confirmed, err := h.Store.ConfirmPromotionOperation(
				ctx,
				operation.ID,
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
			operation = confirmed
		default:
			return fmt.Errorf(
				"%w: unknown promotion operation state %q",
				ErrPromotionOperationConflict,
				operation.Status,
			)
		}
	}
}

func (h HandlerService) reconcilePromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	operation PromotionOperationRow,
	submitErr error,
) error {
	status, statusErr := h.YouTube.PublicationStatus(ctx, operation.PlatformVideoID)
	if statusErr == nil &&
		observedPrivacy(status.Privacy) == operation.TargetPrivacy &&
		!isSeverePublicationStatus(status.PublishStatus) {
		confirmed, err := h.Store.ConfirmPromotionOperation(
			ctx,
			operation.ID,
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
		return h.finalizePromotionOperation(ctx, item, confirmed.ID)
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
	uncertain, err := h.Store.MarkPromotionOperationUncertain(ctx, operation.ID, status, reason)
	if err != nil {
		return err
	}
	if uncertain.Status == PromotionConfirmed || uncertain.Status == PromotionFinalized {
		return h.finalizePromotionOperation(ctx, item, uncertain.ID)
	}
	if err := h.holdUncertainPromotionOperation(ctx, item, uncertain, reason); err != nil {
		return errors.Join(fmt.Errorf("%w: %s", ErrPromotionOutcomeUncertain, reason), err)
	}
	return fmt.Errorf("%w: %s", ErrPromotionOutcomeUncertain, reason)
}

func (h HandlerService) finalizePromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	operationID string,
) error {
	return h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		publicationID := firstString(item.PayloadJSON, "publication_id")
		publication, task, err := fencedStore.LockPromotionOperatorScope(ctx, publicationID)
		if err != nil {
			return err
		}
		operation, err := fencedStore.LockPromotionOperation(ctx, operationID)
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
			valid, err := fencedStore.ValidPromotionPlanAuthority(ctx, task)
			if err != nil {
				return err
			}
			if !valid {
				return fencedStore.HoldTask(
					ctx,
					task.ID,
					"autoflow_plan_authority_invalid",
					"Publication promotion plan authority is missing, stale, or revoked",
					"promote_publication_plan_authority",
				)
			}
		}
		if held, err := h.withStore(fencedStore).holdInvalidPreUploadReview(
			ctx,
			task,
			"promote_publication_human_review",
		); held {
			return err
		}
		if taskUsesExternalAssets(task) || boolValue(item.PayloadJSON["manual_review"]) {
			valid, err := fencedStore.ValidPromotionHumanReview(
				ctx,
				task,
				publication,
				operation.TargetPrivacy,
			)
			if err != nil {
				return err
			}
			if !valid {
				return fencedStore.HoldTask(
					ctx,
					task.ID,
					"human_review_evidence_invalid",
					"Publication promotion human review evidence is missing or stale",
					"promote_publication_human_review",
				)
			}
		}
		return fencedStore.FinalizePromotionOperation(
			ctx,
			operation.ID,
			metricsPollDelay(h.Config),
		)
	})
}

func (h HandlerService) holdUncertainPromotionOperation(
	ctx context.Context,
	item QueueItemRow,
	operation PromotionOperationRow,
	reason string,
) error {
	return h.Store.WithQueueExecutionFence(ctx, item, func(fencedStore *Store) error {
		publication, task, err := fencedStore.LockPromotionOperatorScope(
			ctx,
			operation.PublicationID,
		)
		if err != nil {
			return err
		}
		locked, err := fencedStore.LockPromotionOperation(ctx, operation.ID)
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
		return fencedStore.HoldPromotionOperationUncertain(ctx, publication, locked, reason)
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
	task, err := h.Store.GetProductionTask(ctx, publication.ProductionTaskID)
	if err != nil {
		return err
	}
	if task.State == TaskHeld || task.State == TaskFailed || task.State == TaskRejected {
		return nil
	}
	status, err := h.YouTube.PublicationStatus(ctx, publication.PlatformContentID)
	if err != nil {
		return err
	}
	if isSeverePublicationStatus(status.PublishStatus) {
		if err := h.Store.MarkPublicationSevereDedup(ctx, publication, status, h.Store.Now()); err != nil {
			return err
		}
		channelID := task.ChannelProfileID
		_, err := h.Store.EnqueueAlert(ctx, platformRejectedAlert(publication, channelID, status), 5, item.ID)
		return err
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
	task, err := h.Store.GetProductionTask(ctx, publication.ProductionTaskID)
	if err != nil {
		return err
	}
	if task.State == TaskHeld || task.State == TaskFailed || task.State == TaskRejected {
		return nil
	}
	var schedule *MetricScheduleRow
	if firstString(item.PayloadJSON, "metric_schedule_id") != "" {
		lockedSchedule, err := h.Store.LockMetricScheduleForQueue(ctx, item)
		if err != nil {
			return err
		}
		if lockedSchedule.Status != MetricSchedulePending {
			return nil
		}
		schedule = &lockedSchedule
	}
	metrics := mapFromAny(item.PayloadJSON["metrics"])
	if !HasRecognizedMetrics(metrics) && publication.PlatformContentID != "" && h.YouTube != nil {
		fetched, err := h.YouTube.FetchMetrics(ctx, publication.PlatformContentID)
		if err == nil && HasRecognizedMetrics(fetched) {
			metrics = fetched
		}
	}
	if !HasRecognizedMetrics(metrics) {
		if schedule != nil {
			return h.Store.RequeueOrExpireMetricSchedule(
				ctx,
				publication,
				*schedule,
				item,
				h.Config.MetricsPollMaxAttempts,
				metricsPollDelay(h.Config),
			)
		}
		return h.Store.RequeueOrHoldMetrics(ctx, publication, item, h.Config.MetricsPollMaxAttempts, metricsPollDelay(h.Config))
	}
	score, fields := MetricsCompleteness(metrics)
	reward, components := RewardScore(metrics, PublicationRewardContext{StablePublication: true})
	if schedule != nil {
		return h.Store.CompleteMetricSchedule(
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
	channelID := ""
	if account, err := h.Store.getPublishingAccount(ctx, accountID); err == nil {
		channelID = account.ChannelProfileID
	}
	if alert, ok := quotaLowAlert(channelID, accountID, health.QuotaRemaining); ok {
		if _, err := h.Store.EnqueueAlert(ctx, alert, 5, item.ID); err != nil {
			return err
		}
	}
	return h.Store.UpdateAccountHealth(ctx, accountID, health)
}

func (h HandlerService) HandleSendAlert(ctx context.Context, item QueueItemRow) error {
	now := time.Now().UTC()
	if h.Store != nil && h.Store.Now != nil {
		now = h.Store.Now().UTC()
	}
	alert, err := parseAlertPayload(item.PayloadJSON, now)
	if err != nil {
		return err
	}
	sink := h.Alerts
	if sink == nil {
		sink = LogAlertSink{}
	}
	return sink.Send(ctx, alert)
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
