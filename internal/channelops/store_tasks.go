package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var uuidPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (s *Store) RunTick(ctx context.Context, channelID string, bucket string, h HandlerService) error {
	return s.RunTickWithPlanDelay(ctx, channelID, bucket, 0, h)
}

func (s *Store) RunTickWithPlanDelay(
	ctx context.Context,
	channelID string,
	bucket string,
	planDelay time.Duration,
	h HandlerService,
) error {
	if planDelay < 0 || planDelay > time.Hour {
		return errors.New("plan delay must be from 0 through 1 hour")
	}
	now := s.Now().UTC()
	channel, lanes, accounts, seeds, signals, laneFormats, err := s.LoadTickInputs(ctx, channelID, now)
	if err != nil {
		return err
	}
	candidates := BuildTickCandidates(channel, lanes, accounts, seeds, signals, laneFormats, bucket)
	alerts := []AlertPayload{}
	evaluatedCandidates, pdsAlerts, err := evaluateTickCandidatePolicy(ctx, channel, candidates, h)
	if err != nil {
		return err
	}
	candidates = evaluatedCandidates
	alerts = append(alerts, pdsAlerts...)
	accepted, rejected := acceptedRejected(candidates)
	if len(accepted) == 0 {
		alerts = append(alerts, materialLowSupplyAlert(channelID, bucket, len(accepted), len(rejected)))
	}
	result := TickResult{DryRun: channel.DryRun, Accepted: accepted, Rejected: rejected}
	tx, ownsTransaction, err := s.beginOrReuse(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	tickAuditID, err := s.insertTickAudit(ctx, tx, channelID, bucket, result, map[string]any{
		"bucket":          bucket,
		"config_version":  channel.ConfigVersion,
		"accepted_count":  len(accepted),
		"rejected_count":  len(rejected),
		"handler_version": "go",
	})
	if err != nil {
		return err
	}
	decisionAuditIDs, err := s.insertDecisionAuditEntries(ctx, tx, tickAuditID, channelID, result)
	if err != nil {
		return err
	}
	for _, alert := range alerts {
		if _, err := s.enqueueAlert(ctx, tx, alert, 5, ""); err != nil {
			return err
		}
	}
	if channel.DryRun {
		if ownsTransaction {
			if err := tx.Commit(ctx); err != nil {
				return err
			}
		}
		committed = true
		return nil
	}

	for _, candidate := range accepted {
		taskID, err := s.insertProductionTask(ctx, tx, channel, candidate, now)
		if err != nil {
			return err
		}
		if candidate.DiscoverySignal != nil {
			if err := s.markDiscoverySignalConverted(ctx, tx, candidate.DiscoverySignal.ID, taskID); err != nil {
				return err
			}
		}
		if auditID := decisionAuditIDs[candidate.CandidateID]; auditID != "" {
			if err := s.attachDecisionAuditTask(ctx, tx, auditID, taskID); err != nil {
				return err
			}
		}
		channelProfileID := channel.ID
		if _, err := s.enqueue(ctx, tx, EnqueueOptions{
			Kind:             QueuePlanTask,
			IdempotencyKey:   "plan_task:" + taskID,
			Payload:          map[string]any{"production_task_id": taskID, "channel_id": channel.ID},
			Priority:         100,
			RunAfter:         now.Add(planDelay),
			ChannelProfileID: &channelProfileID,
		}); err != nil {
			return err
		}
	}
	if ownsTransaction {
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	committed = true
	return nil
}

func evaluateTickCandidatePolicy(ctx context.Context, channel ChannelProfileRow, candidates []TickCandidate, h HandlerService) ([]TickCandidate, []AlertPayload, error) {
	if h.PDS == nil {
		return candidates, nil, nil
	}
	alerts := []AlertPayload{}
	for i := range candidates {
		candidate := &candidates[i]
		if candidate.Rejected || candidate.Account == nil {
			continue
		}
		decision, err := h.PDS.Decide(ctx, PDSDecisionRequest{
			ActorID:    candidate.Account.ID,
			ActionType: "candidate_accept",
			Platform:   "youtube",
			Content: map[string]any{
				"title":       candidate.TitleSeed,
				"description": candidate.Prompt,
			},
			Context: map[string]any{
				"channel_profile_id": channel.ID,
				"candidate_id":       candidate.CandidateID,
				"source_kind":        candidate.SourceKind,
				"topic_lane_id":      candidateLaneID(*candidate),
				"lane_format_id":     candidateFormatID(*candidate),
			},
		})
		if err != nil {
			return candidates, nil, err
		}
		candidate.PDSDecisionJSON = pdsDecisionAuditJSON(decision)
		if alert, ok := maybePDSOutageAlert(decision, channel.ID, candidate.CandidateID, "candidate_accept"); ok {
			alerts = append(alerts, alert)
		}
		switch decision.Verdict {
		case "", "allow":
		case "block":
			rejectCandidate(candidate, "pds_blocked", "PDS blocked candidate acceptance.")
		default:
			rejectCandidate(candidate, "pds_flagged_for_review", "PDS flagged candidate acceptance for review.")
		}
	}
	return candidates, alerts, nil
}

func (s *Store) GetProductionTask(ctx context.Context, taskID string) (ProductionTaskRow, error) {
	if err := requireUUID("production_task_id", taskID); err != nil {
		return ProductionTaskRow{}, err
	}
	var row ProductionTaskRow
	var rationaleJSON, scoreJSON, sourcePlatformsJSON, materialIDsJSON, transitionJSON, snapshotJSON []byte
	err := s.db().QueryRow(ctx, `
		SELECT id, channel_profile_id, topic_lane_id, lane_format_id, target_account_id,
		       manual_seed_id, discovery_signal_id, source, title_seed, prompt, rationale_json,
		       score_breakdown_json, source_platforms_json, material_library_ids_json,
		       uses_external_assets, approval_mode, autoflow_plan_id, autoflow_run_id, job_id, state,
		       blocked_by_guard, failure_reason, failure_category, transition_history_json,
		       channel_config_version_snapshot, channel_config_snapshot_json,
		       state_updated_at
		FROM production_tasks
		WHERE id = $1::uuid
	`, taskID).Scan(
		&row.ID,
		&row.ChannelProfileID,
		&row.TopicLaneID,
		&row.LaneFormatID,
		&row.TargetAccountID,
		&row.ManualSeedID,
		&row.DiscoverySignalID,
		&row.Source,
		&row.TitleSeed,
		&row.Prompt,
		&rationaleJSON,
		&scoreJSON,
		&sourcePlatformsJSON,
		&materialIDsJSON,
		&row.UsesExternalAssets,
		&row.ApprovalMode,
		&row.AutoFlowPlanID,
		&row.AutoFlowRunID,
		&row.JobID,
		&row.State,
		&row.BlockedByGuard,
		&row.FailureReason,
		&row.FailureCategory,
		&transitionJSON,
		&row.ChannelConfigVersionSnapshot,
		&snapshotJSON,
		&row.StateUpdatedAt,
	)
	if err != nil {
		return ProductionTaskRow{}, err
	}
	if err := unmarshalJSONObject(rationaleJSON, &row.RationaleJSON); err != nil {
		return ProductionTaskRow{}, fmt.Errorf("scan production_tasks.rationale_json: %w", err)
	}
	if err := unmarshalJSONObject(scoreJSON, &row.ScoreBreakdownJSON); err != nil {
		return ProductionTaskRow{}, fmt.Errorf("scan production_tasks.score_breakdown_json: %w", err)
	}
	if err := unmarshalJSONStringSlice(sourcePlatformsJSON, &row.SourcePlatformsJSON); err != nil {
		return ProductionTaskRow{}, fmt.Errorf("scan production_tasks.source_platforms_json: %w", err)
	}
	if err := unmarshalJSONStringSlice(materialIDsJSON, &row.MaterialLibraryIDsJSON); err != nil {
		return ProductionTaskRow{}, fmt.Errorf("scan production_tasks.material_library_ids_json: %w", err)
	}
	if err := unmarshalJSONMapSlice(transitionJSON, &row.TransitionHistoryJSON); err != nil {
		return ProductionTaskRow{}, fmt.Errorf("scan production_tasks.transition_history_json: %w", err)
	}
	if err := unmarshalJSONObject(snapshotJSON, &row.ChannelConfigSnapshotJSON); err != nil {
		return ProductionTaskRow{}, fmt.Errorf("scan production_tasks.channel_config_snapshot_json: %w", err)
	}
	return row, nil
}

func (s *Store) HoldTask(ctx context.Context, taskID string, guard string, reason string, transitionReason string) error {
	return s.holdTask(ctx, taskID, "", guard, reason, nil, transitionReason)
}

func (s *Store) HoldTaskWithPlan(ctx context.Context, taskID string, planID string, guard string, reason string, transitionReason string) error {
	return s.holdTask(ctx, taskID, planID, guard, reason, nil, transitionReason)
}

func (s *Store) HoldTaskWithPDS(ctx context.Context, taskID string, guard string, decision PDSDecision, transitionReason string) error {
	return s.HoldTaskWithPlanAndPDS(ctx, taskID, "", guard, decision, transitionReason)
}

func (s *Store) HoldTaskWithPlanAndPDS(ctx context.Context, taskID string, planID string, guard string, decision PDSDecision, transitionReason string) error {
	reason := fmt.Sprintf("PDS verdict: %s", decision.Verdict)
	return s.holdTask(ctx, taskID, planID, guard, reason, decision, transitionReason)
}

func (s *Store) MarkTaskPlanningAndEnqueueExecute(ctx context.Context, taskID string, planID string, planPayload map[string]any, parentQueueItemID string) error {
	if err := requireUUID("production_task_id", taskID); err != nil {
		return err
	}
	if err := requireUUID("autoflow_plan_id", planID); err != nil {
		return err
	}
	task, err := s.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	rationalePatch, err := json.Marshal(map[string]any{"autoflow_plan_payload": jsonObject(planPayload)})
	if err != nil {
		return err
	}
	now := s.Now().UTC()
	if _, err := s.db().Exec(ctx, `
		UPDATE production_tasks
		SET autoflow_plan_id = $2::uuid,
		    state = $3::text,
		    blocked_by_guard = NULL,
		    failure_reason = NULL,
		    failure_category = NULL,
		    rationale_json = (COALESCE(rationale_json, '{}'::json)::jsonb || $6::jsonb)::json,
		    state_updated_at = $4::timestamptz,
		    updated_at = $4::timestamp,
		    transition_history_json = (
		        COALESCE(transition_history_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object(
		            'from', state,
		            'to', $3::text,
		            'reason', $5::text,
		            'at', to_char($4::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		        ))
		    )::json
		WHERE id = $1::uuid
	`, taskID, planID, TaskPlanning, now, "plan_task", rationalePatch); err != nil {
		return err
	}
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return err
	}
	channelProfileID := task.ChannelProfileID
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueueExecuteTask,
		IdempotencyKey:    "execute_task:" + taskID,
		Payload:           map[string]any{"production_task_id": taskID, "autoflow_plan_id": planID},
		Priority:          100,
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) MarkTaskProducingAndEnqueueObserve(ctx context.Context, taskID string, runID string, jobID string, parentQueueItemID string) error {
	if err := requireUUID("production_task_id", taskID); err != nil {
		return err
	}
	if err := requireUUID("autoflow_run_id", runID); err != nil {
		return err
	}
	if err := requireUUID("job_id", jobID); err != nil {
		return err
	}
	task, err := s.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	now := s.Now().UTC()
	if _, err := s.db().Exec(ctx, `
		UPDATE production_tasks
		SET autoflow_run_id = $2::uuid,
		    job_id = $3::uuid,
		    state = $4::text,
		    blocked_by_guard = NULL,
		    failure_reason = NULL,
		    failure_category = NULL,
		    state_updated_at = $5::timestamptz,
		    updated_at = $5::timestamp,
		    transition_history_json = (
		        COALESCE(transition_history_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object(
		            'from', state,
		            'to', $4::text,
		            'reason', $6::text,
		            'at', to_char($5::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		        ))
		    )::json
		WHERE id = $1::uuid
	`, taskID, runID, jobID, TaskProducing, now, "execute_task"); err != nil {
		return err
	}
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return err
	}
	channelProfileID := task.ChannelProfileID
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueueObserveJob,
		IdempotencyKey:    "observe_job:" + taskID + ":" + runID + ":" + jobID + ":0",
		Payload:           map[string]any{"production_task_id": taskID, "run_id": runID, "job_id": jobID},
		Priority:          100,
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) ReenqueueObserve(ctx context.Context, taskID string, parentQueueItemID string, delay time.Duration) error {
	task, err := s.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task.JobID == nil || *task.JobID == "" {
		return fmt.Errorf("task %s has no AutoFlow job id", task.ID)
	}
	if task.AutoFlowRunID == nil || *task.AutoFlowRunID == "" {
		return fmt.Errorf("task %s has no AutoFlow run id", task.ID)
	}
	if delay <= 0 {
		delay = time.Minute
	}
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return err
	}
	channelProfileID := task.ChannelProfileID
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueueObserveJob,
		IdempotencyKey:    "observe_job:" + taskID + ":" + *task.AutoFlowRunID + ":" + *task.JobID + ":" + parentQueueItemID,
		Payload:           map[string]any{"production_task_id": taskID, "run_id": *task.AutoFlowRunID, "job_id": *task.JobID},
		Priority:          100,
		RunAfter:          s.Now().UTC().Add(delay),
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) MarkTaskReadyToPublish(ctx context.Context, task ProductionTaskRow, observation AutoFlowJobObservation, parentQueueItemID string) error {
	if err := requireUUID("production_task_id", task.ID); err != nil {
		return err
	}
	now := s.Now().UTC()
	rationalePatch, err := json.Marshal(map[string]any{
		"autoflow_job_observation": map[string]any{
			"status":          observation.Status,
			"run_payload":     jsonObject(observation.RunPayload),
			"upload_metadata": jsonObject(observation.UploadMetadata),
		},
	})
	if err != nil {
		return err
	}
	if _, err := s.db().Exec(ctx, `
		UPDATE production_tasks
		SET state = $2::text,
		    blocked_by_guard = NULL,
		    failure_reason = NULL,
		    failure_category = NULL,
		    rationale_json = (COALESCE(rationale_json, '{}'::json)::jsonb || $3::jsonb)::json,
		    state_updated_at = $4::timestamptz,
		    updated_at = $4::timestamp,
		    transition_history_json = (
		        COALESCE(transition_history_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object(
		            'from', state,
		            'to', $2::text,
		            'reason', $5::text,
		            'at', to_char($4::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		        ))
		    )::json
		WHERE id = $1::uuid
	`, task.ID, TaskScheduled, rationalePatch, now, "observe_job"); err != nil {
		return err
	}
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return err
	}
	channelProfileID := task.ChannelProfileID
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueuePublishTask,
		IdempotencyKey:    "publish_task:" + task.ID,
		Payload:           map[string]any{"production_task_id": task.ID},
		Priority:          100,
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) FailTask(ctx context.Context, taskID string, reason string, transitionReason string) error {
	if err := requireUUID("production_task_id", taskID); err != nil {
		return err
	}
	if reason == "" {
		reason = "AutoFlow job failed"
	}
	category := FailureCategoryFor(transitionReason, reason)
	now := s.Now().UTC()
	_, err := s.db().Exec(ctx, `
		UPDATE production_tasks
		SET state = $2::text,
		    failure_reason = $3::text,
		    failure_category = $6::text,
		    state_updated_at = $4::timestamptz,
		    updated_at = $4::timestamp,
		    transition_history_json = (
		        COALESCE(transition_history_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object(
		            'from', state,
		            'to', $2::text,
		            'reason', $5::text,
		            'at', to_char($4::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		        ))
		    )::json
		WHERE id = $1::uuid
	`, taskID, TaskFailed, reason, now, transitionReason, category)
	return err
}

func (s *Store) holdTask(ctx context.Context, taskID string, planID string, guard string, reason string, decision any, transitionReason string) error {
	if err := requireUUID("production_task_id", taskID); err != nil {
		return err
	}
	planIDValue, err := optionalUUID("autoflow_plan_id", planID)
	if err != nil {
		return err
	}
	evidenceJSON := []byte("{}")
	hasEvidence := decision != nil
	if decision != nil {
		evidenceJSON, err = json.Marshal(decision)
		if err != nil {
			return err
		}
	}
	category := holdFailureCategoryFor(guard, reason, transitionReason, decision)
	now := s.Now().UTC()
	_, err = s.db().Exec(ctx, `
		UPDATE production_tasks
		SET autoflow_plan_id = COALESCE($2::uuid, autoflow_plan_id),
		    state = $3::text,
		    blocked_by_guard = $4::text,
		    failure_reason = $5::text,
		    failure_category = $10::text,
		    agent_approval_evidence_json = CASE WHEN $6 THEN $7::json ELSE agent_approval_evidence_json END,
		    state_updated_at = $8::timestamptz,
		    updated_at = $8::timestamp,
		    transition_history_json = (
		        COALESCE(transition_history_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object(
		            'from', state,
		            'to', $3::text,
		            'reason', $9::text,
		            'at', to_char($8::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		        ))
		    )::json
		WHERE id = $1::uuid
	`, taskID, planIDValue, TaskHeld, guard, reason, hasEvidence, evidenceJSON, now, transitionReason, category)
	return err
}

func unmarshalJSONMapSlice(raw []byte, dest *[]map[string]any) error {
	if len(raw) == 0 || string(raw) == "null" {
		*dest = []map[string]any{}
		return nil
	}
	if err := json.Unmarshal(raw, dest); err != nil {
		return err
	}
	if *dest == nil {
		*dest = []map[string]any{}
	}
	return nil
}

func requireUUID(field string, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", field)
	}
	if !uuidPattern.MatchString(value) {
		return fmt.Errorf("%s must be a UUID, got %q", field, value)
	}
	return nil
}

func optionalUUID(field string, value string) (*string, error) {
	if value == "" {
		return nil, nil
	}
	if !uuidPattern.MatchString(value) {
		return nil, fmt.Errorf("%s must be a UUID, got %q", field, value)
	}
	return &value, nil
}
