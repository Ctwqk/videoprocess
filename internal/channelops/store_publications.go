package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateOrUpdatePublicationFromTask(ctx context.Context, task ProductionTaskRow, parentQueueItemID string) error {
	videoID := uploadVideoID(task.RationaleJSON)
	if videoID == "" {
		return s.FailTask(ctx, task.ID, "publish_task missing YouTube video id", "publish_task")
	}
	account, err := s.getPublishingAccount(ctx, task.TargetAccountID)
	if err != nil {
		return err
	}
	now := s.Now().UTC()
	desiredPrivacy := desiredPrivacy(task, account)
	currentPrivacy := safePrivacy(firstString(uploadMetadata(task.RationaleJSON), "privacy", "current_privacy"))
	if currentPrivacy == "" {
		currentPrivacy = "private"
	}
	compliance := "known_risk_accepted"
	if task.Source == SourceManualSeed {
		compliance = "assumed_fair_use"
	}
	status := "uploaded"
	if taskUsesExternalAssets(task) && !account.ExternalAutoPublish {
		status = "held"
	}

	var publicationID string
	err = s.Pool.QueryRow(ctx, `
		WITH existing AS (
			SELECT id FROM publication_records WHERE production_task_id = $1::uuid LIMIT 1
		), updated AS (
			UPDATE publication_records p
			SET platform = 'youtube',
			    account_id = $2::uuid,
			    platform_content_id = $3,
			    title = $4,
			    description = $5,
			    desired_privacy = $6,
			    current_privacy = $7,
			    publish_status = CASE WHEN p.publish_status IN ('held', 'uploaded') THEN $8 ELSE p.publish_status END,
			    uploaded_at = COALESCE(p.uploaded_at, $9::timestamptz),
			    compliance_disposition = $10,
			    quota_units_estimated = CASE WHEN p.quota_units_estimated = 0 THEN 1600 ELSE p.quota_units_estimated END,
			    updated_at = $9::timestamp
			FROM existing
			WHERE p.id = existing.id
			RETURNING p.id
		), inserted AS (
			INSERT INTO publication_records (
				id, production_task_id, platform, account_id, platform_content_id, permalink,
				title, description, tags_json, thumbnail_storage_path, desired_privacy,
				current_privacy, publish_status, uploaded_at, scheduled_publish_at, public_at,
				compliance_disposition, quota_units_estimated, last_metrics_polled_at,
				warnings_json, created_at, updated_at
			)
			SELECT gen_random_uuid(), $1::uuid, 'youtube', $2::uuid, $3, NULL,
				       $4, $5, '[]'::json, NULL, $6, $7, $8, $9::timestamptz, NULL, NULL,
				       $10, 1600, NULL, '[]'::json, $9::timestamp, $9::timestamp
			WHERE NOT EXISTS (SELECT 1 FROM existing)
			RETURNING id
		)
		SELECT id FROM updated
		UNION ALL
		SELECT id FROM inserted
		LIMIT 1
	`, task.ID, account.ID, videoID, publicationTitle(task), task.Prompt, desiredPrivacy, currentPrivacy, status, now, compliance).Scan(&publicationID)
	if err != nil {
		return err
	}

	if status == "held" {
		return s.HoldTask(ctx, task.ID, "external_asset_auto_publish_required", "External platform assets require human review before publication", "publish_task")
	}
	if err := s.writeMaterialUsageLedgerFromTask(ctx, task, publicationID, now); err != nil {
		return err
	}
	if err := s.markTaskUploadedPrivate(ctx, task.ID, now); err != nil {
		return err
	}
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return err
	}
	scheduled := now.Add(time.Hour)
	channelProfileID := task.ChannelProfileID
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueuePromotePublication,
		IdempotencyKey:    fmt.Sprintf("promote_publication:%s:%s:%s", publicationID, desiredPrivacy, scheduled.Format(time.RFC3339)),
		Payload:           map[string]any{"publication_id": publicationID, "scheduled_at": scheduled.Format(time.RFC3339), "target_visibility": desiredPrivacy},
		Priority:          70,
		RunAfter:          scheduled,
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) PromotePublication(ctx context.Context, publicationID string, targetVisibility string, scheduledAt time.Time, decision PDSDecision, parentQueueItemID string, metricsDelay time.Duration) error {
	publication, err := s.GetPublication(ctx, publicationID)
	if err != nil {
		return err
	}
	if publication.PublishStatus == "rejected" || publication.PublishStatus == "removed" {
		return nil
	}
	if publication.PublishStatus != "uploaded" && publication.PublishStatus != "held" {
		return fmt.Errorf("publication %s is not ready for promotion", publication.ID)
	}
	visibility := safePromotionVisibility(targetVisibility)
	if visibility == "" {
		visibility = safePrivacy(publication.DesiredPrivacy)
	}
	if visibility == "" {
		visibility = "unlisted"
	}
	now := s.Now().UTC()
	if scheduledAt.IsZero() {
		scheduledAt = now
	}
	if metricsDelay <= 0 {
		metricsDelay = time.Hour
	}
	status := "scheduled"
	var publicAt *time.Time
	if visibility == "public" && !scheduledAt.After(now) {
		status = "public"
		publicAt = &now
	}
	_, err = s.Pool.Exec(ctx, `
		UPDATE publication_records
		SET desired_privacy = $2,
		    current_privacy = CASE WHEN $3 = 'public' THEN $2 ELSE current_privacy END,
		    publish_status = $3,
			    scheduled_publish_at = $4::timestamptz,
			    public_at = COALESCE($5::timestamptz, public_at),
		    warnings_json = (
		        COALESCE(warnings_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object('promotion_decision', $6::jsonb))
		    )::json,
			    updated_at = $7::timestamp
		WHERE id = $1::uuid
	`, publication.ID, visibility, status, scheduledAt.UTC(), publicAt, mustJSON(decision), now)
	if err != nil {
		return err
	}
	if err := s.updateTaskState(ctx, publication.ProductionTaskID, TaskScheduled, "", "", "promote_publication", "", now); err != nil {
		return err
	}
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return err
	}
	channelProfileID, err := s.channelProfileIDForTask(ctx, publication.ProductionTaskID)
	if err != nil {
		return err
	}
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueueCollectMetrics,
		IdempotencyKey:    fmt.Sprintf("collect_metrics:%s:poll:0", publication.ID),
		Payload:           map[string]any{"publication_id": publication.ID, "metrics_poll_count": 0},
		Priority:          90,
		RunAfter:          scheduledAt.UTC().Add(metricsDelay),
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	if err != nil {
		return err
	}
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueueReconcilePublication,
		IdempotencyKey:    fmt.Sprintf("reconcile_publication:%s:%s", publication.ID, scheduledAt.UTC().Format(time.RFC3339)),
		Payload:           map[string]any{"publication_id": publication.ID},
		Priority:          80,
		RunAfter:          scheduledAt.UTC().Add(30 * time.Minute),
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) GetPublication(ctx context.Context, publicationID string) (PublicationRow, error) {
	if err := requireUUID("publication_id", publicationID); err != nil {
		return PublicationRow{}, err
	}
	var row PublicationRow
	var warningsJSON []byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, production_task_id, platform, account_id, platform_content_id, permalink,
		       title, description, desired_privacy, current_privacy, publish_status,
		       uploaded_at, scheduled_publish_at, public_at, compliance_disposition,
		       quota_units_estimated, last_metrics_polled_at, warnings_json, created_at, updated_at
		FROM publication_records
		WHERE id = $1::uuid
	`, publicationID).Scan(
		&row.ID,
		&row.ProductionTaskID,
		&row.Platform,
		&row.AccountID,
		&row.PlatformContentID,
		&row.Permalink,
		&row.Title,
		&row.Description,
		&row.DesiredPrivacy,
		&row.CurrentPrivacy,
		&row.PublishStatus,
		&row.UploadedAt,
		&row.ScheduledPublishAt,
		&row.PublicAt,
		&row.ComplianceDisposition,
		&row.QuotaUnitsEstimated,
		&row.LastMetricsPolledAt,
		&warningsJSON,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	if err != nil {
		return PublicationRow{}, err
	}
	if len(warningsJSON) > 0 {
		if err := json.Unmarshal(warningsJSON, &row.WarningsJSON); err != nil {
			return PublicationRow{}, err
		}
	}
	return row, nil
}

func (s *Store) UpdatePublicationStatus(ctx context.Context, publicationID string, status YouTubePublicationStatus) error {
	if err := requireUUID("publication_id", publicationID); err != nil {
		return err
	}
	now := s.Now().UTC()
	publishStatus := normalizedStatus(status.PublishStatus)
	privacy := observedPrivacy(status.Privacy)
	if privacy == "public" && (publishStatus == "" || publishStatus == "uploaded" || publishStatus == "scheduled") {
		publishStatus = "public"
	}
	_, err := s.Pool.Exec(ctx, `
		UPDATE publication_records
		SET publish_status = COALESCE(NULLIF($2, ''), publish_status),
		    current_privacy = COALESCE(NULLIF($3, ''), current_privacy),
		    permalink = COALESCE(NULLIF($4, ''), permalink),
		    public_at = CASE
			        WHEN COALESCE(NULLIF($2, ''), publish_status) = 'public' THEN COALESCE(public_at, $5::timestamptz)
		        ELSE public_at
		    END,
			    updated_at = $5::timestamp
		WHERE id = $1::uuid
	`, publicationID, publishStatus, privacy, status.Permalink, now)
	return err
}

func (s *Store) MarkPublicationSevereDedup(ctx context.Context, publication PublicationRow, status YouTubePublicationStatus, now time.Time) error {
	eventType := normalizedStatus(status.PublishStatus)
	if eventType == "" {
		eventType = "takedown"
	}
	dayStart := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)
	dedupKey := TakedownDedupKey(publication.ID, eventType, now)
	rawJSON := mustJSON(status.Raw)
	var existingID string
	err := s.Pool.QueryRow(ctx, `
		SELECT id
		FROM takedown_events
		WHERE publication_id = $1::uuid
		  AND event_type = $2
		  AND detected_at >= $3
		  AND detected_at < $4
		ORDER BY detected_at ASC
		LIMIT 1
	`, publication.ID, eventType, dayStart, dayEnd).Scan(&existingID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		_, err = s.Pool.Exec(ctx, `
			INSERT INTO takedown_events (
				id, publication_id, event_type, detected_at, severity, raw_payload_json, auto_actions_taken_json
			)
			VALUES (
				gen_random_uuid(), $1::uuid, $2, $3, 'severe', $4::json,
				jsonb_build_array(jsonb_build_object('action', 'hold_task', 'dedup_key', $5::text, 'at', $6::text))::json
			)
		`, publication.ID, eventType, now.UTC(), rawJSON, dedupKey, now.UTC().Format(time.RFC3339))
	} else {
		_, err = s.Pool.Exec(ctx, `
			UPDATE takedown_events
			SET auto_actions_taken_json = (
				COALESCE(auto_actions_taken_json, '[]'::json)::jsonb ||
				jsonb_build_array(jsonb_build_object('repeat', true, 'dedup_key', $2::text, 'at', $3::text, 'raw', $4::jsonb))
			)::json
			WHERE id = $1::uuid
		`, existingID, dedupKey, now.UTC().Format(time.RFC3339), rawJSON)
	}
	if err != nil {
		return err
	}
	severeStatus := "rejected"
	if eventType == "removed" || eventType == "claim" || eventType == "claimed" || eventType == "takedown" {
		severeStatus = "removed"
	}
	if _, err := s.Pool.Exec(ctx, `
		UPDATE publication_records
		SET publish_status = $2,
		    current_privacy = COALESCE(NULLIF($3, ''), current_privacy),
		    permalink = COALESCE(NULLIF($4, ''), permalink),
		    warnings_json = (
		        COALESCE(warnings_json, '[]'::json)::jsonb ||
		        jsonb_build_array($5::jsonb)
		    )::json,
		    updated_at = $6
		WHERE id = $1::uuid
	`, publication.ID, severeStatus, safePrivacy(status.Privacy), status.Permalink, rawJSON, now.UTC()); err != nil {
		return err
	}
	return s.updateTaskState(ctx, publication.ProductionTaskID, TaskHeld, "platform_rejected", "YouTube reported "+eventType, "reconcile_publication", FailureYouTubeStatus, now.UTC())
}

func (s *Store) RequeueOrHoldMetrics(ctx context.Context, publication PublicationRow, item QueueItemRow, maxPolls int, metricsDelay time.Duration) error {
	if maxPolls <= 0 {
		maxPolls = 24
	}
	if metricsDelay <= 0 {
		metricsDelay = time.Hour
	}
	now := s.Now().UTC()
	pollCount := intOrDefault(item.PayloadJSON["metrics_poll_count"], 0)
	nextCount := pollCount + 1
	if _, err := s.Pool.Exec(ctx, `
		UPDATE publication_records
			SET last_metrics_polled_at = $2::timestamptz, updated_at = $2::timestamp
		WHERE id = $1::uuid
	`, publication.ID, now); err != nil {
		return err
	}
	if nextCount >= maxPolls {
		return s.updateTaskState(ctx, publication.ProductionTaskID, TaskHeld, "metrics_unavailable", "Publication metrics were unavailable after polling", "collect_metrics", FailureMetrics, now)
	}
	parentID, err := optionalUUID("parent_queue_item_id", item.ID)
	if err != nil {
		return err
	}
	channelProfileID, err := s.channelProfileIDForTask(ctx, publication.ProductionTaskID)
	if err != nil {
		return err
	}
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind:              QueueCollectMetrics,
		IdempotencyKey:    fmt.Sprintf("collect_metrics:%s:poll:%d", publication.ID, nextCount),
		Payload:           map[string]any{"publication_id": publication.ID, "metrics_poll_count": nextCount},
		Priority:          90,
		RunAfter:          now.Add(metricsDelay),
		ChannelProfileID:  &channelProfileID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) UpsertFeedbackSnapshot(ctx context.Context, publication PublicationRow, metrics map[string]any, stage string, score float64, fields []string, reward float64, rewardComponents map[string]any) error {
	now := s.Now().UTC()
	stage = SnapshotStageFromPayload(map[string]any{"snapshot_stage": stage})
	rawJSON := mustJSON(metrics)
	fieldsJSON := mustJSON(fields)
	rewardComponentsJSON := mustJSON(rewardComponents)
	retentionJSON := []byte("null")
	if retention := listValue(firstAny(metrics, "retention_curve_json", "retention_curve")); retention != nil {
		retentionJSON = mustJSON(retention)
	}
	values := feedbackValues(metrics)
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO feedback_snapshots (
			id, publication_id, snapshot_stage, collected_at, views, likes, comments, shares,
			avg_view_duration_sec, retention_curve_json, ctr, impressions,
			metrics_completeness_score, available_fields_json, virality_score, raw_json,
			reward_score, reward_components_json
		)
		VALUES (
			gen_random_uuid(), $1::uuid, $2, $3::timestamptz, $4, $5, $6, $7,
			$8, $9::json, $10, $11, $12, $13::json, $14, $15::json,
			$16, $17::json
		)
		ON CONFLICT (publication_id, snapshot_stage) DO UPDATE
		SET collected_at = EXCLUDED.collected_at,
		    views = EXCLUDED.views,
		    likes = EXCLUDED.likes,
		    comments = EXCLUDED.comments,
		    shares = EXCLUDED.shares,
		    avg_view_duration_sec = EXCLUDED.avg_view_duration_sec,
		    retention_curve_json = EXCLUDED.retention_curve_json,
		    ctr = EXCLUDED.ctr,
		    impressions = EXCLUDED.impressions,
		    metrics_completeness_score = EXCLUDED.metrics_completeness_score,
		    available_fields_json = EXCLUDED.available_fields_json,
		    virality_score = EXCLUDED.virality_score,
		    raw_json = EXCLUDED.raw_json,
		    reward_score = EXCLUDED.reward_score,
		    reward_components_json = EXCLUDED.reward_components_json
	`, publication.ID, stage, now, values.Views, values.Likes, values.Comments, values.Shares,
		values.AvgViewDurationSec, retentionJSON, values.CTR, values.Impressions, score, fieldsJSON,
		values.ViralityScore, rawJSON, reward, rewardComponentsJSON)
	if err != nil {
		return err
	}
	if _, err := s.Pool.Exec(ctx, `
		UPDATE publication_records
			SET last_metrics_polled_at = $2::timestamptz, updated_at = $2::timestamp
		WHERE id = $1::uuid
	`, publication.ID, now); err != nil {
		return err
	}
	return s.updateTaskState(ctx, publication.ProductionTaskID, TaskMeasured, "", "", "collect_metrics", "", now)
}

func (s *Store) UpdateAccountHealth(ctx context.Context, accountID string, health YouTubeAccountHealth) error {
	if err := requireUUID("account_id", accountID); err != nil {
		return err
	}
	status := "invalid"
	if health.Authenticated {
		status = "ok"
	}
	now := s.Now().UTC()
	healthJSON := mustJSON(map[string]any{
		"channelops_health": map[string]any{
			"authenticated":   health.Authenticated,
			"quota_remaining": health.QuotaRemaining,
			"checked_at":      now.Format(time.RFC3339),
			"raw":             jsonObject(health.Raw),
		},
	})
	_, err := s.Pool.Exec(ctx, `
		UPDATE publishing_accounts
		SET last_token_check_at = $2,
		    last_token_check_status = $3,
		    enabled = CASE WHEN $4 THEN enabled ELSE false END,
		    platform_specific_config_json = (
		        COALESCE(platform_specific_config_json, '{}'::json)::jsonb || $5::jsonb
		    )::json,
		    updated_at = $2
		WHERE id = $1::uuid
	`, accountID, now, status, health.Authenticated, healthJSON)
	return err
}

func (s *Store) writeMaterialUsageLedgerFromTask(ctx context.Context, task ProductionTaskRow, publicationID string, now time.Time) error {
	if err := requireUUID("publication_id", publicationID); err != nil {
		return err
	}
	observation := mapFromAny(jsonObject(task.RationaleJSON)["autoflow_job_observation"])
	planPayload := mapFromAny(jsonObject(task.RationaleJSON)["autoflow_plan_payload"])
	refs := ExtractMaterialReferences(
		planPayload,
		mapFromAny(observation["run_payload"]),
		mapFromAny(observation["upload_metadata"]),
	)
	for _, ref := range refs {
		metadataJSON := mustJSON(ref.Metadata)
		var assetID *string
		if uuidPattern.MatchString(ref.AssetID) {
			assetID = &ref.AssetID
		}
		_, err := s.Pool.Exec(ctx, `
			INSERT INTO material_usage_ledger (
				id, material_id, asset_id, channel_profile_id, topic_lane_id,
				publishing_account_id, publication_id, used_at, segment_signature,
				metadata_json
			)
			SELECT gen_random_uuid(), $1::text, $2::uuid, $3::uuid, $4::uuid,
			       $5::uuid, $6::uuid, $7::timestamptz, $8::text, $9::json
			WHERE NOT EXISTS (
				SELECT 1
				FROM material_usage_ledger
				WHERE publication_id = $6::uuid
				  AND material_id = $1::text
				  AND segment_signature = $8::text
			)
		`, ref.MaterialID, assetID, task.ChannelProfileID, task.TopicLaneID, task.TargetAccountID,
			publicationID, now.UTC(), ref.SegmentSignature, metadataJSON)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) getPublishingAccount(ctx context.Context, accountID string) (PublishingAccountRow, error) {
	if err := requireUUID("account_id", accountID); err != nil {
		return PublishingAccountRow{}, err
	}
	var row PublishingAccountRow
	err := s.Pool.QueryRow(ctx, `
		SELECT id, channel_profile_id, platform, account_label, platform_account_id,
		       enabled, paused_until, default_privacy, external_asset_auto_publish, created_at
		FROM publishing_accounts
		WHERE id = $1::uuid
	`, accountID).Scan(
		&row.ID,
		&row.ChannelProfileID,
		&row.Platform,
		&row.AccountLabel,
		&row.PlatformAccountID,
		&row.Enabled,
		&row.PausedUntil,
		&row.DefaultPrivacy,
		&row.ExternalAutoPublish,
		&row.CreatedAt,
	)
	return row, err
}

func (s *Store) markTaskUploadedPrivate(ctx context.Context, taskID string, now time.Time) error {
	return s.updateTaskState(ctx, taskID, TaskUploadedPrivate, "", "", "publish_task", "", now)
}

func (s *Store) updateTaskState(ctx context.Context, taskID string, state string, guard string, reason string, transitionReason string, failureCategory string, now time.Time) error {
	if err := requireUUID("production_task_id", taskID); err != nil {
		return err
	}
	var guardValue *string
	if guard != "" {
		guardValue = &guard
	}
	var reasonValue *string
	if reason != "" {
		reasonValue = &reason
	}
	var categoryValue *string
	if failureCategory != "" {
		categoryValue = &failureCategory
	}
	_, err := s.Pool.Exec(ctx, `
		UPDATE production_tasks
		SET state = $2::text,
		    blocked_by_guard = $3::text,
		    failure_reason = $4::text,
		    failure_category = $7::text,
		    state_updated_at = $5::timestamptz,
		    updated_at = $5::timestamp,
		    transition_history_json = (
		        COALESCE(transition_history_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object(
		            'from', state,
		            'to', $2::text,
		            'reason', $6::text,
		            'at', to_char($5::timestamptz AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		        ))
		    )::json
		WHERE id = $1::uuid
	`, taskID, state, guardValue, reasonValue, now.UTC(), transitionReason, categoryValue)
	return err
}

func (s *Store) channelProfileIDForTask(ctx context.Context, taskID string) (string, error) {
	if err := requireUUID("production_task_id", taskID); err != nil {
		return "", err
	}
	var channelProfileID string
	err := s.Pool.QueryRow(ctx, `
		SELECT channel_profile_id FROM production_tasks WHERE id = $1::uuid
	`, taskID).Scan(&channelProfileID)
	return channelProfileID, err
}

type feedbackMetrics struct {
	Views              int
	Likes              int
	Comments           int
	Shares             int
	AvgViewDurationSec float64
	CTR                *float64
	Impressions        *int
	ViralityScore      float64
}

func feedbackValues(metrics map[string]any) feedbackMetrics {
	return feedbackMetrics{
		Views:              nonnegativeInt(metrics["views"], 0),
		Likes:              nonnegativeInt(metrics["likes"], 0),
		Comments:           nonnegativeInt(metrics["comments"], 0),
		Shares:             nonnegativeInt(metrics["shares"], 0),
		AvgViewDurationSec: nonnegativeFloat(metrics["avg_view_duration_sec"], 0),
		CTR:                optionalFloat(metrics["ctr"]),
		Impressions:        optionalInt(metrics["impressions"]),
		ViralityScore:      nonnegativeFloat(metrics["virality_score"], 0),
	}
}

func uploadVideoID(rationale map[string]any) string {
	return firstString(uploadMetadata(rationale), "video_id", "platform_content_id", "youtube_video_id")
}

func uploadMetadata(rationale map[string]any) map[string]any {
	observation := mapFromAny(jsonObject(rationale)["autoflow_job_observation"])
	return mapFromAny(observation["upload_metadata"])
}

func publicationTitle(task ProductionTaskRow) string {
	if strings.TrimSpace(task.TitleSeed) != "" {
		return task.TitleSeed
	}
	prompt := strings.TrimSpace(task.Prompt)
	if len(prompt) <= 80 {
		return prompt
	}
	return prompt[:80]
}

func desiredPrivacy(task ProductionTaskRow, account PublishingAccountRow) string {
	snapshot := jsonObject(task.ChannelConfigSnapshotJSON)
	laneFormat := mapFromAny(snapshot["lane_format"])
	if privacy := safePrivacy(laneFormat["default_publish_visibility"]); privacy != "" {
		return privacy
	}
	if privacy := safePrivacy(account.DefaultPrivacy); privacy != "" {
		return privacy
	}
	accountSnapshot := mapFromAny(snapshot["account"])
	if privacy := safePrivacy(accountSnapshot["default_privacy"]); privacy != "" {
		return privacy
	}
	return "unlisted"
}

func taskUsesExternalAssets(task ProductionTaskRow) bool {
	if task.UsesExternalAssets {
		return true
	}
	if len(task.SourcePlatformsJSON) > 0 {
		return true
	}
	snapshot := jsonObject(task.ChannelConfigSnapshotJSON)
	laneFormat := mapFromAny(snapshot["lane_format"])
	return len(anyStringSlice(laneFormat["source_platforms_json"])) > 0
}

func safePrivacy(value any) string {
	privacy := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	switch privacy {
	case "private", "unlisted":
		return privacy
	default:
		return ""
	}
}

func safePromotionVisibility(value any) string {
	visibility := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	switch visibility {
	case "private", "unlisted":
		return visibility
	default:
		return ""
	}
}

func observedPrivacy(value any) string {
	privacy := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
	switch privacy {
	case "private", "unlisted", "public":
		return privacy
	default:
		return ""
	}
}

func normalizedStatus(value string) string {
	status := strings.ToLower(strings.TrimSpace(value))
	switch status {
	case "processed":
		return "uploaded"
	default:
		return status
	}
}

func mapFromAny(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{}
}

func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(fmt.Sprint(values[key]))
		if value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}

func firstAny(values map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			return value
		}
	}
	return nil
}

func stringOrFallback(value any, fallback string) string {
	text := strings.TrimSpace(fmt.Sprint(value))
	if text == "" || text == "<nil>" {
		return fallback
	}
	return text
}

func boolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return typed == "true" || typed == "1" || typed == "yes"
	default:
		return false
	}
}

func intOrDefault(value any, fallback int) int {
	if parsed, ok := parseMetricFloat(value); ok {
		return int(parsed)
	}
	return fallback
}

func nonnegativeInt(value any, fallback int) int {
	parsed := intOrDefault(value, fallback)
	if parsed < 0 {
		return fallback
	}
	return parsed
}

func optionalInt(value any) *int {
	if parsed, ok := parseMetricFloat(value); ok && parsed >= 0 {
		result := int(parsed)
		return &result
	}
	return nil
}

func nonnegativeFloat(value any, fallback float64) float64 {
	parsed, ok := parseMetricFloat(value)
	if !ok || parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return fallback
	}
	return parsed
}

func optionalFloat(value any) *float64 {
	parsed, ok := parseMetricFloat(value)
	if !ok || parsed < 0 || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return nil
	}
	return &parsed
}

func parseMetricFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func listValue(value any) []any {
	if value == nil {
		return nil
	}
	if typed, ok := value.([]any); ok {
		return typed
	}
	return nil
}

func anyStringSlice(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		result := []string{}
		for _, item := range typed {
			text := strings.TrimSpace(fmt.Sprint(item))
			if text != "" && text != "<nil>" {
				result = append(result, text)
			}
		}
		return result
	default:
		return []string{}
	}
}

func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return raw
}
