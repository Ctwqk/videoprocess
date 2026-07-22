package channelops

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type MetricStageSpec struct {
	Stage      string
	DueAfter   time.Duration
	GraceAfter time.Duration
}

type MetricSchedulePlan struct {
	Stage          string
	EffectiveStart time.Time
	DueAt          time.Time
	GraceUntil     time.Time
	IdempotencyKey string
}

var metricStageSpecs = [...]MetricStageSpec{
	{Stage: "1h", DueAfter: time.Hour, GraceAfter: 3 * time.Hour},
	{Stage: "6h", DueAfter: 6 * time.Hour, GraceAfter: 12 * time.Hour},
	{Stage: "24h", DueAfter: 24 * time.Hour, GraceAfter: 30 * time.Hour},
	{Stage: "72h", DueAfter: 72 * time.Hour, GraceAfter: 84 * time.Hour},
	{Stage: "7d", DueAfter: 168 * time.Hour, GraceAfter: 192 * time.Hour},
}

func MetricStageSpecs() []MetricStageSpec {
	return append([]MetricStageSpec(nil), metricStageSpecs[:]...)
}

func BuildMetricSchedulePlans(publicationID string, effectiveStart time.Time) []MetricSchedulePlan {
	start := effectiveStart.UTC()
	plans := make([]MetricSchedulePlan, 0, len(metricStageSpecs))
	for _, spec := range metricStageSpecs {
		plans = append(plans, MetricSchedulePlan{
			Stage:          spec.Stage,
			EffectiveStart: start,
			DueAt:          start.Add(spec.DueAfter),
			GraceUntil:     start.Add(spec.GraceAfter),
			IdempotencyKey: fmt.Sprintf(
				"collect_metrics:%s:stage:%s:attempt:0",
				publicationID,
				spec.Stage,
			),
		})
	}
	return plans
}

func (s *Store) EnsurePublicationMetricSchedules(
	ctx context.Context,
	publicationID string,
	channelID string,
	parentQueueItemID string,
	effectiveStart time.Time,
) error {
	if err := requireUUID("publication_id", publicationID); err != nil {
		return err
	}
	if err := requireUUID("channel_profile_id", channelID); err != nil {
		return err
	}
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return err
	}
	for _, plan := range BuildMetricSchedulePlans(publicationID, effectiveStart) {
		schedule, err := s.insertOrGetMetricSchedule(ctx, publicationID, plan)
		if err != nil {
			return err
		}
		if schedule.Status != "pending" {
			continue
		}
		resolvedChannelID := channelID
		if _, err := s.Enqueue(ctx, EnqueueOptions{
			Kind:           QueueCollectMetrics,
			IdempotencyKey: plan.IdempotencyKey,
			Payload: map[string]any{
				"publication_id":     publicationID,
				"metric_schedule_id": schedule.ID,
				"snapshot_stage":     plan.Stage,
				"metrics_poll_count": 0,
			},
			Priority:          90,
			RunAfter:          plan.DueAt,
			ChannelProfileID:  &resolvedChannelID,
			ParentQueueItemID: parentID,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) insertOrGetMetricSchedule(
	ctx context.Context,
	publicationID string,
	plan MetricSchedulePlan,
) (MetricScheduleRow, error) {
	var row MetricScheduleRow
	var availableFieldsJSON []byte
	err := s.db().QueryRow(ctx, `
		WITH inserted AS (
			INSERT INTO publication_metric_schedules (
				id, publication_id, snapshot_stage, effective_start_at, due_at,
				grace_until, status, attempt_count, available_fields_json,
				created_at, updated_at
			)
			VALUES (
				gen_random_uuid(), $1::uuid, $2, $3::timestamptz, $4::timestamptz,
				$5::timestamptz, 'pending', 0, '[]'::json, NOW(), NOW()
			)
			ON CONFLICT (publication_id, snapshot_stage) DO NOTHING
			RETURNING id, publication_id, snapshot_stage, effective_start_at, due_at,
			          grace_until, status, attempt_count, last_attempt_at, completed_at,
			          available_fields_json, last_error_code, created_at, updated_at
		)
		SELECT id, publication_id, snapshot_stage, effective_start_at, due_at,
		       grace_until, status, attempt_count, last_attempt_at, completed_at,
		       available_fields_json, last_error_code, created_at, updated_at
		FROM inserted
		UNION ALL
		SELECT id, publication_id, snapshot_stage, effective_start_at, due_at,
		       grace_until, status, attempt_count, last_attempt_at, completed_at,
		       available_fields_json, last_error_code, created_at, updated_at
		FROM publication_metric_schedules
		WHERE publication_id = $1::uuid AND snapshot_stage = $2
		LIMIT 1
	`, publicationID, plan.Stage, plan.EffectiveStart, plan.DueAt, plan.GraceUntil).Scan(
		&row.ID,
		&row.PublicationID,
		&row.SnapshotStage,
		&row.EffectiveStartAt,
		&row.DueAt,
		&row.GraceUntil,
		&row.Status,
		&row.AttemptCount,
		&row.LastAttemptAt,
		&row.CompletedAt,
		&availableFieldsJSON,
		&row.LastErrorCode,
		&row.CreatedAt,
		&row.UpdatedAt,
	)
	if err != nil {
		return MetricScheduleRow{}, err
	}
	if err := json.Unmarshal(availableFieldsJSON, &row.AvailableFieldsJSON); err != nil {
		return MetricScheduleRow{}, err
	}
	return row, nil
}
