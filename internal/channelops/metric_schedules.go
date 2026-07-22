package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
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

type MetricRetryDecision struct {
	AttemptCount int
	Expire       bool
	RunAfter     time.Time
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

func MetricScheduleRetryDecision(
	schedule MetricScheduleRow,
	now time.Time,
	maxAttempts int,
	retryDelay time.Duration,
) MetricRetryDecision {
	if maxAttempts <= 0 {
		maxAttempts = 24
	}
	if retryDelay <= 0 {
		retryDelay = time.Hour
	}
	attemptCount := schedule.AttemptCount + 1
	now = now.UTC()
	if attemptCount >= maxAttempts || !now.Before(schedule.GraceUntil.UTC()) {
		return MetricRetryDecision{AttemptCount: attemptCount, Expire: true}
	}
	runAfter := now.Add(retryDelay)
	if runAfter.After(schedule.GraceUntil.UTC()) {
		runAfter = schedule.GraceUntil.UTC()
	}
	return MetricRetryDecision{
		AttemptCount: attemptCount,
		RunAfter:     runAfter,
	}
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

func (s *Store) LockMetricScheduleForQueue(
	ctx context.Context,
	item QueueItemRow,
) (MetricScheduleRow, error) {
	scheduleID := firstString(item.PayloadJSON, "metric_schedule_id")
	publicationID := firstString(item.PayloadJSON, "publication_id")
	stage := firstString(item.PayloadJSON, "snapshot_stage")
	if err := requireUUID("metric_schedule_id", scheduleID); err != nil {
		return MetricScheduleRow{}, fmt.Errorf("%w: %v", ErrQueueAuthorityInvalid, err)
	}
	if err := requireUUID("publication_id", publicationID); err != nil {
		return MetricScheduleRow{}, fmt.Errorf("%w: %v", ErrQueueAuthorityInvalid, err)
	}
	if !isMetricScheduleStage(stage) {
		return MetricScheduleRow{}, fmt.Errorf(
			"%w: metric schedule stage is invalid",
			ErrQueueAuthorityInvalid,
		)
	}

	row, err := s.getMetricSchedule(ctx, scheduleID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return MetricScheduleRow{}, fmt.Errorf(
			"%w: metric schedule does not exist",
			ErrQueueAuthorityInvalid,
		)
	}
	if err != nil {
		return MetricScheduleRow{}, err
	}
	if row.PublicationID != publicationID || row.SnapshotStage != stage {
		return MetricScheduleRow{}, fmt.Errorf(
			"%w: metric schedule payload does not match persisted authority",
			ErrQueueAuthorityInvalid,
		)
	}
	return row, nil
}

func (s *Store) getMetricSchedule(
	ctx context.Context,
	scheduleID string,
	lock bool,
) (MetricScheduleRow, error) {
	query := `
		SELECT id, publication_id, snapshot_stage, effective_start_at, due_at,
		       grace_until, status, attempt_count, last_attempt_at, completed_at,
		       available_fields_json, last_error_code, created_at, updated_at
		FROM publication_metric_schedules
		WHERE id = $1::uuid
	`
	if lock {
		query += " FOR UPDATE"
	}
	return scanMetricSchedule(s.db().QueryRow(ctx, query, scheduleID))
}

func (s *Store) RequeueOrExpireMetricSchedule(
	ctx context.Context,
	publication PublicationRow,
	schedule MetricScheduleRow,
	item QueueItemRow,
	maxAttempts int,
	retryDelay time.Duration,
) error {
	if schedule.Status != MetricSchedulePending {
		return nil
	}
	now := s.Now().UTC()
	decision := MetricScheduleRetryDecision(schedule, now, maxAttempts, retryDelay)
	if _, err := s.db().Exec(ctx, `
		UPDATE publication_records
		SET last_metrics_polled_at = $2::timestamptz, updated_at = $2::timestamp
		WHERE id = $1::uuid
	`, publication.ID, now); err != nil {
		return err
	}

	if decision.Expire {
		tag, err := s.db().Exec(ctx, `
			UPDATE publication_metric_schedules
			SET status = $2,
			    attempt_count = $3,
			    last_attempt_at = $4::timestamptz,
			    completed_at = $4::timestamptz,
			    last_error_code = $5,
			    updated_at = $4::timestamp
			WHERE id = $1::uuid AND status = $6
		`, schedule.ID, MetricScheduleExpired, decision.AttemptCount, now,
			MetricErrorUnavailable, MetricSchedulePending)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: metric schedule expiration lost authority", ErrQueueAuthorityInvalid)
		}
		if schedule.SnapshotStage == "24h" {
			return s.updateTaskState(
				ctx,
				publication.ProductionTaskID,
				TaskHeld,
				MetricErrorUnavailable,
				"Publication 24h metrics were unavailable after the durable collection window",
				QueueCollectMetrics,
				FailureMetrics,
				now,
			)
		}
		return nil
	}

	tag, err := s.db().Exec(ctx, `
		UPDATE publication_metric_schedules
		SET attempt_count = $2,
		    last_attempt_at = $3::timestamptz,
		    last_error_code = $4,
		    updated_at = $3::timestamp
		WHERE id = $1::uuid AND status = $5
	`, schedule.ID, decision.AttemptCount, now, MetricErrorUnavailable, MetricSchedulePending)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: metric schedule retry lost authority", ErrQueueAuthorityInvalid)
	}
	parentID, err := optionalUUID("parent_queue_item_id", item.ID)
	if err != nil {
		return err
	}
	channelID, err := s.channelProfileIDForTask(ctx, publication.ProductionTaskID)
	if err != nil {
		return err
	}
	_, err = s.Enqueue(ctx, EnqueueOptions{
		Kind: QueueCollectMetrics,
		IdempotencyKey: fmt.Sprintf(
			"collect_metrics:%s:stage:%s:attempt:%d",
			publication.ID,
			schedule.SnapshotStage,
			decision.AttemptCount,
		),
		Payload: map[string]any{
			"publication_id":     publication.ID,
			"metric_schedule_id": schedule.ID,
			"snapshot_stage":     schedule.SnapshotStage,
			"metrics_poll_count": decision.AttemptCount,
		},
		Priority:          90,
		RunAfter:          decision.RunAfter,
		ChannelProfileID:  &channelID,
		ParentQueueItemID: parentID,
	})
	return err
}

func (s *Store) CompleteMetricSchedule(
	ctx context.Context,
	publication PublicationRow,
	schedule MetricScheduleRow,
	metrics map[string]any,
	score float64,
	fields []string,
	reward float64,
	rewardComponents map[string]any,
) error {
	if schedule.Status != MetricSchedulePending {
		return nil
	}
	if err := s.UpsertFeedbackSnapshot(
		ctx,
		publication,
		metrics,
		schedule.SnapshotStage,
		score,
		fields,
		reward,
		rewardComponents,
	); err != nil {
		return err
	}
	now := s.Now().UTC()
	availableFields := append([]string(nil), fields...)
	sort.Strings(availableFields)
	fieldsJSON := mustJSON(availableFields)
	tag, err := s.db().Exec(ctx, `
		UPDATE publication_metric_schedules
		SET status = $2,
		    attempt_count = attempt_count + 1,
		    last_attempt_at = $3::timestamptz,
		    completed_at = $3::timestamptz,
		    available_fields_json = $4::json,
		    last_error_code = NULL,
		    updated_at = $3::timestamp
		WHERE id = $1::uuid AND status = $5
	`, schedule.ID, MetricScheduleSucceeded, now, fieldsJSON, MetricSchedulePending)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: metric schedule completion lost authority", ErrQueueAuthorityInvalid)
	}
	return nil
}

func isMetricScheduleStage(stage string) bool {
	for _, spec := range metricStageSpecs {
		if stage == spec.Stage {
			return true
		}
	}
	return false
}

func scanMetricSchedule(row queueItemScanner) (MetricScheduleRow, error) {
	var schedule MetricScheduleRow
	var availableFieldsJSON []byte
	err := row.Scan(
		&schedule.ID,
		&schedule.PublicationID,
		&schedule.SnapshotStage,
		&schedule.EffectiveStartAt,
		&schedule.DueAt,
		&schedule.GraceUntil,
		&schedule.Status,
		&schedule.AttemptCount,
		&schedule.LastAttemptAt,
		&schedule.CompletedAt,
		&availableFieldsJSON,
		&schedule.LastErrorCode,
		&schedule.CreatedAt,
		&schedule.UpdatedAt,
	)
	if err != nil {
		return MetricScheduleRow{}, err
	}
	if err := json.Unmarshal(availableFieldsJSON, &schedule.AvailableFieldsJSON); err != nil {
		return MetricScheduleRow{}, err
	}
	return schedule, nil
}
