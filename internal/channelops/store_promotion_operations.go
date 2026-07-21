package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrPromotionOperationConflict = errors.New("promotion operation conflict")

func (s *Store) GetPromotionOperationForPublication(
	ctx context.Context,
	publicationID string,
) (*PromotionOperationRow, error) {
	if err := requireUUID("publication_id", publicationID); err != nil {
		return nil, err
	}
	operation, err := scanPromotionOperation(s.db().QueryRow(ctx, promotionOperationSelect+`
		WHERE publication_id = $1::uuid
	`, publicationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &operation, nil
}

func (s *Store) ReservePromotionOperation(
	ctx context.Context,
	publication PublicationRow,
	queueItemID string,
	targetPrivacy string,
	scheduledAt time.Time,
	decision PDSDecision,
) (PromotionOperationRow, error) {
	if err := requireUUID("publication_id", publication.ID); err != nil {
		return PromotionOperationRow{}, err
	}
	if err := requireUUID("production_task_id", publication.ProductionTaskID); err != nil {
		return PromotionOperationRow{}, err
	}
	if err := requireUUID("queue_item_id", queueItemID); err != nil {
		return PromotionOperationRow{}, err
	}
	privacy := safePromotionVisibility(targetPrivacy)
	if privacy == "" {
		return PromotionOperationRow{}, fmt.Errorf("%w: target privacy must be private or unlisted", ErrPromotionOperationConflict)
	}
	if strings.TrimSpace(publication.PlatformContentID) == "" {
		return PromotionOperationRow{}, fmt.Errorf("%w: platform video id is required", ErrPromotionOperationConflict)
	}
	if scheduledAt.IsZero() {
		scheduledAt = s.Now().UTC()
	}
	operationID := uuid.NewString()
	attemptKey := "channelops-promotion:" + operationID
	now := s.Now().UTC()
	operation, err := scanPromotionOperation(s.db().QueryRow(ctx, `
		INSERT INTO publication_promotion_operations (
			id, publication_id, production_task_id, queue_item_id, platform_video_id,
			target_privacy, scheduled_at, attempt_key, status, decision_json,
			observed_privacy, observed_publish_status, evidence_json, error_message,
			request_attempted_at, confirmed_at, completed_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4::uuid, $5,
			$6, $7::timestamptz, $8, $9, $10::json,
			NULL, NULL, '{}'::json, NULL, NULL, NULL, NULL, $11::timestamp, $11::timestamp
		)
		ON CONFLICT (publication_id) DO UPDATE
		SET decision_json = CASE
				WHEN publication_promotion_operations.status = 'finalized'
					THEN publication_promotion_operations.decision_json
				ELSE EXCLUDED.decision_json
			END,
			updated_at = CASE
				WHEN publication_promotion_operations.status = 'finalized'
					THEN publication_promotion_operations.updated_at
				ELSE EXCLUDED.updated_at
			END
		RETURNING id, publication_id, production_task_id, queue_item_id,
		          platform_video_id, target_privacy, scheduled_at, attempt_key, status,
		          decision_json, observed_privacy, observed_publish_status, evidence_json,
		          error_message, request_attempted_at, confirmed_at, completed_at,
		          created_at, updated_at
	`, operationID, publication.ID, publication.ProductionTaskID, queueItemID,
		publication.PlatformContentID, privacy, scheduledAt.UTC(), attemptKey,
		PromotionReserved, mustJSON(decision), now))
	if err != nil {
		return PromotionOperationRow{}, err
	}
	if operation.ProductionTaskID != publication.ProductionTaskID ||
		operation.QueueItemID != queueItemID ||
		operation.PlatformVideoID != publication.PlatformContentID ||
		operation.TargetPrivacy != privacy {
		return PromotionOperationRow{}, fmt.Errorf(
			"%w: existing operation does not match publication/video/target",
			ErrPromotionOperationConflict,
		)
	}
	return operation, nil
}

func (s *Store) BeginPromotionSubmission(
	ctx context.Context,
	operationID string,
) (PromotionOperationRow, bool, error) {
	if err := requireUUID("promotion_operation_id", operationID); err != nil {
		return PromotionOperationRow{}, false, err
	}
	now := s.Now().UTC()
	operation, err := scanPromotionOperation(s.db().QueryRow(ctx, `
		UPDATE publication_promotion_operations
		SET status = $2,
		    request_attempted_at = COALESCE(request_attempted_at, $3::timestamptz),
		    error_message = NULL,
		    updated_at = $3::timestamp
		WHERE id = $1::uuid AND status = $4
		RETURNING id, publication_id, production_task_id, queue_item_id,
		          platform_video_id, target_privacy, scheduled_at, attempt_key, status,
		          decision_json, observed_privacy, observed_publish_status, evidence_json,
		          error_message, request_attempted_at, confirmed_at, completed_at,
		          created_at, updated_at
	`, operationID, PromotionSubmitting, now, PromotionReserved))
	if err == nil {
		return operation, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return PromotionOperationRow{}, false, err
	}
	operation, err = s.getPromotionOperation(ctx, operationID, false)
	return operation, false, err
}

func (s *Store) ConfirmPromotionOperation(
	ctx context.Context,
	operationID string,
	status YouTubePublicationStatus,
	evidence map[string]any,
) (PromotionOperationRow, error) {
	privacy := observedPrivacy(status.Privacy)
	if privacy == "" {
		return PromotionOperationRow{}, fmt.Errorf("promotion confirmation requires observed privacy")
	}
	videoID := strings.TrimSpace(status.VideoID)
	now := s.Now().UTC()
	operation, err := scanPromotionOperation(s.db().QueryRow(ctx, `
		UPDATE publication_promotion_operations
		SET status = $2,
		    observed_privacy = $3,
		    observed_publish_status = NULLIF($4, ''),
		    evidence_json = (
		        COALESCE(evidence_json, '{}'::json)::jsonb || $5::jsonb
		    )::json,
		    error_message = NULL,
		    confirmed_at = COALESCE(confirmed_at, $7::timestamptz),
		    updated_at = $7::timestamp
		WHERE id = $1::uuid
		  AND target_privacy = $3
		  AND ($6 = '' OR platform_video_id = $6)
		  AND status IN ($8, $9, $2)
		RETURNING id, publication_id, production_task_id, queue_item_id,
		          platform_video_id, target_privacy, scheduled_at, attempt_key, status,
		          decision_json, observed_privacy, observed_publish_status, evidence_json,
		          error_message, request_attempted_at, confirmed_at, completed_at,
		          created_at, updated_at
	`, operationID, PromotionConfirmed, privacy, normalizedStatus(status.PublishStatus),
		mustJSON(evidence), videoID, now, PromotionSubmitting, PromotionUncertain))
	if errors.Is(err, pgx.ErrNoRows) {
		operation, err = s.getPromotionOperation(ctx, operationID, false)
	}
	if err != nil {
		return PromotionOperationRow{}, err
	}
	if operation.Status == PromotionFinalized {
		return operation, nil
	}
	if operation.Status != PromotionConfirmed || operation.TargetPrivacy != privacy ||
		(videoID != "" && operation.PlatformVideoID != videoID) {
		return PromotionOperationRow{}, fmt.Errorf(
			"%w: observed privacy %q does not confirm target %q",
			ErrPromotionOperationConflict,
			privacy,
			operation.TargetPrivacy,
		)
	}
	return operation, nil
}

func (s *Store) MarkPromotionOperationUncertain(
	ctx context.Context,
	operationID string,
	status YouTubePublicationStatus,
	reason string,
) (PromotionOperationRow, error) {
	now := s.Now().UTC()
	privacy := observedPrivacy(status.Privacy)
	evidence := map[string]any{
		"reconciliation": map[string]any{
			"observed_privacy":        privacy,
			"observed_publish_status": normalizedStatus(status.PublishStatus),
			"checked_at":              now.Format(time.RFC3339Nano),
		},
	}
	operation, err := scanPromotionOperation(s.db().QueryRow(ctx, `
		UPDATE publication_promotion_operations
		SET status = $2,
		    observed_privacy = NULLIF($3, ''),
		    observed_publish_status = NULLIF($4, ''),
		    evidence_json = (
		        COALESCE(evidence_json, '{}'::json)::jsonb || $5::jsonb
		    )::json,
		    error_message = $6,
		    updated_at = $7::timestamp
		WHERE id = $1::uuid AND status IN ($8, $2)
		RETURNING id, publication_id, production_task_id, queue_item_id,
		          platform_video_id, target_privacy, scheduled_at, attempt_key, status,
		          decision_json, observed_privacy, observed_publish_status, evidence_json,
		          error_message, request_attempted_at, confirmed_at, completed_at,
		          created_at, updated_at
	`, operationID, PromotionUncertain, privacy, normalizedStatus(status.PublishStatus),
		mustJSON(evidence), reason, now, PromotionSubmitting))
	if errors.Is(err, pgx.ErrNoRows) {
		operation, err = s.getPromotionOperation(ctx, operationID, false)
	}
	return operation, err
}

func (s *Store) LockPromotionOperation(
	ctx context.Context,
	operationID string,
) (PromotionOperationRow, error) {
	return s.getPromotionOperation(ctx, operationID, true)
}

func (s *Store) FinalizePromotionOperation(
	ctx context.Context,
	operationID string,
	metricsDelay time.Duration,
) error {
	operation, err := s.LockPromotionOperation(ctx, operationID)
	if err != nil {
		return err
	}
	if operation.Status == PromotionFinalized {
		return nil
	}
	if operation.Status != PromotionConfirmed {
		return fmt.Errorf(
			"%w: cannot finalize operation in %s state",
			ErrPromotionOperationConflict,
			operation.Status,
		)
	}
	if operation.ObservedPrivacy == nil || observedPrivacy(*operation.ObservedPrivacy) != operation.TargetPrivacy {
		return fmt.Errorf(
			"%w: confirmed operation does not match observed target privacy",
			ErrPromotionOperationConflict,
		)
	}
	if err := s.PromotePublication(
		ctx,
		operation.PublicationID,
		operation.TargetPrivacy,
		operation.ScheduledAt,
		operation.Decision,
		operation.QueueItemID,
		metricsDelay,
	); err != nil {
		return err
	}
	now := s.Now().UTC()
	tag, err := s.db().Exec(ctx, `
		UPDATE publication_promotion_operations
		SET status = $2,
		    completed_at = COALESCE(completed_at, $3::timestamptz),
		    error_message = NULL,
		    updated_at = $3::timestamp
		WHERE id = $1::uuid AND status = $4
	`, operation.ID, PromotionFinalized, now, PromotionConfirmed)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: promotion operation finalization lost authority", ErrPromotionOperationConflict)
	}
	return nil
}

func (s *Store) HoldPromotionOperationUncertain(
	ctx context.Context,
	publication PublicationRow,
	operation PromotionOperationRow,
	reason string,
) error {
	now := s.Now().UTC()
	if _, err := s.db().Exec(ctx, `
		UPDATE publication_records
		SET warnings_json = (
		        COALESCE(warnings_json, '[]'::json)::jsonb ||
		        jsonb_build_array(jsonb_build_object(
		            'promotion_operation_id', $2::text,
		            'promotion_status', $3::text,
		            'reason', $4::text
		        ))
		    )::json,
		    updated_at = $5::timestamp
		WHERE id = $1::uuid
	`, publication.ID, operation.ID, operation.Status, reason, now); err != nil {
		return err
	}
	return s.updateTaskState(
		ctx,
		publication.ProductionTaskID,
		TaskHeld,
		"promotion_outcome_uncertain",
		reason,
		"promote_publication_uncertain",
		FailureYouTubeStatus,
		now,
	)
}

func (s *Store) getPromotionOperation(
	ctx context.Context,
	operationID string,
	lock bool,
) (PromotionOperationRow, error) {
	if err := requireUUID("promotion_operation_id", operationID); err != nil {
		return PromotionOperationRow{}, err
	}
	query := promotionOperationSelect + " WHERE id = $1::uuid"
	if lock {
		query += " FOR UPDATE"
	}
	return scanPromotionOperation(s.db().QueryRow(ctx, query, operationID))
}

const promotionOperationSelect = `
	SELECT id, publication_id, production_task_id, queue_item_id,
	       platform_video_id, target_privacy, scheduled_at, attempt_key, status,
	       decision_json, observed_privacy, observed_publish_status, evidence_json,
	       error_message, request_attempted_at, confirmed_at, completed_at,
	       created_at, updated_at
	FROM publication_promotion_operations
`

func scanPromotionOperation(row pgx.Row) (PromotionOperationRow, error) {
	var operation PromotionOperationRow
	var decisionJSON []byte
	var evidenceJSON []byte
	err := row.Scan(
		&operation.ID,
		&operation.PublicationID,
		&operation.ProductionTaskID,
		&operation.QueueItemID,
		&operation.PlatformVideoID,
		&operation.TargetPrivacy,
		&operation.ScheduledAt,
		&operation.AttemptKey,
		&operation.Status,
		&decisionJSON,
		&operation.ObservedPrivacy,
		&operation.ObservedPublishStatus,
		&evidenceJSON,
		&operation.ErrorMessage,
		&operation.RequestAttemptedAt,
		&operation.ConfirmedAt,
		&operation.CompletedAt,
		&operation.CreatedAt,
		&operation.UpdatedAt,
	)
	if err != nil {
		return PromotionOperationRow{}, err
	}
	if err := json.Unmarshal(decisionJSON, &operation.Decision); err != nil {
		return PromotionOperationRow{}, err
	}
	if err := json.Unmarshal(evidenceJSON, &operation.EvidenceJSON); err != nil {
		return PromotionOperationRow{}, err
	}
	if operation.EvidenceJSON == nil {
		operation.EvidenceJSON = map[string]any{}
	}
	return operation, nil
}
