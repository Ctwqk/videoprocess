package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	preUploadReviewScope = "external_asset_pre_upload"
	promotionReviewScope = "publication_promotion"
)

type planReviewAuthority struct {
	ID                   string
	Status               string
	Rights               map[string]any
	ReviewApprovedAt     *time.Time
	ApprovedRevisionHash *string
}

func (s *Store) ValidPreUploadHumanReview(ctx context.Context, task ProductionTaskRow) (bool, error) {
	if task.AutoFlowPlanID == nil || strings.TrimSpace(*task.AutoFlowPlanID) == "" {
		return false, nil
	}
	plan, err := s.loadPlanReviewAuthority(ctx, *task.AutoFlowPlanID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if invalidReviewPlan(plan) || plan.ReviewApprovedAt == nil || plan.ApprovedRevisionHash == nil ||
		strings.TrimSpace(*plan.ApprovedRevisionHash) == "" || plan.ID != *task.AutoFlowPlanID {
		return false, nil
	}
	evidence := mapFromAny(task.HumanReviewEvidenceJSON["pre_upload"])
	if firstString(evidence, "kind") != "human_review" || firstString(evidence, "scope") != preUploadReviewScope {
		return false, nil
	}
	if strings.TrimSpace(firstString(evidence, "human_actor")) == "" || !validReviewTimestamp(firstString(evidence, "reviewed_at")) {
		return false, nil
	}
	if firstString(evidence, "autoflow_plan_id") != plan.ID {
		return false, nil
	}
	if firstString(evidence, "plan_approved_revision_hash") != *plan.ApprovedRevisionHash {
		return false, nil
	}
	return timestampMatches(firstString(evidence, "plan_review_approved_at"), *plan.ReviewApprovedAt), nil
}

func (s *Store) ValidPromotionHumanReview(
	ctx context.Context,
	task ProductionTaskRow,
	publication PublicationRow,
	targetVisibility string,
) (bool, error) {
	evidence := mapFromAny(task.HumanReviewEvidenceJSON["promotion"])
	if firstString(evidence, "kind") != "human_review" || firstString(evidence, "scope") != promotionReviewScope {
		return false, nil
	}
	if strings.TrimSpace(firstString(evidence, "human_actor")) == "" || !validReviewTimestamp(firstString(evidence, "reviewed_at")) {
		return false, nil
	}
	if firstString(evidence, "production_task_id") != task.ID || firstString(evidence, "publication_id") != publication.ID {
		return false, nil
	}
	if firstString(evidence, "target_visibility") != targetVisibility {
		return false, nil
	}
	if !taskUsesExternalAssets(task) {
		return true, nil
	}
	validPreUpload, err := s.ValidPreUploadHumanReview(ctx, task)
	if err != nil || !validPreUpload {
		return validPreUpload, err
	}
	preUpload := mapFromAny(task.HumanReviewEvidenceJSON["pre_upload"])
	return firstString(evidence, "autoflow_plan_id") == firstString(preUpload, "autoflow_plan_id") &&
		firstString(evidence, "plan_review_approved_at") == firstString(preUpload, "plan_review_approved_at") &&
		firstString(evidence, "plan_approved_revision_hash") == firstString(preUpload, "plan_approved_revision_hash"), nil
}

func (s *Store) loadPlanReviewAuthority(ctx context.Context, planID string) (planReviewAuthority, error) {
	if err := requireUUID("autoflow_plan_id", planID); err != nil {
		return planReviewAuthority{}, err
	}
	var plan planReviewAuthority
	var rightsJSON []byte
	err := s.db().QueryRow(ctx, `
		SELECT id, status, rights_json, review_approved_at, approved_revision_hash
		FROM autoflow_plans
		WHERE id = $1::uuid
	`, planID).Scan(&plan.ID, &plan.Status, &rightsJSON, &plan.ReviewApprovedAt, &plan.ApprovedRevisionHash)
	if err != nil {
		return planReviewAuthority{}, err
	}
	if err := json.Unmarshal(rightsJSON, &plan.Rights); err != nil {
		return planReviewAuthority{}, err
	}
	return plan, nil
}

func invalidReviewPlan(plan planReviewAuthority) bool {
	status := strings.ToLower(strings.TrimSpace(plan.Status))
	rightsStatus := strings.ToLower(strings.TrimSpace(firstString(plan.Rights, "status")))
	return status == "blocked" || status == "rejected" || rightsStatus == "blocked" || rightsStatus == "rejected"
}

func validReviewTimestamp(raw string) bool {
	_, err := time.Parse(time.RFC3339Nano, raw)
	return err == nil
}

func timestampMatches(raw string, expected time.Time) bool {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	return err == nil && parsed.Equal(expected)
}

func (h HandlerService) holdInvalidPreUploadReview(ctx context.Context, task ProductionTaskRow, transitionReason string) (bool, error) {
	if !taskUsesExternalAssets(task) {
		return false, nil
	}
	valid, err := h.Store.ValidPreUploadHumanReview(ctx, task)
	if err != nil {
		return true, err
	}
	if valid {
		return false, nil
	}
	return true, h.Store.HoldTask(
		ctx,
		task.ID,
		"human_review_evidence_invalid",
		"External asset human review evidence is missing or stale",
		transitionReason,
	)
}
