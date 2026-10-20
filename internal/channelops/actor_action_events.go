package channelops

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Actor-action events feed vp-feature-aggregator's per-actor windows, which
// PDS reads on every decision. They are written to event_outbox in the same
// transaction as the state change they describe and published to Kafka by
// vp-event-outbox-relay-swarm. Payloads must satisfy
// services/vp-feature-aggregator/schemas/vp.actor.actions.v1.json, mirroring
// backend/app/events/schemas.py build_actor_action_event.
const (
	actorActionsTopic  = "vp.actor.actions.v1"
	actorActionsSource = "videoprocess.channel_ops"
)

const (
	ActorActionCandidateAccepted             = "candidate_accepted"
	ActorActionCandidateBlocked              = "candidate_blocked"
	ActorActionCandidateFlagged              = "candidate_flagged"
	ActorActionPublicationPromotionAttempted = "publication_promotion_attempted"
	ActorActionPublicationPromotionBlocked   = "publication_promotion_blocked"
	ActorActionPublicationScheduled          = "publication_scheduled"
)

// Python's datetime.isoformat() precision; the aggregator parses with Pydantic.
const actorActionTimeLayout = "2006-01-02T15:04:05.000000Z07:00"

func buildActorActionEvent(
	eventID string,
	actorID string,
	actionType string,
	platform string,
	metadata map[string]any,
	occurredAt time.Time,
) map[string]any {
	if metadata == nil {
		metadata = map[string]any{}
	}
	return map[string]any{
		"event_id":      eventID,
		"topic_version": actorActionsTopic,
		"actor_id":      actorID,
		"action_type":   actionType,
		"platform":      platform,
		"occurred_at":   occurredAt.UTC().Format(actorActionTimeLayout),
		"source":        actorActionsSource,
		"metadata":      metadata,
	}
}

// pdsDecisionEventMetadata mirrors _pds_decision_event_metadata in
// backend/app/channel_agent/service.py.
func pdsDecisionEventMetadata(decision PDSDecision) map[string]any {
	reasonCodes := []string{}
	for _, reason := range decision.Reasons {
		if code, ok := reason["code"].(string); ok && code != "" {
			reasonCodes = append(reasonCodes, code)
		}
	}
	metadata := map[string]any{
		"decision_id":   decision.DecisionID,
		"verdict":       decision.Verdict,
		"score":         decision.Score,
		"rules_version": decision.RulesVersion,
		"reason_codes":  reasonCodes,
	}
	if warning, ok := decision.Metadata["warning"]; ok && warning != nil {
		metadata["warning"] = warning
	}
	return metadata
}

func mergeEventMetadata(parts ...map[string]any) map[string]any {
	merged := map[string]any{}
	for _, part := range parts {
		for key, value := range part {
			merged[key] = value
		}
	}
	return merged
}

func eventPlatform(platform string) string {
	if strings.TrimSpace(platform) == "" {
		return "youtube"
	}
	return platform
}

func (s *Store) enqueueActorAction(
	ctx context.Context,
	actorID string,
	actionType string,
	platform string,
	metadata map[string]any,
) error {
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	eventID := uuid.NewString()
	payload, err := json.Marshal(buildActorActionEvent(eventID, actorID, actionType, eventPlatform(platform), metadata, now))
	if err != nil {
		return err
	}
	_, err = s.db().Exec(ctx, `
		INSERT INTO event_outbox (id, topic, key, payload, created_at, attempt_count)
		VALUES ($1, $2, $3, $4::json, $5::timestamptz, 0)
	`, eventID, actorActionsTopic, actorID, payload, now)
	return err
}

// enqueueTickCandidateEvent records a tick-stage candidate outcome: accepted
// (taskID set) or rejected by the candidate_accept PDS decision. Mirrors the
// Python runner's _pds_candidate_guard and _emit_candidate_accepted.
func (s *Store) enqueueTickCandidateEvent(ctx context.Context, candidate TickCandidate, taskID string) error {
	if candidate.Account == nil {
		return nil
	}
	actionType := ActorActionCandidateAccepted
	metadata := map[string]any{
		"lane_id":      "",
		"candidate_id": candidate.CandidateID,
	}
	if candidate.Lane != nil {
		metadata["lane_id"] = candidate.Lane.ID
	}
	if taskID != "" {
		metadata["task_id"] = taskID
		metadata["source"] = candidate.Source
	} else {
		switch candidate.RejectionGuard {
		case "pds_blocked":
			actionType = ActorActionCandidateBlocked
		case "pds_flagged_for_review":
			actionType = ActorActionCandidateFlagged
		default:
			return nil
		}
		metadata["guard"] = candidate.RejectionGuard
	}
	if len(candidate.PDSDecisionJSON) > 0 {
		metadata = mergeEventMetadata(metadata, pdsDecisionEventMetadata(pdsDecisionFromAuditJSON(candidate.PDSDecisionJSON)))
	}
	return s.enqueueActorAction(ctx, candidate.Account.ID, actionType, candidate.Account.Platform, metadata)
}

// pdsDecisionFromAuditJSON reverses pdsDecisionAuditJSON for event metadata.
func pdsDecisionFromAuditJSON(audit map[string]any) PDSDecision {
	decision := PDSDecision{}
	decision.Metadata, _ = audit["metadata"].(map[string]any)
	decision.DecisionID, _ = audit["decision_id"].(string)
	decision.Verdict, _ = audit["verdict"].(string)
	decision.RulesVersion, _ = audit["rules_version"].(string)
	decision.Score, _ = audit["score"].(float64)
	switch reasons := audit["reasons"].(type) {
	case []map[string]any:
		decision.Reasons = reasons
	case []any:
		for _, reason := range reasons {
			if mapped, ok := reason.(map[string]any); ok {
				decision.Reasons = append(decision.Reasons, mapped)
			}
		}
	}
	return decision
}

func promotionEventMetadata(publication PublicationRow, targetVisibility string, scheduledAt time.Time) map[string]any {
	return map[string]any{
		"task_id":           publication.ProductionTaskID,
		"publication_id":    publication.ID,
		"target_visibility": targetVisibility,
		"scheduled_at":      scheduledAt.UTC().Format(actorActionTimeLayout),
	}
}

func (s *Store) enqueuePublicationEvent(
	ctx context.Context,
	publication PublicationRow,
	actionType string,
	metadata map[string]any,
) error {
	return s.enqueueActorAction(ctx, publication.AccountID, actionType, publication.Platform, metadata)
}
