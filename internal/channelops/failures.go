package channelops

import "strings"

func FailureCategoryFor(context string, reason string) string {
	normalizedContext := strings.ToLower(strings.TrimSpace(context))
	lower := strings.ToLower(strings.TrimSpace(context + " " + reason))

	switch {
	case containsAny(lower, "pds"):
		return FailurePDS
	case strings.Contains(normalizedContext, QueueCollectMetrics) || containsAny(lower, "metrics", "analytics"):
		return FailureMetrics
	case containsAny(lower, "auth", "oauth", "token", "credential"):
		return FailureAuth
	case containsAny(lower, "quota"):
		return FailureQuota
	case containsAny(lower, "discovery", "trend signal"):
		return FailureDiscovery
	case containsAny(lower, "learning", "reward"):
		return FailureLearning
	case containsAny(lower, "validation", "invalid"):
		return FailureValidation
	case strings.Contains(normalizedContext, QueuePlanTask) || containsAny(lower, "planner", "planning", "schema"):
		return FailurePlanning
	case containsAny(lower, "takedown", "publish_status", "rejected", "removed") ||
		(strings.Contains(lower, "status") && !strings.Contains(lower, "autoflow job status")):
		return FailureYouTubeStatus
	case strings.Contains(normalizedContext, QueuePublishTask) || containsAny(lower, "upload", "publish", "youtube", "video_id", "thumbnail"):
		return FailureUpload
	case strings.Contains(normalizedContext, QueueExecuteTask) ||
		strings.Contains(normalizedContext, QueueObserveJob) ||
		containsAny(lower, "render", "autoflow", "worker"):
		return FailureRender
	default:
		return FailureOther
	}
}

func holdFailureCategoryFor(guard string, reason string, transitionReason string, decision any) string {
	if decision != nil || containsAny(strings.ToLower(transitionReason+" "+guard), "pds") {
		return FailurePDS
	}
	if isValidationHold(guard, reason, transitionReason) {
		return FailureValidation
	}
	return FailureCategoryFor(transitionReason, strings.TrimSpace(guard+" "+reason))
}

func isValidationHold(guard string, reason string, transitionReason string) bool {
	lower := strings.ToLower(guard + " " + reason + " " + transitionReason)
	return containsAny(
		lower,
		"missing_youtube_upload_node",
		"human_approval_required",
		"external_asset_auto_publish_required",
		"validation",
		"invalid",
		"must contain",
		"requires human approval",
		"requires human review",
	)
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}
