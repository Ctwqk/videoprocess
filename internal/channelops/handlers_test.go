package channelops

import (
	"testing"
	"time"
)

func TestPlanDecisionFlagHoldsTask(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-flag", Verdict: "flag"})
	if result.NextState != TaskHeld {
		t.Fatalf("NextState = %s", result.NextState)
	}
	if result.BlockedByGuard != "pds_flagged_for_review" {
		t.Fatalf("BlockedByGuard = %s", result.BlockedByGuard)
	}
	if result.EnqueueExecute {
		t.Fatal("flagged plan must not enqueue execute")
	}
}

func TestPlanDecisionBlockHoldsTask(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-block", Verdict: "block"})
	if result.NextState != TaskHeld || result.BlockedByGuard != "pds_blocked" || result.EnqueueExecute {
		t.Fatalf("result = %#v", result)
	}
}

func TestPlanDecisionAllowEnqueuesExecute(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-allow", Verdict: "allow"})
	if result.NextState != TaskPlanning || !result.EnqueueExecute {
		t.Fatalf("result = %#v", result)
	}
}

func TestExistingExecutionRequiresRunAndJob(t *testing.T) {
	runID := "00000000-0000-0000-0000-000000000201"
	jobID := "00000000-0000-0000-0000-000000000301"
	task := ProductionTaskRow{AutoFlowRunID: &runID, JobID: &jobID}

	gotRunID, gotJobID, ok := ExistingExecution(task)
	if !ok {
		t.Fatal("existing execution should be detected")
	}
	if gotRunID != runID || gotJobID != jobID {
		t.Fatalf("execution = %s/%s", gotRunID, gotJobID)
	}

	task.JobID = nil
	if _, _, ok := ExistingExecution(task); ok {
		t.Fatal("run without job should not count as existing execution")
	}
}

func TestTakedownDedupKeyUsesPublicationEventDay(t *testing.T) {
	key := TakedownDedupKey("pub-1", "rejected", mustTime("2026-05-21T17:15:00Z"))
	if key != "pub-1:rejected:2026-05-21" {
		t.Fatalf("key = %s", key)
	}
}

func TestPromotionVisibilityDoesNotAllowPublic(t *testing.T) {
	if got := safePromotionVisibility("public"); got != "" {
		t.Fatalf("public promotion visibility = %q", got)
	}
	if got := safePromotionVisibility("unlisted"); got != "unlisted" {
		t.Fatalf("unlisted promotion visibility = %q", got)
	}
}

func TestObservedPrivacyAllowsPublic(t *testing.T) {
	if got := observedPrivacy("public"); got != "public" {
		t.Fatalf("observed privacy = %q", got)
	}
}

func mustTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
