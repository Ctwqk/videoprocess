package channelops

import "testing"

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
