package channelops

import (
	"reflect"
	"testing"
	"time"
)

func TestMetricStageSpecs(t *testing.T) {
	want := []MetricStageSpec{
		{Stage: "1h", DueAfter: time.Hour, GraceAfter: 3 * time.Hour},
		{Stage: "6h", DueAfter: 6 * time.Hour, GraceAfter: 12 * time.Hour},
		{Stage: "24h", DueAfter: 24 * time.Hour, GraceAfter: 30 * time.Hour},
		{Stage: "72h", DueAfter: 72 * time.Hour, GraceAfter: 84 * time.Hour},
		{Stage: "7d", DueAfter: 168 * time.Hour, GraceAfter: 192 * time.Hour},
	}

	if got := MetricStageSpecs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("MetricStageSpecs() = %#v, want %#v", got, want)
	}
}

func TestMetricStageSpecsReturnsIndependentSlice(t *testing.T) {
	first := MetricStageSpecs()
	first[0].Stage = "mutated"

	if got := MetricStageSpecs()[0].Stage; got != "1h" {
		t.Fatalf("MetricStageSpecs()[0].Stage = %q, want 1h", got)
	}
}

func TestBuildMetricSchedulePlansCreatesFiveStageSpecificQueueFacts(t *testing.T) {
	publicationID := "00000000-0000-0000-0000-000000000101"
	effectiveStart := time.Date(2026, 7, 21, 20, 0, 0, 0, time.UTC)

	plans := BuildMetricSchedulePlans(publicationID, effectiveStart)
	if len(plans) != 5 {
		t.Fatalf("plan count = %d, want 5", len(plans))
	}
	for index, plan := range plans {
		spec := MetricStageSpecs()[index]
		if plan.Stage != spec.Stage {
			t.Fatalf("plan[%d].Stage = %q, want %q", index, plan.Stage, spec.Stage)
		}
		if !plan.EffectiveStart.Equal(effectiveStart) {
			t.Fatalf("plan[%d].EffectiveStart = %s, want %s", index, plan.EffectiveStart, effectiveStart)
		}
		if want := effectiveStart.Add(spec.DueAfter); !plan.DueAt.Equal(want) {
			t.Fatalf("plan[%d].DueAt = %s, want %s", index, plan.DueAt, want)
		}
		if want := effectiveStart.Add(spec.GraceAfter); !plan.GraceUntil.Equal(want) {
			t.Fatalf("plan[%d].GraceUntil = %s, want %s", index, plan.GraceUntil, want)
		}
		wantKey := "collect_metrics:" + publicationID + ":stage:" + spec.Stage + ":attempt:0"
		if plan.IdempotencyKey != wantKey {
			t.Fatalf("plan[%d].IdempotencyKey = %q, want %q", index, plan.IdempotencyKey, wantKey)
		}
	}
}

func TestMetricScheduleRetryDecisionRetriesBeforeGrace(t *testing.T) {
	now := time.Date(2026, 7, 21, 21, 0, 0, 0, time.UTC)
	schedule := MetricScheduleRow{
		AttemptCount: 0,
		GraceUntil:   now.Add(2 * time.Hour),
	}

	decision := MetricScheduleRetryDecision(schedule, now, 24, time.Hour)
	if decision.Expire {
		t.Fatal("first unavailable attempt unexpectedly expired")
	}
	if decision.AttemptCount != 1 {
		t.Fatalf("attempt count = %d, want 1", decision.AttemptCount)
	}
	if want := now.Add(time.Hour); !decision.RunAfter.Equal(want) {
		t.Fatalf("run_after = %s, want %s", decision.RunAfter, want)
	}
}

func TestMetricScheduleRetryDecisionClampsToGrace(t *testing.T) {
	now := time.Date(2026, 7, 21, 21, 0, 0, 0, time.UTC)
	grace := now.Add(20 * time.Minute)
	schedule := MetricScheduleRow{AttemptCount: 4, GraceUntil: grace}

	decision := MetricScheduleRetryDecision(schedule, now, 24, time.Hour)
	if decision.Expire {
		t.Fatal("attempt before grace unexpectedly expired")
	}
	if !decision.RunAfter.Equal(grace) {
		t.Fatalf("run_after = %s, want grace %s", decision.RunAfter, grace)
	}
}

func TestMetricScheduleRetryDecisionExpiresAtGraceOrAttemptCap(t *testing.T) {
	now := time.Date(2026, 7, 21, 21, 0, 0, 0, time.UTC)

	for name, schedule := range map[string]MetricScheduleRow{
		"grace":       {AttemptCount: 0, GraceUntil: now},
		"attempt cap": {AttemptCount: 2, GraceUntil: now.Add(time.Hour)},
	} {
		t.Run(name, func(t *testing.T) {
			decision := MetricScheduleRetryDecision(schedule, now, 3, time.Hour)
			if !decision.Expire {
				t.Fatalf("decision = %#v, want expiration", decision)
			}
			if decision.AttemptCount != schedule.AttemptCount+1 {
				t.Fatalf("attempt count = %d, want %d", decision.AttemptCount, schedule.AttemptCount+1)
			}
			if !decision.RunAfter.IsZero() {
				t.Fatalf("expired run_after = %s, want zero", decision.RunAfter)
			}
		})
	}
}
