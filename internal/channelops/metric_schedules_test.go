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
