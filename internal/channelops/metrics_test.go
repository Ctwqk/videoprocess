package channelops

import (
	"math"
	"testing"
)

func TestMetricsCompletenessPartial(t *testing.T) {
	score, fields := MetricsCompleteness(map[string]any{"views": 100, "likes": 9})

	if math.Abs(score-0.25) > 0.0001 {
		t.Fatalf("score = %f", score)
	}
	if len(fields) != 2 || fields[0] != "views" || fields[1] != "likes" {
		t.Fatalf("fields = %#v", fields)
	}
}

func TestMetricsCompletenessRetentionAndImpressions(t *testing.T) {
	score, fields := MetricsCompleteness(map[string]any{
		"retention_curve": []any{0.9, 0.7},
		"impressions":     500,
	})

	if math.Abs(score-0.35) > 0.0001 {
		t.Fatalf("score = %f fields=%#v", score, fields)
	}
	if len(fields) != 2 || fields[0] != "retention_curve_json" || fields[1] != "impressions" {
		t.Fatalf("fields = %#v", fields)
	}
}

func TestHasRecognizedMetrics(t *testing.T) {
	if HasRecognizedMetrics(map[string]any{"unknown": 1}) {
		t.Fatal("unknown metric should not be recognized")
	}
	if !HasRecognizedMetrics(map[string]any{"ctr": 0.12}) {
		t.Fatal("ctr should be recognized")
	}
}

func TestMetricsCompletenessIgnoresInvalidMetricValues(t *testing.T) {
	score, fields := MetricsCompleteness(map[string]any{
		"views":           nil,
		"likes":           "",
		"retention_curve": []any{},
		"ctr":             []int64{},
		"impressions":     map[string]string{},
	})

	if score != 0 {
		t.Fatalf("score = %f", score)
	}
	if len(fields) != 0 {
		t.Fatalf("fields = %#v", fields)
	}
	if HasRecognizedMetrics(map[string]any{
		"views":           nil,
		"likes":           "",
		"retention_curve": []any{},
		"ctr":             []int64{},
		"impressions":     map[string]string{},
	}) {
		t.Fatal("invalid metric values should not be recognized")
	}
}

func TestMetricsCompletenessIgnoresNaNAndInf(t *testing.T) {
	score, fields := MetricsCompleteness(map[string]any{
		"views": math.NaN(),
		"likes": math.Inf(1),
		"ctr":   float32(math.Inf(-1)),
	})

	if score != 0 {
		t.Fatalf("score = %f", score)
	}
	if len(fields) != 0 {
		t.Fatalf("fields = %#v", fields)
	}
}
