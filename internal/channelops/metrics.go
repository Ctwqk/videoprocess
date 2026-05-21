package channelops

import (
	"math"
	"reflect"
	"strings"
)

var metricWeights = []struct {
	Key     string
	Aliases []string
	Weight  float64
}{
	{Key: "views", Aliases: []string{"views"}, Weight: 0.15},
	{Key: "likes", Aliases: []string{"likes"}, Weight: 0.10},
	{Key: "comments", Aliases: []string{"comments"}, Weight: 0.05},
	{Key: "shares", Aliases: []string{"shares"}, Weight: 0.05},
	{Key: "avg_view_duration_sec", Aliases: []string{"avg_view_duration_sec"}, Weight: 0.20},
	{Key: "retention_curve_json", Aliases: []string{"retention_curve_json", "retention_curve"}, Weight: 0.20},
	{Key: "ctr", Aliases: []string{"ctr"}, Weight: 0.10},
	{Key: "impressions", Aliases: []string{"impressions"}, Weight: 0.15},
}

func MetricsCompleteness(metrics map[string]any) (float64, []string) {
	if metrics == nil {
		return 0, []string{}
	}

	score := 0.0
	fields := []string{}
	for _, item := range metricWeights {
		if hasAnyMetric(metrics, item.Aliases) {
			score += item.Weight
			fields = append(fields, item.Key)
		}
	}
	return score, fields
}

func HasRecognizedMetrics(metrics map[string]any) bool {
	_, fields := MetricsCompleteness(metrics)
	return len(fields) > 0
}

func hasAnyMetric(metrics map[string]any, aliases []string) bool {
	for _, alias := range aliases {
		if value, ok := metrics[alias]; ok && validMetricValue(value) {
			return true
		}
	}
	return false
}

func validMetricValue(value any) bool {
	if value == nil {
		return false
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) != ""
	case float32:
		return !math.IsNaN(float64(typed)) && !math.IsInf(float64(typed), 0)
	case float64:
		return !math.IsNaN(typed) && !math.IsInf(typed, 0)
	}

	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice:
		return reflected.Len() > 0
	default:
		return true
	}
}
