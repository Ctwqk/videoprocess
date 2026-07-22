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

func SnapshotStageFromPayload(payload map[string]any) string {
	stage := strings.TrimSpace(firstString(payload, "snapshot_stage", "stage", "window"))
	switch stage {
	case "immediate", "1h", "6h", "24h", "72h", "7d":
		return stage
	default:
		return "24h"
	}
}

type PublicationRewardContext struct {
	ChannelMedianViews float64
	StablePublication  bool
}

func RewardScore(metrics map[string]any, context PublicationRewardContext) (float64, map[string]any) {
	components := map[string]any{}
	totalWeight := 0.0
	weighted := 0.0
	add := func(name string, weight float64, value float64) {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return
		}
		if value < 0 {
			value = 0
		}
		if value > 1 {
			value = 1
		}
		components[name] = value
		totalWeight += weight
		weighted += weight * value
	}

	views := floatFromAny(metrics["views"])
	if views > 0 {
		median := context.ChannelMedianViews
		if median <= 0 {
			median = views
		}
		add("views", 0.20, views/(median*2))
	}
	likes := floatFromAny(metrics["likes"])
	comments := floatFromAny(metrics["comments"])
	shares := floatFromAny(metrics["shares"])
	if views > 0 && likes+comments+shares > 0 {
		add("engagement_rate", 0.20, ((likes+comments*2+shares*3)/views)*10)
	}
	if ctr := floatFromAny(metrics["ctr"]); ctr > 0 {
		add("ctr", 0.20, ctr/0.12)
	}
	if duration := floatFromAny(metrics["avg_view_duration_sec"]); duration > 0 {
		add("avg_view_duration_sec", 0.30, duration/45)
	}
	if context.StablePublication {
		add("publish_stability", 0.10, 1)
	}
	if totalWeight == 0 {
		return 0, components
	}
	components["total_weight"] = totalWeight
	return weighted / totalWeight, components
}

func floatFromAny(value any) float64 {
	parsed, ok := parseMetricFloat(value)
	if !ok {
		return 0
	}
	return parsed
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
