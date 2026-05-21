package channelops

import "testing"

func TestRewardScoreRenormalizesAvailableComponents(t *testing.T) {
	metrics := map[string]any{
		"views":                 1000,
		"likes":                 50,
		"comments":              10,
		"avg_view_duration_sec": 18.0,
	}

	score, components := RewardScore(metrics, PublicationRewardContext{ChannelMedianViews: 500, StablePublication: true})

	if score <= 0 {
		t.Fatalf("expected positive reward score")
	}
	if components["views"] == nil || components["engagement_rate"] == nil {
		t.Fatalf("expected reward components to include views and engagement_rate: %#v", components)
	}
}

func TestSnapshotStageFromPayload(t *testing.T) {
	if got := SnapshotStageFromPayload(map[string]any{"snapshot_stage": "6h"}); got != "6h" {
		t.Fatalf("stage=%q", got)
	}
	if got := SnapshotStageFromPayload(map[string]any{}); got != "24h" {
		t.Fatalf("default stage=%q", got)
	}
}

func TestLearningRecommendationActions(t *testing.T) {
	if got := LearningRecommendation(3, 0.9)["action"]; got != "insufficient_data" {
		t.Fatalf("low sample action = %#v", got)
	}
	if got := LearningRecommendation(12, 0.7)["action"]; got != "promote_more" {
		t.Fatalf("high reward action = %#v", got)
	}
	if got := LearningRecommendation(12, 0.2)["action"]; got != "cool_down" {
		t.Fatalf("low reward action = %#v", got)
	}
}
