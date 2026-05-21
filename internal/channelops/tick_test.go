package channelops

import (
	"reflect"
	"testing"
)

func TestBuildCandidatesManualThenLaneDriven(t *testing.T) {
	laneID := "lane-1"
	account := PublishingAccountRow{ID: "acct-1", Enabled: true}
	manual := ManualSeedRow{
		ID:                  "seed-1",
		TopicLaneID:         &laneID,
		Prompt:              "manual prompt",
		TitleSeed:           "manual title",
		SourcePolicy:        "remix_with_review",
		SourcePlatformsJSON: []string{"bilibili"},
	}
	lane := TopicLaneRow{ID: laneID, Name: "lane", Enabled: true, MaxPostsPerDay: 3}
	shortFormat := LaneFormatRow{
		ID:                  "fmt-short",
		TopicLaneID:         laneID,
		FormatKey:           "short",
		Enabled:             true,
		SourcePlatformsJSON: []string{"xiaohongshu"},
	}
	longFormat := LaneFormatRow{
		ID:                  "fmt-long",
		TopicLaneID:         laneID,
		FormatKey:           "long",
		Enabled:             true,
		SourcePlatformsJSON: []string{"douyin"},
	}

	candidates := BuildTickCandidates(
		ChannelProfileRow{ID: "ch", DefaultAspectRatio: "9:16"},
		[]TopicLaneRow{lane},
		[]PublishingAccountRow{account},
		[]ManualSeedRow{manual},
		map[string][]LaneFormatRow{laneID: {shortFormat, longFormat}},
		"2026-05-21-18",
	)

	if len(candidates) != 3 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	if candidates[0].Source != SourceManualSeed {
		t.Fatalf("first source = %s", candidates[0].Source)
	}
	if candidates[1].Source != SourceLaneSeed || candidates[2].Source != SourceLaneSeed {
		t.Fatalf("lane-driven sources = %s, %s", candidates[1].Source, candidates[2].Source)
	}
	if !reflect.DeepEqual(candidates[0].SourcePlatformsJSON, []string{"bilibili"}) {
		t.Fatalf("manual source platforms = %#v", candidates[0].SourcePlatformsJSON)
	}
	if !reflect.DeepEqual(candidates[1].SourcePlatformsJSON, []string{"xiaohongshu"}) {
		t.Fatalf("first lane source platforms = %#v", candidates[1].SourcePlatformsJSON)
	}
	if !reflect.DeepEqual(candidates[2].SourcePlatformsJSON, []string{"douyin"}) {
		t.Fatalf("second lane source platforms = %#v", candidates[2].SourcePlatformsJSON)
	}
}

func TestBuildCandidatesLimitsLaneFormatToOneCandidatePerTick(t *testing.T) {
	laneID := "lane-1"
	account := PublishingAccountRow{ID: "acct-1", Enabled: true}
	lane := TopicLaneRow{ID: laneID, Name: "lane", Enabled: true, MaxPostsPerDay: 4}
	format := LaneFormatRow{ID: "fmt-1", TopicLaneID: laneID, FormatKey: "short", Enabled: true}

	candidates := BuildTickCandidates(
		ChannelProfileRow{ID: "ch"},
		[]TopicLaneRow{lane},
		[]PublishingAccountRow{account},
		nil,
		map[string][]LaneFormatRow{laneID: {format}},
		"2026-05-21-18",
	)

	if len(candidates) != 1 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	if candidates[0].LaneFormat == nil || candidates[0].LaneFormat.ID != "fmt-1" {
		t.Fatalf("unexpected lane format: %#v", candidates[0].LaneFormat)
	}
}

func TestTrendYouTubeIsNotManualOverride(t *testing.T) {
	seed := ManualSeedRow{ID: "seed-1", SourcePolicy: SourceTrendYT}
	candidate := CandidateFromManualSeed(seed, nil, nil, nil, "bucket")
	if candidate.SourceKind != SourceTrendYT {
		t.Fatalf("SourceKind = %s", candidate.SourceKind)
	}
	if candidate.ManualMaterialOverride {
		t.Fatal("trend_youtube should not get manual material override")
	}
}

func TestDryRunAuditDoesNotCreateTasks(t *testing.T) {
	result := TickResult{DryRun: true, Accepted: []TickCandidate{{CandidateID: "c1"}}}
	if result.TasksToCreate() != 0 {
		t.Fatalf("TasksToCreate = %d", result.TasksToCreate())
	}
}
