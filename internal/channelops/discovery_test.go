package channelops

import "testing"

func TestBuildTickCandidatesIncludesDiscoverySignals(t *testing.T) {
	channel := ChannelProfileRow{ID: "channel", DefaultAspectRatio: "9:16"}
	lane := TopicLaneRow{ID: "lane", Name: "AI", Enabled: true, MaxPostsPerDay: 3}
	format := LaneFormatRow{
		ID:                  "format",
		TopicLaneID:         "lane",
		FormatKey:           "shorts",
		Enabled:             true,
		SourcePlatformsJSON: []string{"youtube"},
	}
	account := PublishingAccountRow{ID: "account", Enabled: true}
	signal := DiscoverySignalRow{
		ID:               "signal",
		ChannelProfileID: "channel",
		TopicLaneID:      ptrString("lane"),
		Source:           "youtube_search",
		SourceExternalID: "yt-1",
		Title:            "Trend",
		Summary:          "A useful trend",
	}

	candidates := BuildTickCandidates(
		channel,
		[]TopicLaneRow{lane},
		[]PublishingAccountRow{account},
		nil,
		[]DiscoverySignalRow{signal},
		map[string][]LaneFormatRow{"lane": []LaneFormatRow{format}},
		"bucket",
	)

	found := false
	for _, candidate := range candidates {
		if candidate.SourceKind != SourceTrendYT {
			continue
		}
		found = true
		if candidate.ManualMaterialOverride {
			t.Fatalf("trend discovery candidate received manual override")
		}
		if candidate.DiscoverySignal == nil || candidate.DiscoverySignal.ID != "signal" {
			t.Fatalf("missing discovery signal on candidate: %#v", candidate.DiscoverySignal)
		}
		if candidate.Source != SourceTrendYT {
			t.Fatalf("candidate source = %q, want %q", candidate.Source, SourceTrendYT)
		}
	}
	if !found {
		t.Fatalf("expected trend_youtube candidate")
	}
}

func ptrString(value string) *string { return &value }
