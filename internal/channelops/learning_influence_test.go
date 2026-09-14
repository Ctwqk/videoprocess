package channelops

import (
	"context"
	"reflect"
	"testing"
)

func TestLearningStateDoesNotAffectCandidateSelection(t *testing.T) {
	selectedIDs := func(firstLearning, secondLearning map[string]any) []string {
		candidates := []TickCandidate{
			{CandidateID: "first", LearningContextJSON: firstLearning},
			{CandidateID: "second", LearningContextJSON: secondLearning},
		}
		evaluated, alerts, err := evaluateTickCandidatePolicy(
			context.Background(), ChannelProfileRow{}, candidates, HandlerService{},
		)
		if err != nil {
			t.Fatalf("evaluate candidate policy: %v", err)
		}
		if len(alerts) != 0 {
			t.Fatalf("alerts = %#v, want none", alerts)
		}
		accepted, rejected := acceptedRejected(evaluated)
		if len(rejected) != 0 {
			t.Fatalf("rejected = %#v, want none", rejected)
		}
		ids := make([]string, 0, len(accepted))
		for _, candidate := range accepted {
			ids = append(ids, candidate.CandidateID)
		}
		return ids
	}

	low := map[string]any{"recommendation": "deprioritize", "reward": -1.0}
	high := map[string]any{"recommendation": "prioritize", "reward": 1.0}
	want := []string{"first", "second"}
	if got := selectedIDs(low, high); !reflect.DeepEqual(got, want) {
		t.Fatalf("low/high learning selection = %#v, want %#v", got, want)
	}
	if got := selectedIDs(high, low); !reflect.DeepEqual(got, want) {
		t.Fatalf("high/low learning selection = %#v, want %#v", got, want)
	}
}
