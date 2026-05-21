package channelops

import (
	"os"
	"strings"
	"testing"
)

func TestLearningStateDoesNotAffectCandidateSelection(t *testing.T) {
	source, err := os.ReadFile("tick.go")
	if err != nil {
		t.Fatalf("read tick.go: %v", err)
	}
	text := string(source)
	if strings.Contains(text, "LearningState") && strings.Contains(text, "sort.Slice") {
		t.Fatalf("tick.go must not sort candidates by LearningState in Phase D")
	}
}
