package channelops

import (
	"os"
	"strings"
	"testing"
)

func TestFailureCategoryForContextPrefersHandlerContext(t *testing.T) {
	tests := []struct {
		context string
		reason  string
		want    string
	}{
		{context: "plan_task", reason: "planner rejected schema", want: FailurePlanning},
		{context: "execute_task", reason: "render worker failed", want: FailureRender},
		{context: "publish_task", reason: "youtube upload quota exhausted", want: FailureQuota},
		{context: "collect_metrics", reason: "analytics unavailable", want: FailureMetrics},
		{context: "observe_job", reason: "video_id missing", want: FailureUpload},
	}
	for _, tt := range tests {
		if got := FailureCategoryFor(tt.context, tt.reason); got != tt.want {
			t.Fatalf("FailureCategoryFor(%q, %q)=%q want %q", tt.context, tt.reason, got, tt.want)
		}
	}
}

func TestPublicationStateTransitionsPersistFailureCategory(t *testing.T) {
	source, err := os.ReadFile("store_publications.go")
	if err != nil {
		t.Fatalf("read store_publications.go: %v", err)
	}
	text := string(source)
	for _, want := range []string{
		"failure_category = $7::text",
		"FailureYouTubeStatus",
		"FailureMetrics",
		`TaskScheduled, "", "", "promote_publication", "", now`,
		`TaskMeasured, "", "", "collect_metrics", "", now`,
		`TaskUploadedPrivate, "", "", "publish_task", "", now`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("store_publications.go missing %q", want)
		}
	}
}
