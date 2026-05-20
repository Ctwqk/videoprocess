package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func TestGoNodeConfigInjectsSourceAssetIDWithoutMutatingOriginal(t *testing.T) {
	assetID := "asset-123"
	original := map[string]any{"preset": "copy"}
	node := contracts.PipelineNode{
		Type: "source",
		Data: contracts.PipelineNodeData{
			Config:  original,
			AssetID: &assetID,
		},
	}

	got := goNodeConfig(node)

	if got["asset_id"] != assetID {
		t.Fatalf("asset_id = %v; want %q", got["asset_id"], assetID)
	}
	if _, ok := original["asset_id"]; ok {
		t.Fatalf("goNodeConfig mutated original config: %#v", original)
	}
	if got["preset"] != "copy" {
		t.Fatalf("preset = %v; want copy", got["preset"])
	}
}

func TestGoNodeConfigPreservesExplicitAssetID(t *testing.T) {
	assetID := "asset-123"
	node := contracts.PipelineNode{
		Type: "source",
		Data: contracts.PipelineNodeData{
			Config:  map[string]any{"asset_id": "explicit"},
			AssetID: &assetID,
		},
	}

	got := goNodeConfig(node)

	if got["asset_id"] != "explicit" {
		t.Fatalf("asset_id = %v; want explicit", got["asset_id"])
	}
}

func TestGoSubmittedByDefaultsToSystem(t *testing.T) {
	if got := goSubmittedBy(""); got != "system" {
		t.Fatalf("goSubmittedBy(\"\") = %q; want system", got)
	}
	if got := goSubmittedBy("alice"); got != "alice" {
		t.Fatalf("goSubmittedBy(\"alice\") = %q; want alice", got)
	}
}

func TestFinalArtifactNodeSetHandlesEmptySlice(t *testing.T) {
	got := finalArtifactNodeSet(nil)
	if len(got) != 0 {
		t.Fatalf("finalArtifactNodeSet(nil) len = %d; want 0", len(got))
	}

	got = finalArtifactNodeSet([]string{})
	if len(got) != 0 {
		t.Fatalf("finalArtifactNodeSet(empty) len = %d; want 0", len(got))
	}
}

func TestFinalArtifactNodeListSkipsEmptyAndDeduplicates(t *testing.T) {
	got := finalArtifactNodeList([]string{"tail", "", "main", "tail"})
	want := []string{"tail", "main"}
	if len(got) != len(want) {
		t.Fatalf("finalArtifactNodeList len = %d; want %d (%#v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("finalArtifactNodeList[%d] = %q; want %q (%#v)", i, got[i], want[i], got)
		}
	}
}

func TestGoJobStoreMethodSignatures(t *testing.T) {
	var s *Store
	var _ func(context.Context, GoJobCreateInput) (JobDetailRow, error) = s.CreateGoJob
	var _ func(context.Context, string) (JobDetailRow, error) = s.LoadGoJobForUpdate
	var _ func(context.Context, string, map[string]any) error = s.MarkGoJobPlanning
	var _ func(context.Context, string) error = s.MarkGoJobRunning
	var _ func(context.Context, string, []string) (bool, error) = s.MarkGoNodeQueued
	var _ func(context.Context, string) error = s.ReleaseGoNodeQueueClaim
	var _ func(context.Context, string, string, string) error = s.MarkGoNodeSucceeded
	var _ func(context.Context, string, string, string) error = s.MarkGoNodeFailed
	var _ func(context.Context, string, string) error = s.IncrementGoNodeRetry
	var _ func(context.Context, string, []string) error = s.SkipGoDownstreamNodes
	var _ func(context.Context, string, string, *string, []string) error = s.FinalizeGoJob
	var _ func(context.Context) ([]JobDetailRow, error) = s.ListRecoverableGoJobs
	var _ func(context.Context, string, time.Time) error = s.ResetStaleGoNodes
	var _ func(context.Context, string, string, string) (string, error) = s.CreateSourceArtifact
}

func TestNodeExecutionRowInternalFieldsAreJSONIgnored(t *testing.T) {
	raw, err := json.Marshal(NodeExecutionRow{
		ID:         "node-exec",
		NodeConfig: map[string]any{"preset": "copy"},
		RetryCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	if strings.Contains(body, "node_config") {
		t.Fatalf("NodeConfig leaked into JSON: %s", body)
	}
	if strings.Contains(body, "retry_count") {
		t.Fatalf("RetryCount leaked into JSON: %s", body)
	}
}
