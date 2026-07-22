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
	var _ func(context.Context, []GoJobCreateInput) ([]JobDetailRow, error) = s.CreateGoJobs
	var _ func(context.Context, string) (JobDetailRow, error) = s.LoadGoJobForUpdate
	var _ func(context.Context, string, map[string]any) error = s.MarkGoJobPlanning
	var _ func(context.Context, string, map[string]any) (bool, error) = s.ClaimGoJobPlanning
	var _ func(context.Context, string) error = s.MarkGoJobRunning
	var _ func(context.Context, string) error = s.MarkGoJobWaitingWindow
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

func TestGoJobPlanningActionForAuthority(t *testing.T) {
	const jobID = "11111111-1111-4111-8111-111111111111"
	const otherJobID = "22222222-2222-4222-8222-222222222222"
	tests := []struct {
		name   string
		state  string
		guard  string
		status string
		expect goJobPlanningAction
	}{
		{name: "closed pending parks", state: "CLOSED", status: "PENDING", expect: goJobPlanningPark},
		{name: "closed running parks", state: "CLOSED", status: "RUNNING", expect: goJobPlanningPark},
		{name: "draining pending parks", state: "DRAINING", status: "PENDING", expect: goJobPlanningPark},
		{name: "draining waiting parks", state: "DRAINING", status: "WAITING_WINDOW", expect: goJobPlanningPark},
		{name: "draining validating claims", state: "DRAINING", status: "VALIDATING", expect: goJobPlanningClaim},
		{name: "draining planning claims", state: "DRAINING", status: "PLANNING", expect: goJobPlanningClaim},
		{name: "draining running claims", state: "DRAINING", status: "RUNNING", expect: goJobPlanningClaim},
		{name: "open mismatched guard parks", state: "OPEN", guard: otherJobID, status: "PENDING", expect: goJobPlanningPark},
		{name: "open exact guard claims", state: "OPEN", guard: jobID, status: "PENDING", expect: goJobPlanningClaim},
		{name: "open legacy guard claims", state: "OPEN", status: "PENDING", expect: goJobPlanningClaim},
		{name: "unknown state preserves claim", state: "PAUSED", guard: otherJobID, status: "PENDING", expect: goJobPlanningClaim},
		{name: "terminal success skips", state: "OPEN", guard: jobID, status: "SUCCEEDED", expect: goJobPlanningSkip},
		{name: "terminal failure skips", state: "OPEN", guard: jobID, status: "FAILED", expect: goJobPlanningSkip},
		{name: "terminal cancellation skips", state: "OPEN", guard: jobID, status: "CANCELLED", expect: goJobPlanningSkip},
		{name: "terminal partial failure skips", state: "OPEN", guard: jobID, status: "PARTIALLY_FAILED", expect: goJobPlanningSkip},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := goJobPlanningActionFor(tt.state, tt.guard, jobID, tt.status); got != tt.expect {
				t.Fatalf("action = %v; want %v", got, tt.expect)
			}
		})
	}
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
