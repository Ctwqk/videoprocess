package orchestrator

import (
	"reflect"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/store"
)

func TestJobViewFromStoreRowConvertsEngineFields(t *testing.T) {
	outputID := "artifact-1"
	workerID := "ffmpeg_go-worker@worker-a:123"
	row := store.JobDetailRow{
		JobRow: store.JobRow{
			ID:                "job-1",
			Status:            "RUNNING",
			OrchestratorOwner: "go",
		},
		PipelineSnapshot: contracts.PipelineDefinition{
			Nodes: []contracts.PipelineNode{{ID: "source", Type: "source"}},
		},
		ExecutionPlan: map[string]any{
			"topo_order": []any{"source"},
			"dependencies": map[string]any{
				"source": []any{},
			},
		},
		NodeExecutions: []store.NodeExecutionRow{{
			ID:               "node-exec-1",
			NodeID:           "source",
			NodeType:         "source",
			NodeLabel:        "Source",
			Status:           "SUCCEEDED",
			RetryCount:       1,
			NodeConfig:       map[string]any{"asset_id": "asset-1"},
			OutputArtifactID: &outputID,
			InputArtifactIDs: []string{"input-1"},
			WorkerID:         &workerID,
		}},
	}

	got, err := JobViewFromStoreRow(row)
	if err != nil {
		t.Fatal(err)
	}

	if got.ID != "job-1" || got.Status != "RUNNING" || got.OrchestratorOwner != "go" {
		t.Fatalf("job fields = %#v", got)
	}
	if len(got.PipelineSnapshot.Nodes) != 1 || got.PipelineSnapshot.Nodes[0].ID != "source" {
		t.Fatalf("pipeline snapshot = %#v", got.PipelineSnapshot)
	}
	if !reflect.DeepEqual(got.ExecutionPlan["dependencies"], map[string][]string{"source": []string{}}) {
		t.Fatalf("dependencies = %#v", got.ExecutionPlan["dependencies"])
	}
	if len(got.Nodes) != 1 {
		t.Fatalf("nodes len = %d; want 1", len(got.Nodes))
	}
	node := got.Nodes[0]
	if node.OutputArtifactID != outputID {
		t.Fatalf("output artifact = %q; want %q", node.OutputArtifactID, outputID)
	}
	if node.WorkerID != workerID {
		t.Fatalf("worker id = %q; want %q", node.WorkerID, workerID)
	}
	if node.RetryCount != 1 {
		t.Fatalf("retry count = %d; want 1", node.RetryCount)
	}
	if node.NodeConfig["asset_id"] != "asset-1" {
		t.Fatalf("node config = %#v", node.NodeConfig)
	}
}
