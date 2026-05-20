package orchestrator

import (
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func TestClassifyGoEligibilityTask2Cases(t *testing.T) {
	t.Skip("Task 2 user-owned implementation: remove this skip while implementing eligibility")

	tests := []struct {
		name       string
		def        contracts.PipelineDefinition
		wantOK     bool
		wantReason string
	}{
		{
			name:   "pure first-wave ffmpeg graph is eligible",
			def:    task2EligiblePipeline("asset-1"),
			wantOK: true,
		},
		{
			name:       "unsupported node type is rejected",
			def:        task2PipelineWithNode("smart_trim", "asset-1"),
			wantOK:     false,
			wantReason: `node type "smart_trim" remains Python-owned`,
		},
		{
			name:       "source without asset is rejected",
			def:        task2EligiblePipeline(""),
			wantOK:     false,
			wantReason: "source node",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyGoEligibility(tc.def)
			if got.Eligible != tc.wantOK {
				t.Fatalf("Eligible = %v; want %v (reason=%q)", got.Eligible, tc.wantOK, got.Reason)
			}
			if tc.wantReason != "" && got.Reason != tc.wantReason {
				t.Fatalf("Reason = %q; want %q", got.Reason, tc.wantReason)
			}
		})
	}
}

func task2EligiblePipeline(assetID string) contracts.PipelineDefinition {
	nodes := []contracts.PipelineNode{
		task2SourceNode(assetID),
		{
			ID:   "trim_1",
			Type: "trim",
			Data: contracts.PipelineNodeData{
				Label:  "Trim",
				Config: map[string]any{"start_time": "0", "duration": "1"},
			},
		},
		{
			ID:   "export_1",
			Type: "export",
			Data: contracts.PipelineNodeData{
				Label:  "Export",
				Config: map[string]any{"output_dir": "/tmp", "filename": "out.mp4"},
			},
		},
	}
	return contracts.PipelineDefinition{
		Nodes: nodes,
		Edges: []contracts.PipelineEdge{
			{ID: "e1", Source: "source_1", SourceHandle: "output", Target: "trim_1", TargetHandle: "input"},
			{ID: "e2", Source: "trim_1", SourceHandle: "output", Target: "export_1", TargetHandle: "input"},
		},
		Viewport: map[string]float64{},
	}
}

func task2PipelineWithNode(nodeType string, assetID string) contracts.PipelineDefinition {
	def := task2EligiblePipeline(assetID)
	def.Nodes[1].Type = nodeType
	return def
}

func task2SourceNode(assetID string) contracts.PipelineNode {
	node := contracts.PipelineNode{
		ID:   "source_1",
		Type: "source",
		Data: contracts.PipelineNodeData{
			Label:  "Source",
			Config: map[string]any{},
		},
	}
	if assetID != "" {
		node.Data.AssetID = &assetID
		node.Data.Config["asset_id"] = assetID
	}
	return node
}
