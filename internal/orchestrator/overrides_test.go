package orchestrator

import (
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func TestApplyInputOverridesTask2Cases(t *testing.T) {
	tests := []struct {
		name         string
		overrides    map[string]any
		wantSourceID string
		wantDuration string
		wantCRF      float64
	}{
		{
			name:         "top-level asset_id binds source nodes",
			overrides:    map[string]any{"asset_id": "asset-top"},
			wantSourceID: "asset-top",
		},
		{
			name:         "dotted node field overrides config",
			overrides:    map[string]any{"trim_1.duration": "2"},
			wantSourceID: "asset-original",
			wantDuration: "2",
		},
		{
			name:         "nested node map overrides config",
			overrides:    map[string]any{"transcode_1": map[string]any{"crf": float64(23)}},
			wantSourceID: "asset-original",
			wantCRF:      23,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ApplyInputOverrides(task2OverridePipeline(), tc.overrides)
			source := got.Nodes[0]
			if source.Data.AssetID == nil || *source.Data.AssetID != tc.wantSourceID {
				t.Fatalf("source asset_id = %v; want %q", source.Data.AssetID, tc.wantSourceID)
			}
			if configID, _ := source.Data.Config["asset_id"].(string); configID != tc.wantSourceID {
				t.Fatalf("source config asset_id = %q; want %q", configID, tc.wantSourceID)
			}
			if tc.wantDuration != "" {
				if duration, _ := got.Nodes[1].Data.Config["duration"].(string); duration != tc.wantDuration {
					t.Fatalf("trim duration = %q; want %q", duration, tc.wantDuration)
				}
			}
			if tc.wantCRF != 0 {
				if crf, _ := got.Nodes[2].Data.Config["crf"].(float64); crf != tc.wantCRF {
					t.Fatalf("transcode crf = %v; want %v", got.Nodes[2].Data.Config["crf"], tc.wantCRF)
				}
			}
		})
	}
}

func TestApplyInputOverridesDoesNotMutateOriginal(t *testing.T) {
	original := task2OverridePipeline()
	_ = ApplyInputOverrides(original, map[string]any{"asset_id": "asset-new", "trim_1.duration": "2"})

	if original.Nodes[0].Data.AssetID == nil || *original.Nodes[0].Data.AssetID != "asset-original" {
		t.Fatalf("original source asset_id mutated: %#v", original.Nodes[0].Data.AssetID)
	}
	if duration, _ := original.Nodes[1].Data.Config["duration"].(string); duration != "1" {
		t.Fatalf("original trim duration mutated: %q", duration)
	}
}

func TestApplyInputOverridesTopLevelAssetOnlyAppliesToFirstSource(t *testing.T) {
	got := ApplyInputOverrides(task2TwoSourcePipeline(), map[string]any{
		"asset_id":          "asset-top",
		"source_2.asset_id": "asset-second",
	})

	if got.Nodes[0].Data.AssetID == nil || *got.Nodes[0].Data.AssetID != "asset-top" {
		t.Fatalf("source_1 asset_id = %v; want asset-top", got.Nodes[0].Data.AssetID)
	}
	if got.Nodes[1].Data.AssetID == nil || *got.Nodes[1].Data.AssetID != "asset-second" {
		t.Fatalf("source_2 asset_id = %v; want asset-second", got.Nodes[1].Data.AssetID)
	}
}

func TestApplyInputOverridesNodeIDValueSetsAssetID(t *testing.T) {
	got := ApplyInputOverrides(task2OverridePipeline(), map[string]any{"source_1": "asset-node"})

	if got.Nodes[0].Data.AssetID == nil || *got.Nodes[0].Data.AssetID != "asset-node" {
		t.Fatalf("source_1 asset_id = %v; want asset-node", got.Nodes[0].Data.AssetID)
	}
	if configID, _ := got.Nodes[0].Data.Config["asset_id"].(string); configID != "asset-node" {
		t.Fatalf("source_1 config asset_id = %q; want asset-node", configID)
	}
}

func task2OverridePipeline() contracts.PipelineDefinition {
	def := task2EligiblePipeline("asset-original")
	def.Nodes = append(def.Nodes[:2], contracts.PipelineNode{
		ID:   "transcode_1",
		Type: "transcode",
		Data: contracts.PipelineNodeData{
			Label:  "Transcode",
			Config: map[string]any{"crf": float64(20)},
		},
	}, def.Nodes[2])
	def.Edges = []contracts.PipelineEdge{
		{ID: "e1", Source: "source_1", SourceHandle: "output", Target: "trim_1", TargetHandle: "input"},
		{ID: "e2", Source: "trim_1", SourceHandle: "output", Target: "transcode_1", TargetHandle: "input"},
		{ID: "e3", Source: "transcode_1", SourceHandle: "output", Target: "export_1", TargetHandle: "input"},
	}
	return def
}

func task2TwoSourcePipeline() contracts.PipelineDefinition {
	def := task2OverridePipeline()
	def.Nodes = append([]contracts.PipelineNode{
		task2SourceNode("asset-original"),
		task2SourceNode("asset-original-2"),
	}, def.Nodes[1:]...)
	def.Nodes[1].ID = "source_2"
	def.Edges = []contracts.PipelineEdge{
		{ID: "e1", Source: "source_1", SourceHandle: "output", Target: "trim_1", TargetHandle: "input"},
		{ID: "e2", Source: "trim_1", SourceHandle: "output", Target: "transcode_1", TargetHandle: "input"},
		{ID: "e3", Source: "transcode_1", SourceHandle: "output", Target: "export_1", TargetHandle: "input"},
	}
	return def
}
