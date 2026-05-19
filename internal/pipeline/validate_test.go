package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

func TestValidateMatchesBasicGoldenFixture(t *testing.T) {
	var def contracts.PipelineDefinition
	raw, err := os.ReadFile(filepath.Join("..", "..", "backend", "tests", "golden", "go_migration", "pipeline_basic.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatal(err)
	}

	result := Validate(def)

	if !result.Valid {
		t.Fatalf("result.Valid = false, errors = %#v", result.Errors)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("errors = %#v", result.Errors)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("warnings = %#v", result.Warnings)
	}
}

func ptr(s string) *string { return &s }

func TestValidateFlagsMissingRequiredInput(t *testing.T) {
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			// A trim node with no inbound edge: its required `input` is missing.
			{ID: "trim_1", Type: "trim", Data: contracts.PipelineNodeData{Label: "Trim"}},
		},
	}
	result := Validate(def)
	if result.Valid {
		t.Fatal("expected validation to fail")
	}
	if !hasError(result, "missing_required_input") {
		t.Fatalf("expected missing_required_input, got %#v", result.Errors)
	}
}

func TestValidateFlagsMissingAssetOnSourceNode(t *testing.T) {
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			{ID: "src_1", Type: "source", Data: contracts.PipelineNodeData{Label: "Source"}},
		},
	}
	result := Validate(def)
	if !hasError(result, "missing_asset") {
		t.Fatalf("expected missing_asset, got %#v", result.Errors)
	}
}

func TestValidateAcceptsSourceWithAssetID(t *testing.T) {
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			{
				ID:   "src_1",
				Type: "source",
				Data: contracts.PipelineNodeData{Label: "Source", AssetID: ptr("00000000-0000-0000-0000-000000000001")},
			},
		},
	}
	result := Validate(def)
	if hasError(result, "missing_asset") {
		t.Fatalf("source with asset_id should not flag missing_asset: %#v", result.Errors)
	}
}

func TestValidateAcceptsSourceAssetInConfig(t *testing.T) {
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			{
				ID:   "src_1",
				Type: "source",
				Data: contracts.PipelineNodeData{
					Label:  "Source",
					Config: map[string]any{"asset_id": "00000000-0000-0000-0000-000000000001"},
				},
			},
		},
	}
	result := Validate(def)
	if hasError(result, "missing_asset") {
		t.Fatalf("config.asset_id binding should satisfy missing_asset: %#v", result.Errors)
	}
}

func TestValidateFlagsDuplicateInputPort(t *testing.T) {
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			{
				ID:   "src_a",
				Type: "source",
				Data: contracts.PipelineNodeData{AssetID: ptr("00000000-0000-0000-0000-000000000001")},
			},
			{
				ID:   "src_b",
				Type: "source",
				Data: contracts.PipelineNodeData{AssetID: ptr("00000000-0000-0000-0000-000000000002")},
			},
			{ID: "trim_1", Type: "trim", Data: contracts.PipelineNodeData{Label: "Trim"}},
		},
		Edges: []contracts.PipelineEdge{
			{ID: "e_a", Source: "src_a", Target: "trim_1", SourceHandle: "output", TargetHandle: "input"},
			{ID: "e_b", Source: "src_b", Target: "trim_1", SourceHandle: "output", TargetHandle: "input"},
		},
	}
	result := Validate(def)
	if !hasError(result, "duplicate_input_port") {
		t.Fatalf("expected duplicate_input_port, got %#v", result.Errors)
	}
}

func hasError(r contracts.ValidationResult, kind string) bool {
	for _, e := range r.Errors {
		if e.Type == kind {
			return true
		}
	}
	return false
}
