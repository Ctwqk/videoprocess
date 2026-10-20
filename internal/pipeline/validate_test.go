package pipeline

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
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

// TestValidateMatchesSharedPythonCases runs the cases that
// backend/tests/test_go_contract_fixtures.py checks against dag.py.
func TestValidateMatchesSharedPythonCases(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "backend", "tests", "golden", "go_migration", "pipeline_validation_cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	type expectation struct {
		Valid        bool     `json:"valid"`
		ErrorTypes   []string `json:"error_types"`
		WarningTypes []string `json:"warning_types"`
	}
	var fixture struct {
		Cases []struct {
			Name       string                       `json:"name"`
			Definition contracts.PipelineDefinition `json:"definition"`
			Expected   expectation                  `json:"expected"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) == 0 {
		t.Fatal("no shared validation cases")
	}
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			result := Validate(tc.Definition)
			observed := expectation{Valid: result.Valid, ErrorTypes: []string{}, WarningTypes: []string{}}
			for _, e := range result.Errors {
				observed.ErrorTypes = append(observed.ErrorTypes, e.Type)
			}
			for _, w := range result.Warnings {
				observed.WarningTypes = append(observed.WarningTypes, w.Type)
			}
			sort.Strings(observed.ErrorTypes)
			sort.Strings(observed.WarningTypes)
			if !reflect.DeepEqual(observed, tc.Expected) {
				t.Fatalf("observed %+v, want %+v (errors %#v)", observed, tc.Expected, result.Errors)
			}
		})
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

func TestValidateTopLevelSourceAssetStillRequiresConfigParam(t *testing.T) {
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
		t.Fatalf("top-level asset_id should not flag missing_asset: %#v", result.Errors)
	}
	if !hasError(result, "invalid_param") {
		t.Fatalf("top-level asset_id should still require config.asset_id param: %#v", result.Errors)
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

func TestValidateRejectsUnsupportedPythonOwnedGraph(t *testing.T) {
	def := contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			{
				ID:   "search_1",
				Type: "youtube_search",
				Data: contracts.PipelineNodeData{Label: "YouTube Search"},
			},
		},
	}

	result := Validate(def)

	if result.Valid {
		t.Fatal("expected validation to fail")
	}
	if len(result.Errors) != 1 {
		t.Fatalf("expected exactly one error, got %#v", result.Errors)
	}
	err := result.Errors[0]
	if err.Type != "unsupported_go_validation" {
		t.Fatalf("error type = %q", err.Type)
	}
	if err.NodeID == nil || *err.NodeID != "search_1" {
		t.Fatalf("node_id = %#v", err.NodeID)
	}
	expected := "Go validator does not own validation for node type 'youtube_search'; route this graph to Python"
	if err.Message != expected {
		t.Fatalf("message = %q", err.Message)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("warnings = %#v", result.Warnings)
	}
}

func TestValidateAcceptsFirstWaveFFmpegGraph(t *testing.T) {
	def := firstWaveFFmpegGraph()

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

func firstWaveFFmpegGraph() contracts.PipelineDefinition {
	return contracts.PipelineDefinition{
		Nodes: []contracts.PipelineNode{
			{
				ID:   "source_1",
				Type: "source",
				Data: contracts.PipelineNodeData{
					Label:   "Source",
					AssetID: ptr("00000000-0000-0000-0000-000000000001"),
					Config:  map[string]any{"asset_id": "00000000-0000-0000-0000-000000000001"},
				},
			},
			{ID: "crop_1", Type: "vertical_crop", Data: contracts.PipelineNodeData{Label: "Vertical Crop"}},
			{ID: "title_1", Type: "title_overlay", Data: contracts.PipelineNodeData{Label: "Title Overlay"}},
			{ID: "export_1", Type: "export", Data: contracts.PipelineNodeData{Label: "Export"}},
		},
		Edges: []contracts.PipelineEdge{
			{ID: "edge_1", Source: "source_1", Target: "crop_1", SourceHandle: "output", TargetHandle: "input"},
			{ID: "edge_2", Source: "crop_1", Target: "title_1", SourceHandle: "output", TargetHandle: "input"},
			{ID: "edge_3", Source: "title_1", Target: "export_1", SourceHandle: "output", TargetHandle: "input"},
		},
		Viewport: map[string]float64{"x": 0, "y": 0, "zoom": 1},
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
