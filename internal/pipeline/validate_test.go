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
