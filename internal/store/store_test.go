package store

import (
	"encoding/json"
	"testing"
)

func TestMimeForExtension(t *testing.T) {
	cases := map[string]string{
		".mp4": "video/mp4",
		".mkv": "video/x-matroska",
		".wav": "audio/wav",
		".srt": "application/x-subrip",
		".bin": "video/mp4",
	}
	for ext, want := range cases {
		if got := GuessMime(ext); got != want {
			t.Fatalf("GuessMime(%q) = %q; want %q", ext, got, want)
		}
	}
}

func TestDetailRowsExposePythonCompatibleJSONKeys(t *testing.T) {
	pipeline := PipelineRow{ID: "p1", TemplateTags: nil}
	asset := AssetRow{ID: "a1"}
	artifact := ArtifactDetailRow{ID: "art1", Kind: "INTERMEDIATE"}
	job := JobDetailRow{JobRow: JobRow{ID: "j1"}, NodeExecutions: []NodeExecutionRow{}}

	assertJSONKey := func(name string, value any, key string) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("%s marshal: %v", name, err)
		}
		var payload map[string]any
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Fatalf("%s unmarshal: %v", name, err)
		}
		if _, ok := payload[key]; !ok {
			t.Fatalf("%s missing json key %q in %s", name, key, string(data))
		}
	}

	assertJSONKey("pipeline", pipeline, "template_tags")
	assertJSONKey("asset", asset, "original_name")
	assertJSONKey("artifact", artifact, "node_execution_id")
	assertJSONKey("job", job, "node_executions")
}
