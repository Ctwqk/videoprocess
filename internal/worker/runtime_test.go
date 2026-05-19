package worker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/store"
)

type fakeTaskStore struct {
	state        store.ExecutionState
	input        store.ArtifactRow
	createdInput store.CreateArtifactInput
	runningNode  string
}

func (f *fakeTaskStore) LoadExecutionState(context.Context, string) (store.ExecutionState, error) {
	return f.state, nil
}

func (f *fakeTaskStore) MarkNodeRunning(_ context.Context, nodeExecutionID string, _ string) error {
	f.runningNode = nodeExecutionID
	return nil
}

func (f *fakeTaskStore) GetArtifact(context.Context, string) (store.ArtifactRow, error) {
	return f.input, nil
}

func (f *fakeTaskStore) CreateIntermediateArtifact(_ context.Context, in store.CreateArtifactInput) (string, error) {
	f.createdInput = in
	return "00000000-0000-0000-0000-000000000777", nil
}

type fakeMediaHandler struct {
	seenInput  string
	seenOutput string
}

func (h *fakeMediaHandler) NodeType() string { return "trim" }

func (h *fakeMediaHandler) Execute(ctx context.Context, inputPath string, outputPath string, config map[string]any) error {
	h.seenInput = inputPath
	h.seenOutput = outputPath
	return os.WriteFile(outputPath, []byte("media"), 0o644)
}

func TestMediaTaskHandlerCreatesArtifactResult(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(inputPath, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeFake := &fakeTaskStore{
		state: store.ExecutionState{
			JobID:           "00000000-0000-0000-0000-000000000101",
			NodeExecutionID: "00000000-0000-0000-0000-000000000201",
			JobStatus:       contracts.JobStatusRunning,
			NodeStatus:      contracts.NodeStatusQueued,
		},
		input: store.ArtifactRow{
			ID:             "00000000-0000-0000-0000-000000000301",
			Filename:       "input.mp4",
			StorageBackend: "local",
			StoragePath:    inputPath,
		},
	}
	media := &fakeMediaHandler{}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:          storeFake,
		Storage:        storage.LocalBackend{Root: root},
		StorageBackend: "local",
		LocalRoot:      root,
		WorkerID:       "ffmpeg_go-worker@test:1",
	}, media)

	result, err := handler.Execute(context.Background(), TaskMessage{
		JobID:           "00000000-0000-0000-0000-000000000101",
		NodeExecutionID: "00000000-0000-0000-0000-000000000201",
		NodeType:        "trim",
		Config:          map[string]any{"duration": "1", "output_format": "mp4"},
		InputArtifacts:  map[string]any{"input": "00000000-0000-0000-0000-000000000301"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.OutputArtifactID == "" {
		t.Fatal("OutputArtifactID must be populated")
	}
	if storeFake.runningNode != "00000000-0000-0000-0000-000000000201" {
		t.Fatalf("running node = %q", storeFake.runningNode)
	}
	if storeFake.createdInput.StorageBackend != "local" || storeFake.createdInput.StoragePath == "" {
		t.Fatalf("created artifact = %#v", storeFake.createdInput)
	}
	if media.seenInput != inputPath {
		t.Fatalf("input path = %q", media.seenInput)
	}
}
