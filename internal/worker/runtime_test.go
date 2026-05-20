package worker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/store"
)

type fakeTaskStore struct {
	state        store.ExecutionState
	input        store.ArtifactRow
	artifacts    map[string]store.ArtifactRow
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

func (f *fakeTaskStore) GetArtifact(_ context.Context, id string) (store.ArtifactRow, error) {
	if f.artifacts != nil {
		artifact, ok := f.artifacts[id]
		if !ok {
			return store.ArtifactRow{}, errors.New("artifact not found")
		}
		return artifact, nil
	}
	return f.input, nil
}

func (f *fakeTaskStore) CreateIntermediateArtifact(_ context.Context, in store.CreateArtifactInput) (string, error) {
	f.createdInput = in
	return "00000000-0000-0000-0000-000000000777", nil
}

type fakeMediaHandler struct {
	seenInputs map[string]string
	seenOutput string
	seenConfig map[string]any
	metadata   map[string]any
}

func (h *fakeMediaHandler) NodeType() string { return "trim" }

func (h *fakeMediaHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	h.seenInputs = inputPaths
	h.seenOutput = outputPath
	h.seenConfig = config
	return h.metadata, os.WriteFile(outputPath, []byte("media"), 0o644)
}

func TestBuildInputMapResolvesNamedPortsAndCopiesMediaInfo(t *testing.T) {
	root := t.TempDir()
	videoPath := filepath.Join(root, "video.mp4")
	audioPath := filepath.Join(root, "audio.wav")
	if err := os.WriteFile(videoPath, []byte("video"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(audioPath, []byte("audio"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeFake := &fakeTaskStore{
		artifacts: map[string]store.ArtifactRow{
			"video-artifact": {
				ID:             "video-artifact",
				Filename:       "video.mp4",
				StorageBackend: "local",
				StoragePath:    videoPath,
				MediaInfo:      map[string]any{"width": 1920.0},
			},
			"audio-artifact": {
				ID:             "audio-artifact",
				Filename:       "audio.wav",
				StorageBackend: "local",
				StoragePath:    audioPath,
				MediaInfo:      map[string]any{"channels": 2.0},
			},
		},
	}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:     storeFake,
		Storage:   storage.LocalBackend{Root: root},
		LocalRoot: root,
	}, &fakeMediaHandler{})

	inputs, cleanup, err := handler.BuildInputMap(context.Background(), map[string]any{
		"video": "video-artifact",
		"audio": "audio-artifact",
	})
	defer cleanup()
	if err != nil {
		t.Fatal(err)
	}
	if inputs.Paths["video"] != videoPath || inputs.Paths["audio"] != audioPath {
		t.Fatalf("paths = %#v", inputs.Paths)
	}
	videoMeta := inputs.MediaInfo["video"].(map[string]any)
	audioMeta := inputs.MediaInfo["audio"].(map[string]any)
	videoMeta["width"] = 1280.0
	audioMeta["channels"] = 1.0
	if storeFake.artifacts["video-artifact"].MediaInfo.(map[string]any)["width"] != 1920.0 {
		t.Fatalf("video media info was not copied: %#v", storeFake.artifacts["video-artifact"].MediaInfo)
	}
	if storeFake.artifacts["audio-artifact"].MediaInfo.(map[string]any)["channels"] != 2.0 {
		t.Fatalf("audio media info was not copied: %#v", storeFake.artifacts["audio-artifact"].MediaInfo)
	}
}

type memoryStorage struct {
	data map[string][]byte
}

func (s memoryStorage) Read(_ context.Context, path string) ([]byte, error) {
	return s.data[path], nil
}

func (s memoryStorage) Save(context.Context, string, []byte) error {
	return nil
}

func (s memoryStorage) Exists(context.Context, string) (bool, error) {
	return false, nil
}

func (s memoryStorage) Delete(context.Context, string) error {
	return nil
}

func (s memoryStorage) LocalPath(string) (string, bool) {
	return "", false
}

func TestBuildInputMapCleanupRemovesDownloadedInputs(t *testing.T) {
	storeFake := &fakeTaskStore{
		artifacts: map[string]store.ArtifactRow{
			"remote-artifact": {
				ID:             "remote-artifact",
				Filename:       "remote.mp4",
				StorageBackend: "minio",
				StoragePath:    "bucket/remote.mp4",
			},
		},
	}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:   storeFake,
		Storage: memoryStorage{data: map[string][]byte{"bucket/remote.mp4": []byte("remote")}},
	}, &fakeMediaHandler{})

	inputs, cleanup, err := handler.BuildInputMap(context.Background(), map[string]any{"input": "remote-artifact"})
	if err != nil {
		t.Fatal(err)
	}
	inputPath := inputs.Paths["input"]
	if _, err := os.Stat(inputPath); err != nil {
		t.Fatalf("downloaded input was not written: %v", err)
	}
	cleanup()
	if _, err := os.Stat(inputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("downloaded input was not cleaned up: %v", err)
	}
}

func TestMediaTaskHandlerInjectsInputArtifactMetaIntoClonedConfig(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(inputPath, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	originalConfig := map[string]any{"duration": "1", "output_format": "mp4"}
	storeFake := &fakeTaskStore{
		state: store.ExecutionState{
			JobID:           "00000000-0000-0000-0000-000000000101",
			NodeExecutionID: "00000000-0000-0000-0000-000000000201",
			JobStatus:       contracts.JobStatusRunning,
			NodeStatus:      contracts.NodeStatusQueued,
		},
		artifacts: map[string]store.ArtifactRow{
			"input-artifact": {
				ID:             "input-artifact",
				Filename:       "input.mp4",
				StorageBackend: "local",
				StoragePath:    inputPath,
				MediaInfo:      map[string]any{"duration": 3.5},
			},
		},
	}
	media := &fakeMediaHandler{metadata: map[string]any{"duration": 1.0}}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:          storeFake,
		Storage:        storage.LocalBackend{Root: root},
		StorageBackend: "local",
		LocalRoot:      root,
		WorkerID:       "ffmpeg_go-worker@test:1",
	}, media)

	_, err := handler.Execute(context.Background(), TaskMessage{
		JobID:           "00000000-0000-0000-0000-000000000101",
		NodeExecutionID: "00000000-0000-0000-0000-000000000201",
		NodeType:        "trim",
		Config:          originalConfig,
		InputArtifacts:  map[string]any{"input": "input-artifact"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, mutated := originalConfig["_input_artifact_meta"]; mutated {
		t.Fatalf("task config was mutated: %#v", originalConfig)
	}
	metaByPort, ok := media.seenConfig["_input_artifact_meta"].(map[string]any)
	if !ok {
		t.Fatalf("missing input artifact meta in cloned config: %#v", media.seenConfig)
	}
	inputMeta := metaByPort["input"].(map[string]any)
	if inputMeta["duration"] != 3.5 {
		t.Fatalf("input meta = %#v", inputMeta)
	}
	if storeFake.createdInput.MediaInfo.(map[string]any)["duration"] != 1.0 {
		t.Fatalf("created media info = %#v", storeFake.createdInput.MediaInfo)
	}
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
	if media.seenInputs["input"] != inputPath {
		t.Fatalf("input path = %q", media.seenInputs["input"])
	}
}

type cancelAfterRunningStore struct {
	fakeTaskStore
	loads atomic.Int32
}

func (f *cancelAfterRunningStore) LoadExecutionState(ctx context.Context, nodeExecutionID string) (store.ExecutionState, error) {
	count := f.loads.Add(1)
	if count >= 2 {
		state := f.state
		state.NodeStatus = contracts.NodeStatusCancelled
		return state, nil
	}
	return f.state, nil
}

type blockingMediaHandler struct {
	cancelled chan struct{}
}

func (h *blockingMediaHandler) NodeType() string { return "trim" }

func (h *blockingMediaHandler) Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error) {
	<-ctx.Done()
	close(h.cancelled)
	return nil, ctx.Err()
}

func TestMediaTaskHandlerCancelsDuringExecutionWhenStateChanges(t *testing.T) {
	root := t.TempDir()
	inputPath := filepath.Join(root, "input.mp4")
	if err := os.WriteFile(inputPath, []byte("input"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeFake := &cancelAfterRunningStore{
		fakeTaskStore: fakeTaskStore{
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
		},
	}
	media := &blockingMediaHandler{cancelled: make(chan struct{})}
	handler := NewMediaTaskHandler(RuntimeEnv{
		Store:              storeFake,
		Storage:            storage.LocalBackend{Root: root},
		StorageBackend:     "local",
		LocalRoot:          root,
		WorkerID:           "ffmpeg_go-worker@test:1",
		CancelPollInterval: time.Millisecond,
	}, media)

	_, err := handler.Execute(context.Background(), TaskMessage{
		JobID:           "00000000-0000-0000-0000-000000000101",
		NodeExecutionID: "00000000-0000-0000-0000-000000000201",
		NodeType:        "trim",
		Config:          map[string]any{"duration": "1"},
		InputArtifacts:  map[string]any{"input": "00000000-0000-0000-0000-000000000301"},
	})
	if !errors.Is(err, ErrConfirmedCancellation) {
		t.Fatalf("err = %v; want ErrConfirmedCancellation", err)
	}
	select {
	case <-media.cancelled:
	case <-time.After(time.Second):
		t.Fatal("media handler context was not cancelled")
	}
}
