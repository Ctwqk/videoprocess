package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/Ctwqk/videoprocess/internal/storage"
	"github.com/Ctwqk/videoprocess/internal/store"
)

type TaskStore interface {
	LoadExecutionState(ctx context.Context, nodeExecutionID string) (store.ExecutionState, error)
	MarkNodeRunning(ctx context.Context, nodeExecutionID string, workerID string) error
	GetArtifact(ctx context.Context, id string) (store.ArtifactRow, error)
	CreateIntermediateArtifact(ctx context.Context, in store.CreateArtifactInput) (string, error)
}

type RuntimeEnv struct {
	Store              TaskStore
	Storage            storage.Backend
	StorageBackend     string
	LocalRoot          string
	WorkerID           string
	Logger             *slog.Logger
	CancelPollInterval time.Duration
}

type MediaHandler interface {
	NodeType() string
	Execute(ctx context.Context, inputPaths map[string]string, outputPath string, config map[string]any) (map[string]any, error)
}

type MediaTaskHandler struct {
	env   RuntimeEnv
	media MediaHandler
}

func NewMediaTaskHandler(env RuntimeEnv, media MediaHandler) MediaTaskHandler {
	return MediaTaskHandler{env: env, media: media}
}

func (h MediaTaskHandler) NodeType() string {
	return h.media.NodeType()
}

func (h MediaTaskHandler) Execute(ctx context.Context, task TaskMessage) (NodeResult, error) {
	if h.env.Store == nil {
		return NodeResult{}, errors.New("worker store is required")
	}
	state, err := h.env.Store.LoadExecutionState(ctx, task.NodeExecutionID)
	if err != nil {
		return NodeResult{}, fmt.Errorf("load execution state: %w", err)
	}
	if state.JobStatus == contracts.JobStatusCancelled || state.NodeStatus == contracts.NodeStatusCancelled {
		return NodeResult{}, ErrConfirmedCancellation
	}
	if err := h.env.Store.MarkNodeRunning(ctx, task.NodeExecutionID, h.env.WorkerID); err != nil {
		return NodeResult{}, fmt.Errorf("mark node running: %w", err)
	}

	inputs, cleanup, err := h.BuildInputMap(ctx, task.InputArtifacts)
	if err != nil {
		return NodeResult{}, err
	}
	defer cleanup()
	handlerConfig := cloneConfig(task.Config)
	handlerConfig["_input_artifact_meta"] = inputs.MediaInfo

	ext := outputExtension(task.NodeType, task.Config)
	filename := task.NodeExecutionID + ext
	outputStoragePath := path.Join("artifacts", task.JobID, filename)
	outputLocalPath := filepath.Join(h.localRoot(), outputStoragePath)
	if err := os.MkdirAll(filepath.Dir(outputLocalPath), 0o755); err != nil {
		return NodeResult{}, err
	}

	execCtx, cancel := context.WithCancel(ctx)
	cancelled := make(chan struct{}, 1)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		h.watchCancellation(execCtx, cancel, task.NodeExecutionID, cancelled)
	}()
	mediaInfo, err := h.media.Execute(execCtx, inputs.Paths, outputLocalPath, handlerConfig)
	cancel()
	<-watchDone
	if err != nil {
		select {
		case <-cancelled:
			return NodeResult{}, ErrConfirmedCancellation
		default:
			return NodeResult{}, err
		}
	}
	info, err := os.Stat(outputLocalPath)
	if err != nil {
		return NodeResult{}, fmt.Errorf("handler did not produce output: %w", err)
	}

	storageBackend, storagePath, err := h.persistOutput(ctx, outputLocalPath, outputStoragePath)
	if err != nil {
		return NodeResult{}, err
	}
	artifactID, err := h.env.Store.CreateIntermediateArtifact(ctx, store.CreateArtifactInput{
		JobID:           task.JobID,
		NodeExecutionID: task.NodeExecutionID,
		Kind:            contracts.ArtifactKindIntermediate,
		Filename:        filename,
		MimeType:        store.GuessMime(ext),
		FileSize:        info.Size(),
		StorageBackend:  storageBackend,
		StoragePath:     storagePath,
		MediaInfo:       normalizeMediaInfo(mediaInfo),
	})
	if err != nil {
		return NodeResult{}, fmt.Errorf("create artifact row: %w", err)
	}
	return NodeResult{OutputArtifactID: artifactID}, nil
}

func normalizeMediaInfo(mediaInfo map[string]any) map[string]any {
	if mediaInfo == nil {
		return map[string]any{}
	}
	return mediaInfo
}

func (h MediaTaskHandler) localRoot() string {
	if h.env.LocalRoot != "" {
		return h.env.LocalRoot
	}
	return "/tmp/vp_storage"
}
