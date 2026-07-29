package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
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

type RegisteredArtifactStore interface {
	RequireWorkerNodeClaim(context.Context, store.WorkerNodeClaim) error
	PersistWorkerArtifact(
		context.Context,
		store.WorkerNodeClaim,
		store.CreateArtifactInput,
		store.WorkerArtifactSaver,
	) (string, error)
}

type RuntimeEnv struct {
	Store                   TaskStore
	Storage                 storage.Backend
	StorageBackend          string
	LocalRoot               string
	WorkerID                string
	Logger                  *slog.Logger
	CancelPollInterval      time.Duration
	StorageOperationTimeout time.Duration
	StorageSaveAttempts     int
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
	var registeredStore RegisteredArtifactStore
	if task.WorkerClaim == nil {
		state, err := h.env.Store.LoadExecutionState(ctx, task.NodeExecutionID)
		if err != nil {
			return NodeResult{}, fmt.Errorf("load execution state: %w", err)
		}
		if state.JobStatus == contracts.JobStatusCancelled ||
			state.NodeStatus == contracts.NodeStatusCancelled {
			return NodeResult{}, ErrConfirmedCancellation
		}
		if err := h.env.Store.MarkNodeRunning(
			ctx,
			task.NodeExecutionID,
			h.env.WorkerID,
		); err != nil {
			return NodeResult{}, fmt.Errorf("mark node running: %w", err)
		}
	} else {
		var ok bool
		registeredStore, ok = h.env.Store.(RegisteredArtifactStore)
		if !ok {
			return NodeResult{}, errors.New("registered worker store is required")
		}
		if err := registeredStore.RequireWorkerNodeClaim(
			ctx,
			*task.WorkerClaim,
		); err != nil {
			return NodeResult{}, ErrRegistrationLost
		}
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
	storagePrefix := "artifacts"
	if task.WorkerClaim != nil {
		filename = task.NodeExecutionID + "-" +
			workerClaimGeneration(*task.WorkerClaim) + ext
		if strings.TrimSpace(h.env.StorageBackend) != "" &&
			h.env.StorageBackend != "local" {
			storagePrefix = "staging/artifacts"
		}
	}
	outputStoragePath := path.Join(storagePrefix, task.JobID, filename)
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
	if task.WorkerClaim != nil {
		if err := registeredStore.RequireWorkerNodeClaim(
			execCtx,
			*task.WorkerClaim,
		); err != nil {
			cancel()
			<-watchDone
			return NodeResult{}, ErrRegistrationLost
		}
	}
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

	artifactInput := store.CreateArtifactInput{
		JobID:           task.JobID,
		NodeExecutionID: task.NodeExecutionID,
		Kind:            contracts.ArtifactKindIntermediate,
		Filename:        filename,
		MimeType:        store.GuessMime(ext),
		FileSize:        info.Size(),
		MediaInfo:       normalizeMediaInfo(mediaInfo),
	}
	var artifactID string
	if task.WorkerClaim == nil {
		storageBackend, storagePath, persistErr := h.persistOutput(
			ctx,
			outputLocalPath,
			outputStoragePath,
		)
		if persistErr != nil {
			return NodeResult{}, persistErr
		}
		artifactInput.StorageBackend = storageBackend
		artifactInput.StoragePath = storagePath
		artifactID, err = h.env.Store.CreateIntermediateArtifact(
			ctx,
			artifactInput,
		)
	} else {
		artifactInput.StorageBackend = h.storageBackend()
		artifactInput.StoragePath = outputStoragePath
		if artifactInput.StorageBackend == "local" {
			artifactInput.StoragePath = outputLocalPath
		}
		artifactID, err = registeredStore.PersistWorkerArtifact(
			ctx,
			*task.WorkerClaim,
			artifactInput,
			func(saveContext context.Context) error {
				if artifactInput.StorageBackend == "local" {
					return nil
				}
				return h.saveRemoteOutputWithRetry(
					saveContext,
					outputLocalPath,
					outputStoragePath,
				)
			},
		)
	}
	if err != nil {
		return NodeResult{}, fmt.Errorf("create artifact row: %w", err)
	}
	return NodeResult{OutputArtifactID: artifactID}, nil
}

func (h MediaTaskHandler) storageBackend() string {
	backend := strings.TrimSpace(h.env.StorageBackend)
	if backend == "" {
		return "local"
	}
	return backend
}

func (h MediaTaskHandler) saveRemoteOutputWithRetry(
	ctx context.Context,
	outputLocalPath string,
	outputStoragePath string,
) error {
	if h.env.Storage == nil {
		return errors.New("remote storage backend is not configured")
	}
	data, err := os.ReadFile(outputLocalPath)
	if err != nil {
		return fmt.Errorf("read output for upload: %w", err)
	}
	attempts := h.env.StorageSaveAttempts
	if attempts <= 0 {
		attempts = 3
	}
	if attempts > 5 {
		attempts = 5
	}
	timeout := h.env.StorageOperationTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		saveContext, cancel := context.WithTimeout(ctx, timeout)
		lastErr = h.env.Storage.Save(
			saveContext,
			outputStoragePath,
			data,
		)
		cancel()
		if lastErr == nil {
			return nil
		}
		if attempt+1 < attempts {
			timer := time.NewTimer(
				time.Duration(25*(1<<attempt)) * time.Millisecond,
			)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return fmt.Errorf("save output artifact: %w", lastErr)
}

func workerClaimGeneration(claim store.WorkerNodeClaim) string {
	material := strings.Join([]string{
		claim.JobID.String(),
		claim.NodeExecutionID.String(),
		claim.WorkerID,
		pythonUTCISOFormat(claim.WorkerStartedAt),
	}, "\x00")
	digest := sha256.Sum256([]byte(material))
	return hex.EncodeToString(digest[:])[:16]
}

func pythonUTCISOFormat(value time.Time) string {
	utc := value.UTC()
	formatted := utc.Format("2006-01-02T15:04:05")
	if microseconds := utc.Nanosecond() / 1000; microseconds != 0 {
		formatted += fmt.Sprintf(".%06d", microseconds)
	}
	return formatted + "+00:00"
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
