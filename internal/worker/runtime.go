package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

type WorkerArtifactStreamSaver interface {
	SaveStream(context.Context, string, io.Reader, int64) error
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
	persistContext := ctx
	cancelPersist := func() {}
	if task.WorkerClaim != nil {
		persistContext, cancelPersist = context.WithTimeout(
			ctx,
			h.storageOperationTimeout(),
		)
	}
	defer cancelPersist()
	var info os.FileInfo
	if task.WorkerClaim == nil {
		info, err = os.Stat(outputLocalPath)
	} else {
		info, err = regularWorkerOutputInfo(
			persistContext,
			outputLocalPath,
		)
	}
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
			persistContext,
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
					info.Size(),
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
	outputSize int64,
) error {
	if h.env.Storage == nil {
		return errors.New("remote storage backend is not configured")
	}
	streamSaver, ok := h.env.Storage.(WorkerArtifactStreamSaver)
	if !ok {
		return errors.New("remote storage backend does not support streaming")
	}
	attempts := h.env.StorageSaveAttempts
	if attempts <= 0 {
		attempts = 3
	}
	if attempts > 5 {
		attempts = 5
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		source, err := openWorkerOutputStream(
			ctx,
			outputLocalPath,
			outputSize,
		)
		if err != nil {
			return fmt.Errorf("open output for upload: %w", err)
		}
		lastErr = saveWorkerOutputStream(
			ctx,
			streamSaver,
			outputStoragePath,
			source,
			outputSize,
		)
		_ = source.Close()
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

func (h MediaTaskHandler) storageOperationTimeout() time.Duration {
	if h.env.StorageOperationTimeout > 0 {
		return h.env.StorageOperationTimeout
	}
	return 10 * time.Second
}

type workerOutputInfoResult struct {
	info os.FileInfo
	err  error
}

func regularWorkerOutputInfo(
	ctx context.Context,
	outputLocalPath string,
) (os.FileInfo, error) {
	result := make(chan workerOutputInfoResult, 1)
	go func() {
		info, err := os.Lstat(outputLocalPath)
		result <- workerOutputInfoResult{info: info, err: err}
	}()
	select {
	case completed := <-result:
		if completed.err != nil {
			return nil, completed.err
		}
		if !completed.info.Mode().IsRegular() {
			return nil, errors.New("worker output is not a regular file")
		}
		return completed.info, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

type workerOutputOpenResult struct {
	file *os.File
	err  error
}

func openWorkerOutputStream(
	ctx context.Context,
	outputLocalPath string,
	expectedSize int64,
) (*os.File, error) {
	result := make(chan workerOutputOpenResult, 1)
	go func() {
		file, err := os.OpenFile(outputLocalPath, os.O_RDONLY, 0)
		if err == nil {
			info, statErr := file.Stat()
			switch {
			case statErr != nil:
				err = statErr
			case !info.Mode().IsRegular():
				err = errors.New("worker output is not a regular file")
			case info.Size() != expectedSize:
				err = errors.New("worker output changed before upload")
			}
			if err != nil {
				_ = file.Close()
				file = nil
			}
		}
		result <- workerOutputOpenResult{file: file, err: err}
	}()
	select {
	case completed := <-result:
		return completed.file, completed.err
	case <-ctx.Done():
		go func() {
			completed := <-result
			if completed.file != nil {
				_ = completed.file.Close()
			}
		}()
		return nil, context.Cause(ctx)
	}
}

func saveWorkerOutputStream(
	ctx context.Context,
	saver WorkerArtifactStreamSaver,
	outputStoragePath string,
	source *os.File,
	outputSize int64,
) error {
	result := make(chan error, 1)
	go func() {
		result <- saver.SaveStream(
			ctx,
			outputStoragePath,
			source,
			outputSize,
		)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		_ = source.Close()
		return context.Cause(ctx)
	}
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
