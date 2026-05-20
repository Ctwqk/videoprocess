package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Ctwqk/videoprocess/internal/store"
)

func (h MediaTaskHandler) resolveInput(ctx context.Context, artifact store.ArtifactRow) (string, func(), error) {
	if artifact.StorageBackend == "local" {
		if filepath.IsAbs(artifact.StoragePath) {
			return artifact.StoragePath, func() {}, nil
		}
		if h.env.Storage != nil {
			if localPath, ok := h.env.Storage.LocalPath(artifact.StoragePath); ok {
				return localPath, func() {}, nil
			}
		}
		return filepath.Join(h.localRoot(), artifact.StoragePath), func() {}, nil
	}
	if h.env.Storage == nil {
		return "", func() {}, fmt.Errorf("storage backend %q is not configured", artifact.StorageBackend)
	}
	data, err := h.env.Storage.Read(ctx, artifact.StoragePath)
	if err != nil {
		return "", func() {}, fmt.Errorf("read input artifact %s: %w", artifact.ID, err)
	}
	suffix := filepath.Ext(artifact.Filename)
	if suffix == "" {
		suffix = ".mp4"
	}
	return writeTempFile("vp_input", suffix, data)
}

func (h MediaTaskHandler) persistOutput(ctx context.Context, outputLocalPath string, outputStoragePath string) (string, string, error) {
	backend := strings.TrimSpace(h.env.StorageBackend)
	if backend == "" {
		backend = "local"
	}
	if backend == "local" {
		return "local", outputLocalPath, nil
	}
	if h.env.Storage == nil {
		return "", "", fmt.Errorf("storage backend %q is not configured", backend)
	}
	data, err := os.ReadFile(outputLocalPath)
	if err != nil {
		return "", "", fmt.Errorf("read output for upload: %w", err)
	}
	if err := h.env.Storage.Save(ctx, outputStoragePath, data); err != nil {
		return "", "", fmt.Errorf("save output artifact: %w", err)
	}
	return backend, outputStoragePath, nil
}

func outputExtension(nodeType string, config map[string]any) string {
	if nodeType == "speech_to_subtitle" || nodeType == "subtitle_translate" {
		return ".srt"
	}
	if nodeType == "subtitle_to_speech" {
		return ".wav"
	}
	if nodeType == "material_library_ingest" {
		return ".json"
	}
	if nodeType == "transcode" {
		if raw, ok := config["format"].(string); ok && raw != "" {
			return "." + raw
		}
	}
	if raw, ok := config["output_format"].(string); ok && raw != "" {
		return "." + raw
	}
	return ".mp4"
}

func writeTempFile(prefix string, suffix string, data []byte) (string, func(), error) {
	file, err := os.CreateTemp("", prefix+"_*"+suffix)
	if err != nil {
		return "", func() {}, err
	}
	path := file.Name()
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", func() {}, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", func() {}, err
	}
	return path, func() { _ = os.Remove(path) }, nil
}
