package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/Ctwqk/videoprocess/internal/config"
)

// Backend abstracts artifact storage. Method semantics mirror
// `backend/app/storage/base.py StorageBackend`.
type Backend interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Save(ctx context.Context, path string, data []byte) error
	Exists(ctx context.Context, path string) (bool, error)
	Delete(ctx context.Context, path string) error
	// LocalPath returns the on-disk file path when the backend stores data
	// on a local filesystem the worker can read directly. The second return
	// is false for object stores; callers must fall back to Read.
	LocalPath(path string) (string, bool)
}

// LocalBackend stores artifacts under a single root directory, matching the
// Python `LocalStorageBackend` path layout.
type LocalBackend struct {
	Root string
}

func (b LocalBackend) fullPath(path string) string {
	return filepath.Join(b.Root, path)
}

func (b LocalBackend) Read(ctx context.Context, path string) ([]byte, error) {
	return os.ReadFile(b.fullPath(path))
}

func (b LocalBackend) Save(ctx context.Context, path string, data []byte) error {
	full := b.fullPath(path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0o644)
}

func (b LocalBackend) Exists(ctx context.Context, path string) (bool, error) {
	_, err := os.Stat(b.fullPath(path))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func (b LocalBackend) Delete(ctx context.Context, path string) error {
	err := os.Remove(b.fullPath(path))
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (b LocalBackend) LocalPath(path string) (string, bool) {
	return b.fullPath(path), true
}

// FromConfig returns the storage backend selected by cfg.StorageBackend.
// Unknown values fall back to local, matching `app/storage/manager.py` behavior.
func FromConfig(ctx context.Context, cfg config.Config) (Backend, error) {
	switch cfg.StorageBackend {
	case "minio":
		return NewMinIOBackend(ctx, MinIOOptions{
			Endpoint:  cfg.MinIOEndpoint,
			AccessKey: cfg.MinIOAccessKey,
			SecretKey: cfg.MinIOSecretKey,
			Bucket:    cfg.MinIOBucket,
			Secure:    cfg.MinIOSecure,
		})
	default:
		return LocalBackend{Root: cfg.StorageLocalRoot}, nil
	}
}
