package storage

import (
	"context"
	"os"
	"path/filepath"
)

type Backend interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Save(ctx context.Context, path string, data []byte) error
	Exists(ctx context.Context, path string) (bool, error)
	LocalPath(path string) (string, bool)
}

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
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (b LocalBackend) LocalPath(path string) (string, bool) {
	return b.fullPath(path), true
}
