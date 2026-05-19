package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/config"
)

func TestLocalBackendRoundTrip(t *testing.T) {
	dir := t.TempDir()
	backend := LocalBackend{Root: dir}
	ctx := context.Background()

	if err := backend.Save(ctx, "nested/dir/file.bin", []byte("hello")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := backend.Read(ctx, "nested/dir/file.bin")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("Read = %q want %q", string(got), "hello")
	}

	exists, err := backend.Exists(ctx, "nested/dir/file.bin")
	if err != nil || !exists {
		t.Fatalf("Exists = (%v, %v)", exists, err)
	}

	resolved, ok := backend.LocalPath("nested/dir/file.bin")
	if !ok {
		t.Fatal("LocalPath should return ok=true for local backend")
	}
	if resolved != filepath.Join(dir, "nested/dir/file.bin") {
		t.Fatalf("LocalPath = %q", resolved)
	}

	if err := backend.Delete(ctx, "nested/dir/file.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	exists, _ = backend.Exists(ctx, "nested/dir/file.bin")
	if exists {
		t.Fatal("file should not exist after Delete")
	}

	// Delete of a missing path must not error, mirroring LocalStorageBackend.delete.
	if err := backend.Delete(ctx, "missing"); err != nil {
		t.Fatalf("Delete missing path: %v", err)
	}
}

func TestFromConfigFallsBackToLocal(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Config{StorageBackend: "", StorageLocalRoot: dir}

	backend, err := FromConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	if _, ok := backend.(LocalBackend); !ok {
		t.Fatalf("expected LocalBackend, got %T", backend)
	}
}

func TestFromConfigMinIORequiresEndpoint(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{StorageBackend: "minio", MinIOBucket: "videoprocess"}

	_, err := FromConfig(ctx, cfg)
	if err == nil {
		t.Fatal("FromConfig should return an error when MinIO endpoint is missing")
	}
}
