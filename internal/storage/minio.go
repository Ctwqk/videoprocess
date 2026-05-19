package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinIOOptions captures the runtime configuration the Python MinIO backend
// reads from settings (`MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, ...).
type MinIOOptions struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Secure    bool
}

// MinIOBackend mirrors `backend/app/storage/minio_backend.py`.
//
// It uses a key-per-artifact layout inside a single bucket. Object-store paths
// are not directly readable by ffmpeg, so LocalPath returns (_, false) and
// callers must download via Read before passing the bytes to a temp file.
type MinIOBackend struct {
	client *minio.Client
	bucket string
}

// NewMinIOBackend constructs a MinIOBackend and ensures the bucket exists.
// Bucket creation matches the Python implementation (created on first use).
func NewMinIOBackend(ctx context.Context, opts MinIOOptions) (*MinIOBackend, error) {
	if opts.Endpoint == "" {
		return nil, errors.New("minio: endpoint is required")
	}
	if opts.Bucket == "" {
		return nil, errors.New("minio: bucket is required")
	}
	client, err := minio.New(opts.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.AccessKey, opts.SecretKey, ""),
		Secure: opts.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("minio: build client: %w", err)
	}
	exists, err := client.BucketExists(ctx, opts.Bucket)
	if err != nil {
		return nil, fmt.Errorf("minio: probe bucket: %w", err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, opts.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("minio: create bucket %q: %w", opts.Bucket, err)
		}
	}
	return &MinIOBackend{client: client, bucket: opts.Bucket}, nil
}

func (b *MinIOBackend) Read(ctx context.Context, path string) ([]byte, error) {
	obj, err := b.client.GetObject(ctx, b.bucket, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

func (b *MinIOBackend) Save(ctx context.Context, path string, data []byte) error {
	_, err := b.client.PutObject(
		ctx,
		b.bucket,
		path,
		bytes.NewReader(data),
		int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	return err
}

func (b *MinIOBackend) Exists(ctx context.Context, path string) (bool, error) {
	_, err := b.client.StatObject(ctx, b.bucket, path, minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.Code == "NoSuchKey" || resp.StatusCode == 404 {
		return false, nil
	}
	return false, err
}

func (b *MinIOBackend) Delete(ctx context.Context, path string) error {
	return b.client.RemoveObject(ctx, b.bucket, path, minio.RemoveObjectOptions{})
}

// LocalPath always returns ("", false) because MinIO keys are not visible to
// the worker filesystem. Matches the Python contract `get_local_path -> None`.
func (b *MinIOBackend) LocalPath(path string) (string, bool) {
	return "", false
}
