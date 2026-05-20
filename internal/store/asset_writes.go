package store

import (
	"context"
	"fmt"
)

type AssetStorageRow struct {
	AssetRow
	StorageBackend string
	StoragePath    string
}

type CreateAssetInput struct {
	Filename       string
	OriginalName   string
	MimeType       *string
	FileSize       int64
	StorageBackend string
	StoragePath    string
	MediaInfo      any
}

func (s *Store) CreateAsset(ctx context.Context, in CreateAssetInput) (AssetRow, error) {
	var row AssetRow
	var id [16]byte
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO assets (
			filename, original_name, mime_type, file_size, storage_backend, storage_path, media_info
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, filename, original_name, mime_type, file_size, media_info, uploaded_at
	`, in.Filename, in.OriginalName, in.MimeType, in.FileSize, in.StorageBackend, in.StoragePath, in.MediaInfo).Scan(
		&id,
		&row.Filename,
		&row.OriginalName,
		&row.MimeType,
		&row.FileSize,
		&row.MediaInfo,
		&row.UploadedAt,
	)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(id)
	return row, nil
}

func (s *Store) GetAssetForDownload(ctx context.Context, id string) (AssetStorageRow, error) {
	var row AssetStorageRow
	var uuid [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, filename, original_name, mime_type, file_size, media_info, uploaded_at,
		       storage_backend, storage_path
		FROM assets
		WHERE id = $1
	`, id).Scan(
		&uuid,
		&row.Filename,
		&row.OriginalName,
		&row.MimeType,
		&row.FileSize,
		&row.MediaInfo,
		&row.UploadedAt,
		&row.StorageBackend,
		&row.StoragePath,
	)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	return row, nil
}

func (s *Store) PrepareDeleteAsset(ctx context.Context, id string) (AssetStorageRow, error) {
	row, err := s.GetAssetForDownload(ctx, id)
	if err != nil {
		return row, err
	}
	active, err := s.assetHasActiveReference(ctx, id)
	if err != nil {
		return row, err
	}
	if active {
		return row, fmt.Errorf("%w: asset is referenced by active jobs", ErrConflict)
	}
	return row, nil
}

func (s *Store) DeleteAssetRecord(ctx context.Context, id string) error {
	tag, err := s.Pool.Exec(ctx, "DELETE FROM assets WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("asset not found")
	}
	return nil
}

func (s *Store) DeleteAsset(ctx context.Context, id string) (AssetStorageRow, error) {
	row, err := s.PrepareDeleteAsset(ctx, id)
	if err != nil {
		return row, err
	}
	if err := s.DeleteAssetRecord(ctx, id); err != nil {
		return row, err
	}
	return row, nil
}

func (s *Store) assetHasActiveReference(ctx context.Context, id string) (bool, error) {
	var active bool
	err := s.Pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM jobs j
			WHERE j.status IN ('PENDING', 'WAITING_WINDOW', 'VALIDATING', 'PLANNING', 'RUNNING')
			  AND j.pipeline_snapshot::text LIKE '%' || $1 || '%'
		) OR EXISTS (
			SELECT 1
			FROM node_executions ne
			JOIN jobs j ON j.id = ne.job_id
			WHERE j.status IN ('PENDING', 'WAITING_WINDOW', 'VALIDATING', 'PLANNING', 'RUNNING')
			  AND ne.node_config::text LIKE '%' || $1 || '%'
		)
	`, id).Scan(&active)
	return active, err
}
