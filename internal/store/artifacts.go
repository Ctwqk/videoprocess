package store

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

type ArtifactRow struct {
	ID              string
	JobID           string
	NodeExecutionID string
	Filename        string
	MimeType        *string
	FileSize        *int64
	StorageBackend  string
	StoragePath     string
	MediaInfo       any
	CreatedAt       time.Time
}

type CreateArtifactInput struct {
	JobID           string
	NodeExecutionID string
	Kind            contracts.ArtifactKind
	Filename        string
	MimeType        string
	FileSize        int64
	StorageBackend  string
	StoragePath     string
	MediaInfo       any
}

func (s *Store) GetArtifact(ctx context.Context, id string) (ArtifactRow, error) {
	var row ArtifactRow
	var uuid [16]byte
	var jobUUID [16]byte
	var nodeUUID [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, job_id, node_execution_id, filename, mime_type, file_size,
		       storage_backend, storage_path, media_info, created_at
		FROM artifacts
		WHERE id = $1
	`, id).Scan(&uuid, &jobUUID, &nodeUUID, &row.Filename, &row.MimeType, &row.FileSize, &row.StorageBackend, &row.StoragePath, &row.MediaInfo, &row.CreatedAt)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	row.JobID = uuidString(jobUUID)
	row.NodeExecutionID = uuidString(nodeUUID)
	return row, nil
}

func (s *Store) CreateIntermediateArtifact(ctx context.Context, in CreateArtifactInput) (string, error) {
	var id [16]byte
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO artifacts (
			job_id, node_execution_id, kind, filename, mime_type, file_size,
			storage_backend, storage_path, media_info
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id
	`, in.JobID, in.NodeExecutionID, in.Kind, in.Filename, in.MimeType, in.FileSize, in.StorageBackend, in.StoragePath, in.MediaInfo).Scan(&id)
	if err != nil {
		return "", err
	}
	return uuidString(id), nil
}

func GuessMime(ext string) string {
	switch ext {
	case ".mp4":
		return "video/mp4"
	case ".mkv":
		return "video/x-matroska"
	case ".json":
		return "application/json"
	case ".webm":
		return "video/webm"
	case ".avi":
		return "video/x-msvideo"
	case ".mov":
		return "video/quicktime"
	case ".srt":
		return "application/x-subrip"
	case ".wav":
		return "audio/wav"
	case ".mp3":
		return "audio/mpeg"
	default:
		return "video/mp4"
	}
}
