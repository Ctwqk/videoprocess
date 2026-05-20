package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type ArtifactDetailRow struct {
	ID              string    `json:"id"`
	JobID           string    `json:"job_id"`
	NodeExecutionID string    `json:"node_execution_id"`
	Kind            string    `json:"kind"`
	Filename        string    `json:"filename"`
	MimeType        *string   `json:"mime_type"`
	FileSize        *int64    `json:"file_size"`
	CreatedAt       time.Time `json:"created_at"`
}

type NodeExecutionRow struct {
	ID                      string     `json:"id"`
	NodeID                  string     `json:"node_id"`
	NodeType                string     `json:"node_type"`
	NodeLabel               string     `json:"node_label"`
	Status                  string     `json:"status"`
	Progress                int        `json:"progress"`
	WorkerID                *string    `json:"worker_id"`
	QueuedAt                *time.Time `json:"queued_at"`
	StartedAt               *time.Time `json:"started_at"`
	CompletedAt             *time.Time `json:"completed_at"`
	ErrorMessage            *string    `json:"error_message"`
	InputArtifactIDs        []string   `json:"input_artifact_ids"`
	OutputArtifactID        *string    `json:"output_artifact_id"`
	OutputArtifactFilename  *string    `json:"output_artifact_filename"`
	OutputArtifactMediaInfo any        `json:"output_artifact_media_info"`
}

type JobDetailRow struct {
	JobRow
	PipelineSnapshot any                `json:"pipeline_snapshot"`
	ExecutionPlan    any                `json:"execution_plan"`
	NodeExecutions   []NodeExecutionRow `json:"node_executions"`
}

func (s *Store) GetPipeline(ctx context.Context, id string) (PipelineRow, error) {
	var row PipelineRow
	var uuid [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, name, description, definition, is_template, template_tags,
		       created_at, updated_at, version
		FROM pipelines
		WHERE id = $1
	`, id).Scan(&uuid, &row.Name, &row.Description, &row.Definition, &row.IsTemplate,
		&row.TemplateTags, &row.CreatedAt, &row.UpdatedAt, &row.Version)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	if row.TemplateTags == nil {
		row.TemplateTags = []string{}
	}
	return row, nil
}

func (s *Store) GetAssetDetail(ctx context.Context, id string) (AssetRow, error) {
	var row AssetRow
	var uuid [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, filename, original_name, mime_type, file_size, media_info, uploaded_at
		FROM assets
		WHERE id = $1
	`, id).Scan(&uuid, &row.Filename, &row.OriginalName, &row.MimeType, &row.FileSize, &row.MediaInfo, &row.UploadedAt)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	return row, nil
}

func (s *Store) GetArtifactDetail(ctx context.Context, id string) (ArtifactDetailRow, error) {
	var row ArtifactDetailRow
	var uuid [16]byte
	var jobUUID [16]byte
	var nodeUUID [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, job_id, node_execution_id, kind::text, filename, mime_type, file_size, created_at
		FROM artifacts
		WHERE id = $1
	`, id).Scan(&uuid, &jobUUID, &nodeUUID, &row.Kind, &row.Filename, &row.MimeType, &row.FileSize, &row.CreatedAt)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(uuid)
	row.JobID = uuidString(jobUUID)
	row.NodeExecutionID = uuidString(nodeUUID)
	return row, nil
}

func (s *Store) GetJobDetail(ctx context.Context, id string) (JobDetailRow, error) {
	var row JobDetailRow
	var jobUUID [16]byte
	var pipelineUUID [16]byte
	err := s.Pool.QueryRow(ctx, `
		SELECT id, pipeline_id, status::text, submitted_at, started_at, completed_at,
		       error_message, submitted_by, retry_count, orchestrator_owner, pipeline_snapshot, execution_plan
		FROM jobs
		WHERE id = $1
	`, id).Scan(&jobUUID, &pipelineUUID, &row.Status, &row.SubmittedAt, &row.StartedAt,
		&row.CompletedAt, &row.ErrorMessage, &row.SubmittedBy, &row.RetryCount,
		&row.OrchestratorOwner, &row.PipelineSnapshot, &row.ExecutionPlan)
	if err != nil {
		return row, err
	}
	row.ID = uuidString(jobUUID)
	row.PipelineID = uuidString(pipelineUUID)

	nodes, err := s.listNodeExecutions(ctx, row.ID)
	if err != nil {
		return row, err
	}
	row.NodeExecutions = nodes
	return row, nil
}

func (s *Store) listNodeExecutions(ctx context.Context, jobID string) ([]NodeExecutionRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT ne.id, ne.node_id, ne.node_type, ne.node_label, ne.status::text, ne.progress,
		       ne.worker_id, ne.queued_at, ne.started_at, ne.completed_at, ne.error_message,
		       ne.input_artifact_ids, ne.output_artifact_id, a.filename, a.media_info
		FROM node_executions ne
		LEFT JOIN artifacts a ON a.id = ne.output_artifact_id
		WHERE ne.job_id = $1
		ORDER BY ne.id ASC
	`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]NodeExecutionRow, 0)
	for rows.Next() {
		var row NodeExecutionRow
		var uuid [16]byte
		var inputUUIDs []pgtype.UUID
		var outputUUID pgtype.UUID
		if err := rows.Scan(&uuid, &row.NodeID, &row.NodeType, &row.NodeLabel, &row.Status,
			&row.Progress, &row.WorkerID, &row.QueuedAt, &row.StartedAt, &row.CompletedAt,
			&row.ErrorMessage, &inputUUIDs, &outputUUID, &row.OutputArtifactFilename,
			&row.OutputArtifactMediaInfo); err != nil {
			return nil, err
		}
		row.ID = uuidString(uuid)
		row.InputArtifactIDs = make([]string, 0, len(inputUUIDs))
		for _, inputUUID := range inputUUIDs {
			if inputUUID.Valid {
				row.InputArtifactIDs = append(row.InputArtifactIDs, uuidString(inputUUID.Bytes))
			}
		}
		if outputUUID.Valid {
			outputID := uuidString(outputUUID.Bytes)
			row.OutputArtifactID = &outputID
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
