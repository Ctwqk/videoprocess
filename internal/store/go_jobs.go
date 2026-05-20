package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type GoJobCreateInput struct {
	PipelineID       string
	PipelineSnapshot contracts.PipelineDefinition
	SubmittedBy      string
}

type GoNodeExecutionInput struct {
	NodeID     string
	NodeType   string
	NodeLabel  string
	NodeConfig map[string]any
}

func (s *Store) CreateGoJob(ctx context.Context, in GoJobCreateInput) (JobDetailRow, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return JobDetailRow{}, err
	}
	defer tx.Rollback(ctx)

	snapshot, err := json.Marshal(in.PipelineSnapshot)
	if err != nil {
		return JobDetailRow{}, err
	}

	var jobID [16]byte
	err = tx.QueryRow(ctx, `
        INSERT INTO jobs (pipeline_id, pipeline_snapshot, status, submitted_by, orchestrator_owner)
        VALUES ($1, $2, 'PENDING', $3, 'go')
        RETURNING id
    `, in.PipelineID, string(snapshot), goSubmittedBy(in.SubmittedBy)).Scan(&jobID)
	if err != nil {
		return JobDetailRow{}, err
	}
	jobIDStr := uuidString(jobID)

	for _, node := range in.PipelineSnapshot.Nodes {
		if _, err := tx.Exec(ctx, `
            INSERT INTO node_executions (job_id, node_id, node_type, node_label, node_config, status)
            VALUES ($1, $2, $3, $4, $5, 'PENDING')
        `, jobIDStr, node.ID, node.Type, goNodeLabel(node), goNodeConfig(node)); err != nil {
			return JobDetailRow{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return JobDetailRow{}, err
	}
	return s.GetJobDetail(ctx, jobIDStr)
}

func (s *Store) LoadGoJobForUpdate(ctx context.Context, jobID string) (JobDetailRow, error) {
	// This is an owner-guarded load only. It intentionally does not issue
	// SELECT FOR UPDATE because Store methods do not expose transaction scope
	// to callers, so any row lock would be released before the caller can act.
	var id [16]byte
	if err := s.Pool.QueryRow(ctx, `
        SELECT id
        FROM jobs
        WHERE id = $1 AND orchestrator_owner = 'go'
    `, jobID).Scan(&id); err != nil {
		return JobDetailRow{}, err
	}
	return s.GetJobDetail(ctx, uuidString(id))
}

func (s *Store) MarkGoJobPlanning(ctx context.Context, jobID string, executionPlan map[string]any) error {
	tag, err := s.Pool.Exec(ctx, `
        UPDATE jobs
        SET status = 'PLANNING', execution_plan = $2, started_at = COALESCE(started_at, NOW())
        WHERE id = $1 AND orchestrator_owner = 'go'
    `, jobID, executionPlan)
	return guardedExecResult(tag.RowsAffected(), err)
}

func (s *Store) MarkGoJobRunning(ctx context.Context, jobID string) error {
	tag, err := s.Pool.Exec(ctx, `
        UPDATE jobs
        SET status = 'RUNNING', started_at = COALESCE(started_at, NOW())
        WHERE id = $1 AND orchestrator_owner = 'go'
    `, jobID)
	return guardedExecResult(tag.RowsAffected(), err)
}

func (s *Store) MarkGoNodeQueued(ctx context.Context, nodeExecutionID string, inputArtifactIDs []string) (bool, error) {
	inputUUIDs, err := uuidArray(inputArtifactIDs)
	if err != nil {
		return false, err
	}
	tag, err := s.Pool.Exec(ctx, `
        UPDATE node_executions
        SET status = 'QUEUED',
            queued_at = COALESCE(queued_at, NOW()),
            input_artifact_ids = $2,
            error_message = NULL,
            error_trace = NULL
        WHERE id = $1
          AND status = 'PENDING'
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, nodeExecutionID, inputUUIDs)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() > 0 {
		return true, nil
	}
	if err := s.ensureGoNodeExists(ctx, nodeExecutionID); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Store) ReleaseGoNodeQueueClaim(ctx context.Context, nodeExecutionID string) error {
	tag, err := s.Pool.Exec(ctx, `
        UPDATE node_executions
        SET status = 'PENDING',
            queued_at = NULL,
            input_artifact_ids = ARRAY[]::uuid[]
        WHERE id = $1
          AND status = 'QUEUED'
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, nodeExecutionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	return s.ensureGoNodeExists(ctx, nodeExecutionID)
}

func (s *Store) MarkGoNodeSucceeded(ctx context.Context, jobID string, nodeExecutionID string, outputArtifactID string) error {
	tag, err := s.Pool.Exec(ctx, `
        UPDATE node_executions
        SET status = 'SUCCEEDED',
            progress = 100,
            completed_at = COALESCE(completed_at, NOW()),
            error_message = NULL,
            error_trace = NULL,
            output_artifact_id = $3
        WHERE id = $2
          AND job_id = $1
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, jobID, nodeExecutionID, outputArtifactID)
	return guardedExecResult(tag.RowsAffected(), err)
}

func (s *Store) MarkGoNodeFailed(ctx context.Context, jobID string, nodeExecutionID string, errorMessage string) error {
	tag, err := s.Pool.Exec(ctx, `
        UPDATE node_executions
        SET status = 'FAILED',
            completed_at = COALESCE(completed_at, NOW()),
            error_message = $3
        WHERE id = $2
          AND job_id = $1
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, jobID, nodeExecutionID, errorMessage)
	return guardedExecResult(tag.RowsAffected(), err)
}

func (s *Store) IncrementGoNodeRetry(ctx context.Context, jobID string, nodeExecutionID string) error {
	tag, err := s.Pool.Exec(ctx, `
        UPDATE node_executions
        SET retry_count = retry_count + 1,
            status = 'PENDING',
            queued_at = NULL,
            started_at = NULL,
            completed_at = NULL,
            worker_id = NULL,
            error_message = NULL,
            error_trace = NULL
        WHERE id = $2
          AND job_id = $1
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, jobID, nodeExecutionID)
	return guardedExecResult(tag.RowsAffected(), err)
}

func (s *Store) SkipGoDownstreamNodes(ctx context.Context, jobID string, nodeIDs []string) error {
	if err := s.ensureGoJobExists(ctx, jobID); err != nil {
		return err
	}
	if len(nodeIDs) == 0 {
		return nil
	}
	_, err := s.Pool.Exec(ctx, `
        UPDATE node_executions
        SET status = 'SKIPPED',
            completed_at = COALESCE(completed_at, NOW())
        WHERE job_id = $1
          AND node_id = ANY($2)
          AND status IN ('PENDING', 'QUEUED')
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, jobID, nodeIDs)
	return err
}

func (s *Store) FinalizeGoJob(ctx context.Context, jobID string, status string, errorMessage *string, finalArtifactNodeIDs []string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var jobExists bool
	if err := tx.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM jobs
            WHERE id = $1 AND orchestrator_owner = 'go'
        )
    `, jobID).Scan(&jobExists); err != nil {
		return err
	}
	if !jobExists {
		return pgx.ErrNoRows
	}

	finalNodeIDs := finalArtifactNodeList(finalArtifactNodeIDs)
	if len(finalNodeIDs) > 0 {
		var validFinalNodes int
		if err := tx.QueryRow(ctx, `
            SELECT COUNT(DISTINCT ne.node_id)
            FROM node_executions ne
            JOIN jobs j ON j.id = ne.job_id
            JOIN artifacts a ON a.id = ne.output_artifact_id
            WHERE ne.job_id = $1
              AND ne.node_id = ANY($2)
              AND ne.status = 'SUCCEEDED'
              AND j.orchestrator_owner = 'go'
              AND a.job_id = ne.job_id
              AND a.node_execution_id = ne.id
        `, jobID, finalNodeIDs).Scan(&validFinalNodes); err != nil {
			return err
		}
		if validFinalNodes != len(finalNodeIDs) {
			return fmt.Errorf("%w: final artifact nodes must have successful output artifacts", ErrConflict)
		}

		tag, err := tx.Exec(ctx, `
            UPDATE artifacts
            SET kind = 'FINAL'
            WHERE job_id = $1
              AND node_execution_id IN (
                  SELECT ne.id
                  FROM node_executions ne
                  JOIN jobs j ON j.id = ne.job_id
                  WHERE ne.job_id = $1
                    AND ne.node_id = ANY($2)
                    AND j.orchestrator_owner = 'go'
                    AND ne.status = 'SUCCEEDED'
                    AND ne.output_artifact_id = artifacts.id
              )
        `, jobID, finalNodeIDs)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != int64(len(finalNodeIDs)) {
			return fmt.Errorf("%w: final artifact promotion count mismatch", ErrConflict)
		}
	}

	tag, err := tx.Exec(ctx, `
        UPDATE jobs
        SET status = $2,
            error_message = $3,
            completed_at = COALESCE(completed_at, NOW())
        WHERE id = $1 AND orchestrator_owner = 'go'
    `, jobID, status, errorMessage)
	if err := guardedExecResult(tag.RowsAffected(), err); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListRecoverableGoJobs(ctx context.Context) ([]JobDetailRow, error) {
	rows, err := s.Pool.Query(ctx, `
        SELECT id
        FROM jobs
        WHERE orchestrator_owner = 'go'
          AND status IN ('PENDING', 'PLANNING', 'RUNNING')
        ORDER BY submitted_at ASC
    `)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0)
	for rows.Next() {
		var id [16]byte
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, uuidString(id))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	jobs := make([]JobDetailRow, 0, len(ids))
	for _, id := range ids {
		job, err := s.GetJobDetail(ctx, id)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (s *Store) ResetStaleGoNodes(ctx context.Context, jobID string, staleBefore time.Time) error {
	if err := s.ensureGoJobExists(ctx, jobID); err != nil {
		return err
	}
	_, err := s.Pool.Exec(ctx, `
        UPDATE node_executions
        SET status = 'PENDING',
            queued_at = NULL,
            started_at = NULL,
            worker_id = NULL,
            input_artifact_ids = ARRAY[]::uuid[],
            error_message = NULL,
            error_trace = NULL
        WHERE job_id = $1
          AND (
              (status = 'RUNNING' AND (started_at IS NULL OR started_at < $2))
              OR (status = 'QUEUED' AND (queued_at IS NULL OR queued_at < $2))
          )
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, jobID, staleBefore)
	return err
}

func (s *Store) CreateSourceArtifact(ctx context.Context, jobID string, nodeExecutionID string, assetID string) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	var filename string
	var originalName string
	var mimeType *string
	var fileSize *int64
	var storageBackend string
	var storagePath string
	var mediaInfo map[string]any
	if err := tx.QueryRow(ctx, `
        SELECT filename, original_name, mime_type, file_size, storage_backend, storage_path, media_info
        FROM assets
        WHERE id = $1
    `, assetID).Scan(&filename, &originalName, &mimeType, &fileSize, &storageBackend, &storagePath, &mediaInfo); err != nil {
		return "", err
	}
	artifactMediaInfo := copyAnyMap(mediaInfo)
	artifactMediaInfo["source_asset_id"] = assetID
	artifactMediaInfo["asset_id"] = assetID
	if originalName != "" {
		artifactMediaInfo["original_name"] = originalName
	}

	var artifactID [16]byte
	if err := tx.QueryRow(ctx, `
        INSERT INTO artifacts (
            job_id, node_execution_id, kind, filename, mime_type, file_size,
            storage_backend, storage_path, media_info
        )
        SELECT $1, $2, 'INTERMEDIATE', $3, $4, $5, $6, $7, $8
        WHERE EXISTS (
            SELECT 1
            FROM node_executions ne
            JOIN jobs j ON j.id = ne.job_id
            WHERE ne.id = $2
              AND ne.job_id = $1
              AND j.orchestrator_owner = 'go'
        )
        RETURNING id
    `, jobID, nodeExecutionID, filename, mimeType, fileSize, storageBackend, storagePath, artifactMediaInfo).Scan(&artifactID); err != nil {
		return "", err
	}
	artifactIDStr := uuidString(artifactID)

	tag, err := tx.Exec(ctx, `
        UPDATE node_executions
        SET status = 'SUCCEEDED',
            progress = 100,
            completed_at = COALESCE(completed_at, NOW()),
            error_message = NULL,
            error_trace = NULL,
            output_artifact_id = $3
        WHERE id = $2
          AND job_id = $1
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.id = node_executions.job_id
                AND j.orchestrator_owner = 'go'
          )
    `, jobID, nodeExecutionID, artifactIDStr)
	if err := guardedExecResult(tag.RowsAffected(), err); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return artifactIDStr, nil
}

func goSubmittedBy(value string) string {
	if value == "" {
		return "system"
	}
	return value
}

func goNodeLabel(node contracts.PipelineNode) string {
	if node.Data.Label == "" {
		return node.Type
	}
	return node.Data.Label
}

func goNodeConfig(node contracts.PipelineNode) map[string]any {
	config := copyAnyMap(node.Data.Config)
	if node.Type == "source" && node.Data.AssetID != nil {
		if _, exists := config["asset_id"]; !exists {
			config["asset_id"] = *node.Data.AssetID
		}
	}
	return config
}

func copyAnyMap(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func finalArtifactNodeSet(nodeIDs []string) map[string]struct{} {
	set := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID != "" {
			set[nodeID] = struct{}{}
		}
	}
	return set
}

func finalArtifactNodeList(nodeIDs []string) []string {
	seen := finalArtifactNodeSet(nil)
	out := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID == "" {
			continue
		}
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		out = append(out, nodeID)
	}
	return out
}

func (s *Store) ensureGoJobExists(ctx context.Context, jobID string) error {
	var exists bool
	if err := s.Pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM jobs
            WHERE id = $1 AND orchestrator_owner = 'go'
        )
    `, jobID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return pgx.ErrNoRows
	}
	return nil
}

func (s *Store) ensureGoNodeExists(ctx context.Context, nodeExecutionID string) error {
	var exists bool
	if err := s.Pool.QueryRow(ctx, `
        SELECT EXISTS (
            SELECT 1
            FROM node_executions ne
            JOIN jobs j ON j.id = ne.job_id
            WHERE ne.id = $1 AND j.orchestrator_owner = 'go'
        )
    `, nodeExecutionID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return pgx.ErrNoRows
	}
	return nil
}

func guardedExecResult(rowsAffected int64, err error) error {
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

func uuidArray(ids []string) ([]pgtype.UUID, error) {
	out := make([]pgtype.UUID, 0, len(ids))
	for _, id := range ids {
		var uuid pgtype.UUID
		if err := uuid.Scan(id); err != nil {
			return nil, fmt.Errorf("invalid uuid %q: %w", id, err)
		}
		out = append(out, uuid)
	}
	return out, nil
}
