package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ArtifactCleanupCandidate struct {
	ID             string
	StorageBackend string
	StoragePath    string
	FileSize       int64
	DeleteStorage  bool
}

type ArtifactCleanupResult struct {
	DeletedCount int   `json:"deleted_count"`
	FreedBytes   int64 `json:"freed_bytes"`
}

func (s *Store) PersistWorkerArtifact(
	ctx context.Context,
	claim WorkerNodeClaim,
	in CreateArtifactInput,
	save WorkerArtifactSaver,
) (string, error) {
	if err := validateWorkerNodeClaim(claim); err != nil {
		return "", err
	}
	jobID, jobErr := uuid.Parse(in.JobID)
	nodeExecutionID, nodeErr := uuid.Parse(in.NodeExecutionID)
	if save == nil ||
		jobErr != nil ||
		nodeErr != nil ||
		jobID != claim.JobID ||
		nodeExecutionID != claim.NodeExecutionID ||
		in.Kind != contracts.ArtifactKindIntermediate ||
		strings.TrimSpace(in.Filename) == "" ||
		in.Filename != strings.TrimSpace(in.Filename) ||
		in.FileSize < 0 ||
		strings.TrimSpace(in.StorageBackend) == "" ||
		in.StorageBackend != strings.TrimSpace(in.StorageBackend) ||
		strings.TrimSpace(in.StoragePath) == "" ||
		in.StoragePath != strings.TrimSpace(in.StoragePath) {
		return "", &WorkerRegistrationError{Code: "artifact_mismatch"}
	}
	mediaInfoJSON, err := json.Marshal(in.MediaInfo)
	if err != nil {
		return "", &WorkerRegistrationError{Code: "artifact_mismatch"}
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	if err := requireWorkerNodeClaimTx(ctx, tx, claim); err != nil {
		return "", err
	}
	err = save(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return "", context.Cause(ctx)
		}
		return "", errors.New("worker artifact save failed")
	}
	if err := requireWorkerNodeClaimTx(ctx, tx, claim); err != nil {
		return "", err
	}
	var artifactText string
	if err := tx.QueryRow(
		ctx,
		`
		SELECT public.vp_persist_worker_artifact(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12::jsonb
		)::text
		`,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
		claim.JobID,
		claim.NodeExecutionID,
		in.Filename,
		in.MimeType,
		in.FileSize,
		in.StorageBackend,
		in.StoragePath,
		string(mediaInfoJSON),
	).Scan(&artifactText); err != nil {
		return "", sanitizeWorkerRegistrationError(err)
	}
	artifactID, err := uuid.Parse(artifactText)
	if err != nil || artifactID == uuid.Nil {
		return "", &WorkerRegistrationError{Code: "artifact_mismatch"}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", sanitizeWorkerRegistrationError(err)
	}
	return artifactID.String(), nil
}

func (s *Store) CleanupArtifactCandidates(ctx context.Context, jobID *string) ([]ArtifactCleanupCandidate, error) {
	args := []any{}
	filter := ""
	if jobID != nil && *jobID != "" {
		args = append(args, *jobID)
		filter = " AND a.job_id = $1"
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT a.id, a.storage_backend, a.storage_path, COALESCE(a.file_size, 0),
		       ne.node_id, ne.status::text, j.pipeline_snapshot
		FROM artifacts a
		JOIN jobs j ON a.job_id = j.id
		JOIN node_executions ne ON a.node_execution_id = ne.id
		WHERE a.kind = 'INTERMEDIATE'
		  AND j.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'PARTIALLY_FAILED')
	`+filter, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	candidates := make([]ArtifactCleanupCandidate, 0)
	for rows.Next() {
		var id [16]byte
		var candidate ArtifactCleanupCandidate
		var nodeID string
		var nodeStatus string
		var snapshot any
		if err := rows.Scan(&id, &candidate.StorageBackend, &candidate.StoragePath, &candidate.FileSize, &nodeID, &nodeStatus, &snapshot); err != nil {
			return nil, err
		}
		if nodeStatus == "SUCCEEDED" && isTerminalSnapshotNode(snapshot, nodeID) {
			continue
		}
		candidate.ID = uuidString(id)
		candidate.DeleteStorage = !strings.HasPrefix(candidate.StoragePath, "download-cache/")
		if candidate.DeleteStorage {
			shared, err := s.storagePathIsShared(ctx, candidate.ID, candidate.StorageBackend, candidate.StoragePath)
			if err != nil {
				return nil, err
			}
			candidate.DeleteStorage = !shared
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (s *Store) DeleteArtifactRecord(ctx context.Context, id string) error {
	if _, err := s.Pool.Exec(ctx, "UPDATE node_executions SET output_artifact_id = NULL WHERE output_artifact_id = $1", id); err != nil {
		return err
	}
	_, err := s.Pool.Exec(ctx, "DELETE FROM artifacts WHERE id = $1", id)
	return err
}

func (s *Store) storagePathIsShared(ctx context.Context, artifactID string, backend string, path string) (bool, error) {
	var shared bool
	err := s.Pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM assets
			WHERE storage_backend = $1 AND storage_path = $2
		) OR EXISTS (
			SELECT 1 FROM artifacts
			WHERE id != $3 AND storage_backend = $1 AND storage_path = $2
		)
	`, backend, path, artifactID).Scan(&shared)
	return shared, err
}

func isTerminalSnapshotNode(snapshot any, nodeID string) bool {
	edgeSources := map[string]struct{}{}
	root, ok := snapshot.(map[string]any)
	if !ok {
		return false
	}
	edges, ok := root["edges"].([]any)
	if !ok {
		return false
	}
	for _, raw := range edges {
		edge, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		source, ok := edge["source"].(string)
		if ok && source != "" {
			edgeSources[source] = struct{}{}
		}
	}
	_, hasDownstream := edgeSources[nodeID]
	return !hasDownstream
}
