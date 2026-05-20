package store

import (
	"context"
	"strings"
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
