package store

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
)

type ExecutionState struct {
	JobID           string
	NodeExecutionID string
	JobStatus       contracts.JobStatus
	NodeStatus      contracts.NodeStatus
}

func (s *Store) LoadExecutionState(ctx context.Context, nodeExecutionID string) (ExecutionState, error) {
	var state ExecutionState
	var jobUUID [16]byte
	var nodeUUID [16]byte
	var jobStatus string
	var nodeStatus string
	err := s.Pool.QueryRow(ctx, `
		SELECT j.id, ne.id, j.status::text, ne.status::text
		FROM node_executions ne
		JOIN jobs j ON j.id = ne.job_id
		WHERE ne.id = $1
	`, nodeExecutionID).Scan(&jobUUID, &nodeUUID, &jobStatus, &nodeStatus)
	if err != nil {
		return state, err
	}
	state.JobID = uuidString(jobUUID)
	state.NodeExecutionID = uuidString(nodeUUID)
	state.JobStatus = contracts.JobStatus(jobStatus)
	state.NodeStatus = contracts.NodeStatus(nodeStatus)
	return state, nil
}

func (s *Store) MarkNodeRunning(ctx context.Context, nodeExecutionID string, workerID string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE node_executions
		SET status = 'RUNNING', started_at = $2, worker_id = $3
		WHERE id = $1
	`, nodeExecutionID, time.Now().UTC(), workerID)
	return err
}
