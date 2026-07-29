package store

import (
	"context"
	"time"

	"github.com/Ctwqk/videoprocess/internal/contracts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func (s *Store) ClaimWorkerNode(
	ctx context.Context,
	lease WorkerRegistrationLease,
	jobID uuid.UUID,
	nodeExecutionID uuid.UUID,
	proof WorkerTaskDeliveryProof,
) (WorkerNodeClaim, error) {
	var empty WorkerNodeClaim
	if jobID == uuid.Nil ||
		nodeExecutionID == uuid.Nil ||
		lease.RegistrationID == uuid.Nil ||
		lease.LeaseEpoch <= 0 ||
		lease.RedisConsumerID == "" {
		return empty, &WorkerRegistrationError{Code: "claim_mismatch"}
	}
	if err := validateWorkerTaskDeliveryProof(proof); err != nil {
		return empty, err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	var workerStartedAt time.Time
	var attestationText string
	if err := tx.QueryRow(
		ctx,
		`
		SELECT worker_started_at, attestation_id::text
		FROM public.vp_claim_worker_node(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		)
		`,
		lease.RegistrationID,
		lease.LeaseEpoch,
		lease.RedisConsumerID,
		jobID,
		nodeExecutionID,
		proof.RedisStream,
		proof.ConsumerGroup,
		proof.MessageID,
		proof.PayloadSHA256,
		proof.DispatchKey,
	).Scan(&workerStartedAt, &attestationText); err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	attestationID, err := uuid.Parse(attestationText)
	if err != nil {
		return empty, &WorkerRegistrationError{
			Code: "task_delivery_attestation_mismatch",
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return empty, sanitizeWorkerRegistrationError(err)
	}
	return WorkerNodeClaim{
		RegistrationID:  lease.RegistrationID,
		LeaseEpoch:      lease.LeaseEpoch,
		WorkerID:        lease.RedisConsumerID,
		WorkerStartedAt: workerStartedAt.UTC(),
		JobID:           jobID,
		NodeExecutionID: nodeExecutionID,
		AttestationID:   attestationID,
		Delivery:        proof,
	}, nil
}

func (s *Store) RequireWorkerNodeClaim(
	ctx context.Context,
	claim WorkerNodeClaim,
) error {
	if err := validateWorkerNodeClaim(claim); err != nil {
		return err
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	defer rollbackWorkerTransaction(tx)
	if err := requireWorkerNodeClaimTx(ctx, tx, claim); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}

func requireWorkerNodeClaimTx(
	ctx context.Context,
	tx pgx.Tx,
	claim WorkerNodeClaim,
) error {
	if _, err := tx.Exec(
		ctx,
		`
		SELECT public.vp_require_worker_node_claim(
			$1, $2, $3, $4, $5, $6
		)
		`,
		claim.RegistrationID,
		claim.LeaseEpoch,
		claim.WorkerID,
		claim.WorkerStartedAt,
		claim.JobID,
		claim.NodeExecutionID,
	); err != nil {
		return sanitizeWorkerRegistrationError(err)
	}
	return nil
}
