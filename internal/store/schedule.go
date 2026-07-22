package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

const VideoScheduleServiceName = "videoprocess"

var ErrVideoScheduleGuardMismatch = errors.New("video schedule guarded open mismatch")

type VideoScheduleStatusRow struct {
	ServiceName  string     `json:"service_name"`
	State        string     `json:"state"`
	GuardedJobID *string    `json:"guarded_job_id"`
	WaitingJobs  int        `json:"waiting_jobs"`
	ActiveJobs   int        `json:"active_jobs"`
	QueuedNodes  int        `json:"queued_nodes"`
	RunningNodes int        `json:"running_nodes"`
	UpdatedAt    *time.Time `json:"updated_at"`
	UpdatedBy    *string    `json:"updated_by"`
	ReleasedJobs int        `json:"released_jobs"`
}

func (s *Store) GetVideoScheduleStatus(ctx context.Context) (VideoScheduleStatusRow, error) {
	return s.getVideoScheduleStatus(ctx, 0)
}

func (s *Store) SetVideoScheduleState(ctx context.Context, state string) (VideoScheduleStatusRow, error) {
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO runtime_schedules (service_name, state, updated_by)
		VALUES ($1, $2, 'go_api')
		ON CONFLICT (service_name)
		DO UPDATE SET state = EXCLUDED.state,
		              guarded_job_id = NULL,
		              updated_by = EXCLUDED.updated_by,
		              updated_at = NOW()
	`, VideoScheduleServiceName, state); err != nil {
		return VideoScheduleStatusRow{}, err
	}
	released := 0
	if state == "OPEN" {
		tag, err := s.Pool.Exec(ctx, `
			UPDATE jobs
			SET status = 'PENDING', error_message = NULL, completed_at = NULL
			WHERE status = 'WAITING_WINDOW'
			  AND orchestrator_owner = 'go'
		`)
		if err != nil {
			return VideoScheduleStatusRow{}, err
		}
		released = int(tag.RowsAffected())
	}
	return s.getVideoScheduleStatus(ctx, released)
}

func (s *Store) OpenVideoScheduleForJob(
	ctx context.Context,
	expectedJobID string,
) (VideoScheduleStatusRow, error) {
	parsedJobID, err := uuid.Parse(expectedJobID)
	if err != nil || parsedJobID.String() != expectedJobID {
		return VideoScheduleStatusRow{}, ErrVideoScheduleGuardMismatch
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return VideoScheduleStatusRow{}, err
	}
	defer tx.Rollback(ctx)

	var scheduleState string
	if err := tx.QueryRow(ctx, `
		SELECT state
		FROM runtime_schedules
		WHERE service_name = $1
		FOR UPDATE
	`, VideoScheduleServiceName).Scan(&scheduleState); err != nil {
		return VideoScheduleStatusRow{}, err
	}
	if scheduleState != "CLOSED" {
		return VideoScheduleStatusRow{}, ErrVideoScheduleGuardMismatch
	}

	rows, err := tx.Query(ctx, `
		SELECT id::text, status::text, orchestrator_owner
		FROM jobs
		WHERE status IN ('WAITING_WINDOW', 'PENDING', 'VALIDATING', 'PLANNING', 'RUNNING')
		ORDER BY id
		FOR UPDATE
	`)
	if err != nil {
		return VideoScheduleStatusRow{}, err
	}
	type guardedJob struct {
		id     string
		status string
		owner  string
	}
	jobs := make([]guardedJob, 0, 2)
	for rows.Next() {
		var job guardedJob
		if err := rows.Scan(&job.id, &job.status, &job.owner); err != nil {
			rows.Close()
			return VideoScheduleStatusRow{}, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return VideoScheduleStatusRow{}, err
	}
	rows.Close()

	nodeRows, err := tx.Query(ctx, `
		SELECT id::text
		FROM node_executions
		WHERE status IN ('QUEUED', 'RUNNING')
		ORDER BY id
		FOR UPDATE
	`)
	if err != nil {
		return VideoScheduleStatusRow{}, err
	}
	activeNodeCount := 0
	for nodeRows.Next() {
		var nodeID string
		if err := nodeRows.Scan(&nodeID); err != nil {
			nodeRows.Close()
			return VideoScheduleStatusRow{}, err
		}
		activeNodeCount++
	}
	if err := nodeRows.Err(); err != nil {
		nodeRows.Close()
		return VideoScheduleStatusRow{}, err
	}
	nodeRows.Close()

	if len(jobs) != 1 || jobs[0].id != expectedJobID || jobs[0].status != "WAITING_WINDOW" ||
		jobs[0].owner != "go" || activeNodeCount != 0 {
		return VideoScheduleStatusRow{}, ErrVideoScheduleGuardMismatch
	}
	if _, err := tx.Exec(ctx, `
		UPDATE runtime_schedules
		SET state = 'OPEN',
		    guarded_job_id = $2::uuid,
		    updated_by = 'go_api_guarded',
		    updated_at = NOW()
		WHERE service_name = $1
	`, VideoScheduleServiceName, expectedJobID); err != nil {
		return VideoScheduleStatusRow{}, err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE jobs
		SET status = 'PENDING', error_message = NULL, completed_at = NULL
		WHERE id = $1::uuid
		  AND status = 'WAITING_WINDOW'
		  AND orchestrator_owner = 'go'
	`, expectedJobID)
	if err != nil {
		return VideoScheduleStatusRow{}, err
	}
	if tag.RowsAffected() != 1 {
		return VideoScheduleStatusRow{}, ErrVideoScheduleGuardMismatch
	}
	if err := tx.Commit(ctx); err != nil {
		return VideoScheduleStatusRow{}, err
	}
	return s.getVideoScheduleStatus(ctx, 1)
}

func (s *Store) getVideoScheduleStatus(ctx context.Context, releasedJobs int) (VideoScheduleStatusRow, error) {
	var row VideoScheduleStatusRow
	err := s.Pool.QueryRow(ctx, `
		SELECT service_name, state, guarded_job_id::text, updated_at, updated_by
		FROM runtime_schedules
		WHERE service_name = $1
	`, VideoScheduleServiceName).Scan(
		&row.ServiceName,
		&row.State,
		&row.GuardedJobID,
		&row.UpdatedAt,
		&row.UpdatedBy,
	)
	if err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM jobs WHERE status = 'WAITING_WINDOW'
	`).Scan(&row.WaitingJobs); err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM jobs WHERE status IN ('PENDING', 'VALIDATING', 'PLANNING', 'RUNNING')
	`).Scan(&row.ActiveJobs); err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM node_executions WHERE status = 'QUEUED'
	`).Scan(&row.QueuedNodes); err != nil {
		return row, err
	}
	if err := s.Pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM node_executions WHERE status = 'RUNNING'
	`).Scan(&row.RunningNodes); err != nil {
		return row, err
	}
	row.ReleasedJobs = releasedJobs
	return row, nil
}
