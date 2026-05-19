package store

import (
	"context"
	"time"
)

const VideoScheduleServiceName = "videoprocess"

type VideoScheduleStatusRow struct {
	ServiceName  string     `json:"service_name"`
	State        string     `json:"state"`
	WaitingJobs  int        `json:"waiting_jobs"`
	ActiveJobs   int        `json:"active_jobs"`
	QueuedNodes  int        `json:"queued_nodes"`
	RunningNodes int        `json:"running_nodes"`
	UpdatedAt    *time.Time `json:"updated_at"`
	UpdatedBy    *string    `json:"updated_by"`
	ReleasedJobs int        `json:"released_jobs"`
}

func (s *Store) GetVideoScheduleStatus(ctx context.Context) (VideoScheduleStatusRow, error) {
	var row VideoScheduleStatusRow
	err := s.Pool.QueryRow(ctx, `
		SELECT service_name, state, updated_at, updated_by
		FROM runtime_schedules
		WHERE service_name = $1
	`, VideoScheduleServiceName).Scan(&row.ServiceName, &row.State, &row.UpdatedAt, &row.UpdatedBy)
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
	row.ReleasedJobs = 0
	return row, nil
}
