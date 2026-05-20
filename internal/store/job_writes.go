package store

import (
	"context"
	"fmt"
)

var terminalJobStatuses = map[string]struct{}{
	"SUCCEEDED":        {},
	"FAILED":           {},
	"CANCELLED":        {},
	"PARTIALLY_FAILED": {},
}

func (s *Store) CancelJob(ctx context.Context, id string) (JobDetailRow, error) {
	var status string
	if err := s.Pool.QueryRow(ctx, "SELECT status::text FROM jobs WHERE id = $1", id).Scan(&status); err != nil {
		return JobDetailRow{}, err
	}
	if _, terminal := terminalJobStatuses[status]; !terminal {
		if _, err := s.Pool.Exec(ctx, `
			UPDATE jobs
			SET status = 'CANCELLED', completed_at = COALESCE(completed_at, NOW())
			WHERE id = $1
		`, id); err != nil {
			return JobDetailRow{}, err
		}
		if _, err := s.Pool.Exec(ctx, `
			UPDATE node_executions
			SET status = 'CANCELLED', completed_at = COALESCE(completed_at, NOW())
			WHERE job_id = $1 AND status IN ('PENDING', 'QUEUED', 'RUNNING')
		`, id); err != nil {
			return JobDetailRow{}, err
		}
	}
	return s.GetJobDetail(ctx, id)
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	var status string
	if err := s.Pool.QueryRow(ctx, "SELECT status::text FROM jobs WHERE id = $1", id).Scan(&status); err != nil {
		return err
	}
	if _, terminal := terminalJobStatuses[status]; !terminal {
		return fmt.Errorf("%w: only terminal jobs can be deleted", ErrConflict)
	}
	tag, err := s.Pool.Exec(ctx, "DELETE FROM jobs WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("job not found")
	}
	return nil
}

func (s *Store) GetJobForRerun(ctx context.Context, id string) (JobDetailRow, error) {
	return s.GetJobDetail(ctx, id)
}
