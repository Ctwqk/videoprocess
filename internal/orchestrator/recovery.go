package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

const defaultGoRecoveryStaleNodeAge = 10 * time.Minute

type RecoveryStore interface {
	ListRecoverableGoJobs(ctx context.Context) ([]JobView, error)
	ResetStaleGoNodes(ctx context.Context, jobID string, staleBefore time.Time) error
}

type JobStarter interface {
	StartJob(ctx context.Context, jobID string) error
}

type RecoveryRunner struct {
	Store        RecoveryStore
	Engine       JobStarter
	Interval     time.Duration
	StaleNodeAge time.Duration
	Clock        func() time.Time
	Logger       *slog.Logger
}

func (r *RecoveryRunner) RunOnce(ctx context.Context) error {
	if r.Store == nil {
		return errors.New("recovery store is nil")
	}
	if r.Engine == nil {
		return errors.New("recovery engine is nil")
	}
	jobs, err := r.Store.ListRecoverableGoJobs(ctx)
	if err != nil {
		observeGoOrchestratorRecovery("error")
		return err
	}
	staleBefore := r.now().Add(-r.staleNodeAge())
	for _, job := range jobs {
		if job.OrchestratorOwner != goOrchestratorOwner {
			observeGoOrchestratorRecovery("skipped")
			continue
		}
		if err := r.Store.ResetStaleGoNodes(ctx, job.ID, staleBefore); err != nil {
			observeGoOrchestratorRecovery("error")
			return fmt.Errorf("reset stale Go nodes for job %s: %w", job.ID, err)
		}
		if err := r.Engine.StartJob(ctx, job.ID); err != nil {
			observeGoOrchestratorRecovery("error")
			return fmt.Errorf("start recovered Go job %s: %w", job.ID, err)
		}
		observeGoOrchestratorRecovery("started")
	}
	return nil
}

func (r *RecoveryRunner) Run(ctx context.Context) error {
	if err := r.RunOnce(ctx); err != nil {
		r.logger().Warn("initial Go orchestrator recovery failed", "error", err)
	}
	ticker := time.NewTicker(r.interval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.RunOnce(ctx); err != nil {
				r.logger().Warn("periodic Go orchestrator recovery failed", "error", err)
			}
		}
	}
}

func (r *RecoveryRunner) interval() time.Duration {
	if r.Interval > 0 {
		return r.Interval
	}
	return time.Minute
}

func (r *RecoveryRunner) staleNodeAge() time.Duration {
	if r.StaleNodeAge > 0 {
		return r.StaleNodeAge
	}
	return defaultGoRecoveryStaleNodeAge
}

func (r *RecoveryRunner) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

func (r *RecoveryRunner) logger() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}
