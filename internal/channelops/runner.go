package channelops

import (
	"context"
	"fmt"
	"time"
)

type Runner struct {
	Config         Config
	Store          *Store
	Scheduler      Scheduler
	ClaimableKinds []string
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	st, err := OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	runner := &Runner{Config: cfg, Store: st, ClaimableKinds: []string{}}
	runner.Scheduler = Scheduler{Store: st}
	return runner, nil
}

func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(r.Config.RunnerPollSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.runOnce(ctx); err != nil {
				return err
			}
		}
	}
}

func (r *Runner) runOnce(ctx context.Context) error {
	if r.Store == nil {
		return nil
	}
	if r.Scheduler.Store != nil {
		_, _ = r.Scheduler.RunOnce(ctx, r.Store.Now())
	}
	if len(r.ClaimableKinds) == 0 {
		return nil
	}
	item, err := r.Store.ClaimNextForKinds(ctx, "channelops-go-runner", r.ClaimableKinds)
	if err != nil {
		return err
	}
	if item == nil {
		return nil
	}
	return r.Store.MarkQueueFailedOrRetry(ctx, *item, fmt.Sprintf("handler not registered yet: %s", item.Kind))
}

func (r *Runner) Close() {
	if r.Store != nil {
		r.Store.Close()
	}
}
