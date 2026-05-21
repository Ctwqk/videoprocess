package channelops

import (
	"context"
	"time"
)

type Runner struct {
	Config Config
	Store  *Store
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	st, err := OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	return &Runner{Config: cfg, Store: st}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(r.Config.RunnerPollSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Queue and scheduler work is wired in later tasks.
		}
	}
}

func (r *Runner) Close() {
	if r.Store != nil {
		r.Store.Close()
	}
}
