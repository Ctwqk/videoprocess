package channelops

import (
	"context"
	"time"
)

type Runner struct {
	Config Config
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	return &Runner{Config: cfg}, nil
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

func (r *Runner) Close() {}
