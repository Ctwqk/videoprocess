package channelops

import (
	"context"
	"net/http"
	"time"
)

type Runner struct {
	Config    Config
	Store     *Store
	Scheduler Scheduler
	Handlers  HandlerService
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	st, err := OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	pds := PDSClient{
		Enabled:     cfg.PDSEnabled,
		DevAllowAll: cfg.DevAllowAllPDS,
		BaseURL:     cfg.PDSBaseURL,
		ClientID:    cfg.PDSClientID,
		Timeout:     cfg.PDSTimeout,
		HTTPClient:  &http.Client{Timeout: cfg.PDSTimeout},
	}
	runner := &Runner{Config: cfg, Store: st}
	runner.Scheduler = Scheduler{Store: st}
	runner.Handlers = HandlerService{Store: st, PDS: pds, Config: cfg}
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
	if err := r.Handlers.ReadinessError(); err != nil {
		return err
	}
	claimableKinds := r.Handlers.ClaimableKinds()
	if len(claimableKinds) == 0 {
		return nil
	}
	item, err := r.Store.ClaimNextForKinds(ctx, "channelops-go-runner", claimableKinds)
	if err != nil {
		return err
	}
	if item == nil {
		return nil
	}
	if err := r.Handlers.Handle(ctx, *item); err != nil {
		return r.Store.MarkQueueFailedOrRetry(ctx, *item, err.Error())
	}
	return r.Store.MarkQueueDone(ctx, item.ID)
}

func (r *Runner) Close() {
	if r.Store != nil {
		r.Store.Close()
	}
}
