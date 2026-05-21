package channelops

import (
	"context"
	"net/http"
	"time"
)

type Runner struct {
	Config           Config
	Store            *Store
	Scheduler        Scheduler
	Handlers         HandlerService
	lastSchedulerRun time.Time
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	st, err := OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	st.DefaultMaxAttempts = cfg.MaxQueueAttempts
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
	runner.Handlers = newRunnerHandlerService(st, cfg, pds)
	return runner, nil
}

func newRunnerHandlerService(st *Store, cfg Config, pdsOverride ...PDSDecider) HandlerService {
	var pds PDSDecider = PDSClient{
		Enabled:     cfg.PDSEnabled,
		DevAllowAll: cfg.DevAllowAllPDS,
		BaseURL:     cfg.PDSBaseURL,
		ClientID:    cfg.PDSClientID,
		Timeout:     cfg.PDSTimeout,
		HTTPClient:  &http.Client{Timeout: cfg.PDSTimeout},
	}
	if len(pdsOverride) > 0 && pdsOverride[0] != nil {
		pds = pdsOverride[0]
	}
	youtube := YouTubeManagerClient{BaseURL: cfg.YouTubeManagerURL, Timeout: 20 * time.Second}
	autoflow := HTTPAutoFlowClient{BaseURL: cfg.AutoFlowBaseURL, Timeout: cfg.AutoFlowTimeout}
	return HandlerService{Store: st, PDS: pds, AutoFlow: autoflow, YouTube: youtube, Config: cfg}
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
	now := r.Store.Now()
	if r.Scheduler.Store != nil && ShouldRunScheduler(r.lastSchedulerRun, now, r.Config.SchedulerPollSeconds) {
		_, _ = r.Scheduler.RunOnce(ctx, now)
		r.lastSchedulerRun = now
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

func ShouldRunScheduler(lastRun time.Time, now time.Time, pollSeconds int) bool {
	if lastRun.IsZero() {
		return true
	}
	if pollSeconds <= 0 {
		pollSeconds = 60
	}
	return !now.Before(lastRun.Add(time.Duration(pollSeconds) * time.Second))
}

func (r *Runner) Close() {
	if r.Store != nil {
		r.Store.Close()
	}
}
