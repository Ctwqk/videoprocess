package channelops

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

type Runner struct {
	Config           Config
	Store            *Store
	Scheduler        Scheduler
	Handlers         HandlerService
	schedulerMu      sync.RWMutex
	lastSchedulerRun time.Time
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
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
	var discovery DiscoveryClient
	if discoveryConfigValid(cfg) {
		discovery = HTTPDiscoveryClient{
			BaseURL:    cfg.AutoFlowBaseURL,
			Timeout:    cfg.DiscoveryTimeout,
			HTTPClient: &http.Client{Timeout: cfg.DiscoveryTimeout},
		}
	}
	return HandlerService{
		Store: st, PDS: pds, AutoFlow: autoflow, YouTube: youtube, Discovery: discovery,
		Alerts: NewAlertSink(cfg), Config: cfg,
	}
}

func discoveryConfigValid(cfg Config) bool {
	if cfg.discoveryTimeoutParseFailed || !validDiscoveryTimeout(cfg.DiscoveryTimeout) {
		return false
	}
	_, err := discoveryEndpoint(cfg.AutoFlowBaseURL)
	return err == nil
}

func (r *Runner) Run(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.runOnce(ctx); err != nil && !errors.Is(err, ErrQueueLeaseLost) {
		return err
	}
	for {
		timer := time.NewTimer(time.Duration(r.Config.EffectiveRunnerPollSeconds(r.now())) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			if err := r.runOnce(ctx); err != nil && !errors.Is(err, ErrQueueLeaseLost) {
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
	if r.Scheduler.Store != nil && ShouldRunScheduler(r.LastSchedulerRun(), now, r.Config.EffectiveSchedulerPollSeconds(now)) {
		_, _ = r.Scheduler.RunOnce(ctx, now)
		r.SetLastSchedulerRun(now)
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
		if errors.Is(err, ErrQueueAuthorityInvalid) {
			return r.Store.MarkQueueRejected(ctx, *item, err.Error())
		}
		return r.Store.MarkQueueFailedOrRetry(ctx, *item, err.Error())
	}
	return r.Store.MarkQueueDone(ctx, *item)
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

func (r *Runner) now() time.Time {
	if r.Store != nil && r.Store.Now != nil {
		return r.Store.Now()
	}
	return time.Now().UTC()
}

func (r *Runner) LastSchedulerRun() time.Time {
	r.schedulerMu.RLock()
	defer r.schedulerMu.RUnlock()
	return r.lastSchedulerRun
}

func (r *Runner) SetLastSchedulerRun(value time.Time) {
	r.schedulerMu.Lock()
	defer r.schedulerMu.Unlock()
	r.lastSchedulerRun = value.UTC()
}

func (r *Runner) Close() {
	if r.Store != nil {
		r.Store.Close()
	}
}
