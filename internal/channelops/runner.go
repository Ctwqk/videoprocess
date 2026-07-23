package channelops

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

const (
	LeaderRoleActive      = "active"
	LeaderRoleStandby     = "standby"
	LeaderRoleUnavailable = "unavailable"

	runnerLeadershipCloseTimeout = 5 * time.Second
	runnerClaimReleaseTimeout    = 5 * time.Second
)

type LeaderStatus struct {
	Role      string
	Authority *LeaderAuthority
	Err       error
}

type LeadershipController interface {
	EnsureActive(context.Context, time.Time) (*LeaderAuthority, error)
	Status() LeaderStatus
	Close(context.Context, time.Time) error
}

type leadershipOperation struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type postgresLeadershipController struct {
	operation   chan struct{}
	lifecycleMu sync.Mutex
	inFlight    *leadershipOperation
	closing     bool
	closed      bool
	statusMu    sync.RWMutex
	status      LeaderStatus
	store       *Store
	holderID    string
	lease       *LeaderLease
}

func newPostgresLeadershipController(store *Store, holderID string) LeadershipController {
	controller := &postgresLeadershipController{
		operation: make(chan struct{}, 1),
		status:    LeaderStatus{Role: LeaderRoleStandby},
		store:     store,
		holderID:  holderID,
	}
	controller.operation <- struct{}{}
	return controller
}

func (c *postgresLeadershipController) EnsureActive(
	ctx context.Context,
	now time.Time,
) (*LeaderAuthority, error) {
	if c == nil {
		return nil, ErrLeaderAuthorityUnavailable
	}
	if err := c.acquireOperation(ctx); err != nil {
		return nil, err
	}
	operationCtx, operation, err := c.beginOperation(ctx)
	if err != nil {
		c.releaseOperation()
		return nil, err
	}
	defer c.finishOperation(operation)

	if c.lease != nil {
		if err := c.lease.Heartbeat(operationCtx, now); err != nil {
			c.lease = nil
			if errors.Is(err, ErrLeaderAuthorityLost) {
				c.publishStatus(LeaderStatus{Role: LeaderRoleStandby})
				return nil, nil
			}
			c.publishStatus(LeaderStatus{Role: LeaderRoleUnavailable, Err: err})
			return nil, err
		}
		authority := c.lease.Authority()
		c.publishStatus(LeaderStatus{
			Role:      LeaderRoleActive,
			Authority: cloneLeaderAuthority(authority),
		})
		return cloneLeaderAuthority(authority), nil
	}

	lease, acquired, err := c.store.TryAcquireLeader(operationCtx, c.holderID, now)
	if err != nil {
		c.publishStatus(LeaderStatus{Role: LeaderRoleUnavailable, Err: err})
		return nil, err
	}
	if !acquired {
		c.publishStatus(LeaderStatus{Role: LeaderRoleStandby})
		return nil, nil
	}

	c.lease = lease
	authority := lease.Authority()
	c.publishStatus(LeaderStatus{
		Role:      LeaderRoleActive,
		Authority: cloneLeaderAuthority(authority),
	})
	return cloneLeaderAuthority(authority), nil
}

func (c *postgresLeadershipController) Status() LeaderStatus {
	if c == nil {
		return LeaderStatus{
			Role: LeaderRoleUnavailable,
			Err:  ErrLeaderAuthorityUnavailable,
		}
	}
	c.statusMu.RLock()
	status := c.status
	status.Authority = cloneLeaderAuthorityPointer(status.Authority)
	c.statusMu.RUnlock()
	if status.Role == LeaderRoleActive && status.Authority != nil {
		configured, published := c.store.leadership.snapshot()
		if !configured || !sameLeaderAuthority(published, *status.Authority) {
			return LeaderStatus{Role: LeaderRoleStandby}
		}
	}
	return status
}

func (c *postgresLeadershipController) Close(ctx context.Context, now time.Time) error {
	if c == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	c.lifecycleMu.Lock()
	if c.closed {
		c.lifecycleMu.Unlock()
		return nil
	}
	c.closing = true
	operation := c.inFlight
	if operation != nil {
		operation.cancel()
	}
	c.lifecycleMu.Unlock()

	if operation != nil {
		select {
		case <-operation.done:
		case <-ctx.Done():
			c.publishStatus(LeaderStatus{Role: LeaderRoleUnavailable, Err: ctx.Err()})
			return ctx.Err()
		}
	}
	if err := c.acquireOperation(ctx); err != nil {
		c.publishStatus(LeaderStatus{Role: LeaderRoleUnavailable, Err: err})
		return err
	}
	defer c.releaseOperation()

	c.lifecycleMu.Lock()
	if c.closed {
		c.lifecycleMu.Unlock()
		return nil
	}
	c.closed = true
	lease := c.lease
	c.lease = nil
	c.lifecycleMu.Unlock()
	if lease == nil {
		c.publishStatus(LeaderStatus{Role: LeaderRoleStandby})
		return nil
	}

	err := lease.Release(ctx, now)
	if err == nil || errors.Is(err, ErrLeaderAuthorityLost) {
		c.publishStatus(LeaderStatus{Role: LeaderRoleStandby})
		return err
	}
	c.publishStatus(LeaderStatus{Role: LeaderRoleUnavailable, Err: err})
	return err
}

func (c *postgresLeadershipController) beginOperation(
	ctx context.Context,
) (context.Context, *leadershipOperation, error) {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.closing || c.closed {
		return nil, nil, ErrLeaderAuthorityUnavailable
	}
	operationCtx, cancel := context.WithCancel(ctx)
	operation := &leadershipOperation{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	c.inFlight = operation
	return operationCtx, operation, nil
}

func (c *postgresLeadershipController) finishOperation(operation *leadershipOperation) {
	operation.cancel()
	c.lifecycleMu.Lock()
	if c.inFlight == operation {
		c.inFlight = nil
	}
	close(operation.done)
	c.lifecycleMu.Unlock()
	c.releaseOperation()
}

func (c *postgresLeadershipController) acquireOperation(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.operation:
		if err := ctx.Err(); err != nil {
			c.operation <- struct{}{}
			return err
		}
		return nil
	}
}

func (c *postgresLeadershipController) releaseOperation() {
	c.operation <- struct{}{}
}

func (c *postgresLeadershipController) publishStatus(status LeaderStatus) {
	status.Authority = cloneLeaderAuthorityPointer(status.Authority)
	c.statusMu.Lock()
	c.status = status
	c.statusMu.Unlock()
}

type Runner struct {
	Config                 Config
	Store                  *Store
	Scheduler              Scheduler
	Handlers               HandlerService
	Leadership             LeadershipController
	schedulerRun           func(context.Context, Scheduler, time.Time) (int, error)
	recomputeLearning      func(context.Context, *Store, string, int) error
	schedulerMu            sync.RWMutex
	lastSchedulerRun       time.Time
	leadershipCloseTimeout time.Duration
	closeStore             func()
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
	runner := &Runner{
		Config:     cfg,
		Store:      st,
		Leadership: newPostgresLeadershipController(st, cfg.RunnerID),
	}
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
	now := r.now()
	var authority *LeaderAuthority
	if r.Leadership != nil {
		var err error
		authority, err = r.Leadership.EnsureActive(ctx, now)
		if err != nil {
			if errors.Is(err, ErrLeaderAuthorityLost) {
				return nil
			}
			return err
		}
		if authority == nil {
			return nil
		}
	}
	if r.Store == nil {
		return nil
	}
	if r.Leadership == nil {
		return r.runLegacyOnce(ctx, now)
	}
	if authority.Epoch <= 0 || authority.HolderID == "" {
		return fmt.Errorf("%w: invalid active leader authority", ErrLeaderAuthorityUnavailable)
	}

	if r.Scheduler.Store != nil && ShouldRunScheduler(r.LastSchedulerRun(), now, r.Config.EffectiveSchedulerPollSeconds(now)) {
		err := r.Store.WithLeaderExecutionFence(ctx, func(fencedStore *Store) error {
			scheduler := r.Scheduler
			scheduler.Store = fencedStore
			_, err := r.executeScheduler(ctx, scheduler, now)
			return err
		})
		if leaderFenceRejected(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.SetLastSchedulerRun(now)
	}
	if err := r.Handlers.ReadinessError(); err != nil {
		return err
	}
	claimableKinds := r.Handlers.ClaimableKinds()
	if len(claimableKinds) == 0 {
		return nil
	}
	err := r.Store.WithLeaderExecutionFence(ctx, func(fencedStore *Store) error {
		_, err := fencedStore.recoverStaleDiscoveryLeases(ctx, now)
		return err
	})
	if leaderFenceRejected(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var item *QueueItemRow
	workerID := fmt.Sprintf("%s:epoch:%d", authority.HolderID, authority.Epoch)
	err = r.Store.WithLeaderExecutionFence(ctx, func(fencedStore *Store) error {
		var claimErr error
		item, claimErr = fencedStore.ClaimNextForKinds(ctx, workerID, claimableKinds)
		return claimErr
	})
	if leaderFenceRejected(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if item == nil {
		return nil
	}
	confirmedAuthority, authorityErr := r.Leadership.EnsureActive(ctx, r.now())
	if authorityErr != nil ||
		confirmedAuthority == nil ||
		confirmedAuthority.HolderID != authority.HolderID ||
		confirmedAuthority.Epoch != authority.Epoch {
		return r.releaseClaimAfterAuthorityChange(ctx, *item)
	}
	if err := r.Handlers.Handle(ctx, *item); err != nil {
		if leaderFenceRejected(err) {
			return r.releaseClaimAfterAuthorityChange(ctx, *item)
		}
		if errors.Is(err, ErrQueueAuthorityInvalid) {
			return r.completeQueueClaimWithAuthority(ctx, *item, func(fenced *Store) error {
				return fenced.MarkQueueRejected(ctx, *item, err.Error())
			})
		}
		return r.completeQueueClaimWithAuthority(ctx, *item, func(fenced *Store) error {
			return fenced.MarkQueueFailedOrRetry(ctx, *item, err.Error())
		})
	}
	return r.completeQueueClaimWithAuthority(ctx, *item, func(fenced *Store) error {
		return fenced.MarkQueueDone(ctx, *item)
	})
}

func (r *Runner) executeScheduler(
	ctx context.Context,
	scheduler Scheduler,
	now time.Time,
) (int, error) {
	if r.schedulerRun != nil {
		return r.schedulerRun(ctx, scheduler, now)
	}
	return scheduler.RunOnce(ctx, now)
}

func (r *Runner) releaseClaimAfterAuthorityChange(ctx context.Context, item QueueItemRow) error {
	releaseCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		runnerClaimReleaseTimeout,
	)
	defer cancel()
	err := r.Store.ReleaseQueueClaim(releaseCtx, item)
	if errors.Is(err, ErrQueueLeaseLost) {
		return nil
	}
	return err
}

func (r *Runner) completeQueueClaimWithAuthority(
	ctx context.Context,
	item QueueItemRow,
	complete func(*Store) error,
) error {
	err := r.Store.WithLeaderExecutionFence(ctx, complete)
	if leaderFenceRejected(err) {
		return r.releaseClaimAfterAuthorityChange(ctx, item)
	}
	return err
}

func (r *Runner) runLegacyOnce(ctx context.Context, now time.Time) error {
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
	if _, err := r.Store.recoverStaleDiscoveryLeases(ctx, now); err != nil {
		return err
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

func leaderFenceRejected(err error) bool {
	return errors.Is(err, ErrLeaderAuthorityLost) ||
		errors.Is(err, ErrLeaderAuthorityUnavailable)
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

func (r *Runner) Close() error {
	if r == nil {
		return nil
	}
	now := r.now()
	if r.Leadership != nil {
		timeout := r.leadershipCloseTimeout
		if timeout <= 0 {
			timeout = runnerLeadershipCloseTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := r.Leadership.Close(ctx, now)
		cancel()
		if err != nil {
			return fmt.Errorf("close ChannelOps leadership: %w", err)
		}
	}
	if r.closeStore != nil {
		r.closeStore()
	} else if r.Store != nil {
		r.Store.Close()
	}
	return nil
}
