package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

var ErrUnhealthy = errors.New("channelops runner unhealthy")

const runnerReadinessLeadershipTimeout = 2 * time.Second

type HealthStatus struct {
	Status            string            `json:"status"`
	DB                string            `json:"db"`
	LastSchedulerRun  *time.Time        `json:"last_scheduler_run,omitempty"`
	LeaderRole        string            `json:"leader_role"`
	LeaderEpoch       *int64            `json:"leader_epoch,omitempty"`
	LeaderHolderID    string            `json:"leader_holder_id,omitempty"`
	LeaderHeartbeatAt *time.Time        `json:"leader_heartbeat_at,omitempty"`
	Errors            map[string]string `json:"errors,omitempty"`
}

func (s HealthStatus) Err() error {
	if s.Status == "ok" {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrUnhealthy, s.Errors)
}

type HealthChecker interface {
	HealthCheck(ctx context.Context) HealthStatus
}

type readinessChecker interface {
	ReadyCheck(ctx context.Context) HealthStatus
}

func (r *Runner) HealthCheck(ctx context.Context) HealthStatus {
	status := r.ReadyCheck(ctx)
	now := time.Now().UTC()
	if r != nil && r.Store != nil && r.Store.Now != nil {
		now = r.Store.Now().UTC()
	}
	lastRun := r.LastSchedulerRun()
	if !lastRun.IsZero() {
		last := lastRun.UTC()
		status.LastSchedulerRun = &last
	}
	allowedStaleness := 2 * time.Duration(r.Config.EffectiveSchedulerPollSeconds(now)) * time.Second
	if allowedStaleness <= 0 {
		allowedStaleness = 2 * time.Minute
	}
	switch {
	case lastRun.IsZero():
		addHealthError(&status, "scheduler", "scheduler has not run")
	case now.Sub(lastRun) > allowedStaleness:
		addHealthError(&status, "scheduler", fmt.Sprintf("scheduler stale: last run %s", lastRun.UTC().Format(time.RFC3339)))
	}
	finalizeHealthStatus(&status)
	return status
}

func (r *Runner) ReadyCheck(ctx context.Context) HealthStatus {
	status := HealthStatus{
		Status:     "ok",
		DB:         "ok",
		LeaderRole: LeaderRoleUnavailable,
	}
	if r == nil || r.Store == nil || r.Store.Pool == nil {
		status.DB = "unconfigured"
		addHealthError(&status, "db", "store pool is not configured")
	} else if err := r.Store.Pool.Ping(ctx); err != nil {
		status.DB = "error"
		addHealthError(&status, "db", err.Error())
	}
	if r != nil && r.Leadership != nil {
		applyLeadershipHealth(&status, r.validatedReadinessLeadership(ctx))
	}
	finalizeHealthStatus(&status)
	return status
}

func (r *Runner) validatedReadinessLeadership(ctx context.Context) LeaderStatus {
	validationCtx, cancel := context.WithTimeout(ctx, runnerReadinessLeadershipTimeout)
	defer cancel()
	authority, err := r.Leadership.EnsureActive(validationCtx, r.now())
	if err != nil {
		return LeaderStatus{Role: LeaderRoleUnavailable, Err: err}
	}
	if authority == nil {
		return LeaderStatus{Role: LeaderRoleStandby}
	}
	return LeaderStatus{
		Role:      LeaderRoleActive,
		Authority: cloneLeaderAuthorityPointer(authority),
	}
}

func applyLeadershipHealth(status *HealthStatus, leadership LeaderStatus) {
	switch leadership.Role {
	case LeaderRoleActive:
		status.LeaderRole = LeaderRoleActive
		if leadership.Authority == nil {
			addHealthError(status, "leadership", "active leader authority is unavailable")
			return
		}
		epoch := leadership.Authority.Epoch
		heartbeat := leadership.Authority.HeartbeatAt.UTC()
		status.LeaderEpoch = &epoch
		status.LeaderHolderID = leadership.Authority.HolderID
		status.LeaderHeartbeatAt = &heartbeat
	case LeaderRoleStandby:
		status.LeaderRole = LeaderRoleStandby
		addHealthError(status, "leadership", "leader is standby")
	case LeaderRoleUnavailable:
		status.LeaderRole = LeaderRoleUnavailable
		message := ErrLeaderAuthorityUnavailable.Error()
		if leadership.Err != nil {
			message = leadership.Err.Error()
		}
		addHealthError(status, "leadership", message)
	default:
		status.LeaderRole = LeaderRoleUnavailable
		addHealthError(status, "leadership", "leader status is unavailable")
	}
}

func NewHealthHandler(checker HealthChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		writeHealthStatus(w, checker.HealthCheck(r.Context()))
	})
}

func NewReadyHandler(checker HealthChecker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if ready, ok := checker.(readinessChecker); ok {
			writeHealthStatus(w, ready.ReadyCheck(r.Context()))
			return
		}
		writeHealthStatus(w, checker.HealthCheck(r.Context()))
	})
}

func NewRunnerHTTPHandler(runner *Runner) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", NewHealthHandler(runner))
	mux.Handle("/readyz", NewReadyHandler(runner))
	mux.HandleFunc("/internal/learning/recompute", runner.handleLearningRecomputeHTTP)
	return mux
}

func (r *Runner) handleLearningRecomputeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	channelID := req.URL.Query().Get("channel_id")
	if channelID == "" {
		writeJSONError(w, http.StatusBadRequest, "channel_id is required")
		return
	}
	windowDays := 7
	if raw := req.URL.Query().Get("window_days"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeJSONError(w, http.StatusBadRequest, "window_days must be a positive integer")
			return
		}
		windowDays = parsed
	}
	if err := r.RecomputeLearning(req.Context(), channelID, windowDays); err != nil {
		if leaderFenceRejected(err) {
			writeJSONError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"channel_id": channelID, "window_days": windowDays, "recomputed": true})
}

func (r *Runner) RecomputeLearning(ctx context.Context, channelID string, windowDays int) error {
	if r == nil || r.Store == nil {
		return errors.New("channelops runner store is not configured")
	}
	if r.Leadership == nil {
		return r.Store.RecomputeLearningState(ctx, channelID, windowDays)
	}
	authority, err := r.Leadership.EnsureActive(ctx, r.now())
	if err != nil {
		if errors.Is(err, ErrLeaderAuthorityLost) {
			return err
		}
		return errors.Join(ErrLeaderAuthorityUnavailable, err)
	}
	if authority == nil {
		return ErrLeaderAuthorityUnavailable
	}
	return r.Store.WithLeaderExecutionFence(ctx, func(fencedStore *Store) error {
		if r.recomputeLearning != nil {
			return r.recomputeLearning(ctx, fencedStore, channelID, windowDays)
		}
		return fencedStore.RecomputeLearningState(ctx, channelID, windowDays)
	})
}

func RunHTTPServer(ctx context.Context, addr string, handler http.Handler) error {
	server := &http.Server{Addr: addr, Handler: handler}
	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errCh
	case err := <-errCh:
		return err
	}
}

func writeHealthStatus(w http.ResponseWriter, status HealthStatus) {
	code := http.StatusOK
	if status.Err() != nil {
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, status)
}

func writeJSONError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]any{"detail": message})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func addHealthError(status *HealthStatus, key string, message string) {
	if status.Errors == nil {
		status.Errors = map[string]string{}
	}
	status.Errors[key] = message
	status.Status = "unhealthy"
}

func finalizeHealthStatus(status *HealthStatus) {
	if len(status.Errors) == 0 {
		status.Status = "ok"
		status.Errors = nil
		if status.DB == "" {
			status.DB = "ok"
		}
		return
	}
	status.Status = "unhealthy"
}
