package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunnerHealthCheckRejectsStaleSchedulerRun(t *testing.T) {
	now := time.Date(2026, 5, 21, 18, 2, 1, 0, time.UTC)
	runner := &Runner{
		Config: Config{SchedulerPollSeconds: 60},
		Store:  &Store{Now: func() time.Time { return now }},
	}
	runner.SetLastSchedulerRun(now.Add(-3 * time.Minute))

	status := runner.HealthCheck(context.Background())

	if status.Status != "unhealthy" {
		t.Fatalf("status = %q, want unhealthy", status.Status)
	}
	if status.LastSchedulerRun == nil || !strings.Contains(status.Errors["scheduler"], "stale") {
		t.Fatalf("health status = %#v", status)
	}
}

func TestRunnerHealthReportsActiveLeaderRoleAndEpoch(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	heartbeat := time.Date(2026, 7, 23, 18, 2, 3, 0, time.UTC)
	authority := &LeaderAuthority{
		ServiceName: leaderServiceName,
		HolderID:    "channelops-go@colima-127:1",
		Epoch:       42,
		AcquiredAt:  heartbeat.Add(-time.Minute),
		HeartbeatAt: heartbeat,
	}
	runner := &Runner{
		Store: fixture.Store,
		Leadership: &fakeLeadershipController{
			authority: authority,
			status:    LeaderStatus{Role: LeaderRoleActive, Authority: authority},
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()

	NewReadyHandler(runner).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	var payload HealthStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode active health response: %v", err)
	}
	if payload.LeaderRole != LeaderRoleActive ||
		payload.LeaderEpoch == nil || *payload.LeaderEpoch != authority.Epoch ||
		payload.LeaderHolderID != authority.HolderID ||
		payload.LeaderHeartbeatAt == nil || !payload.LeaderHeartbeatAt.Equal(heartbeat) {
		t.Fatalf("active leader health = %#v", payload)
	}
}

func TestRunnerReadyRejectsStandbyAndUnavailableLeaderRoles(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, tt := range []struct {
		name   string
		status LeaderStatus
	}{
		{name: "standby", status: LeaderStatus{Role: LeaderRoleStandby}},
		{name: "unavailable", status: LeaderStatus{Role: LeaderRoleUnavailable, Err: errors.New("database unavailable")}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			runner := &Runner{
				Store:      fixture.Store,
				Leadership: &fakeLeadershipController{status: tt.status},
			}
			request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			recorder := httptest.NewRecorder()

			NewReadyHandler(runner).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status code = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
			}
			var payload HealthStatus
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatalf("decode non-active health response: %v", err)
			}
			if payload.LeaderRole != tt.status.Role {
				t.Fatalf("leader role = %q, want %q", payload.LeaderRole, tt.status.Role)
			}
			if _, ok := payload.Errors["leadership"]; !ok {
				t.Fatalf("leadership error missing from %#v", payload)
			}
		})
	}
}

func TestLearningRecomputeEndpointRequiresActiveLeadership(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, tt := range []struct {
		name       string
		leadership *fakeLeadershipController
	}{
		{
			name:       "standby",
			leadership: &fakeLeadershipController{status: LeaderStatus{Role: LeaderRoleStandby}},
		},
		{
			name: "unavailable",
			leadership: &fakeLeadershipController{
				status:    LeaderStatus{Role: LeaderRoleUnavailable, Err: errors.New("database unavailable")},
				ensureErr: errors.New("database unavailable"),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			insertLearningStateForHealthTest(t, ctx, fixture)
			runner := &Runner{Store: fixture.Store, Leadership: tt.leadership}
			request := httptest.NewRequest(
				http.MethodPost,
				"/internal/learning/recompute?channel_id="+fixture.ChannelID+"&window_days=7",
				nil,
			)
			recorder := httptest.NewRecorder()

			NewRunnerHTTPHandler(runner).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusServiceUnavailable {
				t.Fatalf("status code = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
			}
			if tt.leadership.ensureCalls != 1 {
				t.Fatalf("EnsureActive calls = %d, want 1", tt.leadership.ensureCalls)
			}
			if count := countLearningStatesForHealthTest(t, ctx, fixture); count != 1 {
				t.Fatalf("learning state count = %d, want 1", count)
			}
		})
	}
}

func TestLearningRecomputeEndpointUsesActiveLeaderFence(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := fixture.Store.Now()
	lease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@admin-active:1", now)
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(time.Minute))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	insertLearningStateForHealthTest(t, ctx, fixture)
	authority := lease.Authority()
	runner := &Runner{
		Store: fixture.Store,
		Leadership: &fakeLeadershipController{
			authority: &authority,
			status:    LeaderStatus{Role: LeaderRoleActive, Authority: &authority},
		},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/learning/recompute?channel_id="+fixture.ChannelID+"&window_days=7",
		nil,
	)
	recorder := httptest.NewRecorder()

	NewRunnerHTTPHandler(runner).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if count := countLearningStatesForHealthTest(t, ctx, fixture); count != 0 {
		t.Fatalf("learning state count = %d, want 0", count)
	}
}

func TestLearningRecomputeEndpointRejectsLostLeaderWithoutMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := fixture.Store.Now()
	lease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@admin-lost:1", now)
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(time.Minute))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	insertLearningStateForHealthTest(t, ctx, fixture)
	authority := lease.Authority()
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET epoch = epoch + 1
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		t.Fatalf("advance admin leader epoch: %v", err)
	}
	runner := &Runner{
		Store: fixture.Store,
		Leadership: &fakeLeadershipController{
			authority: &authority,
			status:    LeaderStatus{Role: LeaderRoleActive, Authority: &authority},
		},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/learning/recompute?channel_id="+fixture.ChannelID+"&window_days=7",
		nil,
	)
	recorder := httptest.NewRecorder()

	NewRunnerHTTPHandler(runner).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status code = %d, want 503; body=%s", recorder.Code, recorder.Body.String())
	}
	if count := countLearningStatesForHealthTest(t, ctx, fixture); count != 1 {
		t.Fatalf("learning state count = %d, want 1", count)
	}
}

func TestLearningRecomputeEndpointRollsBackFencedMutationOnError(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := fixture.Store.Now()
	lease := acquireLeaderTestLease(t, ctx, fixture.Store, "channelops-go@admin-rollback:1", now)
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(time.Minute))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	insertLearningStateForHealthTest(t, ctx, fixture)
	authority := lease.Authority()
	rollbackErr := errors.New("roll back recompute")
	runner := &Runner{
		Store: fixture.Store,
		Leadership: &fakeLeadershipController{
			authority: &authority,
			status:    LeaderStatus{Role: LeaderRoleActive, Authority: &authority},
		},
		recomputeLearning: func(ctx context.Context, fencedStore *Store, channelID string, windowDays int) error {
			if fencedStore == fixture.Store || !fencedStore.hasExecutionTransaction() {
				t.Fatal("recompute did not receive the fenced store")
			}
			if err := fencedStore.RecomputeLearningState(ctx, channelID, windowDays); err != nil {
				return err
			}
			return rollbackErr
		},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/internal/learning/recompute?channel_id="+fixture.ChannelID+"&window_days=7",
		nil,
	)
	recorder := httptest.NewRecorder()

	NewRunnerHTTPHandler(runner).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status code = %d, want 500; body=%s", recorder.Code, recorder.Body.String())
	}
	if count := countLearningStatesForHealthTest(t, ctx, fixture); count != 1 {
		t.Fatalf("learning state count after rollback = %d, want 1", count)
	}
}

func insertLearningStateForHealthTest(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture) {
	t.Helper()
	now := fixture.Store.Now()
	if _, err := fixture.Store.Pool.Exec(ctx, `
		INSERT INTO learning_states (
			id, channel_profile_id, dimension_type, dimension_key, window_days,
			sample_count, avg_reward, confidence, recommendation_json,
			last_computed_at, created_at, updated_at
			) VALUES (
				gen_random_uuid(), $1::uuid, 'source', 'health-test-seed', 7,
				1, 0.5, 0.05, '{}'::json, $2::timestamptz, $2::timestamp, $2::timestamp
			)
	`, fixture.ChannelID, now); err != nil {
		t.Fatalf("insert learning state: %v", err)
	}
}

func countLearningStatesForHealthTest(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture) int {
	t.Helper()
	var count int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM learning_states
		WHERE channel_profile_id = $1::uuid
		  AND dimension_type = 'source'
		  AND window_days = 7
	`, fixture.ChannelID).Scan(&count); err != nil {
		t.Fatalf("count learning states: %v", err)
	}
	return count
}

func TestRunnerHealthCheckUsesThrottledSchedulerStaleness(t *testing.T) {
	now := time.Date(2026, 6, 7, 17, 20, 0, 0, time.UTC) // 10:20 PDT
	runner := &Runner{
		Config: Config{
			SchedulerPollSeconds:         60,
			ThrottleEnabled:              true,
			ThrottleTimeZone:             "America/Los_Angeles",
			ThrottleStartHour:            8,
			ThrottleEndHour:              24,
			ThrottleSchedulerPollSeconds: 1800,
		},
		Store: &Store{Now: func() time.Time { return now }},
	}
	runner.SetLastSchedulerRun(now.Add(-20 * time.Minute))

	status := runner.HealthCheck(context.Background())

	if _, ok := status.Errors["scheduler"]; ok {
		t.Fatalf("scheduler health error = %q, want no scheduler error during throttled window", status.Errors["scheduler"])
	}
}

func TestHealthHandlerReturns503WhenUnhealthy(t *testing.T) {
	checker := healthCheckerFunc(func(ctx context.Context) HealthStatus {
		return HealthStatus{Status: "unhealthy", DB: "ok", Errors: map[string]string{"scheduler": "stale"}}
	})
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()

	NewHealthHandler(checker).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status code = %d, want 503", recorder.Code)
	}
	var payload map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload["status"] != "unhealthy" {
		t.Fatalf("response = %#v", payload)
	}
}

func TestReadinessHandlerChecksDBOnly(t *testing.T) {
	checker := healthCheckerFunc(func(ctx context.Context) HealthStatus {
		return HealthStatus{Status: "ok", DB: "ok"}
	})
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()

	NewReadyHandler(checker).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestHealthStatusErrorDetectsUnhealthy(t *testing.T) {
	status := HealthStatus{Status: "unhealthy", Errors: map[string]string{"db": "down"}}
	if err := status.Err(); err == nil || !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("Err() = %v, want ErrUnhealthy", err)
	}
}

type healthCheckerFunc func(context.Context) HealthStatus

func (f healthCheckerFunc) HealthCheck(ctx context.Context) HealthStatus {
	return f(ctx)
}
