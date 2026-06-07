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
