package main

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/channelops"
)

type fakeManagedRunner struct {
	run   func(context.Context) error
	close func() error
}

func (r *fakeManagedRunner) Run(ctx context.Context) error {
	return r.run(ctx)
}

func (r *fakeManagedRunner) Close() error {
	return r.close()
}

func (*fakeManagedRunner) Handler() http.Handler {
	return http.NewServeMux()
}

func TestRunWithDependenciesClosesAfterJoiningServerOnRunnerError(t *testing.T) {
	runnerErr := errors.New("runner failed")
	var serverJoined atomic.Bool
	var closed atomic.Bool
	runner := &fakeManagedRunner{
		run: func(context.Context) error {
			return runnerErr
		},
		close: func() error {
			if !serverJoined.Load() {
				t.Error("runner closed before HTTP server joined")
			}
			closed.Store(true)
			return nil
		},
	}
	deps := runDependencies{
		loadConfig: runnerMainTestConfig,
		signalContext: func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		},
		newRunner: func(context.Context, channelops.Config) (managedRunner, error) {
			return runner, nil
		},
		runHTTPServer: func(ctx context.Context, _ string, _ http.Handler) error {
			<-ctx.Done()
			serverJoined.Store(true)
			return ctx.Err()
		},
	}

	code := runWithDependencies(deps)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !closed.Load() {
		t.Fatal("runner was not closed")
	}
}

func TestRunWithDependenciesClosesAfterJoiningRunnerOnServerError(t *testing.T) {
	serverErr := errors.New("server failed")
	var runnerJoined atomic.Bool
	var closed atomic.Bool
	runner := &fakeManagedRunner{
		run: func(ctx context.Context) error {
			<-ctx.Done()
			runnerJoined.Store(true)
			return ctx.Err()
		},
		close: func() error {
			if !runnerJoined.Load() {
				t.Error("runner closed before runner goroutine joined")
			}
			closed.Store(true)
			return nil
		},
	}
	deps := runDependencies{
		loadConfig: runnerMainTestConfig,
		signalContext: func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		},
		newRunner: func(context.Context, channelops.Config) (managedRunner, error) {
			return runner, nil
		},
		runHTTPServer: func(context.Context, string, http.Handler) error {
			return serverErr
		},
	}

	code := runWithDependencies(deps)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !closed.Load() {
		t.Fatal("runner was not closed")
	}
}

func TestRunWithDependenciesReturnsFailureWhenRunnerCloseFails(t *testing.T) {
	closeErr := errors.New("leadership operation did not join")
	var closed atomic.Bool
	runner := &fakeManagedRunner{
		run: func(context.Context) error {
			return nil
		},
		close: func() error {
			closed.Store(true)
			return closeErr
		},
	}
	deps := runDependencies{
		loadConfig: runnerMainTestConfig,
		signalContext: func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		},
		newRunner: func(context.Context, channelops.Config) (managedRunner, error) {
			return runner, nil
		},
		runHTTPServer: func(ctx context.Context, _ string, _ http.Handler) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	code := runWithDependencies(deps)

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !closed.Load() {
		t.Fatal("runner Close was not called")
	}
}

func runnerMainTestConfig() channelops.Config {
	return channelops.Config{
		DatabaseURL:                  "postgresql://vp:test@localhost:5432/videoprocess",
		RunnerID:                     "channelops-go@test:1",
		AutoFlowTimeout:              time.Second,
		DiscoveryTimeout:             30 * time.Second,
		PDSTimeout:                   time.Second,
		RunnerPollSeconds:            1,
		SchedulerPollSeconds:         1,
		HealthPort:                   8080,
		ThrottleTimeZone:             "America/Los_Angeles",
		ThrottleStartHour:            8,
		ThrottleEndHour:              24,
		ThrottleRunnerPollSeconds:    1,
		ThrottleSchedulerPollSeconds: 1,
		MaxQueueAttempts:             3,
		MetricsPollMaxAttempts:       1,
		MetricsPollDelayMinutes:      1,
		RetentionQueueDays:           1,
		RetentionAuditDays:           1,
		RetentionFeedbackDays:        1,
	}
}
