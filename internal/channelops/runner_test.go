package channelops

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunnerRunWaitsForCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := &Runner{Config: Config{RunnerPollSeconds: 1}}
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case err := <-errCh:
		t.Fatalf("Run returned before cancellation: %v", err)
	case <-time.After(1100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestRunnerRunOnceRejectsMissingHandlerDependencies(t *testing.T) {
	store := &Store{Now: func() time.Time {
		return time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	}}
	runner := &Runner{
		Store:    store,
		Handlers: HandlerService{Store: store, PDS: PDSClient{}},
	}

	err := runner.runOnce(context.Background())
	if err == nil {
		t.Fatal("expected missing handler dependencies to return an error")
	}
	if !strings.Contains(err.Error(), "autoflow client is not configured") {
		t.Fatalf("error = %v", err)
	}
}

func TestNewRunnerHandlerServiceConfiguresAutoFlowClient(t *testing.T) {
	store := &Store{Now: func() time.Time {
		return time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	}}
	handler := newRunnerHandlerService(store, validConfig())
	if handler.AutoFlow == nil {
		t.Fatal("AutoFlow client is nil")
	}
	if err := handler.ReadinessError(); err != nil {
		t.Fatalf("ReadinessError returned error: %v", err)
	}
}

func TestShouldRunSchedulerHonorsPollSeconds(t *testing.T) {
	lastRun := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)

	if ShouldRunScheduler(lastRun, lastRun.Add(59*time.Second), 60) {
		t.Fatal("scheduler should not run before configured poll interval")
	}
	if !ShouldRunScheduler(lastRun, lastRun.Add(60*time.Second), 60) {
		t.Fatal("scheduler should run at configured poll interval")
	}
	if !ShouldRunScheduler(time.Time{}, lastRun, 60) {
		t.Fatal("scheduler should run when it has not run yet")
	}
}

func TestNewRunnerAppliesQueueMaxAttemptsConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	cfg := LoadConfig()
	cfg.LiveMode = false
	cfg.MaxQueueAttempts = 6

	runner, err := NewRunner(ctx, cfg)
	if err != nil {
		t.Skipf("ChannelOps runner test requires reachable DATABASE_URL %q: %v", cfg.DatabaseURL, err)
	}
	defer runner.Close()

	if runner.Store.DefaultMaxAttempts != 6 {
		t.Fatalf("DefaultMaxAttempts = %d, want 6", runner.Store.DefaultMaxAttempts)
	}
}
