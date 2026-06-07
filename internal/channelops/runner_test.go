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

func TestRunnerRunPerformsInitialRunBeforeFirstSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &Store{Now: func() time.Time {
		return time.Date(2026, 6, 7, 17, 0, 0, 0, time.UTC)
	}}
	runner := &Runner{
		Config:   Config{RunnerPollSeconds: 60},
		Store:    store,
		Handlers: HandlerService{Store: store, PDS: PDSClient{}},
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	select {
	case err := <-errCh:
		if err == nil || !strings.Contains(err.Error(), "autoflow client is not configured") {
			t.Fatalf("Run returned %v, want initial readiness error", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Run did not perform initial run before sleeping")
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
	if handler.Alerts == nil {
		t.Fatal("Alerts sink is nil")
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

func TestConfigEffectivePollSecondsUsesDaytimeThrottleWindow(t *testing.T) {
	cfg := validConfig()
	cfg.ThrottleEnabled = true
	cfg.ThrottleTimeZone = "America/Los_Angeles"
	cfg.ThrottleStartHour = 8
	cfg.ThrottleEndHour = 24
	cfg.ThrottleRunnerPollSeconds = 300
	cfg.ThrottleSchedulerPollSeconds = 1800

	daytimePacific := time.Date(2026, 6, 7, 17, 0, 0, 0, time.UTC) // 10:00 PDT
	if got := cfg.EffectiveRunnerPollSeconds(daytimePacific); got != 300 {
		t.Fatalf("daytime runner poll = %d, want 300", got)
	}
	if got := cfg.EffectiveSchedulerPollSeconds(daytimePacific); got != 1800 {
		t.Fatalf("daytime scheduler poll = %d, want 1800", got)
	}

	overnightPacific := time.Date(2026, 6, 7, 8, 0, 0, 0, time.UTC) // 01:00 PDT
	if got := cfg.EffectiveRunnerPollSeconds(overnightPacific); got != 5 {
		t.Fatalf("overnight runner poll = %d, want 5", got)
	}
	if got := cfg.EffectiveSchedulerPollSeconds(overnightPacific); got != 60 {
		t.Fatalf("overnight scheduler poll = %d, want 60", got)
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
