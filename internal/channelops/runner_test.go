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
	if handler.Discovery == nil {
		t.Fatal("Discovery client is nil for valid config")
	}
	if err := handler.ReadinessError(); err != nil {
		t.Fatalf("ReadinessError returned error: %v", err)
	}
}

func TestNewRunnerHandlerServiceConfiguresDiscoveryDirectly(t *testing.T) {
	for _, tt := range []struct {
		name          string
		mutate        func(*Config)
		loadMalformed bool
	}{
		{name: "missing base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "  " }},
		{name: "credential base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "http://user:password@api:8080" }},
		{name: "query base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "http://api:8080?credential=secret" }},
		{name: "empty query base URL", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "http://api:8080?" }},
		{name: "invalid scheme", mutate: func(cfg *Config) { cfg.AutoFlowBaseURL = "ftp://api:8080" }},
		{name: "invalid timeout", mutate: func(cfg *Config) { cfg.DiscoveryTimeout = 29 * time.Second }},
		{name: "malformed timeout", loadMalformed: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfig()
			if tt.loadMalformed {
				t.Setenv("CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS", "not-an-integer")
				cfg = LoadConfig()
			}
			cfg.LiveMode = false
			if tt.mutate != nil {
				tt.mutate(&cfg)
			}
			handler := newRunnerHandlerService(&Store{}, cfg)
			if handler.Discovery != nil {
				t.Fatal("Discovery client configured for invalid discovery settings")
			}
			if containsString(handler.ClaimableKinds(), QueueIngestDiscovery) {
				t.Fatal("ClaimableKinds includes discovery for invalid discovery settings")
			}
			if err := handler.ReadinessError(); err != nil {
				t.Fatalf("invalid optional discovery settings changed readiness: %v", err)
			}
		})
	}
}

func TestNewRunnerHandlerServiceDiscoveryIgnoresUnrelatedConfigValidation(t *testing.T) {
	cfg := validConfig()
	cfg.DatabaseURL = ""
	handler := newRunnerHandlerService(&Store{}, cfg)
	if handler.Discovery == nil || !containsString(handler.ClaimableKinds(), QueueIngestDiscovery) {
		t.Fatal("unrelated invalid config disabled valid discovery settings")
	}
}

func TestRunnerDiscoveryQueueUsesLeaseAwareRetryAndCompletion(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name       string
		client     *recordingDiscoveryClient
		wantStatus string
		wantError  string
	}{
		{
			name: "retry", client: &recordingDiscoveryClient{err: errors.New("credential=top-secret provider-title=private")},
			wantStatus: QueueStatusQueued, wantError: "discovery ingestion failed",
		},
		{
			name: "done", client: &recordingDiscoveryClient{}, wantStatus: QueueStatusSucceeded,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			channelID := fixture.ChannelID
			queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
				Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner:" + tt.name + ":" + channelID,
				Payload:  map[string]any{"channel_id": channelID, "source": "youtube_search", "bucket": "2026-07-21-18", "scheduler_bucket": "2026-07-21-18"},
				Priority: 80, ChannelProfileID: &channelID,
			})
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			request := DiscoveryIngestRequest{QueueItemID: queueID, ChannelID: channelID, Source: "youtube_search", SchedulerBucket: "2026-07-21-18"}
			if tt.client.observation.RunID == "" && tt.client.err == nil {
				tt.client.observation = discoveryObservationForTest(request)
			}
			handler.Discovery = tt.client
			runner := &Runner{Store: fixture.Store, Handlers: handler}
			if err := runner.runOnce(ctx); err != nil {
				t.Fatalf("runOnce: %v", err)
			}
			var status string
			var lastError *string
			var lockedBy *string
			var lockedAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, last_error, locked_by, locked_at
				FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&status, &lastError, &lockedBy, &lockedAt); err != nil {
				t.Fatalf("select queue: %v", err)
			}
			if status != tt.wantStatus {
				t.Fatalf("status = %q, want %q", status, tt.wantStatus)
			}
			if tt.wantError != "" && (lastError == nil || *lastError != tt.wantError) {
				t.Fatal("last_error was not the fixed discovery failure")
			}
			if tt.wantError == "" && lastError != nil {
				t.Fatalf("last_error = %q, want nil", *lastError)
			}
			if lockedBy != nil || lockedAt != nil {
				t.Fatalf("lease remains locked_by=%v locked_at=%v", lockedBy, lockedAt)
			}
			if tt.client.calls != 1 {
				t.Fatalf("client calls = %d, want 1", tt.client.calls)
			}
		})
	}
}

func TestRunnerDiscoveryLeaseRaceCannotFinalizeReplacementLease(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name        string
		maxAttempts int
		clientError bool
	}{
		{name: "done", maxAttempts: 3},
		{name: "retry", maxAttempts: 3, clientError: true},
		{name: "deadletter", maxAttempts: 1, clientError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			channelID := fixture.ChannelID
			bucket := "2026-07-21-18"
			queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
				Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner-lease-race:" + tt.name + ":" + channelID,
				Payload: map[string]any{
					"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
				},
				Priority: 80, ChannelProfileID: &channelID, MaxAttempts: tt.maxAttempts,
			})
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}

			client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
				if _, err := fixture.Store.Pool.Exec(ctx, `
					UPDATE channel_ops_queue_items
					SET locked_by = 'replacement-worker', locked_at = locked_at + INTERVAL '1 second'
					WHERE id = $1::uuid AND status = $2
				`, queueID, QueueStatusRunning); err != nil {
					return DiscoveryObservation{}, err
				}
				if tt.clientError {
					return DiscoveryObservation{}, errors.New("credential=top-secret provider-title=private")
				}
				return discoveryObservationForTest(request), nil
			}}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			runner := &Runner{Store: fixture.Store, Handlers: handler}
			if err := runner.runOnce(ctx); !errors.Is(err, ErrQueueLeaseLost) || err.Error() != "queue lease lost" {
				t.Fatal("runOnce did not return the queue lease lost sentinel")
			}

			var status string
			var lockedBy *string
			var lockedAt *time.Time
			var lastError *string
			var deadLetterAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, locked_by, locked_at, last_error, dead_letter_at
				FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&status, &lockedBy, &lockedAt, &lastError, &deadLetterAt); err != nil {
				t.Fatalf("select queue: %v", err)
			}
			if status != QueueStatusRunning || lockedBy == nil || *lockedBy != "replacement-worker" || lockedAt == nil || lastError != nil || deadLetterAt != nil {
				t.Fatal("stale runner changed the replacement lease")
			}
		})
	}
}

func TestRunnerRecoversStaleDiscoveryLeaseBeforeClaim(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	staleLockedAt := fixture.Store.Now().Add(-15 * time.Minute)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, attempt_count = 1, locked_by = 'crashed-runner', locked_at = $3
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, staleLockedAt); err != nil {
		t.Fatalf("seed stale discovery lease: %v", err)
	}

	var replacementLockedAt time.Time
	var recoveredError *string
	client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
		var status, lockedBy string
		var attempts int
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT status, attempt_count, locked_by, locked_at, last_error
			FROM channel_ops_queue_items WHERE id = $1::uuid
		`, queueID).Scan(&status, &attempts, &lockedBy, &replacementLockedAt, &recoveredError); err != nil {
			return DiscoveryObservation{}, err
		}
		if status != QueueStatusRunning || attempts != 2 || lockedBy != "channelops-go-runner" {
			return DiscoveryObservation{}, errors.New("replacement discovery lease was not claimed")
		}
		return discoveryObservationForTest(request), nil
	}}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	runner := &Runner{Store: fixture.Store, Handlers: handler}

	if err := runner.runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	if !replacementLockedAt.After(staleLockedAt) {
		t.Fatalf("replacement locked_at = %s, stale locked_at = %s", replacementLockedAt, staleLockedAt)
	}
	if recoveredError == nil || *recoveredError != "discovery_lease_recovered" {
		t.Fatalf("recovery error category = %v", recoveredError)
	}
	var status string
	var attempts int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &attempts); err != nil {
		t.Fatalf("select recovered discovery row: %v", err)
	}
	if status != QueueStatusSucceeded || attempts != 2 || client.calls != 1 {
		t.Fatalf("recovered row = status %s attempts %d calls %d", status, attempts, client.calls)
	}
}

func TestRunnerDiscoveryRecoveryLeavesFreshAndOtherKindsRunning(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	freshID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	otherID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind: QueueLearningRecompute, IdempotencyKey: "recovery-other-kind:" + t.Name(),
		Payload: map[string]any{
			"channel_id": fixture.ChannelID, "bucket": "2026-05-21-18", "window_days": []int{7, 30},
		},
		Priority: 180, ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue other queue kind: %v", err)
	}
	freshLockedAt := fixture.Store.Now().Add(-15*time.Minute + time.Second)
	staleLockedAt := fixture.Store.Now().Add(-time.Hour)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    attempt_count = 1,
		    locked_by = CASE WHEN id = $1::uuid THEN 'fresh-runner' ELSE 'other-runner' END,
		    locked_at = CASE WHEN id = $1::uuid THEN $4::timestamptz ELSE $5::timestamptz END
		WHERE id IN ($1::uuid, $3::uuid)
	`, freshID, QueueStatusRunning, otherID, freshLockedAt, staleLockedAt); err != nil {
		t.Fatalf("seed untouched running leases: %v", err)
	}

	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	for _, tt := range []struct {
		id       string
		owner    string
		lockedAt time.Time
	}{
		{id: freshID, owner: "fresh-runner", lockedAt: freshLockedAt},
		{id: otherID, owner: "other-runner", lockedAt: staleLockedAt},
	} {
		var status, owner string
		var lockedAt time.Time
		var attempts int
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT status, attempt_count, locked_by, locked_at
			FROM channel_ops_queue_items WHERE id = $1::uuid
		`, tt.id).Scan(&status, &attempts, &owner, &lockedAt); err != nil {
			t.Fatalf("select untouched running lease: %v", err)
		}
		if status != QueueStatusRunning || attempts != 1 || owner != tt.owner || !lockedAt.Equal(tt.lockedAt) {
			t.Fatalf("running lease changed = %s/%d/%s/%s", status, attempts, owner, lockedAt)
		}
	}
	if client.calls != 0 {
		t.Fatalf("fresh discovery client calls = %d, want 0", client.calls)
	}
}

func TestRunnerRecoversDiscoveryRowsWithMalformedLeaseOwnership(t *testing.T) {
	for _, tt := range []struct {
		name     string
		lockedBy *string
		lockedAt *time.Time
	}{
		{name: "missing owner", lockedAt: timePointer(time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC))},
		{name: "blank owner", lockedBy: stringPointer("   "), lockedAt: timePointer(time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC))},
		{name: "missing timestamp", lockedBy: stringPointer("crashed-runner")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
			if _, err := fixture.Store.Pool.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2, attempt_count = 1, locked_by = $3, locked_at = $4
				WHERE id = $1::uuid
			`, queueID, QueueStatusRunning, tt.lockedBy, tt.lockedAt); err != nil {
				t.Fatalf("seed malformed discovery lease: %v", err)
			}

			client := &recordingDiscoveryClient{}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			client.ingest = func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
				return discoveryObservationForTest(request), nil
			}
			if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
				t.Fatalf("runOnce: %v", err)
			}
			var status string
			var attempts int
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, attempt_count FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&status, &attempts); err != nil {
				t.Fatalf("select recovered malformed lease: %v", err)
			}
			if status != QueueStatusSucceeded || attempts != 2 || client.calls != 1 {
				t.Fatalf("malformed recovery = status %s attempts %d calls %d", status, attempts, client.calls)
			}
		})
	}
}

func TestRunnerDeadLettersExhaustedStaleDiscoveryLease(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, attempt_count = max_attempts,
		    locked_by = 'crashed-runner', locked_at = $3
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, fixture.Store.Now().Add(-time.Hour)); err != nil {
		t.Fatalf("seed exhausted discovery lease: %v", err)
	}

	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	var status string
	var lastError *string
	var lockedBy *string
	var lockedAt, deadLetterAt *time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, last_error, locked_by, locked_at, dead_letter_at
		FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &lastError, &lockedBy, &lockedAt, &deadLetterAt); err != nil {
		t.Fatalf("select exhausted discovery lease: %v", err)
	}
	if status != QueueStatusDeadLettered || lastError == nil || *lastError != "discovery_lease_recovered" || lockedBy != nil || lockedAt != nil || deadLetterAt == nil {
		t.Fatalf("exhausted recovery = %s/%v/%v/%v/%v", status, lastError, lockedBy, lockedAt, deadLetterAt)
	}
	if client.calls != 0 {
		t.Fatalf("exhausted discovery client calls = %d, want 0", client.calls)
	}
}

func TestRunnerRecoveryFencesStaleDiscoveryOwnerCompletion(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 3)
	oldOwner := "crashed-runner"
	oldLockedAt := fixture.Store.Now().Add(-time.Hour)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2, attempt_count = 1, locked_by = $3, locked_at = $4
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, oldOwner, oldLockedAt); err != nil {
		t.Fatalf("seed stale discovery owner: %v", err)
	}
	staleItem := QueueItemRow{
		ID: queueID, Status: QueueStatusRunning, AttemptCount: 1, MaxAttempts: 3,
		LockedBy: &oldOwner, LockedAt: &oldLockedAt,
	}

	var staleCompletionErr error
	client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
		staleCompletionErr = fixture.Store.MarkQueueDone(ctx, staleItem)
		return discoveryObservationForTest(request), nil
	}}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}
	if !errors.Is(staleCompletionErr, ErrQueueLeaseLost) {
		t.Fatalf("stale completion error = %v, want ErrQueueLeaseLost", staleCompletionErr)
	}
	var status string
	var attempts int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &attempts); err != nil {
		t.Fatalf("select stale owner recovery: %v", err)
	}
	if status != QueueStatusSucceeded || attempts != 2 {
		t.Fatalf("stale owner recovery = status %s attempts %d", status, attempts)
	}
}

func TestRunnerRecoveryReconcilesSucceededDiscoveryRunAtMaxAttempts(t *testing.T) {
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, 1)
	staleLockedAt := fixture.Store.Now().Add(-time.Hour)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    attempt_count = max_attempts,
		    locked_by = 'lost-response-runner',
		    locked_at = $3,
		    last_error = 'old provider response details',
		    dead_letter_at = $3
		WHERE id = $1::uuid
	`, queueID, QueueStatusRunning, staleLockedAt); err != nil {
		t.Fatalf("seed stale max-attempt discovery lease: %v", err)
	}
	finishedAt := fixture.Store.Now().Add(-time.Minute)
	runID := insertDiscoveryRecoveryRun(t, ctx, fixture, queueID, "succeeded", 1, &finishedAt)

	client := &recordingDiscoveryClient{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
	handler.Discovery = client
	if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
		t.Fatalf("runOnce: %v", err)
	}

	var status string
	var attempts int
	var lockedBy, lastError *string
	var lockedAt, deadLetterAt *time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status, attempt_count, locked_by, locked_at, last_error, dead_letter_at
		FROM channel_ops_queue_items WHERE id = $1::uuid
	`, queueID).Scan(&status, &attempts, &lockedBy, &lockedAt, &lastError, &deadLetterAt); err != nil {
		t.Fatalf("select reconciled queue row: %v", err)
	}
	if status != QueueStatusSucceeded || attempts != 1 || lockedBy != nil || lockedAt != nil || lastError != nil || deadLetterAt != nil {
		t.Fatalf("reconciled queue = %s/%d/%v/%v/%v/%v", status, attempts, lockedBy, lockedAt, lastError, deadLetterAt)
	}
	if client.calls != 0 {
		t.Fatalf("succeeded replay client calls = %d, want 0", client.calls)
	}
	var runStatus string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT status FROM discovery_ingestion_runs WHERE id = $1::uuid
	`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("select succeeded durable run: %v", err)
	}
	if runStatus != "succeeded" {
		t.Fatalf("durable run status = %q, want succeeded", runStatus)
	}
}

func TestRunnerRecoveryInvalidatesRunningDiscoveryGeneration(t *testing.T) {
	for _, tt := range []struct {
		name         string
		maxAttempts  int
		wantQueue    string
		wantAttempts int
		wantClaim    bool
	}{
		{name: "requeue", maxAttempts: 3, wantQueue: QueueStatusSucceeded, wantAttempts: 2, wantClaim: true},
		{name: "dead letter", maxAttempts: 1, wantQueue: QueueStatusDeadLettered, wantAttempts: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			queueID := enqueueDiscoveryRecoveryItem(t, ctx, fixture, tt.maxAttempts)
			if _, err := fixture.Store.Pool.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2, attempt_count = 1,
				    locked_by = 'old-python-owner', locked_at = $3
				WHERE id = $1::uuid
			`, queueID, QueueStatusRunning, fixture.Store.Now().Add(-time.Hour)); err != nil {
				t.Fatalf("seed stale discovery queue generation: %v", err)
			}
			const oldGeneration = 7
			runID := insertDiscoveryRecoveryRun(t, ctx, fixture, queueID, "running", oldGeneration, nil)

			var statusBeforeLate string
			var generationBeforeLate int
			var finishedBeforeLate *time.Time
			var errorBeforeLate *string
			var lateRows int64 = -1
			lateTerminalUpdate := func() error {
				if err := fixture.Store.Pool.QueryRow(ctx, `
					SELECT status, attempt_count, finished_at, last_error_code
					FROM discovery_ingestion_runs WHERE id = $1::uuid
				`, runID).Scan(&statusBeforeLate, &generationBeforeLate, &finishedBeforeLate, &errorBeforeLate); err != nil {
					return err
				}
				result, err := fixture.Store.Pool.Exec(ctx, `
					UPDATE discovery_ingestion_runs
					SET status = 'succeeded', finished_at = $3, last_error_code = NULL
					WHERE id = $1::uuid
					  AND status = 'running'
					  AND attempt_count = $2
				`, runID, oldGeneration, fixture.Store.Now().Add(time.Minute))
				if err != nil {
					return err
				}
				lateRows = result.RowsAffected()
				return nil
			}

			client := &recordingDiscoveryClient{}
			if tt.wantClaim {
				client.ingest = func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
					if err := lateTerminalUpdate(); err != nil {
						return DiscoveryObservation{}, err
					}
					return discoveryObservationForTest(request), nil
				}
			}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			if err := (&Runner{Store: fixture.Store, Handlers: handler}).runOnce(ctx); err != nil {
				t.Fatalf("runOnce: %v", err)
			}
			if !tt.wantClaim {
				if err := lateTerminalUpdate(); err != nil {
					t.Fatalf("late durable terminal update: %v", err)
				}
			}

			if statusBeforeLate != "failed" || generationBeforeLate != oldGeneration || finishedBeforeLate == nil || !finishedBeforeLate.Equal(fixture.Store.Now()) || errorBeforeLate == nil || *errorBeforeLate != "discovery_lease_recovered" {
				t.Fatalf("durable run before late update = %s/%d/%v/%v", statusBeforeLate, generationBeforeLate, finishedBeforeLate, errorBeforeLate)
			}
			if lateRows != 0 {
				t.Fatalf("late generation terminal rows = %d, want 0", lateRows)
			}
			var runStatus string
			if err := fixture.Store.Pool.QueryRow(ctx, `SELECT status FROM discovery_ingestion_runs WHERE id = $1::uuid`, runID).Scan(&runStatus); err != nil {
				t.Fatalf("select invalidated durable run: %v", err)
			}
			if runStatus != "failed" {
				t.Fatalf("durable run status after late update = %q, want failed", runStatus)
			}

			var queueStatus string
			var attempts int
			var deadLetterAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, attempt_count, dead_letter_at
				FROM channel_ops_queue_items WHERE id = $1::uuid
			`, queueID).Scan(&queueStatus, &attempts, &deadLetterAt); err != nil {
				t.Fatalf("select recovered queue generation: %v", err)
			}
			if queueStatus != tt.wantQueue || attempts != tt.wantAttempts {
				t.Fatalf("queue recovery = %s/%d, want %s/%d", queueStatus, attempts, tt.wantQueue, tt.wantAttempts)
			}
			if (tt.wantQueue == QueueStatusDeadLettered) != (deadLetterAt != nil) {
				t.Fatalf("queue dead_letter_at = %v for status %s", deadLetterAt, queueStatus)
			}
			wantCalls := 0
			if tt.wantClaim {
				wantCalls = 1
			}
			if client.calls != wantCalls {
				t.Fatalf("discovery client calls = %d, want %d", client.calls, wantCalls)
			}
		})
	}
}

func enqueueDiscoveryRecoveryItem(t *testing.T, ctx context.Context, fixture *ChannelOpsFixture, maxAttempts int) string {
	t.Helper()
	channelID := fixture.ChannelID
	bucket := "2026-05-21-18"
	queueID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-recovery:" + t.Name(),
		Payload: map[string]any{
			"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
		},
		Priority: 80, ChannelProfileID: &channelID, MaxAttempts: maxAttempts,
	})
	if err != nil {
		t.Fatalf("enqueue discovery recovery item: %v", err)
	}
	return queueID
}

func insertDiscoveryRecoveryRun(
	t *testing.T,
	ctx context.Context,
	fixture *ChannelOpsFixture,
	queueID string,
	status string,
	generation int,
	finishedAt *time.Time,
) string {
	t.Helper()
	var runID string
	if err := fixture.Store.Pool.QueryRow(ctx, `
		INSERT INTO discovery_ingestion_runs (
			id, channel_profile_id, queue_item_id, source, scheduler_bucket,
			query_version, status, attempt_count, query_count, created_count,
			refreshed_count, expired_count, quota_units_estimated,
			policy_snapshot_json, started_at, finished_at, last_error_code
		) VALUES (
			gen_random_uuid(), $1::uuid, $2::uuid, 'youtube_search', '2026-05-21-18',
			'youtube-lane-keyword-v1', $3, $4, 0, 0, 0, 0, 0,
			'{}'::json, $5, $6, NULL
		)
		RETURNING id::text
	`, fixture.ChannelID, queueID, status, generation, fixture.Store.Now().Add(-time.Hour), finishedAt).Scan(&runID); err != nil {
		t.Fatalf("insert discovery recovery run: %v", err)
	}
	return runID
}

func stringPointer(value string) *string {
	return &value
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func TestRunnerRunContinuesAfterDiscoveryLeaseLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, tt := range []struct {
		name     string
		seedPoll bool
	}{
		{name: "initial poll"},
		{name: "timer poll", seedPoll: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(context.Background())
			fixture.InsertChannelWithLaneAccountSeed(context.Background())
			channelID := fixture.ChannelID
			bucket := "2026-07-21-18"

			if tt.seedPoll {
				if _, err := fixture.Store.Enqueue(context.Background(), EnqueueOptions{
					Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner-lease-loss-seed:" + channelID,
					Payload: map[string]any{
						"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
					},
					Priority: 70, ChannelProfileID: &channelID,
				}); err != nil {
					t.Fatalf("enqueue seed: %v", err)
				}
			}
			targetID, err := fixture.Store.Enqueue(context.Background(), EnqueueOptions{
				Kind: QueueIngestDiscovery, IdempotencyKey: "discovery-runner-lease-loss-target:" + tt.name + ":" + channelID,
				Payload: map[string]any{
					"channel_id": channelID, "source": "youtube_search", "bucket": bucket, "scheduler_bucket": bucket,
				},
				Priority: 80, ChannelProfileID: &channelID,
			})
			if err != nil {
				t.Fatalf("enqueue target: %v", err)
			}

			initialPollCompleted := make(chan struct{})
			leaseReplaced := make(chan struct{}, 1)
			calls := 0
			client := &recordingDiscoveryClient{ingest: func(request DiscoveryIngestRequest) (DiscoveryObservation, error) {
				calls++
				if tt.seedPoll && calls == 1 {
					close(initialPollCompleted)
					return discoveryObservationForTest(request), nil
				}
				result, err := fixture.Store.Pool.Exec(context.Background(), `
					UPDATE channel_ops_queue_items
					SET locked_by = 'replacement-worker', locked_at = locked_at + INTERVAL '1 second'
					WHERE id = $1::uuid AND status = $2
				`, targetID, QueueStatusRunning)
				if err != nil {
					return DiscoveryObservation{}, err
				}
				if result.RowsAffected() != 1 {
					return DiscoveryObservation{}, errors.New("target discovery lease was not running")
				}
				leaseReplaced <- struct{}{}
				return discoveryObservationForTest(request), nil
			}}
			handler := fixture.HandlerService(PDSDecision{Verdict: "allow"})
			handler.Discovery = client
			runner := &Runner{Config: Config{RunnerPollSeconds: 1}, Store: fixture.Store, Handlers: handler}
			errCh := make(chan error, 1)
			go func() { errCh <- runner.Run(ctx) }()

			if tt.seedPoll {
				select {
				case <-initialPollCompleted:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("initial poll did not complete before timer poll")
				}
			}
			select {
			case <-leaseReplaced:
			case <-time.After(2 * time.Second):
				t.Fatal("runner did not reach the discovery lease replacement")
			}
			select {
			case err := <-errCh:
				t.Fatalf("Run returned after lease loss: %v", err)
			case <-time.After(100 * time.Millisecond):
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
		})
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
