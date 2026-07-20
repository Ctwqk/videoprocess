package channelops

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRetryDelayUsesExponentialBackoff(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 5 * time.Minute},
		{attempt: 2, want: 10 * time.Minute},
		{attempt: 3, want: 20 * time.Minute},
		{attempt: 4, want: 30 * time.Minute},
		{attempt: 9, want: 30 * time.Minute},
	}
	for _, tc := range cases {
		if got := RetryDelay(tc.attempt); got != tc.want {
			t.Fatalf("RetryDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestShouldDeadLetter(t *testing.T) {
	if ShouldDeadLetter(4, 5) {
		t.Fatal("attempt 4 of 5 should retry")
	}
	if !ShouldDeadLetter(5, 5) {
		t.Fatal("attempt 5 of 5 should dead-letter")
	}
	if !ShouldDeadLetter(3, 0) {
		t.Fatal("default max attempts should match Python default of 3")
	}
}

func TestClaimNextForKindsEmptyDoesNotClaim(t *testing.T) {
	store := &Store{}
	item, err := store.ClaimNextForKinds(context.Background(), "worker-1", nil)
	if err != nil {
		t.Fatalf("ClaimNextForKinds returned error: %v", err)
	}
	if item != nil {
		t.Fatalf("ClaimNextForKinds returned item for empty kinds: %#v", item)
	}
}

func TestClaimNextForKindsQueryFiltersAndOrdersLikePython(t *testing.T) {
	if !strings.Contains(claimNextForKindsQuery, "kind = ANY($4)") {
		t.Fatalf("claim query does not filter by owned kinds:\n%s", claimNextForKindsQuery)
	}
	if !strings.Contains(claimNextForKindsQuery, "ORDER BY q.priority ASC, q.created_at ASC") {
		t.Fatalf("claim query does not match Python ordering:\n%s", claimNextForKindsQuery)
	}
	if strings.Contains(claimNextForKindsQuery, "run_after ASC, created_at") {
		t.Fatalf("claim query should not sort by run_after before created_at:\n%s", claimNextForKindsQuery)
	}
}

func TestClaimNextForChannelAndKindsQueryScopesChannel(t *testing.T) {
	if !strings.Contains(claimNextForChannelAndKindsQuery, "COALESCE(q.channel_profile_id, authority.authoritative_channel_id) = $5::uuid") {
		t.Fatalf("scoped claim query does not filter by channel_profile_id:\n%s", claimNextForChannelAndKindsQuery)
	}
	if !strings.Contains(claimNextForChannelAndKindsQuery, "kind = ANY($4)") {
		t.Fatalf("scoped claim query does not filter by owned kinds:\n%s", claimNextForChannelAndKindsQuery)
	}
}

func TestQueueStatusConstantsForSQL(t *testing.T) {
	if QueueStatusQueued != "queued" {
		t.Fatalf("QueueStatusQueued = %q", QueueStatusQueued)
	}
	if QueueStatusRunning != "running" {
		t.Fatalf("QueueStatusRunning = %q", QueueStatusRunning)
	}
	if QueueStatusSucceeded != "succeeded" {
		t.Fatalf("QueueStatusSucceeded = %q", QueueStatusSucceeded)
	}
	if QueueStatusDeadLettered != "dead_lettered" {
		t.Fatalf("QueueStatusDeadLettered = %q", QueueStatusDeadLettered)
	}
}

func TestEnqueueUsesStoreDefaultMaxAttempts(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	fixture.Store.DefaultMaxAttempts = 5
	channelID := fixture.ChannelID
	itemID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:             QueueAccountHealth,
		IdempotencyKey:   "account_health:" + fixture.AccountID + ":default-attempts",
		Payload:          map[string]any{"account_id": fixture.AccountID},
		ChannelProfileID: &channelID,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	var maxAttempts int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT max_attempts
		FROM channel_ops_queue_items
		WHERE id = $1::uuid
	`, itemID).Scan(&maxAttempts); err != nil {
		t.Fatalf("select max_attempts: %v", err)
	}
	if maxAttempts != 5 {
		t.Fatalf("max_attempts = %d, want 5", maxAttempts)
	}
}

func TestClaimRejectsDisabledAndHaltedChannelsButKeepsGlobalItems(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	channelID := fixture.ChannelID
	_, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:             QueueAccountHealth,
		IdempotencyKey:   "account_health:" + fixture.AccountID + ":channel-state",
		Payload:          map[string]any{"account_id": fixture.AccountID},
		ChannelProfileID: &channelID,
	})
	if err != nil {
		t.Fatalf("Enqueue channel item: %v", err)
	}
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_profiles SET enabled = FALSE WHERE id = $1::uuid
	`, fixture.ChannelID); err != nil {
		t.Fatalf("disable channel: %v", err)
	}
	item, err := fixture.Store.ClaimNextForChannelAndKinds(
		ctx, "disabled-worker", fixture.ChannelID, []string{QueueAccountHealth},
	)
	if err != nil {
		t.Fatalf("claim disabled channel: %v", err)
	}
	if item != nil {
		t.Fatalf("claimed disabled channel item: %#v", item)
	}

	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_profiles
		SET enabled = TRUE, halted_at = NOW()
		WHERE id = $1::uuid
	`, fixture.ChannelID); err != nil {
		t.Fatalf("halt channel: %v", err)
	}
	item, err = fixture.Store.ClaimNextForKinds(ctx, "halted-worker", []string{QueueAccountHealth})
	if err != nil {
		t.Fatalf("claim halted channel: %v", err)
	}
	if item != nil {
		t.Fatalf("claimed halted channel item: %#v", item)
	}

	globalID, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueCleanupExpired,
		IdempotencyKey: "cleanup_expired:global-channel-state-test:" + time.Now().UTC().Format("20060102150405.000000000"),
		Payload:        map[string]any{},
	})
	if err != nil {
		t.Fatalf("Enqueue global item: %v", err)
	}
	defer func() {
		_, _ = fixture.Store.Pool.Exec(ctx, `
			DELETE FROM channel_ops_queue_items WHERE id = $1::uuid
		`, globalID)
	}()
	item, err = fixture.Store.ClaimNextForKinds(ctx, "global-worker", []string{QueueCleanupExpired})
	if err != nil {
		t.Fatalf("claim global item: %v", err)
	}
	if item == nil || item.ID != globalID {
		t.Fatalf("global claim = %#v, want %s", item, globalID)
	}
}

func TestQueueLeasePreventsStaleSuccessAndRetryAfterDeadLetter(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	for _, completion := range []string{"success", "retry"} {
		t.Run(completion, func(t *testing.T) {
			channelID := fixture.ChannelID
			_, err := fixture.Store.Enqueue(ctx, EnqueueOptions{
				Kind:             QueueAccountHealth,
				IdempotencyKey:   "account_health:" + fixture.AccountID + ":stale-" + completion,
				Payload:          map[string]any{"account_id": fixture.AccountID},
				ChannelProfileID: &channelID,
			})
			if err != nil {
				t.Fatalf("Enqueue: %v", err)
			}
			item, err := fixture.Store.ClaimNextForKinds(ctx, "lease-worker", []string{QueueAccountHealth})
			if err != nil {
				t.Fatalf("ClaimNextForKinds: %v", err)
			}
			if item == nil || item.LockedAt == nil || item.LockedBy == nil {
				t.Fatalf("claimed item has no running lease: %#v", item)
			}
			if _, err := fixture.Store.Pool.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2, dead_letter_at = NOW(), locked_by = NULL, locked_at = NULL
				WHERE id = $1::uuid
			`, item.ID, QueueStatusDeadLettered); err != nil {
				t.Fatalf("dead-letter claimed item: %v", err)
			}

			switch completion {
			case "success":
				err = fixture.Store.MarkQueueDone(ctx, *item)
			case "retry":
				err = fixture.Store.MarkQueueFailedOrRetry(ctx, *item, "stale runner failure")
			}
			if err != nil {
				t.Fatalf("stale %s completion: %v", completion, err)
			}

			var status string
			var lockedBy *string
			var lockedAt *time.Time
			var deadLetterAt *time.Time
			if err := fixture.Store.Pool.QueryRow(ctx, `
				SELECT status, locked_by, locked_at, dead_letter_at
				FROM channel_ops_queue_items
				WHERE id = $1::uuid
			`, item.ID).Scan(&status, &lockedBy, &lockedAt, &deadLetterAt); err != nil {
				t.Fatalf("select queue item: %v", err)
			}
			if status != QueueStatusDeadLettered || lockedBy != nil || lockedAt != nil || deadLetterAt == nil {
				t.Fatalf("queue state after stale %s = %s/%v/%v/%v", completion, status, lockedBy, lockedAt, deadLetterAt)
			}
		})
	}
}
