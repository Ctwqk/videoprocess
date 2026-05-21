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
	if !strings.Contains(claimNextForKindsQuery, "ORDER BY priority ASC, created_at ASC") {
		t.Fatalf("claim query does not match Python ordering:\n%s", claimNextForKindsQuery)
	}
	if strings.Contains(claimNextForKindsQuery, "run_after ASC, created_at") {
		t.Fatalf("claim query should not sort by run_after before created_at:\n%s", claimNextForKindsQuery)
	}
}

func TestClaimNextForChannelAndKindsQueryScopesChannel(t *testing.T) {
	if !strings.Contains(claimNextForChannelAndKindsQuery, "channel_profile_id = $5::uuid") {
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
