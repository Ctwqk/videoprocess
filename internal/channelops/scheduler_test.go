package channelops

import (
	"context"
	"testing"
	"time"
)

func TestChannelDueForTick(t *testing.T) {
	now := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	channel := ChannelProfileRow{Enabled: true, TickIntervalMinutes: 60}
	if !ChannelDueForTick(channel, now.Add(31*time.Minute)) {
		t.Fatal("enabled channel should be eligible on every scheduler pass; bucket idempotency handles cadence")
	}

	channel.Enabled = false
	if ChannelDueForTick(channel, now) {
		t.Fatal("disabled channel should not be due")
	}

	channel.Enabled = true
	halted := now.Add(-time.Hour)
	channel.HaltedAt = &halted
	if ChannelDueForTick(channel, now) {
		t.Fatal("halted channel should not be due")
	}
}

func TestTickIdempotencyKey(t *testing.T) {
	got := TickIdempotencyKey("channel-1", "2026-05-21-18")
	if got != "agent_tick:channel-1:2026-05-21-18" {
		t.Fatalf("key = %s", got)
	}
}

func TestSchedulerBucketMatchesPythonCadence(t *testing.T) {
	now := time.Date(2026, 5, 19, 10, 42, 33, 0, time.UTC)
	cases := []struct {
		interval int
		want     string
	}{
		{interval: 15, want: "2026-05-19-10-30"},
		{interval: 30, want: "2026-05-19-10-30"},
		{interval: 60, want: "2026-05-19-10"},
		{interval: 240, want: "2026-05-19-08"},
		{interval: 5, want: "2026-05-19-10-30"},
	}
	for _, tc := range cases {
		if got := SchedulerBucket(now, tc.interval); got != tc.want {
			t.Fatalf("SchedulerBucket(%d) = %q, want %q", tc.interval, got, tc.want)
		}
	}
}

func TestSchedulerBucketDoesNotRestartNonDivisorIntervalsAtUTCMidnight(t *testing.T) {
	beforeMidnight := time.Date(2026, 7, 21, 23, 30, 0, 0, time.UTC)
	afterMidnight := time.Date(2026, 7, 22, 0, 1, 0, 0, time.UTC)

	beforeBucket := SchedulerBucket(beforeMidnight, 1000)
	afterBucket := SchedulerBucket(afterMidnight, 1000)
	if afterBucket != beforeBucket {
		t.Fatalf("1000-minute bucket restarted at UTC midnight: before = %q, after = %q", beforeBucket, afterBucket)
	}
}

func TestSchedulerRunOnceUsesIntervalAwareBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	fixture.SetTickInterval(ctx, 15)
	scheduler := Scheduler{Store: fixture.Store}

	first := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	second := first.Add(15 * time.Minute)
	if got, err := scheduler.RunOnce(ctx, first); err != nil || got != 1 {
		t.Fatalf("RunOnce first = %d, %v", got, err)
	}
	if got, err := scheduler.RunOnce(ctx, second); err != nil || got != 1 {
		t.Fatalf("RunOnce second same hour = %d, %v", got, err)
	}

	var count int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_ops_queue_items
		WHERE channel_profile_id = $1::uuid
		  AND kind = $2
		  AND payload_json ->> 'bucket' IN ('2026-05-21-18-00', '2026-05-21-18-15')
		  AND payload_json ->> 'scheduler_bucket' IN ('2026-05-21-18-00', '2026-05-21-18-15')
	`, fixture.ChannelID, QueueAgentTick).Scan(&count); err != nil {
		t.Fatalf("count scheduler queue items: %v", err)
	}
	if count != 2 {
		t.Fatalf("agent_tick rows with interval buckets = %d, want 2", count)
	}
}

func TestSchedulerRunOnceDoesNotRepeatSameFourHourBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	fixture.SetTickInterval(ctx, 240)
	scheduler := Scheduler{Store: fixture.Store}

	first := time.Date(2026, 5, 21, 8, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)
	if got, err := scheduler.RunOnce(ctx, first); err != nil || got != 1 {
		t.Fatalf("RunOnce first = %d, %v", got, err)
	}
	if got, err := scheduler.RunOnce(ctx, second); err != nil || got != 0 {
		t.Fatalf("RunOnce second same 4h bucket = %d, %v", got, err)
	}
}

func TestSchedulerRunOnceEnqueuesOperationalMaintenance(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	scheduler := Scheduler{Store: fixture.Store}
	now := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)

	if _, err := scheduler.RunOnce(ctx, now); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	for _, want := range []string{QueueCleanupExpired, QueueLearningRecompute} {
		var count int
		if err := fixture.Store.Pool.QueryRow(ctx, `
			SELECT count(*)
			FROM channel_ops_queue_items
			WHERE kind = $1
		`, want).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", want, err)
		}
		if count != 1 {
			t.Fatalf("%s queue count = %d, want 1", want, count)
		}
	}
}

func TestSchedulerRunOnceSchedulesEnabledDiscoveryOncePerPolicyBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	fixture.SetDiscoveryContentMix(ctx, `{"youtube_discovery":{"enabled":true}}`)
	scheduler := Scheduler{Store: fixture.Store}
	first := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	second := first.Add(time.Hour)

	if got, err := scheduler.RunOnce(ctx, first); err != nil || got != 1 {
		t.Fatalf("RunOnce first = %d, %v", got, err)
	}
	if got, err := scheduler.RunOnce(ctx, second); err != nil || got != 1 {
		t.Fatalf("RunOnce second = %d, %v", got, err)
	}

	bucket := SchedulerBucket(first, 360)
	var count, priority int
	var key, source, payloadChannel, schedulerBucket string
	var hasBucket bool
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*), min(priority), min(idempotency_key), min(payload_json ->> 'source'),
		       min(payload_json ->> 'channel_id'), min(payload_json ->> 'scheduler_bucket'),
		       bool_or(payload_json::jsonb ? 'bucket')
		FROM channel_ops_queue_items
		WHERE kind = $1
	`, QueueIngestDiscovery).Scan(&count, &priority, &key, &source, &payloadChannel, &schedulerBucket, &hasBucket); err != nil {
		t.Fatalf("select discovery queue item: %v", err)
	}
	if count != 1 || priority != 80 || key != DiscoveryIdempotencyKey(fixture.ChannelID, "youtube_search", bucket) || source != "youtube_search" || payloadChannel != fixture.ChannelID || schedulerBucket != bucket || !hasBucket {
		t.Fatalf("discovery row count/priority/key/source/channel/scheduler_bucket/has_bucket = %d/%d/%q/%q/%q/%q/%t", count, priority, key, source, payloadChannel, schedulerBucket, hasBucket)
	}

	var agentTicks int
	if err := fixture.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM channel_ops_queue_items WHERE kind = $1`, QueueAgentTick).Scan(&agentTicks); err != nil {
		t.Fatalf("count agent ticks: %v", err)
	}
	if agentTicks != 2 {
		t.Fatalf("agent tick count = %d, want 2", agentTicks)
	}
}

func TestSchedulerRunOnceDiscoveryFailClosesWithoutChangingAgentTick(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, tt := range []struct {
		name       string
		contentMix string
	}{
		{name: "default disabled", contentMix: `{}`},
		{name: "invalid enabled policy", contentMix: `{"youtube_discovery":{"enabled":true,"interval_minutes":59}}`},
		{name: "decimal interval remains invalid through jsonb", contentMix: `{"youtube_discovery":{"enabled":true,"interval_minutes":360.0}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			fixture.SetDiscoveryContentMix(ctx, tt.contentMix)

			if got, err := (Scheduler{Store: fixture.Store}).RunOnce(ctx, fixture.Store.Now()); err != nil || got != 1 {
				t.Fatalf("RunOnce = %d, %v", got, err)
			}
			for _, kind := range []string{QueueAgentTick, QueueIngestDiscovery} {
				var count int
				if err := fixture.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM channel_ops_queue_items WHERE kind = $1`, kind).Scan(&count); err != nil {
					t.Fatalf("count %s: %v", kind, err)
				}
				want := 0
				if kind == QueueAgentTick {
					want = 1
				}
				if count != want {
					t.Fatalf("%s count = %d, want %d", kind, count, want)
				}
			}
		})
	}
}
