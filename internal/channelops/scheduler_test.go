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
