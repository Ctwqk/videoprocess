package channelops

import (
	"testing"
	"time"
)

func TestChannelDueForTick(t *testing.T) {
	now := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	channel := ChannelProfileRow{Enabled: true, TickIntervalMinutes: 60}
	if !ChannelDueForTick(channel, now) {
		t.Fatal("enabled hourly channel should be due at hour boundary")
	}

	channel.TickIntervalMinutes = 15
	if !ChannelDueForTick(channel, now.Add(30*time.Minute)) {
		t.Fatal("15-minute channel should be due when UTC minute is divisible by interval")
	}
	if ChannelDueForTick(channel, now.Add(31*time.Minute)) {
		t.Fatal("15-minute channel should not be due when UTC minute is not divisible by interval")
	}

	channel.TickIntervalMinutes = 0
	if !ChannelDueForTick(channel, now) {
		t.Fatal("channel without explicit interval should default to hourly")
	}
	if ChannelDueForTick(channel, now.Add(30*time.Minute)) {
		t.Fatal("channel without explicit interval should not be due away from hour boundary")
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
