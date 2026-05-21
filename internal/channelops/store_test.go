package channelops

import (
	"testing"
	"time"
)

func TestUTCBucket(t *testing.T) {
	now := time.Date(2026, 5, 21, 10, 42, 33, 0, time.FixedZone("PDT", -7*3600))
	got := UTCBucket(now)
	if got != "2026-05-21-17" {
		t.Fatalf("UTCBucket = %q", got)
	}
}

func TestTransitionPayload(t *testing.T) {
	at := time.Date(2026, 5, 21, 17, 0, 0, 0, time.UTC)
	got := Transition("selected", "planning", "plan_task", at)
	if got["from"] != "selected" || got["to"] != "planning" || got["reason"] != "plan_task" {
		t.Fatalf("unexpected transition: %#v", got)
	}
	if got["at"] != "2026-05-21T17:00:00Z" {
		t.Fatalf("unexpected transition timestamp: %#v", got["at"])
	}
}

func TestQueueRowFieldNamesAndStatuses(t *testing.T) {
	lockedAt := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	lockedBy := "runner-1"
	lastError := "temporary failure"
	deadLetterAt := lockedAt.Add(time.Hour)

	queue := QueueItemRow{
		Status:       QueueStatusQueued,
		AttemptCount: 1,
		MaxAttempts:  3,
		RunAfter:     lockedAt,
		LockedAt:     &lockedAt,
		LockedBy:     &lockedBy,
		LastError:    &lastError,
		DeadLetterAt: &deadLetterAt,
	}
	if queue.Status != "queued" || queue.AttemptCount != 1 {
		t.Fatalf("unexpected queue row: %#v", queue)
	}
	if QueueStatusRunning != "running" ||
		QueueStatusSucceeded != "succeeded" ||
		QueueStatusFailed != "failed" ||
		QueueStatusDeadLettered != "dead_lettered" {
		t.Fatal("queue status constants drifted from Python model values")
	}

	task := ProductionTaskRow{ChannelConfigVersionSnapshot: 1}
	if task.ChannelConfigVersionSnapshot != 1 {
		t.Fatalf("unexpected production task row: %#v", task)
	}
}
