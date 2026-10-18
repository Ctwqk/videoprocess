package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ctwqk/videoprocess/internal/store"
)

func TestContinuityWaitRetriesOnlyExactRunning(t *testing.T) {
	for _, test := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"running", store.ErrWorkerRedisContinuityRunning, true},
		{"untrusted_typed_running", &store.WorkerRegistrationError{Code: "worker_redis_continuity_running"}, false},
		{"text_running", errors.New("worker_redis_continuity_running"), false},
		{"stale", &store.WorkerRegistrationError{Code: "worker_redis_continuity_stale"}, false},
		{"error", &store.WorkerRegistrationError{Code: "worker_redis_continuity_error"}, false},
		{"missing", &store.WorkerRegistrationError{Code: "worker_redis_continuity_missing"}, false},
		{"identity", &store.WorkerRegistrationError{Code: "database_principal_mismatch"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := []string{}
			db := &startupDatabaseStub{calls: &calls, continuityErr: test.err}
			db.continuityHook = func() {
				if len(calls) == 2 {
					db.continuityErr = nil
				}
			}
			err := waitWorkerRedisContinuity(context.Background(), db, time.Second, time.Millisecond)
			if test.retry {
				if err != nil || len(calls) != 2 {
					t.Fatalf("retry: err=%v calls=%v", err, calls)
				}
			} else if err == nil || len(calls) != 1 {
				t.Fatalf("non-running: err=%v calls=%v", err, calls)
			}
			if db.continuityAge != 90 {
				t.Fatalf("max age=%d", db.continuityAge)
			}
		})
	}
}

func TestContinuityWaitBudgetAndCancellation(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		calls := []string{}
		ctx, cancel := context.WithCancel(context.Background())
		db := &startupDatabaseStub{calls: &calls, continuityErr: store.ErrWorkerRedisContinuityRunning}
		if cancelParent {
			db.continuityHook = cancel
		}
		err := waitWorkerRedisContinuity(ctx, db, 10*time.Millisecond, time.Millisecond)
		cancel()
		if err == nil || len(calls) == 0 {
			t.Fatalf("err=%v calls=%v", err, calls)
		}
		if cancelParent && len(calls) != 1 {
			t.Fatalf("retried after cancellation: %v", calls)
		}
	}
}

type blockedContinuityDatabase struct{ startupDatabaseStub }

func (s *blockedContinuityDatabase) RequireWorkerRedisContinuity(ctx context.Context, age int) error {
	if age != 90 {
		return errors.New("unexpected freshness bound")
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestContinuityWaitBudgetIncludesDatabaseCall(t *testing.T) {
	started := time.Now()
	err := waitWorkerRedisContinuity(context.Background(), &blockedContinuityDatabase{}, 10*time.Millisecond, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("database wait was not bounded: %v", err)
	}
}
