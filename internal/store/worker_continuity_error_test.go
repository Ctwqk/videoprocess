package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestContinuityRunningRequiresExactPostgresMetadata(t *testing.T) {
	for _, test := range []struct {
		err   error
		retry bool
	}{
		{&pgconn.PgError{Code: "P0001", Message: "worker_redis_continuity_running"}, true},
		{fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "P0001", Message: "worker_redis_continuity_running"}), true},
		{&pgconn.PgError{Code: "08006", Message: "worker_redis_continuity_running"}, false},
		{&pgconn.PgError{Code: "P0001", Message: "prefix worker_redis_continuity_running"}, false},
		{&pgconn.PgError{Code: "P0001", Message: "worker_redis_continuity_running suffix"}, false},
		{errors.New("worker_redis_continuity_running"), false},
		{&WorkerRegistrationError{Code: "worker_redis_continuity_running"}, false},
	} {
		if got := errors.Is(workerRedisContinuityError(test.err), ErrWorkerRedisContinuityRunning); got != test.retry {
			t.Fatalf("structured running classification=%v; want %v", got, test.retry)
		}
	}
}
