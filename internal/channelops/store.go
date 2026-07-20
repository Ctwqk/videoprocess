package channelops

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Pool               *pgxpool.Pool
	Now                func() time.Time
	DefaultMaxAttempts int
	executionDB        dbExecutor
	executionChannelID *string
}

type dbExecutor interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func OpenStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool, Now: func() time.Time { return time.Now().UTC() }, DefaultMaxAttempts: 3}, nil
}

func (s *Store) Close() {
	if s != nil && s.Pool != nil {
		s.Pool.Close()
	}
}

func (s *Store) db() dbExecutor {
	if s != nil && s.executionDB != nil {
		return s.executionDB
	}
	return s.Pool
}

func (s *Store) withExecutionDB(db dbExecutor, channelID *string) *Store {
	clone := *s
	clone.executionDB = db
	clone.executionChannelID = channelID
	return &clone
}

func (s *Store) beginOrReuse(ctx context.Context) (pgx.Tx, bool, error) {
	if tx, ok := s.executionDB.(pgx.Tx); ok {
		return tx, false, nil
	}
	tx, err := s.Pool.Begin(ctx)
	return tx, true, err
}

func (s *Store) hasExecutionTransaction() bool {
	if s == nil {
		return false
	}
	_, ok := s.executionDB.(pgx.Tx)
	return ok
}

func UTCBucket(now time.Time) string {
	return now.UTC().Format("2006-01-02-15")
}

func Transition(from string, to string, reason string, at time.Time) map[string]any {
	return map[string]any{
		"from":   from,
		"to":     to,
		"reason": reason,
		"at":     at.UTC().Format(time.RFC3339),
	}
}

func jsonObject(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}

func stringSlice(value []string) []string {
	if value == nil {
		return []string{}
	}
	return value
}
