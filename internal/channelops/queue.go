package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const claimNextQuery = `
	WITH picked AS (
		SELECT id
		FROM channel_ops_queue_items
		WHERE status = $2
		  AND dead_letter_at IS NULL
		  AND run_after <= NOW()
		ORDER BY priority ASC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE channel_ops_queue_items q
	SET status = $3,
	    locked_by = $1,
	    locked_at = NOW(),
	    attempt_count = attempt_count + 1
	FROM picked
	WHERE q.id = picked.id
	RETURNING q.id, q.kind, q.idempotency_key, q.payload_json, q.status, q.priority,
	          q.attempt_count, q.max_attempts, q.run_after, q.locked_at, q.locked_by,
	          q.last_error, q.dead_letter_at, q.channel_profile_id, q.parent_queue_item_id
`

const claimNextForKindsQuery = `
	WITH picked AS (
		SELECT id
		FROM channel_ops_queue_items
		WHERE status = $2
		  AND dead_letter_at IS NULL
		  AND run_after <= NOW()
		  AND kind = ANY($4)
		ORDER BY priority ASC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE channel_ops_queue_items q
	SET status = $3,
	    locked_by = $1,
	    locked_at = NOW(),
	    attempt_count = attempt_count + 1
	FROM picked
	WHERE q.id = picked.id
	RETURNING q.id, q.kind, q.idempotency_key, q.payload_json, q.status, q.priority,
	          q.attempt_count, q.max_attempts, q.run_after, q.locked_at, q.locked_by,
	          q.last_error, q.dead_letter_at, q.channel_profile_id, q.parent_queue_item_id
`

func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 5 * time.Minute
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= 30*time.Minute {
			return 30 * time.Minute
		}
	}
	return delay
}

func ShouldDeadLetter(attemptCount int, maxAttempts int) bool {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	return attemptCount >= maxAttempts
}

type EnqueueOptions struct {
	Kind              string
	IdempotencyKey    string
	Payload           map[string]any
	Priority          int
	RunAfter          time.Time
	ChannelProfileID  *string
	ParentQueueItemID *string
	MaxAttempts       int
}

func (s *Store) Enqueue(ctx context.Context, opts EnqueueOptions) (string, error) {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 3
	}
	if opts.RunAfter.IsZero() {
		opts.RunAfter = s.Now().UTC()
	}
	payload, err := json.Marshal(jsonObject(opts.Payload))
	if err != nil {
		return "", err
	}

	var id string
	err = s.Pool.QueryRow(ctx, `
		INSERT INTO channel_ops_queue_items
			(id, kind, idempotency_key, payload_json, status, priority, run_after, attempt_count,
			 max_attempts, channel_profile_id, parent_queue_item_id)
		VALUES (gen_random_uuid(), $1, $2, $3::jsonb, $4, $5, $6, 0, $7, $8, $9)
		ON CONFLICT (idempotency_key) DO UPDATE
		SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id
	`, opts.Kind, opts.IdempotencyKey, payload, QueueStatusQueued, opts.Priority, opts.RunAfter,
		opts.MaxAttempts, opts.ChannelProfileID, opts.ParentQueueItemID).Scan(&id)
	return id, err
}

func (s *Store) ClaimNext(ctx context.Context, workerID string) (*QueueItemRow, error) {
	row := s.Pool.QueryRow(ctx, claimNextQuery, workerID, QueueStatusQueued, QueueStatusRunning)

	item, err := scanQueueItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Store) ClaimNextForKinds(ctx context.Context, workerID string, kinds []string) (*QueueItemRow, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	row := s.Pool.QueryRow(ctx, claimNextForKindsQuery, workerID, QueueStatusQueued, QueueStatusRunning, kinds)

	item, err := scanQueueItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Store) MarkQueueDone(ctx context.Context, id string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    last_error = NULL,
		    locked_by = NULL,
		    locked_at = NULL
		WHERE id = $1
	`, id, QueueStatusSucceeded)
	return err
}

func (s *Store) MarkQueueFailedOrRetry(ctx context.Context, item QueueItemRow, message string) error {
	if ShouldDeadLetter(item.AttemptCount, item.MaxAttempts) {
		_, err := s.Pool.Exec(ctx, `
			UPDATE channel_ops_queue_items
			SET status = $2,
			    last_error = $3,
			    dead_letter_at = NOW(),
			    locked_by = NULL,
			    locked_at = NULL
			WHERE id = $1
		`, item.ID, QueueStatusDeadLettered, message)
		return err
	}

	_, err := s.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    last_error = $3,
		    run_after = NOW() + $4::interval,
		    locked_by = NULL,
		    locked_at = NULL
		WHERE id = $1
	`, item.ID, QueueStatusQueued, message, pgInterval(RetryDelay(item.AttemptCount)))
	return err
}

type queueItemScanner interface {
	Scan(dest ...any) error
}

func scanQueueItem(row queueItemScanner) (*QueueItemRow, error) {
	var item QueueItemRow
	var payloadBytes []byte
	if err := row.Scan(&item.ID, &item.Kind, &item.IdempotencyKey, &payloadBytes, &item.Status,
		&item.Priority, &item.AttemptCount, &item.MaxAttempts, &item.RunAfter, &item.LockedAt,
		&item.LockedBy, &item.LastError, &item.DeadLetterAt, &item.ChannelProfileID,
		&item.ParentQueueItemID); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(payloadBytes, &item.PayloadJSON); err != nil {
		return nil, err
	}
	return &item, nil
}

func pgInterval(d time.Duration) string {
	return fmt.Sprintf("%f seconds", d.Seconds())
}
