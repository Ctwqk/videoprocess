package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const queueAuthorityCTE = `
	WITH queue_references AS (
		SELECT
			q.id,
			q.kind,
			q.channel_profile_id AS stored_channel_id,
			CASE
				WHEN q.payload_json ->> 'production_task_id' ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
				THEN (q.payload_json ->> 'production_task_id')::uuid
			END AS task_id,
			CASE
				WHEN q.payload_json ->> 'publication_id' ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
				THEN (q.payload_json ->> 'publication_id')::uuid
			END AS publication_id,
			CASE
				WHEN q.payload_json ->> 'account_id' ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
				THEN (q.payload_json ->> 'account_id')::uuid
			END AS account_id,
			CASE
				WHEN q.payload_json ->> 'channel_id' ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
				THEN (q.payload_json ->> 'channel_id')::uuid
			END AS payload_channel_id,
			NULLIF(BTRIM(q.payload_json ->> 'channel_id'), '') AS payload_channel_value
		FROM channel_ops_queue_items AS q
	), authoritative_queue_channels AS (
		SELECT
			refs.id,
			CASE
				WHEN refs.kind IN ('plan_task', 'execute_task', 'observe_job', 'publish_task')
					THEN task.channel_profile_id
				WHEN refs.kind IN ('promote_publication', 'reconcile_publication', 'collect_metrics')
					THEN publication_task.channel_profile_id
				WHEN refs.kind = 'account_health' THEN account.channel_profile_id
				WHEN refs.kind IN ('agent_tick', 'learning_recompute', 'ingest_discovery') THEN payload_channel.id
				WHEN refs.kind = 'send_alert' THEN
					CASE
						WHEN refs.payload_channel_value IS NULL THEN stored_channel.id
						ELSE payload_channel.id
					END
			END AS authoritative_channel_id,
			refs.kind = 'cleanup_expired'
				OR (
					refs.kind = 'send_alert'
					AND refs.payload_channel_value IS NULL
					AND refs.stored_channel_id IS NULL
				) AS is_global
		FROM queue_references AS refs
		LEFT JOIN production_tasks AS task ON task.id = refs.task_id
		LEFT JOIN publication_records AS publication ON publication.id = refs.publication_id
		LEFT JOIN production_tasks AS publication_task
			ON publication_task.id = publication.production_task_id
		LEFT JOIN publishing_accounts AS account ON account.id = refs.account_id
		LEFT JOIN channel_profiles AS payload_channel ON payload_channel.id = refs.payload_channel_id
		LEFT JOIN channel_profiles AS stored_channel ON stored_channel.id = refs.stored_channel_id
	)
`

const queueAuthorityClaimPredicate = `
		  AND (
			authority.is_global
			OR (
				authority.authoritative_channel_id IS NOT NULL
				AND q.channel_profile_id IS NOT DISTINCT FROM authority.authoritative_channel_id
				AND EXISTS (
					SELECT 1 FROM channel_profiles AS executable_channel
					WHERE executable_channel.id = authority.authoritative_channel_id
					  AND executable_channel.enabled = TRUE
					  AND executable_channel.halted_at IS NULL
				)
			)
			OR (
				NOT authority.is_global
				AND q.kind <> 'ingest_discovery'
				AND (
					authority.authoritative_channel_id IS NULL
					OR q.channel_profile_id IS DISTINCT FROM authority.authoritative_channel_id
				)
			)
		  )
`

const (
	// Keep in sync with backend/app/services/discovery_ingestion.py RUN_STALE_AFTER.
	discoveryLeaseStaleAfter   = 15 * time.Minute
	discoveryLeaseRecoveryCode = "discovery_lease_recovered"
)

const claimNextQuery = queueAuthorityCTE + `
	, picked AS (
		SELECT q.id
		FROM channel_ops_queue_items q
		JOIN authoritative_queue_channels AS authority ON authority.id = q.id
		WHERE q.status = $2
		  AND q.dead_letter_at IS NULL
		  AND q.run_after <= NOW()
		` + queueAuthorityClaimPredicate + `
		ORDER BY q.priority ASC, q.created_at ASC
		FOR UPDATE OF q SKIP LOCKED
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

const claimNextForKindsQuery = queueAuthorityCTE + `
	, picked AS (
		SELECT q.id
		FROM channel_ops_queue_items q
		JOIN authoritative_queue_channels AS authority ON authority.id = q.id
		WHERE q.status = $2
		  AND q.dead_letter_at IS NULL
		  AND q.run_after <= NOW()
		  AND q.kind = ANY($4)
		` + queueAuthorityClaimPredicate + `
		ORDER BY q.priority ASC, q.created_at ASC
		FOR UPDATE OF q SKIP LOCKED
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

const claimNextForChannelAndKindsQuery = queueAuthorityCTE + `
	, picked AS (
		SELECT q.id
		FROM channel_ops_queue_items q
		JOIN authoritative_queue_channels AS authority ON authority.id = q.id
		WHERE q.status = $2
		  AND q.dead_letter_at IS NULL
		  AND q.run_after <= NOW()
		  AND q.kind = ANY($4)
		  AND COALESCE(q.channel_profile_id, authority.authoritative_channel_id) = $5::uuid
		` + queueAuthorityClaimPredicate + `
		ORDER BY q.priority ASC, q.created_at ASC
		FOR UPDATE OF q SKIP LOCKED
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
	return s.enqueue(ctx, s.db(), opts)
}

func (s *Store) enqueue(ctx context.Context, db dbExecutor, opts EnqueueOptions) (string, error) {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = s.defaultMaxAttempts()
	}
	if opts.RunAfter.IsZero() {
		opts.RunAfter = s.Now().UTC()
	}
	payload, err := json.Marshal(jsonObject(opts.Payload))
	if err != nil {
		return "", err
	}

	var id string
	err = db.QueryRow(ctx, `
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

func (s *Store) defaultMaxAttempts() int {
	if s != nil && s.DefaultMaxAttempts > 0 {
		return s.DefaultMaxAttempts
	}
	return 3
}

func (s *Store) ClaimNext(ctx context.Context, workerID string) (*QueueItemRow, error) {
	row := s.db().QueryRow(ctx, claimNextQuery, workerID, QueueStatusQueued, QueueStatusRunning)

	item, err := scanQueueItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Store) recoverStaleDiscoveryLeases(ctx context.Context, now time.Time) (int64, error) {
	current := now.UTC()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	type staleDiscoveryLease struct {
		id           string
		attemptCount int
		maxAttempts  int
	}
	rows, err := tx.Query(ctx, `
		SELECT id::text, attempt_count, max_attempts
		FROM channel_ops_queue_items
		WHERE kind = $1
		  AND status = $2
		  AND (
		    locked_by IS NULL
		    OR BTRIM(locked_by) = ''
		    OR locked_at IS NULL
		    OR locked_at <= $3
		  )
		ORDER BY id
		FOR UPDATE SKIP LOCKED
	`, QueueIngestDiscovery, QueueStatusRunning, current.Add(-discoveryLeaseStaleAfter))
	if err != nil {
		return 0, err
	}
	leases := []staleDiscoveryLease{}
	for rows.Next() {
		var lease staleDiscoveryLease
		if err := rows.Scan(&lease.id, &lease.attemptCount, &lease.maxAttempts); err != nil {
			rows.Close()
			return 0, err
		}
		leases = append(leases, lease)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()

	runStatuses := make(map[string]string, len(leases))
	for _, lease := range leases {
		var status string
		err := tx.QueryRow(ctx, `
			SELECT status
			FROM discovery_ingestion_runs
			WHERE queue_item_id = $1::uuid
			FOR UPDATE
		`, lease.id).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
		}
		runStatuses[lease.id] = status
	}

	var recovered int64
	for _, lease := range leases {
		if runStatuses[lease.id] == "succeeded" {
			result, err := tx.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2,
				    last_error = NULL,
				    dead_letter_at = NULL,
				    locked_by = NULL,
				    locked_at = NULL
				WHERE id = $1::uuid AND status = $3
			`, lease.id, QueueStatusSucceeded, QueueStatusRunning)
			if err := queueLeaseUpdateResult(result.RowsAffected(), err); err != nil {
				return 0, err
			}
			recovered++
			continue
		}

		if runStatuses[lease.id] == "running" {
			result, err := tx.Exec(ctx, `
				UPDATE discovery_ingestion_runs
				SET status = 'failed',
				    finished_at = $2,
				    last_error_code = $3
				WHERE queue_item_id = $1::uuid AND status = 'running'
			`, lease.id, current, discoveryLeaseRecoveryCode)
			if err != nil {
				return 0, err
			}
			if result.RowsAffected() != 1 {
				return 0, fmt.Errorf("discovery run lease lost for queue item %s", lease.id)
			}
		}

		if lease.attemptCount < lease.maxAttempts {
			result, err := tx.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2,
				    run_after = $3,
				    last_error = $4,
				    dead_letter_at = NULL,
				    locked_by = NULL,
				    locked_at = NULL
				WHERE id = $1::uuid AND status = $5
			`, lease.id, QueueStatusQueued, current, discoveryLeaseRecoveryCode, QueueStatusRunning)
			if err := queueLeaseUpdateResult(result.RowsAffected(), err); err != nil {
				return 0, err
			}
		} else {
			result, err := tx.Exec(ctx, `
				UPDATE channel_ops_queue_items
				SET status = $2,
				    last_error = $3,
				    dead_letter_at = $4,
				    locked_by = NULL,
				    locked_at = NULL
				WHERE id = $1::uuid AND status = $5
			`, lease.id, QueueStatusDeadLettered, discoveryLeaseRecoveryCode, current, QueueStatusRunning)
			if err := queueLeaseUpdateResult(result.RowsAffected(), err); err != nil {
				return 0, err
			}
		}
		recovered++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return recovered, nil
}

func (s *Store) ClaimNextForKinds(ctx context.Context, workerID string, kinds []string) (*QueueItemRow, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	row := s.db().QueryRow(ctx, claimNextForKindsQuery, workerID, QueueStatusQueued, QueueStatusRunning, kinds)

	item, err := scanQueueItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Store) ClaimNextForChannelAndKinds(ctx context.Context, workerID string, channelID string, kinds []string) (*QueueItemRow, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	if err := requireUUID("channel_profile_id", channelID); err != nil {
		return nil, err
	}
	row := s.db().QueryRow(ctx, claimNextForChannelAndKindsQuery, workerID, QueueStatusQueued, QueueStatusRunning, kinds, channelID)

	item, err := scanQueueItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Store) MarkQueueDone(ctx context.Context, item QueueItemRow) error {
	lockedBy, lockedAt, err := runningLease(item)
	if err != nil {
		return err
	}
	result, err := s.db().Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    last_error = NULL,
		    locked_by = NULL,
		    locked_at = NULL
		WHERE id = $1::uuid
		  AND status = $3
		  AND locked_by = $4
		  AND locked_at = $5
	`, item.ID, QueueStatusSucceeded, QueueStatusRunning, lockedBy, lockedAt)
	return queueLeaseUpdateResult(result.RowsAffected(), err)
}

func (s *Store) MarkQueueFailedOrRetry(ctx context.Context, item QueueItemRow, message string) error {
	lockedBy, lockedAt, err := runningLease(item)
	if err != nil {
		return err
	}
	if ShouldDeadLetter(item.AttemptCount, item.MaxAttempts) {
		result, err := s.db().Exec(ctx, `
			UPDATE channel_ops_queue_items
			SET status = $2,
			    last_error = $3,
			    dead_letter_at = NOW(),
			    locked_by = NULL,
			    locked_at = NULL
			WHERE id = $1::uuid
			  AND status = $4
			  AND locked_by = $5
			  AND locked_at = $6
		`, item.ID, QueueStatusDeadLettered, message, QueueStatusRunning, lockedBy, lockedAt)
		return queueLeaseUpdateResult(result.RowsAffected(), err)
	}

	result, err := s.db().Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    last_error = $3,
		    run_after = NOW() + $4::interval,
		    locked_by = NULL,
		    locked_at = NULL
		WHERE id = $1::uuid
		  AND status = $5
		  AND locked_by = $6
		  AND locked_at = $7
	`, item.ID, QueueStatusQueued, message, pgInterval(RetryDelay(item.AttemptCount)),
		QueueStatusRunning, lockedBy, lockedAt)
	return queueLeaseUpdateResult(result.RowsAffected(), err)
}

func (s *Store) MarkQueueRejected(ctx context.Context, item QueueItemRow, message string) error {
	lockedBy, lockedAt, err := runningLease(item)
	if err != nil {
		return err
	}
	result, err := s.db().Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $2,
		    last_error = $3,
		    dead_letter_at = NOW(),
		    locked_by = NULL,
		    locked_at = NULL
		WHERE id = $1::uuid
		  AND status = $4
		  AND locked_by = $5
		  AND locked_at = $6
	`, item.ID, QueueStatusDeadLettered, message, QueueStatusRunning, lockedBy, lockedAt)
	return queueLeaseUpdateResult(result.RowsAffected(), err)
}

var ErrQueueLeaseLost = errors.New("queue lease lost")

func queueLeaseUpdateResult(rowsAffected int64, err error) error {
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrQueueLeaseLost
	}
	return nil
}

func runningLease(item QueueItemRow) (string, time.Time, error) {
	if item.Status != QueueStatusRunning || item.LockedBy == nil || strings.TrimSpace(*item.LockedBy) == "" || item.LockedAt == nil {
		return "", time.Time{}, fmt.Errorf("queue item %s has no running lease", item.ID)
	}
	return *item.LockedBy, *item.LockedAt, nil
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
