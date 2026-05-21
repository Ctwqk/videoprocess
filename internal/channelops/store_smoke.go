package channelops

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

const liveSmokeQueueLimit = 100

const claimNextLiveSmokeForChannelAndKindsQuery = `
	WITH picked AS (
		SELECT id
		FROM channel_ops_queue_items
		WHERE status = $3
		  AND dead_letter_at IS NULL
		  AND run_after <= NOW()
		  AND channel_profile_id = $1::uuid
		  AND kind = ANY($5)
		ORDER BY priority ASC, created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	UPDATE channel_ops_queue_items q
	SET status = $4,
	    locked_by = $2,
	    locked_at = NOW(),
	    attempt_count = attempt_count + 1
	FROM picked
	WHERE q.id = picked.id
	RETURNING q.id, q.kind, q.idempotency_key, q.payload_json, q.status, q.priority,
	          q.attempt_count, q.max_attempts, q.run_after, q.locked_at, q.locked_by,
	          q.last_error, q.dead_letter_at, q.channel_profile_id, q.parent_queue_item_id
`

func (s *Store) RunLiveSmoke(ctx context.Context, channelID string, handler HandlerService) (SmokeResult, error) {
	if s == nil {
		return SmokeResult{}, errors.New("channelops store is not configured")
	}
	if err := handler.ReadinessError(); err != nil {
		return SmokeResult{}, err
	}
	bucket := UTCBucket(s.Now())
	if err := s.RunTick(ctx, channelID, bucket, handler); err != nil {
		return SmokeResult{}, err
	}
	claimableKinds := handler.ClaimableKinds()
	for i := 0; i < liveSmokeQueueLimit; i++ {
		if err := s.advanceLiveSmokeQueue(ctx, channelID, claimableKinds); err != nil {
			return SmokeResult{}, err
		}
		item, err := s.claimNextLiveSmokeForChannelAndKinds(ctx, channelID, "channelops-live-smoke", claimableKinds)
		if err != nil {
			return SmokeResult{}, err
		}
		if item == nil {
			break
		}
		if err := handler.Handle(ctx, *item); err != nil {
			_ = s.MarkQueueFailedOrRetry(ctx, *item, err.Error())
			return SmokeResult{}, err
		}
		if err := s.MarkQueueDone(ctx, item.ID); err != nil {
			return SmokeResult{}, err
		}
	}
	return s.SmokeResultForChannel(ctx, channelID)
}

func (s *Store) advanceLiveSmokeQueue(ctx context.Context, channelID string, kinds []string) error {
	if len(kinds) == 0 {
		return nil
	}
	_, err := s.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET run_after = NOW()
		WHERE channel_profile_id = $1::uuid
		  AND status = $2
		  AND dead_letter_at IS NULL
		  AND kind = ANY($3)
		  AND run_after > NOW()
	`, channelID, QueueStatusQueued, kinds)
	return err
}

func (s *Store) claimNextLiveSmokeForChannelAndKinds(ctx context.Context, channelID string, workerID string, kinds []string) (*QueueItemRow, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	row := s.Pool.QueryRow(ctx, claimNextLiveSmokeForChannelAndKindsQuery, channelID, workerID, QueueStatusQueued, QueueStatusRunning, kinds)

	item, err := scanQueueItem(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Store) SmokeResultForChannel(ctx context.Context, channelID string) (SmokeResult, error) {
	var result SmokeResult
	err := s.Pool.QueryRow(ctx, `
		SELECT
			EXISTS(
				SELECT 1
				FROM production_tasks
				WHERE channel_profile_id = $1::uuid
				  AND state IN ('scheduled', 'measured')
			),
			EXISTS(
				SELECT 1
				FROM publication_records p
				JOIN production_tasks t ON t.id = p.production_task_id
				WHERE t.channel_profile_id = $1::uuid
				  AND p.current_privacy = 'unlisted'
			),
			EXISTS(
				SELECT 1
				FROM feedback_snapshots f
				JOIN publication_records p ON p.id = f.publication_id
				JOIN production_tasks t ON t.id = p.production_task_id
				WHERE t.channel_profile_id = $1::uuid
			),
			(
				SELECT COUNT(*)
				FROM material_usage_ledger
				WHERE channel_profile_id = $1::uuid
			),
			(
				SELECT COUNT(*)
				FROM takedown_events e
				JOIN publication_records p ON p.id = e.publication_id
				JOIN production_tasks t ON t.id = p.production_task_id
				WHERE t.channel_profile_id = $1::uuid
			)
	`, channelID).Scan(
		&result.TaskScheduled,
		&result.PublicationUnlisted,
		&result.MetricsWritten,
		&result.LedgerRows,
		&result.TakedownRows,
	)
	return result, err
}
