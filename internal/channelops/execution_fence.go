package channelops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrChannelExecutionBlocked = errors.New("channel execution blocked")
	ErrQueueAuthorityInvalid   = errors.New("queue authority invalid")
)

func (s *Store) WithQueueExecutionFence(ctx context.Context, item QueueItemRow, dispatch func(*Store) error) error {
	return s.withExecutionTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.assertLeaderAuthority(ctx, tx, false); err != nil {
			return err
		}
		if err := assertQueueLease(ctx, tx, item); err != nil {
			return err
		}

		channelID, err := resolveQueueAuthority(ctx, tx, item)
		if err != nil {
			return err
		}
		if channelID != nil {
			if item.ChannelProfileID == nil {
				return fmt.Errorf(
					"%w: stored channel is null, authoritative channel %s",
					ErrQueueAuthorityInvalid,
					*channelID,
				)
			}
			if !strings.EqualFold(*item.ChannelProfileID, *channelID) {
				return fmt.Errorf(
					"%w: queue authority mismatch: stored channel %s, authoritative channel %s",
					ErrQueueAuthorityInvalid,
					*item.ChannelProfileID,
					*channelID,
				)
			}
			if err := lockExecutableChannel(ctx, tx, *channelID, queueKindRequiresOpenIntake(item.Kind)); err != nil {
				return err
			}
		}
		return dispatch(s.withExecutionDB(tx, channelID))
	})
}

func assertQueueLease(ctx context.Context, db dbExecutor, item QueueItemRow) error {
	lockedBy, lockedAt, err := runningLease(item)
	if err != nil {
		return errors.Join(ErrQueueLeaseLost, err)
	}
	var found bool
	err = db.QueryRow(ctx, `
		SELECT TRUE
		FROM channel_ops_queue_items
		WHERE id = $1::uuid
		  AND status = $2
		  AND locked_by = $3
		  AND locked_at = $4
		FOR UPDATE
	`, item.ID, QueueStatusRunning, lockedBy, lockedAt).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrQueueLeaseLost
	}
	if err != nil {
		return err
	}
	if !found {
		return ErrQueueLeaseLost
	}
	return nil
}

func (s *Store) WithChannelExecutionFence(ctx context.Context, channelID string, dispatch func(*Store) error) error {
	return s.withChannelExecutionFence(ctx, channelID, false, dispatch)
}

func (s *Store) withChannelExecutionFence(
	ctx context.Context,
	channelID string,
	requireOpenIntake bool,
	dispatch func(*Store) error,
) error {
	if err := requireUUID("channel_profile_id", channelID); err != nil {
		return fmt.Errorf("%w: %v", ErrChannelExecutionBlocked, err)
	}
	return s.withExecutionTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.assertLeaderAuthority(ctx, tx, false); err != nil {
			return err
		}
		if err := lockExecutableChannel(ctx, tx, channelID, requireOpenIntake); err != nil {
			return err
		}
		return dispatch(s.withExecutionDB(tx, &channelID))
	})
}

func (s *Store) withExecutionTransaction(
	ctx context.Context,
	dispatch func(pgx.Tx) error,
) error {
	tx, ownsTransaction, err := s.beginOrReuse(ctx)
	if err != nil {
		return err
	}
	if ownsTransaction {
		defer func() {
			_ = tx.Rollback(ctx)
		}()
	}
	if err := dispatch(tx); err != nil {
		return err
	}
	if !ownsTransaction {
		return nil
	}
	return tx.Commit(ctx)
}

func resolveQueueAuthority(ctx context.Context, db dbExecutor, item QueueItemRow) (*string, error) {
	var channelID string
	var err error
	switch item.Kind {
	case QueuePlanTask, QueueExecuteTask, QueueObserveJob, QueuePublishTask:
		taskID := firstString(item.PayloadJSON, "production_task_id")
		if err := requireUUID("production_task_id", taskID); err != nil {
			return nil, queueAuthorityError(item, err)
		}
		err = db.QueryRow(ctx, `
			SELECT channel_profile_id FROM production_tasks WHERE id = $1::uuid
		`, taskID).Scan(&channelID)
	case QueuePromotePublication, QueueReconcilePublication, QueueCollectMetrics:
		publicationID := firstString(item.PayloadJSON, "publication_id")
		if err := requireUUID("publication_id", publicationID); err != nil {
			return nil, queueAuthorityError(item, err)
		}
		err = db.QueryRow(ctx, `
			SELECT task.channel_profile_id
			FROM publication_records AS publication
			JOIN production_tasks AS task ON task.id = publication.production_task_id
			WHERE publication.id = $1::uuid
		`, publicationID).Scan(&channelID)
	case QueueAccountHealth:
		accountID := firstString(item.PayloadJSON, "account_id")
		if err := requireUUID("account_id", accountID); err != nil {
			return nil, queueAuthorityError(item, err)
		}
		err = db.QueryRow(ctx, `
			SELECT channel_profile_id FROM publishing_accounts WHERE id = $1::uuid
		`, accountID).Scan(&channelID)
	case QueueAgentTick, QueueLearningRecompute, QueueIngestDiscovery:
		channelID = firstString(item.PayloadJSON, "channel_id")
		if err := requireUUID("channel_id", channelID); err != nil {
			return nil, queueAuthorityError(item, err)
		}
		err = db.QueryRow(ctx, `
			SELECT id::text FROM channel_profiles WHERE id = $1::uuid
		`, channelID).Scan(&channelID)
	case QueueSendAlert:
		channelID = firstString(item.PayloadJSON, "channel_id")
		if channelID == "" {
			if item.ChannelProfileID == nil {
				return nil, nil
			}
			channelID = *item.ChannelProfileID
		}
		if err := requireUUID("channel_id", channelID); err != nil {
			return nil, queueAuthorityError(item, err)
		}
		err = db.QueryRow(ctx, `
			SELECT id::text FROM channel_profiles WHERE id = $1::uuid
		`, channelID).Scan(&channelID)
	case QueueCleanupExpired:
		return nil, nil
	default:
		return nil, queueAuthorityError(item, fmt.Errorf("unsupported queue kind %s", item.Kind))
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, queueAuthorityError(item, errors.New("referenced row is missing"))
	}
	if err != nil {
		return nil, err
	}
	return &channelID, nil
}

func queueAuthorityError(item QueueItemRow, cause error) error {
	return fmt.Errorf("%w: queue authority unresolved for %s %s: %v", ErrQueueAuthorityInvalid, item.Kind, item.ID, cause)
}

func lockExecutableChannel(ctx context.Context, db dbExecutor, channelID string, requireOpenIntake bool) error {
	var enabled bool
	var haltedAt *time.Time
	var intakePausedAt *time.Time
	err := db.QueryRow(ctx, `
		SELECT enabled, halted_at, intake_paused_at
		FROM channel_profiles
		WHERE id = $1::uuid
		FOR UPDATE
	`, channelID).Scan(&enabled, &haltedAt, &intakePausedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: channel %s is missing", ErrChannelExecutionBlocked, channelID)
	}
	if err != nil {
		return err
	}
	if !enabled {
		return fmt.Errorf("%w: channel %s is disabled", ErrChannelExecutionBlocked, channelID)
	}
	if haltedAt != nil {
		return fmt.Errorf("%w: channel %s is halted", ErrChannelExecutionBlocked, channelID)
	}
	if requireOpenIntake && intakePausedAt != nil {
		return fmt.Errorf("%w: channel %s intake is paused", ErrChannelExecutionBlocked, channelID)
	}
	return nil
}
