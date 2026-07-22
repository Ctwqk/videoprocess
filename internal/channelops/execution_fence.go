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
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

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
	if err := dispatch(s.withExecutionDB(tx, channelID)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
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
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := lockExecutableChannel(ctx, tx, channelID, requireOpenIntake); err != nil {
		return err
	}
	if err := dispatch(s.withExecutionDB(tx, &channelID)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
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
