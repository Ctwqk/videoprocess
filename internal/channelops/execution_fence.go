package channelops

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrChannelExecutionBlocked = errors.New("channel execution blocked")

func (s *Store) WithChannelExecutionFence(ctx context.Context, channelID string, dispatch func(*Store) error) error {
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

	var enabled bool
	var haltedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT enabled, halted_at
		FROM channel_profiles
		WHERE id = $1::uuid
		FOR UPDATE
	`, channelID).Scan(&enabled, &haltedAt)
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
	if err := dispatch(s.withExecutionDB(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
