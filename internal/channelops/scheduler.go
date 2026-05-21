package channelops

import (
	"context"
	"fmt"
	"time"
)

func ChannelDueForTick(channel ChannelProfileRow, now time.Time) bool {
	if !channel.Enabled || channel.HaltedAt != nil {
		return false
	}
	interval := channel.TickIntervalMinutes
	if interval <= 0 {
		interval = 60
	}
	minute := now.UTC().Minute()
	return minute%interval == 0
}

func TickIdempotencyKey(channelID string, bucket string) string {
	return fmt.Sprintf("agent_tick:%s:%s", channelID, bucket)
}

type Scheduler struct {
	Store *Store
}

func (s Scheduler) RunOnce(ctx context.Context, now time.Time) (int, error) {
	channels, err := s.Store.ListSchedulableChannels(ctx, now)
	if err != nil {
		return 0, err
	}

	enqueued := 0
	bucket := UTCBucket(now)
	for _, channel := range channels {
		if !ChannelDueForTick(channel, now) {
			continue
		}
		created, err := s.Store.InsertSchedulerRun(ctx, channel.ID, bucket)
		if err != nil {
			return enqueued, err
		}
		if !created {
			continue
		}

		channelID := channel.ID
		_, err = s.Store.Enqueue(ctx, EnqueueOptions{
			Kind:             QueueAgentTick,
			IdempotencyKey:   TickIdempotencyKey(channel.ID, bucket),
			Payload:          map[string]any{"channel_id": channel.ID, "bucket": bucket},
			Priority:         100,
			ChannelProfileID: &channelID,
		})
		if err != nil {
			return enqueued, err
		}
		enqueued++
	}
	return enqueued, nil
}

func (s *Store) ListSchedulableChannels(ctx context.Context, now time.Time) ([]ChannelProfileRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, enabled, dry_run, halted_at, tick_interval_minutes, config_version,
		       risk_policy_json, cadence_policy_json, content_mix_policy_json,
		       default_aspect_ratio, false AS external_asset_auto_publish, 0 AS max_posts_per_day,
		       created_at, updated_at
		FROM channel_profiles
		WHERE enabled = TRUE AND halted_at IS NULL
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []ChannelProfileRow{}
	for rows.Next() {
		var row ChannelProfileRow
		if err := rows.Scan(&row.ID, &row.Enabled, &row.DryRun, &row.HaltedAt, &row.TickIntervalMinutes,
			&row.ConfigVersion, &row.RiskPolicyJSON, &row.CadencePolicyJSON, &row.ContentMixPolicyJSON,
			&row.DefaultAspectRatio, &row.ExternalAutoPublish, &row.MaxPostsPerDay, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) InsertSchedulerRun(ctx context.Context, channelID string, bucket string) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `
		INSERT INTO internal_scheduler_runs (channel_profile_id, bucket, status, metadata_json)
		VALUES ($1, $2, 'succeeded', '{}'::jsonb)
		ON CONFLICT (channel_profile_id, bucket) DO NOTHING
	`, channelID, bucket)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
