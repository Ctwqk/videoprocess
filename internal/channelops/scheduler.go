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
	return true
}

func SchedulerBucket(now time.Time, intervalMinutes int) string {
	current := now.UTC()
	interval := normalizedTickIntervalMinutes(intervalMinutes)
	intervalSeconds := int64(interval * 60)
	bucketTime := time.Unix((current.Unix()/intervalSeconds)*intervalSeconds, 0).UTC()
	bucketMinute := bucketTime.Minute()
	if interval >= 60 && bucketMinute == 0 {
		return bucketTime.Format("2006-01-02-15")
	}
	return bucketTime.Format("2006-01-02-15-04")
}

func normalizedTickIntervalMinutes(value int) int {
	if value <= 0 {
		value = 60
	}
	if value < 15 {
		return 15
	}
	return value
}

func TickIdempotencyKey(channelID string, bucket string) string {
	return fmt.Sprintf("agent_tick:%s:%s", channelID, bucket)
}

func CleanupIdempotencyKey(day string) string {
	return fmt.Sprintf("cleanup_expired:%s", day)
}

func LearningRecomputeIdempotencyKey(channelID string, bucket string) string {
	return fmt.Sprintf("learning_recompute:%s:%s", channelID, bucket)
}

func DiscoveryIdempotencyKey(channelID string, source string, bucket string) string {
	return fmt.Sprintf("ingest_discovery:%s:%s:%s", channelID, source, bucket)
}

type Scheduler struct {
	Store *Store
}

// RunOnce returns newly scheduled agent ticks; discovery and operational maintenance are intentionally excluded.
func (s Scheduler) RunOnce(ctx context.Context, now time.Time) (int, error) {
	channels, err := s.Store.ListSchedulableChannels(ctx, now)
	if err != nil {
		return 0, err
	}

	enqueued := 0
	for _, channel := range channels {
		if !ChannelDueForTick(channel, now) {
			continue
		}
		if err := s.enqueueDiscovery(ctx, channel, now); err != nil {
			return enqueued, err
		}
		bucket := SchedulerBucket(now, channel.TickIntervalMinutes)
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
			Payload:          map[string]any{"channel_id": channel.ID, "bucket": bucket, "scheduler_bucket": bucket},
			Priority:         100,
			ChannelProfileID: &channelID,
		})
		if err != nil {
			return enqueued, err
		}
		enqueued++
	}
	if err := s.enqueueOperationalMaintenance(ctx, channels, now); err != nil {
		return enqueued, err
	}
	return enqueued, nil
}

func (s Scheduler) enqueueDiscovery(ctx context.Context, channel ChannelProfileRow, now time.Time) error {
	policy, err := DiscoveryPolicyFromContentMix(channel.ContentMixPolicyJSON)
	if err != nil || !policy.Enabled {
		return nil
	}
	bucket := SchedulerBucket(now, policy.IntervalMinutes)
	channelID := channel.ID
	_, err = s.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueIngestDiscovery,
		IdempotencyKey: DiscoveryIdempotencyKey(channel.ID, discoverySourceYouTubeSearch, bucket),
		Payload: map[string]any{
			"channel_id":       channel.ID,
			"source":           discoverySourceYouTubeSearch,
			"bucket":           bucket,
			"scheduler_bucket": bucket,
		},
		Priority:         80,
		ChannelProfileID: &channelID,
	})
	return err
}

func (s Scheduler) enqueueOperationalMaintenance(ctx context.Context, channels []ChannelProfileRow, now time.Time) error {
	if s.Store == nil {
		return nil
	}
	day := now.UTC().Format("2006-01-02")
	if _, err := s.Store.Enqueue(ctx, EnqueueOptions{
		Kind:           QueueCleanupExpired,
		IdempotencyKey: CleanupIdempotencyKey(day),
		Payload:        map[string]any{"day": day},
		Priority:       200,
	}); err != nil {
		return err
	}
	bucket := SchedulerBucket(now, 360)
	for _, channel := range channels {
		if !ChannelDueForTick(channel, now) {
			continue
		}
		channelID := channel.ID
		if _, err := s.Store.Enqueue(ctx, EnqueueOptions{
			Kind:             QueueLearningRecompute,
			IdempotencyKey:   LearningRecomputeIdempotencyKey(channel.ID, bucket),
			Payload:          map[string]any{"channel_id": channel.ID, "bucket": bucket, "window_days": []int{7, 30}},
			Priority:         180,
			ChannelProfileID: &channelID,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ListSchedulableChannels(ctx context.Context, now time.Time) ([]ChannelProfileRow, error) {
	rows, err := s.db().Query(ctx, `
		SELECT id, enabled, dry_run, halted_at, tick_interval_minutes, config_version,
		       risk_policy_json, cadence_policy_json, content_mix_policy_json,
		       default_aspect_ratio, created_at, updated_at
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
			&row.DefaultAspectRatio, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) InsertSchedulerRun(ctx context.Context, channelID string, bucket string) (bool, error) {
	tag, err := s.db().Exec(ctx, `
		INSERT INTO internal_scheduler_runs (channel_profile_id, bucket, status, metadata_json)
		VALUES ($1, $2, 'succeeded', '{}'::jsonb)
		ON CONFLICT (channel_profile_id, bucket) DO NOTHING
	`, channelID, bucket)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
