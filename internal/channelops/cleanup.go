package channelops

import (
	"context"
	"time"
)

type RetentionConfig struct {
	QueueDays    int
	AuditDays    int
	FeedbackDays int
}

type CleanupResult struct {
	QueueItemsDeleted        int64
	TickAuditsDeleted        int64
	FeedbackSnapshotsDeleted int64
}

func (s *Store) CleanupExpired(ctx context.Context, now time.Time, cfg RetentionConfig) (CleanupResult, error) {
	if cfg.QueueDays <= 0 {
		cfg.QueueDays = 30
	}
	if cfg.AuditDays <= 0 {
		cfg.AuditDays = 90
	}
	if cfg.FeedbackDays <= 0 {
		cfg.FeedbackDays = 365
	}
	if now.IsZero() {
		now = s.Now().UTC()
	}
	result := CleanupResult{}
	queueBefore := now.UTC().AddDate(0, 0, -cfg.QueueDays)
	auditBefore := now.UTC().AddDate(0, 0, -cfg.AuditDays)
	feedbackBefore := now.UTC().AddDate(0, 0, -cfg.FeedbackDays)

	tag, err := s.Pool.Exec(ctx, `
		DELETE FROM channel_ops_queue_items
		WHERE status IN ($1, $2, 'dead_letter')
		  AND updated_at < $3::timestamp
	`, QueueStatusSucceeded, QueueStatusDeadLettered, queueBefore)
	if err != nil {
		return result, err
	}
	result.QueueItemsDeleted = tag.RowsAffected()

	tag, err = s.Pool.Exec(ctx, `
		DELETE FROM agent_tick_audits
		WHERE started_at < $1::timestamptz
	`, auditBefore)
	if err != nil {
		return result, err
	}
	result.TickAuditsDeleted = tag.RowsAffected()

	tag, err = s.Pool.Exec(ctx, `
		DELETE FROM feedback_snapshots
		WHERE collected_at < $1::timestamptz
	`, feedbackBefore)
	if err != nil {
		return result, err
	}
	result.FeedbackSnapshotsDeleted = tag.RowsAffected()
	return result, nil
}
