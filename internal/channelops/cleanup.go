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

// Upload operations of every status and YouTube publications anchor durable
// evidence. Associated publications remain evidence even if their platform drifts.
const publicationEvidenceCTE = `
	WITH RECURSIVE publication_evidence_tasks AS (
		SELECT production_task_id FROM publication_records WHERE lower(platform) = 'youtube'
		UNION
		SELECT production_task_id FROM youtube_upload_operations WHERE production_task_id IS NOT NULL
	), publication_evidence_publications AS (
		SELECT id FROM publication_records
		WHERE production_task_id IN (SELECT production_task_id FROM publication_evidence_tasks)
	)
`

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

	tag, err := s.db().Exec(ctx, publicationEvidenceCTE+`
		, publication_evidence_queues AS (
			SELECT q.id, q.parent_queue_item_id FROM channel_ops_queue_items q
			WHERE replace(q.payload_json->>'production_task_id', '-', '') IN (
				SELECT replace(production_task_id::text, '-', '') FROM publication_evidence_tasks
			) OR replace(q.payload_json->>'publication_id', '-', '') IN (
				SELECT replace(id::text, '-', '') FROM publication_evidence_publications
			) OR replace(q.payload_json->>'metric_schedule_id', '-', '') IN (
				SELECT replace(id::text, '-', '') FROM publication_metric_schedules
				WHERE publication_id IN (SELECT id FROM publication_evidence_publications)
			)
			UNION
			SELECT q.id, q.parent_queue_item_id FROM channel_ops_queue_items q
			JOIN publication_evidence_queues related
			  ON q.id = related.parent_queue_item_id OR q.parent_queue_item_id = related.id
		)
		DELETE FROM channel_ops_queue_items
		WHERE status IN ($1, $2, 'dead_letter')
		  AND updated_at < $3::timestamp
		  AND id NOT IN (SELECT id FROM publication_evidence_queues)
	`, QueueStatusSucceeded, QueueStatusDeadLettered, queueBefore)
	if err != nil {
		return result, err
	}
	result.QueueItemsDeleted = tag.RowsAffected()

	tag, err = s.db().Exec(ctx, `
		DELETE FROM agent_tick_audits
		WHERE started_at < $1::timestamptz
		  AND replay_status = 'legacy_unreplayable'
	`, auditBefore)
	if err != nil {
		return result, err
	}
	result.TickAuditsDeleted = tag.RowsAffected()

	tag, err = s.db().Exec(ctx, publicationEvidenceCTE+`
		DELETE FROM feedback_snapshots
		WHERE collected_at < $1::timestamptz
		  AND publication_id NOT IN (SELECT id FROM publication_evidence_publications)
	`, feedbackBefore)
	if err != nil {
		return result, err
	}
	result.FeedbackSnapshotsDeleted = tag.RowsAffected()
	return result, nil
}
