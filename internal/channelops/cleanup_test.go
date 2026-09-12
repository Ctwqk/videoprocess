package channelops

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type retentionDB struct {
	dbExecutor
	rows map[string]time.Time
}

func (d *retentionDB) Exec(_ context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	if !strings.Contains(q, "DELETE FROM agent_tick_audits") {
		return pgconn.NewCommandTag("DELETE 0"), nil
	}
	count := 0
	for status, at := range d.rows {
		if at.Before(args[0].(time.Time)) && (!strings.Contains(q, "replay_status = 'legacy_unreplayable'") || status == "legacy_unreplayable") {
			delete(d.rows, status)
			count++
		}
	}
	if count == 1 {
		return pgconn.NewCommandTag("DELETE 1"), nil
	}
	return pgconn.NewCommandTag("DELETE 3"), nil
}

func TestCleanupExpiredPreservesMixedSnapshotFacts(t *testing.T) {
	now := time.Unix(1800000000, 0)
	db := &retentionDB{rows: map[string]time.Time{"legacy_unreplayable": now.AddDate(0, 0, -100), "snapshot_complete": now.AddDate(0, 0, -100), "snapshot_pending": now.AddDate(0, 0, -100)}}
	result, err := (&Store{executionDB: db}).CleanupExpired(context.Background(), now, RetentionConfig{})
	if err != nil || result.TickAuditsDeleted != 1 || len(db.rows) != 2 {
		t.Fatalf("mixed retention: %+v, remaining %v, %v", result, db.rows, err)
	}
}

func TestCleanupExpiredRemovesExpiredRowsAndCascadesDecisionAudit(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := fixture.Store.Now().UTC()
	expiredAuditID := testUUID(t, "expired-audit")
	freshAuditID := testUUID(t, "fresh-audit")
	_, err := fixture.Store.Pool.Exec(ctx, `
		INSERT INTO agent_tick_audits (
			id, channel_profile_id, tick_id, started_at, finished_at, dry_run,
			ideas_discovered, candidates_scored, tasks_selected, tasks_rejected,
			guards_triggered_json, decision_summary_json, error_message
		)
		VALUES
			($1::uuid, $3::uuid, 'tick:expired', $5::timestamptz, $5::timestamptz, false, 1, 1, 0, 1, '[]'::json, '{}'::json, NULL),
			($2::uuid, $3::uuid, 'tick:fresh', $4::timestamptz, $4::timestamptz, false, 1, 1, 1, 0, '[]'::json, '{}'::json, NULL)
	`, expiredAuditID, freshAuditID, fixture.ChannelID, now.AddDate(0, 0, -5), now.AddDate(0, 0, -120))
	if err != nil {
		t.Fatalf("insert tick audits: %v", err)
	}
	_, err = fixture.Store.Pool.Exec(ctx, `
		INSERT INTO decision_audit_entries (
			tick_audit_id, channel_profile_id, candidate_id, candidate_source,
			score_json, guard_results_json, pds_decision_json, learning_context_json,
			selected, created_at
		)
		VALUES
			($1::uuid, $3::uuid, 'expired', 'manual_seed', '{}'::json, '[]'::json, '{}'::json, '{}'::json, false, $5::timestamptz),
			($2::uuid, $3::uuid, 'fresh', 'manual_seed', '{}'::json, '[]'::json, '{}'::json, '{}'::json, true, $4::timestamptz)
	`, expiredAuditID, freshAuditID, fixture.ChannelID, now.AddDate(0, 0, -5), now.AddDate(0, 0, -120))
	if err != nil {
		t.Fatalf("insert decision audit entries: %v", err)
	}
	_, err = fixture.Store.Enqueue(ctx, EnqueueOptions{
		Kind:             QueueSendAlert,
		IdempotencyKey:   "expired-alert",
		Payload:          map[string]any{"kind": "quota_low"},
		ChannelProfileID: &fixture.ChannelID,
	})
	if err != nil {
		t.Fatalf("enqueue expired queue item: %v", err)
	}
	_, err = fixture.Store.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = $1, updated_at = $2::timestamp
		WHERE idempotency_key = 'expired-alert'
	`, QueueStatusSucceeded, now.AddDate(0, 0, -40))
	if err != nil {
		t.Fatalf("age queue item: %v", err)
	}

	tx, err := fixture.Store.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	s := fixture.Store.withExecutionDB(tx, &fixture.ChannelID)
	channel, err := s.GetChannelProfile(ctx, fixture.ChannelID)
	if err != nil {
		t.Fatal(err)
	}
	protected := TickCandidate{CandidateID: "protected", Source: SourceManualSeed, Rejected: true}
	p, err := s.captureTickFacts(tickPreparation{Channel: channel, ChannelID: channel.ID, Bucket: "protected-old", Now: now.AddDate(0, 0, -120), Candidates: []TickCandidate{protected}})
	if err != nil {
		t.Fatal(err)
	}
	s.Now = func() time.Time { return p.Now }
	w, err := s.beginSnapshotAudit(ctx, p, TickResult{Rejected: []TickCandidate{protected}}, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.complete(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := fixture.Store.CleanupExpired(ctx, now, RetentionConfig{
		QueueDays:    30,
		AuditDays:    90,
		FeedbackDays: 365,
	})

	if err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}
	if result.QueueItemsDeleted != 1 || result.TickAuditsDeleted != 1 {
		t.Fatalf("cleanup result = %#v", result)
	}
	var expiredDecisionCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM decision_audit_entries
		WHERE candidate_id = 'expired'
	`).Scan(&expiredDecisionCount); err != nil {
		t.Fatalf("count expired decision audits: %v", err)
	}
	if expiredDecisionCount != 0 {
		t.Fatalf("expired decision audit count = %d, want 0", expiredDecisionCount)
	}
	var freshCount int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM decision_audit_entries
		WHERE candidate_id = 'fresh'
	`).Scan(&freshCount); err != nil {
		t.Fatalf("count fresh decision audits: %v", err)
	}
	if freshCount != 1 {
		t.Fatalf("fresh decision audit count = %d, want 1", freshCount)
	}
	assertPGSnapshotFacts(t, fixture.Store, fixture.ChannelID, 1, 0)
}
