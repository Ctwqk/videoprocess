package channelops

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TEMP tables exercise real PostgreSQL JSON/UUID/recursive/delete behavior while
// leaving even the confirmed disposable database's permanent tables untouched.
func retentionPostgres(t *testing.T) (*Store, pgx.Tx) {
	t.Helper()
	raw := os.Getenv("RETENTION_DISPOSABLE_POSTGRES_URL")
	name := os.Getenv("RETENTION_DISPOSABLE_POSTGRES_CONFIRM")
	system := os.Getenv("RETENTION_DISPOSABLE_POSTGRES_SYSTEM_ID")
	if raw == "" {
		raw = os.Getenv("DATABASE_URL")
		if raw == "" {
			t.Skip("explicit disposable PostgreSQL required")
		}
		var err error
		name, err = snapshotDisposableTarget(raw, os.Getenv("CHANNEL_OPS_GO_POSTGRES_TEST_URL"), os.Getenv("GITHUB_ACTIONS"), os.Getenv("CHANNELOPS_REQUIRE_DATABASE"), os.Getenv("POLICY_SNAPSHOTS_DISPOSABLE_CONFIRM"), os.Getenv("POLICY_SNAPSHOTS_DISPOSABLE_SYSTEM_ID"))
		if err != nil {
			t.Fatal(err)
		}
		system = os.Getenv("POLICY_SNAPSHOTS_DISPOSABLE_SYSTEM_ID")
	} else {
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "postgresql" || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.Port() == "5432" || u.RawQuery != "" || u.Fragment != "" || strings.TrimPrefix(u.Path, "/") != name || !strings.HasPrefix(name, "vp_retention_test_") || system == "" {
			t.Fatal("explicit disposable retention database/cluster confirmation required")
		}
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, raw)
	if err != nil {
		t.Fatal("disposable retention database unavailable")
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	var actual, version, cluster string
	if err := conn.QueryRow(ctx, `SELECT current_database(), current_setting('server_version_num'), system_identifier::text FROM pg_control_system()`).Scan(&actual, &version, &cluster); err != nil || actual != name || !strings.HasPrefix(version, "16") || (system != "" && cluster != system) {
		t.Fatal("disposable retention database identity mismatch")
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(ctx) })
	_, err = tx.Exec(ctx, `
		SET LOCAL statement_timeout = '5s';
		CREATE TEMP TABLE publication_records (id uuid PRIMARY KEY, production_task_id uuid, platform text);
		CREATE TEMP TABLE youtube_upload_operations (production_task_id uuid, status text);
		CREATE TEMP TABLE publication_metric_schedules (id uuid PRIMARY KEY, publication_id uuid);
		CREATE TEMP TABLE channel_ops_queue_items (id uuid PRIMARY KEY, parent_queue_item_id uuid, payload_json json, status text, updated_at timestamp);
		CREATE TEMP TABLE feedback_snapshots (id uuid PRIMARY KEY, publication_id uuid, collected_at timestamptz);
		CREATE TEMP TABLE agent_tick_audits (id text, started_at timestamptz, replay_status text);
	`)
	if err != nil {
		t.Fatal(err)
	}
	return &Store{executionDB: tx}, tx
}

func TestCleanupPreservesPublicationEvidencePostgres(t *testing.T) {
	for _, anchor := range []string{"youtube", "YouTube", "reserved", "submitted", "succeeded", "failed", "unknown", "upload_only"} {
		t.Run(anchor, func(t *testing.T) {
			s, tx := retentionPostgres(t)
			ctx := context.Background()
			now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
			old := now.AddDate(0, 0, -400)
			task, pub, metric, otherPub := testUUID(t, "task"), testUUID(t, "pub"), testUUID(t, "metric"), testUUID(t, "other-pub")
			platform := anchor
			if anchor != "youtube" && anchor != "YouTube" {
				platform = "other"
				if _, err := tx.Exec(ctx, `INSERT INTO youtube_upload_operations VALUES($1::uuid,$2)`, task, anchor); err != nil {
					t.Fatal(err)
				}
			}
			if anchor != "upload_only" {
				if _, err := tx.Exec(ctx, `INSERT INTO publication_records VALUES($1::uuid,$2::uuid,$3);`, pub, task, platform); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(ctx, `INSERT INTO publication_metric_schedules VALUES($1::uuid,$2::uuid)`, metric, pub); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO publication_records VALUES($1::uuid,$2::uuid,'vimeo')`, otherPub, testUUID(t, "other-task")); err != nil {
				t.Fatal(err)
			}
			ids := map[string]string{}
			for _, key := range []string{"ancestor", "publish", "automatic", "manual", "reconcile", "metric", "retry", "cycle", "sibling", "unrelated", "other-pub", "fresh", "running"} {
				ids[key] = testUUID(t, key+"-queue")
			}
			rows := []struct{ key, parent, payload, status string }{
				{"ancestor", "", `{}`, "succeeded"},
				{"publish", "ancestor", `{"production_task_id":"` + task + `"}`, "succeeded"},
				{"automatic", "publish", `{"publication_id":"` + pub + `"}`, "cancelled"},
				{"manual", "cycle", `{"publication_id":"` + pub + `"}`, "succeeded"},
				{"reconcile", "manual", `{}`, "succeeded"},
				{"metric", "manual", `{"metric_schedule_id":"` + metric + `"}`, "succeeded"},
				{"retry", "metric", `{"publication_id":"drifted"}`, "dead_lettered"},
				{"cycle", "retry", `[]`, "succeeded"},
				{"sibling", "ancestor", `null`, "dead_letter"},
				{"unrelated", "", `{"production_task_id":"not-a-uuid"}`, "dead_lettered"},
				{"other-pub", "", `{"publication_id":"` + otherPub + `"}`, "succeeded"},
				{"fresh", "", `{}`, "succeeded"},
				{"running", "", `{}`, "running"},
			}
			if anchor == "upload_only" {
				// The task seed alone must preserve even payload-free descendants.
				rows[3].parent = "publish"
			}
			for _, row := range rows {
				var parent any
				if row.parent != "" {
					parent = ids[row.parent]
				}
				at := old
				if row.key == "fresh" {
					at = now
				}
				if _, err := tx.Exec(ctx, `INSERT INTO channel_ops_queue_items VALUES($1::uuid,$2::uuid,$3::json,$4,$5)`, ids[row.key], parent, row.payload, row.status, at); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO feedback_snapshots VALUES($1::uuid,$2::uuid,$5),($3::uuid,$4::uuid,$5)`, testUUID(t, "feedback"), pub, testUUID(t, "other-feedback"), otherPub, old); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO agent_tick_audits VALUES('legacy',$1,'legacy_unreplayable'),('complete',$1,'snapshot_complete'),('pending',$1,'snapshot_pending')`, old); err != nil {
				t.Fatal(err)
			}
			result, err := s.CleanupExpired(ctx, now, RetentionConfig{})
			if err != nil {
				t.Fatal(err)
			}
			wantFeedback := int64(1)
			if anchor == "upload_only" {
				wantFeedback = 2 // No publication exists for either feedback row.
			}
			if result.QueueItemsDeleted != 2 || result.FeedbackSnapshotsDeleted != wantFeedback || result.TickAuditsDeleted != 1 {
				t.Fatalf("cleanup lost evidence or changed unrelated retention: %+v", result)
			}
			var count int
			if err := tx.QueryRow(ctx, `SELECT count(*) FROM channel_ops_queue_items`).Scan(&count); err != nil || count != 11 {
				t.Fatalf("remaining evidence/fresh/running rows=%d, want11: %v", count, err)
			}
		})
	}
}
