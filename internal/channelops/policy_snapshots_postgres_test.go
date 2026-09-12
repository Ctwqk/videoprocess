package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// These are destructive disposable-fixture credentials, never runtime config.
// CI already explicitly owns this service. Local qualification additionally
// requires the exact database name and independently observed cluster system ID.
func snapshotDisposableTarget(raw, ciURL, actions, required, confirmation, systemID string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Hostname() != "127.0.0.1" || u.Port() == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("unconfirmed disposable fixture target")
	}
	name := strings.TrimPrefix(u.Path, "/")
	if actions == "true" && required == "1" && raw == ciURL && (raw == "postgresql://postgres:postgres@127.0.0.1:5432/postgres" || raw == "postgres://postgres:postgres@127.0.0.1:5432/postgres") {
		return name, nil
	}
	if !strings.HasPrefix(name, "vp_owned_inventory_test_") || name != confirmation || systemID == "" {
		return "", errors.New("disposable fixture name/system ID confirmation required")
	}
	for _, r := range systemID {
		if r < '0' || r > '9' {
			return "", errors.New("invalid system ID confirmation")
		}
	}
	return name, nil
}

func TestSnapshotDisposableFixtureRejectsAmbientTargets(t *testing.T) {
	local := "postgresql://fixture:fixture@127.0.0.1:55432/vp_owned_inventory_test_snapshots"
	ci := "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
	for _, tc := range []struct {
		raw, ci, actions, required, confirm, system string
		ok                                          bool
	}{
		{local, "", "", "", "vp_owned_inventory_test_snapshots", "123456", true},
		{ci, ci, "true", "1", "", "", true},
		{ci, ci, "", "1", "", "", false},
		{local, "", "", "", "", "123456", false},
		{local, "", "", "", "vp_owned_inventory_test_snapshots", "", false},
		{local + "?host=elsewhere", "", "", "", "vp_owned_inventory_test_snapshots", "123456", false},
		{"postgresql://127.0.0.1:5432/production", "", "", "", "production", "123456", false},
	} {
		_, err := snapshotDisposableTarget(tc.raw, tc.ci, tc.actions, tc.required, tc.confirm, tc.system)
		if (err == nil) != tc.ok {
			t.Fatalf("disposable target acceptance=%t, want %t", err == nil, tc.ok)
		}
	}
}

func lockSnapshotFixture(t *testing.T, store *Store, raw string) *pgx.Conn {
	t.Helper()
	name, err := snapshotDisposableTarget(raw, os.Getenv("CHANNEL_OPS_GO_POSTGRES_TEST_URL"), os.Getenv("GITHUB_ACTIONS"), os.Getenv("CHANNELOPS_REQUIRE_DATABASE"), os.Getenv("POLICY_SNAPSHOTS_DISPOSABLE_CONFIRM"), os.Getenv("POLICY_SNAPSHOTS_DISPOSABLE_SYSTEM_ID"))
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A separate session lets runner tests close the Store pool normally while
	// keeping disposable cleanup ownership until the fixture itself closes.
	conn, err := pgx.Connect(ctx, raw)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	var current, revision string
	if err := conn.QueryRow(ctx, `SELECT current_database(),version_num FROM public.alembic_version`).Scan(&current, &revision); err != nil || current != name || revision != "044_policy_decision_snapshots" {
		_ = conn.Close(ctx)
		store.Close()
		t.Fatal("fixture database identity or revision044 mismatch")
	}
	if name != "postgres" {
		var system string
		if err := conn.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&system); err != nil || system != os.Getenv("POLICY_SNAPSHOTS_DISPOSABLE_SYSTEM_ID") {
			_ = conn.Close(ctx)
			store.Close()
			t.Fatal("fixture cluster system ID mismatch")
		}
	}
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(774403030044)`).Scan(&locked); err != nil || !locked {
		_ = conn.Close(ctx)
		store.Close()
		t.Fatal("disposable fixture already in use")
	}
	return conn
}

func TestSnapshotFixtureCleanupAfterStoreClose(t *testing.T) {
	if testing.Short() {
		t.Skip("parent-owned disposable PostgreSQL qualification")
	}
	f := NewChannelOpsFixture(t)
	defer f.Close(context.Background())
	f.InsertChannelWithLaneAccountSeed(context.Background())
	done := make(chan struct{})
	go func() { f.Store.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("fixture lock blocked Store pool close")
	}
}

func TestPolicySnapshotsPostgresAtomicMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("parent-owned disposable PostgreSQL qualification")
	}
	for _, name := range []string{"success", "dry-run", "rejected", "empty", "policy-conflict", "duplicate", "missing-link", "extra-decision", "late-failure", "legacy-conflict", "exact-replay", "changed-replay"} {
		t.Run(name, func(t *testing.T) {
			f := NewChannelOpsFixture(t)
			ctx := context.Background()
			defer f.Close(ctx)
			f.InsertChannelWithLaneAccountSeed(ctx)
			at := f.Store.Now().Add(789 * time.Nanosecond)
			f.Store.Now = func() time.Time { return at }
			if name == "dry-run" {
				f.SetDryRun(ctx, true)
			}
			if name == "rejected" {
				f.SetAccountEnabled(ctx, false)
			}
			if name == "empty" {
				if _, err := f.Store.Pool.Exec(ctx, `UPDATE manual_seeds SET status='exhausted' WHERE channel_profile_id=$1::uuid`, f.ChannelID); err != nil {
					t.Fatal(err)
				}
				if _, err := f.Store.Pool.Exec(ctx, `UPDATE topic_lanes SET enabled=false WHERE channel_profile_id=$1::uuid`, f.ChannelID); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := f.Store.Pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			s := f.Store.withExecutionDB(tx, &f.ChannelID)
			p, err := s.prepareTick(ctx, f.ChannelID, "snapshot-matrix", agentTickOptions{})
			if err != nil {
				t.Fatal(err)
			}
			candidates := append([]TickCandidate(nil), p.Candidates...)
			accepted, rejected := acceptedRejected(candidates)
			result := TickResult{DryRun: p.Channel.DryRun, Accepted: accepted, Rejected: rejected}
			summary := map[string]any{"test": "snapshot-matrix"}
			if name == "legacy-conflict" {
				if _, err := s.insertTickAudit(ctx, tx, f.ChannelID, p.Bucket, result, summary); err != nil {
					t.Fatal(err)
				}
			}
			if name == "policy-conflict" {
				var bad PolicyVersion
				if err := cloneSnapshotJSON(p.Policy, &bad); err != nil {
					t.Fatal(err)
				}
				bad.FormulaJSON["selection"] = "conflicting stored content"
				raw, err := json.Marshal(struct {
					PolicyVersion
					ID        string    `json:"id"`
					CreatedAt time.Time `json:"created_at"`
				}{bad, testUUID(t, "conflicting-policy"), p.Now})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(ctx, `INSERT INTO decision_policy_versions SELECT p.* FROM json_populate_record(NULL::decision_policy_versions,$1::json) p`, raw); err != nil {
					t.Fatal(err)
				}
			}
			if name == "duplicate" {
				result.Accepted = append(result.Accepted, result.Accepted[0])
			}
			w, writeErr := s.beginSnapshotAudit(ctx, p, result, summary)
			if writeErr == nil {
				var status string
				if err := tx.QueryRow(ctx, `SELECT replay_status FROM agent_tick_audits WHERE id=$1::uuid`, w.tickID).Scan(&status); err != nil || status != "snapshot_pending" {
					t.Fatalf("premature completion: %s %v", status, err)
				}
				if name == "missing-link" {
					delete(w.snapshotIDs, candidates[0].CandidateID)
				}
				if name == "extra-decision" {
					_, writeErr = s.insertDecisionAuditEntries(ctx, tx, w.tickID, f.ChannelID, result)
				}
				for _, candidate := range accepted {
					if result.DryRun {
						break
					}
					id, err := s.insertProductionTask(ctx, tx, p.Channel, candidate, p.Now)
					if err != nil {
						t.Fatal(err)
					}
					if err := s.attachDecisionAuditTask(ctx, tx, w.decisionIDs[candidate.CandidateID], id); err != nil {
						t.Fatal(err)
					}
					if _, err := s.enqueue(ctx, tx, EnqueueOptions{Kind: QueuePlanTask, IdempotencyKey: "plan_task:" + id, Payload: map[string]any{"production_task_id": id, "channel_id": f.ChannelID}, ChannelProfileID: &f.ChannelID}); err != nil {
						t.Fatal(err)
					}
				}
				if writeErr == nil {
					writeErr = w.complete(ctx, tx)
				}
				if name == "late-failure" && writeErr == nil {
					writeErr = errors.New("failure after sealing before commit")
				}
			}
			failure := name == "policy-conflict" || name == "duplicate" || name == "missing-link" || name == "extra-decision" || name == "late-failure" || name == "legacy-conflict"
			if failure {
				if writeErr == nil {
					t.Fatal("invalid snapshot transaction succeeded")
				}
				if err := tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
				for _, table := range []string{"decision_policy_versions", "candidate_feature_snapshots", "decision_audit_entries", "agent_tick_audits", "production_tasks", "channel_ops_queue_items"} {
					var count int
					if err := f.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM `+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil || count != 0 {
						t.Fatalf("partial %s=%d: %v", table, count, err)
					}
				}
				return
			}
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			assertPGSnapshotFacts(t, f.Store, f.ChannelID, len(candidates), result.TasksToCreate())
			if name == "exact-replay" || name == "changed-replay" {
				replayTx, err := f.Store.Pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer replayTx.Rollback(ctx)
				if name == "changed-replay" {
					result.Accepted[0].PDSDecisionJSON = map[string]any{"verdict": "changed"}
				}
				replay, err := f.Store.withExecutionDB(replayTx, &f.ChannelID).beginSnapshotAudit(ctx, p, result, summary)
				if name == "changed-replay" {
					if err == nil {
						t.Fatal("changed replay accepted")
					}
					return
				}
				if err != nil || !replay.replay {
					t.Fatalf("exact replay: %+v %v", replay, err)
				}
				if err := replayTx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				assertPGSnapshotFacts(t, f.Store, f.ChannelID, len(candidates), result.TasksToCreate())
			}
			for _, q := range []string{`UPDATE agent_tick_audits SET dry_run=NOT dry_run WHERE channel_profile_id=$1::uuid`, `DELETE FROM agent_tick_audits WHERE channel_profile_id=$1::uuid`, `UPDATE decision_audit_entries SET created_task_id=NULL WHERE channel_profile_id=$1::uuid`, `DELETE FROM candidate_feature_snapshots WHERE tick_audit_id IN (SELECT id FROM agent_tick_audits WHERE channel_profile_id=$1::uuid)`, `UPDATE decision_policy_versions SET status='retired' WHERE portfolio_config_json->>'channel_profile_id'=$1`} {
				if len(candidates) == 0 && (strings.Contains(q, "decision_audit_entries") || strings.Contains(q, "candidate_feature_snapshots")) {
					continue
				}
				if _, err := f.Store.Pool.Exec(ctx, q, f.ChannelID); err == nil {
					t.Fatal("immutable fact rewrite accepted")
				}
			}
		})
	}
}

func assertPGSnapshotFacts(t *testing.T, s *Store, channelID string, wantCandidates, wantTasks int) {
	t.Helper()
	var complete, candidates, links, tasks int
	err := s.Pool.QueryRow(context.Background(), `
  SELECT (SELECT count(*) FROM agent_tick_audits WHERE channel_profile_id=$1::uuid AND replay_status='snapshot_complete'),
   (SELECT count(*) FROM candidate_feature_snapshots f JOIN agent_tick_audits a ON a.id=f.tick_audit_id WHERE a.channel_profile_id=$1::uuid),
   (SELECT count(*) FROM decision_audit_entries d JOIN candidate_feature_snapshots f ON f.id=d.feature_snapshot_id
    JOIN agent_tick_audits a ON a.id=d.tick_audit_id WHERE a.channel_profile_id=$1::uuid
    AND d.candidate_id=f.candidate_id AND f.tick_audit_id=a.id AND d.policy_version_id=a.policy_version_id AND f.policy_version_id=a.policy_version_id
    AND d.candidate_set_hash=a.candidate_set_hash AND f.candidate_set_hash=a.candidate_set_hash AND f.feature_as_of=a.feature_as_of
    AND d.baseline_score IS NULL AND d.final_score IS NULL AND d.rank IS NULL
    AND f.normalized_features_json IS NULL AND f.cadence_snapshot_json IS NULL),
   (SELECT count(*) FROM production_tasks WHERE channel_profile_id=$1::uuid)`, channelID).Scan(&complete, &candidates, &links, &tasks)
	if err != nil || complete != 1 || candidates != wantCandidates || links != wantCandidates || tasks != wantTasks {
		t.Fatalf("snapshot facts: complete=%d candidates=%d links=%d tasks=%d err=%v", complete, candidates, links, tasks, err)
	}
}
