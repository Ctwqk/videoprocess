package channelops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// This fixture constructs synthetic terminal history through normal constraints,
// following the already-qualified A2 seed order. It never upgrades a real draft,
// changes a trigger/grant, or treats a fixture certificate as operator approval.
func ownedB2PGSeedRetirement(t *testing.T, store *Store, now time.Time) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	existing, err := loadOwnedHistorySnapshot(ctx, store.Pool, "UCaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal("fixture initial history read failed")
	}
	for _, inv := range historyRows(existing.rows()["owned_seed_inventories"]) {
		legacy := ownedMap(ownedMap(inv["manifest_json"])["legacy_history"])
		if legacy["retired_unassigned_preupload"] != nil {
			return historyTestCopy(t, legacy).(map[string]any)
		}
	}
	rows, cert := ownedB2PGSeedDocument(t, now)
	job := historyObject(rows["jobs"].([]any)[0])
	tx, err := store.Pool.Begin(ctx)
	if err != nil {
		t.Fatal("fixture graph transaction unavailable")
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, `INSERT INTO pipelines(id,name,definition) VALUES($1::uuid,'B2 synthetic terminal graph',$2::json)`, job["pipeline_id"], historyJSON(t, job["pipeline_snapshot"])); err != nil {
		t.Fatal("fixture pipeline insert failed")
	}
	for _, table := range strings.Fields("channel_profiles publishing_accounts assets manual_seeds jobs worker_admission_grants worker_registrations node_executions artifacts production_tasks youtube_upload_operations worker_task_delivery_attestations worker_event_emissions registered_worker_event_receipts registered_worker_event_deliveries worker_task_dispatches") {
		for _, row := range historyRows(rows[table]) {
			value := historyTestCopy(t, row).(map[string]any)
			digest := sha256.Sum256([]byte(historyString(row["id"])))
			if table == "worker_admission_grants" {
				value["token_sha256"] = hex.EncodeToString(digest[:])
			}
			if table == "worker_registrations" {
				value["lease_secret_sha256"] = hex.EncodeToString(digest[:])
			}
			if _, err := tx.Exec(ctx, "INSERT INTO public."+table+" SELECT * FROM json_populate_record(NULL::public."+table+",$1::json)", historyJSON(t, value)); err != nil {
				t.Fatalf("fixture native graph insert failed: %s (%T)", table, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal("fixture native graph commit failed")
	}
	snapshot, err := loadOwnedHistorySnapshot(ctx, store.Pool, "UCaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatal("fixture graph reread failed")
	}
	return ownedB2PGSealSnapshot(t, cert, snapshot)
}

func ownedB2PGSealSnapshot(t *testing.T, cert map[string]any, snapshot ownedHistorySnapshot) map[string]any {
	t.Helper()
	rows := snapshot.rows()
	retained := historyObject(cert["retained_facts"])
	for label, table := range map[string]string{"operation": "youtube_upload_operations", "task": "production_tasks", "job": "jobs", "upload_node": "node_executions", "account": "publishing_accounts", "channel": "channel_profiles", "manual_seed": "manual_seeds"} {
		id := historyObject(retained[label])["id"]
		retained[label] = historyOne(historySelect(rows[table], func(r map[string]any) bool { return r["id"] == id }), "fixture_retained_missing")
	}
	for _, value := range historyArray(retained["source_assets"]) {
		source := historyObject(value)
		id := historyObject(source["asset"])["id"]
		source["asset"] = historyOne(historySelect(rows["assets"], func(r map[string]any) bool { return r["id"] == id }), "fixture_source_missing")
	}
	cert["terminal_graph"] = historyTerminalGraph(rows, cert)
	cert["terminal_graph_sha256"] = historyTestHash(t, cert["terminal_graph"])
	cert["transition_sha256"] = historyTestHash(t, historyObject(retained["task"])["transition_history_json"])
	cert["observed_at"] = historyISO(snapshot.observedAt)
	return map[string]any{"version": 1, "bindings": []any{}, "retired_unassigned_preupload": cert}
}

func ownedB2PGSeedDocument(t *testing.T, now time.Time) (map[string]any, map[string]any) {
	t.Helper()
	f := historyGolden(t, "retired_unassigned")
	cert := historyTestCopy(t, historyTestCertificate(f)).(map[string]any)
	rows := historyTestRows(f)
	rows["owned_seed_inventories"] = []any{}
	delta := now.Sub(historyAt(t, f["now"]))
	var shift func(any) any
	shift = func(v any) any {
		switch v := v.(type) {
		case map[string]any:
			for key, value := range v {
				v[key] = shift(value)
			}
			return v
		case []any:
			for i, value := range v {
				v[i] = shift(value)
			}
			return v
		case string:
			if at, err := time.Parse(time.RFC3339Nano, v); err == nil {
				return ownedISO(at.Add(delta))
			}
		}
		return v
	}
	shift(rows)
	for _, table := range []string{"worker_admission_grants", "worker_registrations"} {
		for _, row := range historyRows(rows[table]) {
			row["image_identity"], row["revoked_at"], row["revoke_reason"] = "vp-python-worker:deploy-aaaaaaaaaaaa", ownedISO(now.Add(-72*time.Hour)), "fixture-retired"
			if table == "worker_admission_grants" {
				row["issued_by"] = "fixture-operator"
			}
		}
	}
	job := historyObject(rows["jobs"].([]any)[0])
	job["orchestrator_owner"] = "python"
	historyObject(rows["publishing_accounts"].([]any)[0])["paused_until"] = ownedISO(now.Add(time.Hour))
	for _, row := range historyRows(rows["artifacts"]) {
		row["kind"] = "INTERMEDIATE"
	}
	for _, emission := range historyRows(rows["worker_event_emissions"]) {
		emission["payload_sha256"] = historyTestHash(t, emission["payload_json"])
		for _, receipt := range historyRows(rows["registered_worker_event_receipts"]) {
			if receipt["source_task_attestation_id"] != emission["source_task_attestation_id"] {
				continue
			}
			receipt["payload_sha256"] = emission["payload_sha256"]
			for _, delivery := range historyRows(rows["registered_worker_event_deliveries"]) {
				if delivery["receipt_id"] == receipt["id"] {
					delivery["payload_sha256"] = emission["payload_sha256"]
				}
			}
		}
	}
	return rows, cert
}

func ownedB2PGHandler(t *testing.T, f *ownedPGFixture, drift *atomic.Bool, reads *atomic.Int32, pds PDSDecider) HandlerService {
	t.Helper()
	return HandlerService{Store: f.store, PDS: pds, Config: Config{OwnedHistoryRedisURL: "redis://fixture-history-reader:fixture-secret@127.0.0.1:55464/15"}, ownedHistoryRedisFactory: func(*redis.Options) ownedHistoryRedisClient {
		reads.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		tx, err := f.store.Pool.Begin(ctx)
		if err != nil {
			t.Fatal("fixture external boundary transaction failed")
		}
		defer tx.Rollback(context.Background())
		if _, err := tx.Exec(ctx, `SELECT id FROM channel_profiles WHERE id=$1::uuid FOR UPDATE NOWAIT`, f.channel.ID); err != nil {
			t.Fatal("channel lock leaked into Redis observation")
		}
		if _, err := tx.Exec(ctx, `SELECT service_name FROM runtime_schedules WHERE service_name='videoprocess' FOR UPDATE NOWAIT`); err != nil {
			t.Fatal("schedule lock leaked into Redis observation")
		}
		client := ownedHistoryRedisFixture(t, historyGolden(t, "retired_unassigned"))
		if drift.Load() {
			for key := range client.markers {
				client.markers[key] = "9999-0"
			}
		}
		return client
	}}
}

func ownedB2PGClaimTick(t *testing.T, f *ownedPGFixture, ctx context.Context, bucket string) QueueItemRow {
	t.Helper()
	if _, err := f.store.Enqueue(ctx, EnqueueOptions{Kind: QueueAgentTick, IdempotencyKey: "b2:" + f.channel.ID + ":" + bucket, Payload: map[string]any{"channel_id": f.channel.ID, "bucket": bucket}, ChannelProfileID: &f.channel.ID}); err != nil {
		t.Fatal("fixture tick enqueue failed")
	}
	item, err := f.store.ClaimNextForKinds(ctx, handlerWorkerID(f.lease.Authority()), []string{QueueAgentTick})
	if err != nil || item == nil || item.ChannelProfileID == nil || *item.ChannelProfileID != f.channel.ID {
		t.Fatal("fixture exact tick claim failed")
	}
	return *item
}

func TestOwnedB2PGQueuedRetirementProofAndAuthorityRecheck(t *testing.T) {
	for _, mode := range []string{"proof_drift", "deny", "error", "queue_loss", "leader_loss", "commit_replay"} {
		t.Run(mode, func(t *testing.T) {
			f := newOwnedPGFixtureWithHistory(t, func(s *Store, at time.Time) map[string]any { return ownedB2PGSeedRetirement(t, s, at) })
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			item := ownedB2PGClaimTick(t, f, ctx, mode)
			var drift atomic.Bool
			var reads, calls atomic.Int32
			pds := ownedTestPDS(func(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
				calls.Add(1)
				switch mode {
				case "proof_drift":
					drift.Store(true)
				case "queue_loss":
					if _, err := f.store.Pool.Exec(ctx, `UPDATE channel_ops_queue_items SET locked_by='replacement' WHERE id=$1::uuid`, item.ID); err != nil {
						return PDSDecision{}, err
					}
				case "leader_loss":
					if err := ownedReleaseLeaderAtDBTime(ctx, f.store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`), f.lease.Release); err != nil {
						return PDSDecision{}, err
					}
				case "deny":
					return PDSDecision{Verdict: "block"}, nil
				case "error":
					return PDSDecision{}, errors.New("synthetic policy failure")
				}
				return ownedProducerRealDecision(), nil
			})
			h := ownedB2PGHandler(t, f, &drift, &reads, pds)
			err := h.HandleAgentTick(ctx, item)
			if calls.Load() != 1 || reads.Load() < 2 {
				t.Fatal("queued v2 path did not perform prepare/revalidate/PDS", calls.Load(), reads.Load())
			}
			switch mode {
			case "queue_loss":
				if !errors.Is(err, ErrQueueLeaseLost) {
					t.Fatalf("queue fence lost: %T", err)
				}
			case "leader_loss":
				if !errors.Is(err, ErrLeaderAuthorityLost) && !errors.Is(err, ErrLeaderAuthorityUnavailable) {
					t.Fatalf("leader fence lost: %T", err)
				}
			default:
				if err != nil {
					t.Fatalf("queued finalization failed: %T", err)
				}
			}
			if mode == "commit_replay" {
				f.assertCounts(t, 1)
				if err := h.HandleAgentTick(ctx, item); err != nil {
					t.Fatalf("committed-result-loss replay failed: %T", err)
				}
				f.assertCounts(t, 1)
				if calls.Load() != 1 {
					t.Fatal("replay selected another item")
				}
			} else {
				f.assertCounts(t, 0)
			}
			if mode == "proof_drift" || mode == "deny" || mode == "error" {
				var state, reason string
				if err := f.store.Pool.QueryRow(ctx, `SELECT state,hold_reason FROM owned_seed_inventories WHERE id=$1::uuid`, *f.channel.OwnedSeedInventoryID).Scan(&state, &reason); err != nil || state != "held" {
					t.Fatal("failed PDS/proof did not leave durable hold")
				}
				if mode == "proof_drift" && reason != "owned_history_retired_redis_changed" {
					t.Fatal("fresh proof was not checked", reason)
				}
				_ = h.HandleAgentTick(ctx, item)
				if calls.Load() != 1 {
					t.Fatal("hold chose replacement")
				}
			}
		})
	}
}

// Keep the fixed native seed schema visible to an offline shape test.
func TestOwnedB2PGSeedColumnShape(t *testing.T) {
	f := historyGolden(t, "retired_unassigned")
	for _, table := range strings.Fields(historyTerminalTables) {
		columns := historyCompleteColumns[table][0]
		if columns == "" {
			continue
		}
		for _, row := range historyRows(historyTestRows(f)[table]) {
			keys := make([]string, 0, len(row))
			for key := range row {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			if len(keys) != len(strings.Fields(columns)) {
				t.Fatal("native seed columns differ", table)
			}
			for _, key := range strings.Fields(columns) {
				if _, ok := row[key]; !ok {
					t.Fatal("native seed missing", table, key)
				}
			}
		}
	}
}

func TestOwnedB2PGSeedFullAssessmentOffline(t *testing.T) {
	channel, data, now, rows, _ := ownedB2Fixture(t, "")
	seed, cert := ownedB2PGSeedDocument(t, now)
	for table, values := range seed {
		rows[table] = append(rows[table].([]any), values.([]any)...)
	}
	snapshot, err := newOwnedHistorySnapshot(historyJSON(t, rows), ownedString(data.Inventory["platform_channel_id"]), now, []byte("[]"))
	if err != nil {
		t.Fatal(err)
	}
	legacy := ownedB2PGSealSnapshot(t, cert, snapshot)
	manifest := ownedMap(data.Inventory["manifest_json"])
	manifest["legacy_history"] = legacy
	data.Inventory["manifest_sha256"] = historyTestHash(t, manifest)
	snapshot, err = newOwnedHistorySnapshot(historyJSON(t, rows), ownedString(data.Inventory["platform_channel_id"]), now, []byte("[]"))
	if err != nil {
		t.Fatal(err)
	}
	request, err := ownedHistoryRedisRequest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := observeOwnedHistoryRedis(context.Background(), request, "redis://fixture-history-reader:fixture-secret@127.0.0.1:55464/15", func(*redis.Options) ownedHistoryRedisClient {
		return ownedHistoryRedisFixture(t, historyGolden(t, "retired_unassigned"))
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = ownedHistoryWithObservations(snapshot, evidence)
	if err != nil {
		t.Fatal(err)
	}
	data.History = &snapshot
	state := assessOwnedInventory(channel, data, now)
	if state.Candidate == nil || state.HoldReason != "" || state.SkipReason != "" {
		t.Fatalf("schema-shaped native terminal fixture does not qualify offline: %+v", state)
	}
}

func TestOwnedB2PGNewBlankOperationDuringQueuedPDS(t *testing.T) {
	f := newOwnedPGFixtureWithHistory(t, func(s *Store, at time.Time) map[string]any { return ownedB2PGSeedRetirement(t, s, at) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	item := ownedB2PGClaimTick(t, f, ctx, "blank-operation")
	job, node, artifact, operation := ownedNewUUID(t), ownedNewUUID(t), ownedNewUUID(t), ownedNewUUID(t)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := f.store.Pool.Exec(ctx, `DELETE FROM youtube_upload_operations WHERE id=$1::uuid`, operation); err != nil {
			t.Error("scratch orphan cleanup failed")
		}
		if _, err := f.store.Pool.Exec(ctx, `DELETE FROM jobs WHERE id=$1::uuid`, job); err != nil {
			t.Error("scratch orphan job cleanup failed")
		}
	})
	var drift atomic.Bool
	var reads, calls atomic.Int32
	h := ownedB2PGHandler(t, f, &drift, &reads, ownedTestPDS(func(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
		calls.Add(1)
		tx, err := f.store.Pool.Begin(ctx)
		if err != nil {
			return PDSDecision{}, err
		}
		defer tx.Rollback(context.Background())
		for _, step := range []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO jobs(id,pipeline_id,pipeline_snapshot,status,submitted_by,retry_count,error_message,orchestrator_owner)
			 SELECT $1::uuid,pipeline_id,pipeline_snapshot,'CANCELLED','synthetic',0,'synthetic orphan','python' FROM jobs WHERE id=$2::uuid`, []any{job, historyRetiredTuple[2]}},
			{`INSERT INTO node_executions(id,job_id,node_id,node_type,node_label,node_config,status,progress,input_artifact_ids,retry_count) VALUES($1::uuid,$2::uuid,'upload','youtube_upload','','{}','CANCELLED',0,'{}'::uuid[],0)`, []any{node, job}},
			{`INSERT INTO artifacts(id,job_id,node_execution_id,kind,filename,mime_type,file_size,storage_backend,storage_path,media_info) VALUES($1::uuid,$2::uuid,$3::uuid,'INTERMEDIATE','synthetic.mp4','video/mp4',100,'local','artifacts/synthetic.mp4','{}')`, []any{artifact, job, node}},
			{`INSERT INTO youtube_upload_operations(id,job_id,node_execution_id,input_artifact_id,content_sha256,title,privacy,status,receipt_json,created_at,updated_at) VALUES($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5,'synthetic','unlisted','reserved','{}',now(),now())`, []any{operation, job, node, artifact, strings.Repeat("f", 64)}},
		} {
			if _, err := tx.Exec(ctx, step.sql, step.args...); err != nil {
				return PDSDecision{}, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return PDSDecision{}, err
		}
		return ownedProducerRealDecision(), nil
	}))
	if err := h.HandleAgentTick(ctx, item); err != nil {
		t.Fatalf("blank-operation finalizer failed: %T", err)
	}
	f.assertCounts(t, 0)
	var reason string
	if err := f.store.Pool.QueryRow(ctx, `SELECT hold_reason FROM owned_seed_inventories WHERE id=$1::uuid`, *f.channel.OwnedSeedInventoryID).Scan(&reason); err != nil || reason != "owned_history_orphan" || calls.Load() != 1 {
		t.Fatal("new orphan was hidden by account joins", reason, calls.Load())
	}
}

type ownedB2RollbackTx struct {
	pgx.Tx
	stop    error
	blocked *atomic.Int32
}

func (p ownedB2RollbackTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	// enqueue writes INSERT ... RETURNING through QueryRow, not Exec.
	if strings.Contains(sql, "INSERT INTO channel_ops_queue_items") && len(args) > 0 && args[0] == QueuePlanTask {
		p.blocked.Add(1)
		return ownedB2FenceRow(func(...any) error { return p.stop })
	}
	return p.Tx.QueryRow(ctx, sql, args...)
}

func TestOwnedB2RollbackInjectionUsesActualPlanEnqueue(t *testing.T) {
	for _, kind := range []string{QueuePlanTask, QueueObserveJob} {
		t.Run(kind, func(t *testing.T) {
			stop := errors.New("synthetic plan queue write failure")
			delegated := errors.New("underlying transaction reached")
			probe := &ownedB2FenceProbe{stop: delegated}
			var blocked atomic.Int32
			tx := ownedB2RollbackTx{Tx: probe, stop: stop, blocked: &blocked}
			s := &Store{Now: func() time.Time { return time.Unix(1, 0).UTC() }}
			_, err := s.enqueue(context.Background(), tx, EnqueueOptions{
				Kind: kind, IdempotencyKey: "offline:" + kind,
			})
			if kind == QueuePlanTask {
				if !errors.Is(err, stop) || len(probe.queries) != 0 || blocked.Load() != 1 {
					t.Fatalf("actual plan enqueue bypassed rollback injection: %v", err)
				}
			} else if !errors.Is(err, delegated) || len(probe.queries) != 1 || blocked.Load() != 0 {
				t.Fatal("rollback injection intercepted an unrelated queue write")
			}
			err = tx.QueryRow(context.Background(), "SELECT clock_timestamp()").Scan()
			if !errors.Is(err, delegated) {
				t.Fatal("rollback injection intercepted proof reads")
			}
		})
	}
}

func TestOwnedB2PGQueuedAtomicRollbackBeforePlanQueue(t *testing.T) {
	f := newOwnedPGFixtureWithHistory(t, func(s *Store, at time.Time) map[string]any { return ownedB2PGSeedRetirement(t, s, at) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	item := ownedB2PGClaimTick(t, f, ctx, "rollback")
	var drift atomic.Bool
	var reads atomic.Int32
	h := ownedB2PGHandler(t, f, &drift, &reads, fakePDS{decision: ownedProducerRealDecision()})
	var before tickPreparation
	if err := h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
		var err error
		before, err = fenced.Store.prepareTick(ctx, f.channel.ID, "rollback", agentTickOptions{})
		return err
	}); err != nil || len(before.Candidates) != 1 {
		t.Fatal("rollback fixture preparation failed")
	}
	candidates := append([]TickCandidate{}, before.Candidates...)
	ownedProducerApproveCandidateFixture(t, f.channel, &candidates[0])
	stop := errors.New("synthetic plan queue write failure")
	var blocked atomic.Int32
	err := h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
		fenced.Store.executionDB = ownedB2RollbackTx{Tx: fenced.Store.executionDB.(pgx.Tx), stop: stop, blocked: &blocked}
		return fenced.Store.finalizeTick(ctx, before, candidates, nil)
	})
	if !errors.Is(err, stop) || blocked.Load() != 1 || reads.Load() < 2 {
		t.Fatalf("rollback boundary not reached: %T; blocked=%d observations=%d", err, blocked.Load(), reads.Load())
	}
	f.assertCounts(t, 0)
	var audits int
	if err := f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM agent_tick_audits WHERE channel_profile_id=$1::uuid`, f.channel.ID).Scan(&audits); err != nil || audits != 0 {
		t.Fatal("rollback left a tick audit")
	}
	if err := h.HandleAgentTick(ctx, item); err != nil {
		t.Fatalf("normal queued retry after rollback failed: %T", err)
	}
	f.assertCounts(t, 1)
}

// Parent supplies an existing deployment-principal credential and native worker
// negative credentials on this exact scratch database. This harness issues no DCL
// and never changes role or substitutes the fixture owner after a permission error.
func TestOwnedB2PGConfiguredNativePrincipals(t *testing.T) {
	f := newOwnedPGFixtureWithHistory(t, func(s *Store, at time.Time) map[string]any { return ownedB2PGSeedRetirement(t, s, at) })
	runtimeRaw := os.Getenv("OWNED_HISTORY_B2_RUNTIME_TEST_URL")
	negativeRaw := os.Getenv("OWNED_HISTORY_B2_WORKER_TEST_URL")
	if runtimeRaw == "" || negativeRaw == "" {
		t.Skip("explicit current Go runtime and native worker scratch credentials required")
	}
	anchor, _ := url.Parse(os.Getenv("OWNED_INVENTORY_DISPOSABLE_TEST_URL"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, entry := range []struct {
		raw     string
		allowed bool
	}{{runtimeRaw, true}, {negativeRaw, false}} {
		dsn, err := ownedDisposableURL(entry.raw, os.Getenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM"))
		if err != nil {
			t.Fatal("unsafe native-principal scratch DSN")
		}
		u, _ := url.Parse(dsn)
		if u.Host != anchor.Host || u.Path != anchor.Path || u.User == nil || u.User.Username() == "" {
			t.Fatal("native-principal target differs from confirmed scratch database")
		}
		s, err := OpenStore(ctx, dsn)
		if err != nil {
			t.Fatal("native-principal connection unavailable")
		}
		defer s.Close()
		var current, session string
		if err := s.Pool.QueryRow(ctx, `SELECT current_user,session_user`).Scan(&current, &session); err != nil || current != u.User.Username() || session != current {
			t.Fatal("native principal was substituted")
		}
		_, err = loadOwnedHistorySnapshot(ctx, s.Pool, ownedString(f.data.Inventory["platform_channel_id"]))
		if entry.allowed {
			if err != nil {
				t.Fatal("current native Go principal lacks the B2 read contract")
			}
			s.leadership = f.store.leadership
			item := ownedB2PGClaimTick(t, f, ctx, "native-principal")
			var drift atomic.Bool
			var reads atomic.Int32
			h := ownedB2PGHandler(t, f, &drift, &reads, fakePDS{decision: ownedProducerRealDecision()})
			h.Store = s
			if err := h.HandleAgentTick(ctx, item); err != nil {
				t.Fatalf("native Go principal cannot execute bounded queued admission: %T", err)
			}
			f.assertCounts(t, 1)
		} else {
			var isWorker bool
			if err := s.Pool.QueryRow(ctx, `SELECT pg_has_role(current_user,'vp_worker_runtime','member')`).Scan(&isWorker); err != nil || !isWorker {
				t.Fatal("negative credential is not the existing native worker principal")
			}
			if err == nil {
				t.Fatal("native worker unexpectedly read operator history")
			}
		}
	}
}
