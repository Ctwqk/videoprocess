package channelops

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func ownedClosureSeven(t *testing.T, data *ownedInventoryData, now time.Time) map[string]any {
	t.Helper()
	data.Inventory["state"] = "exhausted"
	channel := ownedMap(data.Bindings["channel"])
	channel["owned_seed_inventory_id"], channel["intake_paused_at"], channel["intake_pause_reason"] = data.Inventory["id"], ownedISO(now), "owned_inventory_exhausted"
	data.Tasks = nil
	for i, input := range data.Items {
		// Historical seed only: 25h separation leaves both upload 24h floors
		// intact. No fake Store.Now or assertion of an elapsed seven-day run.
		at := now.Add(-time.Duration(2+(6-i)*25) * time.Hour).Truncate(time.Second)
		id := ownedNewUUID(t)
		item := input.Item
		item["production_task_id"], item["consumed_at"], item["state"], item["completed_at"] = id, ownedISO(at.Add(-time.Hour)), "completed", ownedISO(at.Add(30*time.Minute))
		input.Seed["status"] = "exhausted"
		evidence := map[string]any{"inventory_id": data.Inventory["id"], "item_id": item["id"], "manifest_sha256": data.Inventory["manifest_sha256"], "configuration_sha256": ownedMap(data.Inventory["manifest_json"])["configuration_sha256"], "input_asset_id": item["asset_id"], "source_content_sha256": item["content_sha256"], "seed_sha256": item["seed_sha256"]}
		task := map[string]any{"id": id, "title_seed": input.Seed["title_seed"], "prompt": input.Seed["prompt"], "topic_lane_id": data.Inventory["topic_lane_id"], "lane_format_id": data.Inventory["lane_format_id"], "agent_approval_evidence_json": map[string]any{"owned_inventory": evidence}, "channel_config_snapshot_json": map[string]any{"owned_inventory": evidence}, "rationale_json": map[string]any{}}
		h := ownedClosureHistory(t, *data, i, task, at, now, "completion")
		ownedClosureRows(h)
		if i == 6 {
			item["state"], item["completed_at"] = "reserved", nil
			q := historyOne(historySelect(h["queues"], func(q map[string]any) bool { return q["kind"] == QueueReconcilePublication }), "fixture_reconcile")
			q["status"], q["attempt_count"] = "queued", 0
		}
		data.Tasks = append(data.Tasks, h)
	}
	return data.Tasks[6]
}

func ownedCompletionPGFixture(t *testing.T) (*ownedPGFixture, QueueItemRow, PublicationRow) {
	t.Helper()
	f := newOwnedPGFixtureWithWindow(t, nil, 167*time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var now time.Time
	if err := f.store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	seventh := ownedClosureSeven(t, &f.data, now)
	for i, h := range f.data.Tasks {
		ownedClosureSeed(t, f, ctx, h, false)
		item := f.data.Items[i].Item
		var completed *time.Time
		if item["completed_at"] != nil {
			at := historyTime(item["completed_at"])
			completed = &at
		}
		if _, err := f.store.Pool.Exec(ctx, `UPDATE owned_seed_inventory_items SET production_task_id=$2::uuid,state=$3,consumed_at=$4::timestamptz,completed_at=$5::timestamptz WHERE id=$1::uuid`, item["id"], item["production_task_id"], item["state"], historyTime(item["consumed_at"]), completed); err != nil {
			t.Fatal("fixture native item binding", err)
		}
		if _, err := f.store.Pool.Exec(ctx, `UPDATE manual_seeds SET status='exhausted' WHERE id=$1::uuid`, item["manual_seed_id"]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.store.Pool.Exec(ctx, `UPDATE owned_seed_inventories SET state='exhausted' WHERE id=$1::uuid`, *f.channel.OwnedSeedInventoryID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Pool.Exec(ctx, `UPDATE channel_profiles SET intake_paused_at=clock_timestamp(),intake_pause_reason='owned_inventory_exhausted' WHERE id=$1::uuid`, f.channel.ID); err != nil {
		t.Fatal(err)
	}
	item := ownedClosureClaim(t, f, ctx, QueueReconcilePublication)
	pub, err := f.store.GetPublication(ctx, ownedString(historyRows(seventh["publications"])[0]["id"]))
	if err != nil || item.PayloadJSON["publication_id"] != pub.ID {
		t.Fatal("exact seventh publication claim", err)
	}
	snapshot, err := loadOwnedHistorySnapshot(ctx, f.store.Pool, ownedString(f.data.Inventory["platform_channel_id"]))
	if err != nil {
		t.Fatal("native completion fixture reread", err)
	}
	if ids, err := completedOwnedInventoryItems(snapshot, *f.channel.OwnedSeedInventoryID, pub.ID, snapshot.observedAt); err != nil || len(ids) != 0 {
		t.Fatalf("native running completion fixture: completion_count=%d classifier_reason=%v", len(ids), err)
	}
	return f, item, pub
}

func ownedCompletionPGState(t *testing.T, f *ownedPGFixture, ctx context.Context, item QueueItemRow, pub PublicationRow) []byte {
	t.Helper()
	var raw []byte
	if err := f.store.Pool.QueryRow(ctx, `SELECT jsonb_build_object('inventory',(SELECT jsonb_build_object('state',v.state,'hold_reason',v.hold_reason) FROM owned_seed_inventories v WHERE v.id=$3::uuid),'publication',to_jsonb(p),'queue',to_jsonb(q),'items',(SELECT jsonb_agg(to_jsonb(i) ORDER BY ordinal) FROM owned_seed_inventory_items i WHERE i.inventory_id=$3::uuid),'metrics',(SELECT jsonb_agg(to_jsonb(m) ORDER BY m.id) FROM publication_metric_schedules m WHERE m.publication_id=p.id),'future_queues',(SELECT jsonb_agg(to_jsonb(f) ORDER BY f.id) FROM channel_ops_queue_items f WHERE f.payload_json->>'publication_id'=p.id::text AND f.kind='collect_metrics')) FROM publication_records p CROSS JOIN channel_ops_queue_items q WHERE p.id=$1::uuid AND q.id=$2::uuid`, pub.ID, item.ID, *f.channel.OwnedSeedInventoryID).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

type ownedCompletionPGYouTube struct {
	fakeYouTube
	read  func(context.Context) error
	calls int
}

func (api *ownedCompletionPGYouTube) PublicationStatus(ctx context.Context, video string) (YouTubePublicationStatus, error) {
	api.calls++
	if api.read != nil {
		if err := api.read(ctx); err != nil {
			return YouTubePublicationStatus{}, err
		}
	}
	return YouTubePublicationStatus{VideoID: video, Privacy: "unlisted", PublishStatus: "scheduled", Permalink: "https://www.youtube.com/watch?v=" + video}, nil
}

type ownedCompletionRollbackTx struct {
	pgx.Tx
	t       *testing.T
	queueID string
	pubID   string
	blocked *atomic.Int32
	stop    error
}

func (tx ownedCompletionRollbackTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.HasPrefix(sql, "UPDATE owned_seed_inventory_items SET state='completed'") {
		tx.blocked.Add(1)
		var publication, queue string
		var owner *string
		var locked *time.Time
		if err := tx.Tx.QueryRow(ctx, `SELECT p.publish_status,q.status,q.locked_by,q.locked_at FROM publication_records p CROSS JOIN channel_ops_queue_items q WHERE p.id=$1::uuid AND q.id=$2::uuid`, tx.pubID, tx.queueID).Scan(&publication, &queue, &owner, &locked); err != nil || publication != "scheduled" || queue != QueueStatusSucceeded || owner != nil || locked != nil {
			tx.t.Fatal("item update was not preceded by actual publication and cleared successful queue", err)
		}
		return pgconn.CommandTag{}, tx.stop
	}
	return tx.Tx.Exec(ctx, sql, args...)
}

func TestOwnedProducerPGSeventhReconcileAtomicCompletion(t *testing.T) {
	for _, mode := range []string{"commit_replay", "rollback", "lease_loss", "leader_loss"} {
		t.Run(mode, func(t *testing.T) {
			f, item, pub := ownedCompletionPGFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			before := ownedCompletionPGState(t, f, ctx, item, pub)
			api := &ownedCompletionPGYouTube{}
			h := HandlerService{Store: f.store, YouTube: api}
			if mode == "rollback" {
				stop := errors.New("synthetic actual item update failure")
				var blocked atomic.Int32
				err := h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
					task, err := fenced.Store.GetProductionTask(ctx, pub.ProductionTaskID)
					if err != nil {
						return err
					}
					fenced.Store.executionDB = ownedCompletionRollbackTx{Tx: fenced.Store.executionDB.(pgx.Tx), t: t, queueID: item.ID, pubID: pub.ID, blocked: &blocked, stop: stop}
					return fenced.Store.finishOwnedReconcile(ctx, item, pub, task, YouTubePublicationStatus{VideoID: pub.PlatformContentID, Privacy: "unlisted", PublishStatus: "scheduled"})
				})
				if !errors.Is(err, stop) || blocked.Load() != 1 || !bytes.Equal(before, ownedCompletionPGState(t, f, ctx, item, pub)) {
					t.Fatal("actual item-write failure did not roll back every row", err, blocked.Load())
				}
			}
			if mode == "lease_loss" || mode == "leader_loss" {
				api.read = func(ctx context.Context) error {
					if mode == "leader_loss" {
						return ownedReleaseLeaderAtDBTime(ctx, f.store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`), f.lease.Release)
					}
					if err := f.store.ReleaseQueueClaim(ctx, item); err != nil {
						return err
					}
					replacement := ownedClosureClaim(t, f, ctx, QueueReconcilePublication)
					if replacement.ID != item.ID || replacement.LockedAt.Equal(*item.LockedAt) {
						t.Fatal("queue replacement did not acquire a fresh exact lease")
					}
					return nil
				}
			}
			err := h.HandleReconcilePublication(ctx, item)
			afterRaw := ownedCompletionPGState(t, f, ctx, item, pub)
			beforeValue, _ := ownedDecode(before)
			afterValue, _ := ownedDecode(afterRaw)
			old, after := ownedMap(beforeValue), ownedMap(afterValue)
			if api.calls != 1 {
				t.Fatal("normal reconcile did not use exactly one external observation")
			}
			for _, key := range []string{"metrics", "future_queues"} {
				if !ownedPolicyJSONEqual(old[key], after[key]) {
					t.Fatal("normal completion mutated future native metrics", key)
				}
			}
			if mode == "lease_loss" || mode == "leader_loss" {
				want := ErrQueueLeaseLost
				if mode == "leader_loss" {
					want = ErrLeaderAuthorityUnavailable
				}
				if !errors.Is(err, want) || !ownedPolicyJSONEqual(old["publication"], after["publication"]) || !ownedPolicyJSONEqual(old["items"], after["items"]) || ownedMap(after["queue"])["status"] != QueueStatusRunning {
					t.Fatal("lost authority wrote completion or publication", err)
				}
				return
			}
			if err != nil {
				t.Fatal("actual normal seventh reconcile", err)
			}
			queue := ownedMap(after["queue"])
			items := historyRows(after["items"])
			if queue["status"] != QueueStatusSucceeded || queue["locked_at"] != nil || queue["locked_by"] != nil || ownedMap(after["publication"])["publish_status"] != "scheduled" || items[6]["state"] != "completed" || items[6]["completed_at"] == nil {
				inventory := ownedMap(after["inventory"])
				t.Fatalf("completion transaction: inventory_state=%v hold_reason=%v publication_status=%v queue_status=%v lease_owner_present=%t lease_time_present=%t item_count=%d seventh_state=%v seventh_completed_at_present=%t", inventory["state"], inventory["hold_reason"], ownedMap(after["publication"])["publish_status"], queue["status"], queue["locked_by"] != nil, queue["locked_at"] != nil, len(items), items[6]["state"], items[6]["completed_at"] != nil)
			}
			for i := 0; i < 6; i++ {
				if !ownedPolicyJSONEqual(historyRows(old["items"])[i], items[i]) {
					t.Fatal("earlier completed item changed")
				}
			}
			var state, reason string
			if err := f.store.Pool.QueryRow(ctx, `SELECT i.state,c.intake_pause_reason FROM owned_seed_inventories i JOIN channel_profiles c ON c.owned_seed_inventory_id=i.id WHERE i.id=$1::uuid`, *f.channel.OwnedSeedInventoryID).Scan(&state, &reason); err != nil || state != "exhausted" || reason != "owned_inventory_exhausted" {
				t.Fatal("completion altered exhausted pointer/pause", err)
			}
			if err := completeCommittedQueueClaim(ctx, f.store, item); err != nil {
				t.Fatal("normal runner committed-result cleanup", err)
			}
			if err := h.HandleReconcilePublication(ctx, item); !errors.Is(err, ErrQueueLeaseLost) || api.calls != 1 {
				t.Fatal("stale committed reentry repeated external observation", err)
			}
			if !bytes.Equal(afterRaw, ownedCompletionPGState(t, f, ctx, item, pub)) {
				t.Fatal("committed-result cleanup/replay changed rows")
			}
		})
	}
}

func TestOwnedClosureSevenFixtureUsesNormalClassifier(t *testing.T) {
	for _, location := range []*time.Location{time.UTC, time.FixedZone("pgx-local-minus-seven", -7*60*60)} {
		t.Run(location.String(), func(t *testing.T) {
			_, data, now := ownedTestFixture(t)
			now = now.In(location)
			manifest := ownedMap(data.Inventory["manifest_json"])
			starts := now.Add(-167 * time.Hour)
			data.Inventory["starts_at"], data.Inventory["expires_at"], data.Inventory["approved_at"] = ownedISO(starts), ownedISO(starts.Add(168*time.Hour)), ownedISO(starts)
			manifest["starts_at"], manifest["expires_at"] = data.Inventory["starts_at"], data.Inventory["expires_at"]
			data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
			seventh := ownedClosureSeven(t, &data, now)
			pub := historyRows(seventh["publications"])[0]
			check := func() ([]string, error) {
				return completedOwnedInventoryItems(ownedProducerSnapshot(t, data, now), ownedString(data.Inventory["id"]), ownedString(pub["id"]), now)
			}
			if ids, err := check(); err != nil || len(ids) != 0 {
				t.Fatal("queued native fixture pretended to complete", ids, err)
			}
			queue := historyOne(historySelect(seventh["queues"], func(q map[string]any) bool { return q["kind"] == QueueReconcilePublication }), "fixture_reconcile")
			queue["status"], queue["attempt_count"] = "succeeded", 1
			if ids, err := check(); err != nil || len(ids) != 1 || ids[0] != data.Items[6].Item["id"] {
				t.Fatal("shared normal history fixture invalid", ids, err)
			}
			queue["attempt_count"] = 2
			if ids, err := check(); err == nil || err.Error() != "owned_inventory_queue_failed" || len(ids) != 0 {
				t.Fatal("recovered attempt2 must retain strict completion refusal", ids, err)
			}
			queue["attempt_count"] = 1
			data.Items[6].Item["state"], data.Items[6].Item["completed_at"] = "completed", ownedISO(now)
			if ids, err := completedOwnedInventoryItems(ownedProducerSnapshot(t, data, now), ownedString(data.Inventory["id"]), "", now); err != nil || len(ids) != 0 {
				t.Fatal("unfiltered accounting must validate but not rewrite completed items", ids, err)
			}
		})
	}
}
