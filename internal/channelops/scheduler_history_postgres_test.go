package channelops

import (
	"context"
	"strings"
	"testing"
	"time"
)

func insertPausedSchedulerFixture(t *testing.T, store *Store, id string, now time.Time) {
	t.Helper()
	_, err := store.Pool.Exec(context.Background(), `INSERT INTO channel_profiles (
 id,name,positioning,language,default_aspect_ratio,risk_policy_json,content_mix_policy_json,
 cadence_policy_json,alert_policy_json,enabled,dry_run,config_version,tick_interval_minutes,
 created_at,updated_at,intake_paused_at,intake_pause_reason)
 VALUES($1::uuid,'sealed scheduler fixture','','en','9:16','{}','{}','{}','{}',TRUE,FALSE,1,60,$2::timestamptz,$2::timestamptz,$2::timestamptz,'fixture pause')`, id, now)
	if err != nil {
		t.Fatal(err)
	}
}

// Exercise the real installed 040/046 seals. These synthetic histories test the
// scheduler's exclusion boundary, not history qualification or operator approval.
func TestOwnedSchedulerDoesNotEnqueueIntoSealedHistory(t *testing.T) {
	for _, kind := range []string{"retained", "retired"} {
		for _, state := range []string{"approved", "held", "exhausted", "expired", "revoked", "released"} {
			t.Run(kind+"/"+state, func(t *testing.T) {
				ctx := context.Background()
				legacyID := ownedNewUUID(t)
				f := newOwnedPGFixtureWithHistory(t, func(s *Store, now time.Time) map[string]any {
					insertPausedSchedulerFixture(t, s, legacyID, now.Add(-time.Hour))
					if kind == "retired" {
						return map[string]any{"version": 1, "bindings": []any{}, "retired_unassigned_preupload": map[string]any{
							"legacy_channel_profile_id": legacyID, "legacy_account_id": ownedNewUUID(t), "retained_facts": map[string]any{}, "terminal_graph": map[string]any{}}}
					}
					return map[string]any{"version": 2, "bindings": []any{map[string]any{"legacy_channel_profile_id": legacyID, "legacy_account_id": ownedNewUUID(t)}},
						"retired_unassigned_preupload": nil, "retained_terminal_uploads": []any{map[string]any{
							"legacy_channel_profile_id": legacyID, "task_id": ownedNewUUID(t), "publication_id": ownedNewUUID(t),
							"retained_facts": map[string]any{"queues": []any{}, "nodes": []any{}, "metrics": []any{}, "promotion_operations": []any{}}}}}
				})
				now := f.store.Now().UTC()
				if _, err := f.store.Pool.Exec(ctx, `UPDATE channel_profiles SET intake_paused_at=clock_timestamp(),intake_pause_reason='fixture initialization' WHERE id=$1::uuid`, f.channel.ID); err != nil {
					t.Fatal(err)
				}
				if state != "approved" {
					desired := state
					if state == "released" {
						desired = "exhausted"
					}
					if _, err := f.store.Pool.Exec(ctx, `UPDATE owned_seed_inventories SET state=$2::varchar,
      succession_released_at=CASE WHEN $3 THEN clock_timestamp() ELSE NULL END,
      revoked_at=CASE WHEN $2::varchar='revoked' THEN clock_timestamp() ELSE NULL END
      WHERE id=$1::uuid`, *f.channel.OwnedSeedInventoryID, desired, state == "released"); err != nil {
						t.Fatal(err)
					}
				}
				// The seal remains enforced even after the inventory stops admitting work.
				if _, err := f.store.Enqueue(ctx, EnqueueOptions{Kind: QueueLearningRecompute, IdempotencyKey: "must-remain-sealed:" + legacyID,
					Payload: map[string]any{"channel_id": legacyID}, ChannelProfileID: &legacyID}); err == nil || !strings.Contains(err.Error(), "owned_history_sealed") {
					t.Fatalf("native historical seal was not exercised: %v", err)
				}
				ordinaryID := ownedNewUUID(t)
				insertPausedSchedulerFixture(t, f.store, ordinaryID, now)
				t.Cleanup(func() {
					_, _ = f.store.Pool.Exec(context.Background(), `UPDATE channel_profiles SET enabled=FALSE WHERE id=$1::uuid`, ordinaryID)
				})
				if count, err := (Scheduler{Store: f.store}).RunOnce(ctx, now); err != nil || count != 0 {
					t.Fatalf("paused scheduler should remain healthy: count=%d error=%v", count, err)
				}
				var legacyQueues, ordinaryLearning, currentLearning, cleanup int
				if err := f.store.Pool.QueryRow(ctx, `SELECT
      (SELECT count(*) FROM channel_ops_queue_items WHERE channel_profile_id=$1::uuid),
      (SELECT count(*) FROM channel_ops_queue_items WHERE channel_profile_id=$2::uuid AND kind='learning_recompute'),
      (SELECT count(*) FROM channel_ops_queue_items WHERE channel_profile_id=$3::uuid AND kind='learning_recompute'),
      (SELECT count(*) FROM channel_ops_queue_items WHERE idempotency_key=$4)`, legacyID, ordinaryID, f.channel.ID, CleanupIdempotencyKey(now.Format("2006-01-02"))).Scan(&legacyQueues, &ordinaryLearning, &currentLearning, &cleanup); err != nil {
					t.Fatal(err)
				}
				if legacyQueues != 0 || ordinaryLearning != 1 || currentLearning != 1 || cleanup != 1 {
					t.Fatalf("queue counts legacy/ordinary/current/cleanup=%d/%d/%d/%d", legacyQueues, ordinaryLearning, currentLearning, cleanup)
				}
			})
		}
	}
}

func TestOwnedSchedulerHistoryExclusionOnlyForApprovedV2(t *testing.T) {
	f := newOwnedPGFixture(t)
	ctx := context.Background()
	tx, err := f.store.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// TEMP copies isolate selector edge cases from immutable approved public facts.
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE channel_profiles (LIKE public.channel_profiles);
 CREATE TEMP TABLE owned_seed_inventories (LIKE public.owned_seed_inventories);`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO channel_profiles SELECT * FROM public.channel_profiles WHERE id=$1::uuid;
 `, f.channel.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO owned_seed_inventories SELECT * FROM public.owned_seed_inventories WHERE id=$1::uuid`, *f.channel.OwnedSeedInventoryID); err != nil {
		t.Fatal(err)
	}
	s := &Store{executionDB: tx}
	for _, tc := range []struct {
		name     string
		version  int
		approved bool
		retired  bool
		matches  bool
		want     int
	}{
		{"approved binding", 2, true, false, true, 0},
		{"draft binding", 2, false, false, true, 1},
		{"version one", 1, true, false, true, 1},
		{"approved retirement", 2, true, true, true, 0},
		{"unapproved retirement", 2, false, true, true, 1},
		{"other channel", 2, true, false, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channel := f.channel.ID
			if !tc.matches {
				channel = ownedNewUUID(t)
			}
			legacy := map[string]any{"bindings": []any{}, "retired_unassigned_preupload": nil}
			if tc.retired {
				legacy["retired_unassigned_preupload"] = map[string]any{"legacy_channel_profile_id": channel}
			} else {
				legacy["bindings"] = []any{map[string]any{"legacy_channel_profile_id": channel}, map[string]any{"legacy_channel_profile_id": channel}}
			}
			if _, err := tx.Exec(ctx, `UPDATE owned_seed_inventories SET manifest_json=$1::json,
    approved_at=CASE WHEN $2 THEN clock_timestamp() ELSE NULL END`, historyJSON(t, map[string]any{"version": tc.version, "legacy_history": legacy}), tc.approved); err != nil {
				t.Fatal(err)
			}
			channels, err := s.ListSchedulableChannels(ctx, f.store.Now())
			if err != nil || len(channels) != tc.want {
				t.Fatalf("channels=%d, error=%v; want %d", len(channels), err, tc.want)
			}
		})
	}
}
