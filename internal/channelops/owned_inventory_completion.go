package channelops

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Accounting only. This does not classify retirement or authorize new production.
func completedOwnedInventoryItems(snapshot ownedHistorySnapshot, inventoryID, publicationID string, now time.Time) (ids []string, err error) {
	defer func() {
		if p := recover(); p != nil {
			if reason, ok := p.(ownedHistoryError); ok {
				ids, err = nil, reason
			} else {
				panic(p)
			}
		}
	}()
	age := now.Sub(snapshot.observedAt)
	historyRequire(age >= 0 && age <= 60*time.Second, "owned_history_observation_stale")
	rows := snapshot.rows()
	row := historyOne(historySelect(rows["owned_seed_inventories"], func(r map[string]any) bool { return r["id"] == inventoryID }), "owned_inventory_scope")
	manifest, err := decodeOwnedHistoryManifest([]byte(historyCanonical(row["manifest_json"])))
	if err != nil {
		return nil, err
	}
	data := historyObject(historyDecode([]byte(manifest.document)))
	historyRequire(row["manifest_sha256"] == historyHash(data) && data["inventory_id"] == inventoryID && data["platform_channel_id"] == snapshot.platformChannelID &&
		historyEqual(historyFields(row, "channel_profile_id target_account_id platform_channel_id"), historyFields(data, "channel_profile_id target_account_id platform_channel_id")) &&
		row["approved_at"] != nil && !historyTime(row["approved_at"]).After(now), "owned_inventory_manifest_changed")
	channel := historyOne(historySelect(rows["channel_profiles"], func(c map[string]any) bool { return c["id"] == row["channel_profile_id"] }), "owned_inventory_scope")
	historyRequire(channel["halted_at"] == nil, "channel_halted")
	historyRequire(channel["enabled"] == true && channel["dry_run"] == false, "owned_inventory_channel_disabled")
	account := historyOne(historySelect(rows["publishing_accounts"], func(a map[string]any) bool { return a["id"] == row["target_account_id"] }), "owned_inventory_scope")
	historyRequire(account["channel_profile_id"] == channel["id"] && historyPlatform(account) == "youtube" && account["platform_account_id"] == snapshot.platformChannelID, "owned_inventory_scope")
	items := historyRows(historySelect(rows["owned_seed_inventory_items"], func(i map[string]any) bool { return i["inventory_id"] == inventoryID }))
	sort.Slice(items, func(i, j int) bool { return historyInt(items[i]["ordinal"]) < historyInt(items[j]["ordinal"]) })
	reserved := 0
	for _, item := range items {
		if item["state"] == "reserved" {
			reserved++
		}
	}
	historyRequire(len(items) == 7 && reserved <= 1, "owned_inventory_cardinality_invalid")
	entries := historyRows(data["entries"])
	unusedSeen := false
	for index, item := range items {
		historyRequire(historyEqual(historyFields(item, "id ordinal asset_id manual_seed_id content_sha256"), historyFields(entries[index], "id ordinal asset_id manual_seed_id content_sha256")) && item["platform_channel_id"] == snapshot.platformChannelID, "owned_inventory_item_binding")
		if item["state"] == "unused" {
			historyRequire(item["production_task_id"] == nil && item["consumed_at"] == nil && item["completed_at"] == nil, "owned_inventory_consumption")
			unusedSeen = true
			continue
		}
		historyRequire(!unusedSeen && historyIs(item["state"], "reserved", "completed"), "owned_inventory_consumption")
		consumed := historyTime(item["consumed_at"])
		historyRequire(!consumed.Before(historyTime(data["starts_at"])) && consumed.Before(historyTime(data["expires_at"])) && !consumed.After(now), "owned_inventory_consumption")
		if item["state"] == "reserved" {
			historyRequire(item["completed_at"] == nil, "owned_inventory_consumption")
		} else {
			historyRequire(historyBetween(historyTime(item["completed_at"]), consumed, now), "owned_inventory_consumption")
		}
		task := historyOne(historySelect(rows["production_tasks"], func(t map[string]any) bool { return t["id"] == item["production_task_id"] }), "owned_inventory_missing_task")
		historyRequire(task["channel_profile_id"] == channel["id"] && task["target_account_id"] == account["id"] && task["manual_seed_id"] == item["manual_seed_id"], "owned_inventory_history_identity")
		facts := historyTask(rows, task)
		for _, op := range historyRows(facts["operations"]) {
			historyRequire(op["privacy"] == "unlisted", "owned_inventory_receipt")
		}
		_, complete, _ := historyNormal(facts, item, now)
		if complete && (publicationID == "" || len(historySelect(facts["publications"], func(p map[string]any) bool { return p["id"] == publicationID })) == 1) {
			ids = append(ids, historyString(item["id"]))
		}
	}
	return ids, nil
}

// Call after the exact reconcile queue transition, within its existing fence/TX.
// An empty publicationID is the open-intake repair hook. Neither path commits.
func (s *Store) finalizeOwnedInventoryItems(ctx context.Context, channelID, publicationID string) error {
	if !s.hasExecutionTransaction() || s.executionChannelID == nil || *s.executionChannelID != channelID {
		return errOwnedInventory
	}
	db := s.db()
	if err := s.assertLeaderAuthority(ctx, db, false); err != nil {
		return err
	}
	if err := lockExecutableChannel(ctx, db, channelID, false); err != nil {
		return err
	}
	var inventoryID *string
	var err error
	if publicationID == "" {
		err = db.QueryRow(ctx, `SELECT owned_seed_inventory_id::text FROM channel_profiles WHERE id=$1::uuid`, channelID).Scan(&inventoryID)
	} else {
		err = db.QueryRow(ctx, `SELECT i.inventory_id::text FROM owned_seed_inventory_items i JOIN publication_records p ON p.production_task_id=i.production_task_id WHERE p.id=$1::uuid`, publicationID).Scan(&inventoryID)
	}
	if errors.Is(err, pgx.ErrNoRows) || err == nil && inventoryID == nil {
		return nil
	}
	if err != nil {
		return err
	}
	var schedule, platform string
	if err = db.QueryRow(ctx, `SELECT state FROM runtime_schedules WHERE service_name='videoprocess' FOR UPDATE`).Scan(&schedule); err != nil {
		return err
	}
	if err = db.QueryRow(ctx, `SELECT platform_channel_id FROM owned_seed_inventories WHERE id=$1::uuid`, *inventoryID).Scan(&platform); err != nil {
		return err
	}
	if _, err = db.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ownedPlatformKey(platform)); err != nil {
		return err
	}
	var boundChannel string
	if err = db.QueryRow(ctx, `SELECT channel_profile_id::text FROM owned_seed_inventories WHERE id=$1::uuid FOR UPDATE`, *inventoryID).Scan(&boundChannel); err != nil {
		return err
	}
	if boundChannel != channelID {
		return errOwnedInventory
	}
	locked, err := db.Query(ctx, `SELECT id FROM owned_seed_inventory_items WHERE inventory_id=$1::uuid ORDER BY ordinal FOR UPDATE`, *inventoryID)
	if err != nil {
		return err
	}
	for locked.Next() {
	}
	err = locked.Err()
	locked.Close()
	if err != nil {
		return err
	}
	snapshot, err := loadOwnedHistorySnapshot(ctx, db, platform)
	if err != nil {
		return err
	}
	var now time.Time
	if err = db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	ids, err := completedOwnedInventoryItems(snapshot, *inventoryID, publicationID, now)
	if err != nil {
		var reason ownedHistoryError
		if !errors.As(err, &reason) {
			return err
		}
		if _, err = db.Exec(ctx, `UPDATE owned_seed_inventories SET state='held',hold_reason=$2,updated_at=$3::timestamp WHERE id=$1::uuid AND state IN ('approved','exhausted')`, *inventoryID, string(reason), now); err != nil {
			return err
		}
		_, err = db.Exec(ctx, `UPDATE channel_profiles SET intake_paused_at=COALESCE(intake_paused_at,$3),intake_pause_reason=COALESCE(intake_pause_reason,$4) WHERE id=$1::uuid AND owned_seed_inventory_id=$2::uuid`, channelID, *inventoryID, now, string(reason))
		return err
	}
	for _, id := range ids {
		tag, err := db.Exec(ctx, `UPDATE owned_seed_inventory_items SET state='completed',completed_at=$3 WHERE id=$1::uuid AND inventory_id=$2::uuid AND state='reserved'`, id, *inventoryID, now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errOwnedInventory
		}
	}
	return nil
}
