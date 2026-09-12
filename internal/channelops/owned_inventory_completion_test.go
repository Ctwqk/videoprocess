package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type ownedCompletionFenceProbe struct {
	pgx.Tx
	halted  bool
	queries []string
	stop    error
}

func (p *ownedCompletionFenceProbe) QueryRow(_ context.Context, query string, _ ...any) pgx.Row {
	p.queries = append(p.queries, query)
	return ownedB2FenceRow(func(dest ...any) error {
		if strings.Contains(query, "SELECT enabled, halted_at, intake_paused_at") {
			*dest[0].(*bool) = true
			now := time.Now().UTC()
			*dest[2].(**time.Time) = &now
			if p.halted {
				*dest[1].(**time.Time) = &now
			}
			return nil
		}
		if strings.Contains(query, "SELECT owned_seed_inventory_id") {
			id := historyTestUID(100)
			*dest[0].(**string) = &id
			return nil
		}
		return p.stop
	})
}

func TestOwnedCompletionRequiresTransactionAndPreservesEmergencyFence(t *testing.T) {
	id := historyTestUID(2)
	if err := (&Store{}).finalizeOwnedInventoryItems(context.Background(), id, ""); !errors.Is(err, errOwnedInventory) {
		t.Fatalf("unfenced helper: %v", err)
	}
	for _, halted := range []bool{false, true} {
		probe := &ownedCompletionFenceProbe{halted: halted, stop: errors.New("bounded schedule stop")}
		store := &Store{executionDB: probe, executionChannelID: &id}
		err := store.finalizeOwnedInventoryItems(context.Background(), id, "")
		if halted {
			if !errors.Is(err, ErrChannelExecutionBlocked) || len(probe.queries) != 1 {
				t.Fatalf("emergency bypass: %v", err)
			}
		} else if !errors.Is(err, probe.stop) || len(probe.queries) != 3 || !strings.Contains(probe.queries[2], "FROM runtime_schedules") {
			t.Fatalf("inventory pause blocked settlement or lock order changed: %v", err)
		}
	}
}

func ownedCompletionFixture(t *testing.T) map[string]any {
	t.Helper()
	f := historyGolden(t, "direct")
	base := historyTestCopy(t, historyTestRows(f)).(map[string]any)
	rows := historyTestRows(f)
	for key := range rows {
		rows[key] = []any{}
	}
	manifest := historyTestCopy(t, historyTestManifest(historyGolden(t, "history_only"))).(map[string]any)
	delete(manifest, "legacy_history")
	manifest["version"] = json.Number("1")
	manifest["channel_profile_id"], manifest["target_account_id"] = historyTestUID(2), historyTestUID(3)
	now := historyAt(t, f["now"])
	manifest["starts_at"], manifest["expires_at"] = historyISO(now.Add(-6*24*time.Hour)), historyISO(now.Add(24*time.Hour))
	items := []any{}
	for index, entry := range historyRows(manifest["entries"]) {
		pairs := []string{"abcdefghijk", fmt.Sprintf("owned%06d", index)}
		for id := 4; id < 45; id++ {
			pairs = append(pairs, historyTestUID(id), historyTestUID(id+10000*(index+1)))
		}
		replacer := strings.NewReplacer(pairs...)
		var rebind func(any) any
		rebind = func(value any) any {
			switch v := value.(type) {
			case string:
				return replacer.Replace(v)
			case []any:
				out := []any{}
				for _, item := range v {
					out = append(out, rebind(item))
				}
				return out
			case map[string]any:
				out := map[string]any{}
				for k, item := range v {
					out[k] = rebind(item)
				}
				return out
			default:
				return value
			}
		}
		graph := rebind(base).(map[string]any)
		task := historyRows(graph["production_tasks"])[0]
		task["manual_seed_id"] = entry["manual_seed_id"]
		historyRows(graph["youtube_upload_operations"])[0]["content_sha256"] = fmt.Sprintf("%064x", index+100)
		for name, records := range graph {
			if (name == "channel_profiles" || name == "publishing_accounts") && index > 0 {
				continue
			}
			rows[name] = append(historyArray(rows[name]), historyArray(records)...)
		}
		item := historyFields(entry, "id ordinal asset_id manual_seed_id content_sha256")
		item["inventory_id"], item["platform_channel_id"], item["production_task_id"] = manifest["inventory_id"], manifest["platform_channel_id"], task["id"]
		item["state"], item["consumed_at"], item["completed_at"] = "completed", historyISO(now.Add(-26*time.Hour)), historyISO(now.Add(-24*time.Hour))
		if index == 6 {
			item["state"], item["completed_at"] = "reserved", nil
		}
		items = append(items, item)
	}
	rows["owned_seed_inventory_items"] = items
	rows["owned_seed_inventories"] = []any{map[string]any{"id": manifest["inventory_id"], "manifest_json": manifest, "manifest_sha256": historyTestHash(t, manifest), "platform_channel_id": manifest["platform_channel_id"], "channel_profile_id": manifest["channel_profile_id"], "target_account_id": manifest["target_account_id"], "approved_at": manifest["starts_at"], "state": "exhausted"}}
	channel := historyRows(rows["channel_profiles"])[0]
	channel["enabled"], channel["dry_run"], channel["halted_at"], channel["intake_paused_at"] = true, false, nil, f["now"]
	return f
}

func TestOwnedCompletionSeventhAndPersistedQueue(t *testing.T) {
	f := ownedCompletionFixture(t)
	rows := historyTestRows(f)
	item := historyRows(rows["owned_seed_inventory_items"])[6]
	pub := historyRows(rows["publication_records"])[6]
	check := func() ([]string, error) {
		return completedOwnedInventoryItems(historySnapshot(t, f), historyTestUID(100), historyString(pub["id"]), historyAt(t, f["now"]))
	}
	ids, err := check()
	if err != nil || len(ids) != 1 || ids[0] != item["id"] {
		t.Fatalf("seventh: %v %v", ids, err)
	}
	queues := historySelect(rows["channel_ops_queue_items"], func(q map[string]any) bool { return q["kind"] == "reconcile_publication" })
	q := historyObject(queues[6])
	q["status"], q["locked_by"], q["locked_at"] = "running", "normal-runner", f["now"]
	if ids, err = check(); err != nil || len(ids) != 0 {
		t.Fatalf("running: %v %v", ids, err)
	}
	q["status"], q["locked_by"], q["locked_at"] = "succeeded", nil, nil
	item["state"], item["completed_at"] = "completed", f["now"]
	if ids, err = check(); err != nil || len(ids) != 0 {
		t.Fatalf("idempotent: %v %v", ids, err)
	}
}

func TestOwnedCompletionRefusals(t *testing.T) {
	for _, fault := range []string{"uncertain", "failed", "private", "receipt", "job", "node", "public", "metric", "held", "future_consumed", "seed", "scope", "halted", "multiple_reserved"} {
		t.Run(fault, func(t *testing.T) {
			f := ownedCompletionFixture(t)
			rows := historyTestRows(f)
			op := historyRows(rows["youtube_upload_operations"])[6]
			item := historyRows(rows["owned_seed_inventory_items"])[6]
			switch fault {
			case "uncertain", "failed":
				op["status"] = fault
			case "private":
				op["privacy"] = "private"
			case "receipt":
				historyObject(op["receipt_json"])["video_id"] = "wrong-video"
			case "job":
				historyRows(rows["jobs"])[6]["status"] = "FAILED"
			case "node":
				historyRows(rows["node_executions"])[6]["status"] = "FAILED"
			case "public":
				historyRows(rows["publication_records"])[6]["current_privacy"] = "public"
			case "metric":
				historyRows(rows["publication_metric_schedules"])[0]["status"] = "failed"
			case "held":
				item["state"] = "held"
			case "future_consumed":
				item["consumed_at"] = historyISO(historyAt(t, f["now"]).Add(time.Hour))
			case "seed":
				historyRows(rows["production_tasks"])[6]["manual_seed_id"] = historyTestUID(9090)
			case "scope":
				historyRows(rows["production_tasks"])[6]["target_account_id"] = historyTestUID(9090)
			case "halted":
				historyRows(rows["channel_profiles"])[0]["halted_at"] = f["now"]
			case "multiple_reserved":
				historyRows(rows["owned_seed_inventory_items"])[0]["state"] = "reserved"
			}
			ids, err := completedOwnedInventoryItems(historySnapshot(t, f), historyTestUID(100), "", historyAt(t, f["now"]))
			if err == nil || len(ids) != 0 {
				t.Fatalf("accepted %s: %v %v", fault, ids, err)
			}
		})
	}
}
