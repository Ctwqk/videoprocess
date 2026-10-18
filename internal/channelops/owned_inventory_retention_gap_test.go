package channelops

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOwnedRetainedHistoryAllowsNextInventoryCandidate(t *testing.T) {
	for _, reuseLegacyTask := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy_task_reuse_%v", reuseLegacyTask), func(t *testing.T) {
			f := retentionGolden(t)
			now := historyAt(t, f["now"])
			channel, data, _ := ownedTestFixtureOffset(t, 10000)
			data.Inventory["approved_at"] = ownedISO(now)
			data.Inventory["starts_at"] = ownedISO(now.Add(-time.Hour))
			data.Inventory["expires_at"] = ownedISO(now.Add(167 * time.Hour))
			manifest := ownedMap(data.Inventory["manifest_json"])
			manifest["version"] = 2
			manifest["starts_at"], manifest["expires_at"] = data.Inventory["starts_at"], data.Inventory["expires_at"]
			manifest["legacy_history"] = historyTestCopy(t, historyTestManifest(f)["legacy_history"])
			data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
			if reuseLegacyTask {
				data.Items[0].Item["state"] = "reserved"
				data.Items[0].Item["production_task_id"] = historyTestFirst(f, "production_tasks")["id"]
				data.Items[0].Item["consumed_at"] = ownedISO(now)
				data.Items[0].Seed["status"] = "exhausted"
			}
			rows := ownedTestHistoryRows(t, data)
			for table, records := range historyTestRows(f) {
				rows[table] = append(historyArray(rows[table]), historyArray(records)...)
			}
			snapshot, err := newOwnedHistorySnapshot(historyJSON(t, rows), ownedString(data.Inventory["platform_channel_id"]), now, []byte("[]"))
			if err != nil {
				t.Fatal(err)
			}
			data.History = &snapshot
			data.Busy = !ownedQueuesSafe(channel.ID, data, now)
			state := assessOwnedInventory(channel, data, now)
			if reuseLegacyTask {
				if state.HoldReason == "" || state.Candidate != nil || len(state.CompleteItemIDs) != 0 {
					t.Fatalf("legacy upload credited to new inventory: %+v", state)
				}
			} else if state.HoldReason != "" || state.SkipReason != "" || state.Candidate == nil || len(state.CompleteItemIDs) != 0 || state.Candidate.owned.ItemID != data.Items[0].Item["id"] {
				t.Fatalf("certified history blocked the next owned candidate: %+v", state)
			}
		})
	}
}

func TestOwnedRetainedHistoryQueueAdmission(t *testing.T) {
	for _, active := range []string{"none", "current_tick", "dirty_tick", "other_tick", "legacy_metric", "missing", "unapproved", "changed_fact"} {
		t.Run(active, func(t *testing.T) {
			f := retentionGolden(t)
			rows := historyTestRows(f)
			inventory := historyTestFirst(f, "owned_seed_inventories")
			channel := historyString(inventory["channel_profile_id"])
			switch active {
			case "missing":
				delete(historyObject(historyTestManifest(f)["legacy_history"]), "retained_terminal_uploads")
				historyTestRehash(t, f)
			case "unapproved":
				inventory["approved_at"] = nil
			case "changed_fact":
				historyTestFirst(f, "feedback_snapshots")["views"] = json.Number("987654")
			case "none":
			default:
				q := historyObject(historyTestCopy(t, historyTestFirst(f, "channel_ops_queue_items")))
				q["id"], q["channel_profile_id"] = historyTestUID(999), channel
				q["kind"], q["status"], q["attempt_count"] = QueueAgentTick, "running", json.Number("1")
				q["last_error"], q["dead_letter_at"], q["parent_queue_item_id"] = nil, nil, nil
				q["locked_at"], q["locked_by"] = f["now"], "current-runner"
				q["payload_json"] = map[string]any{"channel_id": channel}
				switch active {
				case "dirty_tick":
					q["attempt_count"] = json.Number("2")
				case "other_tick":
					q["channel_profile_id"] = historyTestUID(998)
					q["payload_json"] = map[string]any{"channel_id": historyTestUID(998)}
				case "legacy_metric":
					q["kind"] = QueueCollectMetrics
					q["channel_profile_id"] = historyTestFirst(f, "production_tasks")["channel_profile_id"]
					q["payload_json"] = map[string]any{"publication_id": historyTestFirst(f, "publication_records")["id"]}
				}
				rows["channel_ops_queue_items"] = append(historyArray(rows["channel_ops_queue_items"]), q)
			}
			snapshot := historySnapshot(t, f)
			data := ownedInventoryData{History: &snapshot}
			for _, q := range historyRows(rows["channel_ops_queue_items"]) {
				if q["status"] == "queued" || q["status"] == "running" {
					data.Queues = append(data.Queues, q)
				}
			}
			want := active == "none" || active == "current_tick"
			if got := ownedQueuesSafe(channel, data, historyAt(t, f["now"])); got != want {
				t.Fatalf("queues safe = %v, want %v", got, want)
			}
		})
	}
}

func retentionGolden(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "backend", "tests", "fixtures", "owned_seed_inventory_retention_gap.json"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := ownedDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return historyObject(value)
}

func TestOwnedRetainedTerminalCertificateQualifiesWithoutNewItemCredit(t *testing.T) {
	f := retentionGolden(t)
	historyTestFullAssessment(t, f, f["expected"])
	result := historyTestAssess(t, f)
	if result.BlockReason != nil {
		t.Fatalf("blocked: %s", *result.BlockReason)
	}
	if result.WaitReason != nil || len(result.CompletedItemIDs) != 0 || len(result.Classifications) != 1 || result.Classifications[0].Classification != "history_only" {
		t.Fatalf("wrong assessment: %+v", result)
	}
}

func applyRetentionChanges(f, c map[string]any) {
	for _, v := range historyArray(c["changes"]) {
		change := historyObject(v)
		path := historyArray(change["path"])
		var target any = f
		for _, key := range path[:len(path)-1] {
			switch parent := target.(type) {
			case map[string]any:
				target = parent[historyString(key)]
			case []any:
				target = parent[int(historyInt(key))]
			}
		}
		historyObject(target)[historyString(path[len(path)-1])] = change["value"]
	}
}

func TestOwnedRetainedTerminalSemanticRefusalsWithMatchingHashes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "backend", "tests", "fixtures", "owned_seed_inventory_retention_gap_cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	value, err := ownedDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range historyArray(value) {
		c := historyObject(v)
		t.Run(historyString(c["name"]), func(t *testing.T) {
			f := retentionGolden(t)
			applyRetentionChanges(f, c)
			rows := historyTestRows(f)
			inv := historyTestFirst(f, "owned_seed_inventories")
			section := historyObject(historyTestManifest(f)["legacy_history"])
			cert := historyObject(historyArray(section["retained_terminal_uploads"])[0])
			facts := historyTask(rows, historyTestFirst(f, "production_tasks"))
			facts["promotion_operations"] = rows["publication_promotion_operations"]
			cert["retained_facts"] = facts
			cert["retained_facts_sha256"] = historyTestHash(t, facts)
			q := historyObject(historyObject(historyArray(section["bindings"])[0])["qualification"])
			historyObject(historyArray(q["sanitized_facts"])[0])["operation_sha256"] = historyTestHash(t, historyTestFirst(f, "youtube_upload_operations"))
			q["facts_sha256"] = historyTestHash(t, q["sanitized_facts"])
			inv["manifest_sha256"] = historyTestHash(t, inv["manifest_json"])
			historyTestReason(t, f, "")
		})
		if strings.HasPrefix(historyString(c["name"]), "pending_delivery_") {
			t.Run("acknowledged_"+historyString(c["name"]), func(t *testing.T) {
				f := retentionGolden(t)
				applyRetentionChanges(f, c)
				for _, d := range historyRows(historyTestRows(f)["registered_worker_event_deliveries"]) {
					d["ack_state"] = "acknowledged"
				}
				if result := historyTestAssess(t, f); result.BlockReason != nil {
					t.Fatalf("terminal delivery blocked: %s", *result.BlockReason)
				}
			})
		}
	}
}

func TestOwnedRetainedTerminalStoppedLegacyControls(t *testing.T) {
	for _, control := range []string{"disabled", "dry_run", "halted_at", "intake_paused_at"} {
		t.Run(control, func(t *testing.T) {
			f := retentionGolden(t)
			channel := historyTestFirst(f, "channel_profiles")
			channel["enabled"] = true
			switch control {
			case "disabled":
				channel["enabled"] = false
			case "dry_run":
				channel["dry_run"] = true
			default:
				channel[control] = f["now"]
			}
			if result := historyTestAssess(t, f); result.BlockReason != nil {
				t.Fatalf("stopped producer blocked: %s", *result.BlockReason)
			}
		})
	}
}
