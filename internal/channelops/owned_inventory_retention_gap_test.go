package channelops

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
