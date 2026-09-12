package channelops

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func ownedTestHistoryRows(t *testing.T, data ownedInventoryData) map[string]any {
	t.Helper()
	rows := map[string]any{}
	for _, table := range strings.Fields(historyTableNames) {
		rows[table] = []any{}
	}
	appendRow := func(table string, row map[string]any) {
		if len(row) != 0 {
			rows[table] = append(rows[table].([]any), row)
		}
	}
	appendRow("owned_seed_inventories", data.Inventory)
	appendRow("channel_profiles", ownedMap(data.Bindings["channel"]))
	appendRow("publishing_accounts", ownedMap(data.Bindings["account"]))
	for _, id := range data.AccountIDs {
		if id != ownedString(ownedMap(data.Bindings["account"])["id"]) {
			a := historyTestCopy(t, data.Bindings["account"]).(map[string]any)
			a["id"] = id
			appendRow("publishing_accounts", a)
		}
	}
	for _, item := range data.Items {
		appendRow("owned_seed_inventory_items", item.Item)
		appendRow("manual_seeds", item.Seed)
		appendRow("assets", item.Asset)
	}
	for _, h := range data.Tasks {
		appendRow("production_tasks", ownedMap(h["task"]))
		appendRow("jobs", ownedMap(h["job"]))
		for key, table := range map[string]string{"operations": "youtube_upload_operations", "nodes": "node_executions", "artifacts": "artifacts", "publications": "publication_records", "metrics": "publication_metric_schedules", "feedback": "feedback_snapshots", "queues": "channel_ops_queue_items"} {
			for _, row := range ownedArray(h[key]) {
				appendRow(table, ownedMap(row))
			}
		}
	}
	if data.UnknownOperation {
		appendRow("youtube_upload_operations", map[string]any{"id": ownedTestID(990), "production_task_id": nil})
	}
	return rows
}

func ownedTestAssess(t *testing.T, channel ChannelProfileRow, data ownedInventoryData, now time.Time) ownedTickState {
	t.Helper()
	snapshot, err := newOwnedHistorySnapshot(historyJSON(t, ownedTestHistoryRows(t, data)), ownedString(data.Inventory["platform_channel_id"]), now, []byte("[]"))
	if err != nil {
		return ownedTickState{HoldReason: err.Error()}
	}
	data.History = &snapshot
	return assessOwnedInventory(channel, data, now)
}

func ownedTestQueuesSafe(t *testing.T, channelID string, data ownedInventoryData, now time.Time) bool {
	t.Helper()
	snapshot, err := newOwnedHistorySnapshot(historyJSON(t, ownedTestHistoryRows(t, data)), ownedString(data.Inventory["platform_channel_id"]), now, []byte("[]"))
	if err != nil {
		return false
	}
	data.History = &snapshot
	data.Queues = append([]map[string]any{}, data.Queues...)
	for i, q := range data.Queues {
		data.Queues[i] = ownedMap(historyTestCopy(t, q))
	}
	return ownedQueuesSafe(channelID, data, now)
}

func ownedB2Fixture(t *testing.T, historyName string) (ChannelProfileRow, ownedInventoryData, time.Time, map[string]any, []byte) {
	t.Helper()
	channel, data, now := ownedTestFixtureOffset(t, 10000)
	data.Inventory["approved_at"] = ownedISO(now)
	rows := ownedTestHistoryRows(t, data)
	redisJSON := []byte("[]")
	manifest := ownedMap(data.Inventory["manifest_json"])
	manifest["version"] = 2
	manifest["legacy_history"] = map[string]any{"version": 1, "bindings": []any{}, "retired_unassigned_preupload": nil}
	if historyName != "" {
		f := historyGolden(t, historyName)
		for table, records := range historyTestRows(f) {
			rows[table] = append(rows[table].([]any), records.([]any)...)
		}
		if historyName != "direct" {
			manifest["legacy_history"] = historyTestCopy(t, historyTestManifest(f)["legacy_history"])
		}
		redisJSON = historyJSON(t, f["redis_observations"])
	}
	data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
	return channel, data, now, rows, redisJSON
}

func ownedB2Assess(t *testing.T, channel ChannelProfileRow, data ownedInventoryData, now time.Time, rows map[string]any, redisJSON []byte) ownedTickState {
	t.Helper()
	snapshot, err := newOwnedHistorySnapshot(historyJSON(t, rows), ownedString(data.Inventory["platform_channel_id"]), now, redisJSON)
	if err != nil {
		t.Fatal(err)
	}
	data.History = &snapshot
	return assessOwnedInventory(channel, data, now)
}

func TestOwnedB2UsesA1HistoryMembershipAndGlobalRetirement(t *testing.T) {
	for _, name := range []string{"", "history_only", "retired_unassigned"} {
		t.Run(name, func(t *testing.T) {
			channel, data, now, rows, redisJSON := ownedB2Fixture(t, name)
			state := ownedB2Assess(t, channel, data, now, rows, redisJSON)
			if state.HoldReason != "" || state.SkipReason != "" || state.Candidate == nil {
				t.Fatalf("valid v2 foundation refused: %+v", state)
			}
			if state.Candidate.Account.ID != data.Inventory["target_account_id"] || state.Candidate.owned == nil {
				t.Fatal("history account became production target")
			}
			if !validOwnedHash(state.HistorySHA) || !validOwnedHash(state.HistoryAuthoritySHA) {
				t.Fatal("missing stable effect or retained authority binding")
			}
			if name == "retired_unassigned" && (len(state.RetiredSourceSHA256) != 1 || len(state.RetiredRenderSHA256) != 1 || len(state.CompleteItemIDs) != 0) {
				t.Fatal("retirement became an effect/completion or lost global exclusions")
			}
		})
	}
}

func TestOwnedB2GlobalRetiredHashReuseRefused(t *testing.T) {
	for _, digest := range []string{strings.Repeat("a", 64), strings.Repeat("d", 64)} {
		channel, data, now, rows, redisJSON := ownedB2Fixture(t, "retired_unassigned")
		data.Items[0].Item["content_sha256"] = digest
		manifest := ownedMap(data.Inventory["manifest_json"])
		ownedMap(ownedArray(manifest["entries"])[0])["content_sha256"] = digest
		data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
		state := ownedB2Assess(t, channel, data, now, rows, redisJSON)
		if state.HoldReason != "owned_inventory_retired_hash_reuse" || state.Candidate != nil {
			t.Fatalf("globally retained content was reusable: %+v", state)
		}
	}
}

func TestOwnedB2MissingAndChangedHistoryRefuses(t *testing.T) {
	channel, data, now, rows, redisJSON := ownedB2Fixture(t, "history_only")
	if state := assessOwnedInventory(channel, data, now); state.Candidate != nil || state.HoldReason == "" {
		t.Fatal("missing snapshot accepted")
	}
	before := ownedB2Assess(t, channel, data, now, rows, redisJSON)
	if before.Candidate == nil {
		t.Fatal(before.HoldReason)
	}
	rows["youtube_upload_operations"] = append(rows["youtube_upload_operations"].([]any), map[string]any{"id": ownedTestID(999), "production_task_id": nil})
	if state := ownedB2Assess(t, channel, data, now, rows, redisJSON); state.HoldReason != "owned_history_orphan" || state.Candidate != nil {
		t.Fatal("new blank operation was hidden", state.HoldReason)
	}
}

func TestOwnedB2V1NeverAcceptsV2Fields(t *testing.T) {
	channel, data, now, rows, redisJSON := ownedB2Fixture(t, "")
	manifest := ownedMap(data.Inventory["manifest_json"])
	manifest["version"] = 1
	data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
	if state := ownedB2Assess(t, channel, data, now, rows, redisJSON); state.HoldReason == "" || state.Candidate != nil {
		t.Fatal("v1 document consumed v2 legacy fields")
	}
}

func TestOwnedB2RetainedPredecessorAndAuthorityDrift(t *testing.T) {
	channel, data, now, rows, redisJSON := ownedB2Fixture(t, "history_only")
	before := ownedB2Assess(t, channel, data, now, rows, redisJSON)
	predecessor := historyObject(rows["owned_seed_inventories"].([]any)[1])
	predecessor["state"] = "revoked"
	after := ownedB2Assess(t, channel, data, now, rows, redisJSON)
	if after.Candidate == nil || after.HistorySHA != before.HistorySHA || after.HistoryAuthoritySHA != before.HistoryAuthoritySHA {
		t.Fatal("predecessor revocation erased immutable authority")
	}
	predecessor["approval_reference"] = "synthetic:changed-proof"
	changed := ownedB2Assess(t, channel, data, now, rows, redisJSON)
	oldDigest, _ := ownedAdmissionDigest(data, before)
	newDigest, _ := ownedAdmissionDigest(data, changed)
	if changed.Candidate == nil || changed.HistorySHA != before.HistorySHA || changed.HistoryAuthoritySHA == before.HistoryAuthoritySHA || oldDigest == newDigest {
		t.Fatal("retained authority drift was missing from PDS binding")
	}
	probe := &ownedHoldProbe{stop: errors.New("offline hold reached")}
	err := (&Store{executionDB: probe}).finalizeOwnedTick(context.Background(), tickPreparation{Owned: &before, InputDigest: oldDigest}, tickPreparation{Owned: &changed, InputDigest: newDigest}, nil)
	if !errors.Is(err, probe.stop) || probe.reason != "owned_inventory_inputs_changed" {
		t.Fatal("changed proof after PDS did not hold")
	}
}

func TestOwnedB2HistoryOnlyCanonicalAliasCannotBecomeTarget(t *testing.T) {
	channel, data, now, rows, redisJSON := ownedB2Fixture(t, "history_only")
	legacy := historyObject(rows["publishing_accounts"].([]any)[1])
	legacy["platform_account_id"] = data.Inventory["platform_channel_id"]
	for _, inv := range historyRows(rows["owned_seed_inventories"]) {
		manifest := historyObject(inv["manifest_json"])
		binding := historyObject(historyObject(manifest["legacy_history"])["bindings"].([]any)[0])
		binding["account_descriptor_sha256"] = historyTestHash(t, historyFields(legacy, "id channel_profile_id platform platform_account_id credential_ref platform_specific_config_json"))
		inv["manifest_sha256"] = historyTestHash(t, manifest)
	}
	state := ownedB2Assess(t, channel, data, now, rows, redisJSON)
	if state.Candidate == nil || state.Candidate.Account.ID == legacy["id"] {
		t.Fatal("qualified canonical alias displaced the sole production target", state.HoldReason)
	}
	snapshot, err := newOwnedHistorySnapshot(historyJSON(t, rows), ownedString(data.Inventory["platform_channel_id"]), now, redisJSON)
	if err != nil {
		t.Fatal(err)
	}
	data.History = &snapshot
	data.Inventory["target_account_id"] = legacy["id"]
	if err := ownedHistoryProductionTarget(data, now); err == nil {
		t.Fatal("retained history-only target accepted")
	}
}

type ownedB2FenceRow func(...any) error

func (r ownedB2FenceRow) Scan(dest ...any) error { return r(dest...) }

type ownedB2FenceProbe struct {
	pgx.Tx
	t       *testing.T
	queries []string
	stop    error
}

func (p *ownedB2FenceProbe) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	p.queries = append(p.queries, sql)
	return ownedB2FenceRow(func(dest ...any) error {
		if strings.Contains(sql, "FROM channelops_leader_epochs") {
			for _, d := range dest {
				*d.(*time.Time) = time.Now().UTC()
			}
			return nil
		}
		return p.stop
	})
}
func TestOwnedB2ScheduleFencePrecedesCanonicalAndInventory(t *testing.T) {
	channel, _, _ := ownedTestFixture(t)
	probe := &ownedB2FenceProbe{t: t, stop: errors.New("controlled schedule wait")}
	leader := &leaderState{}
	leader.publish(LeaderAuthority{ServiceName: "channelops", HolderID: "offline", Epoch: 1})
	s := &Store{executionDB: probe, executionChannelID: &channel.ID, leadership: leader}
	_, err := s.prepareOwnedTick(context.Background(), channel, "offline", agentTickOptions{})
	if !errors.Is(err, probe.stop) || len(probe.queries) != 2 || !strings.Contains(probe.queries[1], "FROM runtime_schedules WHERE service_name='videoprocess' FOR UPDATE") {
		t.Fatal("canonical/inventory work preceded the schedule serialization point")
	}
}
