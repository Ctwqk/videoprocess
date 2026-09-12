package channelops

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestOwnedHistorySnapshotCapacity(t *testing.T) {
	for _, count := range []int{4097, 4930, 8192, 8193} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			f := historyGolden(t, "direct")
			rows := historyTestRows(f)
			values := []any{}
			for i := count - 1; i >= 0; i-- {
				values = append(values, map[string]any{"id": historyTestUID(i)})
			}
			rows["assets"] = values
			db := &ownedHistoryReadStub{rows: historyJSON(t, rows), at: historyAt(t, f["observed_at"])}
			snapshot, err := loadOwnedHistorySnapshot(context.Background(), db, f["platform_channel_id"].(string))
			if count == 8193 {
				if err == nil || snapshot.rowsJSON != "" {
					t.Fatal("overflow accepted", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(snapshot.rows()["assets"].([]any)) != count || db.calls != 1 || strings.Count(db.query, "LIMIT 8193") != 27 {
				t.Fatal("capacity lost rows or bounded MVCC statement")
			}
			for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
				values[i], values[j] = values[j], values[i]
			}
			if snapshot.snapshotSHA256() != historyTestHash(t, rows) {
				t.Fatal("sorted digest changed")
			}
		})
	}
}

func TestOwnedHistoryCapacityTailStillClassified(t *testing.T) {
	f := historyGolden(t, "direct")
	rows := historyTestRows(f)
	original := historyTestFirst(f, "node_executions")
	for i := 0; i < 4096; i++ {
		node := historyTestCopy(t, original).(map[string]any)
		node["id"], node["job_id"] = historyTestUID(10000+i), historyTestUID(99999)
		rows["node_executions"] = append(rows["node_executions"].([]any), node)
	}
	if got := historyTestAssess(t, f); got.BlockReason != nil {
		t.Fatal(*got.BlockReason)
	}
	bad := historyTestCopy(t, original).(map[string]any)
	bad["id"], bad["status"] = "ffffffff-ffff-ffff-ffff-ffffffffffff", "RUNNING"
	rows["node_executions"] = append(rows["node_executions"].([]any), bad)
	historyTestReason(t, f, "")
}

func TestOwnedHistoryCapacityDoesNotExpandProofBounds(t *testing.T) {
	if ownedHistoryMaxRows != 4096 || ownedHistoryMaxBytes != 16*1024*1024 {
		t.Fatal("proof/byte bound changed")
	}
	f := historyGolden(t, "retired_unassigned")
	certificate := historyObject(historyObject(historyTestManifest(f)["legacy_history"])["retired_unassigned_preupload"])
	graph := historyObject(certificate["terminal_graph"])
	historyParseGraph(graph)
	original := historyArray(graph["node_executions"])[0]
	values := []any{}
	for i := 0; i < 4097; i++ {
		row := historyTestCopy(t, original).(map[string]any)
		row["id"] = historyTestUID(10000 + i)
		values = append(values, row)
	}
	graph["node_executions"] = values
	var err error
	func() { defer historyRecover(&err, ""); historyParseGraph(graph) }()
	if err == nil {
		t.Fatal("oversized terminal graph accepted")
	}
}
