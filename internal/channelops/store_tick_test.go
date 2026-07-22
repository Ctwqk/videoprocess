package channelops

import (
	"os"
	"strings"
	"testing"
)

func TestProductionTaskInsertIncludesRequiredPriority(t *testing.T) {
	source, err := os.ReadFile("store_tick.go")
	if err != nil {
		t.Fatalf("read store_tick.go: %v", err)
	}
	columns := productionTaskInsertColumnsForTest(string(source))
	if !strings.Contains(columns, "priority") {
		t.Fatalf("production_tasks INSERT columns missing required priority column: %s", columns)
	}
}

func TestStoreTickWritesDecisionAuditEntries(t *testing.T) {
	source, err := os.ReadFile("store_tick.go")
	if err != nil {
		t.Fatalf("read store_tick.go: %v", err)
	}
	text := string(source)
	for _, want := range []string{"INSERT INTO decision_audit_entries", "created_task_id", "learning_context_json"} {
		if !strings.Contains(text, want) {
			t.Fatalf("store_tick.go missing %q", want)
		}
	}
}

func TestRunTickUsesTransactionForDecisionAuditWrites(t *testing.T) {
	source, err := os.ReadFile("store_tasks.go")
	if err != nil {
		t.Fatalf("read store_tasks.go: %v", err)
	}
	text := string(source)
	for _, want := range []string{"s.beginOrReuse(ctx)", "tx.Commit(ctx)", "tx.Rollback(ctx)"} {
		if !strings.Contains(text, want) {
			t.Fatalf("RunTick transaction path missing %q", want)
		}
	}
}

func TestRunTickAcquiresOpenIntakeFenceBeforeReadingInputs(t *testing.T) {
	source, err := os.ReadFile("store_tasks.go")
	if err != nil {
		t.Fatalf("read store_tasks.go: %v", err)
	}
	text := string(source)
	fence := strings.Index(text, "withChannelExecutionFence(ctx, channelID, true")
	load := strings.Index(text, "s.LoadTickInputs(ctx, channelID, now)")
	if fence < 0 || load < 0 || fence > load {
		t.Fatalf("RunTick must acquire the open-intake channel fence before loading inputs")
	}
}

func productionTaskInsertColumnsForTest(source string) string {
	start := strings.Index(source, "INSERT INTO production_tasks")
	if start < 0 {
		return ""
	}
	open := strings.Index(source[start:], "(")
	if open < 0 {
		return ""
	}
	columnStart := start + open + 1
	close := strings.Index(source[columnStart:], ")")
	if close < 0 {
		return ""
	}
	return source[columnStart : columnStart+close]
}
