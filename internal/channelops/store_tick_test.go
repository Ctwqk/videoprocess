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
