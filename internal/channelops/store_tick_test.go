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
