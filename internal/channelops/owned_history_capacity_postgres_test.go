package channelops

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Invoked only by the Python capacity fixture in its confirmed 043 child DB.
func TestOwnedPGHistoryCapacityReadOnly(t *testing.T) {
	raw, expected := os.Getenv("OWNED_D_POSTGRES_TEST_URL"), os.Getenv("OWNED_CAPACITY_EXPECTED_SHA256")
	if testing.Short() || raw == "" || expected == "" {
		t.Skip("parent-only capacity PostgreSQL qualification")
	}
	if !strings.HasPrefix(raw, "postgresql+asyncpg://") || !historySHA.MatchString(expected) ||
		!regexp.MustCompile(`^vp_owned_inventory_test_d_[a-z0-9_]+$`).MatchString(os.Getenv("OWNED_D_POSTGRES_TEST_CONFIRM")) ||
		!regexp.MustCompile(`^[0-9]{10,20}$`).MatchString(os.Getenv("OWNED_D_POSTGRES_SYSTEM_ID")) {
		t.Fatal("invalid explicit D capacity qualification")
	}
	dsn, err := ownedDisposableURL(strings.Replace(raw, "postgresql+asyncpg://", "postgresql://", 1), os.Getenv("OWNED_D_POSTGRES_TEST_CONFIRM"))
	if err != nil {
		t.Fatal("invalid explicit disposable database designation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := OpenStore(ctx, dsn)
	if err != nil {
		t.Fatal("capacity database unavailable")
	}
	defer store.Close()
	var system, version, revision string
	if err := store.Pool.QueryRow(ctx, `SELECT system_identifier::text, current_setting('server_version_num'), (SELECT version_num FROM alembic_version) FROM pg_control_system()`).Scan(&system, &version, &revision); err != nil ||
		system != os.Getenv("OWNED_D_POSTGRES_SYSTEM_ID") || !strings.HasPrefix(version, "16") || revision != "043_owned_history_snapshot_rows" {
		t.Fatal("capacity database identity/head mismatch")
	}
	snapshot, err := loadOwnedHistorySnapshot(ctx, store.Pool, "UC"+strings.Repeat("a", 22))
	if err != nil {
		t.Fatal(err)
	}
	if len(historyArray(snapshot.rows()["assets"])) != 8192 || snapshot.snapshotSHA256() != expected {
		t.Fatal("actual Go complete snapshot count/hash mismatch")
	}
}
