package channelops

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func requireRecoveryTruncateGuard(t *testing.T, ctx context.Context, row pgx.Row) {
	t.Helper()
	var enabled string
	if err := row.Scan(&enabled); err != nil || enabled != "O" {
		t.Fatalf("recovery audit truncate guard = %q, want enabled: %v", enabled, err)
	}
}

const recoveryTruncateGuardQuery = `SELECT tgenabled::text FROM pg_trigger
	WHERE tgrelid='public.owned_inventory_recoveries'::regclass
	AND tgname='owned_inventory_recovery_audit_no_truncate'`

func TestSnapshotFixtureCleanupRestoresRecoveryAuditGuard(t *testing.T) {
	ctx := context.Background()
	f := NewChannelOpsFixture(t)
	defer f.Close(ctx)
	f.InsertChannelWithLaneAccountSeed(ctx)
	if err := f.cleanupRows(ctx); err != nil {
		t.Fatal(err)
	}
	requireRecoveryTruncateGuard(t, ctx, f.snapshotFixtureLock.QueryRow(ctx, recoveryTruncateGuardQuery))
	if _, err := f.snapshotFixtureLock.Exec(ctx, `TRUNCATE public.owned_inventory_recoveries CASCADE`); err == nil || !strings.Contains(err.Error(), "owned_inventory_recovery_audit_immutable") {
		t.Fatalf("empty audit direct truncate must remain forbidden: %v", err)
	}
}

func TestSnapshotFixtureCleanupRefusesNonemptyRecoveryAudit(t *testing.T) {
	ctx := context.Background()
	f := NewChannelOpsFixture(t)
	defer f.Close(ctx)
	f.InsertChannelWithLaneAccountSeed(ctx)
	tx, err := f.snapshotFixtureLock.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Synthetic immutable facts exist only inside this rolled-back transaction.
	defer tx.Rollback(ctx)
	var inventoryID string
	err = tx.QueryRow(ctx, `INSERT INTO owned_seed_inventories
		(client_request_id,channel_profile_id,topic_lane_id,lane_format_id,target_account_id,
		 platform_channel_id,request_sha256,manifest_sha256,manifest_json,starts_at,expires_at,created_by)
		VALUES(gen_random_uuid(),$1::uuid,$2::uuid,$3::uuid,$4::uuid,'UCaaaaaaaaaaaaaaaaaaaaaa',
		 repeat('a',64),repeat('b',64),'{}',now(),now()+interval '168 hours','synthetic-cleanup-test')
		RETURNING id::text`, f.ChannelID, f.LaneID, f.FormatID, f.AccountID).Scan(&inventoryID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO owned_inventory_recoveries
		(id,inventory_id,request_sha256,expected_hold_sha256,recovered_by,recovered_at,transaction_id,
		 before_inventory_json,before_channel_json,evidence_json)
		VALUES(gen_random_uuid(),$1::uuid,repeat('c',64),repeat('d',64),'synthetic-cleanup-test',now(),
		 txid_current(),'{}','{}','{}')`, inventoryID)
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanupChannelOpsFixtureRows(ctx, tx); err == nil || !strings.Contains(err.Error(), "recovery audit is not empty") {
		t.Fatalf("cleanup must preserve nonempty audit: %v", err)
	}
	var auditCount, channelCount int
	if err := tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM owned_inventory_recoveries),
		(SELECT count(*) FROM channel_profiles WHERE id=$1::uuid)`, f.ChannelID).Scan(&auditCount, &channelCount); err != nil || auditCount != 1 || channelCount != 1 {
		t.Fatalf("cleanup changed retained rows: audit=%d channel=%d err=%v", auditCount, channelCount, err)
	}
	requireRecoveryTruncateGuard(t, ctx, tx.QueryRow(ctx, recoveryTruncateGuardQuery))
}

func TestSnapshotFixtureCleanupFailureRollsBackTriggerAndData(t *testing.T) {
	ctx := context.Background()
	f := NewChannelOpsFixture(t)
	defer f.Close(ctx)
	f.InsertChannelWithLaneAccountSeed(ctx)
	_, err := f.snapshotFixtureLock.Exec(ctx, `
		CREATE FUNCTION pg_temp.refuse_fixture_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'synthetic_fixture_cleanup_failure'; END $$;
		CREATE TRIGGER synthetic_fixture_cleanup_failure BEFORE TRUNCATE ON public.channel_profiles
		FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.refuse_fixture_cleanup()`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := f.snapshotFixtureLock.Exec(ctx, `DROP TRIGGER synthetic_fixture_cleanup_failure ON public.channel_profiles`); err != nil {
			t.Error(err)
		}
	}()
	if err := f.cleanupRows(ctx); err == nil || !strings.Contains(err.Error(), "synthetic_fixture_cleanup_failure") {
		t.Fatalf("expected transactional cleanup failure: %v", err)
	}
	requireRecoveryTruncateGuard(t, ctx, f.snapshotFixtureLock.QueryRow(ctx, recoveryTruncateGuardQuery))
	var count int
	if err := f.snapshotFixtureLock.QueryRow(ctx, `SELECT count(*) FROM channel_profiles WHERE id=$1::uuid`, f.ChannelID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed cleanup changed fixture data: count=%d err=%v", count, err)
	}
}

func TestSnapshotFixtureCleanupRequiresHeldFixtureLock(t *testing.T) {
	ctx := context.Background()
	f := NewChannelOpsFixture(t)
	defer f.Close(ctx)
	if _, err := f.snapshotFixtureLock.Exec(ctx, `SELECT pg_advisory_unlock(774403030044)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := f.snapshotFixtureLock.Exec(ctx, `SELECT pg_advisory_lock(774403030044)`); err != nil {
			t.Error(err)
		}
	}()
	if err := f.cleanupRows(ctx); err == nil || !strings.Contains(err.Error(), "fixture lock is not held") {
		t.Fatalf("cleanup accepted a connection without fixture ownership: %v", err)
	}
	requireRecoveryTruncateGuard(t, ctx, f.snapshotFixtureLock.QueryRow(ctx, recoveryTruncateGuardQuery))
}
