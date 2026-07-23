package channelops

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLeaderStoreCloneSharesLeadershipState(t *testing.T) {
	state := &leaderState{}
	store := &Store{leadership: state}
	clone := store.withExecutionDB(nil, nil)

	if clone.leadership != state {
		t.Fatal("store clone did not share leadership state")
	}
	authority := LeaderAuthority{
		ServiceName: leaderServiceName,
		HolderID:    "runner-a",
		Epoch:       7,
		AcquiredAt:  time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
		HeartbeatAt: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	}
	state.publish(authority)

	configured, got := clone.leadership.snapshot()
	if !configured || got == nil || got.Epoch != authority.Epoch {
		t.Fatalf("clone leadership snapshot = configured %t, authority %#v", configured, got)
	}
}

func TestLeaderBlankHolderRejectedBeforePoolAcquisition(t *testing.T) {
	store := &Store{}

	lease, acquired, err := store.TryAcquireLeader(
		context.Background(),
		" \t\n ",
		time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
	)
	if err == nil || !strings.Contains(err.Error(), "holder") {
		t.Fatalf("blank holder error = %v, want holder validation error", err)
	}
	if acquired || lease != nil {
		t.Fatalf("blank holder result = lease %#v, acquired %t", lease, acquired)
	}
}

func TestLeaderExecutionFenceRequiresAuthority(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)

	dispatched := false
	err := fixture.Store.WithLeaderExecutionFence(ctx, func(*Store) error {
		dispatched = true
		return nil
	})
	if !errors.Is(err, ErrLeaderAuthorityUnavailable) {
		t.Fatalf("unconfigured leader fence error = %v, want ErrLeaderAuthorityUnavailable", err)
	}
	if dispatched {
		t.Fatal("unconfigured leader fence invoked dispatch")
	}
}

func TestLeaderLeaseIsExclusiveAndEpochIncrementsAfterRelease(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	secondStore, err := OpenStore(ctx, LoadConfig().DatabaseURL)
	if err != nil {
		fixture.Close(ctx)
		t.Fatalf("open second store: %v", err)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	firstBaseline := fixture.Store.Pool.Stat().AcquiredConns()
	secondBaseline := secondStore.Pool.Stat().AcquiredConns()
	var first *LeaderLease
	var second *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, second, now.Add(4*time.Second))
		releaseLeaderTestLease(t, ctx, first, now.Add(4*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		secondStore.Close()
		fixture.Close(ctx)
	}()

	first, acquired, err := fixture.Store.TryAcquireLeader(ctx, "runner-a", now)
	if err != nil || !acquired || first == nil {
		t.Fatalf("first acquisition = lease %#v, acquired %t, err %v", first, acquired, err)
	}
	if first.Authority.ServiceName != leaderServiceName || first.Authority.Epoch != 1 {
		t.Fatalf("first authority = %#v, want service %q epoch 1", first.Authority, leaderServiceName)
	}

	second, acquired, err = secondStore.TryAcquireLeader(ctx, "runner-b", now)
	if err != nil {
		t.Fatalf("contended acquisition: %v", err)
	}
	if acquired || second != nil {
		t.Fatalf("contended acquisition = lease %#v, acquired %t", second, acquired)
	}
	if got := secondStore.Pool.Stat().AcquiredConns(); got != secondBaseline {
		t.Fatalf("contended acquisition retained %d connections, baseline %d", got, secondBaseline)
	}

	if err := first.Release(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("release first lease: %v", err)
	}
	if got := fixture.Store.Pool.Stat().AcquiredConns(); got != firstBaseline {
		t.Fatalf("first release retained %d connections, baseline %d", got, firstBaseline)
	}
	if err := first.Release(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("second release of first lease: %v", err)
	}
	if got := fixture.Store.Pool.Stat().AcquiredConns(); got != firstBaseline {
		t.Fatalf("idempotent release changed acquired connections to %d, baseline %d", got, firstBaseline)
	}

	second, acquired, err = secondStore.TryAcquireLeader(ctx, "runner-b", now.Add(2*time.Second))
	if err != nil || !acquired || second == nil {
		t.Fatalf("takeover acquisition = lease %#v, acquired %t, err %v", second, acquired, err)
	}
	if second.Authority.Epoch != 2 {
		t.Fatalf("takeover epoch = %d, want 2", second.Authority.Epoch)
	}
	if err := second.Release(ctx, now.Add(3*time.Second)); err != nil {
		t.Fatalf("release takeover lease: %v", err)
	}
	if got := secondStore.Pool.Stat().AcquiredConns(); got != secondBaseline {
		t.Fatalf("takeover release retained %d connections, baseline %d", got, secondBaseline)
	}

	var releasedAt *time.Time
	if err := secondStore.Pool.QueryRow(ctx, `
		SELECT released_at
		FROM channelops_leader_epochs
		WHERE service_name = $1 AND holder_id = $2 AND epoch = $3
	`, leaderServiceName, "runner-b", int64(2)).Scan(&releasedAt); err != nil {
		t.Fatalf("select released authority: %v", err)
	}
	if releasedAt == nil || !releasedAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("released_at = %v, want %v", releasedAt, now.Add(3*time.Second))
	}
}

func TestLeaderHeartbeatUpdatesMatchingAuthorityAndRevokesStaleLease(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	now := time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC)
	baseline := fixture.Store.Pool.Stat().AcquiredConns()
	var lease *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(4*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()

	lease, acquired, err := fixture.Store.TryAcquireLeader(ctx, "runner-heartbeat", now)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire heartbeat lease = lease %#v, acquired %t, err %v", lease, acquired, err)
	}
	heartbeatAt := now.Add(time.Second)
	if err := lease.Heartbeat(ctx, heartbeatAt); err != nil {
		t.Fatalf("heartbeat matching lease: %v", err)
	}

	var storedHeartbeat time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT heartbeat_at
		FROM channelops_leader_epochs
		WHERE service_name = $1 AND holder_id = $2 AND epoch = $3
	`, leaderServiceName, "runner-heartbeat", int64(1)).Scan(&storedHeartbeat); err != nil {
		t.Fatalf("select heartbeat: %v", err)
	}
	if !storedHeartbeat.Equal(heartbeatAt) {
		t.Fatalf("heartbeat_at = %v, want %v", storedHeartbeat, heartbeatAt)
	}

	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET epoch = epoch + 1
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		t.Fatalf("advance leader epoch: %v", err)
	}
	err = lease.Heartbeat(ctx, now.Add(2*time.Second))
	if !errors.Is(err, ErrLeaderAuthorityLost) {
		t.Fatalf("stale heartbeat error = %v, want ErrLeaderAuthorityLost", err)
	}
	if got := fixture.Store.Pool.Stat().AcquiredConns(); got != baseline {
		t.Fatalf("stale heartbeat retained %d connections, baseline %d", got, baseline)
	}

	var unchangedHeartbeat time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT heartbeat_at
		FROM channelops_leader_epochs
		WHERE service_name = $1
	`, leaderServiceName).Scan(&unchangedHeartbeat); err != nil {
		t.Fatalf("select stale heartbeat row: %v", err)
	}
	if !unchangedHeartbeat.Equal(heartbeatAt) {
		t.Fatalf("stale heartbeat changed row to %v, want %v", unchangedHeartbeat, heartbeatAt)
	}
}

func TestLeaderExecutionFenceRejectsOldLeader(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	now := time.Date(2026, 7, 23, 14, 0, 0, 0, time.UTC)
	var lease *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(2*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()

	lease = acquireLeaderTestLease(t, ctx, fixture.Store, "runner-old", now)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET epoch = epoch + 1
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		t.Fatalf("advance leader epoch: %v", err)
	}

	dispatched := false
	err := fixture.Store.WithLeaderExecutionFence(ctx, func(*Store) error {
		dispatched = true
		return nil
	})
	if !errors.Is(err, ErrLeaderAuthorityLost) {
		t.Fatalf("stale leader fence error = %v, want ErrLeaderAuthorityLost", err)
	}
	if dispatched {
		t.Fatal("stale leader fence invoked dispatch")
	}
}

func TestExecutionFenceRejectsOldLeaderBeforeDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	now := time.Date(2026, 7, 23, 15, 0, 0, 0, time.UTC)
	var lease *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(2*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()

	lease = acquireLeaderTestLease(t, ctx, fixture.Store, "runner-queue-old", now)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET epoch = epoch + 1
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		t.Fatalf("advance leader epoch: %v", err)
	}

	dispatched := false
	err := fixture.Store.WithQueueExecutionFence(ctx, QueueItemRow{
		ID:   "not-a-uuid",
		Kind: "unsupported",
	}, func(*Store) error {
		dispatched = true
		return nil
	})
	if !errors.Is(err, ErrLeaderAuthorityLost) {
		t.Fatalf("stale queue fence error = %v, want ErrLeaderAuthorityLost", err)
	}
	if dispatched {
		t.Fatal("stale queue fence invoked dispatch")
	}
}

func TestLeaderExecutionFencesReuseDomainTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := time.Date(2026, 7, 23, 16, 0, 0, 0, time.UTC)
	var lease *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(time.Second))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()

	lease = acquireLeaderTestLease(t, ctx, fixture.Store, "runner-composed", now)
	err := fixture.Store.WithLeaderExecutionFence(ctx, func(leaderStore *Store) error {
		if !leaderStore.hasExecutionTransaction() {
			t.Fatal("leader fence dispatch has no transaction")
		}
		leaderDB := leaderStore.executionDB

		if err := leaderStore.WithQueueExecutionFence(ctx, QueueItemRow{
			ID:   testUUID(t, "leader-queue-fence"),
			Kind: QueueCleanupExpired,
		}, func(queueStore *Store) error {
			if queueStore.executionDB != leaderDB {
				t.Fatal("queue fence did not reuse leader transaction")
			}
			return nil
		}); err != nil {
			return err
		}

		return leaderStore.WithChannelExecutionFence(ctx, fixture.ChannelID, func(channelStore *Store) error {
			if channelStore.executionDB != leaderDB {
				t.Fatal("channel fence did not reuse leader transaction")
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("composed execution fences: %v", err)
	}
}

func acquireLeaderTestLease(
	t *testing.T,
	ctx context.Context,
	store *Store,
	holderID string,
	now time.Time,
) *LeaderLease {
	t.Helper()
	lease, acquired, err := store.TryAcquireLeader(ctx, holderID, now)
	if err != nil || !acquired || lease == nil {
		t.Fatalf("acquire leader %q = lease %#v, acquired %t, err %v", holderID, lease, acquired, err)
	}
	return lease
}

func releaseLeaderTestLease(
	t *testing.T,
	ctx context.Context,
	lease *LeaderLease,
	now time.Time,
) {
	t.Helper()
	if lease == nil {
		return
	}
	if err := lease.Release(ctx, now); err != nil && !errors.Is(err, ErrLeaderAuthorityLost) {
		t.Errorf("release leader test lease: %v", err)
	}
}
