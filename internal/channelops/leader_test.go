package channelops

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLeaderLeaseAuthorityIsNotExportedMutableState(t *testing.T) {
	if _, ok := reflect.TypeFor[LeaderLease]().FieldByName("Authority"); ok {
		t.Fatal("LeaderLease exposes mutable Authority field")
	}
}

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

func TestLeaderAuthorityAccessorReturnsCopyAndReleaseUsesCanonicalIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	now := time.Date(2026, 7, 23, 11, 0, 0, 0, time.UTC)
	baseline := fixture.Store.Pool.Stat().AcquiredConns()
	var lease *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(2*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()

	lease = acquireLeaderTestLease(t, ctx, fixture.Store, "runner-canonical", now)
	mutated := lease.Authority()
	mutated.HolderID = "caller-mutated"
	mutated.Epoch = 99

	canonical := lease.Authority()
	if canonical.HolderID != "runner-canonical" || canonical.Epoch != 1 {
		t.Fatalf("canonical authority changed through returned copy: %#v", canonical)
	}
	if err := lease.Release(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("release canonical lease: %v", err)
	}
	if got := fixture.Store.Pool.Stat().AcquiredConns(); got != baseline {
		t.Fatalf("canonical release retained %d connections, baseline %d", got, baseline)
	}

	var releasedAt *time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT released_at
		FROM channelops_leader_epochs
		WHERE service_name = $1 AND holder_id = $2 AND epoch = $3
	`, leaderServiceName, "runner-canonical", int64(1)).Scan(&releasedAt); err != nil {
		t.Fatalf("select canonical release: %v", err)
	}
	if releasedAt == nil || !releasedAt.Equal(now.Add(time.Second)) {
		t.Fatalf("canonical released_at = %v, want %v", releasedAt, now.Add(time.Second))
	}

	dispatched := false
	err := fixture.Store.WithLeaderExecutionFence(ctx, func(*Store) error {
		dispatched = true
		return nil
	})
	if !errors.Is(err, ErrLeaderAuthorityUnavailable) || dispatched {
		t.Fatalf("released canonical authority fence = dispatched %t, err %v", dispatched, err)
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
	firstAuthority := first.Authority()
	if firstAuthority.ServiceName != leaderServiceName || firstAuthority.Epoch != 1 {
		t.Fatalf("first authority = %#v, want service %q epoch 1", firstAuthority, leaderServiceName)
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
	secondAuthority := second.Authority()
	if secondAuthority.Epoch != 2 {
		t.Fatalf("takeover epoch = %d, want 2", secondAuthority.Epoch)
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

func TestLeaderIndeterminateLockAttemptDiscardsSession(t *testing.T) {
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
	now := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	baseline := fixture.Store.Pool.Stat().AcquiredConns()
	indeterminateErr := errors.New("simulated lost advisory-lock response")
	var replacement *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, replacement, now.Add(2*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		secondStore.Close()
		fixture.Close(ctx)
	}()

	fixture.Store.leaderLockAttempt = func(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
		locked, err := tryLeaderAdvisoryLock(ctx, conn)
		if err != nil {
			return false, err
		}
		if !locked {
			return false, errors.New("test lock attempt did not acquire advisory lock")
		}
		return false, indeterminateErr
	}

	lease, acquired, err := fixture.Store.TryAcquireLeader(ctx, "runner-indeterminate", now)
	if !errors.Is(err, indeterminateErr) {
		t.Fatalf("indeterminate acquisition error = %v, want simulated response error", err)
	}
	if acquired || lease != nil {
		t.Fatalf("indeterminate acquisition = lease %#v, acquired %t", lease, acquired)
	}
	waitLeaderPoolAcquiredBaseline(t, fixture.Store.Pool, baseline, "indeterminate acquisition cleanup")

	replacement, acquired, err = secondStore.TryAcquireLeader(ctx, "runner-after-indeterminate", now.Add(time.Second))
	if err != nil || !acquired || replacement == nil {
		t.Fatalf("acquisition after indeterminate result = lease %#v, acquired %t, err %v", replacement, acquired, err)
	}
	if authority := replacement.Authority(); authority.Epoch != 1 {
		t.Fatalf("replacement epoch = %d, want 1", authority.Epoch)
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

func TestLeaderHeartbeatAndReleaseAreRaceSafeAndReleaseExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	store, tracer := openLeaderTestStoreWithTracer(t, ctx)
	now := time.Date(2026, 7, 23, 13, 30, 0, 0, time.UTC)
	baselineAcquired := store.Pool.Stat().AcquiredConns()
	baselineReleases := tracer.releases.Load()
	var lease *LeaderLease
	var replacement *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, replacement, now.Add(4*time.Second))
		releaseLeaderTestLease(t, ctx, lease, now.Add(4*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		store.Close()
		fixture.Close(ctx)
	}()

	lease = acquireLeaderTestLease(t, ctx, store, "runner-race", now)
	start := make(chan struct{})
	heartbeatResult := make(chan error, 1)
	releaseResult := make(chan error, 1)
	readerStarted := make(chan struct{})
	stopReader := make(chan struct{})
	readerDone := make(chan struct{})

	go func() {
		close(readerStarted)
		defer close(readerDone)
		for {
			select {
			case <-stopReader:
				return
			default:
				_ = lease.Authority()
			}
		}
	}()
	<-readerStarted
	go func() {
		<-start
		heartbeatResult <- lease.Heartbeat(ctx, now.Add(time.Second))
	}()
	go func() {
		<-start
		releaseResult <- lease.Release(ctx, now.Add(2*time.Second))
	}()
	close(start)

	heartbeatErr := <-heartbeatResult
	releaseErr := <-releaseResult
	close(stopReader)
	<-readerDone
	if heartbeatErr != nil && !errors.Is(heartbeatErr, ErrLeaderAuthorityLost) {
		t.Fatalf("concurrent heartbeat error = %v", heartbeatErr)
	}
	if releaseErr != nil {
		t.Fatalf("concurrent release error = %v", releaseErr)
	}
	if got := tracer.releases.Load(); got != baselineReleases+1 {
		t.Fatalf("lease connection release count = %d, want %d", got, baselineReleases+1)
	}
	if got := store.Pool.Stat().AcquiredConns(); got != baselineAcquired {
		t.Fatalf("concurrent heartbeat/release retained %d connections, baseline %d", got, baselineAcquired)
	}

	replacement = acquireLeaderTestLease(t, ctx, store, "runner-after-race", now.Add(3*time.Second))
	if authority := replacement.Authority(); authority.Epoch != 2 {
		t.Fatalf("post-race acquisition epoch = %d, want 2", authority.Epoch)
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

func TestLeaderTakeoverWaitsForOldFencedTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	secondStore, err := OpenStore(ctx, LoadConfig().DatabaseURL)
	if err != nil {
		fixture.Close(ctx)
		t.Fatalf("open second store: %v", err)
	}
	now := time.Date(2026, 7, 23, 14, 30, 0, 0, time.UTC)
	baseline := fixture.Store.Pool.Stat().AcquiredConns()
	var oldLease *LeaderLease
	var takeoverLease *LeaderLease
	releaseOldDispatch := make(chan struct{})
	var releaseOldDispatchOnce sync.Once
	defer func() {
		releaseOldDispatchOnce.Do(func() { close(releaseOldDispatch) })
		releaseLeaderTestLease(t, context.Background(), takeoverLease, now.Add(4*time.Second))
		if oldLease != nil {
			_ = oldLease.Release(context.Background(), now.Add(4*time.Second))
		}
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	oldLease = acquireLeaderTestLease(t, ctx, fixture.Store, "runner-takeover-old", now)
	oldDispatchEntered := make(chan struct{})
	oldFenceDone := make(chan error, 1)
	go func() {
		oldFenceDone <- fixture.Store.WithLeaderExecutionFence(ctx, func(*Store) error {
			close(oldDispatchEntered)
			select {
			case <-releaseOldDispatch:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	waitLeaderTestSignal(t, oldDispatchEntered, "old fenced dispatch")

	dropLeaderTestSession(t, ctx, oldLease)
	lockAcquired := make(chan uint32, 1)
	secondStore.leaderLockAttempt = func(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
		locked, err := tryLeaderAdvisoryLock(ctx, conn)
		if err == nil && locked {
			lockAcquired <- conn.Conn().PgConn().PID()
		}
		return locked, err
	}
	takeoverDone := make(chan leaderAcquireResult, 1)
	go func() {
		lease, acquired, err := secondStore.TryAcquireLeader(ctx, "runner-takeover-new", now.Add(time.Second))
		takeoverDone <- leaderAcquireResult{lease: lease, acquired: acquired, err: err}
	}()
	var takeoverPID uint32
	select {
	case takeoverPID = <-lockAcquired:
	case <-ctx.Done():
		t.Fatalf("wait for replacement advisory lock: %v", ctx.Err())
	}
	waitLeaderEpochAdvanceBlocked(t, ctx, fixture.Store.Pool, takeoverPID)

	select {
	case result := <-takeoverDone:
		t.Fatalf("takeover completed before old fenced transaction committed: %#v", result)
	case <-time.After(200 * time.Millisecond):
	}

	releaseOldDispatchOnce.Do(func() { close(releaseOldDispatch) })
	select {
	case err := <-oldFenceDone:
		if err != nil {
			t.Fatalf("old fenced transaction: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for old fenced transaction: %v", ctx.Err())
	}

	var takeover leaderAcquireResult
	select {
	case takeover = <-takeoverDone:
	case <-ctx.Done():
		t.Fatalf("wait for takeover acquisition: %v", ctx.Err())
	}
	if takeover.err != nil || !takeover.acquired || takeover.lease == nil {
		t.Fatalf("takeover result = %#v", takeover)
	}
	takeoverLease = takeover.lease
	if authority := takeoverLease.Authority(); authority.Epoch != 2 {
		t.Fatalf("takeover epoch = %d, want 2", authority.Epoch)
	}

	dispatched := false
	err = fixture.Store.WithLeaderExecutionFence(ctx, func(*Store) error {
		dispatched = true
		return nil
	})
	if !errors.Is(err, ErrLeaderAuthorityLost) || dispatched {
		t.Fatalf("old store after takeover = dispatched %t, err %v", dispatched, err)
	}
	_ = oldLease.Release(context.Background(), now.Add(3*time.Second))
	oldLease = nil
	waitLeaderPoolAcquiredBaseline(t, fixture.Store.Pool, baseline, "dropped old lease cleanup")
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

func TestChannelExecutionFenceRejectsOldLeaderBeforeDispatch(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	now := time.Date(2026, 7, 23, 15, 30, 0, 0, time.UTC)
	var lease *LeaderLease
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(2*time.Second))
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()

	lease = acquireLeaderTestLease(t, ctx, fixture.Store, "runner-channel-old", now)
	if _, err := fixture.Store.Pool.Exec(ctx, `
		UPDATE channelops_leader_epochs
		SET epoch = epoch + 1
		WHERE service_name = $1
	`, leaderServiceName); err != nil {
		t.Fatalf("advance leader epoch: %v", err)
	}

	dispatched := false
	err := fixture.Store.WithChannelExecutionFence(ctx, fixture.ChannelID, func(*Store) error {
		dispatched = true
		return nil
	})
	if !errors.Is(err, ErrLeaderAuthorityLost) {
		t.Fatalf("stale channel fence error = %v, want ErrLeaderAuthorityLost", err)
	}
	if dispatched {
		t.Fatal("stale channel fence invoked dispatch")
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
	queueKey := "cleanup_expired:composed-leader-fence:" + testUUID(t, "composed queue key")
	defer func() {
		releaseLeaderTestLease(t, ctx, lease, now.Add(time.Second))
		_, _ = fixture.Store.Pool.Exec(context.Background(), `
			DELETE FROM channel_ops_queue_items WHERE idempotency_key = $1
		`, queueKey)
		fixture.ResetLeaderEpoch(ctx)
		fixture.Close(ctx)
	}()
	queueItem := enqueueClaimedQueueItemForTest(t, ctx, fixture, EnqueueOptions{
		Kind:           QueueCleanupExpired,
		IdempotencyKey: queueKey,
		Payload:        map[string]any{},
	})

	lease = acquireLeaderTestLease(t, ctx, fixture.Store, "runner-composed", now)
	err := fixture.Store.WithLeaderExecutionFence(ctx, func(leaderStore *Store) error {
		if !leaderStore.hasExecutionTransaction() {
			t.Fatal("leader fence dispatch has no transaction")
		}
		leaderDB := leaderStore.executionDB

		if err := leaderStore.WithQueueExecutionFence(ctx, queueItem, func(queueStore *Store) error {
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

type leaderAcquireResult struct {
	lease    *LeaderLease
	acquired bool
	err      error
}

type leaderReleaseTracer struct {
	releases atomic.Int64
}

func (t *leaderReleaseTracer) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	_ pgx.TraceQueryStartData,
) context.Context {
	return ctx
}

func (*leaderReleaseTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *leaderReleaseTracer) TraceRelease(*pgxpool.Pool, pgxpool.TraceReleaseData) {
	t.releases.Add(1)
}

func openLeaderTestStoreWithTracer(
	t *testing.T,
	ctx context.Context,
) (*Store, *leaderReleaseTracer) {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(LoadConfig().DatabaseURL)
	if err != nil {
		t.Fatalf("parse traced leader store config: %v", err)
	}
	tracer := &leaderReleaseTracer{}
	cfg.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open traced leader store: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping traced leader store: %v", err)
	}
	return &Store{
		Pool:               pool,
		Now:                func() time.Time { return time.Now().UTC() },
		DefaultMaxAttempts: 3,
		leadership:         &leaderState{},
	}, tracer
}

func waitLeaderTestSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func waitLeaderEpochAdvanceBlocked(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	pid uint32,
) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var state string
		var waitEventType *string
		var query string
		err := pool.QueryRow(ctx, `
			SELECT state, wait_event_type, query
			FROM pg_stat_activity
			WHERE pid = $1
		`, pid).Scan(&state, &waitEventType, &query)
		if err == nil &&
			state == "active" &&
			waitEventType != nil &&
			*waitEventType == "Lock" &&
			strings.Contains(query, "INSERT INTO channelops_leader_epochs") {
			return
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("observe replacement epoch update: %v", err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("replacement epoch update did not block on old fence: %v", ctx.Err())
		}
	}
}

func waitLeaderPoolAcquiredBaseline(
	t *testing.T,
	pool *pgxpool.Pool,
	want int32,
	label string,
) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if got := pool.Stat().AcquiredConns(); got == want {
			return
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf(
				"%s left %d acquired connections, want %d",
				label,
				pool.Stat().AcquiredConns(),
				want,
			)
		}
	}
}

func dropLeaderTestSession(t *testing.T, ctx context.Context, lease *LeaderLease) {
	t.Helper()
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.conn == nil {
		t.Fatal("leader lease has no dedicated connection")
	}
	if err := lease.conn.Conn().Close(ctx); err != nil {
		t.Fatalf("drop leader session: %v", err)
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
