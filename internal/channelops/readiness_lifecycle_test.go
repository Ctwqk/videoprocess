package channelops

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRunnerReadinessRejectsChangedLeaderAuthority(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"epoch", `UPDATE channelops_leader_epochs SET epoch=epoch+1 WHERE service_name=$1`},
		{"holder", `UPDATE channelops_leader_epochs SET holder_id='replacement-holder' WHERE service_name=$1`},
		{"released", `UPDATE channelops_leader_epochs SET released_at=acquired_at WHERE service_name=$1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, f, controller, _ := readinessLifecycleFixture(t)
			if _, err := f.Store.Pool.Exec(ctx, tc.sql, leaderServiceName); err != nil {
				t.Fatal(err)
			}

			status := (&Runner{Store: f.Store, Leadership: controller}).ReadyCheck(ctx)
			if status.Status != "unhealthy" || status.LeaderRole != LeaderRoleUnavailable ||
				status.LeaderEpoch != nil || status.LeaderHolderID != "" || status.LeaderHeartbeatAt != nil {
				t.Fatalf("changed %s passed readiness or leaked stale authority: %#v", tc.name, status)
			}
			if !errors.Is(controller.Status().Err, ErrLeaderAuthorityLost) {
				t.Fatalf("changed %s did not revoke the lease: %#v", tc.name, controller.Status())
			}
			if _, authority := f.Store.leadership.snapshot(); authority != nil {
				t.Fatalf("changed %s left execution authority: %#v", tc.name, authority)
			}
		})
	}
}

func TestRunnerReadinessDeadlineDoesNotCancelInFlightHeartbeat(t *testing.T) {
	ctx, f, controller, authority := readinessLifecycleFixture(t)
	pid := controller.lease.conn.Conn().PgConn().PID()
	blocker, err := f.Store.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	var epoch int64
	if err := blocker.QueryRow(ctx, `SELECT epoch FROM channelops_leader_epochs WHERE service_name=$1 FOR SHARE`, leaderServiceName).Scan(&epoch); err != nil {
		t.Fatal(err)
	}
	heartbeatAt := authority.HeartbeatAt.Add(time.Second)
	var resumed *LeaderAuthority
	heartbeat := startCancellableTestOperation(t, func() {
		_ = blocker.Rollback(context.Background())
	}, func(operationCtx context.Context) error {
		var heartbeatErr error
		resumed, heartbeatErr = controller.EnsureActive(operationCtx, heartbeatAt)
		return heartbeatErr
	})
	waitForPostgresLockWait(t, ctx, f.Store, pid, "heartbeat behind execution fence")

	probeCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	status := (&Runner{Store: f.Store, Leadership: controller}).ReadyCheck(probeCtx)
	cancel()
	if status.Status != "unhealthy" || status.LeaderRole != LeaderRoleUnavailable || status.LeaderEpoch != nil {
		t.Fatalf("readiness waiting for heartbeat = %#v", status)
	}
	if done, err := heartbeat.tryWait(); done {
		t.Fatalf("readiness canceled the independent heartbeat: %v", err)
	}
	active := controller.Status()
	if active.Role != LeaderRoleActive || active.Authority == nil || active.Authority.Epoch != authority.Epoch {
		t.Fatalf("readiness revoked the independent heartbeat lease: %#v", active)
	}

	heartbeat.releaseBlocker()
	if result, err := heartbeat.waitOrCancelAndDrain(time.Second, time.Second); err != nil || result != nil {
		t.Fatalf("resume heartbeat: result=%v wait=%v", result, err)
	}
	if resumed == nil || resumed.Epoch != authority.Epoch || !resumed.HeartbeatAt.Equal(heartbeatAt) ||
		controller.lease.conn.Conn().PgConn().PID() != pid {
		t.Fatalf("heartbeat did not retain the same session and epoch: %#v", resumed)
	}
	var storedHeartbeat time.Time
	if err := f.Store.Pool.QueryRow(ctx, `SELECT epoch,heartbeat_at FROM channelops_leader_epochs WHERE service_name=$1`, leaderServiceName).Scan(&epoch, &storedHeartbeat); err != nil {
		t.Fatal(err)
	}
	if epoch != authority.Epoch || !storedHeartbeat.Equal(heartbeatAt) {
		t.Fatalf("resumed heartbeat was not persisted: epoch=%d heartbeat=%s", epoch, storedHeartbeat)
	}
}

func TestLeadershipCloseCancelsAndJoinsBlockedReadinessValidation(t *testing.T) {
	ctx, f, controller, authority := readinessLifecycleFixture(t)
	pid := controller.lease.conn.Conn().PgConn().PID()
	baseline := f.Store.Pool.Stat().AcquiredConns() - 1
	blocker, err := f.Store.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Rollback(context.Background()) })
	// A table lock deterministically blocks even the new nonlocking SELECT.
	if _, err := blocker.Exec(ctx, `LOCK TABLE channelops_leader_epochs IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	validation := startCancellableTestOperation(t, func() {
		_ = blocker.Rollback(context.Background())
	}, func(operationCtx context.Context) error {
		_, err := controller.ValidateActive(operationCtx)
		return err
	})
	waitForPostgresLockWait(t, ctx, f.Store, pid, "readiness validation behind table lock")

	closeCtx, cancel := context.WithTimeout(ctx, time.Second)
	err = controller.Close(closeCtx, authority.HeartbeatAt.Add(time.Second))
	cancel()
	if err != nil {
		t.Fatalf("Close did not cancel and join readiness: %v", err)
	}
	if result, err := validation.waitOrCancelAndDrain(time.Second, time.Second); err != nil || !errors.Is(result, context.Canceled) {
		t.Fatalf("blocked validation: result=%v wait=%v", result, err)
	}
	if status := controller.Status(); status.Role != LeaderRoleStandby || status.Authority != nil {
		t.Fatalf("closed controller retained authority: %#v", status)
	}
	if authority, err := controller.ValidateActive(ctx); authority != nil || !errors.Is(err, ErrLeaderAuthorityUnavailable) {
		t.Fatalf("closed controller passed readiness: authority=%#v err=%v", authority, err)
	}
	validation.releaseBlocker()
	if err := controller.Close(ctx, f.Store.Now()); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	waitLeaderPoolAcquiredBaseline(t, f.Store.Pool, baseline, "closed readiness validation")
	replacement, acquired, err := f.Store.TryAcquireLeader(ctx, "replacement-after-readiness-close", f.Store.Now().Add(2*time.Second))
	if err != nil || !acquired || replacement == nil {
		t.Fatalf("closed validation retained advisory lock: acquired=%t err=%v", acquired, err)
	}
	if err := replacement.Release(ctx, f.Store.Now().Add(3*time.Second)); err != nil {
		t.Fatalf("release replacement: %v", err)
	}
}

func readinessLifecycleFixture(t *testing.T) (context.Context, *ChannelOpsFixture, *postgresLeadershipController, *LeaderAuthority) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	f := NewChannelOpsFixture(t)
	f.ResetLeaderEpoch(ctx)
	controller := newPostgresLeadershipController(f.Store, "runner-readiness-lifecycle").(*postgresLeadershipController)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := controller.Close(cleanupCtx, f.Store.Now().Add(time.Minute)); err != nil {
			t.Errorf("close readiness fixture leader: %v", err)
		}
		f.ResetLeaderEpoch(cleanupCtx)
		f.Close(cleanupCtx)
	})
	authority, err := controller.EnsureActive(ctx, f.Store.Now())
	if err != nil || authority == nil {
		t.Fatalf("acquire readiness fixture leader: %#v, %v", authority, err)
	}
	return ctx, f, controller, authority
}
