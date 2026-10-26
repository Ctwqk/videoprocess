package channelops

import (
	"context"
	"testing"
	"time"
)

func TestRunnerReadinessPreservesLeaderDuringExecutionFence(t *testing.T) {
	ctx := context.Background()
	f := NewChannelOpsFixture(t)
	f.ResetLeaderEpoch(ctx)
	controller := newPostgresLeadershipController(f.Store, "runner-readiness-fence").(*postgresLeadershipController)
	defer func() {
		_ = controller.Close(ctx, f.Store.Now())
		f.ResetLeaderEpoch(ctx)
		f.Close(ctx)
	}()
	authority, err := controller.EnsureActive(ctx, f.Store.Now())
	if err != nil || authority == nil {
		t.Fatalf("acquire leader: %v, %v", authority, err)
	}
	runner := &Runner{Store: f.Store, Leadership: controller}
	err = f.Store.WithLeaderExecutionFence(ctx, func(*Store) error {
		for i := 0; i < 3; i++ {
			probeCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			status := runner.ReadyCheck(probeCtx)
			cancel()
			if status.Status != "ok" || status.LeaderEpoch == nil || *status.LeaderEpoch != authority.Epoch {
				t.Errorf("readiness inside execution fence = %#v", status)
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	status := controller.Status()
	if status.Role != LeaderRoleActive || status.Authority == nil || status.Authority.Epoch != authority.Epoch {
		t.Fatalf("probe changed leadership: %#v", status)
	}
	var heartbeat time.Time
	var epoch int64
	if err := f.Store.Pool.QueryRow(ctx, `SELECT epoch, heartbeat_at FROM channelops_leader_epochs WHERE service_name=$1`, leaderServiceName).Scan(&epoch, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if epoch != authority.Epoch || !heartbeat.Equal(authority.HeartbeatAt) {
		t.Fatalf("readiness wrote leader row: epoch=%d heartbeat=%s", epoch, heartbeat)
	}
	// The dedicated session must retain its advisory lock after the probes.
	other, acquired, err := f.Store.TryAcquireLeader(ctx, "readiness-contender", f.Store.Now())
	if other != nil {
		defer other.Release(ctx, f.Store.Now())
	}
	if err != nil || acquired {
		t.Fatalf("readiness released leader session: acquired=%t err=%v", acquired, err)
	}
}

func TestRunnerReadinessDoesNotAcquireLeadership(t *testing.T) {
	ctx := context.Background()
	f := NewChannelOpsFixture(t)
	f.ResetLeaderEpoch(ctx)
	controller := newPostgresLeadershipController(f.Store, "runner-readiness-standby")
	defer func() {
		_ = controller.Close(ctx, f.Store.Now())
		f.ResetLeaderEpoch(ctx)
		f.Close(ctx)
	}()
	status := (&Runner{Store: f.Store, Leadership: controller}).ReadyCheck(ctx)
	if status.Status != "unhealthy" || status.LeaderRole != LeaderRoleStandby {
		t.Fatalf("standby readiness = %#v", status)
	}
	var count int
	if err := f.Store.Pool.QueryRow(ctx, `SELECT count(*) FROM channelops_leader_epochs`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("readiness acquired leadership: rows=%d err=%v", count, err)
	}
}
