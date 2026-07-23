package channelops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestLiveSmokeRejectsActiveManagedOwnerBeforeMutation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	secondStore, err := OpenStore(ctx, LoadConfig().DatabaseURL)
	if err != nil {
		t.Fatalf("open smoke standby store: %v", err)
	}
	secondStore.Now = fixture.Store.Now
	now := fixture.Store.Now()
	active := acquireLeaderTestLease(
		t,
		ctx,
		fixture.Store,
		"channelops-go@colima-127:1",
		now,
	)
	defer func() {
		releaseLeaderTestLease(t, context.Background(), active, now.Add(time.Second))
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	before := smokeMutationCountsForTest(t, ctx, fixture.Store, fixture.ChannelID)
	recorder := &externalCallRecorder{}
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	handler.Store = secondStore
	handler.PDS = &recordingPDS{recorder: recorder}
	handler.AutoFlow = &recordingAutoFlow{recorder: recorder}
	handler.YouTube = &recordingYouTube{recorder: recorder}

	_, err = (LiveSmoke{
		Store:    secondStore,
		Handler:  handler,
		HolderID: "channelops-go@maintenance-smoke:1",
	}).Run(ctx, fixture.ChannelID)

	if !errors.Is(err, ErrLiveSmokeLeaderActive) {
		t.Fatalf("active-owner smoke error = %v, want ErrLiveSmokeLeaderActive", err)
	}
	after := smokeMutationCountsForTest(t, ctx, fixture.Store, fixture.ChannelID)
	if after != before {
		t.Fatalf("active-owner smoke mutations = before %#v after %#v", before, after)
	}
	if calls := recorder.total(); calls != 0 {
		t.Fatalf("active-owner smoke made %d external calls", calls)
	}
}

func TestLiveSmokeAcquiresMaintenanceEpochAndUsesFencedWorkerIdentity(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture := NewChannelOpsFixture(t)
	fixture.ResetLeaderEpoch(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	secondStore, err := OpenStore(ctx, LoadConfig().DatabaseURL)
	if err != nil {
		t.Fatalf("open maintenance smoke store: %v", err)
	}
	secondStore.Now = fixture.Store.Now
	now := fixture.Store.Now()
	managed := acquireLeaderTestLease(
		t,
		ctx,
		fixture.Store,
		"channelops-go@colima-127:1",
		now,
	)
	if err := managed.Release(ctx, now.Add(time.Second)); err != nil {
		t.Fatalf("release managed owner for maintenance: %v", err)
	}
	defer func() {
		fixture.ResetLeaderEpoch(context.Background())
		secondStore.Close()
		fixture.Close(context.Background())
	}()

	holderID := "channelops-go@maintenance-smoke:1"
	executeWorkerID := ""
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	handler.Store = secondStore
	handler.AutoFlow = executeHookAutoFlow{
		fakeAutoFlow: fakeAutoFlow{},
		execute: func(
			ctx context.Context,
			task ProductionTaskRow,
			request map[string]any,
		) (AutoFlowExecuteObservation, error) {
			executeWorkerID = firstString(request, "channelops_queue_locked_by")
			return fakeAutoFlow{}.ExecuteTask(ctx, task, request)
		},
	}

	result, err := (LiveSmoke{
		Store:    secondStore,
		Handler:  handler,
		HolderID: holderID,
	}).Run(ctx, fixture.ChannelID)
	if err != nil {
		t.Fatalf("maintenance LiveSmoke: %v", err)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("maintenance smoke result: %v; result=%#v", err, result)
	}

	var storedHolder string
	var epoch int64
	var releasedAt *time.Time
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT holder_id, epoch, released_at
		FROM channelops_leader_epochs
		WHERE service_name = $1
	`, leaderServiceName).Scan(&storedHolder, &epoch, &releasedAt); err != nil {
		t.Fatalf("read maintenance smoke epoch: %v", err)
	}
	expectedWorkerID := fmt.Sprintf("%s:epoch:%d", holderID, epoch)
	if storedHolder != holderID || epoch <= 0 || releasedAt == nil {
		t.Fatalf(
			"maintenance smoke lease = holder %q epoch %d released %v",
			storedHolder,
			epoch,
			releasedAt,
		)
	}
	if executeWorkerID != expectedWorkerID ||
		!strings.HasPrefix(executeWorkerID, holderID+":epoch:") {
		t.Fatalf(
			"maintenance smoke worker = %q, want %q",
			executeWorkerID,
			expectedWorkerID,
		)
	}
}

type smokeMutationCounts struct {
	tasks  int
	audits int
	queue  int
}

func smokeMutationCountsForTest(
	t *testing.T,
	ctx context.Context,
	store *Store,
	channelID string,
) smokeMutationCounts {
	t.Helper()
	var counts smokeMutationCounts
	if err := store.Pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM production_tasks WHERE channel_profile_id = $1::uuid),
			(SELECT COUNT(*) FROM agent_tick_audits WHERE channel_profile_id = $1::uuid),
			(SELECT COUNT(*) FROM channel_ops_queue_items
			 WHERE channel_profile_id = $1::uuid
			    OR payload_json ->> 'channel_id' = $1::text)
	`, channelID).Scan(&counts.tasks, &counts.audits, &counts.queue); err != nil {
		t.Fatalf("read smoke mutation counts: %v", err)
	}
	return counts
}
