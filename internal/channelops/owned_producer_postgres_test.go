package channelops

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Parent-only opt-in through the existing explicit disposable-DB guard. External
// services are fake; native queue/channel/leader transactions and rows are real.
type ownedProducerPGAutoFlow struct {
	fakeAutoFlow
	t                       *testing.T
	f                       *ownedPGFixture
	planID                  string
	planCalls, approveCalls int
	loseFirstApproval       bool
	loseFirstPlan           bool
}

func (a *ownedProducerPGAutoFlow) PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error) {
	a.planCalls++
	graph := ownedProducerPipeline(ownedString(a.f.data.Items[0].Item["asset_id"]))
	_, err := a.f.store.Pool.Exec(ctx, `INSERT INTO autoflow_plans(id,prompt,request_json,intent_json,template_id,pipeline_definition,candidates_json,metadata_json,rights_json,validation_json,status,execution_revision,created_at,updated_at)
		VALUES($1::uuid,$2,$3::json,'{}','input_video',$4::json,'[]','{}','{}','{"valid":true}','draft',1,now(),now()) ON CONFLICT(id) DO NOTHING`, a.planID, task.Prompt, mustJSON(request), mustJSON(graph))
	if err != nil {
		return AutoFlowPlanObservation{}, err
	}
	var stored []byte
	if err := a.f.store.Pool.QueryRow(ctx, `SELECT request_json FROM autoflow_plans WHERE id=$1::uuid`, a.planID).Scan(&stored); err != nil {
		return AutoFlowPlanObservation{}, err
	}
	actual, err := ownedDecode(stored)
	if err != nil || !ownedPolicyJSONEqual(actual, request) {
		a.t.Fatal("native planner retry changed original request", err)
	}
	_, err = a.f.store.Pool.Exec(ctx, `UPDATE production_tasks SET autoflow_plan_id=$2::uuid,rationale_json=(rationale_json::jsonb || $3::jsonb)::json WHERE id=$1::uuid AND (autoflow_plan_id IS NULL OR autoflow_plan_id=$2::uuid)`, task.ID, a.planID, mustJSON(map[string]any{"autoflow_plan_payload": map[string]any{"plan_id": a.planID}}))
	if err != nil {
		return AutoFlowPlanObservation{}, err
	}
	if a.loseFirstPlan && a.planCalls == 1 {
		return AutoFlowPlanObservation{}, errors.New("synthetic native plan response lost after binding")
	}
	return AutoFlowPlanObservation{PlanID: a.planID, UploadNodeCount: 1, PlanPayload: map[string]any{"plan_id": a.planID, "pipeline_definition": graph}}, err
}

func (a *ownedProducerPGAutoFlow) ApprovePlan(ctx context.Context, planID string, _ map[string]any) (AutoFlowApprovalObservation, error) {
	a.approveCalls++
	var taskID string
	if err := a.f.store.Pool.QueryRow(ctx, `SELECT id FROM production_tasks WHERE channel_profile_id=$1::uuid`, a.f.channel.ID).Scan(&taskID); err != nil {
		return AutoFlowApprovalObservation{}, err
	}
	task, err := a.f.store.GetProductionTask(ctx, taskID)
	if err != nil {
		return AutoFlowApprovalObservation{}, err
	}
	// The original API sees durable evidence, never review_notes as authority.
	if _, _, _, found, err := ownedPendingPlan(task); err != nil || !found {
		a.t.Fatal("approval reached without committed exact plan evidence", err)
	}
	if task.State != TaskSelected || planID != a.planID {
		a.t.Fatal("approval changed task or plan identity")
	}
	if _, err := a.f.store.Pool.Exec(ctx, `UPDATE autoflow_plans SET status='approved',agent_approved_by='native-go-test',review_approved_at=COALESCE(review_approved_at,now()),approved_revision_hash=$2,approved_revision=execution_revision,updated_at=now() WHERE id=$1::uuid`, planID, strings.Repeat("a", 64)); err != nil {
		return AutoFlowApprovalObservation{}, err
	}
	if a.loseFirstApproval && a.approveCalls == 1 {
		return AutoFlowApprovalObservation{}, errors.New("synthetic approval response lost after durable effect")
	}
	return AutoFlowApprovalObservation{PlanID: planID, ApprovedRevisionHash: strings.Repeat("a", 64), ApprovedRevision: 1}, nil
}

func ownedProducerPGClaimPlan(t *testing.T, f *ownedPGFixture, ctx context.Context) (QueueItemRow, *ownedProducerPGAutoFlow) {
	t.Helper()
	if err := f.store.RunTick(ctx, f.channel.ID, "owned-producer", HandlerService{PDS: fakePDS{decision: ownedProducerRealDecision()}}); err != nil {
		t.Fatal("native admission", err)
	}
	item, err := f.store.ClaimNextForChannelAndKinds(ctx, handlerWorkerID(f.lease.Authority()), f.channel.ID, []string{QueuePlanTask})
	if err != nil || item == nil {
		t.Fatal("native plan claim", err)
	}
	return *item, &ownedProducerPGAutoFlow{t: t, f: f, planID: ownedNewUUID(t)}
}

func ownedProducerPGAssertTask(t *testing.T, f *ownedPGFixture, ctx context.Context, state string, execute int) ProductionTaskRow {
	t.Helper()
	var id string
	if err := f.store.Pool.QueryRow(ctx, `SELECT id FROM production_tasks WHERE channel_profile_id=$1::uuid`, f.channel.ID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	task, err := f.store.GetProductionTask(ctx, id)
	if err != nil || task.State != state {
		t.Fatal("task state", task.State, err)
	}
	var count int
	if err := f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM channel_ops_queue_items WHERE channel_profile_id=$1::uuid AND kind='execute_task'`, f.channel.ID).Scan(&count); err != nil || count != execute {
		t.Fatal("execute residue", count, err)
	}
	f.assertCounts(t, 1)
	return task
}

// Exercise the runner's real error writer and native due-time reclaim. Never
// rewrite run_after, last_error, attempt_count or the lease to simulate retry.
func ownedProducerPGReclaimAfterFailure(t *testing.T, f *ownedPGFixture, ctx context.Context, item QueueItemRow, failure error) QueueItemRow {
	t.Helper()
	if failure == nil {
		t.Fatal("native retry requires the actual handler error")
	}
	if err := completeUncommittedQueueClaim(ctx, f.store, item, failure, false); err != nil {
		t.Fatal("native failure cleanup", err)
	}
	var status, message string
	var owner *string
	var locked *time.Time
	var due, now time.Time
	var attempts int
	if err := f.store.Pool.QueryRow(ctx, `SELECT status,last_error,locked_by,locked_at,attempt_count,run_after,clock_timestamp() FROM channel_ops_queue_items WHERE id=$1::uuid`, item.ID).Scan(&status, &message, &owner, &locked, &attempts, &due, &now); err != nil {
		t.Fatal(err)
	}
	if status != QueueStatusQueued || message != failure.Error() || owner != nil || locked != nil || attempts != item.AttemptCount || due.Sub(now) < RetryDelay(attempts)-time.Second {
		t.Fatal("native retry row was not retained", status, attempts)
	}
	for now.Before(due) {
		if err := f.lease.Heartbeat(ctx, now.UTC()); err != nil {
			t.Fatal("native leader heartbeat during retry wait", err)
		}
		delay := min(time.Second, due.Sub(now))
		select {
		case <-ctx.Done():
			t.Fatal("natural native retry wait", ctx.Err())
		case <-time.After(delay):
		}
		if err := f.store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			t.Fatal(err)
		}
	}
	retry, err := f.store.ClaimNextForChannelAndKinds(ctx, handlerWorkerID(f.lease.Authority()), f.channel.ID, []string{item.Kind})
	if err != nil || retry == nil {
		t.Fatal("native due-time reclaim", err)
	}
	if retry.ID != item.ID || retry.AttemptCount != item.AttemptCount+1 || retry.LastError == nil || *retry.LastError != failure.Error() || retry.LockedAt == nil || retry.LockedBy == nil || !retry.LockedAt.After(*item.LockedAt) || !ownedPolicyJSONEqual(retry.PayloadJSON, item.PayloadJSON) || retry.IdempotencyKey != item.IdempotencyKey {
		t.Fatal("native reclaim changed identity or lost error/attempt history")
	}
	entered := false
	if err := f.store.WithQueueExecutionFence(ctx, item, func(*Store) error { entered = true; return nil }); !errors.Is(err, ErrQueueLeaseLost) || entered {
		t.Fatal("original lease still authorized after native reclaim", err)
	}
	return *retry
}

func ownedProducerPGAssertRecoveredHistoryHeld(t *testing.T, f *ownedPGFixture, ctx context.Context, item QueueItemRow) {
	t.Helper()
	before := ownedProducerPGAssertTask(t, f, ctx, TaskPlanning, 1)
	if item.AttemptCount != 2 {
		t.Fatal("strict completion proof requires the actual second native attempt")
	}
	// Exercise Task5's existing accounting/repair entry under the exact current
	// claim. Normal success clears last_error; the failed-attempt count survives.
	if err := f.store.WithQueueExecutionFence(ctx, item, func(fenced *Store) error {
		if err := fenced.MarkQueueDone(ctx, item); err != nil {
			return err
		}
		return fenced.finalizeOwnedInventoryItems(ctx, f.channel.ID, "")
	}); err != nil {
		t.Fatal("strict Task5 accounting boundary", err)
	}
	var state, reason, queueState string
	var attempts, completed int
	var lastError, owner *string
	var locked *time.Time
	if err := f.store.Pool.QueryRow(ctx, `SELECT i.state,i.hold_reason,q.status,q.attempt_count,q.last_error,q.locked_by,q.locked_at,(SELECT count(*) FROM owned_seed_inventory_items WHERE inventory_id=i.id AND state='completed') FROM owned_seed_inventories i CROSS JOIN channel_ops_queue_items q WHERE i.id=$1::uuid AND q.id=$2::uuid`, *f.channel.OwnedSeedInventoryID, item.ID).Scan(&state, &reason, &queueState, &attempts, &lastError, &owner, &locked, &completed); err != nil {
		t.Fatal(err)
	}
	if state != "held" || reason != "owned_inventory_queue_failed" || queueState != QueueStatusSucceeded || attempts != 2 || lastError != nil || owner != nil || locked != nil || completed != 0 {
		t.Fatal("retry was erased or strict completion/replacement guard weakened", state, reason, attempts, completed)
	}
	snapshot, err := loadOwnedHistorySnapshot(ctx, f.store.Pool, ownedString(f.data.Inventory["platform_channel_id"]))
	if err != nil {
		t.Fatal(err)
	}
	assessment := assessOwnedHistorySnapshot(snapshot, snapshot.observedAt)
	if assessment.BlockReason == nil || *assessment.BlockReason != "owned_inventory_queue_failed" {
		t.Fatal("A1 accepted recovered prior history", assessment)
	}
	noPDS := ownedTestPDS(func(context.Context, PDSDecisionRequest) (PDSDecision, error) {
		t.Fatal("held recovered item attempted replacement policy")
		return PDSDecision{}, nil
	})
	if err := f.store.RunTick(ctx, f.channel.ID, "after-native-recovery-held", HandlerService{PDS: noPDS}); !errors.Is(err, ErrChannelExecutionBlocked) {
		t.Fatal("held native tick did not refuse replacement", err)
	}
	if after := ownedProducerPGAssertTask(t, f, ctx, TaskPlanning, 1); !bytes.Equal(mustJSON(before), mustJSON(after)) {
		t.Fatal("strict history hold replaced or changed original recovered task/plan/PDS")
	}
}

func TestOwnedProducerPGPlanFreshFenceAndRealPolicy(t *testing.T) {
	for _, mode := range []string{"allow", "advisory", "deny", "degraded", "unavailable", "task_drift", "queue_loss", "leader_loss"} {
		t.Run(mode, func(t *testing.T) {
			f := newOwnedPGFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			item, api := ownedProducerPGClaimPlan(t, f, ctx)
			calls := 0
			h := HandlerService{Store: f.store, AutoFlow: api, PDS: ownedTestPDS(func(ctx context.Context, request PDSDecisionRequest) (PDSDecision, error) {
				calls++
				if request.ActionType != "plan_approval" || request.Context["autoflow_plan_id"] != api.planID {
					t.Fatal("wrong actual PDS request")
				}
				d := ownedProducerRealDecision()
				switch mode {
				case "advisory":
					d.Metadata["warning"] = "quota_advisory"
				case "deny":
					d.Verdict = "block"
				case "degraded":
					d.Metadata["warning"] = "pds_unavailable"
				case "unavailable":
					return PDSDecision{}, errors.New("synthetic unavailable")
				case "task_drift":
					_, err := f.store.Pool.Exec(ctx, `UPDATE production_tasks SET prompt='changed while PDS outside locks' WHERE id=$1::uuid`, item.PayloadJSON["production_task_id"])
					if err != nil {
						return PDSDecision{}, err
					}
				case "queue_loss":
					_, err := f.store.Pool.Exec(ctx, `UPDATE channel_ops_queue_items SET locked_by='replacement' WHERE id=$1::uuid`, item.ID)
					if err != nil {
						return PDSDecision{}, err
					}
				case "leader_loss":
					if err := ownedReleaseLeaderAtDBTime(ctx, f.store.Pool.QueryRow(ctx, `SELECT clock_timestamp()`), f.lease.Release); err != nil {
						return PDSDecision{}, err
					}
				}
				return d, nil
			})}
			err := h.HandlePlanTask(ctx, item)
			if calls != 1 || api.planCalls != 1 {
				t.Fatal("unexpected external retry", calls, api.planCalls)
			}
			switch mode {
			case "allow", "advisory":
				if err != nil || api.approveCalls != 1 {
					t.Fatal("real allow", err, api.approveCalls)
				}
				task := ownedProducerPGAssertTask(t, f, ctx, TaskPlanning, 1)
				if _, _, _, found, err := ownedPendingPlan(task); err != nil || !found {
					t.Fatal("durable plan lost", err)
				}
			case "deny", "degraded", "unavailable":
				if err != nil || api.approveCalls != 0 {
					t.Fatal("policy denial effects", err, api.approveCalls)
				}
				task := ownedProducerPGAssertTask(t, f, ctx, TaskHeld, 0)
				if len(ownedMap(task.AgentApprovalEvidenceJSON["plan_pds"])) == 0 || len(ownedMap(task.AgentApprovalEvidenceJSON["candidate_pds_request"])) == 0 {
					t.Fatal("denial lost audit")
				}
				var state, reason string
				if err := f.store.Pool.QueryRow(ctx, `SELECT state,hold_reason FROM owned_seed_inventories WHERE id=$1::uuid`, *f.channel.OwnedSeedInventoryID).Scan(&state, &reason); err != nil || state != "held" || reason != "owned_inventory_pds_denied" {
					t.Fatal("missing durable inventory hold", state, reason, err)
				}
			default:
				if err == nil || api.approveCalls != 0 {
					t.Fatal("authority drift admitted", err)
				}
				if mode == "queue_loss" && !errors.Is(err, ErrQueueLeaseLost) {
					t.Fatal("wrong queue fence", err)
				}
				if mode == "leader_loss" && !errors.Is(err, ErrLeaderAuthorityUnavailable) {
					t.Fatal("wrong released leader fence", err)
				}
				task := ownedProducerPGAssertTask(t, f, ctx, TaskSelected, 0)
				if task.AutoFlowPlanID == nil || *task.AutoFlowPlanID != api.planID || task.AgentApprovalEvidenceJSON["plan_pds"] != nil {
					t.Fatal("drift changed original native plan binding or committed PDS authority")
				}
			}
		})
	}
}

func TestOwnedProducerPGApprovalResultLossReusesDurablePlan(t *testing.T) {
	f := newOwnedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), RetryDelay(1)+time.Minute)
	defer cancel()
	item, api := ownedProducerPGClaimPlan(t, f, ctx)
	api.loseFirstApproval = true
	calls := 0
	h := HandlerService{Store: f.store, AutoFlow: api, PDS: ownedTestPDS(func(context.Context, PDSDecisionRequest) (PDSDecision, error) {
		calls++
		return ownedProducerRealDecision(), nil
	})}
	failure := h.HandlePlanTask(ctx, item)
	if failure == nil {
		t.Fatal("lost approval response not surfaced")
	}
	task := ownedProducerPGAssertTask(t, f, ctx, TaskSelected, 0)
	if task.AutoFlowPlanID == nil || *task.AutoFlowPlanID != api.planID {
		t.Fatal("pending original plan not durable")
	}
	before := mustJSON(task)
	item = ownedProducerPGReclaimAfterFailure(t, f, ctx, item, failure)
	if after := ownedProducerPGAssertTask(t, f, ctx, TaskSelected, 0); !bytes.Equal(before, mustJSON(after)) {
		t.Fatal("queue retry changed durable original plan/PDS/task")
	}
	if err := h.HandlePlanTask(ctx, item); err != nil {
		t.Fatal("idempotent original approval retry", err)
	}
	ownedProducerPGAssertTask(t, f, ctx, TaskPlanning, 1)
	if err := h.HandlePlanTask(ctx, item); err != nil {
		t.Fatal("committed task retry", err)
	}
	if calls != 1 || api.planCalls != 1 || api.approveCalls != 2 {
		t.Fatal("retry replanned or reevaluated PDS", calls, api.planCalls, api.approveCalls)
	}
	ownedProducerPGAssertRecoveredHistoryHeld(t, f, ctx, item)
}

func TestOwnedProducerPGNativePlanResponseLossReusesOriginalBinding(t *testing.T) {
	f := newOwnedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), RetryDelay(1)+time.Minute)
	defer cancel()
	item, api := ownedProducerPGClaimPlan(t, f, ctx)
	api.loseFirstPlan = true
	calls := 0
	h := HandlerService{Store: f.store, AutoFlow: api, PDS: ownedTestPDS(func(context.Context, PDSDecisionRequest) (PDSDecision, error) {
		calls++
		return ownedProducerRealDecision(), nil
	})}
	failure := h.HandlePlanTask(ctx, item)
	if failure == nil {
		t.Fatal("native plan response loss hidden")
	}
	task := ownedProducerPGAssertTask(t, f, ctx, TaskSelected, 0)
	if task.AutoFlowPlanID == nil || *task.AutoFlowPlanID != api.planID || calls != 0 || api.approveCalls != 0 {
		t.Fatal("lost native plan had later effects")
	}
	item = ownedProducerPGReclaimAfterFailure(t, f, ctx, item, failure)
	if err := h.HandlePlanTask(ctx, item); err != nil {
		t.Fatal("native plan retry", err)
	}
	ownedProducerPGAssertTask(t, f, ctx, TaskPlanning, 1)
	if calls != 1 || api.planCalls != 2 || api.approveCalls != 1 {
		t.Fatal("native plan retry call counts")
	}
	var count int
	if err := f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM autoflow_plans WHERE id=$1::uuid`, api.planID).Scan(&count); err != nil || count != 1 {
		t.Fatal("original native plan not reused", err)
	}
	ownedProducerPGAssertRecoveredHistoryHeld(t, f, ctx, item)
}

type ownedProducerRollbackTx struct {
	pgx.Tx
	stop    error
	blocked *atomic.Int32
}

func (p ownedProducerRollbackTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if strings.Contains(query, "INSERT INTO channel_ops_queue_items") && len(args) > 0 && args[0] == QueueExecuteTask {
		p.blocked.Add(1)
		return ownedB2FenceRow(func(...any) error { return p.stop })
	}
	return p.Tx.QueryRow(ctx, query, args...)
}

func TestOwnedProducerRollbackInjectionUsesActualExecuteEnqueue(t *testing.T) {
	stop := errors.New("synthetic execute queue failure")
	delegated := errors.New("underlying transaction")
	for _, kind := range []string{QueueExecuteTask, QueuePlanTask} {
		probe := &ownedB2FenceProbe{stop: delegated}
		var blocked atomic.Int32
		tx := ownedProducerRollbackTx{Tx: probe, stop: stop, blocked: &blocked}
		s := &Store{Now: func() time.Time { return time.Unix(1, 0) }}
		_, err := s.enqueue(context.Background(), tx, EnqueueOptions{Kind: kind, IdempotencyKey: "offline:" + kind})
		if kind == QueueExecuteTask {
			if !errors.Is(err, stop) || blocked.Load() != 1 || len(probe.queries) != 0 {
				t.Fatal("wrong rollback boundary", err)
			}
		} else if !errors.Is(err, delegated) || blocked.Load() != 0 {
			t.Fatal("unrelated enqueue intercepted", err)
		}
	}
}

func TestOwnedProducerPGPlanningRollbackKeepsOriginalPendingPlan(t *testing.T) {
	f := newOwnedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), RetryDelay(1)+time.Minute)
	defer cancel()
	item, api := ownedProducerPGClaimPlan(t, f, ctx)
	api.loseFirstApproval = true
	h := HandlerService{Store: f.store, AutoFlow: api, PDS: fakePDS{decision: ownedProducerRealDecision()}}
	if err := h.HandlePlanTask(ctx, item); err == nil {
		t.Fatal("missing intentional approval response loss")
	}
	task := ownedProducerPGAssertTask(t, f, ctx, TaskSelected, 0)
	plan, _, _, found, err := ownedPendingPlan(task)
	if err != nil || !found {
		t.Fatal(err)
	}
	before := mustJSON(task)
	stop := errors.New("synthetic execute queue failure")
	var blocked atomic.Int32
	err = h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
		fenced.Store.executionDB = ownedProducerRollbackTx{Tx: fenced.Store.executionDB.(pgx.Tx), stop: stop, blocked: &blocked}
		return fenced.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, api.planID, plan.PlanPayload, AutoFlowApprovalObservation{PlanID: api.planID, ApprovedRevisionHash: strings.Repeat("a", 64), ApprovedRevision: 1}, item.ID)
	})
	if !errors.Is(err, stop) || blocked.Load() != 1 {
		t.Fatal("actual rollback boundary not reached", err, blocked.Load())
	}
	after := ownedProducerPGAssertTask(t, f, ctx, TaskSelected, 0)
	if !bytes.Equal(mustJSON(after), before) {
		t.Fatal("rollback changed original pending task/evidence/history")
	}
	item = ownedProducerPGReclaimAfterFailure(t, f, ctx, item, err)
	if err := h.HandlePlanTask(ctx, item); err != nil {
		t.Fatal("normal retry after rollback", err)
	}
	ownedProducerPGAssertTask(t, f, ctx, TaskPlanning, 1)
	ownedProducerPGAssertRecoveredHistoryHeld(t, f, ctx, item)
}
