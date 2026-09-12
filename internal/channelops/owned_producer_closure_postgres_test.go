package channelops

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Normal upload/history rows are synthetic fixture inputs, not measured worker
// or seven-day proof. The tested producer/completion writes use real native TXs.
func ownedClosureHistory(t *testing.T, data ownedInventoryData, index int, task map[string]any, at, now time.Time, stage string) map[string]any {
	t.Helper()
	input := data.Items[index]
	seedData := ownedInventoryData{Items: []ownedInventoryInput{{Item: historyTestCopy(t, input.Item).(map[string]any), Seed: historyTestCopy(t, input.Seed).(map[string]any)}}}
	if stage == "promotion" {
		ownedTestPendingPromotion(t, &seedData, at)
	} else {
		ownedTestCompletedHistory(t, &seedData, at)
	}
	h := seedData.Tasks[0]
	pairs := []string{ownedTestID(2), ownedString(data.Inventory["channel_profile_id"]), ownedTestID(5), ownedString(data.Inventory["target_account_id"]), ownedTestID(201), ownedString(input.Item["manual_seed_id"]), ownedTestID(501), ownedString(task["id"]), "abcdefghijk", strings.ReplaceAll(ownedNewUUID(t), "-", "")[:11]}
	for id := 502; id <= 550; id++ {
		pairs = append(pairs, ownedTestID(id), ownedNewUUID(t))
	}
	// IDs also occur inside native canonical queue keys and receipt URLs.
	rebound, err := ownedDecode([]byte(strings.NewReplacer(pairs...).Replace(string(mustJSON(h)))))
	if err != nil {
		t.Fatal(err)
	}
	h = ownedMap(rebound)
	base := historyTestCopy(t, task).(map[string]any)
	for k, v := range ownedMap(h["task"]) {
		base[k] = v
	}
	h["task"] = base
	op := historyRows(h["operations"])[0]
	op["privacy"], op["title"] = "unlisted", task["title_seed"]
	digest := sha256.Sum256([]byte(ownedString(op["id"])))
	op["content_sha256"] = fmt.Sprintf("%x", digest)
	receipt := ownedMap(op["receipt_json"])
	receipt["privacy"], receipt["title"] = op["privacy"], op["title"]
	job := ownedMap(h["job"])
	job["pipeline_id"], job["pipeline_snapshot"] = ownedNewUUID(t), ownedProducerPipeline(ownedString(input.Item["asset_id"]))
	job["submitted_at"], job["started_at"] = ownedISO(at.Add(-time.Minute)), ownedISO(at.Add(-time.Minute))
	upload := historyRows(h["nodes"])[0]
	upload["node_id"], upload["node_config"] = "youtube_upload", map[string]any{"privacy": "unlisted"}
	nodes := []any{}
	for _, spec := range [][2]string{{"source", "video_source"}, {"transcode", "transcode"}, {"export", "export"}} {
		node := map[string]any{"id": ownedNewUUID(t), "job_id": job["id"], "node_id": spec[0], "node_type": spec[1], "node_config": map[string]any{}, "status": "SUCCEEDED", "completed_at": ownedISO(at), "input_artifact_ids": []any{}}
		if spec[0] == "transcode" {
			node["output_artifact_id"] = op["input_artifact_id"]
		}
		nodes = append(nodes, node)
	}
	h["nodes"] = append(nodes, upload)
	output := historyRows(h["artifacts"])[0]
	output["media_info"] = map[string]any{"youtube": receipt}
	h["artifacts"] = []any{map[string]any{"id": op["input_artifact_id"], "job_id": job["id"], "node_execution_id": ownedMap(nodes[1])["id"], "media_info": map[string]any{}}, output}
	rationale := ownedMap(base["rationale_json"])
	rationale["autoflow_job_observation"] = map[string]any{"upload_metadata": receipt}
	base["rationale_json"] = rationale
	pub := historyRows(h["publications"])[0]
	pub["title"], pub["description"] = base["title_seed"], base["prompt"]
	feedback := []any{}
	for _, m := range historyRows(h["metrics"]) {
		q := historyOne(historySelect(h["queues"], func(q map[string]any) bool { return ownedMap(q["payload_json"])["metric_schedule_id"] == m["id"] }), "fixture_metric_queue")
		due := historyTime(m["due_at"])
		m["status"], m["attempt_count"], m["completed_at"], m["last_attempt_at"] = "pending", 0, nil, nil
		q["status"], q["attempt_count"] = "queued", 0
		if !due.After(now) {
			m["status"], m["attempt_count"], m["completed_at"], m["last_attempt_at"] = "succeeded", 1, ownedISO(due), ownedISO(due)
			q["status"], q["attempt_count"] = "succeeded", 1
			feedback = append(feedback, map[string]any{"id": ownedNewUUID(t), "publication_id": pub["id"], "snapshot_stage": m["snapshot_stage"], "collected_at": ownedISO(due)})
		}
	}
	h["feedback"] = feedback
	for _, queue := range historyRows(h["queues"]) {
		if queue["idempotency_key"] == nil {
			queue["idempotency_key"] = ownedString(queue["kind"]) + ":" + ownedString(base["id"])
		}
		if queue["run_after"] == nil {
			queue["run_after"] = ownedISO(at)
		}
	}
	if stage == "publish" {
		h["publications"], h["metrics"], h["feedback"] = []any{}, []any{}, []any{}
		h["queues"] = []any{map[string]any{"id": ownedNewUUID(t), "channel_profile_id": base["channel_profile_id"], "kind": QueuePublishTask, "idempotency_key": "publish_task:" + ownedString(base["id"]), "payload_json": map[string]any{"production_task_id": base["id"]}, "status": "queued", "run_after": ownedISO(at), "attempt_count": 0}}
	}
	return h
}

// Only native model defaults missing from the small shared history fixture.
func ownedClosureRows(h map[string]any) map[string]any {
	rows := map[string]any{"production_tasks": []any{h["task"]}, "jobs": []any{h["job"]}}
	for key, table := range map[string]string{"nodes": "node_executions", "artifacts": "artifacts", "operations": "youtube_upload_operations", "publications": "publication_records", "queues": "channel_ops_queue_items", "metrics": "publication_metric_schedules", "feedback": "feedback_snapshots"} {
		rows[table] = h[key]
	}
	defaults := map[string]map[string]any{
		"production_tasks":             {"source": "manual_seed", "title_seed": "owned", "prompt": "owned", "rationale_json": map[string]any{}, "score_breakdown_json": map[string]any{}, "portfolio_bucket": "explore", "source_platforms_json": []any{}, "material_library_ids_json": []any{}, "uses_external_assets": false, "approval_mode": "agent", "agent_approval_evidence_json": map[string]any{}, "human_review_evidence_json": map[string]any{}, "priority": 0, "retry_count": 0, "channel_config_version_snapshot": 1, "channel_config_snapshot_json": map[string]any{}, "transition_history_json": []any{}},
		"jobs":                         {"submitted_by": "offline-test", "retry_count": 0, "orchestrator_owner": "python"},
		"node_executions":              {"node_label": "", "node_config": map[string]any{}, "progress": 100, "retry_count": 0, "input_artifact_ids": []any{}},
		"artifacts":                    {"kind": "INTERMEDIATE", "filename": "synthetic.mp4", "mime_type": "video/mp4", "file_size": 100, "storage_backend": "local", "storage_path": "artifacts/synthetic.mp4"},
		"publication_records":          {"title": "owned", "description": "owned", "tags_json": []any{}, "compliance_disposition": "assumed_fair_use", "quota_units_estimated": 1600, "warnings_json": []any{}},
		"channel_ops_queue_items":      {"priority": 70, "max_attempts": 3},
		"publication_metric_schedules": {"available_fields_json": []any{}},
		"feedback_snapshots":           {"views": 1, "likes": 0, "comments": 0, "shares": 0, "avg_view_duration_sec": 0, "metrics_completeness_score": 1, "available_fields_json": []any{}, "reward_components_json": map[string]any{}, "virality_score": 0, "raw_json": map[string]any{}},
	}
	for table, values := range rows {
		for _, row := range historyRows(values) {
			for key, value := range defaults[table] {
				if _, exists := row[key]; !exists {
					row[key] = value
				}
			}
		}
	}
	return rows
}

func ownedClosureInsert(t *testing.T, ctx context.Context, tx pgx.Tx, table string, row map[string]any) {
	t.Helper()
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	columns := strings.Join(keys, ",")
	if _, err := tx.Exec(ctx, "INSERT INTO "+table+" ("+columns+") SELECT "+columns+" FROM json_populate_record(NULL::"+table+",$1::json)", mustJSON(row)); err != nil {
		t.Fatalf("normal fixture insert %s: %v", table, err)
	}
}

func ownedClosureSeed(t *testing.T, f *ownedPGFixture, ctx context.Context, h map[string]any, existingTask bool) {
	t.Helper()
	rows := ownedClosureRows(h)
	tx, err := f.store.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	job := ownedMap(h["job"])
	if _, err := tx.Exec(ctx, `INSERT INTO pipelines(id,name,definition) VALUES($1::uuid,'D synthetic completed graph',$2::json)`, job["pipeline_id"], mustJSON(job["pipeline_snapshot"])); err != nil {
		t.Fatal(err)
	}
	for _, table := range strings.Fields("jobs node_executions artifacts production_tasks youtube_upload_operations publication_records publication_metric_schedules channel_ops_queue_items feedback_snapshots") {
		for _, row := range historyRows(rows[table]) {
			if table == "production_tasks" && existingTask {
				if _, err := tx.Exec(ctx, `UPDATE production_tasks SET job_id=$2::uuid,state=$3,rationale_json=$4::json WHERE id=$1::uuid`, row["id"], row["job_id"], row["state"], mustJSON(row["rationale_json"])); err != nil {
					t.Fatal(err)
				}
				continue
			}
			ownedClosureInsert(t, ctx, tx, table, row)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func ownedClosureClaim(t *testing.T, f *ownedPGFixture, ctx context.Context, kind string) QueueItemRow {
	t.Helper()
	item, err := f.store.ClaimNextForChannelAndKinds(ctx, handlerWorkerID(f.lease.Authority()), f.channel.ID, []string{kind})
	if err != nil || item == nil {
		t.Fatal("native closure claim", kind, err)
	}
	return *item
}

func ownedClosurePlanned(t *testing.T, f *ownedPGFixture, ctx context.Context) (ProductionTaskRow, QueueItemRow) {
	t.Helper()
	plan, api := ownedProducerPGClaimPlan(t, f, ctx)
	h := HandlerService{Store: f.store, AutoFlow: api, PDS: fakePDS{decision: ownedProducerRealDecision()}}
	if err := h.HandlePlanTask(ctx, plan); err != nil {
		t.Fatal("native owned plan", err)
	}
	if err := completeCommittedQueueClaim(ctx, f.store, plan); err != nil {
		t.Fatal(err)
	}
	return ownedProducerPGAssertTask(t, f, ctx, TaskPlanning, 1), ownedClosureClaim(t, f, ctx, QueueExecuteTask)
}

func TestOwnedProducerPGFirstExecuteFreshAuthority(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(fmt.Sprintf("drift_%t", drift), func(t *testing.T) {
			f := newOwnedPGFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			task, item := ownedClosurePlanned(t, f, ctx)
			runID, jobID, calls := ownedNewUUID(t), ownedNewUUID(t), 0
			h := HandlerService{Store: f.store, AutoFlow: executeHookAutoFlow{execute: func(ctx context.Context, actual ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error) {
				calls++
				if actual.ID != task.ID || request["channelops_queue_item_id"] != item.ID || request["channelops_queue_locked_by"] != *item.LockedBy || request["channelops_queue_locked_at"] != item.LockedAt.UTC().Format(time.RFC3339Nano) {
					t.Fatal("execution lost exact native claim")
				}
				if drift {
					if _, err := f.store.Pool.Exec(ctx, `UPDATE production_tasks SET prompt='changed outside locks' WHERE id=$1::uuid`, task.ID); err != nil {
						return AutoFlowExecuteObservation{}, err
					}
				}
				return AutoFlowExecuteObservation{RunID: runID, JobID: jobID, Status: "running"}, nil
			}}}
			err := h.HandleExecuteTask(ctx, item)
			if (err != nil) != drift || calls != 1 {
				t.Fatal("first execute fresh boundary", err, calls)
			}
			current, err := f.store.GetProductionTask(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			var observes int
			if err := f.store.Pool.QueryRow(ctx, `SELECT count(*) FROM channel_ops_queue_items WHERE channel_profile_id=$1::uuid AND kind='observe_job'`, f.channel.ID).Scan(&observes); err != nil {
				t.Fatal(err)
			}
			if drift {
				if current.State != TaskPlanning || current.JobID != nil || current.AutoFlowRunID != nil || observes != 0 {
					t.Fatal("stale execute committed residue")
				}
			} else if current.State != TaskProducing || current.JobID == nil || *current.JobID != jobID || current.AutoFlowRunID == nil || *current.AutoFlowRunID != runID || observes != 1 {
				t.Fatal("actual execution link/observe enqueue missing")
			}
		})
	}
}

func ownedClosurePublicationFixture(t *testing.T, stage string) (*ownedPGFixture, QueueItemRow) {
	t.Helper()
	f := newOwnedPGFixtureWithWindow(t, nil, 4*time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	task, execute := ownedClosurePlanned(t, f, ctx)
	if err := completeCommittedQueueClaim(ctx, f.store, execute); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	var now time.Time
	if err := f.store.Pool.QueryRow(ctx, `SELECT to_jsonb(t),clock_timestamp() FROM production_tasks t WHERE id=$1::uuid`, task.ID).Scan(&raw, &now); err != nil {
		t.Fatal(err)
	}
	value, err := ownedDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	history := ownedClosureHistory(t, f.data, 0, ownedMap(value), now.UTC().Add(-2*time.Hour).Truncate(time.Second), now, stage)
	ownedClosureSeed(t, f, ctx, history, true)
	kind := QueuePublishTask
	if stage == "promotion" {
		kind = QueuePromotePublication
	}
	return f, ownedClosureClaim(t, f, ctx, kind)
}

func TestOwnedProducerPGPublishFreshAuthority(t *testing.T) {
	for _, drift := range []bool{false, true} {
		t.Run(fmt.Sprintf("drift_%t", drift), func(t *testing.T) {
			f, item := ownedClosurePublicationFixture(t, "publish")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			calls := 0
			h := HandlerService{Store: f.store, PDS: ownedTestPDS(func(ctx context.Context, request PDSDecisionRequest) (PDSDecision, error) {
				calls++
				if request.Context["production_task_id"] != item.PayloadJSON["production_task_id"] || request.ActionType != "publish" || request.Context["owned_inventory"] == nil {
					t.Fatal("publish PDS lost owned binding")
				}
				if drift {
					if _, err := f.store.Pool.Exec(ctx, `UPDATE production_tasks SET prompt='changed during publish PDS' WHERE id=$1::uuid`, item.PayloadJSON["production_task_id"]); err != nil {
						return PDSDecision{}, err
					}
				}
				return ownedProducerRealDecision(), nil
			})}
			err := h.HandlePublishTask(ctx, item)
			if (err != nil) != drift || calls != 1 {
				t.Fatal("publish fresh fence", err, calls)
			}
			var publications, promotions int
			if err := f.store.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM publication_records p JOIN production_tasks t ON t.id=p.production_task_id WHERE t.channel_profile_id=$1::uuid),(SELECT count(*) FROM channel_ops_queue_items WHERE channel_profile_id=$1::uuid AND kind='promote_publication')`, f.channel.ID).Scan(&publications, &promotions); err != nil {
				t.Fatal(err)
			}
			task, err := f.store.GetProductionTask(ctx, firstString(item.PayloadJSON, "production_task_id"))
			if err != nil {
				t.Fatal(err)
			}
			if drift {
				if publications != 0 || promotions != 0 || task.State != TaskScheduled || task.AgentApprovalEvidenceJSON["publish_pds"] != nil {
					t.Fatal("stale publish committed residue")
				}
			} else if publications != 1 || promotions != 1 || task.State != TaskUploadedPrivate || task.AgentApprovalEvidenceJSON["publish_pds"] == nil {
				t.Fatal("publish transaction missing native effects")
			}
		})
	}
}

func TestOwnedProducerPGPromotionFreshAuthorityAndMissingPlan(t *testing.T) {
	for _, mode := range []string{"allow", "pds_drift", "missing_plan", "reserved_missing_plan", "store_reserve_missing_plan", "store_begin_missing_plan", "submitting_missing_plan", "uncertain_missing_plan", "confirmed_missing_plan"} {
		t.Run(mode, func(t *testing.T) {
			f, item := ownedClosurePublicationFixture(t, "promotion")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			youtube := &durablePromotionYouTube{}
			pdsCalls := 0
			h := HandlerService{Store: f.store, YouTube: youtube, PDS: ownedTestPDS(func(ctx context.Context, _ PDSDecisionRequest) (PDSDecision, error) {
				pdsCalls++
				if mode == "pds_drift" {
					if _, err := f.store.Pool.Exec(ctx, `UPDATE production_tasks SET prompt='changed during promotion PDS' WHERE id=(SELECT production_task_id FROM publication_records WHERE id=$1::uuid)`, item.PayloadJSON["publication_id"]); err != nil {
						return PDSDecision{}, err
					}
				}
				return ownedProducerRealDecision(), nil
			})}
			pub, err := f.store.GetPublication(ctx, firstString(item.PayloadJSON, "publication_id"))
			if err != nil {
				t.Fatal(err)
			}
			var preparation promotionPreparation
			resume := mode == "submitting_missing_plan" || mode == "uncertain_missing_plan" || mode == "confirmed_missing_plan"
			reserved := mode == "reserved_missing_plan" || mode == "store_begin_missing_plan"
			if reserved || resume {
				if err := h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
					var err error
					preparation, err = fenced.preparePromotion(ctx, item)
					return err
				}); err != nil {
					t.Fatal("valid prepare for reserved baseline", err)
				}
				if err := h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
					var err error
					preparation, err = fenced.finalizePromotionDecision(ctx, item, preparation, ownedProducerRealDecision())
					return err
				}); err != nil || preparation.Operation.Status != PromotionReserved {
					t.Fatal("valid reservation baseline", err)
				}
				if resume {
					if err := h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
						op, submit, err := fenced.Store.BeginPromotionSubmission(ctx, preparation.Operation.ID)
						if err != nil || !submit {
							t.Fatal("valid native submission boundary", err)
						}
						status := YouTubePublicationStatus{VideoID: op.PlatformVideoID, Privacy: "unlisted", PublishStatus: "scheduled"}
						if mode == "uncertain_missing_plan" {
							_, err = fenced.Store.MarkPromotionOperationUncertain(ctx, op.ID, status, "synthetic response loss")
						} else if mode == "confirmed_missing_plan" {
							_, err = fenced.Store.ConfirmPromotionOperation(ctx, op.ID, status, map[string]any{"synthetic_status": true})
						}
						return err
					}); err != nil {
						t.Fatal("native submitted/observed baseline", err)
					}
				}
			}
			if strings.Contains(mode, "missing_plan") {
				if mode == "store_reserve_missing_plan" {
					task, err := f.store.GetProductionTask(ctx, pub.ProductionTaskID)
					if err != nil {
						t.Fatal(err)
					}
					evidence, _ := ownedPolicyEvidence(ownedPromotionPolicyRequest(pub, task, "unlisted"), ownedProducerRealDecision())
					if _, err := f.store.Pool.Exec(ctx, `UPDATE production_tasks SET agent_approval_evidence_json=(agent_approval_evidence_json::jsonb || jsonb_build_object('promotion_pds',$2::jsonb))::json WHERE id=$1::uuid`, task.ID, mustJSON(evidence)); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := f.store.Pool.Exec(ctx, `UPDATE production_tasks SET autoflow_plan_id=NULL,agent_approval_evidence_json=(agent_approval_evidence_json::jsonb-'plan_pds')::json WHERE id=$1::uuid`, pub.ProductionTaskID); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "store_reserve_missing_plan":
				err = h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
					_, err := fenced.Store.ReservePromotionOperation(ctx, pub, item.ID, "unlisted", time.Now().UTC(), ownedProducerRealDecision())
					return err
				})
			case "store_begin_missing_plan":
				err = h.withOwnedTickQueuePhase(ctx, item, func(fenced HandlerService) error {
					_, _, err := fenced.Store.BeginPromotionSubmission(ctx, preparation.Operation.ID)
					return err
				})
			default:
				err = h.HandlePromotePublication(ctx, item)
			}
			operation, readErr := f.store.GetPromotionOperationForPublication(ctx, pub.ID)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if resume {
				wantReads := int32(1)
				if mode == "confirmed_missing_plan" {
					wantReads = 0
				}
				if err != nil || operation == nil || operation.ID != preparation.Operation.ID || operation.Status != PromotionFinalized || youtube.scheduleCalls.Load() != 0 || youtube.statusCalls.Load() != wantReads || pdsCalls != 0 {
					t.Fatal("existing settlement was blocked or produced a second submission", err, operation, youtube.scheduleCalls.Load(), youtube.statusCalls.Load(), pdsCalls)
				}
			} else if mode == "allow" {
				if err != nil || operation == nil || operation.Status != PromotionFinalized || youtube.scheduleCalls.Load() != 1 || pdsCalls != 1 {
					t.Fatal("actual owned promotion did not finalize", err, operation, youtube.scheduleCalls.Load(), pdsCalls)
				}
				if err := h.HandlePromotePublication(ctx, item); err != nil || youtube.scheduleCalls.Load() != 1 || pdsCalls != 1 {
					t.Fatal("finalized promotion replay duplicated effect", err)
				}
			} else if err == nil || youtube.scheduleCalls.Load() != 0 || reserved && (operation == nil || operation.Status != PromotionReserved || operation.RequestAttemptedAt != nil) || !reserved && operation != nil || mode != "pds_drift" && pdsCalls != 0 {
				t.Fatal("stale or missing plan admitted new reservation/submission", err, operation, youtube.scheduleCalls.Load(), pdsCalls)
			}
		})
	}
}

func TestOwnedClosureNativeRowsHaveRequiredFieldsAndReferences(t *testing.T) {
	// Derived from the existing native models' non-null columns without server
	// defaults. PG constraints remain parent-qualified, never disabled here.
	required := map[string]string{
		"production_tasks":             "id channel_profile_id target_account_id source title_seed prompt rationale_json score_breakdown_json portfolio_bucket source_platforms_json material_library_ids_json uses_external_assets approval_mode agent_approval_evidence_json human_review_evidence_json priority state retry_count channel_config_version_snapshot channel_config_snapshot_json transition_history_json",
		"jobs":                         "id pipeline_id pipeline_snapshot status submitted_by retry_count orchestrator_owner",
		"node_executions":              "id job_id node_id node_type node_label node_config status progress retry_count input_artifact_ids",
		"artifacts":                    "id job_id node_execution_id kind filename storage_backend storage_path",
		"youtube_upload_operations":    "id job_id node_execution_id input_artifact_id content_sha256 title privacy status receipt_json",
		"publication_records":          "id production_task_id platform account_id platform_content_id title description tags_json desired_privacy current_privacy publish_status compliance_disposition quota_units_estimated warnings_json",
		"publication_metric_schedules": "id publication_id snapshot_stage effective_start_at due_at grace_until status attempt_count available_fields_json",
		"channel_ops_queue_items":      "id kind idempotency_key priority payload_json status run_after attempt_count max_attempts",
		"feedback_snapshots":           "id publication_id snapshot_stage views likes comments shares avg_view_duration_sec metrics_completeness_score available_fields_json reward_components_json virality_score raw_json",
	}
	for _, stage := range []string{"publish", "promotion", "completion"} {
		t.Run(stage, func(t *testing.T) {
			data, task, now := ownedProducerFixture(t)
			h := ownedClosureHistory(t, data, 0, task, now.Add(-2*time.Hour).Truncate(time.Second), now, stage)
			rows := ownedClosureRows(h)
			for table, columns := range required {
				ids := map[string]bool{}
				keys := map[string]bool{}
				for _, row := range historyRows(rows[table]) {
					for _, key := range strings.Fields(columns) {
						if row[key] == nil {
							t.Fatalf("%s missing native %s", table, key)
						}
					}
					id := ownedString(row["id"])
					if !uuidPattern.MatchString(id) || ids[id] {
						t.Fatal("invalid/duplicate native row ID", table)
					}
					ids[id] = true
					if table == "channel_ops_queue_items" {
						key := ownedString(row["idempotency_key"])
						if keys[key] {
							t.Fatal("duplicate native queue key")
						}
						keys[key] = true
					}
				}
			}
			for _, ref := range [][3]string{{"node_executions", "job_id", "jobs"}, {"artifacts", "node_execution_id", "node_executions"}, {"youtube_upload_operations", "input_artifact_id", "artifacts"}, {"youtube_upload_operations", "node_execution_id", "node_executions"}, {"publication_records", "production_task_id", "production_tasks"}, {"publication_metric_schedules", "publication_id", "publication_records"}, {"feedback_snapshots", "publication_id", "publication_records"}, {"channel_ops_queue_items", "parent_queue_item_id", "channel_ops_queue_items"}} {
				index := historyIndex(rows[ref[2]])
				for _, row := range historyRows(rows[ref[0]]) {
					if row[ref[1]] != nil && index[ownedString(row[ref[1]])] == nil {
						t.Fatal("orphan normal fixture reference", ref)
					}
				}
			}
			item := data.Items[0].Item
			if wait, _, _ := historyNormal(historyObject(historyDecode(mustJSON(h))), item, now); wait == "" {
				t.Fatal("current seeded work lost its real normal-history wait")
			}
		})
	}
}
