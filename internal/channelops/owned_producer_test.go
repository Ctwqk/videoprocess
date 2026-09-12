package channelops

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestOwnedProducerCandidateBoundaryRequiresRealDecision(t *testing.T) {
	for _, real := range []bool{false, true} {
		channel, data, now := ownedTestFixture(t)
		candidate := ownedTestAssess(t, channel, data, now).Candidate
		decision := PDSDecision{Verdict: "allow", DecisionID: "incomplete"}
		if real {
			decision = ownedProducerRealDecision()
		}
		candidates, _, err := evaluateTickCandidatePolicy(context.Background(), channel, []TickCandidate{*candidate}, HandlerService{PDS: fakePDS{decision: decision}})
		if err != nil || len(candidates) != 1 || candidates[0].Rejected == real {
			t.Fatalf("real=%t candidate=%+v error=%v", real, candidates, err)
		}
	}
}

func ownedProducerRealDecision() PDSDecision {
	return PDSDecision{DecisionID: "native-decision", Verdict: "allow", RulesVersion: "native-rules-v1", EvaluatedRules: []string{"owned_source", "unlisted"}, Metadata: map[string]any{}}
}

func ownedProducerApproveCandidateFixture(t *testing.T, channel ChannelProfileRow, candidate *TickCandidate) {
	decision := ownedProducerRealDecision()
	evidence, err := ownedPolicyEvidence(ownedCandidatePolicyRequest(channel, *candidate), decision)
	if err != nil {
		t.Fatal(err)
	}
	candidate.PDSDecisionJSON, candidate.PDSRequestJSON = pdsDecisionAuditJSON(decision), ownedMap(evidence["request"])
}

func ownedProducerFixture(t *testing.T) (ownedInventoryData, map[string]any, time.Time) {
	t.Helper()
	channel, data, now := ownedTestFixture(t)
	state := ownedTestAssess(t, channel, data, now)
	if state.Candidate == nil {
		t.Fatalf("candidate: %+v", state)
	}
	evidence, err := ownedDecode([]byte(mustJSON(ownedTaskEvidence(*state.Candidate))))
	if err != nil {
		t.Fatal(err)
	}
	id := ownedTestID(600)
	item := data.Items[0].Item
	item["state"], item["production_task_id"], item["consumed_at"] = "reserved", id, ownedISO(now)
	data.Items[0].Seed["status"] = "exhausted"
	c := ownedMap(data.Bindings["channel"])
	c["owned_seed_inventory_id"], c["halted_at"], c["intake_paused_at"], c["intake_pause_reason"] = *channel.OwnedSeedInventoryID, nil, nil, nil
	task := map[string]any{"id": id, "channel_profile_id": channel.ID, "target_account_id": data.Inventory["target_account_id"], "topic_lane_id": data.Inventory["topic_lane_id"], "lane_format_id": data.Inventory["lane_format_id"], "manual_seed_id": item["manual_seed_id"], "source": "manual_seed", "approval_mode": "agent", "uses_external_assets": false, "source_platforms_json": []any{}, "material_library_ids_json": []any{}, "retry_count": json.Number("0"), "failure_reason": nil, "blocked_by_guard": nil, "state": "selected", "job_id": nil, "agent_approval_evidence_json": map[string]any{"owned_inventory": evidence}, "channel_config_snapshot_json": map[string]any{"owned_inventory": evidence}}
	task["prompt"], task["title_seed"] = data.Items[0].Seed["prompt"], data.Items[0].Seed["title_seed"]
	ownedMap(task["channel_config_snapshot_json"])["manual_seed"] = map[string]any{"constraints_json": data.Items[0].Seed["constraints_json"]}
	policy, err := ownedPolicyEvidence(ownedCandidatePolicyRequest(channel, *state.Candidate), ownedProducerRealDecision())
	if err != nil {
		t.Fatal(err)
	}
	ownedMap(task["agent_approval_evidence_json"])["candidate_pds"] = policy["response"]
	ownedMap(task["agent_approval_evidence_json"])["candidate_pds_request"] = policy["request"]
	task["rationale_json"] = map[string]any{"candidate_id": state.Candidate.CandidateID}
	data.Tasks = []map[string]any{{"task": task}}
	return data, task, now
}

func TestOwnedProducerRevalidatesAllImmutableInputs(t *testing.T) {
	for _, variant := range []string{"window_shift", "task_prompt", "task_title", "task_constraints", "unused_asset", "unused_seed", "candidate_actor", "candidate_rules", "candidate_request_missing"} {
		t.Run(variant, func(t *testing.T) {
			data, task, now := ownedProducerFixture(t)
			switch variant {
			case "window_shift":
				data.Inventory["starts_at"] = ownedISO(historyTime(data.Inventory["starts_at"]).Add(-time.Minute))
				data.Inventory["expires_at"] = ownedISO(historyTime(data.Inventory["expires_at"]).Add(-time.Minute))
			case "task_prompt":
				task["prompt"] = "substituted prompt"
			case "task_title":
				task["title_seed"] = "substituted title"
			case "task_constraints":
				ownedMap(task["channel_config_snapshot_json"])["manual_seed"] = map[string]any{"constraints_json": map[string]any{"input_asset_id": ownedTestID(999)}}
			case "unused_asset":
				data.Items[6].Asset["storage_path"] = "substituted.mp4"
			case "unused_seed":
				data.Items[6].Seed["prompt"] = "substituted prompt"
			case "candidate_actor":
				ownedMap(ownedMap(task["agent_approval_evidence_json"])["candidate_pds_request"])["actor_id"] = ownedTestID(999)
			case "candidate_rules":
				ownedMap(ownedMap(task["agent_approval_evidence_json"])["candidate_pds"])["evaluated_rules"] = []any{}
			case "candidate_request_missing":
				delete(ownedMap(task["agent_approval_evidence_json"]), "candidate_pds_request")
			}
			if _, _, err := assessOwnedProducerSnapshot(ownedProducerSnapshot(t, data, now), now, ownedString(task["id"]), ""); err == nil {
				t.Fatal("changed immutable producer input admitted")
			}
		})
	}
}

func ownedProducerSnapshot(t *testing.T, data ownedInventoryData, now time.Time) ownedHistorySnapshot {
	t.Helper()
	snapshot, err := newOwnedHistorySnapshot(historyJSON(t, ownedTestHistoryRows(t, data)), ownedString(data.Inventory["platform_channel_id"]), now, []byte("[]"))
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestOwnedProducerTypedAuthorityRejectsSnapshotAndScopeBypass(t *testing.T) {
	for _, variant := range []string{"valid", "exhausted", "expired", "revoked", "held", "wrong_item", "manual_forgery", "foreign_channel", "private", "paused", "wrong_asset", "seed_drift", "snapshot_drift", "ordinary_without_item"} {
		t.Run(variant, func(t *testing.T) {
			data, task, now := ownedProducerFixture(t)
			c := ownedMap(data.Bindings["channel"])
			switch variant {
			case "exhausted":
				data.Inventory["state"], c["intake_paused_at"], c["intake_pause_reason"] = "exhausted", ownedISO(now), "owned_inventory_exhausted"
			case "expired":
				now = now.Add(7 * 24 * time.Hour)
			case "revoked":
				data.Inventory["revoked_at"] = ownedISO(now)
			case "held":
				data.Inventory["state"], data.Inventory["hold_reason"] = "held", "policy"
			case "wrong_item":
				data.Items[0].Item["production_task_id"] = ownedTestID(999)
			case "manual_forgery":
				task["approval_mode"] = "human"
			case "foreign_channel":
				task["channel_profile_id"] = ownedTestID(999)
			case "private":
				ownedMap(data.Bindings["account"])["default_privacy"] = "private"
			case "paused":
				c["intake_paused_at"], c["intake_pause_reason"] = ownedISO(now), "operator"
			case "wrong_asset":
				data.Items[0].Asset["storage_path"] = "assets/substitute.mp4"
			case "seed_drift":
				data.Items[0].Seed["prompt"] = "substitute"
			case "snapshot_drift":
				task["channel_config_snapshot_json"] = map[string]any{"owned_inventory": map[string]any{"item_id": "forged"}}
			case "ordinary_without_item":
				data.Items[0].Item["production_task_id"], data.Items[0].Item["state"] = nil, "unused"
				task["agent_approval_evidence_json"], task["channel_config_snapshot_json"] = map[string]any{}, map[string]any{}
			}
			identity, _, err := assessOwnedProducerSnapshot(ownedProducerSnapshot(t, data, now), now, ownedString(task["id"]), "")
			valid := variant == "valid" || variant == "exhausted"
			if (err == nil) != valid {
				t.Fatalf("authority %s: %+v, %v", variant, identity, err)
			}
			if valid && (identity.InventoryID != data.Inventory["id"] || identity.ItemID != data.Items[0].Item["id"] || identity.SourceSHA256 != data.Items[0].Item["content_sha256"]) {
				t.Fatalf("wrong authority: %+v", identity)
			}
		})
	}
}

func TestOwnedProducerCurrentNativeRetryDoesNotBecomePriorHistory(t *testing.T) {
	for _, status := range []string{"queued", "running", "succeeded"} {
		t.Run(status, func(t *testing.T) {
			data, task, now := ownedProducerFixture(t)
			q := map[string]any{"id": ownedTestID(701), "kind": QueuePlanTask, "channel_profile_id": task["channel_profile_id"], "payload_json": map[string]any{"production_task_id": task["id"]}, "status": status, "attempt_count": json.Number("2"), "max_attempts": json.Number("3"), "last_error": "synthetic response loss", "dead_letter_at": nil, "locked_by": nil, "locked_at": nil}
			if status == "running" {
				q["locked_by"], q["locked_at"] = "native-current-claim", ownedISO(now)
			}
			if status == "succeeded" {
				q["last_error"] = nil
			}
			data.Tasks[0]["queues"] = []any{q}
			snapshot := ownedProducerSnapshot(t, data, now)
			before := historyJSON(t, snapshot.rows())
			if _, _, err := assessOwnedProducerSnapshot(snapshot, now, ownedString(task["id"]), ""); err != nil {
				t.Fatal("current native retry refused before exact lease-fenced reentry", err)
			}
			prior := assessOwnedHistorySnapshot(snapshot, now)
			if prior.BlockReason == nil || *prior.BlockReason != "owned_inventory_queue_failed" {
				t.Fatal("A1 prior-history queue predicate weakened", prior)
			}
			if string(before) != string(historyJSON(t, snapshot.rows())) {
				t.Fatal("producer assessment erased error/attempt evidence")
			}
		})
	}
}

func TestOwnedProducerRealPDSEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*PDSDecision)
		valid  bool
	}{
		{"real", func(*PDSDecision) {}, true},
		{"advisory", func(d *PDSDecision) { d.Metadata["warning"] = "quota_advisory" }, true},
		{"flag", func(d *PDSDecision) { d.Verdict = "flag" }, false},
		{"block", func(d *PDSDecision) { d.Verdict = "block" }, false},
		{"missing_decision", func(d *PDSDecision) { d.DecisionID = " " }, false},
		{"missing_version", func(d *PDSDecision) { d.RulesVersion = " " }, false},
		{"missing_rules", func(d *PDSDecision) { d.EvaluatedRules = nil }, false},
		{"blank_rule", func(d *PDSDecision) { d.EvaluatedRules = []string{"owned", " "} }, false},
		{"missing_metadata", func(d *PDSDecision) { d.Metadata = nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := ownedProducerRealDecision()
			tc.mutate(&d)
			if err := requireOwnedRealPDS(d); (err == nil) != tc.valid {
				t.Fatalf("real PDS evidence = %v, want valid %t", err, tc.valid)
			}
		})
	}
	for _, key := range []string{"disabled", "dev", "dev_allow_all", "noop", "fallback", "degraded", "fail_policy"} {
		t.Run(key, func(t *testing.T) {
			d := ownedProducerRealDecision()
			d.Metadata[key] = "allow"
			if err := requireOwnedRealPDS(d); err == nil {
				t.Fatal("degraded allow accepted")
			}
		})
	}
	for _, warning := range []string{"pds_disabled", "pds_unavailable", "pds_parse_failed", "dev_allow_all", "noop", "degraded", "pds_degraded"} {
		t.Run(warning, func(t *testing.T) {
			d := ownedProducerRealDecision()
			d.Metadata["warning"] = warning
			if err := requireOwnedRealPDS(d); err == nil {
				t.Fatal("fallback warning accepted")
			}
		})
	}
}

func TestOwnedProducerPDSAuditBindsActualRequestAndDenial(t *testing.T) {
	request := PDSDecisionRequest{ActorID: "account", ActionType: "plan_approval", Platform: "youtube", Content: map[string]any{"title": "owned"}, Context: map[string]any{"production_task_id": "task", "owned_inventory": map[string]any{"item_id": "item"}}}
	decision := ownedProducerRealDecision()
	decision.Verdict = "block"
	evidence, err := ownedPolicyEvidence(request, decision)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"request": map[string]any{"actor_id": "account", "action_type": "plan_approval", "platform": "youtube", "content": request.Content, "context": request.Context}, "response": pdsDecisionAuditJSON(decision)}
	wantJSON, err := ownedDecode([]byte(mustJSON(want)))
	if err != nil {
		t.Fatal(err)
	}
	if !ownedEqual(evidence, wantJSON) {
		t.Fatalf("request/denial evidence lost: %#v", evidence)
	}
	request.Context["production_task_id"] = "different-task"
	decision.Metadata["later_mutation"] = true
	if ownedMap(evidence["request"])["context"].(map[string]any)["production_task_id"] != "task" || ownedMap(ownedMap(evidence["response"])["metadata"])["later_mutation"] != nil {
		t.Fatal("prepared audit aliases mutable request/response")
	}
}

func TestOwnedProducerPreparedSnapshotBindsAuthority(t *testing.T) {
	task := ProductionTaskRow{ID: ownedTestID(600), ChannelProfileID: ownedTestID(2)}
	snapshot, err := newPreparedTaskSnapshot(task)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Producer = &ownedProducerAuthority{Identity: ownedProducerIdentity{TaskID: task.ID, InventoryID: ownedTestID(1)}, Digest: "before"}
	for _, current := range []*ownedProducerAuthority{nil, {Identity: snapshot.Producer.Identity, Digest: "changed"}, {Identity: ownedProducerIdentity{TaskID: ownedTestID(999)}, Digest: "before"}} {
		if err := snapshot.validateProducer(current); err == nil {
			t.Fatal("authority drift accepted")
		}
	}
	if err := snapshot.validateProducer(snapshot.Producer); err != nil {
		t.Fatal(err)
	}
	snapshot.Producer = nil
	if err := snapshot.validateProducer(&ownedProducerAuthority{Digest: "newly-approved"}); err == nil {
		t.Fatal("new authority missed")
	}
	if err := snapshot.validateProducer(nil); err != nil {
		t.Fatal("ordinary unchanged authority rejected")
	}
}

func ownedProducerPipeline(assetID string) map[string]any {
	nodes := []any{}
	for _, kind := range []string{"source", "transcode", "export", "youtube_upload"} {
		data := map[string]any{"config": map[string]any{}}
		if kind == "source" {
			data["asset_id"], data["config"] = assetID, map[string]any{"media_type": "video"}
		}
		if kind == "youtube_upload" {
			data["config"] = map[string]any{"privacy": "unlisted"}
		}
		nodes = append(nodes, map[string]any{"id": kind, "type": kind, "position": map[string]any{"x": 0, "y": 0}, "data": data})
	}
	edges := []any{}
	for _, pair := range [][2]string{{"source", "transcode"}, {"transcode", "export"}, {"transcode", "youtube_upload"}} {
		edges = append(edges, map[string]any{"id": pair[1], "source": pair[0], "target": pair[1], "sourceHandle": "video", "targetHandle": "video"})
	}
	return map[string]any{"nodes": nodes, "edges": edges}
}

func TestOwnedProducerPipelineCannotSubstituteSourceOrPrivacy(t *testing.T) {
	for _, variant := range []string{"valid", "private", "public", "source", "extra_source", "detached", "remote", "duplicate_id"} {
		t.Run(variant, func(t *testing.T) {
			graph := ownedProducerPipeline(ownedTestID(101))
			nodes := historyRows(graph["nodes"])
			switch variant {
			case "private", "public":
				ownedMap(ownedMap(nodes[3]["data"])["config"])["privacy"] = variant
			case "source":
				ownedMap(nodes[0]["data"])["asset_id"] = ownedTestID(102)
			case "extra_source":
				ownedMap(nodes[1]["data"])["asset_id"] = ownedTestID(102)
			case "detached":
				graph["edges"] = ownedArray(graph["edges"])[1:]
			case "remote":
				nodes[1]["type"] = "fetch_video"
			case "duplicate_id":
				nodes[2]["id"] = nodes[1]["id"]
			}
			if err := requireOwnedProducerPipeline(graph, ownedTestID(101)); (err == nil) != (variant == "valid") {
				t.Fatalf("pipeline %s: %v", variant, err)
			}
		})
	}
}

func TestOwnedProducerPendingPlanReplaysExactDurableDecision(t *testing.T) {
	planID := ownedTestID(700)
	task := ProductionTaskRow{ID: ownedTestID(600), ChannelProfileID: ownedTestID(2), TargetAccountID: ownedTestID(5), TitleSeed: "owned", Prompt: "owned prompt", AutoFlowPlanID: &planID, AgentApprovalEvidenceJSON: map[string]any{"owned_inventory": map[string]any{"input_asset_id": ownedTestID(101)}}}
	observation := AutoFlowPlanObservation{PlanID: planID, UploadNodeCount: 1, PlanPayload: map[string]any{"plan_id": planID, "pipeline_definition": ownedProducerPipeline(ownedTestID(101))}}
	task.RationaleJSON = map[string]any{"autoflow_plan_payload": observation.PlanPayload}
	request := ownedPlanPolicyRequest(task, observation)
	evidence, err := ownedPolicyEvidence(request, ownedProducerRealDecision())
	if err != nil {
		t.Fatal(err)
	}
	task.AgentApprovalEvidenceJSON["plan_pds"] = evidence
	for _, variant := range []string{"valid", "row_json_roundtrip", "minimal_native_context", "wrong_channel", "plan_id", "subject", "action", "rules", "source"} {
		t.Run(variant, func(t *testing.T) {
			copy := task
			copy.AgentApprovalEvidenceJSON = historyTestCopy(t, task.AgentApprovalEvidenceJSON).(map[string]any)
			policy := ownedMap(copy.AgentApprovalEvidenceJSON["plan_pds"])
			switch variant {
			case "minimal_native_context":
				policy["request"].(map[string]any)["context"] = map[string]any{"production_task_id": task.ID, "autoflow_plan_id": planID}
			case "wrong_channel":
				ownedMap(ownedMap(policy["request"])["context"])["channel_id"] = ownedTestID(999)
			case "row_json_roundtrip":
				if err := json.Unmarshal([]byte(mustJSON(task.AgentApprovalEvidenceJSON)), &copy.AgentApprovalEvidenceJSON); err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal([]byte(mustJSON(task.RationaleJSON)), &copy.RationaleJSON); err != nil {
					t.Fatal(err)
				}
			case "plan_id":
				other := ownedTestID(701)
				copy.AutoFlowPlanID = &other
			case "subject":
				ownedMap(policy["request"])["actor_id"] = ownedTestID(999)
			case "action":
				ownedMap(policy["request"])["action_type"] = "candidate_accept"
			case "rules":
				ownedMap(policy["response"])["evaluated_rules"] = []any{}
			case "source":
				ownedMap(copy.AgentApprovalEvidenceJSON["owned_inventory"])["input_asset_id"] = ownedTestID(102)
			}
			got, _, _, found, err := ownedPendingPlan(copy)
			valid := variant == "valid" || variant == "row_json_roundtrip" || variant == "minimal_native_context"
			if valid && (err != nil || !found || got.PlanID != planID) || !valid && err == nil {
				t.Fatalf("pending plan %s: %+v %t %v", variant, got, found, err)
			}
		})
	}
}

func TestOwnedProducerNativePlanBindingIsTheOnlyAcceptedTaskMutation(t *testing.T) {
	for _, variant := range []string{"native_binding", "already_bound", "title_drift", "different_plan", "extra_rationale", "wrong_response"} {
		t.Run(variant, func(t *testing.T) {
			original := ProductionTaskRow{ID: ownedTestID(600), State: TaskSelected, RationaleJSON: map[string]any{"candidate_id": "owned-original"}}
			planID := ownedTestID(700)
			current := original
			current.AutoFlowPlanID = &planID
			current.RationaleJSON = map[string]any{"candidate_id": "owned-original", "autoflow_plan_payload": map[string]any{"plan_id": planID}}
			if variant == "already_bound" {
				original = current
			}
			snapshot, err := newPreparedTaskSnapshot(original)
			if err != nil {
				t.Fatal(err)
			}
			observation := AutoFlowPlanObservation{PlanID: planID, PlanPayload: map[string]any{"plan_id": planID}}
			switch variant {
			case "title_drift":
				current.TitleSeed = "changed"
			case "different_plan":
				other := ownedTestID(701)
				current.AutoFlowPlanID = &other
			case "extra_rationale":
				current.RationaleJSON["unauthorized"] = true
			case "wrong_response":
				observation.PlanPayload["plan_id"] = ownedTestID(701)
			}
			err = validateOwnedPlanBinding(snapshot, current, observation)
			valid := variant == "native_binding" || variant == "already_bound"
			if (err == nil) != valid {
				t.Fatal("native binding", variant, err)
			}
		})
	}
	task := ProductionTaskRow{ID: ownedTestID(600), State: TaskSelected, AutoFlowPlanID: ptrString(ownedTestID(700)), RationaleJSON: map[string]any{"autoflow_plan_payload": map[string]any{"plan_id": ownedTestID(700)}}}
	if _, _, _, found, err := ownedPendingPlan(task); err != nil || found {
		t.Fatal("native plan response loss must resume original planner without pretending to have PDS", err)
	}
}

func TestOwnedProducerPlanningRequestStaysExactAfterNativePlanBinding(t *testing.T) {
	task := ProductionTaskRow{ID: ownedTestID(600), RationaleJSON: map[string]any{"candidate_id": "original"}}
	before := ownedPlanningRequest(task)
	task.AutoFlowPlanID = ptrString(ownedTestID(700))
	task.RationaleJSON["autoflow_plan_payload"] = map[string]any{"plan_id": *task.AutoFlowPlanID}
	if !ownedPolicyJSONEqual(before, ownedPlanningRequest(task)) {
		t.Fatal("native binding changed immutable original request")
	}
	if task.RationaleJSON["autoflow_plan_payload"] == nil {
		t.Fatal("request construction mutated task evidence")
	}
	task.RationaleJSON["candidate_id"] = "different"
	if ownedPolicyJSONEqual(before, ownedPlanningRequest(task)) {
		t.Fatal("unrelated request drift ignored")
	}
}

func TestOwnedProducerPromotionPolicyPinsRequest(t *testing.T) {
	task := ProductionTaskRow{ID: ownedTestID(600), TargetAccountID: ownedTestID(5), AutoFlowPlanID: ptrString(ownedTestID(700)), AgentApprovalEvidenceJSON: map[string]any{"owned_inventory": map[string]any{"item_id": ownedTestID(301), "input_asset_id": ownedTestID(101)}}}
	plan := AutoFlowPlanObservation{PlanID: *task.AutoFlowPlanID, PlanPayload: map[string]any{"plan_id": *task.AutoFlowPlanID, "pipeline_definition": ownedProducerPipeline(ownedTestID(101))}}
	task.RationaleJSON = map[string]any{"autoflow_plan_payload": plan.PlanPayload}
	task.AgentApprovalEvidenceJSON["plan_pds"], _ = ownedPolicyEvidence(ownedPlanPolicyRequest(task, plan), ownedProducerRealDecision())
	pub := PublicationRow{ID: ownedTestID(800), ProductionTaskID: task.ID, AccountID: task.TargetAccountID, Platform: "youtube", Title: "title", Description: "description"}
	d := ownedProducerRealDecision()
	evidence, err := ownedPolicyEvidence(ownedPromotionPolicyRequest(pub, task, "unlisted"), d)
	if err != nil {
		t.Fatal(err)
	}
	task.AgentApprovalEvidenceJSON["promotion_pds"] = evidence
	for _, variant := range []string{"valid", "private", "publication", "account", "decision", "missing_plan_and_pds", "missing_plan", "missing_plan_pds"} {
		t.Run(variant, func(t *testing.T) {
			p, decision, target := pub, d, "unlisted"
			current := task
			current.AgentApprovalEvidenceJSON = historyTestCopy(t, task.AgentApprovalEvidenceJSON).(map[string]any)
			switch variant {
			case "private":
				target = "private"
			case "publication":
				p.ID = ownedTestID(999)
			case "account":
				p.AccountID = ownedTestID(999)
			case "decision":
				decision.DecisionID = "different"
			case "missing_plan_and_pds":
				current.AutoFlowPlanID = nil
				delete(current.AgentApprovalEvidenceJSON, "plan_pds")
			case "missing_plan":
				current.AutoFlowPlanID = nil
			case "missing_plan_pds":
				delete(current.AgentApprovalEvidenceJSON, "plan_pds")
			}
			if err := requireOwnedPromotionPolicy(p, current, target, decision); (err == nil) != (variant == "valid") {
				t.Fatalf("promotion %s: %v", variant, err)
			}
		})
	}
}

func TestOwnedProducerHistoryExclusionKeepsCompleteClassification(t *testing.T) {
	for _, variant := range []string{"current", "missing", "blank_current", "orphan_current", "other_blank"} {
		t.Run(variant, func(t *testing.T) {
			f := historyGolden(t, "direct")
			task := historyTestFirst(f, "production_tasks")
			current := ownedString(task["id"])
			want := historyTestCopy(t, f["expected"]).(map[string]any)
			want["stable_history_sha256"] = historyTestHash(t, []any{})
			switch variant {
			case "missing":
				current = historyTestUID(99999)
				want = historyTestRefusal("owned_inventory_producer_missing")
			case "blank_current":
				historyTestFirst(f, "publishing_accounts")["platform_account_id"] = ""
				want = historyTestRefusal("owned_history_unclassified")
			case "orphan_current":
				task["target_account_id"] = historyTestUID(99999)
				want = historyTestRefusal("owned_history_orphan")
			case "other_blank":
				rows := historyTestRows(f)
				otherTask := historyTestCopy(t, task).(map[string]any)
				otherAccount := historyTestCopy(t, historyTestFirst(f, "publishing_accounts")).(map[string]any)
				otherOperation := historyTestCopy(t, historyTestFirst(f, "youtube_upload_operations")).(map[string]any)
				otherTask["id"], otherTask["target_account_id"] = historyTestUID(90001), historyTestUID(90002)
				otherAccount["id"], otherAccount["platform_account_id"] = otherTask["target_account_id"], ""
				otherOperation["id"], otherOperation["production_task_id"] = historyTestUID(90003), otherTask["id"]
				for table, row := range map[string]map[string]any{"production_tasks": otherTask, "publishing_accounts": otherAccount, "youtube_upload_operations": otherOperation} {
					rows[table] = append(rows[table].([]any), row)
				}
				want = historyTestRefusal("owned_history_unclassified")
			}
			got := assessOwnedProducerHistory(historySnapshot(t, f), historyAt(t, f["now"]), current)
			value, err := ownedDecode(mustHistoryAssessmentJSON(t, got))
			if err != nil || !ownedEqual(value, want) {
				t.Fatalf("producer assessment = %s; want %s (%v)", mustHistoryAssessmentJSON(t, got), historyJSON(t, want), err)
			}
		})
	}
}

func TestOwnedProducerHistoryRetirementAndOtherTaskFloorsRemain(t *testing.T) {
	f := historyGolden(t, "retired_unassigned")
	rows := historyTestRows(f)
	task := historyTestCopy(t, historyTestFirst(f, "production_tasks")).(map[string]any)
	account := historyTestCopy(t, historyTestFirst(f, "publishing_accounts")).(map[string]any)
	channel := historyTestCopy(t, historyTestFirst(f, "channel_profiles")).(map[string]any)
	channel["id"] = historyTestUID(90004)
	account["id"], account["platform_account_id"] = historyTestUID(90002), f["platform_channel_id"]
	account["channel_profile_id"] = channel["id"]
	task["id"], task["target_account_id"], task["state"] = historyTestUID(90001), account["id"], "selected"
	task["channel_profile_id"] = channel["id"]
	task["job_id"], task["retry_count"], task["blocked_by_guard"], task["failure_reason"] = nil, json.Number("0"), nil, nil
	rows["production_tasks"] = append(rows["production_tasks"].([]any), task)
	rows["publishing_accounts"] = append(rows["publishing_accounts"].([]any), account)
	rows["channel_profiles"] = append(rows["channel_profiles"].([]any), channel)
	got := assessOwnedProducerHistory(historySnapshot(t, f), historyAt(t, f["now"]), ownedString(task["id"]))
	if got.BlockReason != nil || len(got.RetiredSourceSHA256) == 0 || len(got.RetiredRenderSHA256) == 0 || len(got.Classifications) == 0 {
		t.Fatalf("retirement vanished with current task exclusion: %s", mustHistoryAssessmentJSON(t, got))
	}

	f = historyGolden(t, "direct")
	rows = historyTestRows(f)
	task = historyTestCopy(t, historyTestFirst(f, "production_tasks")).(map[string]any)
	task["id"], task["state"], task["job_id"] = historyTestUID(90001), "selected", nil
	rows["production_tasks"] = append(rows["production_tasks"].([]any), task)
	op := historyTestFirst(f, "youtube_upload_operations")
	op["request_attempted_at"] = f["now"]
	// A changed attempt after its completion is rejected, not omitted with the current task.
	got = assessOwnedProducerHistory(historySnapshot(t, f), historyAt(t, f["now"]), ownedString(task["id"]))
	if got.BlockReason == nil || *got.BlockReason != "owned_inventory_receipt" {
		t.Fatalf("other task effect escaped validation: %#v", got)
	}
}
