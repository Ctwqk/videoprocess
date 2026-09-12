package channelops

import (
	"encoding/json"
	"strings"
	"time"
)

func requireOwnedRealPDS(decision PDSDecision) error {
	if decision.Verdict != "allow" || strings.TrimSpace(decision.DecisionID) == "" || strings.TrimSpace(decision.RulesVersion) == "" || len(decision.EvaluatedRules) == 0 || decision.Metadata == nil {
		return ownedHistoryError("owned_inventory_pds_evidence")
	}
	for _, rule := range decision.EvaluatedRules {
		if strings.TrimSpace(rule) == "" {
			return ownedHistoryError("owned_inventory_pds_evidence")
		}
	}
	if isPDSFailPolicyDecision(decision) {
		return ownedHistoryError("owned_inventory_pds_fallback")
	}
	for _, key := range []string{"disabled", "dev", "dev_allow_all", "noop", "fallback", "degraded"} {
		if historyTruth(decision.Metadata[key]) {
			return ownedHistoryError("owned_inventory_pds_fallback")
		}
	}
	switch ownedString(decision.Metadata["warning"]) {
	case "dev_allow_all", "noop", "degraded", "pds_degraded":
		return ownedHistoryError("owned_inventory_pds_fallback")
	}
	return nil
}

// Preserve the actual request, including fields omitted by PDSDecisionRequest's
// transport JSON tags. Denials use this same immutable audit shape.
func ownedPolicyEvidence(request PDSDecisionRequest, decision PDSDecision) (map[string]any, error) {
	value := map[string]any{"request": map[string]any{"actor_id": request.ActorID, "action_type": request.ActionType, "platform": request.Platform, "content": mapOrEmpty(request.Content), "context": mapOrEmpty(request.Context)}, "response": pdsDecisionAuditJSON(decision)}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	copy, err := ownedDecode(raw)
	if err != nil {
		return nil, err
	}
	return ownedMap(copy), nil
}

func ownedCandidatePolicyRequest(channel ChannelProfileRow, candidate TickCandidate) PDSDecisionRequest {
	policyContext := map[string]any{"channel_profile_id": channel.ID, "candidate_id": candidate.CandidateID, "source_kind": candidate.SourceKind, "topic_lane_id": candidateLaneID(candidate), "lane_format_id": candidateFormatID(candidate)}
	if candidate.owned != nil {
		policyContext["owned_inventory"] = ownedTaskEvidence(candidate)
	}
	return PDSDecisionRequest{ActorID: candidate.Account.ID, ActionType: "candidate_accept", Platform: "youtube", Content: map[string]any{"title": candidate.TitleSeed, "description": candidate.Prompt}, Context: policyContext}
}

func requireOwnedCandidatePolicy(channel ChannelProfileRow, candidate TickCandidate) error {
	var decision PDSDecision
	if err := json.Unmarshal([]byte(mustJSON(candidate.PDSDecisionJSON)), &decision); err != nil {
		return ownedHistoryError("owned_inventory_pds_evidence")
	}
	if err := requireOwnedRealPDS(decision); err != nil {
		return err
	}
	evidence, err := ownedPolicyEvidence(ownedCandidatePolicyRequest(channel, candidate), decision)
	if err != nil {
		return err
	}
	if !ownedEqual(evidence["request"], candidate.PDSRequestJSON) {
		return ownedHistoryError("owned_inventory_pds_context")
	}
	return nil
}

func requireOwnedTaskCandidatePolicy(task map[string]any) error {
	approval := ownedMap(task["agent_approval_evidence_json"])
	var decision PDSDecision
	if err := json.Unmarshal([]byte(mustJSON(approval["candidate_pds"])), &decision); err != nil {
		return ownedHistoryError("owned_inventory_pds_evidence")
	}
	if err := requireOwnedRealPDS(decision); err != nil {
		return err
	}
	candidateID := ownedString(ownedMap(task["rationale_json"])["candidate_id"])
	if candidateID == "" {
		return ownedHistoryError("owned_inventory_pds_context")
	}
	request := PDSDecisionRequest{ActorID: ownedString(task["target_account_id"]), ActionType: "candidate_accept", Platform: "youtube",
		Content: map[string]any{"title": task["title_seed"], "description": task["prompt"]},
		Context: map[string]any{"channel_profile_id": task["channel_profile_id"], "candidate_id": candidateID, "source_kind": "manual_seed", "topic_lane_id": task["topic_lane_id"], "lane_format_id": task["lane_format_id"], "owned_inventory": approval["owned_inventory"]}}
	evidence, err := ownedPolicyEvidence(request, decision)
	if err != nil {
		return err
	}
	if !ownedPolicyJSONEqual(evidence["request"], approval["candidate_pds_request"]) {
		return ownedHistoryError("owned_inventory_pds_context")
	}
	return nil
}

func requireOwnedProducerPipeline(raw any, assetID string) (err error) {
	defer historyRecover(&err, "owned_inventory_pipeline_invalid")
	encoded, encodeErr := json.Marshal(raw)
	historyRequire(encodeErr == nil, "owned_inventory_pipeline_invalid")
	normalized, decodeErr := ownedDecode(encoded)
	historyRequire(decodeErr == nil, "owned_inventory_pipeline_invalid")
	graph := historyParsePipeline(normalized, "owned_inventory_pipeline_invalid")
	allowed := map[string]bool{"source": true, "trim": true, "vertical_crop": true, "title_overlay": true, "transcode": true, "export": true, "youtube_upload": true}
	var source, upload, export string
	for id, node := range graph.nodes {
		kind := historyString(node["type"])
		data := historyObject(node["data"])
		config := ownedMap(data["config"])
		historyRequire(allowed[kind], "owned_inventory_pipeline_source")
		if kind == "source" {
			historyRequire(source == "", "owned_inventory_pipeline_source")
			source = id
			historyRequire(data["asset_id"] == assetID && (config["asset_id"] == nil || config["asset_id"] == assetID) && config["media_type"] == "video", "owned_inventory_pipeline_binding")
		} else {
			_, hasAsset := config["asset_id"]
			historyRequire(data["asset_id"] == nil && !hasAsset, "owned_inventory_pipeline_source")
		}
		if kind == "youtube_upload" {
			historyRequire(upload == "" && config["privacy"] == "unlisted", "owned_inventory_pipeline_binding")
			upload = id
		}
		if kind == "export" {
			historyRequire(export == "", "owned_inventory_pipeline_branch")
			export = id
		}
	}
	historyRequire(source != "" && upload != "" && export != "" && len(graph.edges) == len(graph.nodes)-1, "owned_inventory_pipeline_branch")
	parents, incoming, outgoing := map[string]bool{}, map[string]int{}, map[string]int{}
	for _, edge := range graph.edges {
		from, to := historyString(edge["source"]), historyString(edge["target"])
		incoming[to]++
		outgoing[from]++
		if to == upload || to == export {
			parents[from] = true
		}
	}
	historyRequire(len(parents) == 1, "owned_inventory_pipeline_branch")
	for id, node := range graph.nodes {
		in, out := 1, 1
		if id == source {
			in = 0
		}
		if id == upload || id == export {
			out = 0
		}
		if parents[id] {
			out = 2
			historyRequire(node["type"] == "transcode", "owned_inventory_pipeline_branch")
		}
		historyRequire(incoming[id] == in && outgoing[id] == out, "owned_inventory_pipeline_branch")
	}
	return nil
}

func ownedPlanPolicyRequest(task ProductionTaskRow, observation AutoFlowPlanObservation) PDSDecisionRequest {
	context := map[string]any{"production_task_id": task.ID, "autoflow_plan_id": observation.PlanID}
	if evidence := ownedMap(task.AgentApprovalEvidenceJSON["owned_inventory"]); len(evidence) != 0 {
		context["channel_id"], context["owned_inventory"] = task.ChannelProfileID, evidence
	}
	return PDSDecisionRequest{ActorID: task.TargetAccountID, ActionType: "plan_approval", Platform: "youtube", Content: map[string]any{"title": task.TitleSeed, "description": task.Prompt}, Context: context}
}

func ownedPlanningRequest(task ProductionTaskRow) map[string]any {
	rationale := map[string]any{}
	for key, value := range task.RationaleJSON {
		if key != "autoflow_plan_payload" {
			rationale[key] = value
		}
	}
	task.RationaleJSON = rationale
	return AutoFlowRequestForTask(task)
}

// The native planner commits exactly this task link before returning. No other
// task mutation is authorized by its response, and history is rechecked next.
func validateOwnedPlanBinding(prepared preparedTaskSnapshot, current ProductionTaskRow, observation AutoFlowPlanObservation) error {
	if observation.PlanPayload["plan_id"] != observation.PlanID || !uuidPattern.MatchString(observation.PlanID) {
		return ErrHandlerSnapshotStale
	}
	if current.AutoFlowPlanID == nil {
		return prepared.validate(current)
	}
	if *current.AutoFlowPlanID != observation.PlanID {
		return ErrHandlerSnapshotStale
	}
	if prepared.Task.AutoFlowPlanID != nil {
		return prepared.validate(current)
	}
	expectedRationale := map[string]any{}
	for k, v := range prepared.Task.RationaleJSON {
		expectedRationale[k] = v
	}
	expectedRationale["autoflow_plan_payload"] = map[string]any{"plan_id": observation.PlanID}
	if !ownedPolicyJSONEqual(current.RationaleJSON, expectedRationale) {
		return ErrHandlerSnapshotStale
	}
	current.AutoFlowPlanID = prepared.Task.AutoFlowPlanID
	current.RationaleJSON = prepared.Task.RationaleJSON
	return prepared.validate(current)
}

func ownedPendingPlan(task ProductionTaskRow) (AutoFlowPlanObservation, PDSDecisionRequest, PDSDecision, bool, error) {
	empty := func(err error) (AutoFlowPlanObservation, PDSDecisionRequest, PDSDecision, bool, error) {
		return AutoFlowPlanObservation{}, PDSDecisionRequest{}, PDSDecision{}, false, err
	}
	policy := ownedMap(task.AgentApprovalEvidenceJSON["plan_pds"])
	if task.AutoFlowPlanID == nil && len(policy) == 0 {
		return empty(nil)
	}
	if task.AutoFlowPlanID != nil && len(policy) == 0 && task.State == TaskSelected && ownedPolicyJSONEqual(task.RationaleJSON["autoflow_plan_payload"], map[string]any{"plan_id": *task.AutoFlowPlanID}) {
		return empty(nil)
	}
	if task.AutoFlowPlanID == nil || len(policy) == 0 {
		return empty(ErrHandlerSnapshotStale)
	}
	payload := ownedMap(task.RationaleJSON["autoflow_plan_payload"])
	if payload["plan_id"] != *task.AutoFlowPlanID {
		return empty(ErrHandlerSnapshotStale)
	}
	assetID := ownedString(ownedMap(task.AgentApprovalEvidenceJSON["owned_inventory"])["input_asset_id"])
	if err := requireOwnedProducerPipeline(payload["pipeline_definition"], assetID); err != nil {
		return empty(err)
	}
	observation := AutoFlowPlanObservation{PlanID: *task.AutoFlowPlanID, UploadNodeCount: 1, PlanPayload: payload}
	storedRequest := ownedMap(policy["request"])
	context := ownedMap(storedRequest["context"])
	if storedRequest["actor_id"] != task.TargetAccountID || storedRequest["action_type"] != "plan_approval" || storedRequest["platform"] != "youtube" || !ownedPolicyJSONEqual(storedRequest["content"], map[string]any{"title": task.TitleSeed, "description": task.Prompt}) || context["production_task_id"] != task.ID || context["autoflow_plan_id"] != observation.PlanID {
		return empty(ownedHistoryError("owned_inventory_pds_context"))
	}
	if channel, ok := context["channel_id"]; ok && channel != task.ChannelProfileID {
		return empty(ownedHistoryError("owned_inventory_pds_context"))
	}
	if owned, ok := context["owned_inventory"]; ok && !ownedPolicyJSONEqual(owned, task.AgentApprovalEvidenceJSON["owned_inventory"]) {
		return empty(ownedHistoryError("owned_inventory_pds_context"))
	}
	request := PDSDecisionRequest{ActorID: task.TargetAccountID, ActionType: "plan_approval", Platform: "youtube", Content: ownedMap(storedRequest["content"]), Context: context}
	var decision PDSDecision
	if err := json.Unmarshal([]byte(mustJSON(policy["response"])), &decision); err != nil {
		return empty(ownedHistoryError("owned_inventory_pds_evidence"))
	}
	if err := requireOwnedRealPDS(decision); err != nil {
		return empty(err)
	}
	expected, err := ownedPolicyEvidence(request, decision)
	if err != nil {
		return empty(err)
	}
	if !ownedPolicyJSONEqual(expected, policy) {
		return empty(ownedHistoryError("owned_inventory_pds_context"))
	}
	return observation, request, decision, true, nil
}

func ownedPromotionPolicyRequest(publication PublicationRow, task ProductionTaskRow, target string) PDSDecisionRequest {
	context := map[string]any{"publication_id": publication.ID, "production_task_id": publication.ProductionTaskID, "target_visibility": target}
	if evidence := ownedMap(task.AgentApprovalEvidenceJSON["owned_inventory"]); len(evidence) != 0 {
		context["owned_inventory"] = evidence
	}
	return PDSDecisionRequest{ActorID: publication.AccountID, ActionType: "publish", Platform: publication.Platform, Content: map[string]any{"title": publication.Title, "description": publication.Description}, Context: context}
}

func requireOwnedPromotionPolicy(publication PublicationRow, task ProductionTaskRow, target string, decision PDSDecision) error {
	if _, _, _, found, err := ownedPendingPlan(task); err != nil {
		return err
	} else if !found {
		return ErrHandlerSnapshotStale
	}
	if target != "unlisted" || publication.ProductionTaskID != task.ID || publication.AccountID != task.TargetAccountID || publication.Platform != "youtube" {
		return ownedHistoryError("owned_inventory_publication_identity")
	}
	if err := requireOwnedRealPDS(decision); err != nil {
		return err
	}
	expected, err := ownedPolicyEvidence(ownedPromotionPolicyRequest(publication, task, target), decision)
	if err != nil {
		return err
	}
	if !ownedPolicyJSONEqual(expected, task.AgentApprovalEvidenceJSON["promotion_pds"]) {
		return ownedHistoryError("owned_inventory_pds_context")
	}
	return nil
}

// These are existing Go JSON API/row values, not frozen Python manifest bytes.
// The same serializer used for task snapshots preserves their actual JSON shape.
func ownedPolicyJSONEqual(left, right any) bool {
	l, le := handlerSnapshotDigest(left)
	r, re := handlerSnapshotDigest(right)
	return le == nil && re == nil && l == r
}

type ownedProducerIdentity struct {
	TaskID, PlatformChannelID, InventoryID, ItemID, SourceSHA256 string
}

type ownedProducerAuthority struct {
	Identity ownedProducerIdentity
	Digest   string
}

func ownedProducerIdentityFromSnapshot(snapshot ownedHistorySnapshot, now time.Time, taskID string) (identity ownedProducerIdentity, err error) {
	defer historyRecover(&err, "")
	rows := snapshot.rows()
	one := func(table string, match func(map[string]any) bool) map[string]any {
		return historyOne(historySelect(rows[table], match), "owned_inventory_producer_binding")
	}
	task := one("production_tasks", func(r map[string]any) bool { return r["id"] == taskID })
	account := one("publishing_accounts", func(r map[string]any) bool { return r["id"] == task["target_account_id"] })
	channel := one("channel_profiles", func(r map[string]any) bool { return r["id"] == task["channel_profile_id"] })
	bindings, certificate, _ := historyApprovedAuthority(rows, now)
	historyRequire(bindings[ownedString(account["id"])] == nil, "owned_inventory_historical_producer_pinned")
	for _, binding := range bindings {
		historyRequire(binding["legacy_channel_profile_id"] != channel["id"], "owned_inventory_historical_producer_pinned")
	}
	historyRequire(certificate == nil || certificate["legacy_account_id"] != account["id"] && certificate["legacy_channel_profile_id"] != channel["id"], "owned_inventory_historical_producer_pinned")
	historyRequire(account["channel_profile_id"] == channel["id"] && historyPlatform(account) == "youtube" && historyUC.MatchString(ownedString(account["platform_account_id"])) && account["platform_account_id"] == snapshot.platformChannelID, "owned_history_unclassified")
	identity = ownedProducerIdentity{TaskID: taskID, PlatformChannelID: snapshot.platformChannelID}
	occupied := historySelect(rows["owned_seed_inventories"], func(r map[string]any) bool {
		return r["approved_at"] != nil && r["succession_released_at"] == nil && r["platform_channel_id"] == snapshot.platformChannelID
	})
	items := historySelect(rows["owned_seed_inventory_items"], func(r map[string]any) bool { return r["production_task_id"] == taskID })
	evidence := ownedMap(ownedMap(task["agent_approval_evidence_json"])["owned_inventory"])
	snapshotEvidence := ownedMap(ownedMap(task["channel_config_snapshot_json"])["owned_inventory"])
	if len(occupied) == 0 {
		historyRequire(len(items) == 0 && len(evidence) == 0 && len(snapshotEvidence) == 0, "owned_inventory_producer_binding")
		return identity, nil
	}
	row := historyOne(occupied, "owned_inventory_producer_binding")
	item := historyOne(items, "owned_inventory_producer_binding")
	manifestBytes, encodeErr := ownedCanonical(row["manifest_json"])
	historyRequire(encodeErr == nil, "owned_inventory_manifest_changed")
	_, decodeErr := decodeOwnedHistoryManifest(manifestBytes)
	manifest := historyObject(row["manifest_json"])
	historyRequire(decodeErr == nil && ownedHasHash(manifest, row["manifest_sha256"]), "owned_inventory_manifest_changed")
	historyRequire(manifest["inventory_id"] == row["id"] && historyEqual(historyFields(manifest, "channel_profile_id topic_lane_id lane_format_id target_account_id platform_channel_id privacy max_admissions minimum_interval_seconds"), historyFields(row, "channel_profile_id topic_lane_id lane_format_id target_account_id platform_channel_id privacy max_admissions minimum_interval_seconds")) && historyTime(manifest["starts_at"]).Equal(historyTime(row["starts_at"])) && historyTime(manifest["expires_at"]).Equal(historyTime(row["expires_at"])), "owned_inventory_manifest_changed")
	// All seven immutable entries remain pinned, including inputs not yet admitted.
	allItems := historySelect(rows["owned_seed_inventory_items"], func(r map[string]any) bool { return r["inventory_id"] == row["id"] })
	historyRequire(len(allItems) == 7, "owned_inventory_cardinality")
	for _, bound := range historyRows(allItems) {
		seed := one("manual_seeds", func(r map[string]any) bool { return r["id"] == bound["manual_seed_id"] })
		asset := one("assets", func(r map[string]any) bool { return r["id"] == bound["asset_id"] })
		entry := historyOne(historySelect(manifest["entries"], func(r map[string]any) bool { return r["id"] == bound["id"] }), "owned_inventory_manifest_changed")
		actual := historyFields(bound, "id ordinal asset_id manual_seed_id content_sha256 byte_size provenance_sha256 seed_sha256")
		actual["storage_descriptor"], actual["provenance_evidence"], actual["prompt"], actual["title_seed"] = bound["storage_descriptor_json"], bound["provenance_evidence_json"], seed["prompt"], seed["title_seed"]
		historyRequire(ownedEqual(actual, entry) && ownedHasHash(historyFields(seed, "id channel_profile_id topic_lane_id target_account_id prompt title_seed source_policy source_platforms_json material_library_ids_json constraints_json"), bound["seed_sha256"]), "owned_inventory_seed_changed")
		descriptor, descriptorErr := ownedAssetDescriptor(asset)
		historyRequire(descriptorErr == nil && ownedEqual(descriptor, bound["storage_descriptor_json"]) && historyEqual(bound["byte_size"], asset["file_size"]), "owned_inventory_asset_changed")
		provenance := ownedMap(bound["provenance_evidence_json"])
		historyRequire(provenance["rights"] == "owned" && provenance["provenance"] == "generated" && ownedHasHash(provenance, bound["provenance_sha256"]), "owned_inventory_provenance_changed")
	}
	starts, expires := historyTime(row["starts_at"]), historyTime(row["expires_at"])
	historyRequire(historyIs(row["state"], "approved", "exhausted") && row["revoked_at"] == nil && row["hold_reason"] == nil && historyTruth(row["approved_by"]) && historyTruth(row["approval_reference"]) && !historyTime(row["approved_at"]).After(now) && !starts.After(now) && now.Before(expires) && expires.Sub(starts) == 7*24*time.Hour, "owned_inventory_producer_inactive")
	historyRequire(channel["owned_seed_inventory_id"] == row["id"] && row["channel_profile_id"] == channel["id"] && row["target_account_id"] == account["id"] && item["inventory_id"] == row["id"] && item["platform_channel_id"] == snapshot.platformChannelID && item["state"] == "reserved" && item["consumed_at"] != nil && task["manual_seed_id"] == item["manual_seed_id"] && task["topic_lane_id"] == row["topic_lane_id"] && task["lane_format_id"] == row["lane_format_id"], "owned_inventory_producer_binding")
	historyRequire(channel["enabled"] == true && channel["dry_run"] == false && channel["halted_at"] == nil && (channel["intake_paused_at"] == nil || row["state"] == "exhausted" && channel["intake_pause_reason"] == "owned_inventory_exhausted") && account["enabled"] == true && account["paused_until"] == nil && account["default_privacy"] == "unlisted" && row["privacy"] == "unlisted" && account["external_asset_auto_publish"] == false, "owned_inventory_producer_controls")
	historyRequire(task["source"] == "manual_seed" && task["approval_mode"] == "agent" && task["uses_external_assets"] == false && ownedEqual(task["source_platforms_json"], []any{}) && ownedEqual(task["material_library_ids_json"], []any{}) && historyEqual(task["retry_count"], json.Number("0")) && task["failure_reason"] == nil && task["blocked_by_guard"] == nil && historyIs(task["state"], "selected", "planning", "producing", "uploaded_private", "scheduled"), "owned_inventory_producer_task")
	seed := one("manual_seeds", func(r map[string]any) bool { return r["id"] == item["manual_seed_id"] })
	entry := historyOne(historySelect(manifest["entries"], func(r map[string]any) bool { return r["id"] == item["id"] }), "owned_inventory_producer_binding")
	boundEntry := historyFields(item, "id ordinal asset_id manual_seed_id content_sha256 byte_size provenance_sha256 seed_sha256")
	boundEntry["storage_descriptor"], boundEntry["provenance_evidence"], boundEntry["prompt"], boundEntry["title_seed"] = item["storage_descriptor_json"], item["provenance_evidence_json"], seed["prompt"], seed["title_seed"]
	seedBinding := historyFields(seed, "id channel_profile_id topic_lane_id target_account_id prompt title_seed source_policy source_platforms_json material_library_ids_json constraints_json")
	historyRequire(seed["status"] == "exhausted" && ownedEqual(entry, boundEntry) && ownedHasHash(seedBinding, item["seed_sha256"]) && seed["target_account_id"] == task["target_account_id"] && seed["channel_profile_id"] == channel["id"] && seed["source_policy"] == "owned_only" && ownedEqual(seed["source_platforms_json"], []any{}) && ownedEqual(seed["material_library_ids_json"], []any{}), "owned_inventory_seed_changed")
	historyRequire(task["prompt"] == seed["prompt"] && task["title_seed"] == seed["title_seed"], "owned_inventory_seed_changed")
	constraints := ownedMap(seed["constraints_json"])
	historyRequire(constraints["input_asset_id"] == item["asset_id"] && constraints["source_strategy"] == "input_video" && constraints["planning_mode"] == "template", "owned_inventory_source_changed")
	historyRequire(ownedEqual(ownedMap(ownedMap(task["channel_config_snapshot_json"])["manual_seed"])["constraints_json"], constraints), "owned_inventory_source_changed")
	asset := one("assets", func(r map[string]any) bool { return r["id"] == item["asset_id"] })
	descriptor, descriptorErr := ownedAssetDescriptor(asset)
	historyRequire(descriptorErr == nil && ownedEqual(descriptor, item["storage_descriptor_json"]), "owned_inventory_asset_changed")
	historyRequire(ownedEqual(evidence, snapshotEvidence), "owned_inventory_evidence_changed")
	for key, value := range map[string]any{"inventory_id": row["id"], "item_id": item["id"], "manifest_sha256": row["manifest_sha256"], "configuration_sha256": manifest["configuration_sha256"], "input_asset_id": item["asset_id"], "source_content_sha256": item["content_sha256"], "seed_sha256": item["seed_sha256"]} {
		historyRequire(evidence[key] == value, "owned_inventory_evidence_changed")
	}
	if err := requireOwnedTaskCandidatePolicy(task); err != nil {
		return identity, err
	}
	identity.InventoryID, identity.ItemID, identity.SourceSHA256 = historyID(row["id"]), historyID(item["id"]), historyHashValue(item["content_sha256"])
	return identity, nil
}

func assessOwnedProducerSnapshot(snapshot ownedHistorySnapshot, now time.Time, taskID, renderSHA256 string) (identity ownedProducerIdentity, assessment ownedHistoryAssessment, err error) {
	defer historyRecover(&err, "")
	identity, err = ownedProducerIdentityFromSnapshot(snapshot, now, taskID)
	if err != nil {
		return ownedProducerIdentity{}, ownedHistoryAssessment{}, err
	}
	assessment = assessOwnedProducerHistory(snapshot, now, taskID)
	if assessment.BlockReason != nil {
		return identity, assessment, ownedHistoryError(*assessment.BlockReason)
	}
	if assessment.WaitReason != nil {
		return identity, assessment, ownedHistoryError(*assessment.WaitReason)
	}
	rows := snapshot.rows()
	if identity.InventoryID != "" {
		// Current-task retry state is fenced by the native queue lease, not A1's
		// prior-effect normal-history gate. Complete classification stays above.
		for _, op := range historyRows(rows["youtube_upload_operations"]) {
			if op["production_task_id"] == taskID {
				currentHash := historyHashValue(op["content_sha256"])
				historyRequire(renderSHA256 == "" || renderSHA256 == currentHash, "owned_inventory_render_sha256")
				renderSHA256 = currentHash
			}
		}
	}
	retired := append(append([]string{}, assessment.RetiredSourceSHA256...), assessment.RetiredRenderSHA256...)
	for _, hash := range retired {
		historyRequire(identity.SourceSHA256 != hash && renderSHA256 != hash, "owned_inventory_retired_hash_reuse")
	}
	historyRequire(identity.SourceSHA256 != "" || len(retired) == 0, "owned_inventory_source_evidence_missing")
	for _, item := range historyRows(rows["owned_seed_inventory_items"]) {
		if item["production_task_id"] != taskID && item["state"] != "unused" && item["platform_channel_id"] == snapshot.platformChannelID && identity.SourceSHA256 != "" {
			historyRequire(item["content_sha256"] != identity.SourceSHA256, "owned_inventory_source_reuse")
		}
	}
	if renderSHA256 != "" {
		historyRequire(historySHA.MatchString(renderSHA256), "owned_inventory_render_sha256")
		members := map[string]bool{}
		for _, c := range assessment.Classifications {
			if c.PlatformChannelID != nil && *c.PlatformChannelID == snapshot.platformChannelID {
				members[c.OperationID] = true
			}
		}
		for _, op := range historyRows(rows["youtube_upload_operations"]) {
			if members[ownedString(op["id"])] && op["production_task_id"] != taskID {
				historyRequire(op["content_sha256"] != renderSHA256, "owned_inventory_render_reuse")
			}
		}
	}
	return identity, assessment, nil
}
