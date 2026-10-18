package channelops

import (
	"sort"
	"strings"
	"time"
)

const retainedTerminalReason = "owned_history_retained_terminal_invalid"
const retainedTerminalFields = "version classification operation_id task_id publication_id legacy_account_id legacy_channel_profile_id observed_at server_subject approval_reference retained_facts retained_facts_sha256"
const retainedFactFields = "task operations job nodes artifacts publications queues metrics feedback promotion_operations"

func retainedRequire(ok bool) { historyRequire(ok, retainedTerminalReason) }

func historyParseRetainedTerminal(value any) map[string]any {
	c := historyExact(value, retainedTerminalFields)
	retainedRequire(historyInt(c["version"]) == 1 && c["classification"] == "retained_terminal_upload_queue_gap")
	for _, key := range strings.Fields("operation_id task_id publication_id legacy_account_id legacy_channel_profile_id") {
		historyID(c[key])
	}
	historyUTC(c["observed_at"])
	historyText(c["server_subject"])
	historyText(c["approval_reference"])
	facts := historyExact(c["retained_facts"], retainedFactFields)
	for _, key := range strings.Fields(retainedFactFields) {
		if key == "task" || key == "job" {
			historyID(historyObject(facts[key])["id"])
			continue
		}
		rows := historyRows(facts[key])
		retainedRequire(len(rows) <= 128)
		previous := ""
		for _, row := range rows {
			id := historyID(row["id"])
			retainedRequire(id > previous)
			previous = id
		}
	}
	retainedRequire(historyHash(facts) == historyHashValue(c["retained_facts_sha256"]) && len(historyCanonical(c)) <= 1024*1024)
	return c
}

// Only present primary facts are selected. No absent queue row is reconstructed.
func historyRetainedTerminalFacts(rows, task map[string]any, now time.Time) map[string]any {
	channel := historyOne(historySelect(rows["channel_profiles"], func(r map[string]any) bool { return r["id"] == task["channel_profile_id"] }), retainedTerminalReason)
	account := historyOne(historySelect(rows["publishing_accounts"], func(r map[string]any) bool { return r["id"] == task["target_account_id"] }), retainedTerminalReason)
	retainedRequire(account["channel_profile_id"] == channel["id"])
	enabled, enabledOK := channel["enabled"].(bool)
	dryRun, dryRunOK := channel["dry_run"].(bool)
	retainedRequire(enabledOK && dryRunOK)
	for _, key := range []string{"halted_at", "intake_paused_at"} {
		value, exists := channel[key]
		retainedRequire(exists && (value == nil || historyBefore(historyTime(value), now)))
	}
	retainedRequire(!enabled || dryRun || channel["halted_at"] != nil || channel["intake_paused_at"] != nil)
	f := historyTask(rows, task)
	op, job, pub := historyOne(historyArray(f["operations"]), retainedTerminalReason), historyObject(f["job"]), historyOne(historyArray(f["publications"]), retainedTerminalReason)
	f["promotion_operations"] = historySelect(rows["publication_promotion_operations"], func(r map[string]any) bool {
		return r["production_task_id"] == task["id"] || r["publication_id"] == pub["id"]
	})
	p := historyOne(historyArray(f["promotion_operations"]), retainedTerminalReason)
	retainedRequire(task["state"] == "measured" && historyInt(task["retry_count"]) == 0 && task["failure_reason"] == nil && task["blocked_by_guard"] == nil && (task["failure_category"] == nil || task["failure_category"] == ""))
	retainedRequire(len(historySelect(rows["owned_seed_inventory_items"], func(i map[string]any) bool { return i["production_task_id"] == task["id"] })) == 0)
	for _, key := range []string{"agent_approval_evidence_json", "channel_config_snapshot_json"} {
		_, own := historyDefaultObject(task, key)["owned_inventory"]
		retainedRequire(!own)
	}
	retainedRequire(op["status"] == "succeeded" && op["error_message"] == nil && op["production_task_id"] == task["id"] && op["job_id"] == task["job_id"] && historySHA.MatchString(historyString(op["content_sha256"])))
	historyID(op["manager_task_id"])
	attempted, completed := historyTime(op["request_attempted_at"]), historyTime(op["completed_at"])
	retainedRequire(historyBefore(attempted, completed) && historyBefore(completed, now.Add(-30*24*time.Hour)))
	receipt := historyExact(op["receipt_json"], "video_id url title privacy tags quota_estimate")
	video := historyString(op["platform_video_id"])
	retainedRequire(historyVideo.MatchString(video) && historyIs(op["privacy"], "private", "unlisted") && receipt["privacy"] == op["privacy"] && receipt["title"] == op["title"] && receipt["video_id"] == video && receipt["url"] == "https://www.youtube.com/watch?v="+video)
	retainedRequire(job["id"] == op["job_id"] && job["status"] == "SUCCEEDED" && job["error_message"] == nil && historyBetween(historyTime(job["completed_at"]), completed, now))
	nodes := historyRows(f["nodes"])
	artifacts := historyArray(f["artifacts"])
	retainedRequire(len(nodes) >= 1 && len(nodes) <= 128 && len(artifacts) <= 128)
	uploads := 0
	for _, n := range nodes {
		retainedRequire(n["status"] == "SUCCEEDED" && n["job_id"] == job["id"] && n["error_message"] == nil)
		if n["node_type"] == "youtube_upload" {
			uploads++
			retainedRequire(n["id"] == op["node_execution_id"] && historyEqual(n["input_artifact_ids"], []any{op["input_artifact_id"]}) && historyBetween(historyTime(n["completed_at"]), completed, historyTime(job["completed_at"])))
			historyOne(historySelect(artifacts, func(a map[string]any) bool {
				return a["id"] == n["output_artifact_id"] && a["node_execution_id"] == n["id"] && a["job_id"] == job["id"] && historyEqual(historyDefaultObject(a, "media_info")["youtube"], receipt)
			}), retainedTerminalReason)
		}
	}
	retainedRequire(uploads == 1 && pub["production_task_id"] == task["id"] && pub["account_id"] == task["target_account_id"] && pub["platform"] == "youtube" && pub["platform_content_id"] == video && pub["desired_privacy"] == "unlisted" && pub["current_privacy"] == "unlisted" && pub["public_at"] == nil && historyIs(pub["publish_status"], "uploaded", "scheduled"))
	start, uploaded := historyTime(pub["scheduled_publish_at"]), historyTime(pub["uploaded_at"])
	retainedRequire(historyBetween(uploaded, completed, start) && historyBefore(start, now))
	retainedRequire(p["production_task_id"] == task["id"] && p["publication_id"] == pub["id"] && p["platform_video_id"] == video && p["status"] == "finalized" && p["error_message"] == nil && p["target_privacy"] == "unlisted" && p["observed_privacy"] == "unlisted" && historyIs(p["observed_publish_status"], "uploaded", "scheduled") && historyTime(p["scheduled_at"]).Equal(start) && p["attempt_key"] == "channelops-promotion:"+historyID(p["id"]))
	requested, confirmed, done := historyTime(p["request_attempted_at"]), historyTime(p["confirmed_at"]), historyTime(p["completed_at"])
	retainedRequire(historyBetween(requested, completed, confirmed) && historyBetween(done, confirmed, now))
	decision, evidence := historyObject(p["decision_json"]), historyObject(p["evidence_json"])
	retainedRequire(decision["verdict"] == "allow" && historyTruth(decision["decision_id"]) && historyTruth(decision["rules_version"]) && len(evidence) > 0)
	manual := historyID(p["queue_item_id"])
	auto := historyOne(historyArray(f["queues"]), retainedTerminalReason)
	payload := historyObject(auto["payload_json"])
	due := uploaded.Add(time.Hour)
	retainedRequire(auto["kind"] == "promote_publication" && auto["status"] == "cancelled" && auto["last_error"] == "replaced_by_immediate_unlisted_canary_promotion" && historyInt(auto["attempt_count"]) == 0 && auto["locked_at"] == nil && auto["locked_by"] == nil && auto["channel_profile_id"] == task["channel_profile_id"] && auto["id"] != manual && historyTime(auto["run_after"]).Equal(due) && auto["idempotency_key"] == "promote_publication:"+historyString(pub["id"])+":unlisted:"+historyZ(due) && payload["publication_id"] == pub["id"] && payload["target_visibility"] == "unlisted" && payload["scheduled_at"] == historyZ(due) && historyBetween(historyTime(auto["dead_letter_at"]), uploaded, requested))
	parent := historyID(auto["parent_queue_item_id"])
	retainedRequire(parent != manual && parent != auto["id"])
	for _, q := range historyRows(rows["channel_ops_queue_items"]) {
		retainedRequire(q["id"] != manual && q["id"] != parent)
		if q["channel_profile_id"] == task["channel_profile_id"] {
			retainedRequire(historyIs(q["status"], "succeeded", "cancelled"))
		}
		if historyDefaultObject(q, "payload_json")["job_id"] == job["id"] {
			retainedRequire(q["id"] == auto["id"])
		}
	}
	metrics, feedback := historyArray(f["metrics"]), historyArray(f["feedback"])
	retainedRequire(len(metrics) == 5 && len(feedback) == 6)
	for _, stage := range historyMetricStages {
		m := historyOne(historySelect(metrics, func(m map[string]any) bool { return m["snapshot_stage"] == stage.name }), retainedTerminalReason)
		due, grace := start.Add(time.Duration(stage.due)*time.Hour), start.Add(time.Duration(stage.grace)*time.Hour)
		done := historyTime(m["completed_at"])
		retainedRequire(m["publication_id"] == pub["id"] && historyTime(m["effective_start_at"]).Equal(start) && historyTime(m["due_at"]).Equal(due) && historyTime(m["grace_until"]).Equal(grace) && m["status"] == "succeeded" && historyInt(m["attempt_count"]) == 1 && m["last_error_code"] == nil && done.Equal(historyTime(m["last_attempt_at"])) && historyBetween(done, due, grace) && historyBefore(done, now))
		f := historyOne(historySelect(feedback, func(f map[string]any) bool { return f["snapshot_stage"] == stage.name }), retainedTerminalReason)
		retainedRequire(f["publication_id"] == pub["id"] && historyBetween(historyTime(f["collected_at"]), due, done))
	}
	immediate := historyOne(historySelect(feedback, func(f map[string]any) bool { return f["snapshot_stage"] == "immediate" }), retainedTerminalReason)
	retainedRequire(immediate["publication_id"] == pub["id"] && historyBetween(historyTime(immediate["collected_at"]), completed, now))
	retainedRequire(len(historySelect(rows["jobs"], func(j map[string]any) bool { return j["parent_job_id"] == job["id"] })) == 0)
	graph := historyTerminalGraph(rows, map[string]any{"job_id": job["id"], "upload_node_id": op["node_execution_id"], "task_id": task["id"], "legacy_channel_profile_id": task["channel_profile_id"]})
	checks := map[string]map[string][]string{"worker_task_dispatches": {"resolution_state": {"acknowledged", "cancelled"}}, "worker_task_delivery_attestations": {"ack_state": {"acknowledged"}}, "worker_event_emissions": {"emission_state": {"resolved"}}, "registered_worker_event_receipts": {"application_state": {"applied"}, "ack_state": {"acknowledged"}, "source_task_ack_state": {"acknowledged"}}, "registered_worker_event_deliveries": {"ack_state": {"acknowledged"}}}
	for table, fields := range checks {
		for _, row := range historyRows(graph[table]) {
			for key, states := range fields {
				retainedRequire(historyIs(row[key], states...))
			}
		}
	}
	for _, key := range strings.Fields(retainedFactFields) {
		if key == "task" || key == "job" {
			continue
		}
		a := historyArray(f[key])
		sort.Slice(a, func(i, j int) bool {
			return historyString(historyObject(a[i])["id"]) < historyString(historyObject(a[j])["id"])
		})
		f[key] = a
	}
	return f
}

func historyApprovedRetainedTerminal(rows map[string]any, bindings map[string]map[string]any, now time.Time) map[string]map[string]any {
	certificates := map[string]map[string]any{}
	for _, inventory := range historyRows(rows["owned_seed_inventories"]) {
		if inventory["approved_at"] == nil {
			continue
		}
		section := historyDefaultObject(historyObject(inventory["manifest_json"]), "legacy_history")
		raw, ok := section["retained_terminal_uploads"]
		if !ok {
			continue
		}
		for _, value := range historyArray(raw) {
			c := historyParseRetainedTerminal(value)
			id := historyString(c["operation_id"])
			retainedRequire(certificates[id] == nil || historyEqual(certificates[id], c))
			b := bindings[historyString(c["legacy_account_id"])]
			retainedRequire(b != nil && b["legacy_channel_profile_id"] == c["legacy_channel_profile_id"])
			found := false
			for _, v := range historyArray(b["qualified_operation_ids"]) {
				found = found || v == id
			}
			retainedRequire(found)
			q := historyObject(b["qualification"])
			at := historyUTC(c["observed_at"])
			retainedRequire(at.Equal(historyUTC(q["observed_at"])) && historyBetween(historyTime(inventory["approved_at"]), at, now) && c["server_subject"] == q["server_subject"] && c["approval_reference"] == q["approval_reference"])
			task := historyOne(historySelect(rows["production_tasks"], func(t map[string]any) bool { return t["id"] == c["task_id"] }), retainedTerminalReason)
			retainedRequire(task["target_account_id"] == c["legacy_account_id"] && task["channel_profile_id"] == c["legacy_channel_profile_id"])
			f := historyRetainedTerminalFacts(rows, task, at)
			retainedRequire(historyObject(historyArray(f["operations"])[0])["id"] == id && historyObject(historyArray(f["publications"])[0])["id"] == c["publication_id"] && historyHash(f) == c["retained_facts_sha256"] && historyEqual(f, c["retained_facts"]))
			certificates[id] = c
		}
	}
	return certificates
}
