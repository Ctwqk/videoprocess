package channelops

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"
	"time"
)

func historyTestCertificate(f map[string]any) map[string]any {
	return historyTestManifest(f)["legacy_history"].(map[string]any)["retired_unassigned_preupload"].(map[string]any)
}
func historyTestFind(t *testing.T, f map[string]any, table, key string, value any) map[string]any {
	t.Helper()
	for _, v := range historyTestRows(f)[table].([]any) {
		r := v.(map[string]any)
		if r[key] == value {
			return r
		}
	}
	t.Fatalf("missing test row %s %s", table, key)
	return nil
}
func historyTestRetry(t *testing.T, f map[string]any) map[string]any {
	t.Helper()
	for _, v := range historyTestRows(f)["worker_task_dispatches"].([]any) {
		r := v.(map[string]any)
		if r["origin_receipt_id"] != nil {
			return r
		}
	}
	t.Fatal("missing retry")
	return nil
}
func historyTestReseal(t *testing.T, f map[string]any) {
	t.Helper()
	c := historyTestCertificate(f)
	retained := c["retained_facts"].(map[string]any)
	for key, table := range map[string]string{"operation": "youtube_upload_operations", "task": "production_tasks", "job": "jobs", "account": "publishing_accounts", "channel": "channel_profiles", "manual_seed": "manual_seeds"} {
		retained[key] = historyTestCopy(t, historyTestFirst(f, table))
	}
	retained["upload_node"] = historyTestCopy(t, historyTestFind(t, f, "node_executions", "id", c["upload_node_id"]))
	graph := map[string]any{}
	for _, table := range strings.Fields(historyTerminalTables) {
		a := historyTestCopy(t, historyTestRows(f)[table]).([]any)
		sort.Slice(a, func(i, j int) bool {
			return a[i].(map[string]any)["id"].(string) < a[j].(map[string]any)["id"].(string)
		})
		graph[table] = a
	}
	c["terminal_graph"] = graph
	c["terminal_graph_sha256"] = historyTestHash(t, graph)
	c["transition_sha256"] = historyTestHash(t, historyTestFirst(f, "production_tasks")["transition_history_json"])
	historyTestRehash(t, f)
}

func TestOwnedHistoryRetiredDrift(t *testing.T) {
	for _, bad := range []string{"attempt", "manager", "video", "receipt", "complete", "fk", "registration", "started", "job_running", "node_running", "unhalt", "unpause", "claim", "emission", "delivery", "pending", "marker", "stale", "future", "origin", "retry_key", "retry_message", "retry_hash", "retry_no_ack", "retry_authorized", "retry_invented_cancel", "source_missing", "source_hash", "render_hash", "payload_missing", "orphan_receipt", "new_job", "new_queue", "no_redis", "source_hash_missing", "extra_observation", "duplicate_observation", "no_certificate", "v1"} {
		t.Run(bad, func(t *testing.T) {
			f := historyGolden(t, "retired_unassigned")
			rows := historyTestRows(f)
			c := historyTestCertificate(f)
			op := historyTestFirst(f, "youtube_upload_operations")
			upload := historyTestFind(t, f, "node_executions", "id", c["upload_node_id"])
			retry := historyTestRetry(t, f)
			redis := f["redis_observations"].([]any)
			var observation map[string]any
			for _, v := range redis {
				r := v.(map[string]any)
				if r["dispatch_key"] == retry["dispatch_key"] {
					observation = r
				}
			}
			switch bad {
			case "attempt":
				op["request_attempted_at"] = f["now"]
			case "manager":
				op["manager_task_id"] = historyTestUID(999)
			case "video":
				op["platform_video_id"] = "abcdefghijk"
			case "receipt":
				op["receipt_json"] = map[string]any{"video_id": "abcdefghijk"}
			case "complete":
				op["completed_at"] = f["now"]
			case "fk":
				op["production_task_id"] = nil
			case "registration":
				upload["worker_registration_id"] = nil
			case "started":
				upload["started_at"] = nil
			case "job_running":
				historyTestFirst(f, "jobs")["status"] = "RUNNING"
			case "node_running":
				upload["status"] = "RUNNING"
			case "unhalt":
				historyTestFirst(f, "channel_profiles")["halted_at"] = nil
			case "unpause":
				historyTestFirst(f, "channel_profiles")["intake_paused_at"] = nil
			case "claim":
				a := historyTestCopy(t, historyTestFirst(f, "worker_task_delivery_attestations")).(map[string]any)
				a["id"] = historyTestUID(999)
				a["dispatch_key"] = retry["dispatch_key"]
				rows["worker_task_delivery_attestations"] = append(rows["worker_task_delivery_attestations"].([]any), a)
			case "emission":
				a := historyTestCopy(t, historyTestFirst(f, "worker_event_emissions")).(map[string]any)
				a["id"] = historyTestUID(999)
				a["emission_state"] = "prepared"
				rows["worker_event_emissions"] = append(rows["worker_event_emissions"].([]any), a)
			case "delivery":
				historyTestFirst(f, "registered_worker_event_deliveries")["ack_state"] = "pending"
			case "pending":
				observation["pending_message_ids"] = []any{retry["redis_message_id"]}
			case "marker":
				observation["marker_message_id"] = "9999-0"
			case "stale":
				observation["observed_at"] = historyISO(historyAt(t, f["now"]).Add(-61 * time.Second))
			case "future":
				observation["observed_at"] = historyISO(historyAt(t, f["now"]).Add(time.Second))
			case "origin":
				retry["origin_receipt_id"] = nil
			case "retry_key":
				retry["dispatch_key"] = historyTestUID(999)
			case "retry_message":
				retry["redis_message_id"] = "9999-0"
			case "retry_hash":
				retry["payload_sha256"] = strings.Repeat("f", 64)
			case "retry_no_ack":
				retry["acknowledged_at"] = nil
			case "retry_authorized":
				retry["resolution_state"] = "cancel_authorized"
			case "retry_invented_cancel":
				retry["cancelled_at"] = retry["acknowledged_at"]
			case "source_missing":
				rows["assets"] = []any{}
			case "source_hash":
				historyTestFirst(f, "assets")["media_info"].(map[string]any)["content_sha256"] = strings.Repeat("f", 64)
			case "render_hash":
				op["content_sha256"] = strings.Repeat("f", 64)
			case "payload_missing":
				delete(retry, "payload_json")
			case "orphan_receipt":
				historyTestFirst(f, "registered_worker_event_receipts")["source_task_attestation_id"] = historyTestUID(999)
			case "new_job":
				j := historyTestCopy(t, historyTestFirst(f, "jobs")).(map[string]any)
				j["id"] = historyTestUID(999)
				j["parent_job_id"] = c["job_id"]
				rows["jobs"] = append(rows["jobs"].([]any), j)
			case "new_queue":
				rows["channel_ops_queue_items"] = []any{map[string]any{"id": historyTestUID(999), "channel_profile_id": c["legacy_channel_profile_id"], "kind": "execute_task", "status": "queued", "payload_json": map[string]any{"production_task_id": c["task_id"]}}}
			case "no_redis":
				f["redis_observations"] = []any{}
			case "source_hash_missing":
				c["retained_facts"].(map[string]any)["source_assets"] = []any{}
				historyTestRehash(t, f)
			case "extra_observation":
				extra := historyTestCopy(t, observation).(map[string]any)
				extra["dispatch_key"] = historyTestUID(999)
				f["redis_observations"] = append(redis, extra)
			case "duplicate_observation":
				f["redis_observations"] = append(redis, historyTestCopy(t, observation))
			case "no_certificate":
				rows["owned_seed_inventories"] = []any{}
			case "v1":
				m := historyTestManifest(f)
				m["version"] = json.Number("1")
				delete(m, "legacy_history")
				historyTestRehash(t, f)
			}
			historyTestReason(t, f, "")
		})
	}
}

func TestOwnedHistoryRetiredRehashedSemantics(t *testing.T) {
	for _, bad := range []string{"origin_completed", "retry_emission", "missing_ack", "applied", "att_mismatch", "payload_event", "wrong_stream", "extra_dispatch", "broken_dependency", "duplicate_edge", "cycle", "duplicate_input", "source_path", "source_claim", "grant_image", "transition", "invented_cancel", "never_attempted", "missing_column", "orphan_delivery", "dispatch_config", "dispatch_input", "unresolved_emission", "registration_missing", "grant_missing", "registration_fingerprint", "upload_privacy", "operation_attempt", "claim_worker", "artifact_orphan", "ack_time", "receipt_payload_hash", "source_message", "receipt_stream", "retired_target", "retired_uc"} {
		t.Run(bad, func(t *testing.T) {
			f := historyGolden(t, "retired_unassigned")
			r := historyTestRows(f)
			c := historyTestCertificate(f)
			retry := historyTestRetry(t, f)
			att := historyTestFirst(f, "worker_task_delivery_attestations")
			receipt := historyTestFirst(f, "registered_worker_event_receipts")
			job := historyTestFirst(f, "jobs")
			edges := job["pipeline_snapshot"].(map[string]any)["edges"].([]any)
			switch bad {
			case "origin_completed":
				retry["origin_receipt_id"] = receipt["id"]
			case "retry_emission":
				e := historyTestCopy(t, historyTestFirst(f, "worker_event_emissions")).(map[string]any)
				e["id"] = historyTestUID(401)
				e["source_task_attestation_id"] = historyTestUID(402)
				e["payload_json"].(map[string]any)["task_dispatch_key"] = retry["dispatch_key"]
				r["worker_event_emissions"] = append(r["worker_event_emissions"].([]any), e)
			case "missing_ack":
				historyTestFirst(f, "registered_worker_event_deliveries")["acknowledged_at"] = nil
			case "applied":
				receipt["application_state"] = "accepted"
			case "att_mismatch":
				att["worker_lease_epoch"] = json.Number("99")
			case "payload_event":
				receipt["payload_json"].(map[string]any)["event"] = "node_failed"
			case "wrong_stream":
				retry["redis_stream"] = "vp:tasks:vision"
			case "extra_dispatch":
				d := historyTestCopy(t, retry).(map[string]any)
				d["id"] = historyTestUID(401)
				d["dispatch_key"] = historyTestUID(402)
				d["redis_message_id"] = "9000-0"
				d["payload_json"].(map[string]any)["dispatch_key"] = d["dispatch_key"]
				d["payload_sha256"] = historyTestHash(t, d["payload_json"])
				r["worker_task_dispatches"] = append(r["worker_task_dispatches"].([]any), d)
				obs := map[string]any{"kind": "task", "redis_stream": d["redis_stream"], "consumer_group": d["consumer_group"], "message_id": d["redis_message_id"], "marker_message_id": d["redis_message_id"], "dispatch_key": d["dispatch_key"], "payload_sha256": d["payload_sha256"], "pending_message_ids": []any{}, "observed_at": f["now"]}
				f["redis_observations"] = append(f["redis_observations"].([]any), obs)
			case "broken_dependency":
				edges[1].(map[string]any)["source"] = "missing_node"
			case "duplicate_edge":
				edges[1].(map[string]any)["id"] = edges[0].(map[string]any)["id"]
			case "cycle":
				edges[0].(map[string]any)["source"] = edges[0].(map[string]any)["target"]
			case "duplicate_input":
				e := historyTestCopy(t, edges[0]).(map[string]any)
				e["id"] = "extra"
				job["pipeline_snapshot"].(map[string]any)["edges"] = append(edges, e)
			case "source_path":
				historyTestFirst(f, "artifacts")["storage_path"] = "artifacts/other.mp4"
			case "source_claim":
				historyTestFirst(f, "node_executions")["worker_registration_id"] = historyTestFirst(f, "worker_registrations")["id"]
			case "grant_image":
				historyTestFirst(f, "worker_admission_grants")["image_identity"] = "sha256:" + strings.Repeat("f", 64)
			case "transition":
				a := historyTestFirst(f, "production_tasks")["transition_history_json"].([]any)
				a[len(a)-1].(map[string]any)["from"] = "uploaded_private"
			case "invented_cancel":
				retry["cancelled_at"] = retry["acknowledged_at"]
			case "never_attempted":
				never := historyTestFind(t, f, "worker_task_dispatches", "resolution_state", "cancelled")
				never["delivery_attempted_at"] = never["cancelled_at"]
			case "missing_column":
				delete(retry, "payload_json")
			case "orphan_delivery":
				d := historyTestCopy(t, historyTestFirst(f, "registered_worker_event_deliveries")).(map[string]any)
				d["id"] = historyTestUID(401)
				d["receipt_id"] = historyTestUID(402)
				r["registered_worker_event_deliveries"] = append(r["registered_worker_event_deliveries"].([]any), d)
			case "dispatch_config":
				retry["payload_json"].(map[string]any)["config"] = `{"unsafe":true}`
				retry["payload_sha256"] = historyTestHash(t, retry["payload_json"])
			case "dispatch_input":
				retry["payload_json"].(map[string]any)["input_artifacts"] = `{}`
				retry["payload_sha256"] = historyTestHash(t, retry["payload_json"])
			case "unresolved_emission":
				historyTestFirst(f, "worker_event_emissions")["emission_state"] = "prepared"
			case "registration_missing":
				r["worker_registrations"] = []any{}
			case "grant_missing":
				r["worker_admission_grants"] = []any{}
			case "registration_fingerprint":
				historyTestFirst(f, "worker_registrations")["database_fingerprint"] = "unknown"
			case "upload_privacy":
				historyTestFirst(f, "youtube_upload_operations")["privacy"] = "public"
			case "operation_attempt":
				historyTestFirst(f, "youtube_upload_operations")["request_attempted_at"] = f["now"]
			case "claim_worker":
				att["worker_id"] = "foreign-consumer"
			case "artifact_orphan":
				historyTestFirst(f, "artifacts")["node_execution_id"] = historyTestUID(999)
			case "ack_time":
				receipt["source_task_acknowledged_at"] = f["now"]
			case "receipt_payload_hash":
				receipt["payload_sha256"] = strings.Repeat("f", 64)
			case "source_message":
				receipt["source_task_message_id"] = "999-0"
			case "receipt_stream":
				receipt["redis_stream"] = "vp:events:foreign"
			case "retired_target":
				historyTestManifest(f)["target_account_id"] = c["legacy_account_id"]
			case "retired_uc":
				c["canonical_platform_channel_id"] = "UC" + strings.Repeat("a", 22)
			}
			historyTestReseal(t, f)
			historyTestReason(t, f, "")
		})
	}
}

func historyTestReceiptAuthorizedACK(t *testing.T) map[string]any {
	t.Helper()
	f := historyGolden(t, "retired_unassigned")
	att := historyTestFirst(f, "worker_task_delivery_attestations")
	r := historyTestFind(t, f, "registered_worker_event_receipts", "source_task_attestation_id", att["id"])
	d := historyTestFind(t, f, "worker_task_dispatches", "dispatch_key", att["dispatch_key"])
	att["ack_event_emission_id"] = nil
	ack := historyISO(historyAt(t, r["applied_at"]).Add(10 * time.Second))
	att["acknowledged_at"] = ack
	d["acknowledged_at"] = ack
	r["source_task_acknowledged_at"] = ack
	historyTestReseal(t, f)
	return f
}
func TestOwnedHistoryReceiptAuthorizedACK(t *testing.T) {
	f := historyTestReceiptAuthorizedACK(t)
	result := historyTestAssess(t, f)
	if result.BlockReason != nil {
		t.Fatal(*result.BlockReason)
	}
	if len(result.AccountIDs) != 0 || len(result.CompletedItemIDs) != 0 || result.WaitReason != nil || result.Classifications[0].PlatformChannelID != nil {
		t.Fatal("retirement became authority/effect")
	}
	for _, bad := range []string{"missing_receipt", "unapplied_receipt", "foreign_receipt", "wrong_emission", "premature_ack"} {
		t.Run(bad, func(t *testing.T) {
			f := historyTestReceiptAuthorizedACK(t)
			rows := historyTestRows(f)
			att := historyTestFirst(f, "worker_task_delivery_attestations")
			r := historyTestFirst(f, "registered_worker_event_receipts")
			switch bad {
			case "missing_receipt":
				rows["registered_worker_event_receipts"] = rows["registered_worker_event_receipts"].([]any)[1:]
			case "unapplied_receipt":
				r["application_state"] = "accepted"
				r["applied_at"] = nil
			case "foreign_receipt":
				r["source_task_attestation_id"] = rows["worker_task_delivery_attestations"].([]any)[1].(map[string]any)["id"]
			case "wrong_emission":
				att["ack_event_emission_id"] = rows["worker_event_emissions"].([]any)[1].(map[string]any)["id"]
			case "premature_ack":
				ack := historyISO(historyAt(t, r["applied_at"]).Add(-time.Second))
				att["acknowledged_at"] = ack
				historyTestFind(t, f, "worker_task_dispatches", "dispatch_key", att["dispatch_key"])["acknowledged_at"] = ack
				r["source_task_acknowledged_at"] = ack
			}
			historyTestReseal(t, f)
			historyTestReason(t, f, "owned_history_retired_receipt")
		})
	}
}

func TestOwnedHistoryRetiredBookkeepingAndFreshRedis(t *testing.T) {
	f := historyGolden(t, "retired_unassigned")
	before := historyTestAssess(t, f)
	for _, v := range historyTestRows(f)["worker_registrations"].([]any) {
		r := v.(map[string]any)
		r["heartbeat_at"] = f["now"]
		r["lease_expires_at"] = historyISO(historyAt(t, f["now"]).Add(time.Hour))
		r["status"] = "revoked"
		r["revoked_at"] = f["now"]
		r["revoke_reason"] = "native supersession"
		r["superseded_by"] = historyTestUID(999)
	}
	for _, v := range historyTestRows(f)["worker_admission_grants"].([]any) {
		r := v.(map[string]any)
		r["state"] = "revoked"
		r["revoked_at"] = f["now"]
		r["revoke_reason"] = "native supersession"
		r["updated_at"] = f["now"]
	}
	after := historyTestAssess(t, f)
	if string(mustHistoryAssessmentJSON(t, before)) != string(mustHistoryAssessmentJSON(t, after)) {
		t.Fatal("bookkeeping altered retained execution proof")
	}
	for _, v := range f["redis_observations"].([]any) {
		v.(map[string]any)["observed_at"] = historyISO(historyAt(t, f["now"]).Add(-60 * time.Second))
	}
	if r := historyTestAssess(t, f); r.BlockReason != nil {
		t.Fatal(*r.BlockReason)
	}
	// The certificate timestamp is valid, but never replaces omitted fresh Redis evidence.
	f["redis_observations"] = []any{}
	historyTestReason(t, f, "owned_history_retired_redis_missing")
}

func TestOwnedHistoryFrozenRegistryRouting(t *testing.T) {
	for _, name := range []string{"subtitle", "speech_to_subtitle", "subtitle_translate", "subtitle_to_speech"} {
		if got := historyWorkerType(name); got != "ffmpeg" {
			t.Errorf("%s route %q, want ffmpeg", name, got)
		}
	}
	if historyWorkerType("caller-defined") != "" {
		t.Fatal("unknown routing accepted")
	}
}

func TestOwnedHistoryNativeReceiptTimeRepresentations(t *testing.T) {
	for _, style := range []string{"space", "offset", "basic_offset", "naive"} {
		t.Run(style, func(t *testing.T) {
			f := historyGolden(t, "retired_unassigned")
			r := historyTestFirst(f, "registered_worker_event_receipts")
			e := historyTestFind(t, f, "worker_event_emissions", "source_task_attestation_id", r["source_task_attestation_id"])
			p := r["payload_json"].(map[string]any)
			at := historyAt(t, p["started_at"])
			switch style {
			case "space":
				p["started_at"] = strings.Replace(p["started_at"].(string), "T", " ", 1)
			case "offset":
				p["started_at"] = at.In(time.FixedZone("native", 3600)).Format(time.RFC3339Nano)
			case "basic_offset":
				p["started_at"] = at.Format("2006-01-02T15:04:05-0700")
			case "naive":
				p["started_at"] = at.Format("2006-01-02T15:04:05")
			}
			e["payload_json"] = historyTestCopy(t, p)
			sha := historyTestHash(t, p)
			r["payload_sha256"] = sha
			e["payload_sha256"] = sha
			for _, v := range historyTestRows(f)["registered_worker_event_deliveries"].([]any) {
				d := v.(map[string]any)
				if d["receipt_id"] == r["id"] {
					d["payload_sha256"] = sha
					for _, v := range f["redis_observations"].([]any) {
						o := v.(map[string]any)
						if o["kind"] == "event" && o["message_id"] == d["message_id"] {
							o["payload_sha256"] = sha
						}
					}
				}
			}
			historyTestReseal(t, f)
			result := historyTestAssess(t, f)
			if style == "naive" {
				if result.BlockReason == nil || *result.BlockReason != "owned_history_invalid" {
					t.Fatal("naive receipt start accepted", result)
				}
			} else if result.BlockReason != nil {
				t.Fatal(*result.BlockReason)
			}
		})
	}
}

func TestOwnedHistoryLeaseEpochComparisonDoesNotSaturate(t *testing.T) {
	for _, pair := range [][2]string{{"4611686018427387905", "4611686018427387904"}, {"9223372036854775807", "9223372036854775806"}, {"1000000000000000000000000000000", "999999999999999999999999999999"}} {
		t.Run(pair[0], func(t *testing.T) {
			f := historyGolden(t, "retired_unassigned")
			att := historyTestFirst(f, "worker_task_delivery_attestations")
			epoch := json.Number(pair[0])
			r := historyTestFind(t, f, "registered_worker_event_receipts", "source_task_attestation_id", att["id"])
			e := historyTestFind(t, f, "worker_event_emissions", "source_task_attestation_id", att["id"])
			n := historyTestFind(t, f, "node_executions", "id", att["node_execution_id"])
			reg := historyTestFind(t, f, "worker_registrations", "id", att["worker_registration_id"])
			for _, row := range []map[string]any{att, r, e, n} {
				row["worker_lease_epoch"] = epoch
			}
			reg["lease_epoch"] = epoch
			p := r["payload_json"].(map[string]any)
			p["worker_lease_epoch"] = string(epoch)
			e["payload_json"] = historyTestCopy(t, p)
			sha := historyTestHash(t, p)
			r["payload_sha256"] = sha
			e["payload_sha256"] = sha
			for _, v := range historyTestRows(f)["registered_worker_event_deliveries"].([]any) {
				d := v.(map[string]any)
				if d["receipt_id"] == r["id"] {
					d["payload_sha256"] = sha
					for _, v := range f["redis_observations"].([]any) {
						o := v.(map[string]any)
						if o["kind"] == "event" && o["message_id"] == d["message_id"] {
							o["payload_sha256"] = sha
						}
					}
				}
			}
			historyTestReseal(t, f)
			if result := historyTestAssess(t, f); result.BlockReason != nil {
				t.Fatal(*result.BlockReason)
			}
			reg["lease_epoch"] = json.Number(pair[1])
			historyTestReseal(t, f)
			historyTestReason(t, f, "owned_history_retired_receipt")
		})
	}
}

func TestOwnedHistoryMalformedUnlinkedEventCannotDisappear(t *testing.T) {
	f := historyGolden(t, "retired_unassigned")
	e := historyTestCopy(t, historyTestFirst(f, "worker_event_emissions")).(map[string]any)
	e["id"] = historyTestUID(901)
	e["job_id"] = historyTestUID(902)
	e["node_execution_id"] = historyTestUID(903)
	e["source_task_attestation_id"] = historyTestUID(904)
	e["payload_json"] = []any{}
	rows := historyTestRows(f)
	rows["worker_event_emissions"] = append(rows["worker_event_emissions"].([]any), e)
	historyTestReason(t, f, "owned_history_invalid")
}
