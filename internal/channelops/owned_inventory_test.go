package channelops

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type ownedTestPDS func(context.Context, PDSDecisionRequest) (PDSDecision, error)

func (f ownedTestPDS) Decide(ctx context.Context, request PDSDecisionRequest) (PDSDecision, error) {
	return f(ctx, request)
}

func ownedTestID(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }

func ownedTestHash(t *testing.T, value any) string {
	t.Helper()
	hash, err := ownedHash(value)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func ownedTestFixture(t *testing.T) (ChannelProfileRow, ownedInventoryData, time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 11, 8, 0, 0, 0, time.UTC)
	id := ownedTestID(1)
	channel := ChannelProfileRow{ID: ownedTestID(2), Enabled: true, OwnedSeedInventoryID: &id, OwnedInventoryActive: true, TickIntervalMinutes: 1, ConfigVersion: 1}
	data := ownedInventoryData{
		Inventory: map[string]any{"id": id, "channel_profile_id": channel.ID, "topic_lane_id": ownedTestID(3), "lane_format_id": ownedTestID(4), "target_account_id": ownedTestID(5), "platform_channel_id": "UCaaaaaaaaaaaaaaaaaaaaaa", "privacy": "unlisted", "max_admissions": 7, "minimum_interval_seconds": 86400, "state": "approved", "approved_at": "2026-09-11T07:00:00+00:00", "approved_by": "operator", "approval_reference": "review:seven", "revoked_at": nil, "succession_released_at": nil, "starts_at": "2026-09-11T07:00:00+00:00", "expires_at": "2026-09-18T07:00:00+00:00"},
		Bindings: map[string]any{
			"channel": map[string]any{"id": channel.ID, "config_version": 1, "name": "\u4e2d\u6587", "positioning": "owned", "language": "zh", "default_aspect_ratio": "9:16", "risk_policy_json": map[string]any{}, "content_mix_policy_json": map[string]any{}, "cadence_policy_json": map[string]any{}, "alert_policy_json": map[string]any{}, "enabled": true, "dry_run": false},
			"account": map[string]any{"id": ownedTestID(5), "channel_profile_id": channel.ID, "platform": "youtube", "platform_account_id": "UCaaaaaaaaaaaaaaaaaaaaaa", "credential_ref": "opaque/test", "platform_specific_config_json": map[string]any{}, "default_privacy": "unlisted", "external_asset_auto_publish": false, "enabled": true, "paused_until": nil},
			"lane":    map[string]any{"id": ownedTestID(3), "channel_profile_id": channel.ID, "name": "owned", "description": "seven", "weight": 1.0, "keywords_json": []any{}, "negative_keywords_json": []any{}, "min_posts_per_week": 1, "max_posts_per_day": 1, "max_consecutive_streak": 7, "cooldown_after_post_minutes": 0, "enabled": true, "paused_until": nil},
			"format":  map[string]any{"id": ownedTestID(4), "topic_lane_id": ownedTestID(3), "format_key": "owned", "enabled": true, "weight": 1.0, "target_duration_sec": 30, "template_pool_json": []any{}, "source_platforms_json": []any{}, "default_publish_visibility": "unlisted"},
		},
		AccountIDs: []string{ownedTestID(5)}, RuntimeOpen: true,
	}
	entries := []any{}
	for n := 1; n <= 7; n++ {
		assetID, seedID := ownedTestID(100+n), ownedTestID(200+n)
		seed := map[string]any{"id": seedID, "channel_profile_id": channel.ID, "topic_lane_id": ownedTestID(3), "target_account_id": ownedTestID(5), "prompt": fmt.Sprintf("owned %d", n), "title_seed": "owned", "source_policy": "owned_only", "source_platforms_json": []any{}, "material_library_ids_json": []any{}, "constraints_json": map[string]any{"input_asset_id": assetID, "source_strategy": "input_video", "planning_mode": "template"}}
		seedHash := ownedTestHash(t, seed)
		seed["status"] = "active"
		asset := map[string]any{"id": assetID, "storage_backend": "local", "storage_path": "assets/" + assetID + ".mp4", "file_size": 100, "mime_type": "video/mp4", "media_info": map[string]any{"license": "owned", "provenance": "generated", "duration": 30}}
		descriptor := map[string]any{"id": assetID, "storage_backend": "local", "storage_path": asset["storage_path"], "file_size": 100, "mime_type": "video/mp4", "media_info_sha256": ownedTestHash(t, map[string]any{"duration": 30})}
		provenance := map[string]any{"rights": "owned", "provenance": "generated", "evidence_reference": "record:owned", "evidence_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte("evidence"))), "attestation": "owned generated"}
		entry := map[string]any{"id": ownedTestID(300 + n), "ordinal": n, "asset_id": assetID, "manual_seed_id": seedID, "content_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprint(n)))), "byte_size": 100, "storage_descriptor": descriptor, "provenance_evidence": provenance, "provenance_sha256": ownedTestHash(t, provenance), "seed_sha256": seedHash, "prompt": seed["prompt"], "title_seed": seed["title_seed"]}
		item := map[string]any{"id": entry["id"], "inventory_id": id, "platform_channel_id": data.Inventory["platform_channel_id"], "ordinal": n, "manual_seed_id": seedID, "asset_id": assetID, "content_sha256": entry["content_sha256"], "byte_size": 100, "storage_descriptor_json": descriptor, "provenance_evidence_json": provenance, "provenance_sha256": entry["provenance_sha256"], "seed_sha256": seedHash, "state": "unused", "production_task_id": nil, "consumed_at": nil}
		data.Items = append(data.Items, ownedInventoryInput{Item: item, Seed: seed, Asset: asset})
		entries = append(entries, entry)
	}
	manifest := map[string]any{"version": 1, "inventory_id": id, "channel_profile_id": channel.ID, "topic_lane_id": ownedTestID(3), "lane_format_id": ownedTestID(4), "target_account_id": ownedTestID(5), "platform_channel_id": data.Inventory["platform_channel_id"], "starts_at": data.Inventory["starts_at"], "expires_at": data.Inventory["expires_at"], "privacy": "unlisted", "max_admissions": 7, "minimum_interval_seconds": 86400, "tick_interval_minutes": 1, "configuration_sha256": ownedTestHash(t, data.Bindings), "entries": entries}
	data.Inventory["manifest_json"] = manifest
	data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
	return channel, data, now
}

func TestOwnedSelectsExactLowestUnusedAndStableCandidate(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	state := assessOwnedInventory(channel, data, now)
	if state.HoldReason != "" || state.SkipReason != "" || state.Candidate == nil {
		t.Fatalf("state = %+v", state)
	}
	if got, want := state.Candidate.CandidateID, "owned_inventory:"+ownedTestID(1)+":"+ownedTestID(301); got != want {
		t.Fatalf("candidate = %s, want %s", got, want)
	}
	if state.Candidate.Seed == nil || state.Candidate.Seed.ID != ownedTestID(201) || state.Candidate.Account.ID != ownedTestID(5) || state.Candidate.owned == nil {
		t.Fatal("exact typed authority missing")
	}
	if got := firstString(state.Candidate.ConstraintsJSON, "input_asset_id"); got != ownedTestID(101) {
		t.Fatalf("input asset = %s", got)
	}
	if key := ownedPlatformKey("UCaaaaaaaaaaaaaaaaaaaaaa"); key != -818065374254745958 {
		t.Fatalf("Python advisory key mismatch: %d", key)
	}
}

func TestOwnedAdmissionRejectsImmutableBindingDrift(t *testing.T) {
	cases := map[string]func(*ownedInventoryData){
		"cardinality": func(d *ownedInventoryData) { d.Items = d.Items[:6] },
		"alias":       func(d *ownedInventoryData) { d.AccountIDs = append(d.AccountIDs, ownedTestID(999)) },
		"config":      func(d *ownedInventoryData) { d.Bindings["channel"].(map[string]any)["name"] = "changed" },
		"seed":        func(d *ownedInventoryData) { d.Items[0].Seed["prompt"] = "changed" },
		"asset":       func(d *ownedInventoryData) { d.Items[0].Asset["storage_path"] = "assets/changed.mp4" },
		"privacy":     func(d *ownedInventoryData) { d.Inventory["privacy"] = "private" },
		"manifest":    func(d *ownedInventoryData) { d.Inventory["manifest_sha256"] = "0" },
		"unsigned":    func(d *ownedInventoryData) { d.Inventory["approved_by"] = "" },
		"released":    func(d *ownedInventoryData) { d.Inventory["succession_released_at"] = "2026-09-11T07:30:00Z" },
		"recycled":    func(d *ownedInventoryData) { d.Items[0].Item["production_task_id"] = ownedTestID(900) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			mutate(&data)
			state := assessOwnedInventory(channel, data, now)
			if state.HoldReason == "" || state.Candidate != nil {
				t.Fatalf("did not hold invalid %s", name)
			}
		})
	}
}

func TestOwnedWindowAndRuntimeGates(t *testing.T) {
	for _, mode := range []string{"early", "expired", "closed", "guarded", "busy", "terminal"} {
		t.Run(mode, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			switch mode {
			case "early":
				now = now.Add(-2 * time.Hour)
			case "expired":
				now = now.Add(168 * time.Hour)
			case "closed":
				data.RuntimeOpen = false
			case "guarded":
				data.RuntimeGuarded = true
			case "busy":
				data.Busy = true
			case "terminal":
				data.Inventory["state"] = "held"
			}
			state := assessOwnedInventory(channel, data, now)
			if state.Candidate != nil {
				t.Fatalf("%s admitted a candidate", mode)
			}
			if mode == "expired" && state.HoldReason == "" {
				t.Fatal("expiry did not close intake")
			}
		})
	}
}

func TestOwnedCanonicalMatchesPythonApprovalFixtures(t *testing.T) {
	_, data, _ := ownedTestFixture(t)
	// Full selected-field configuration, produced by the actual Python service.sha256().
	if got := ownedTestHash(t, data.Bindings); got != "94d8b5493f2198ffd802bcd45a1f38ab90d919a3535cba93df41444575f5ba57" {
		t.Fatalf("Python configuration digest mismatch: %s", got)
	}
	// Expected bytes and hashes are from the actual Python approval canonical()/sha256().
	cases := []struct{ raw, canonical, hash string }{
		{`{"title":"\u4e2d\u6587\u6807\u9898","ascii":"<>&/\\\"","astral":"\ud83d\ude42","controls":"\n\t\u007f"}`,
			`{"ascii":"<>&/\\\"","astral":"\ud83d\ude42","controls":"\n\t\u007f","title":"\u4e2d\u6587\u6807\u9898"}`,
			"9282d7310d2a473eba2ea5e477de3201c2ee4255efd672a565373b3da8887ef7"},
		{`{"float":1.0,"zero":-0.0,"small":0.00001,"plain":0.0001,"large":1e16,"boundary":1e15,"tiny":1e-7,"integer":9007199254740993}`,
			`{"boundary":1000000000000000.0,"float":1.0,"integer":9007199254740993,"large":1e+16,"plain":0.0001,"small":1e-05,"tiny":1e-07,"zero":-0.0}`,
			"34c7ed5a8ca5f9516aeffc2378d14758107b6846db962ce6b0d4d1a2615d83da"},
		{`{"starts_at":"2026-09-11T08:00:00+00:00","expires_at":"2026-09-18T08:00:00.000001+00:00","items":[null,true,false,3.14]}`,
			`{"expires_at":"2026-09-18T08:00:00.000001+00:00","items":[null,true,false,3.14],"starts_at":"2026-09-11T08:00:00+00:00"}`,
			"75ed65971df33fc4d4a8af28c514cfe8a51637e5e02977f2f5cd9ca342b07156"},
	}
	for _, tc := range cases {
		value, err := ownedDecode([]byte(tc.raw))
		if err != nil {
			t.Fatal(err)
		}
		actual, err := ownedCanonical(value)
		if err != nil || string(actual) != tc.canonical {
			t.Fatalf("canonical = %s, %v; want %s", actual, err, tc.canonical)
		}
		hash, err := ownedHash(value)
		if err != nil || hash != tc.hash {
			t.Fatalf("hash = %s, %v; want %s", hash, err, tc.hash)
		}
	}
}

func TestOwnedCanonicalRejectsAmbiguousJSON(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `{} {}`, `{"x":NaN}`, `{"x":"\ud800"}`} {
		if _, err := ownedDecode([]byte(raw)); err == nil {
			t.Fatalf("accepted ambiguous input %s", raw)
		}
	}
	if _, err := ownedCanonical(json.Number("1e999")); err == nil {
		t.Fatal("accepted non-finite number")
	}
}

func TestOwnedPointerSuppressesAllOrdinaryCandidates(t *testing.T) {
	id := "00000000-0000-0000-0000-000000000001"
	channel := ChannelProfileRow{ID: "channel", OwnedSeedInventoryID: &id}
	lane := TopicLaneRow{ID: "lane", Enabled: true, MaxPostsPerDay: 3}
	account := PublishingAccountRow{ID: "account", Enabled: true}
	format := LaneFormatRow{ID: "format", Enabled: true}
	seed := ManualSeedRow{ID: "seed", Prompt: "ordinary seed", SourcePolicy: "owned_only"}
	signal := DiscoverySignalRow{ID: "signal", Title: "discovery"}
	for _, active := range []bool{false, true} {
		channel.OwnedInventoryActive = active
		got := BuildTickCandidates(channel, []TopicLaneRow{lane}, []PublishingAccountRow{account}, []ManualSeedRow{seed}, []DiscoverySignalRow{signal}, map[string][]LaneFormatRow{"lane": {format}}, "bucket")
		if len(got) != 0 {
			t.Fatalf("inventory pointer allowed %d fallback candidates", len(got))
		}
	}
}

func TestOwnedProfileMinuteBucketKeepsOrdinaryFloor(t *testing.T) {
	now := time.Date(2026, 9, 11, 8, 7, 59, 0, time.UTC)
	id := "00000000-0000-0000-0000-000000000001"
	channel := ChannelProfileRow{Enabled: true, TickIntervalMinutes: 1}
	if got := channelSchedulerBucket(channel, now); got != "2026-09-11-08-00" {
		t.Fatalf("ordinary bucket = %s", got)
	}
	channel.OwnedSeedInventoryID = &id
	if ChannelDueForTick(channel, now) {
		t.Fatal("inactive inventory is due")
	}
	channel.OwnedInventoryActive = true
	if !ChannelDueForTick(channel, now) {
		t.Fatal("active inventory not due")
	}
	if got := channelSchedulerBucket(channel, now); got != "2026-09-11-08-07" {
		t.Fatalf("owned bucket = %s", got)
	}
}

func ownedTestCompletedHistory(t *testing.T, data *ownedInventoryData, at time.Time) {
	t.Helper()
	taskID, jobID, nodeID, pubID := ownedTestID(501), ownedTestID(502), ownedTestID(503), ownedTestID(504)
	item := data.Items[0].Item
	item["state"], item["production_task_id"], item["consumed_at"] = "reserved", taskID, ownedISO(at.Add(-time.Hour))
	data.Items[0].Seed["status"] = "exhausted"
	receipt := map[string]any{"video_id": "abcdefghijk", "privacy": "private", "title": "owned", "url": "https://www.youtube.com/watch?v=abcdefghijk", "tags": []any{}, "quota_estimate": 1600}
	op := map[string]any{"id": ownedTestID(505), "production_task_id": taskID, "job_id": jobID, "node_execution_id": nodeID, "input_artifact_id": ownedTestID(506), "content_sha256": fmt.Sprintf("%x", sha256.Sum256([]byte("render"))), "status": "succeeded", "privacy": "private", "title": "owned", "manager_task_id": ownedTestID(507), "platform_video_id": "abcdefghijk", "receipt_json": receipt, "error_message": nil, "request_attempted_at": ownedISO(at.Add(-time.Minute)), "completed_at": ownedISO(at)}
	pub := map[string]any{"id": pubID, "production_task_id": taskID, "account_id": ownedTestID(5), "platform": "youtube", "platform_content_id": "abcdefghijk", "desired_privacy": "unlisted", "current_privacy": "unlisted", "publish_status": "uploaded", "public_at": nil, "uploaded_at": ownedISO(at), "scheduled_publish_at": ownedISO(at)}
	queues := []any{}
	queue := func(id int, kind, key string, payload map[string]any, run time.Time) map[string]any {
		return map[string]any{"id": ownedTestID(id), "channel_profile_id": ownedTestID(2), "kind": kind, "idempotency_key": key, "payload_json": payload, "status": "succeeded", "run_after": ownedISO(run), "locked_by": nil, "locked_at": nil, "attempt_count": 1, "last_error": nil, "dead_letter_at": nil}
	}
	promote := queue(508, QueuePromotePublication, "promote_publication:"+pubID, map[string]any{"publication_id": pubID, "target_visibility": "unlisted", "channel_profile_id": ownedTestID(2)}, at)
	reconcile := queue(509, QueueReconcilePublication, "reconcile_publication:"+pubID+":"+at.Format(time.RFC3339), map[string]any{"publication_id": pubID}, at.Add(30*time.Minute))
	reconcile["parent_queue_item_id"] = promote["id"]
	queues = append(queues, promote, reconcile)
	metrics, feedback := []any{}, []any{}
	for i, plan := range BuildMetricSchedulePlans(pubID, at) {
		id := ownedTestID(520 + i)
		m := map[string]any{"id": id, "publication_id": pubID, "snapshot_stage": plan.Stage, "effective_start_at": ownedISO(at), "due_at": ownedISO(plan.DueAt), "grace_until": ownedISO(plan.GraceUntil), "status": "pending", "attempt_count": 0, "last_error_code": nil, "completed_at": nil}
		q := queue(530+i, QueueCollectMetrics, plan.IdempotencyKey, map[string]any{"publication_id": pubID, "metric_schedule_id": id, "snapshot_stage": plan.Stage}, plan.DueAt)
		q["status"], q["attempt_count"] = "queued", 0
		if plan.DueAt.Sub(at) <= 24*time.Hour {
			m["status"], m["attempt_count"], m["completed_at"] = "succeeded", 1, ownedISO(plan.DueAt)
			q["status"], q["attempt_count"] = "succeeded", 1
			feedback = append(feedback, map[string]any{"publication_id": pubID, "snapshot_stage": plan.Stage})
		}
		metrics, queues = append(metrics, m), append(queues, q)
	}
	data.Tasks = []map[string]any{{
		"task":       map[string]any{"id": taskID, "channel_profile_id": ownedTestID(2), "target_account_id": ownedTestID(5), "manual_seed_id": ownedTestID(201), "job_id": jobID, "state": "scheduled", "retry_count": 0, "failure_reason": nil, "blocked_by_guard": nil},
		"operations": []any{op}, "job": map[string]any{"id": jobID, "status": "SUCCEEDED", "completed_at": ownedISO(at), "error_message": nil},
		"nodes":        []any{map[string]any{"id": nodeID, "job_id": jobID, "node_type": "youtube_upload", "status": "SUCCEEDED", "output_artifact_id": ownedTestID(510), "completed_at": ownedISO(at), "error_message": nil, "input_artifact_ids": []any{ownedTestID(506)}}},
		"artifacts":    []any{map[string]any{"id": ownedTestID(510), "job_id": jobID, "node_execution_id": nodeID, "media_info": map[string]any{"youtube": receipt}}},
		"publications": []any{pub}, "queues": queues, "metrics": metrics, "feedback": feedback,
	}}
}

func TestOwnedRollingCompletionFloorAndSettlement(t *testing.T) {
	for _, delay := range []time.Duration{-time.Second, 0, time.Second, 2 * time.Hour} {
		t.Run(delay.String(), func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			completed := now.Add(24 * time.Hour)
			ownedTestCompletedHistory(t, &data, completed)
			if delay < 0 {
				m := ownedMap(ownedArray(data.Tasks[0]["metrics"])[2])
				m["status"], m["attempt_count"], m["completed_at"] = "pending", 0, nil
				q := ownedMap(ownedArray(data.Tasks[0]["queues"])[4])
				q["status"], q["attempt_count"] = "queued", 0
				data.Tasks[0]["feedback"] = ownedArray(data.Tasks[0]["feedback"])[:2]
			}
			state := assessOwnedInventory(channel, data, completed.Add(24*time.Hour+delay))
			if state.HoldReason != "" || len(state.CompleteItemIDs) != 1 {
				t.Fatalf("normal receipt failed settlement: %+v", state)
			}
			if (state.Candidate != nil) != (delay >= 0) {
				t.Fatalf("24h floor: %+v", state)
			}
			if state.Candidate != nil && state.Candidate.Seed.ID != ownedTestID(202) {
				t.Fatal("did not select next ordinal")
			}
		})
	}
}

func TestOwnedHistoryFailsClosed(t *testing.T) {
	mutations := map[string]func(*ownedInventoryData){
		"orphan":       func(d *ownedInventoryData) { d.UnknownOperation = true },
		"missing_task": func(d *ownedInventoryData) { d.Tasks = nil },
		"uncertain":    func(d *ownedInventoryData) { ownedMap(ownedArray(d.Tasks[0]["operations"])[0])["status"] = "uncertain" },
		"missing_attempt": func(d *ownedInventoryData) {
			delete(ownedMap(ownedArray(d.Tasks[0]["operations"])[0]), "request_attempted_at")
		},
		"bad_receipt": func(d *ownedInventoryData) {
			ownedMap(ownedArray(d.Tasks[0]["operations"])[0])["platform_video_id"] = "other_video"
		},
		"failed_job": func(d *ownedInventoryData) { ownedMap(d.Tasks[0]["job"])["status"] = "FAILED" },
		"public": func(d *ownedInventoryData) {
			ownedMap(ownedArray(d.Tasks[0]["publications"])[0])["current_privacy"] = "public"
		},
		"failed_reconcile": func(d *ownedInventoryData) { ownedMap(ownedArray(d.Tasks[0]["queues"])[1])["status"] = "dead_letter" },
		"reconcile_parent": func(d *ownedInventoryData) {
			ownedMap(ownedArray(d.Tasks[0]["queues"])[1])["parent_queue_item_id"] = ownedTestID(999)
		},
		"missing_metrics": func(d *ownedInventoryData) { d.Tasks[0]["metrics"] = []any{} },
		"failed_metrics":  func(d *ownedInventoryData) { ownedMap(ownedArray(d.Tasks[0]["metrics"])[0])["status"] = "expired" },
		"no_feedback":     func(d *ownedInventoryData) { d.Tasks[0]["feedback"] = []any{} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestCompletedHistory(t, &data, now)
			mutate(&data)
			state := assessOwnedInventory(channel, data, now.Add(25*time.Hour))
			if state.Candidate != nil || state.HoldReason == "" {
				t.Fatalf("unsafe %s: %+v", name, state)
			}
		})
	}
}

func TestOwnedOutstandingDoesNotSpendAnotherItem(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	ownedTestCompletedHistory(t, &data, now)
	ownedMap(data.Tasks[0]["task"])["state"] = "selected"
	data.Tasks[0]["operations"], data.Tasks[0]["publications"], data.Tasks[0]["job"] = []any{}, []any{}, nil
	for _, later := range []time.Duration{0, time.Hour, 48 * time.Hour} {
		state := assessOwnedInventory(channel, data, now.Add(later))
		if state.Candidate != nil || state.SkipReason != "owned_inventory_outstanding" || state.HoldReason != "" {
			t.Fatalf("outstanding replay: %+v", state)
		}
	}
}

func TestOwnedPDSUnavailableAndFallbackCloseAdmission(t *testing.T) {
	for _, mode := range []string{"nil", "error", "empty", "block", "flag", "fallback", "dev", "allow"} {
		t.Run(mode, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			candidate := *assessOwnedInventory(channel, data, now).Candidate
			handler := HandlerService{}
			if mode != "nil" {
				handler.PDS = ownedTestPDS(func(_ context.Context, request PDSDecisionRequest) (PDSDecision, error) {
					if ownedMap(request.Context["owned_inventory"])["item_id"] != ownedTestID(301) {
						t.Fatal("missing policy binding")
					}
					if mode == "error" {
						return PDSDecision{}, errors.New("fake policy failure")
					}
					decision := PDSDecision{Verdict: mode, DecisionID: "fixture-decision", RulesVersion: "fixture-rules"}
					if mode == "empty" {
						decision.Verdict = ""
					}
					if mode == "fallback" {
						decision = failPolicyDecision("candidate_accept", "pds_unavailable")
					}
					if mode == "dev" {
						decision.Verdict = "allow"
						decision.Metadata = map[string]any{"warning": "dev_allow_all", "fail_policy": "allow"}
					}
					return decision, nil
				})
			}
			got, _, err := evaluateTickCandidatePolicy(context.Background(), channel, []TickCandidate{candidate}, handler)
			if err != nil || len(got) != 1 || got[0].Rejected != (mode != "allow") {
				t.Fatalf("policy %s admitted=%v err=%v", mode, !got[0].Rejected, err)
			}
		})
	}
}

func TestOwnedNotDueNeverCallsPDS(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	data.RuntimeOpen = false
	state := assessOwnedInventory(channel, data, now)
	var candidates []TickCandidate
	if state.Candidate != nil {
		candidates = append(candidates, *state.Candidate)
	}
	h := HandlerService{PDS: ownedTestPDS(func(context.Context, PDSDecisionRequest) (PDSDecision, error) {
		t.Fatal("not-due PDS called")
		return PDSDecision{}, nil
	})}
	if _, _, err := evaluateTickCandidatePolicy(context.Background(), channel, candidates, h); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedSnapshotPreservesManualConstraintsAndTypedReferences(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	candidate := *assessOwnedInventory(channel, data, now).Candidate
	snapshot := channelConfigSnapshot(channel, candidate)
	if !ownedEqual(ownedMap(snapshot["manual_seed"])["constraints_json"], data.Items[0].Seed["constraints_json"]) {
		t.Fatal("owned input constraints lost")
	}
	if ownedMap(snapshot["owned_inventory"])["manifest_sha256"] != data.Inventory["manifest_sha256"] {
		t.Fatal("manifest reference missing")
	}
	candidate.owned = nil
	if _, exists := channelConfigSnapshot(channel, candidate)["owned_inventory"]; exists {
		t.Fatal("ordinary manual seed got typed authority")
	}
}

func TestOwnedAllSevenConsumedNeverFallsBack(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	data.Inventory["state"] = "exhausted"
	state := assessOwnedInventory(channel, data, now)
	if state.Candidate != nil || state.SkipReason != "owned_inventory_terminal" {
		t.Fatal("terminal inventory admitted")
	}
}

func TestOwnedHistoryAcrossMidnightAndSlowCompletionDoesNotResetFloor(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	completed := now.Add(15*time.Hour + 59*time.Minute)
	ownedTestCompletedHistory(t, &data, completed)
	op := ownedMap(ownedArray(data.Tasks[0]["operations"])[0])
	op["request_attempted_at"] = ownedISO(completed.Add(-6 * time.Hour))
	for _, value := range ownedArray(data.Tasks[0]["metrics"]) {
		m := ownedMap(value)
		m["status"], m["attempt_count"], m["completed_at"] = "pending", 0, nil
	}
	for _, value := range ownedArray(data.Tasks[0]["queues"])[2:] {
		q := ownedMap(value)
		q["status"], q["attempt_count"] = "queued", 0
	}
	data.Tasks[0]["feedback"] = []any{}
	state := assessOwnedInventory(channel, data, completed.Add(31*time.Minute))
	if state.HoldReason != "" || state.SkipReason != "owned_inventory_cooldown" || state.Candidate != nil {
		t.Fatalf("midnight reset the floor: %+v", state)
	}
	ownedTestCompletedHistory(t, &data, completed)
	op = ownedMap(ownedArray(data.Tasks[0]["operations"])[0])
	op["request_attempted_at"] = ownedISO(completed.Add(-6 * time.Hour))
	m := ownedMap(ownedArray(data.Tasks[0]["metrics"])[2])
	m["status"], m["attempt_count"], m["completed_at"] = "pending", 0, nil
	q := ownedMap(ownedArray(data.Tasks[0]["queues"])[4])
	q["status"], q["attempt_count"] = "queued", 0
	data.Tasks[0]["feedback"] = ownedArray(data.Tasks[0]["feedback"])[:2]
	state = assessOwnedInventory(channel, data, completed.Add(24*time.Hour-time.Second))
	if state.HoldReason != "" || state.SkipReason != "owned_inventory_cooldown" || state.Candidate != nil {
		t.Fatal("attempt time bypassed delayed completion floor")
	}
}

func TestOwnedMetricsDueGraceAndQueueIdentity(t *testing.T) {
	for _, mode := range []string{"due_pending", "past_grace", "future_exact", "future_wrong_payload", "foreign_queue"} {
		t.Run(mode, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestCompletedHistory(t, &data, now)
			at := now.Add(24 * time.Hour)
			if mode == "due_pending" || mode == "past_grace" {
				m := ownedMap(ownedArray(data.Tasks[0]["metrics"])[2])
				m["status"], m["attempt_count"], m["completed_at"] = "pending", 0, nil
				q := ownedMap(ownedArray(data.Tasks[0]["queues"])[4])
				q["status"], q["attempt_count"] = "queued", 0
				data.Tasks[0]["feedback"] = ownedArray(data.Tasks[0]["feedback"])[:2]
				if mode == "past_grace" {
					at = now.Add(30 * time.Hour)
				}
			}
			future := ownedMap(ownedArray(data.Tasks[0]["queues"])[6])
			raw, _ := json.Marshal(future)
			value, _ := ownedDecode(raw)
			data.Queues = []map[string]any{ownedMap(value)}
			if mode == "future_wrong_payload" {
				ownedMap(data.Queues[0]["payload_json"])["publication_id"] = ownedTestID(999)
			}
			if mode == "foreign_queue" {
				data.Queues[0]["kind"] = QueueExecuteTask
			}
			safe := ownedQueuesSafe(channel.ID, data, at)
			state := assessOwnedInventory(channel, data, at)
			switch mode {
			case "due_pending":
				if state.Candidate != nil || state.SkipReason != "owned_inventory_metrics_pending" {
					t.Fatal("due feedback was skipped")
				}
			case "past_grace":
				if state.HoldReason == "" {
					t.Fatal("missing feedback past grace not held")
				}
			case "future_exact":
				if !safe || state.Candidate == nil {
					t.Fatal("future feedback blocked admission")
				}
			default:
				if safe {
					t.Fatal("unrelated/changed active queue accepted")
				}
			}
		})
	}
}

func TestOwnedSeventhOrdinalAfterSixSettledDailyAttempts(t *testing.T) {
	channel, data, start := ownedTestFixture(t)
	now := start.Add(6 * 24 * time.Hour)
	for ordinal := 1; ordinal <= 6; ordinal++ {
		_, previous, _ := ownedTestFixture(t)
		at := start.Add(time.Duration(ordinal-1) * 24 * time.Hour)
		ownedTestCompletedHistory(t, &previous, at)
		history := previous.Tasks[0]
		for i, value := range ownedArray(history["metrics"]) {
			m := ownedMap(value)
			due, _ := ownedTime(m["due_at"])
			if due.After(now) {
				continue
			}
			m["status"], m["attempt_count"], m["completed_at"] = "succeeded", 1, ownedISO(due)
			q := ownedMap(ownedArray(history["queues"])[i+2])
			q["status"], q["attempt_count"] = "succeeded", 1
		}
		feedback := []any{}
		for _, value := range ownedArray(history["metrics"]) {
			m := ownedMap(value)
			if m["status"] == "succeeded" {
				feedback = append(feedback, map[string]any{"publication_id": m["publication_id"], "snapshot_stage": m["snapshot_stage"]})
			}
		}
		history["feedback"] = feedback
		raw, err := json.Marshal(history)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		text = strings.ReplaceAll(text, "abcdefghijk", fmt.Sprintf("video%06d", ordinal))
		text = strings.ReplaceAll(text, fmt.Sprintf("%x", sha256.Sum256([]byte("render"))), fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("render%d", ordinal)))))
		for id := 501; id <= 534; id++ {
			text = strings.ReplaceAll(text, ownedTestID(id), ownedTestID(id+1000*ordinal))
		}
		text = strings.ReplaceAll(text, ownedTestID(201), ownedTestID(200+ordinal))
		value, err := ownedDecode([]byte(text))
		if err != nil {
			t.Fatal(err)
		}
		data.Tasks = append(data.Tasks, ownedMap(value))
		item := data.Items[ordinal-1].Item
		item["state"], item["production_task_id"], item["consumed_at"], item["completed_at"] = "completed", ownedTestID(501+1000*ordinal), ownedISO(at.Add(-time.Hour)), ownedISO(at.Add(31*time.Minute))
		data.Items[ordinal-1].Seed["status"] = "exhausted"
	}
	state := assessOwnedInventory(channel, data, now)
	if state.HoldReason != "" || state.SkipReason != "" || state.ConsumedCount != 6 || state.Candidate == nil || state.Candidate.Seed.ID != ownedTestID(207) {
		t.Fatalf("seventh ordinal rejected: %+v", state)
	}
	data.Inventory["state"] = "exhausted"
	if state := assessOwnedInventory(channel, data, now); state.Candidate != nil {
		t.Fatal("exhausted scope selected an eighth task")
	}
}

func TestOwnedAgentMarkerRequiresActualExecutionFence(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	candidate := *assessOwnedInventory(channel, data, now).Candidate
	_, err := (&Store{}).InsertProductionTask(context.Background(), channel, candidate, now)
	if !errors.Is(err, errOwnedInventory) {
		t.Fatal("typed reference bypassed transaction authority")
	}
}
