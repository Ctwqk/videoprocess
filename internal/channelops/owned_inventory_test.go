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

	"github.com/jackc/pgx/v5/pgconn"
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
	return ownedTestFixtureOffset(t, 0)
}

func ownedTestFixtureOffset(t *testing.T, offset int) (ChannelProfileRow, ownedInventoryData, time.Time) {
	t.Helper()
	ownedTestID := func(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n+offset) }
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
	state := ownedTestAssess(t, channel, data, now)
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
			state := ownedTestAssess(t, channel, data, now)
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
			state := ownedTestAssess(t, channel, data, now)
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
		q := queue(530+i, QueueCollectMetrics, plan.IdempotencyKey, map[string]any{"publication_id": pubID, "metric_schedule_id": id, "snapshot_stage": plan.Stage, "metrics_poll_count": 0}, plan.DueAt)
		q["parent_queue_item_id"] = promote["id"]
		q["status"], q["attempt_count"] = "queued", 0
		if plan.DueAt.Sub(at) <= 24*time.Hour {
			m["status"], m["attempt_count"], m["completed_at"] = "succeeded", 1, ownedISO(plan.DueAt)
			m["last_attempt_at"] = m["completed_at"]
			q["status"], q["attempt_count"] = "succeeded", 1
			feedback = append(feedback, map[string]any{"id": ownedTestID(540 + i), "publication_id": pubID, "snapshot_stage": plan.Stage})
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

func ownedTestPendingPromotion(t *testing.T, data *ownedInventoryData, at time.Time) {
	t.Helper()
	ownedTestCompletedHistory(t, data, at)
	h := data.Tasks[0]
	pub := ownedMap(ownedArray(h["publications"])[0])
	pub["scheduled_publish_at"] = nil
	ownedMap(h["task"])["state"] = TaskUploadedPrivate
	due := at.Add(time.Hour)
	parent := map[string]any{"id": ownedTestID(550), "kind": QueuePublishTask, "channel_profile_id": ownedTestID(2), "payload_json": map[string]any{"production_task_id": ownedTestID(501)}, "status": "succeeded", "attempt_count": 1}
	promote := ownedMap(ownedArray(h["queues"])[0])
	promote["status"], promote["attempt_count"] = "queued", 0
	promote["parent_queue_item_id"], promote["run_after"] = parent["id"], ownedISO(due)
	promote["idempotency_key"] = "promote_publication:" + ownedString(pub["id"]) + ":unlisted:" + due.Format(time.RFC3339)
	promote["payload_json"] = map[string]any{"publication_id": pub["id"], "target_visibility": "unlisted", "scheduled_at": due.Format(time.RFC3339)}
	h["queues"], h["metrics"], h["feedback"] = []any{parent, promote}, []any{}, []any{}
}

func TestOwnedNormalUnlistedPendingPromotionWaits(t *testing.T) {
	for _, status := range []string{"queued", "running"} {
		t.Run(status, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestPendingPromotion(t, &data, now)
			q := ownedMap(ownedArray(data.Tasks[0]["queues"])[1])
			q["status"] = status
			if status == "running" {
				q["attempt_count"], q["locked_by"], q["locked_at"] = 1, "normal-worker", ownedISO(now.Add(time.Hour))
			}
			state := ownedTestAssess(t, channel, data, now.Add(time.Hour))
			if state.HoldReason != "" || state.SkipReason != "owned_inventory_outstanding" || state.Candidate != nil || len(state.CompleteItemIDs) != 0 {
				t.Fatalf("normal promotion must retain reservation: %+v", state)
			}
		})
	}
}

func TestOwnedPromotionCommitBeforeQueueCompletionRetainsReservation(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	ownedTestCompletedHistory(t, &data, now)
	queues := ownedArray(data.Tasks[0]["queues"])
	promote, reconcile := ownedMap(queues[0]), ownedMap(queues[1])
	promote["status"], promote["locked_at"], promote["locked_by"] = "running", ownedISO(now), "normal-worker"
	reconcile["status"], reconcile["attempt_count"] = "queued", 0
	got := ownedTestAssess(t, channel, data, now)
	if got.HoldReason != "" || got.Candidate != nil || got.SkipReason != "owned_inventory_outstanding" || len(got.CompleteItemIDs) != 0 {
		t.Fatalf("normal promotion commit gap did not wait: %+v", got)
	}
	reconcile["status"], reconcile["attempt_count"] = "succeeded", 1
	if got := ownedTestAssess(t, channel, data, now); got.HoldReason == "" {
		t.Fatal("reconciliation completed before promotion queue settlement")
	}
}

func TestOwnedPendingPromotionRejectsUnrelatedOrMalformedWork(t *testing.T) {
	for _, mode := range []string{"video", "public", "status", "due", "key", "parent", "missing", "duplicate", "metrics"} {
		t.Run(mode, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestPendingPromotion(t, &data, now)
			h := data.Tasks[0]
			pub, q := ownedMap(ownedArray(h["publications"])[0]), ownedMap(ownedArray(h["queues"])[1])
			switch mode {
			case "video":
				pub["platform_content_id"] = "other_video"
			case "public":
				pub["current_privacy"] = "public"
			case "status":
				pub["publish_status"] = "held"
			case "due":
				q["run_after"] = ownedISO(now)
			case "key":
				q["idempotency_key"] = "other"
			case "parent":
				q["parent_queue_item_id"] = ownedTestID(999)
			case "missing":
				h["queues"] = ownedArray(h["queues"])[:1]
			case "duplicate":
				h["queues"] = append(ownedArray(h["queues"]), q)
			case "metrics":
				h["metrics"] = []any{map[string]any{"status": "pending"}}
			}
			if got := ownedTestAssess(t, channel, data, now.Add(time.Hour)); got.HoldReason == "" || got.Candidate != nil {
				t.Fatalf("unsafe pending promotion: %+v", got)
			}
		})
	}
}

type ownedHoldProbe struct {
	dbExecutor
	reason string
	stop   error
}

func (p *ownedHoldProbe) Exec(_ context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(query, "UPDATE owned_seed_inventories SET state=CASE") {
		p.reason = args[1].(string)
	}
	return pgconn.CommandTag{}, p.stop
}

func TestOwnedFailedPolicySurvivesTransientRuntimeBlock(t *testing.T) {
	for _, runtime := range []string{"closed", "busy"} {
		for _, policy := range []string{"block", "error"} {
			t.Run(runtime+"/"+policy, func(t *testing.T) {
				channel, data, now := ownedTestFixture(t)
				before := ownedTestAssess(t, channel, data, now)
				candidate := *before.Candidate
				candidate.Rejected = true
				if policy == "block" {
					candidate.PDSDecisionJSON = map[string]any{"verdict": "block"}
				}
				data.RuntimeOpen, data.Busy = runtime != "closed", runtime == "busy"
				current := ownedTestAssess(t, channel, data, now)
				probe := &ownedHoldProbe{stop: errors.New("hold probe")}
				store := &Store{executionDB: probe}
				err := store.finalizeOwnedTick(context.Background(), tickPreparation{Owned: &before, Candidates: []TickCandidate{*before.Candidate}}, tickPreparation{Owned: &current}, []TickCandidate{candidate})
				if !errors.Is(err, probe.stop) || probe.reason != "owned_inventory_pds_denied" {
					t.Fatalf("policy failure was discarded: reason=%s error=%v", probe.reason, err)
				}
			})
		}
	}
}

func TestOwnedHistoryWatermarkIgnoresValidMetricLeaseChurn(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	ownedTestCompletedHistory(t, &data, now)
	q := ownedMap(ownedArray(data.Tasks[0]["queues"])[4])
	q["status"], q["locked_at"], q["locked_by"] = "running", ownedISO(now.Add(24*time.Hour)), "worker"
	before := ownedTestAssess(t, channel, data, now.Add(24*time.Hour))
	q["status"], q["locked_at"], q["locked_by"] = "succeeded", nil, nil
	after := ownedTestAssess(t, channel, data, now.Add(24*time.Hour))
	if before.Candidate != nil || before.HoldReason != "" || after.Candidate == nil || before.HistorySHA != after.HistorySHA {
		t.Fatal("valid metric lease churn changed immutable history watermark")
	}
	ownedMap(ownedArray(data.Tasks[0]["operations"])[0])["content_sha256"] = strings.Repeat("b", 64)
	changed := ownedTestAssess(t, channel, data, now.Add(25*time.Hour))
	if changed.HistorySHA == before.HistorySHA {
		t.Fatal("operation binding drift was hidden")
	}
}

func TestOwnedAdmissionDigestRechecksReadinessWithoutHashingTickLease(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	q := map[string]any{"id": ownedTestID(580), "kind": QueueAgentTick, "channel_profile_id": channel.ID, "payload_json": map[string]any{"channel_id": channel.ID}, "status": "queued", "attempt_count": 0}
	data.Queues = []map[string]any{q}
	before, err := ownedAdmissionDigest(data, ownedTestAssess(t, channel, data, now))
	if err != nil {
		t.Fatal(err)
	}
	q["status"], q["attempt_count"], q["locked_by"], q["locked_at"] = "running", 1, "other-tick-worker", ownedISO(now)
	if !ownedTestQueuesSafe(t, channel.ID, data, now) {
		t.Fatal("valid tick lease rejected")
	}
	after, err := ownedAdmissionDigest(data, ownedTestAssess(t, channel, data, now))
	if err != nil || after != before {
		t.Fatal("tick lease affected stable bindings")
	}
	data.RuntimeOpen = false
	closed, _ := ownedAdmissionDigest(data, ownedTestAssess(t, channel, data, now))
	if closed == before {
		t.Fatal("pre-PDS runtime revalidation was removed")
	}
	data.RuntimeOpen = true
	q["kind"] = QueueExecuteTask
	if ownedTestQueuesSafe(t, channel.ID, data, now) {
		t.Fatal("unsafe work accepted")
	}
}

func TestOwnedFailedPolicyCannotHoldNextItemAfterCompetingAdmission(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	before := ownedTestAssess(t, channel, data, now)
	rejected := *before.Candidate
	rejected.Rejected = true
	ownedTestCompletedHistory(t, &data, now)
	data.Tasks[0]["operations"], data.Tasks[0]["publications"] = []any{}, []any{}
	current := ownedTestAssess(t, channel, data, now)
	probe := &ownedHoldProbe{stop: errors.New("unexpected mutation")}
	err := (&Store{executionDB: probe}).finalizeOwnedTick(context.Background(), tickPreparation{Owned: &before, Candidates: []TickCandidate{*before.Candidate}}, tickPreparation{Owned: &current}, []TickCandidate{rejected})
	if err != nil || probe.reason != "" {
		t.Fatal("losing policy outcome held a different unused item")
	}
}

func ownedTestMetricRetry(t *testing.T, data *ownedInventoryData, at time.Time, recovered bool) {
	t.Helper()
	ownedTestCompletedHistory(t, data, at)
	h := data.Tasks[0]
	m := ownedMap(ownedArray(h["metrics"])[2])
	q := ownedMap(ownedArray(h["queues"])[4])
	next := map[string]any{}
	for k, v := range q {
		next[k] = v
	}
	next["id"], next["parent_queue_item_id"] = ownedTestID(560), q["id"]
	next["idempotency_key"] = strings.TrimSuffix(ownedString(q["idempotency_key"]), "0") + "1"
	next["run_after"], next["status"], next["attempt_count"] = ownedISO(at.Add(25*time.Hour)), "queued", 0
	payload := map[string]any{}
	for k, v := range ownedMap(q["payload_json"]) {
		payload[k] = v
	}
	payload["metrics_poll_count"] = 1
	next["payload_json"] = payload
	m["status"], m["attempt_count"], m["last_error_code"], m["completed_at"], m["last_attempt_at"] = "pending", 1, MetricErrorUnavailable, nil, ownedISO(at.Add(24*time.Hour))
	h["feedback"] = ownedArray(h["feedback"])[:2]
	if recovered {
		m["status"], m["attempt_count"], m["last_error_code"], m["completed_at"], m["last_attempt_at"] = "succeeded", 2, nil, ownedISO(at.Add(25*time.Hour)), ownedISO(at.Add(25*time.Hour))
		next["status"], next["attempt_count"] = "succeeded", 1
		h["feedback"] = append(ownedArray(h["feedback"]), map[string]any{"id": ownedTestID(542), "publication_id": m["publication_id"], "snapshot_stage": "24h"})
	}
	h["queues"] = append(ownedArray(h["queues"]), next)
}

func TestOwnedMetricRetryPendingAndRecovered(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		t.Run(fmt.Sprint(recovered), func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestMetricRetry(t, &data, now, recovered)
			got := ownedTestAssess(t, channel, data, now.Add(25*time.Hour))
			if got.HoldReason != "" || (got.Candidate != nil) != recovered || (!recovered && got.SkipReason != "owned_inventory_metrics_pending") {
				t.Fatalf("normal metric retry rejected: %+v", got)
			}
		})
	}
}

func TestOwnedMetricRetryCommitBeforeQueueCompletionWaits(t *testing.T) {
	for _, recovered := range []bool{false, true} {
		t.Run(fmt.Sprint(recovered), func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestMetricRetry(t, &data, now, recovered)
			queues := ownedArray(data.Tasks[0]["queues"])
			index := 4
			if recovered {
				index = len(queues) - 1
			}
			q := ownedMap(queues[index])
			q["status"], q["locked_at"], q["locked_by"] = "running", ownedISO(now.Add(24*time.Hour)), "normal-worker"
			got := ownedTestAssess(t, channel, data, now.Add(25*time.Hour))
			if got.HoldReason != "" || got.Candidate != nil || got.SkipReason != "owned_inventory_metrics_pending" {
				t.Fatalf("normal result/queue completion boundary rejected: %+v", got)
			}
		})
	}
}

func TestOwnedMetricRetryRejectsMalformedChains(t *testing.T) {
	for _, mode := range []string{"parent", "duplicate", "missing", "key", "poll", "publication", "channel", "early", "late", "error", "parent_failed", "attempts", "expired", "unrelated_schedule"} {
		t.Run(mode, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestMetricRetry(t, &data, now, false)
			h := data.Tasks[0]
			queues := ownedArray(h["queues"])
			q, m := ownedMap(queues[len(queues)-1]), ownedMap(ownedArray(h["metrics"])[2])
			switch mode {
			case "parent":
				q["parent_queue_item_id"] = ownedTestID(999)
			case "duplicate":
				h["queues"] = append(queues, q)
			case "missing":
				h["queues"] = queues[:len(queues)-1]
			case "key":
				q["idempotency_key"] = "other"
			case "poll":
				ownedMap(q["payload_json"])["metrics_poll_count"] = 2
			case "publication":
				ownedMap(q["payload_json"])["publication_id"] = ownedTestID(999)
			case "channel":
				q["channel_profile_id"] = ownedTestID(999)
			case "early":
				q["run_after"] = ownedISO(now.Add(23 * time.Hour))
			case "late":
				q["run_after"] = ownedISO(now.Add(31 * time.Hour))
			case "error":
				m["last_error_code"] = "other"
			case "parent_failed":
				ownedMap(queues[4])["status"] = "failed"
			case "attempts":
				m["attempt_count"] = 2
			case "expired":
				now = now.Add(6 * time.Hour)
			case "unrelated_schedule":
				extra := map[string]any{}
				for k, v := range q {
					extra[k] = v
				}
				extra["id"] = ownedTestID(998)
				extra["payload_json"] = map[string]any{"publication_id": m["publication_id"], "metric_schedule_id": ownedTestID(999), "snapshot_stage": "24h", "metrics_poll_count": 1}
				h["queues"] = append(queues, extra)
			}
			if got := ownedTestAssess(t, channel, data, now.Add(25*time.Hour)); got.HoldReason == "" || got.Candidate != nil {
				t.Fatalf("malformed metric chain accepted: %+v", got)
			}
		})
	}
}

func ownedTestHistoricalReplacement(t *testing.T, data *ownedInventoryData, at time.Time) {
	t.Helper()
	ownedTestCompletedHistory(t, data, at)
	// This is historical account activity, not a replacement of a current inventory item.
	data.Items[0].Item["state"], data.Items[0].Item["production_task_id"], data.Items[0].Item["consumed_at"] = "unused", nil, nil
	data.Items[0].Seed["status"] = "active"
	h := data.Tasks[0]
	pub := ownedMap(ownedArray(h["publications"])[0])
	pub["uploaded_at"] = ownedISO(at.Add(-time.Minute))
	op := ownedMap(ownedArray(h["operations"])[0])
	op["completed_at"] = pub["uploaded_at"]
	op["request_attempted_at"] = ownedISO(at.Add(-2 * time.Minute))
	manual := ownedMap(ownedArray(h["queues"])[0])
	manual["idempotency_key"] = "promote_publication:" + ownedString(pub["id"]) + ":unlisted:manual"
	manual["run_after"] = ownedISO(at.Add(-time.Second))
	autoDue := at.Add(59 * time.Minute)
	parent := map[string]any{"id": ownedTestID(570), "kind": QueuePublishTask, "channel_profile_id": ownedTestID(2), "payload_json": map[string]any{"production_task_id": ownedTestID(501)}, "status": "succeeded", "attempt_count": 1}
	auto := map[string]any{"id": ownedTestID(571), "kind": QueuePromotePublication, "idempotency_key": "promote_publication:" + ownedString(pub["id"]) + ":unlisted:" + autoDue.Format(time.RFC3339), "channel_profile_id": ownedTestID(2), "parent_queue_item_id": parent["id"], "payload_json": map[string]any{"publication_id": pub["id"], "target_visibility": "unlisted", "scheduled_at": autoDue.Format(time.RFC3339)}, "run_after": ownedISO(autoDue), "status": "cancelled", "attempt_count": 0, "last_error": "replaced_by_immediate_unlisted_canary_promotion", "dead_letter_at": ownedISO(at.Add(-2 * time.Second))}
	h["queues"] = append(ownedArray(h["queues"]), parent, auto)
}

func TestOwnedHistoricalPromotionReplacementRequiresSettlement(t *testing.T) {
	for _, mode := range []string{"valid", "reason", "claimed", "lock", "cancel_time", "key", "scope", "parent", "manual_failed", "manual_scope", "manual_key", "manual_public", "manual_parent", "reconcile_pending", "reconcile_parent", "video", "account", "uncertain", "extra_promotion"} {
		t.Run(mode, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			ownedTestHistoricalReplacement(t, &data, now)
			h := data.Tasks[0]
			queues := ownedArray(h["queues"])
			auto, manual, reconcile := ownedMap(queues[len(queues)-1]), ownedMap(queues[0]), ownedMap(queues[1])
			switch mode {
			case "reason":
				auto["last_error"] = "other"
			case "claimed":
				auto["attempt_count"] = 1
			case "lock":
				auto["locked_by"] = "old-worker"
			case "cancel_time":
				auto["dead_letter_at"] = ownedISO(now.Add(time.Hour))
			case "key":
				auto["idempotency_key"] = "other"
			case "scope":
				auto["channel_profile_id"] = ownedTestID(999)
			case "parent":
				auto["parent_queue_item_id"] = ownedTestID(999)
			case "manual_failed":
				manual["status"] = "failed"
			case "manual_scope":
				ownedMap(manual["payload_json"])["publication_id"] = ownedTestID(999)
			case "manual_key":
				manual["idempotency_key"] = "other"
			case "manual_public":
				ownedMap(manual["payload_json"])["target_visibility"] = "public"
			case "manual_parent":
				manual["parent_queue_item_id"] = auto["id"]
			case "reconcile_pending":
				reconcile["status"] = "queued"
			case "reconcile_parent":
				reconcile["parent_queue_item_id"] = auto["id"]
			case "video":
				ownedMap(ownedArray(h["publications"])[0])["platform_content_id"] = "wrong_video"
			case "account":
				ownedMap(ownedArray(h["publications"])[0])["account_id"] = ownedTestID(999)
			case "uncertain":
				ownedMap(ownedArray(h["operations"])[0])["status"] = "uncertain"
			case "extra_promotion":
				h["queues"] = append(queues, manual)
			}
			state := ownedTestAssess(t, channel, data, now.Add(25*time.Hour))
			if mode == "valid" {
				if state.HoldReason != "" || state.Candidate == nil {
					t.Fatalf("settled administrative replacement rejected: %+v", state)
				}
				before := state.HistorySHA
				auto["priority"] = 71
				if next := ownedTestAssess(t, channel, data, now.Add(25*time.Hour)); next.HistorySHA == before {
					t.Fatal("cancelled evidence was omitted from watermark")
				}
			} else if state.HoldReason == "" || state.Candidate != nil {
				t.Fatalf("invalid replacement accepted: %+v", state)
			}
		})
	}
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
				m["last_attempt_at"] = nil
				q := ownedMap(ownedArray(data.Tasks[0]["queues"])[4])
				q["status"], q["attempt_count"] = "queued", 0
				data.Tasks[0]["feedback"] = ownedArray(data.Tasks[0]["feedback"])[:2]
			}
			state := ownedTestAssess(t, channel, data, completed.Add(24*time.Hour+delay))
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
			state := ownedTestAssess(t, channel, data, now.Add(25*time.Hour))
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
		state := ownedTestAssess(t, channel, data, now.Add(later))
		if state.Candidate != nil || state.SkipReason != "owned_inventory_outstanding" || state.HoldReason != "" {
			t.Fatalf("outstanding replay: %+v", state)
		}
	}
}

func TestOwnedPDSUnavailableAndFallbackCloseAdmission(t *testing.T) {
	for _, mode := range []string{"nil", "error", "empty", "block", "flag", "fallback", "dev", "allow"} {
		t.Run(mode, func(t *testing.T) {
			channel, data, now := ownedTestFixture(t)
			candidate := *ownedTestAssess(t, channel, data, now).Candidate
			handler := HandlerService{}
			if mode != "nil" {
				handler.PDS = ownedTestPDS(func(_ context.Context, request PDSDecisionRequest) (PDSDecision, error) {
					if ownedMap(request.Context["owned_inventory"])["item_id"] != ownedTestID(301) {
						t.Fatal("missing policy binding")
					}
					if mode == "error" {
						return PDSDecision{}, errors.New("fake policy failure")
					}
					decision := PDSDecision{Verdict: mode, DecisionID: "fixture-decision", RulesVersion: "fixture-rules", EvaluatedRules: []string{"owned_source"}, Metadata: map[string]any{}}
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
	state := ownedTestAssess(t, channel, data, now)
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
	candidate := *ownedTestAssess(t, channel, data, now).Candidate
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
	state := ownedTestAssess(t, channel, data, now)
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
		m["last_attempt_at"] = nil
	}
	for _, value := range ownedArray(data.Tasks[0]["queues"])[2:] {
		q := ownedMap(value)
		q["status"], q["attempt_count"] = "queued", 0
	}
	data.Tasks[0]["feedback"] = []any{}
	state := ownedTestAssess(t, channel, data, completed.Add(31*time.Minute))
	if state.HoldReason != "" || state.SkipReason != "owned_inventory_cooldown" || state.Candidate != nil {
		t.Fatalf("midnight reset the floor: %+v", state)
	}
	ownedTestCompletedHistory(t, &data, completed)
	op = ownedMap(ownedArray(data.Tasks[0]["operations"])[0])
	op["request_attempted_at"] = ownedISO(completed.Add(-6 * time.Hour))
	m := ownedMap(ownedArray(data.Tasks[0]["metrics"])[2])
	m["status"], m["attempt_count"], m["completed_at"] = "pending", 0, nil
	m["last_attempt_at"] = nil
	q := ownedMap(ownedArray(data.Tasks[0]["queues"])[4])
	q["status"], q["attempt_count"] = "queued", 0
	data.Tasks[0]["feedback"] = ownedArray(data.Tasks[0]["feedback"])[:2]
	state = ownedTestAssess(t, channel, data, completed.Add(24*time.Hour-time.Second))
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
				m["last_attempt_at"] = nil
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
			safe := ownedTestQueuesSafe(t, channel.ID, data, at)
			state := ownedTestAssess(t, channel, data, at)
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
			m["last_attempt_at"] = m["completed_at"]
			q := ownedMap(ownedArray(history["queues"])[i+2])
			q["status"], q["attempt_count"] = "succeeded", 1
		}
		feedback := []any{}
		for i, value := range ownedArray(history["metrics"]) {
			m := ownedMap(value)
			if m["status"] == "succeeded" {
				feedback = append(feedback, map[string]any{"id": ownedTestID(540 + i), "publication_id": m["publication_id"], "snapshot_stage": m["snapshot_stage"]})
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
		for id := 501; id <= 544; id++ {
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
	state := ownedTestAssess(t, channel, data, now)
	if state.HoldReason != "" || state.SkipReason != "" || state.ConsumedCount != 6 || state.Candidate == nil || state.Candidate.Seed.ID != ownedTestID(207) {
		t.Fatalf("seventh ordinal rejected: %+v", state)
	}
	data.Inventory["state"] = "exhausted"
	if state := ownedTestAssess(t, channel, data, now); state.Candidate != nil {
		t.Fatal("exhausted scope selected an eighth task")
	}
}

func TestOwnedAgentMarkerRequiresActualExecutionFence(t *testing.T) {
	channel, data, now := ownedTestFixture(t)
	candidate := *ownedTestAssess(t, channel, data, now).Candidate
	_, err := (&Store{}).InsertProductionTask(context.Background(), channel, candidate, now)
	if !errors.Is(err, errOwnedInventory) {
		t.Fatal("typed reference bypassed transaction authority")
	}
}
