package channelops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

var errOwnedInventory = errors.New("owned_inventory_invalid")

type ownedInventoryInput struct{ Item, Seed, Asset map[string]any }
type ownedInventoryData struct {
	Inventory                                           map[string]any
	Bindings                                            map[string]any
	Items                                               []ownedInventoryInput
	AccountIDs                                          []string
	Tasks                                               []map[string]any
	Queues                                              []map[string]any
	RuntimeOpen, RuntimeGuarded, Busy, UnknownOperation bool
}
type ownedCandidateAuthority struct{ InventoryID, ItemID, ManifestSHA, ContentSHA, AssetID, SeedSHA, ConfigurationSHA, HistorySHA string }
type ownedTickState struct {
	InventoryID, HoldReason, SkipReason, HistorySHA string
	UnusedItemID                                    string
	Candidate                                       *TickCandidate
	CompleteItemIDs                                 []string
	ConsumedCount                                   int
}

func ownedPlatformKey(platform string) int64 {
	hash := sha256.Sum256([]byte("owned-inventory:" + platform))
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

func ownedMap(value any) map[string]any { result, _ := value.(map[string]any); return result }
func ownedArray(value any) []any        { result, _ := value.([]any); return result }
func ownedString(value any) string      { result, _ := value.(string); return result }
func ownedInt(value any) int64 {
	switch n := value.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		result, err := strconv.ParseInt(string(n), 10, 64)
		if err == nil {
			return result
		}
	}
	return -1
}
func ownedTime(value any) (time.Time, bool) {
	text, ok := value.(string)
	if !ok {
		return time.Time{}, false
	}
	result, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		result, err = time.Parse("2006-01-02T15:04:05.999999999", text)
	}
	return result.UTC(), err == nil && !result.IsZero()
}
func ownedISO(value time.Time) string {
	value = value.UTC()
	if value.Nanosecond() == 0 {
		return value.Format("2006-01-02T15:04:05+00:00")
	}
	return value.Format("2006-01-02T15:04:05.000000+00:00")
}
func ownedFields(value map[string]any, names string) (map[string]any, error) {
	result := map[string]any{}
	for _, name := range strings.Fields(names) {
		field, ok := value[name]
		if !ok {
			return nil, errOwnedInventory
		}
		result[name] = field
	}
	return result, nil
}
func ownedEqual(left, right any) bool {
	l, le := ownedCanonical(left)
	r, re := ownedCanonical(right)
	return le == nil && re == nil && bytes.Equal(l, r)
}
func ownedHasHash(value any, expected any) bool {
	hash, err := ownedHash(value)
	return err == nil && hash == ownedString(expected)
}

func ownedConfiguration(bindings map[string]any) (map[string]any, error) {
	names := map[string]string{
		"channel": "id config_version name positioning language default_aspect_ratio risk_policy_json content_mix_policy_json cadence_policy_json alert_policy_json enabled dry_run",
		"account": "id channel_profile_id platform platform_account_id credential_ref platform_specific_config_json default_privacy external_asset_auto_publish enabled paused_until",
		"lane":    "id channel_profile_id name description weight keywords_json negative_keywords_json min_posts_per_week max_posts_per_day max_consecutive_streak cooldown_after_post_minutes enabled paused_until",
		"format":  "id topic_lane_id format_key enabled weight target_duration_sec template_pool_json source_platforms_json default_publish_visibility",
	}
	result := map[string]any{}
	for kind, fields := range names {
		row, err := ownedFields(ownedMap(bindings[kind]), fields)
		if err != nil {
			return nil, err
		}
		// PostgreSQL row_to_json renders 1.0 double precision as 1; Python ORM keeps a float.
		if kind == "lane" || kind == "format" {
			weight, err := strconv.ParseFloat(fmtOwnedNumber(row["weight"]), 64)
			if err != nil || math.IsInf(weight, 0) || math.IsNaN(weight) {
				return nil, errOwnedInventory
			}
			row["weight"] = weight
		}
		result[kind] = row
	}
	return result, nil
}
func fmtOwnedNumber(value any) string {
	switch value := value.(type) {
	case json.Number:
		return string(value)
	case float64:
		return strconv.FormatFloat(value, 'g', -1, 64)
	case int:
		return strconv.Itoa(value)
	case int64:
		return strconv.FormatInt(value, 10)
	}
	return ""
}

func ownedAssetDescriptor(asset map[string]any) (map[string]any, error) {
	storagePath := ownedString(asset["storage_path"])
	if !strings.HasPrefix(storagePath, "assets/") || path.Clean(storagePath) != storagePath || strings.ContainsAny(storagePath, "\\\x00") {
		return nil, errOwnedInventory
	}
	if asset["storage_backend"] != "local" && asset["storage_backend"] != "minio" {
		return nil, errOwnedInventory
	}
	if size := ownedInt(asset["file_size"]); size <= 0 || size > 64*1024*1024 || !strings.HasPrefix(ownedString(asset["mime_type"]), "video/") {
		return nil, errOwnedInventory
	}
	info := ownedMap(asset["media_info"])
	if info["license"] != "owned" || info["provenance"] != "generated" {
		return nil, errOwnedInventory
	}
	metadata := map[string]any{}
	for key, value := range info {
		if key != "license" && key != "provenance" {
			metadata[key] = value
		}
	}
	descriptor, err := ownedFields(asset, "id storage_backend storage_path file_size mime_type")
	if err != nil {
		return nil, err
	}
	descriptor["media_info_sha256"], err = ownedHash(metadata)
	return descriptor, err
}

func assessOwnedInventory(channel ChannelProfileRow, data ownedInventoryData, now time.Time) ownedTickState {
	row := data.Inventory
	state := ownedTickState{InventoryID: ownedString(row["id"])}
	hold := func(reason string) ownedTickState { state.HoldReason = reason; state.Candidate = nil; return state }
	skip := func(reason string) ownedTickState { state.SkipReason = reason; state.Candidate = nil; return state }
	if channel.OwnedSeedInventoryID == nil || *channel.OwnedSeedInventoryID != state.InventoryID || row["channel_profile_id"] != channel.ID {
		return hold("owned_inventory_scope")
	}
	if row["state"] != "approved" {
		return skip("owned_inventory_terminal")
	}
	approved, approvedOK := ownedTime(row["approved_at"])
	starts, startOK := ownedTime(row["starts_at"])
	expires, expiryOK := ownedTime(row["expires_at"])
	if !approvedOK || approved.After(now) || strings.TrimSpace(ownedString(row["approved_by"])) == "" || strings.TrimSpace(ownedString(row["approval_reference"])) == "" || row["revoked_at"] != nil || row["succession_released_at"] != nil {
		return hold("owned_inventory_approval")
	}
	if !startOK || !expiryOK || expires.Sub(starts) != 168*time.Hour {
		return hold("owned_inventory_window")
	}
	if !now.Before(expires) {
		return hold("owned_inventory_expired")
	}
	if now.Before(starts) {
		return skip("owned_inventory_not_started")
	}
	if row["privacy"] != "unlisted" || ownedInt(row["max_admissions"]) != 7 || ownedInt(row["minimum_interval_seconds"]) != 86400 || channel.TickIntervalMinutes != 1 {
		return hold("owned_inventory_limits")
	}
	if len(data.AccountIDs) != 1 || data.AccountIDs[0] != row["target_account_id"] {
		return hold("owned_inventory_account_alias")
	}
	configuration, err := ownedConfiguration(data.Bindings)
	if err != nil {
		return hold("owned_inventory_configuration")
	}
	c, a, l, f := ownedMap(configuration["channel"]), ownedMap(configuration["account"]), ownedMap(configuration["lane"]), ownedMap(configuration["format"])
	if c["id"] != channel.ID || c["enabled"] != true || c["dry_run"] != false || a["id"] != row["target_account_id"] || a["channel_profile_id"] != channel.ID || a["platform_account_id"] != row["platform_channel_id"] || (a["platform"] != "youtube" && a["platform"] != "" && a["platform"] != nil) || a["enabled"] != true || a["paused_until"] != nil || a["default_privacy"] != "unlisted" || a["external_asset_auto_publish"] != false || l["id"] != row["topic_lane_id"] || l["channel_profile_id"] != channel.ID || l["enabled"] != true || l["paused_until"] != nil || f["id"] != row["lane_format_id"] || f["topic_lane_id"] != l["id"] || f["enabled"] != true || f["default_publish_visibility"] != "unlisted" || !ownedEqual(f["source_platforms_json"], []any{}) {
		return hold("owned_inventory_binding")
	}
	configHash, _ := ownedHash(configuration)
	if len(data.Items) != 7 {
		return hold("owned_inventory_cardinality")
	}
	entries := []any{}
	seenAssets, seenContent, seenSeeds := map[string]bool{}, map[string]bool{}, map[string]bool{}
	var selected *ownedInventoryInput
	reserved := 0
	for index := range data.Items {
		input := &data.Items[index]
		item, seed := input.Item, input.Seed
		assetID, content, seedID := ownedString(item["asset_id"]), ownedString(item["content_sha256"]), ownedString(item["manual_seed_id"])
		if ownedInt(item["ordinal"]) != int64(index+1) || item["inventory_id"] != state.InventoryID || item["platform_channel_id"] != row["platform_channel_id"] || seenAssets[assetID] || seenContent[content] || seenSeeds[seedID] || !validOwnedHash(content) || seed["id"] != seedID || seed["channel_profile_id"] != channel.ID || seed["target_account_id"] != row["target_account_id"] || seed["topic_lane_id"] != row["topic_lane_id"] || seed["source_policy"] != "owned_only" || !ownedEqual(seed["source_platforms_json"], []any{}) || !ownedEqual(seed["material_library_ids_json"], []any{}) {
			return hold("owned_inventory_item_binding")
		}
		seenAssets[assetID], seenContent[content], seenSeeds[seedID] = true, true, true
		constraints := ownedMap(seed["constraints_json"])
		if constraints["input_asset_id"] != assetID || constraints["source_strategy"] != "input_video" || constraints["planning_mode"] != "template" {
			return hold("owned_inventory_seed_binding")
		}
		seedBinding, err := ownedFields(seed, "id channel_profile_id topic_lane_id target_account_id prompt title_seed source_policy source_platforms_json material_library_ids_json constraints_json")
		if err != nil || !ownedHasHash(seedBinding, item["seed_sha256"]) {
			return hold("owned_inventory_seed_changed")
		}
		descriptor, err := ownedAssetDescriptor(input.Asset)
		if err != nil || input.Asset["id"] != assetID || !ownedEqual(descriptor, item["storage_descriptor_json"]) || ownedInt(item["byte_size"]) != ownedInt(input.Asset["file_size"]) {
			return hold("owned_inventory_asset_changed")
		}
		provenance := ownedMap(item["provenance_evidence_json"])
		if provenance["rights"] != "owned" || provenance["provenance"] != "generated" || !ownedHasHash(provenance, item["provenance_sha256"]) {
			return hold("owned_inventory_provenance_changed")
		}
		entries = append(entries, map[string]any{"id": item["id"], "ordinal": item["ordinal"], "asset_id": assetID, "manual_seed_id": seedID, "content_sha256": content, "byte_size": item["byte_size"], "storage_descriptor": descriptor, "provenance_evidence": provenance, "provenance_sha256": item["provenance_sha256"], "seed_sha256": item["seed_sha256"], "prompt": seed["prompt"], "title_seed": seed["title_seed"]})
		switch item["state"] {
		case "unused":
			if item["production_task_id"] != nil || item["consumed_at"] != nil || seed["status"] != "active" {
				return hold("owned_inventory_consumption")
			}
			if selected == nil {
				selected = input
			}
		case "reserved", "completed":
			if selected != nil {
				return hold("owned_inventory_ordinal_gap")
			}
			consumed, ok := ownedTime(item["consumed_at"])
			if !ok || consumed.Before(starts) || !consumed.Before(expires) || consumed.After(now) || !uuidPattern.MatchString(ownedString(item["production_task_id"])) || seed["status"] != "exhausted" {
				return hold("owned_inventory_consumption")
			}
			state.ConsumedCount++
			if item["state"] == "reserved" {
				reserved++
			}
		case "held":
			return hold("owned_inventory_item_held")
		default:
			return hold("owned_inventory_item_state")
		}
	}
	manifest := map[string]any{"version": 1, "inventory_id": state.InventoryID, "channel_profile_id": channel.ID, "topic_lane_id": row["topic_lane_id"], "lane_format_id": row["lane_format_id"], "target_account_id": row["target_account_id"], "platform_channel_id": row["platform_channel_id"], "starts_at": ownedISO(starts), "expires_at": ownedISO(expires), "privacy": "unlisted", "max_admissions": 7, "minimum_interval_seconds": 86400, "tick_interval_minutes": 1, "configuration_sha256": configHash, "entries": entries}
	if !ownedEqual(row["manifest_json"], manifest) || !ownedHasHash(manifest, row["manifest_sha256"]) {
		return hold("owned_inventory_manifest_changed")
	}
	if reserved > 1 {
		return hold("owned_inventory_multiple_outstanding")
	}
	if selected != nil {
		state.UnusedItemID = ownedString(selected.Item["id"])
	}
	history := assessOwnedHistory(data, now)
	state.HistorySHA = history.digest
	state.CompleteItemIDs = history.complete
	if history.hold != "" {
		return hold(history.hold)
	}
	if history.skip != "" {
		return skip(history.skip)
	}
	if selected == nil {
		return skip("owned_inventory_exhausted")
	}
	if !data.RuntimeOpen || data.RuntimeGuarded || data.Busy || channel.IntakePausedAt != nil {
		return skip("owned_inventory_runtime_not_ready")
	}
	seed := selected.Seed
	laneID, accountID := ownedString(l["id"]), ownedString(a["id"])
	manual := ManualSeedRow{ID: ownedString(seed["id"]), ChannelProfileID: channel.ID, TopicLaneID: &laneID, TargetAccountID: &accountID, Prompt: ownedString(seed["prompt"]), TitleSeed: ownedString(seed["title_seed"]), SourcePolicy: "owned_only", ConstraintsJSON: ownedMap(seed["constraints_json"]), Status: "active"}
	lane := TopicLaneRow{ID: laneID, ChannelProfileID: channel.ID, Name: ownedString(l["name"]), Description: ownedString(l["description"]), Enabled: true}
	format := LaneFormatRow{ID: ownedString(f["id"]), TopicLaneID: laneID, FormatKey: ownedString(f["format_key"]), Enabled: true, TargetDurationSec: int(ownedInt(f["target_duration_sec"])), DefaultPublishVisibility: "unlisted"}
	for _, keyword := range ownedArray(l["keywords_json"]) {
		lane.KeywordsJSON = append(lane.KeywordsJSON, ownedString(keyword))
	}
	for _, template := range ownedArray(f["template_pool_json"]) {
		format.TemplatePoolJSON = append(format.TemplatePoolJSON, ownedString(template))
	}
	account := PublishingAccountRow{ID: accountID, ChannelProfileID: channel.ID, Platform: "youtube", PlatformAccountID: ownedString(a["platform_account_id"]), Enabled: true, DefaultPrivacy: "unlisted"}
	candidate := CandidateFromManualSeed(manual, &lane, &format, &account, "")
	candidate.CandidateID = "owned_inventory:" + state.InventoryID + ":" + ownedString(selected.Item["id"])
	candidate.owned = &ownedCandidateAuthority{InventoryID: state.InventoryID, ItemID: ownedString(selected.Item["id"]), ManifestSHA: ownedString(row["manifest_sha256"]), ContentSHA: ownedString(selected.Item["content_sha256"]), AssetID: ownedString(selected.Item["asset_id"]), SeedSHA: ownedString(selected.Item["seed_sha256"]), ConfigurationSHA: configHash}
	candidate.owned.HistorySHA = state.HistorySHA
	state.Candidate = &candidate
	return state
}

type ownedHistoryState struct {
	hold, skip, digest string
	complete           []string
}

func assessOwnedHistory(data ownedInventoryData, now time.Time) ownedHistoryState {
	state := ownedHistoryState{}
	fail := func(reason string) ownedHistoryState { state.hold = reason; return state }
	if data.UnknownOperation {
		return fail("owned_inventory_unknown_operation")
	}
	items := map[string]map[string]any{}
	for _, input := range data.Items {
		if id := ownedString(input.Item["production_task_id"]); id != "" {
			items[id] = input.Item
		}
	}
	seen, watermark := map[string]bool{}, []any{}
	for _, history := range data.Tasks {
		task := ownedMap(history["task"])
		id := ownedString(task["id"])
		if !uuidPattern.MatchString(id) || seen[id] || task["target_account_id"] != data.Inventory["target_account_id"] {
			return fail("owned_inventory_history_identity")
		}
		seen[id] = true
		item, own := items[id]
		if own && (task["manual_seed_id"] != item["manual_seed_id"] || task["channel_profile_id"] != data.Inventory["channel_profile_id"]) {
			return fail("owned_inventory_history_identity")
		}
		if ownedInt(task["retry_count"]) != 0 || task["failure_reason"] != nil || task["blocked_by_guard"] != nil {
			return fail("owned_inventory_task_failed")
		}
		var replacement map[string]any
		if !own {
			replacement = ownedSettledPromotionReplacement(history)
		}
		for _, value := range ownedArray(history["queues"]) {
			q := ownedMap(value)
			if replacement != nil && q["id"] == ownedMap(replacement["automatic"])["id"] {
				continue
			}
			if !ownedQueueClean(q) || (q["status"] != "queued" && q["status"] != "running" && q["status"] != "succeeded") {
				return fail("owned_inventory_queue_failed")
			}
		}
		if job := ownedMap(history["job"]); job != nil && job["status"] != "SUCCEEDED" && job["status"] != "RUNNING" && job["status"] != "PENDING" && job["status"] != "WAITING_WINDOW" && job["status"] != "VALIDATING" && job["status"] != "PLANNING" {
			return fail("owned_inventory_job_outcome")
		}
		switch task["state"] {
		case TaskSelected, TaskPlanning, TaskProducing, TaskScheduled, TaskUploadedPrivate, TaskMeasured:
		default:
			return fail("owned_inventory_task_failed")
		}
		operations := ownedArray(history["operations"])
		if len(operations) == 0 && own && item["state"] == "reserved" {
			state.skip = "owned_inventory_outstanding"
			continue
		}
		if len(operations) != 1 {
			return fail("owned_inventory_operation_count")
		}
		op := ownedMap(operations[0])
		if op["production_task_id"] != id || op["job_id"] != task["job_id"] || !validOwnedHash(ownedString(op["content_sha256"])) || op["error_message"] != nil {
			return fail("owned_inventory_operation_identity")
		}
		if op["status"] != "succeeded" {
			if own && item["state"] == "reserved" && (op["status"] == "reserved" || op["status"] == "attempted" || op["status"] == "submitted") {
				state.skip = "owned_inventory_outstanding"
				continue
			}
			return fail("owned_inventory_operation_unresolved")
		}
		attempted, aOK := ownedTime(op["request_attempted_at"])
		completed, cOK := ownedTime(op["completed_at"])
		receipt := ownedMap(op["receipt_json"])
		video := ownedString(op["platform_video_id"])
		managerID := ownedString(op["manager_task_id"])
		_, receiptErr := ownedFields(receipt, "video_id url title privacy tags quota_estimate")
		if !aOK || !cOK || completed.Before(attempted) || completed.After(now) || !uuidPattern.MatchString(managerID) || strings.ToLower(managerID) != managerID || len(video) != 11 || strings.Trim(video, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" || receiptErr != nil || receipt["video_id"] != video || receipt["privacy"] != op["privacy"] || receipt["title"] != op["title"] || (op["privacy"] != "private" && op["privacy"] != "unlisted") || receipt["url"] != "https://www.youtube.com/watch?v="+video {
			return fail("owned_inventory_receipt")
		}
		job := ownedMap(history["job"])
		if job["status"] != "SUCCEEDED" {
			if own && item["state"] == "reserved" && job["status"] == "RUNNING" {
				state.skip = "owned_inventory_outstanding"
				continue
			}
			return fail("owned_inventory_job_outcome")
		}
		jobDone, ok := ownedTime(job["completed_at"])
		if !ok || jobDone.Before(completed) || jobDone.After(now) || job["id"] != op["job_id"] || job["error_message"] != nil {
			return fail("owned_inventory_job_receipt")
		}
		uploads := 0
		for _, value := range ownedArray(history["nodes"]) {
			node := ownedMap(value)
			if node["status"] != "SUCCEEDED" || node["job_id"] != job["id"] || node["error_message"] != nil {
				return fail("owned_inventory_node_outcome")
			}
			if node["node_type"] != "youtube_upload" {
				continue
			}
			uploads++
			nodeDone, ok := ownedTime(node["completed_at"])
			if !ok || nodeDone.Before(completed) || nodeDone.After(jobDone) || node["id"] != op["node_execution_id"] || !ownedEqual(node["input_artifact_ids"], []any{op["input_artifact_id"]}) {
				return fail("owned_inventory_node_receipt")
			}
			outputs := 0
			for _, value := range ownedArray(history["artifacts"]) {
				artifact := ownedMap(value)
				if artifact["id"] == node["output_artifact_id"] && artifact["node_execution_id"] == node["id"] && artifact["job_id"] == job["id"] && ownedEqual(ownedMap(artifact["media_info"])["youtube"], receipt) {
					outputs++
				}
			}
			if outputs != 1 {
				return fail("owned_inventory_output_receipt")
			}
		}
		if uploads != 1 {
			return fail("owned_inventory_upload_node_count")
		}
		publications := ownedArray(history["publications"])
		if len(publications) == 0 && own && item["state"] == "reserved" {
			state.skip = "owned_inventory_outstanding"
			continue
		}
		if len(publications) != 1 {
			return fail("owned_inventory_publication_count")
		}
		pub := ownedMap(publications[0])
		if pub["production_task_id"] != id || pub["account_id"] != task["target_account_id"] || pub["platform"] != "youtube" || pub["platform_content_id"] != video || pub["desired_privacy"] != "unlisted" || pub["public_at"] != nil {
			return fail("owned_inventory_publication_identity")
		}
		if pub["current_privacy"] == "private" && own && item["state"] == "reserved" {
			state.skip = "owned_inventory_outstanding"
			continue
		}
		if pub["current_privacy"] != "unlisted" || (pub["publish_status"] != "uploaded" && pub["publish_status"] != "scheduled") {
			return fail("owned_inventory_publication_privacy")
		}
		if pub["scheduled_publish_at"] == nil && own && item["state"] == "reserved" && ownedPendingPromotion(history, pub, completed, now) {
			state.skip = "owned_inventory_outstanding"
			continue
		}
		start, ok := ownedTime(pub["scheduled_publish_at"])
		if !ok || start.Before(completed) || start.After(now) {
			return fail("owned_inventory_publication_time")
		}
		reconciled, valid := ownedReconciled(history, pub, start)
		if !valid {
			return fail("owned_inventory_reconciliation")
		}
		if !reconciled {
			state.skip = "owned_inventory_outstanding"
			continue
		}
		metricHold, metricWait := ownedMetricsReady(history, pub, start, now)
		if metricHold {
			return fail("owned_inventory_metrics")
		}
		if metricWait {
			state.skip = "owned_inventory_metrics_pending"
		}
		if own && item["state"] == "reserved" {
			state.complete = append(state.complete, ownedString(item["id"]))
		}
		if now.Sub(attempted) < 24*time.Hour || now.Sub(completed) < 24*time.Hour {
			state.skip = "owned_inventory_cooldown"
		}
		// Queue leases, feedback and task/publication status advance normally after upload.
		// Revalidate them above, but bind policy to the settled effect and its identities.
		taskBinding, err := ownedFields(task, "id channel_profile_id target_account_id manual_seed_id job_id")
		if err != nil {
			return fail("owned_inventory_history_digest")
		}
		pubBinding, err := ownedFields(pub, "id production_task_id account_id platform platform_content_id desired_privacy current_privacy public_at uploaded_at scheduled_publish_at")
		if err != nil {
			return fail("owned_inventory_history_digest")
		}
		watermark = append(watermark, map[string]any{"task": taskBinding, "operations": history["operations"], "job": history["job"], "nodes": history["nodes"], "artifacts": history["artifacts"], "publication": pubBinding, "settled_promotion_replacement": replacement})
	}
	for id := range items {
		if !seen[id] {
			return fail("owned_inventory_missing_task")
		}
	}
	var err error
	state.digest, err = ownedHash(watermark)
	if err != nil {
		return fail("owned_inventory_history_digest")
	}
	return state
}

func ownedQueueClean(q map[string]any) bool {
	return q["last_error"] == nil && q["dead_letter_at"] == nil && ownedInt(q["attempt_count"]) >= 0 && ownedInt(q["attempt_count"]) <= 1
}

func ownedSettledPromotionReplacement(history map[string]any) map[string]any {
	pubs := ownedArray(history["publications"])
	if len(pubs) != 1 {
		return nil
	}
	pub, task := ownedMap(pubs[0]), ownedMap(history["task"])
	uploaded, uploadOK := ownedTime(pub["uploaded_at"])
	start, startOK := ownedTime(pub["scheduled_publish_at"])
	if !uploadOK || !startOK || start.Before(uploaded) || pub["desired_privacy"] != "unlisted" || pub["current_privacy"] != "unlisted" || pub["public_at"] != nil {
		return nil
	}
	var auto, manual, reconcile, parent map[string]any
	for _, value := range ownedArray(history["queues"]) {
		q := ownedMap(value)
		if q["kind"] == QueueReconcilePublication {
			reconcile = q
		}
		if q["kind"] != QueuePromotePublication {
			continue
		}
		if q["status"] == "cancelled" {
			if auto != nil {
				return nil
			}
			auto = q
		} else {
			if manual != nil {
				return nil
			}
			manual = q
		}
	}
	if auto == nil || manual == nil || auto["id"] == manual["id"] || !uuidPattern.MatchString(ownedString(auto["id"])) || !uuidPattern.MatchString(ownedString(manual["id"])) {
		return nil
	}
	due := uploaded.Add(time.Hour)
	run, runOK := ownedTime(auto["run_after"])
	cancelled, cancelOK := ownedTime(auto["dead_letter_at"])
	manualRun, manualOK := ownedTime(manual["run_after"])
	payload, immediate := ownedMap(auto["payload_json"]), ownedMap(manual["payload_json"])
	if !runOK || !run.Equal(due) || !cancelOK || cancelled.Before(uploaded) || !manualOK || manualRun.Before(cancelled) || manualRun.After(start) || auto["last_error"] != "replaced_by_immediate_unlisted_canary_promotion" || ownedInt(auto["attempt_count"]) != 0 || auto["locked_at"] != nil || auto["locked_by"] != nil || auto["channel_profile_id"] != task["channel_profile_id"] || auto["idempotency_key"] != "promote_publication:"+ownedString(pub["id"])+":unlisted:"+due.Format(time.RFC3339) || payload["publication_id"] != pub["id"] || payload["target_visibility"] != "unlisted" || payload["scheduled_at"] != due.Format(time.RFC3339) {
		return nil
	}
	if manual["status"] != "succeeded" || !ownedQueueClean(manual) || ownedInt(manual["attempt_count"]) != 1 || manual["locked_at"] != nil || manual["locked_by"] != nil || manual["parent_queue_item_id"] != nil || manual["channel_profile_id"] != task["channel_profile_id"] || manual["idempotency_key"] != "promote_publication:"+ownedString(pub["id"])+":unlisted:manual" || immediate["publication_id"] != pub["id"] || immediate["target_visibility"] != "unlisted" || immediate["channel_profile_id"] != task["channel_profile_id"] || immediate["scheduled_at"] != nil {
		return nil
	}
	for _, value := range ownedArray(history["queues"]) {
		q := ownedMap(value)
		if q["id"] == auto["parent_queue_item_id"] {
			parent = q
		}
	}
	if parent == nil || parent["kind"] != QueuePublishTask || parent["status"] != "succeeded" || !ownedQueueClean(parent) || ownedInt(parent["attempt_count"]) != 1 || parent["locked_at"] != nil || parent["locked_by"] != nil || parent["channel_profile_id"] != task["channel_profile_id"] || ownedMap(parent["payload_json"])["production_task_id"] != task["id"] {
		return nil
	}
	done, valid := ownedReconciled(history, pub, start)
	if !valid || !done || reconcile["parent_queue_item_id"] != manual["id"] {
		return nil
	}
	// Retain the entire settled administrative proof, including the cancelled record.
	return map[string]any{"automatic": auto, "manual": manual, "reconcile": reconcile, "publish_parent": parent}
}

func ownedPendingPromotion(history, pub map[string]any, completed, now time.Time) bool {
	uploaded, ok := ownedTime(pub["uploaded_at"])
	if !ok || uploaded.Before(completed) || uploaded.After(now) || pub["publish_status"] != "uploaded" || len(ownedArray(history["metrics"])) != 0 || len(ownedArray(history["feedback"])) != 0 {
		return false
	}
	var promote map[string]any
	for _, value := range ownedArray(history["queues"]) {
		q := ownedMap(value)
		if q["kind"] == QueueReconcilePublication || q["kind"] == QueueCollectMetrics {
			return false
		}
		if q["kind"] == QueuePromotePublication {
			if promote != nil {
				return false
			}
			promote = q
		}
	}
	if promote == nil {
		return false
	}
	task, payload := ownedMap(history["task"]), ownedMap(promote["payload_json"])
	if task["state"] != TaskUploadedPrivate {
		return false
	}
	due := uploaded.Add(time.Hour)
	run, ok := ownedTime(promote["run_after"])
	if !ok || !run.Equal(due) || promote["idempotency_key"] != "promote_publication:"+ownedString(pub["id"])+":unlisted:"+due.Format(time.RFC3339) || promote["channel_profile_id"] != task["channel_profile_id"] || payload["publication_id"] != pub["id"] || payload["target_visibility"] != "unlisted" || payload["scheduled_at"] != due.Format(time.RFC3339) || !ownedQueueClean(promote) || (promote["status"] != "queued" && promote["status"] != "running") {
		return false
	}
	if promote["status"] == "queued" && (ownedInt(promote["attempt_count"]) != 0 || promote["locked_by"] != nil || promote["locked_at"] != nil) {
		return false
	}
	if promote["status"] == "running" {
		_, locked := ownedTime(promote["locked_at"])
		if ownedInt(promote["attempt_count"]) != 1 || ownedString(promote["locked_by"]) == "" || !locked {
			return false
		}
	}
	for _, value := range ownedArray(history["queues"]) {
		q := ownedMap(value)
		if q["id"] == promote["parent_queue_item_id"] && q["kind"] == QueuePublishTask && q["channel_profile_id"] == task["channel_profile_id"] && ownedMap(q["payload_json"])["production_task_id"] == task["id"] && ownedQueueClean(q) && ownedInt(q["attempt_count"]) == 1 {
			if q["status"] == "succeeded" {
				return q["locked_by"] == nil && q["locked_at"] == nil
			}
			_, locked := ownedTime(q["locked_at"])
			return q["status"] == "running" && locked && ownedString(q["locked_by"]) != ""
		}
	}
	return false
}
func ownedReconciled(history, pub map[string]any, start time.Time) (bool, bool) {
	queues := ownedArray(history["queues"])
	var found map[string]any
	for _, value := range queues {
		q := ownedMap(value)
		if q["kind"] != QueueReconcilePublication {
			continue
		}
		if found != nil {
			return false, false
		}
		found = q
	}
	if found == nil {
		return false, false
	}
	task := ownedMap(history["task"])
	runAfter, ok := ownedTime(found["run_after"])
	if !ok || !runAfter.Equal(start.Add(30*time.Minute)) || found["idempotency_key"] != "reconcile_publication:"+ownedString(pub["id"])+":"+start.Format(time.RFC3339) || ownedMap(found["payload_json"])["publication_id"] != pub["id"] || found["channel_profile_id"] != task["channel_profile_id"] || !ownedQueueClean(found) {
		return false, false
	}
	parentOK := false
	for _, value := range queues {
		q := ownedMap(value)
		payload := ownedMap(q["payload_json"])
		if q["id"] == found["parent_queue_item_id"] && q["kind"] == QueuePromotePublication && q["channel_profile_id"] == task["channel_profile_id"] && payload["publication_id"] == pub["id"] && payload["target_visibility"] == "unlisted" && ownedQueueClean(q) {
			if q["status"] == "succeeded" && q["locked_at"] == nil && q["locked_by"] == nil {
				parentOK = true
			}
			_, locked := ownedTime(q["locked_at"])
			if q["status"] == "running" && ownedInt(q["attempt_count"]) == 1 && locked && ownedString(q["locked_by"]) != "" && found["status"] == "queued" && ownedInt(found["attempt_count"]) == 0 && found["locked_at"] == nil && found["locked_by"] == nil {
				return false, true
			}
		}
	}
	if !parentOK {
		return false, false
	}
	switch found["status"] {
	case "queued", "running":
		return false, true
	case "succeeded":
		valid := found["locked_at"] == nil && found["locked_by"] == nil && ownedInt(found["attempt_count"]) == 1
		return valid, valid
	default:
		return false, false
	}
}

func ownedMetricsReady(history, pub map[string]any, start, now time.Time) (hold, wait bool) {
	metrics := ownedArray(history["metrics"])
	if len(metrics) != len(metricStageSpecs) {
		return true, false
	}
	for _, value := range ownedArray(history["queues"]) {
		q := ownedMap(value)
		payload := ownedMap(q["payload_json"])
		if q["kind"] != QueueCollectMetrics || payload["metric_schedule_id"] == nil {
			continue
		}
		matched := false
		for _, value := range metrics {
			m := ownedMap(value)
			if m["id"] == payload["metric_schedule_id"] {
				matched = true
			}
		}
		if !matched {
			return true, false
		}
	}
	for _, plan := range BuildMetricSchedulePlans(ownedString(pub["id"]), start) {
		var m map[string]any
		for _, value := range metrics {
			row := ownedMap(value)
			if row["snapshot_stage"] == plan.Stage {
				if m != nil {
					return true, false
				}
				m = row
			}
		}
		if m == nil || m["publication_id"] != pub["id"] {
			return true, false
		}
		for key, want := range map[string]time.Time{"effective_start_at": start, "due_at": plan.DueAt, "grace_until": plan.GraceUntil} {
			got, ok := ownedTime(m[key])
			if !ok || !got.Equal(want) {
				return true, false
			}
		}
		chainHold, chainWait := ownedMetricChain(history, pub, m, plan, now)
		if chainHold {
			return true, false
		}
		wait = wait || chainWait
		switch m["status"] {
		case "succeeded":
			done, ok := ownedTime(m["completed_at"])
			// A future stage cannot be used as fabricated early feedback.
			if !ok || done.Before(plan.DueAt) || done.After(now) || done.After(plan.GraceUntil) || ownedInt(m["attempt_count"]) < 1 {
				return true, false
			}
			feedback := 0
			for _, value := range ownedArray(history["feedback"]) {
				f := ownedMap(value)
				if f["publication_id"] == pub["id"] && f["snapshot_stage"] == plan.Stage {
					feedback++
				}
			}
			if feedback != 1 {
				return true, false
			}
		case "pending":
			if m["completed_at"] != nil || !now.Before(plan.GraceUntil) {
				return true, false
			}
			if !now.Before(plan.DueAt) {
				wait = true
			}
		default:
			return true, false
		}
	}
	return false, wait
}

func ownedMetricChain(history, pub, metric map[string]any, plan MetricSchedulePlan, now time.Time) (hold, wait bool) {
	attempts := ownedInt(metric["attempt_count"])
	lastIndex := attempts
	succeeded := metric["status"] == MetricScheduleSucceeded
	if succeeded {
		lastIndex--
	}
	if lastIndex < 0 || lastIndex > 1024 {
		return true, false
	}
	if succeeded || attempts == 0 {
		if metric["last_error_code"] != nil {
			return true, false
		}
	} else if metric["status"] != MetricSchedulePending || metric["last_error_code"] != MetricErrorUnavailable {
		return true, false
	}
	lastAttempt, lastOK := ownedTime(metric["last_attempt_at"])
	if attempts > 0 && (!lastOK || lastAttempt.Before(plan.DueAt) || lastAttempt.After(now) || lastAttempt.After(plan.GraceUntil)) {
		return true, false
	}
	chain := make([]map[string]any, lastIndex+1)
	channelID := ownedMap(history["task"])["channel_profile_id"]
	for _, value := range ownedArray(history["queues"]) {
		q := ownedMap(value)
		payload := ownedMap(q["payload_json"])
		if q["kind"] != QueueCollectMetrics || payload["metric_schedule_id"] != metric["id"] {
			continue
		}
		index := ownedInt(payload["metrics_poll_count"])
		if index < 0 || index > lastIndex || chain[index] != nil || !uuidPattern.MatchString(ownedString(q["id"])) || payload["publication_id"] != pub["id"] || payload["snapshot_stage"] != plan.Stage || q["channel_profile_id"] != channelID || q["idempotency_key"] != strings.TrimSuffix(plan.IdempotencyKey, "0")+strconv.FormatInt(index, 10) || !ownedQueueClean(q) {
			return true, false
		}
		chain[index] = q
	}
	var previousRun time.Time
	for index, q := range chain {
		if q == nil {
			return true, false
		}
		run, ok := ownedTime(q["run_after"])
		if !ok || run.After(plan.GraceUntil) || (index == 0 && !run.Equal(plan.DueAt)) || (index > 0 && (!run.After(previousRun) || q["parent_queue_item_id"] != chain[index-1]["id"])) {
			return true, false
		}
		if index == 0 {
			parentOK := false
			for _, value := range ownedArray(history["queues"]) {
				parent := ownedMap(value)
				if parent["id"] == q["parent_queue_item_id"] && parent["kind"] == QueuePromotePublication && parent["status"] == "succeeded" && parent["channel_profile_id"] == channelID && ownedMap(parent["payload_json"])["publication_id"] == pub["id"] && ownedMap(parent["payload_json"])["target_visibility"] == "unlisted" && ownedQueueClean(parent) {
					parentOK = true
				}
			}
			if !parentOK {
				return true, false
			}
		}
		previousRun = run
		switch q["status"] {
		case "succeeded":
			if q["locked_at"] != nil || q["locked_by"] != nil || ownedInt(q["attempt_count"]) != 1 || (int64(index) == lastIndex && !succeeded) {
				return true, false
			}
		case "queued", "running":
			// A handler commits its metric result before the runner marks that queue done.
			if int64(index) < lastIndex && (int64(index) != lastIndex-1 || q["status"] != "running" || succeeded) {
				return true, false
			}
			if q["status"] == "queued" && (ownedInt(q["attempt_count"]) != 0 || q["locked_at"] != nil || q["locked_by"] != nil) {
				return true, false
			}
			if q["status"] == "running" {
				_, locked := ownedTime(q["locked_at"])
				if ownedInt(q["attempt_count"]) != 1 || !locked || ownedString(q["locked_by"]) == "" {
					return true, false
				}
			}
			if succeeded || int64(index) < lastIndex {
				wait = true
			}
		default:
			return true, false
		}
	}
	if succeeded {
		done, ok := ownedTime(metric["completed_at"])
		if !ok || !done.Equal(lastAttempt) || done.Before(previousRun) {
			return true, false
		}
	} else if attempts > 0 && !previousRun.After(lastAttempt) {
		return true, false
	}
	return false, wait
}

// The caller already holds the channel execution fence. Only database work belongs here.
func (s *Store) prepareOwnedTick(ctx context.Context, channel ChannelProfileRow, bucket string, options agentTickOptions) (prepared tickPreparation, err error) {
	if !s.hasExecutionTransaction() || s.executionChannelID == nil || *s.executionChannelID != channel.ID || channel.OwnedSeedInventoryID == nil {
		return tickPreparation{}, errOwnedInventory
	}
	db := s.db()
	defer func() {
		if !errors.Is(err, errOwnedInventory) {
			return
		}
		var now time.Time
		if clockErr := db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); clockErr != nil {
			err = clockErr
			return
		}
		// Malformed immutable JSON is a durable intake hold, not a retriable replacement.
		prepared = tickPreparation{Channel: channel, ChannelID: channel.ID, Bucket: bucket, Options: options, Now: now.UTC(), InputDigest: "owned_inventory_invalid", Owned: &ownedTickState{InventoryID: *channel.OwnedSeedInventoryID, HoldReason: "owned_inventory_invalid"}}
		err = nil
	}()
	if err := s.assertLeaderAuthority(ctx, db, true); err != nil {
		return tickPreparation{}, err
	}
	var platform string
	err = db.QueryRow(ctx, `SELECT platform_channel_id FROM owned_seed_inventories WHERE id=$1::uuid`, *channel.OwnedSeedInventoryID).Scan(&platform)
	if err != nil {
		return tickPreparation{}, err
	}
	if _, err := db.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ownedPlatformKey(platform)); err != nil {
		return tickPreparation{}, err
	}
	loadOne := func(query string, args ...any) (map[string]any, error) {
		var raw []byte
		if err := db.QueryRow(ctx, query, args...).Scan(&raw); err != nil {
			return nil, err
		}
		value, err := ownedDecode(raw)
		if err != nil || ownedMap(value) == nil {
			return nil, errOwnedInventory
		}
		return ownedMap(value), nil
	}
	data := ownedInventoryData{Bindings: map[string]any{}}
	data.Inventory, err = loadOne(`SELECT row_to_json(i) FROM owned_seed_inventories i WHERE id=$1::uuid FOR UPDATE`, *channel.OwnedSeedInventoryID)
	if err != nil {
		return tickPreparation{}, err
	}
	if data.Inventory["platform_channel_id"] != platform {
		return tickPreparation{}, errOwnedInventory
	}
	for _, binding := range []struct {
		name, query string
		id          any
	}{
		{"channel", `SELECT row_to_json(c) FROM channel_profiles c WHERE id=$1::uuid`, channel.ID},
		{"account", `SELECT row_to_json(a) FROM publishing_accounts a WHERE id=$1::uuid FOR SHARE`, data.Inventory["target_account_id"]},
		{"lane", `SELECT row_to_json(l) FROM topic_lanes l WHERE id=$1::uuid FOR SHARE`, data.Inventory["topic_lane_id"]},
		{"format", `SELECT row_to_json(f) FROM lane_format_matrix f WHERE id=$1::uuid FOR SHARE`, data.Inventory["lane_format_id"]},
	} {
		data.Bindings[binding.name], err = loadOne(binding.query, binding.id)
		if err != nil {
			return tickPreparation{}, err
		}
	}
	rows, err := db.Query(ctx, `SELECT id::text FROM publishing_accounts WHERE COALESCE(NULLIF(platform,''),'youtube')='youtube' AND platform_account_id=$1 ORDER BY id FOR SHARE`, platform)
	if err != nil {
		return tickPreparation{}, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		data.AccountIDs = append(data.AccountIDs, id)
	}
	rows.Close()
	if err != nil {
		return tickPreparation{}, err
	}
	if rows.Err() != nil {
		return tickPreparation{}, rows.Err()
	}
	rows, err = db.Query(ctx, `SELECT json_build_object('item',row_to_json(i),'seed',row_to_json(s),'asset',row_to_json(a))
		FROM owned_seed_inventory_items i JOIN manual_seeds s ON s.id=i.manual_seed_id JOIN assets a ON a.id=i.asset_id
		WHERE i.inventory_id=$1::uuid ORDER BY i.ordinal FOR UPDATE OF i FOR SHARE OF s,a`, *channel.OwnedSeedInventoryID)
	if err != nil {
		return tickPreparation{}, err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			break
		}
		var value any
		value, err = ownedDecode(raw)
		if err != nil {
			break
		}
		entry := ownedMap(value)
		data.Items = append(data.Items, ownedInventoryInput{Item: ownedMap(entry["item"]), Seed: ownedMap(entry["seed"]), Asset: ownedMap(entry["asset"])})
	}
	rows.Close()
	if err != nil {
		return tickPreparation{}, err
	}
	if rows.Err() != nil {
		return tickPreparation{}, rows.Err()
	}
	// History includes all scopes, not just this inventory or today's bucket. Orphan uploads
	// cannot be assigned safely to an account and therefore close admission conservatively.
	if err = db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM youtube_upload_operations o
		LEFT JOIN production_tasks t ON t.id=o.production_task_id LEFT JOIN publishing_accounts a ON a.id=t.target_account_id
		WHERE t.id IS NULL OR a.id IS NULL OR COALESCE(NULLIF(a.platform,''),'youtube')<>'youtube' OR a.platform_account_id='')`).Scan(&data.UnknownOperation); err != nil {
		return tickPreparation{}, err
	}
	rows, err = db.Query(ctx, ownedHistorySQL, data.Inventory["target_account_id"], platform)
	if err != nil {
		return tickPreparation{}, err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			break
		}
		var value any
		value, err = ownedDecode(raw)
		if err != nil {
			break
		}
		data.Tasks = append(data.Tasks, ownedMap(value))
		if len(data.Tasks) > 1024 {
			err = errOwnedInventory
			break
		}
	}
	rows.Close()
	if err != nil {
		return tickPreparation{}, err
	}
	if rows.Err() != nil {
		return tickPreparation{}, rows.Err()
	}
	rows, err = db.Query(ctx, `SELECT row_to_json(q) FROM channel_ops_queue_items q WHERE status IN ('queued','running') ORDER BY id LIMIT 1025`)
	if err != nil {
		return tickPreparation{}, err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			break
		}
		var value any
		value, err = ownedDecode(raw)
		if err != nil {
			break
		}
		data.Queues = append(data.Queues, ownedMap(value))
	}
	rows.Close()
	if err != nil {
		return tickPreparation{}, err
	}
	if rows.Err() != nil {
		return tickPreparation{}, rows.Err()
	}
	if len(data.Queues) > 1024 {
		return tickPreparation{}, errOwnedInventory
	}
	if err = db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE status IN ('PENDING','WAITING_WINDOW','VALIDATING','PLANNING','RUNNING'))
		OR EXISTS(SELECT 1 FROM node_executions WHERE status IN ('QUEUED','RUNNING'))
		OR EXISTS(SELECT 1 FROM production_tasks WHERE state NOT IN ('scheduled','uploaded_private','measured','held','failed','rejected'))`).Scan(&data.Busy); err != nil {
		return tickPreparation{}, err
	}
	var schedule string
	var guarded *string
	if err = db.QueryRow(ctx, `SELECT state,guarded_job_id::text FROM runtime_schedules WHERE service_name='videoprocess' FOR SHARE`).Scan(&schedule, &guarded); err != nil {
		return tickPreparation{}, err
	}
	data.RuntimeOpen, data.RuntimeGuarded = schedule == "OPEN", guarded != nil
	var now time.Time
	if err = db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return tickPreparation{}, err
	}
	data.Busy = data.Busy || !ownedQueuesSafe(channel.ID, data, now)
	state := assessOwnedInventory(channel, data, now.UTC())
	digest, err := ownedAdmissionDigest(data, state)
	if err != nil {
		return tickPreparation{}, err
	}
	var candidates []TickCandidate
	if state.Candidate != nil {
		candidates = []TickCandidate{*state.Candidate}
	}
	return tickPreparation{Channel: channel, ChannelID: channel.ID, Bucket: bucket, Options: options, Now: now.UTC(), Candidates: candidates, InputDigest: digest, Owned: &state}, nil
}

func ownedAdmissionDigest(data ownedInventoryData, state ownedTickState) (string, error) {
	// Immutable inputs are validated against the manifest on every fenced read. Runtime
	// readiness and queue leases are fresh guards, not immutable policy input bindings.
	return handlerSnapshotDigest(struct {
		Inventory                map[string]any
		UnusedItemID, HistorySHA string
		Ready                    bool
	}{data.Inventory, state.UnusedItemID, state.HistorySHA, state.Candidate != nil && state.HoldReason == ""})
}

const ownedHistorySQL = `
SELECT json_build_object(
 'task',row_to_json(t),
 'operations',COALESCE((SELECT json_agg(o ORDER BY o.id) FROM youtube_upload_operations o WHERE o.production_task_id=t.id),'[]'::json),
 'job',(SELECT row_to_json(j) FROM jobs j WHERE j.id=t.job_id),
 'nodes',COALESCE((SELECT json_agg(n ORDER BY n.id) FROM node_executions n WHERE n.job_id=t.job_id),'[]'::json),
 'artifacts',COALESCE((SELECT json_agg(a ORDER BY a.id) FROM artifacts a WHERE a.job_id=t.job_id AND a.id IN (SELECT output_artifact_id FROM node_executions WHERE job_id=t.job_id)),'[]'::json),
 'publications',COALESCE((SELECT json_agg(p ORDER BY p.id) FROM publication_records p WHERE p.production_task_id=t.id),'[]'::json),
 'queues',COALESCE((SELECT json_agg(q ORDER BY q.id) FROM channel_ops_queue_items q WHERE q.payload_json->>'production_task_id'=t.id::text OR q.payload_json->>'publication_id' IN (SELECT p.id::text FROM publication_records p WHERE p.production_task_id=t.id)),'[]'::json),
 'metrics',COALESCE((SELECT json_agg(m ORDER BY m.snapshot_stage) FROM publication_metric_schedules m JOIN publication_records p ON p.id=m.publication_id WHERE p.production_task_id=t.id),'[]'::json),
 'feedback',COALESCE((SELECT json_agg(f ORDER BY f.snapshot_stage) FROM feedback_snapshots f JOIN publication_records p ON p.id=f.publication_id WHERE p.production_task_id=t.id),'[]'::json)
) FROM production_tasks t
WHERE (t.target_account_id=$1::uuid
 OR EXISTS(SELECT 1 FROM owned_seed_inventory_items i WHERE i.production_task_id=t.id AND i.platform_channel_id=$2)
 OR t.target_account_id IN (SELECT id FROM publishing_accounts WHERE COALESCE(NULLIF(platform,''),'youtube')='youtube' AND platform_account_id=$2))
AND (t.state NOT IN ('held','failed','rejected') OR EXISTS(SELECT 1 FROM youtube_upload_operations o WHERE o.production_task_id=t.id) OR EXISTS(SELECT 1 FROM owned_seed_inventory_items i WHERE i.production_task_id=t.id))
ORDER BY t.id LIMIT 1025`

func ownedQueuesSafe(channelID string, data ownedInventoryData, now time.Time) bool {
	allowed := map[string]map[string]any{}
	for _, history := range data.Tasks {
		pubs := ownedArray(history["publications"])
		if len(pubs) != 1 {
			continue
		}
		pub := ownedMap(pubs[0])
		start, ok := ownedTime(pub["scheduled_publish_at"])
		if !ok {
			continue
		}
		hold, _ := ownedMetricsReady(history, pub, start, now)
		if hold {
			continue
		}
		for _, value := range ownedArray(history["queues"]) {
			q := ownedMap(value)
			if q["kind"] == QueueCollectMetrics && ownedMap(q["payload_json"])["metric_schedule_id"] != nil {
				allowed[ownedString(q["id"])] = q
			}
		}
	}
	for _, q := range data.Queues {
		if q["kind"] == QueueAgentTick && q["channel_profile_id"] == channelID && ownedMap(q["payload_json"])["channel_id"] == channelID && ownedQueueClean(q) {
			continue
		}
		if expected := allowed[ownedString(q["id"])]; expected != nil && ownedEqual(expected, q) {
			continue
		}
		return false
	}
	return true
}

func (s *Store) finalizeOwnedTick(ctx context.Context, before, current tickPreparation, candidates []TickCandidate) error {
	state := current.Owned
	if state == nil || before.Owned == nil || state.InventoryID != before.Owned.InventoryID {
		return errOwnedInventory
	}
	if state.HoldReason != "" {
		return s.holdOwnedInventory(ctx, current, state.HoldReason, before.Candidates)
	}
	if len(before.Candidates) == 1 && before.Candidates[0].owned != nil && before.Candidates[0].owned.ItemID == state.UnusedItemID && len(candidates) == 1 && candidates[0].CandidateID == before.Candidates[0].CandidateID && candidates[0].RejectionGuard != "owned_inventory_inputs_changed" && (candidates[0].Rejected || firstString(candidates[0].PDSDecisionJSON, "verdict") != "allow") {
		return s.holdOwnedInventory(ctx, current, "owned_inventory_pds_denied", candidates)
	}
	// A competing committed admission or normal completion is a safe no-op on replay.
	if state.Candidate == nil {
		return s.completeOwnedItems(ctx, current)
	}
	if before.InputDigest != current.InputDigest {
		return s.holdOwnedInventory(ctx, current, "owned_inventory_inputs_changed", before.Candidates)
	}
	if len(candidates) != 1 || candidates[0].CandidateID != state.Candidate.CandidateID || candidates[0].Rejected || firstString(candidates[0].PDSDecisionJSON, "verdict") != "allow" {
		return s.holdOwnedInventory(ctx, current, "owned_inventory_pds_denied", candidates)
	}
	if current.Options.PauseIntakeAfterSelection || current.Options.PlanDelay != 0 || current.Channel.DryRun {
		return errOwnedInventory
	}
	if err := s.completeOwnedItems(ctx, current); err != nil {
		return err
	}
	candidate := *state.Candidate
	candidate.PDSDecisionJSON = candidates[0].PDSDecisionJSON
	result := TickResult{Accepted: []TickCandidate{candidate}}
	db := s.db()
	audit, err := s.insertTickAudit(ctx, db, current.ChannelID, current.Bucket, result, map[string]any{"handler_version": "go", "owned_inventory_id": state.InventoryID, "manifest_sha256": candidate.owned.ManifestSHA})
	if err != nil {
		return err
	}
	audits, err := s.insertDecisionAuditEntries(ctx, db, audit, current.ChannelID, result)
	if err != nil {
		return err
	}
	taskID, err := s.insertProductionTask(ctx, db, current.Channel, candidate, current.Now)
	if err != nil {
		return err
	}
	tag, err := db.Exec(ctx, `WITH reservation_clock AS MATERIALIZED (SELECT clock_timestamp() AS at)
		UPDATE owned_seed_inventory_items item SET state='reserved',production_task_id=$2::uuid,consumed_at=c.at
		FROM owned_seed_inventories i,reservation_clock c
		WHERE item.id=$1::uuid AND item.state='unused' AND item.production_task_id IS NULL AND i.id=item.inventory_id
		AND i.state='approved' AND c.at>=i.starts_at AND c.at<i.expires_at`, candidate.owned.ItemID, taskID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errOwnedInventory
	}
	tag, err = db.Exec(ctx, `UPDATE manual_seeds SET status='exhausted',updated_at=$2::timestamp WHERE id=$1::uuid AND status='active'`, candidate.Seed.ID, current.Now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errOwnedInventory
	}
	decisionID := audits[candidate.CandidateID]
	if decisionID == "" {
		return errOwnedInventory
	}
	if err = s.attachDecisionAuditTask(ctx, db, decisionID, taskID); err != nil {
		return err
	}
	_, err = s.enqueue(ctx, db, EnqueueOptions{Kind: QueuePlanTask, IdempotencyKey: "plan_task:" + taskID, Payload: map[string]any{"production_task_id": taskID, "channel_id": current.ChannelID}, Priority: 100, RunAfter: current.Now, ChannelProfileID: &current.ChannelID})
	if err != nil {
		return err
	}
	if state.ConsumedCount == 6 {
		if _, err := db.Exec(ctx, `UPDATE owned_seed_inventories SET state='exhausted',updated_at=$2::timestamp WHERE id=$1::uuid AND state='approved'`, state.InventoryID, current.Now); err != nil {
			return err
		}
		return s.pauseChannelIntake(ctx, db, current.ChannelID, current.Now, "owned_inventory_exhausted")
	}
	return nil
}

func (s *Store) completeOwnedItems(ctx context.Context, preparation tickPreparation) error {
	for _, id := range preparation.Owned.CompleteItemIDs {
		tag, err := s.db().Exec(ctx, `UPDATE owned_seed_inventory_items SET state='completed',completed_at=$2 WHERE id=$1::uuid AND state='reserved'`, id, preparation.Now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return errOwnedInventory
		}
	}
	return nil
}

func (s *Store) holdOwnedInventory(ctx context.Context, preparation tickPreparation, reason string, candidates []TickCandidate) error {
	tag, err := s.db().Exec(ctx, `UPDATE owned_seed_inventories SET state=CASE WHEN $2='owned_inventory_expired' THEN 'expired' ELSE 'held' END,hold_reason=$2,updated_at=$3::timestamp WHERE id=$1::uuid AND state='approved'`, preparation.Owned.InventoryID, reason, preparation.Now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errOwnedInventory
	}
	if err = s.pauseChannelIntake(ctx, s.db(), preparation.ChannelID, preparation.Now, reason); err != nil {
		return err
	}
	rejected := append([]TickCandidate(nil), candidates...)
	for i := range rejected {
		if !rejected[i].Rejected {
			rejectCandidate(&rejected[i], reason, "Owned inventory intake held.")
		}
	}
	result := TickResult{Rejected: rejected}
	audit, err := s.insertTickAudit(ctx, s.db(), preparation.ChannelID, preparation.Bucket, result, map[string]any{"handler_version": "go", "owned_inventory_id": preparation.Owned.InventoryID, "hold_reason": reason})
	if err != nil {
		return err
	}
	_, err = s.insertDecisionAuditEntries(ctx, s.db(), audit, preparation.ChannelID, result)
	return err
}

// This marker is minted only by finalizeOwnedTick after the fresh typed lookup.
func ownedTaskEvidence(candidate TickCandidate) map[string]any {
	a := candidate.owned
	return map[string]any{"inventory_id": a.InventoryID, "item_id": a.ItemID, "manifest_sha256": a.ManifestSHA, "configuration_sha256": a.ConfigurationSHA, "input_asset_id": a.AssetID, "source_content_sha256": a.ContentSHA, "seed_sha256": a.SeedSHA, "history_sha256": a.HistorySHA}
}

func validOwnedHash(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// Python's approval digest is not RFC 8785 or encoding/json's wire format.
func ownedCanonical(value any) ([]byte, error) {
	var out bytes.Buffer
	var appendValue func(any) error
	appendValue = func(value any) error {
		switch value := value.(type) {
		case nil:
			out.WriteString("null")
		case bool:
			out.WriteString(strconv.FormatBool(value))
		case string:
			if !utf8.ValidString(value) {
				return errOwnedInventory
			}
			out.WriteByte('"')
			for _, r := range value {
				switch r {
				case '"', '\\':
					out.WriteByte('\\')
					out.WriteRune(r)
				case '\b':
					out.WriteString(`\b`)
				case '\f':
					out.WriteString(`\f`)
				case '\n':
					out.WriteString(`\n`)
				case '\r':
					out.WriteString(`\r`)
				case '\t':
					out.WriteString(`\t`)
				default:
					if r >= 32 && r < 127 {
						out.WriteRune(r)
					} else if r <= 0xffff {
						out.WriteString(`\u` + hex4(uint16(r)))
					} else {
						hi, lo := utf16.EncodeRune(r)
						out.WriteString(`\u` + hex4(uint16(hi)) + `\u` + hex4(uint16(lo)))
					}
				}
			}
			out.WriteByte('"')
		case json.Number:
			if strings.ContainsAny(string(value), ".eE") {
				n, err := strconv.ParseFloat(string(value), 64)
				if err != nil {
					return errOwnedInventory
				}
				return appendValue(n)
			}
			n, ok := new(big.Int).SetString(string(value), 10)
			if !ok {
				return errOwnedInventory
			}
			out.WriteString(n.String())
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return errOwnedInventory
			}
			scientific := strconv.FormatFloat(value, 'e', -1, 64)
			exponent, err := strconv.Atoi(scientific[strings.LastIndexByte(scientific, 'e')+1:])
			if err != nil {
				return errOwnedInventory
			}
			if value == 0 || exponent >= -4 && exponent < 16 {
				plain := strconv.FormatFloat(value, 'f', -1, 64)
				if !strings.Contains(plain, ".") {
					plain += ".0"
				}
				out.WriteString(plain)
			} else {
				out.WriteString(scientific)
			}
		case int:
			out.WriteString(strconv.Itoa(value))
		case int64:
			out.WriteString(strconv.FormatInt(value, 10))
		case []any:
			out.WriteByte('[')
			for i, item := range value {
				if i > 0 {
					out.WriteByte(',')
				}
				if err := appendValue(item); err != nil {
					return err
				}
			}
			out.WriteByte(']')
		case map[string]any:
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			out.WriteByte('{')
			for i, key := range keys {
				if i > 0 {
					out.WriteByte(',')
				}
				if err := appendValue(key); err != nil {
					return err
				}
				out.WriteByte(':')
				if err := appendValue(value[key]); err != nil {
					return err
				}
			}
			out.WriteByte('}')
		default:
			return errOwnedInventory
		}
		return nil
	}
	if err := appendValue(value); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func hex4(value uint16) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[value>>12], digits[value>>8&15], digits[value>>4&15], digits[value&15]})
}

func ownedHash(value any) (string, error) {
	raw, err := ownedCanonical(value)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:]), nil
}

func ownedDecode(raw []byte) (any, error) {
	if !utf8.Valid(raw) || !ownedValidSurrogates(raw) {
		return nil, errOwnedInventory
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var read func() (any, error)
	read = func() (any, error) {
		token, err := decoder.Token()
		if err != nil {
			return nil, errOwnedInventory
		}
		switch token {
		case json.Delim('{'):
			value := map[string]any{}
			for decoder.More() {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok {
					return nil, errOwnedInventory
				}
				if _, exists := value[name]; exists {
					return nil, errOwnedInventory
				}
				item, err := read()
				if err != nil {
					return nil, err
				}
				value[name] = item
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return nil, errOwnedInventory
			}
			return value, nil
		case json.Delim('['):
			value := []any{}
			for decoder.More() {
				item, err := read()
				if err != nil {
					return nil, err
				}
				value = append(value, item)
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return nil, errOwnedInventory
			}
			return value, nil
		default:
			if _, ok := token.(json.Delim); ok {
				return nil, errOwnedInventory
			}
			return token, nil
		}
	}
	value, err := read()
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errOwnedInventory
	}
	return value, nil
}

func ownedValidSurrogates(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) {
			return false
		}
		if raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return false
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return false
		}
		if n < 0xd800 || n > 0xdbff {
			continue
		}
		if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
