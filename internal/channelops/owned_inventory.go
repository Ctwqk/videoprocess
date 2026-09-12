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
	History                                             *ownedHistorySnapshot
	RuntimeOpen, RuntimeGuarded, Busy, UnknownOperation bool
}
type ownedCandidateAuthority struct {
	InventoryID, ItemID, ManifestSHA, ContentSHA, AssetID, SeedSHA, ConfigurationSHA, HistorySHA, HistoryAuthoritySHA string
	RetiredSourceSHA256, RetiredRenderSHA256                                                                          []string
}
type ownedTickState struct {
	InventoryID, HoldReason, SkipReason, HistorySHA string
	UnusedItemID                                    string
	Candidate                                       *TickCandidate
	CompleteItemIDs                                 []string
	ConsumedCount                                   int
	HistoryAuthoritySHA                             string
	RetiredSourceSHA256, RetiredRenderSHA256        []string
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
	if err := ownedHistoryProductionTarget(data, now); err != nil {
		return hold(err.Error())
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
	manifestBytes, err := ownedCanonical(row["manifest_json"])
	if err != nil {
		return hold("owned_inventory_manifest_changed")
	}
	decoded, err := decodeOwnedHistoryManifest(manifestBytes)
	if err != nil {
		return hold("owned_inventory_manifest_changed")
	}
	if decoded.version == 2 {
		manifest["version"] = 2
		manifest["legacy_history"] = ownedMap(row["manifest_json"])["legacy_history"]
	}
	if !ownedEqual(row["manifest_json"], manifest) || !ownedHasHash(manifest, row["manifest_sha256"]) {
		return hold("owned_inventory_manifest_changed")
	}
	if reserved > 1 {
		return hold("owned_inventory_multiple_outstanding")
	}
	if selected != nil {
		state.UnusedItemID = ownedString(selected.Item["id"])
	}
	history := assessOwnedHistorySnapshot(*data.History, now)
	state.HistorySHA = history.StableHistorySHA256
	state.HistoryAuthoritySHA = history.AuthoritySHA256
	state.RetiredSourceSHA256, state.RetiredRenderSHA256 = history.RetiredSourceSHA256, history.RetiredRenderSHA256
	if history.BlockReason != nil {
		return hold(*history.BlockReason)
	}
	for _, input := range data.Items {
		for _, hash := range append(append([]string{}, history.RetiredSourceSHA256...), history.RetiredRenderSHA256...) {
			if input.Item["content_sha256"] == hash {
				return hold("owned_inventory_retired_hash_reuse")
			}
		}
		for _, id := range history.CompletedItemIDs {
			if input.Item["id"] == id && input.Item["state"] == "reserved" {
				state.CompleteItemIDs = append(state.CompleteItemIDs, id)
			}
		}
	}
	if history.WaitReason != nil {
		return skip(*history.WaitReason)
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
	candidate.owned.HistoryAuthoritySHA = state.HistoryAuthoritySHA
	candidate.owned.RetiredSourceSHA256, candidate.owned.RetiredRenderSHA256 = state.RetiredSourceSHA256, state.RetiredRenderSHA256
	state.Candidate = &candidate
	return state
}

// The caller already holds the channel execution fence. Only database work belongs here.
func (s *Store) prepareOwnedTick(ctx context.Context, channel ChannelProfileRow, bucket string, options agentTickOptions) (prepared tickPreparation, err error) {
	if !s.hasExecutionTransaction() || s.executionChannelID == nil || *s.executionChannelID != channel.ID || channel.OwnedSeedInventoryID == nil {
		return tickPreparation{}, errOwnedInventory
	}
	db := s.db()
	defer func() {
		var historyErr ownedHistoryError
		if !errors.Is(err, errOwnedInventory) && !errors.As(err, &historyErr) {
			return
		}
		reason := "owned_inventory_invalid"
		if historyErr != "" {
			reason = string(historyErr)
		}
		var now time.Time
		if clockErr := db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); clockErr != nil {
			err = clockErr
			return
		}
		// Malformed immutable JSON is a durable intake hold, not a retriable replacement.
		prepared = tickPreparation{Channel: channel, ChannelID: channel.ID, Bucket: bucket, Options: options, Now: now.UTC(), InputDigest: reason, Owned: &ownedTickState{InventoryID: *channel.OwnedSeedInventoryID, HoldReason: reason}}
		err = nil
	}()
	if err := s.assertLeaderAuthority(ctx, db, true); err != nil {
		return tickPreparation{}, err
	}
	var schedule string
	var guarded *string
	if err = db.QueryRow(ctx, `SELECT state,guarded_job_id::text FROM runtime_schedules WHERE service_name='videoprocess' FOR UPDATE`).Scan(&schedule, &guarded); err != nil {
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
	snapshot, err := loadOwnedHistorySnapshot(ctx, db, platform)
	if err != nil {
		return tickPreparation{}, err
	}
	snapshot, err = ownedHistoryWithObservations(snapshot, s.ownedHistoryEvidence)
	if err != nil {
		return tickPreparation{}, err
	}
	data.History = &snapshot
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
		Inventory                                     map[string]any
		UnusedItemID, HistorySHA, HistoryAuthoritySHA string
		RetiredSourceSHA256, RetiredRenderSHA256      []string
		Ready                                         bool
	}{data.Inventory, state.UnusedItemID, state.HistorySHA, state.HistoryAuthoritySHA, state.RetiredSourceSHA256, state.RetiredRenderSHA256, state.Candidate != nil && state.HoldReason == ""})
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
	return map[string]any{"inventory_id": a.InventoryID, "item_id": a.ItemID, "manifest_sha256": a.ManifestSHA, "configuration_sha256": a.ConfigurationSHA, "input_asset_id": a.AssetID, "source_content_sha256": a.ContentSHA, "seed_sha256": a.SeedSHA, "history_sha256": a.HistorySHA, "history_authority_sha256": a.HistoryAuthoritySHA, "retired_source_sha256": a.RetiredSourceSHA256, "retired_render_sha256": a.RetiredRenderSHA256}
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
