package channelops

// Pure port of the frozen A1 history contract. Decoded facts are not authority to
// execute, approve, retire, or admit work. There are deliberately no live readers.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const ownedHistoryMaxRows = 4096
const ownedHistoryMaxBytes = 16 * 1024 * 1024

var historyUC = regexp.MustCompile(`^UC[A-Za-z0-9_-]{22}$`)
var historyUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var historySHA = regexp.MustCompile(`^[0-9a-f]{64}$`)
var historyRedisID = regexp.MustCompile(`^[0-9]+-[0-9]+$`)
var historyVideo = regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`)

const historyTableNames = "owned_seed_inventories owned_seed_inventory_items youtube_upload_operations production_tasks publishing_accounts channel_profiles jobs node_executions artifacts assets manual_seeds publication_records publication_metric_schedules feedback_snapshots channel_ops_queue_items worker_task_dispatches worker_task_delivery_attestations worker_event_emissions registered_worker_event_receipts registered_worker_event_deliveries worker_registrations worker_admission_grants legacy_worker_event_resolutions runtime_schedules publication_promotion_operations worker_redis_marker_cleanup_authorizations worker_redis_marker_repair_audits"
const historyTerminalTables = "node_executions artifacts worker_task_dispatches worker_task_delivery_attestations worker_event_emissions registered_worker_event_receipts registered_worker_event_deliveries worker_registrations worker_admission_grants legacy_worker_event_resolutions channel_ops_queue_items worker_redis_marker_cleanup_authorizations worker_redis_marker_repair_audits"

// The sole retirement identity adopted by A1; not a wildcard retirement policy.
var historyRetiredTuple = [...]string{"c25b9c38-b96a-4a21-80d0-352180cea206", "70d27dfb-f0c5-438c-bdfa-5316dc4f209b", "8061df32-3184-4c99-a5aa-556744a43ba5", "4c1b523f-0a35-45fd-990f-095e8156de2e", "2c4184d5-a02e-41e3-aeeb-16db8122f6e1", "4057a1a3-c37c-4bae-85a9-6d9d3dcac869"}

type ownedHistoryError string

func (e ownedHistoryError) Error() string { return string(e) }
func historyRequire(ok bool, reason ...string) {
	if !ok {
		code := "owned_history_invalid"
		if len(reason) != 0 {
			code = reason[0]
		}
		panic(ownedHistoryError(code))
	}
}
func historyRecover(err *error, code string) {
	if p := recover(); p != nil {
		if e, ok := p.(ownedHistoryError); ok {
			if code != "" {
				e = ownedHistoryError(code)
			}
			*err = e
		} else {
			panic(p)
		}
	}
}
func historyObject(v any) map[string]any          { m, ok := v.(map[string]any); historyRequire(ok); return m }
func historyArray(v any) []any                    { a, ok := v.([]any); historyRequire(ok); return a }
func historyString(v any) string                  { s, ok := v.(string); historyRequire(ok); return s }
func historyGet(m map[string]any, key string) any { v, ok := m[key]; historyRequire(ok); return v }
func historyDefaultObject(row map[string]any, key string) map[string]any {
	v, exists := row[key]
	if !exists {
		return map[string]any{}
	}
	return historyObject(v)
}
func historyFields(m map[string]any, names string) map[string]any {
	out := map[string]any{}
	for _, k := range strings.Fields(names) {
		out[k] = historyGet(m, k)
	}
	return out
}
func historyExact(v any, names string) map[string]any {
	m := historyObject(v)
	fields := strings.Fields(names)
	historyRequire(len(m) == len(fields))
	for _, k := range fields {
		historyGet(m, k)
	}
	return m
}
func historyID(v any) string {
	s := historyString(v)
	historyRequire(historyUUID.MatchString(s))
	return s
}
func historyHashValue(v any) string {
	s := historyString(v)
	historyRequire(historySHA.MatchString(s))
	return s
}
func historyText(v any) string {
	s := historyString(v)
	historyRequire(strings.TrimSpace(s) != "" && utf8.RuneCountInString(s) <= 512)
	return s
}
func historyInteger(v any) *big.Int {
	n, ok := v.(json.Number)
	historyRequire(ok && !strings.ContainsAny(string(n), ".eE"))
	i, valid := new(big.Int).SetString(string(n), 10)
	historyRequire(valid)
	return i
}
func historyInt(v any) int64 {
	i := historyInteger(v)
	// Every use is a bounded counter/ordinal comparison. Saturation preserves
	// Python's arbitrary-integer rejection path without native integer overflow.
	limit := int64(1 << 62)
	if i.Cmp(big.NewInt(limit)) > 0 {
		return limit
	}
	if i.Cmp(big.NewInt(-limit)) < 0 {
		return -limit
	}
	return i.Int64()
}
func historyIs(v any, values ...string) bool {
	historyReference(v)
	s, ok := v.(string)
	if !ok {
		return false
	}
	for _, x := range values {
		if x == s {
			return true
		}
	}
	return false
}
func historyReference(v any) string {
	switch v.(type) {
	case map[string]any, []any:
		historyRequire(false)
	}
	s, _ := v.(string)
	return s
}
func historyTruth(v any) bool {
	switch v := v.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	case json.Number:
		return historyNumeric(v).Sign() != 0
	}
	return true
}
func historyTime(v any) time.Time {
	s := historyString(v)
	if t, ok := historyAwareTime(s); ok {
		return t
	}
	for _, date := range []string{"2006-01-02", "20060102"} {
		if t, err := time.Parse(date, s); err == nil && t.Year() >= 1 {
			return t.UTC().Truncate(time.Microsecond)
		}
		for _, separator := range []string{"T", " "} {
			for _, clock := range []string{"15:04:05.999999999", "15:04", "15", "150405.999999999", "1504"} {
				if t, err := time.Parse(date+separator+clock, s); err == nil && t.Year() >= 1 {
					return t.UTC().Truncate(time.Microsecond)
				}
			}
		}
	}
	historyRequire(false)
	return time.Time{}
}
func historyAwareTime(s string) (time.Time, bool) {
	for _, date := range []string{"2006-01-02", "20060102"} {
		for _, separator := range []string{"T", " "} {
			for _, zone := range []string{"Z07:00", "Z0700", "Z07:00:00", "Z070000", "Z07"} {
				for _, clock := range []string{"15:04:05.999999999", "15:04", "15", "150405.999999999", "1504"} {
					if at, err := time.Parse(date+separator+clock+zone, s); err == nil {
						_, offset := at.Zone()
						if offset <= -24*60*60 || offset >= 24*60*60 {
							return time.Time{}, false
						}
						utc := at.UTC()
						// Python bounds both the parsed date and the UTC conversion.
						if at.Year() < 1 || utc.Year() < 1 || utc.Year() > 9999 {
							return time.Time{}, false
						}
						return utc.Truncate(time.Microsecond), true
					}
				}
			}
		}
	}
	return time.Time{}, false
}
func historyISO(t time.Time) string {
	t = t.UTC()
	layout := "2006-01-02T15:04:05"
	if t.Nanosecond() != 0 {
		layout += ".000000"
	}
	return t.Format(layout) + "+00:00"
}
func historyUTC(v any) time.Time            { t := historyTime(v); historyRequire(historyISO(t) == v); return t }
func historyZ(t time.Time) string           { return t.UTC().Format("2006-01-02T15:04:05Z") }
func historyBefore(a, b time.Time) bool     { return !a.After(b) }
func historyBetween(t, a, b time.Time) bool { return !t.Before(a) && !t.After(b) }
func historyCanonical(v any) string {
	raw, err := ownedCanonical(v)
	historyRequire(err == nil && len(raw) <= ownedHistoryMaxBytes, "owned_history_invalid_json")
	return string(raw)
}
func historyDecode(raw []byte) any {
	historyRequire(len(raw) <= ownedHistoryMaxBytes, "owned_history_invalid_json")
	v, err := ownedDecode(raw)
	historyRequire(err == nil, "owned_history_invalid_json")
	historyCanonical(v)
	return v
}
func historyHash(v any) string {
	sum := sha256.Sum256([]byte(historyCanonical(v)))
	return hex.EncodeToString(sum[:])
}
func historyBytesEqual(a, b any) bool { return historyCanonical(a) == historyCanonical(b) }

// A1 uses JSON value equality for ordinary facts, but FrozenJSON byte equality
// for retained documents. Numeric equality must not change canonical hashes.
func historyEqual(a, b any) bool {
	if x := historyNumeric(a); x != nil {
		y := historyNumeric(b)
		return y != nil && x.Cmp(y) == 0
	}
	switch a := a.(type) {
	case nil:
		return b == nil
	case string:
		s, ok := b.(string)
		return ok && a == s
	case []any:
		x, ok := b.([]any)
		if !ok || len(a) != len(x) {
			return false
		}
		for i := range a {
			if !historyEqual(a[i], x[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		x, ok := b.(map[string]any)
		if !ok || len(a) != len(x) {
			return false
		}
		for k, v := range a {
			other, ok := x[k]
			if !ok || !historyEqual(v, other) {
				return false
			}
		}
		return true
	}
	historyRequire(false)
	return false
}
func historyNumeric(v any) *big.Rat {
	switch v := v.(type) {
	case bool:
		if v {
			return big.NewRat(1, 1)
		}
		return big.NewRat(0, 1)
	case json.Number:
		if strings.ContainsAny(string(v), ".eE") {
			f, err := strconv.ParseFloat(string(v), 64)
			historyRequire(err == nil, "owned_history_invalid_json")
			return new(big.Rat).SetFloat64(f)
		}
		n, ok := new(big.Int).SetString(string(v), 10)
		historyRequire(ok)
		return new(big.Rat).SetInt(n)
	}
	return nil
}
func historyCopy(v any) any { return historyDecode([]byte(historyCanonical(v))) }
func historyRows(v any) []map[string]any {
	result := []map[string]any{}
	for _, r := range historyArray(v) {
		result = append(result, historyObject(r))
	}
	return result
}
func historySelect(v any, match func(map[string]any) bool) []any {
	result := []any{}
	for _, r := range historyRows(v) {
		if match(r) {
			result = append(result, r)
		}
	}
	return result
}
func historyOne(v []any, reason string) map[string]any {
	historyRequire(len(v) == 1, reason)
	return historyObject(v[0])
}
func historyIndex(v any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, r := range historyRows(v) {
		out[historyString(r["id"])] = r
	}
	return out
}
func historySet(v []any) map[string]bool {
	out := map[string]bool{}
	for _, x := range v {
		out[historyString(x)] = true
	}
	return out
}
func historyIDs(v any) map[string]bool {
	out := map[string]bool{}
	for _, r := range historyRows(v) {
		out[historyString(r["id"])] = true
	}
	return out
}
func historySorted(s map[string]bool) []string {
	out := []string{}
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func historySameSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
func historySortRows(a []any) {
	sort.Slice(a, func(i, j int) bool {
		return historyString(historyObject(a[i])["id"]) < historyString(historyObject(a[j])["id"])
	})
}

type ownedHistoryRedisObservation struct {
	kind, stream, group, sha string
	message, key, marker     *string
	pending                  string // canonical immutable array
	observedAt               time.Time
}

func historyNullableString(v any) *string {
	if v == nil {
		return nil
	}
	s := historyString(v)
	return &s
}
func historyOptional(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}
func historyParseRedis(v any) ownedHistoryRedisObservation {
	m := historyExact(v, "kind redis_stream consumer_group message_id dispatch_key payload_sha256 marker_message_id pending_message_ids observed_at")
	historyRequire(historyIs(m["kind"], "task", "event"))
	for _, k := range []string{"message_id", "marker_message_id"} {
		historyRequire(m[k] == nil || historyRedisID.MatchString(historyString(m[k])))
	}
	pending := historyArray(m["pending_message_ids"])
	historyRequire(len(pending) <= ownedHistoryMaxRows)
	for _, id := range pending {
		historyRequire(historyRedisID.MatchString(historyString(id)))
	}
	if m["kind"] == "task" {
		historyID(m["dispatch_key"])
	} else {
		historyRequire(m["dispatch_key"] == nil && m["marker_message_id"] == nil && m["message_id"] != nil)
	}
	return ownedHistoryRedisObservation{historyString(m["kind"]), historyText(m["redis_stream"]), historyText(m["consumer_group"]), historyHashValue(m["payload_sha256"]), historyNullableString(m["message_id"]), historyNullableString(m["dispatch_key"]), historyNullableString(m["marker_message_id"]), historyCanonical(pending), historyUTC(m["observed_at"])}
}

// Storage is immutable strings, not caller-owned slices/maps. Accessors decode a
// fresh copy; even repeated assessments cannot mutate the retained bytes.
type ownedHistorySnapshot struct {
	rowsJSON, redisJSON, platformChannelID string
	observedAt                             time.Time
}

func newOwnedHistorySnapshot(rowsJSON []byte, platformChannelID string, observedAt time.Time, redisJSON []byte) (snapshot ownedHistorySnapshot, err error) {
	defer historyRecover(&err, "")
	historyRequire(historyUC.MatchString(platformChannelID))
	rows := historyObject(historyDecode(rowsJSON))
	historyRequire(len(rows) == len(strings.Fields(historyTableNames)), "owned_history_incomplete")
	for _, name := range strings.Fields(historyTableNames) {
		v, exists := rows[name]
		a, ok := v.([]any)
		historyRequire(exists && ok && len(a) <= ownedHistoryMaxRows, "owned_history_incomplete")
		key := "id"
		if name == "runtime_schedules" {
			key = "service_name"
		}
		seen := map[string]bool{}
		for _, v := range a {
			r := historyObject(v)
			id := historyString(r[key])
			historyRequire(!seen[id], "owned_history_duplicate")
			seen[id] = true
		}
		sort.Slice(a, func(i, j int) bool {
			return historyString(historyObject(a[i])[key]) < historyString(historyObject(a[j])[key])
		})
	}
	redis := historyArray(historyDecode(redisJSON))
	for _, v := range redis {
		historyParseRedis(v)
	}
	return ownedHistorySnapshot{historyCanonical(rows), historyCanonical(redis), platformChannelID, observedAt.UTC()}, nil
}
func (s ownedHistorySnapshot) snapshotSHA256() string {
	sum := sha256.Sum256([]byte(s.rowsJSON))
	return hex.EncodeToString(sum[:])
}
func (s ownedHistorySnapshot) rows() map[string]any {
	return historyObject(historyDecode([]byte(s.rowsJSON)))
}
func (s ownedHistorySnapshot) observations() []ownedHistoryRedisObservation {
	out := []ownedHistoryRedisObservation{}
	for _, v := range historyArray(historyDecode([]byte(s.redisJSON))) {
		out = append(out, historyParseRedis(v))
	}
	return out
}

type ownedHistoryClassification struct {
	OperationID       string  `json:"operation_id"`
	Classification    string  `json:"classification"`
	PlatformChannelID *string `json:"platform_channel_id"`
	AccountID         string  `json:"account_id"`
}
type ownedHistoryTerminalPath struct {
	RecordID string `json:"record_id"`
	Path     string `json:"path"`
}
type ownedHistoryAssessment struct {
	BlockReason         *string                      `json:"block_reason"`
	Classifications     []ownedHistoryClassification `json:"classifications"`
	AccountIDs          []string                     `json:"account_ids"`
	RetiredSourceSHA256 []string                     `json:"retired_source_sha256"`
	RetiredRenderSHA256 []string                     `json:"retired_render_sha256"`
	AuthoritySHA256     string                       `json:"authority_sha256"`
	StableHistorySHA256 string                       `json:"stable_history_sha256"`
	WaitReason          *string                      `json:"wait_reason"`
	CompletedItemIDs    []string                     `json:"completed_item_ids"`
	TerminalPaths       []ownedHistoryTerminalPath   `json:"terminal_paths"`
}

func historyEmptyAssessment(reason *string) ownedHistoryAssessment {
	return ownedHistoryAssessment{BlockReason: reason, Classifications: []ownedHistoryClassification{}, AccountIDs: []string{}, RetiredSourceSHA256: []string{}, RetiredRenderSHA256: []string{}, CompletedItemIDs: []string{}, TerminalPaths: []ownedHistoryTerminalPath{}}
}

type ownedHistoryManifest struct {
	version  int
	document string
}

func decodeOwnedHistoryManifest(raw []byte) (manifest ownedHistoryManifest, err error) {
	defer historyRecover(&err, "owned_history_manifest_invalid")
	m := historyObject(historyDecode(raw))
	version := historyInt(m["version"])
	historyRequire(version == 1 || version == 2)
	names := "version inventory_id channel_profile_id topic_lane_id lane_format_id target_account_id platform_channel_id starts_at expires_at privacy max_admissions minimum_interval_seconds tick_interval_minutes configuration_sha256 entries"
	if version == 2 {
		names += " legacy_history"
	}
	historyExact(m, names)
	for _, k := range strings.Fields("inventory_id channel_profile_id topic_lane_id lane_format_id target_account_id") {
		historyID(m[k])
	}
	historyHashValue(m["configuration_sha256"])
	historyRequire(historyInt(m["tick_interval_minutes"]) == 1)
	start, end := historyUTC(m["starts_at"]), historyUTC(m["expires_at"])
	historyRequire(end.Sub(start) == 7*24*time.Hour && historyUC.MatchString(historyString(m["platform_channel_id"])) && m["privacy"] == "unlisted" && historyInt(m["max_admissions"]) == 7 && historyInt(m["minimum_interval_seconds"]) == 86400)
	entries := historyArray(m["entries"])
	historyRequire(len(entries) == 7)
	ids, seeds, assets, hashes := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i, v := range entries {
		e := historyExact(v, "id ordinal asset_id manual_seed_id content_sha256 byte_size storage_descriptor provenance_evidence provenance_sha256 seed_sha256 prompt title_seed")
		id, seed, asset, hash := historyID(e["id"]), historyID(e["manual_seed_id"]), historyID(e["asset_id"]), historyHashValue(e["content_sha256"])
		historyRequire(!ids[id] && !seeds[seed] && !assets[asset] && !hashes[hash])
		ids[id] = true
		seeds[seed] = true
		assets[asset] = true
		hashes[hash] = true
		bytes := historyInt(e["byte_size"])
		historyRequire(historyInt(e["ordinal"]) == int64(i+1) && bytes > 0 && bytes <= 67108864)
		d := historyExact(e["storage_descriptor"], "id storage_backend storage_path file_size mime_type media_info_sha256")
		historyRequire(d["id"] == asset && historyEqual(d["file_size"], e["byte_size"]))
		historyHashValue(d["media_info_sha256"])
		p := historyExact(e["provenance_evidence"], "rights provenance evidence_reference evidence_sha256 attestation")
		historyRequire(p["rights"] == "owned" && p["provenance"] == "generated")
		historyText(p["evidence_reference"])
		historyText(p["attestation"])
		historyHashValue(p["evidence_sha256"])
		historyRequire(historyHash(p) == historyHashValue(e["provenance_sha256"]))
		historyHashValue(e["seed_sha256"])
		prompt := historyString(e["prompt"])
		historyRequire(strings.TrimSpace(prompt) != "" && utf8.RuneCountInString(prompt) <= 4096 && utf8.RuneCountInString(historyString(e["title_seed"])) <= 512)
	}
	if version == 2 {
		section := historyExact(m["legacy_history"], "version bindings retired_unassigned_preupload")
		historyRequire(historyInt(section["version"]) == 1)
		bindings := historyArray(section["bindings"])
		historyRequire(len(bindings) <= 32)
		previous := ""
		for _, v := range bindings {
			b := historyParseBinding(v)
			id := historyString(b["legacy_account_id"])
			historyRequire(id > previous && b["canonical_platform_channel_id"] == m["platform_channel_id"] && b["legacy_account_id"] != m["target_account_id"])
			previous = id
		}
		if section["retired_unassigned_preupload"] != nil {
			c := historyParseCertificate(section["retired_unassigned_preupload"])
			historyRequire(m["channel_profile_id"] != c["legacy_channel_profile_id"] && m["target_account_id"] != c["legacy_account_id"])
			for _, v := range bindings {
				historyRequire(historyObject(v)["legacy_account_id"] != c["legacy_account_id"])
			}
		}
	}
	return ownedHistoryManifest{int(version), historyCanonical(m)}, nil
}
func historyParseBinding(v any) map[string]any {
	b := historyExact(v, "legacy_account_id legacy_channel_profile_id platform canonical_platform_channel_id use account_descriptor_sha256 qualified_operation_ids qualification")
	historyID(b["legacy_account_id"])
	historyID(b["legacy_channel_profile_id"])
	historyHashValue(b["account_descriptor_sha256"])
	historyRequire(b["platform"] == "youtube" && b["use"] == "history_only" && historyUC.MatchString(historyString(b["canonical_platform_channel_id"])))
	q := historyExact(b["qualification"], "observed_at server_subject manager_endpoint_identity manager_task_id platform_video_id actual_platform_channel_id sanitized_facts facts_sha256 approval_reference")
	observed := historyUTC(q["observed_at"])
	historyText(q["server_subject"])
	historyText(q["approval_reference"])
	endpoint := historyString(q["manager_endpoint_identity"])
	historyRequire(strings.HasPrefix(endpoint, "sha256:"))
	historyHashValue(strings.TrimPrefix(endpoint, "sha256:"))
	facts := historyArray(q["sanitized_facts"])
	ids := historyArray(b["qualified_operation_ids"])
	historyRequire(len(facts) >= 1 && len(facts) <= 128 && len(ids) == len(facts))
	previous := ""
	for i, v := range facts {
		f := historyExact(v, "operation_id manager_task_id platform_video_id actual_platform_channel_id operation_sha256 receipt_sha256 observed_at")
		id := historyID(f["operation_id"])
		historyRequire(id > previous && historyID(ids[i]) == id)
		previous = id
		historyID(f["manager_task_id"])
		historyRequire(historyVideo.MatchString(historyString(f["platform_video_id"])) && historyUC.MatchString(historyString(f["actual_platform_channel_id"])))
		historyHashValue(f["operation_sha256"])
		historyHashValue(f["receipt_sha256"])
		historyRequire(historyBefore(historyUTC(f["observed_at"]), observed) && f["actual_platform_channel_id"] == q["actual_platform_channel_id"] && f["actual_platform_channel_id"] == b["canonical_platform_channel_id"])
		if i == 0 {
			historyRequire(q["manager_task_id"] == f["manager_task_id"] && q["platform_video_id"] == f["platform_video_id"])
		}
	}
	historyRequire(historyHash(facts) == historyHashValue(q["facts_sha256"]))
	return b
}

// Frozen A1 model columns. Only retirement uses complete-row validation; ordinary
// history intentionally accepts the same bounded projections as the Python port.
var historyCompleteColumns = map[string][2]string{
	"youtube_upload_operations":                  {"production_task_id job_id node_execution_id input_artifact_id content_sha256 title privacy status manager_task_id platform_video_id receipt_json error_message request_attempted_at completed_at id created_at updated_at", "production_task_id manager_task_id platform_video_id error_message request_attempted_at completed_at"},
	"production_tasks":                           {"task_group_id channel_profile_id topic_lane_id lane_format_id target_account_id manual_seed_id discovery_signal_id source title_seed prompt rationale_json score_breakdown_json portfolio_bucket source_platforms_json material_library_ids_json uses_external_assets approval_mode agent_approval_evidence_json human_review_evidence_json autoflow_plan_id autoflow_run_id pipeline_id job_id scheduled_at priority state state_updated_at failure_reason failure_category retry_count blocked_by_guard channel_config_version_snapshot channel_config_snapshot_json transition_history_json id created_at updated_at", "task_group_id topic_lane_id lane_format_id manual_seed_id discovery_signal_id autoflow_plan_id autoflow_run_id pipeline_id job_id scheduled_at failure_reason failure_category blocked_by_guard"},
	"publishing_accounts":                        {"channel_profile_id platform account_label platform_account_id credential_ref platform_specific_config_json default_privacy external_asset_auto_publish enabled paused_until last_token_check_at last_token_check_status id created_at updated_at", "paused_until last_token_check_at last_token_check_status"},
	"channel_profiles":                           {"operator_id name positioning language default_aspect_ratio risk_policy_json content_mix_policy_json cadence_policy_json alert_policy_json enabled dry_run halted_at halt_reason intake_paused_at intake_pause_reason config_version tick_interval_minutes owned_seed_inventory_id id created_at updated_at", "operator_id halted_at halt_reason intake_paused_at intake_pause_reason owned_seed_inventory_id"},
	"jobs":                                       {"pipeline_id pipeline_snapshot status execution_plan submitted_at started_at completed_at error_message submitted_by parent_job_id retry_count orchestrator_owner id", "execution_plan started_at completed_at error_message parent_job_id"},
	"node_executions":                            {"job_id node_id node_type node_label node_config status progress worker_id queued_at started_at completed_at error_message error_trace retry_count input_artifact_ids output_artifact_id worker_registration_id worker_lease_epoch id", "worker_id queued_at started_at completed_at error_message error_trace output_artifact_id worker_registration_id worker_lease_epoch"},
	"artifacts":                                  {"job_id node_execution_id kind filename mime_type file_size storage_backend storage_path media_info created_at id", "mime_type file_size media_info"},
	"assets":                                     {"filename original_name mime_type file_size storage_backend storage_path media_info uploaded_at uploaded_by id", "mime_type file_size media_info"},
	"manual_seeds":                               {"channel_profile_id topic_lane_id target_account_id prompt title_seed source_policy source_platforms_json material_library_ids_json constraints_json status id created_at updated_at", "topic_lane_id target_account_id"},
	"channel_ops_queue_items":                    {"kind idempotency_key channel_profile_id priority parent_queue_item_id payload_json status run_after locked_at locked_by attempt_count max_attempts last_error dead_letter_at id created_at updated_at", "channel_profile_id parent_queue_item_id locked_at locked_by last_error dead_letter_at"},
	"worker_task_dispatches":                     {"origin_receipt_id dispatch_key job_id node_execution_id redis_stream consumer_group payload_sha256 payload_json delivery_state delivery_attempted_at delivery_error redis_message_id resolution_state acknowledged_at cancelled_at created_at delivered_at id", "origin_receipt_id delivery_attempted_at delivery_error redis_message_id acknowledged_at cancelled_at delivered_at"},
	"worker_task_delivery_attestations":          {"redis_stream consumer_group message_id payload_sha256 dispatch_key job_id node_execution_id worker_registration_id worker_lease_epoch worker_id worker_started_at ack_state acknowledged_at ack_event_emission_id attested_at id", "acknowledged_at ack_event_emission_id"},
	"worker_event_emissions":                     {"source_task_attestation_id redis_stream consumer_group message_id payload_sha256 payload_json event_type job_id node_execution_id worker_registration_id worker_lease_epoch worker_id worker_started_at emission_state prepared_at emitted_at resolved_at id", "message_id emitted_at resolved_at"},
	"registered_worker_event_receipts":           {"source_task_attestation_id redis_stream consumer_group message_id payload_sha256 payload_json event_type job_id node_execution_id worker_registration_id worker_lease_epoch worker_id worker_started_at source_task_stream source_task_group source_task_message_id application_state ack_state source_task_ack_state accepted_at applied_at acknowledged_at source_task_acknowledged_at id", "applied_at acknowledged_at source_task_acknowledged_at"},
	"registered_worker_event_deliveries":         {"source_task_attestation_id receipt_id redis_stream consumer_group message_id payload_sha256 resolution_state reason_code ack_state accepted_at acknowledged_at id", "receipt_id reason_code acknowledged_at"},
	"worker_registrations":                       {"grant_id service_name worker_type worker_host capabilities_json worker_instance_id worker_slot redis_consumer_id image_identity database_principal database_fingerprint redis_fingerprint storage_fingerprint lease_epoch status registered_at heartbeat_at lease_expires_at revoked_at revoke_reason superseded_by id", "revoked_at revoke_reason superseded_by"},
	"worker_admission_grants":                    {"service_name generation worker_type worker_host capabilities_json release_commit image_identity database_principal redis_stream redis_group endpoint_bindings_json state issued_at issued_by activated_at revoked_at revoke_reason created_at updated_at id", "activated_at revoked_at revoke_reason"},
	"legacy_worker_event_resolutions":            {"redis_stream consumer_group message_id payload_sha256 payload_json event_type job_id node_execution_id resolution_reason operator_id observed_job_status observed_node_status observed_task_state observed_channel_halted_at recorded_at acknowledged_at id", "acknowledged_at"},
	"worker_redis_marker_cleanup_authorizations": {"marker_kind source_id marker_key redis_stream expected_message_id payload_sha256 authorization_state authorized_at claimed_by_run_id claim_expires_at finished_at result_code id", "claimed_by_run_id claim_expires_at finished_at result_code"},
	"worker_redis_marker_repair_audits":          {"source_id action result_code principal created_at id", ""},
}

func historyCompleteRow(v any, table string) map[string]any {
	spec, ok := historyCompleteColumns[table]
	historyRequire(ok)
	r := historyExact(v, spec[0])
	nullable := map[string]bool{}
	for _, k := range strings.Fields(spec[1]) {
		nullable[k] = true
	}
	for _, k := range strings.Fields(spec[0]) {
		historyRequire(nullable[k] || r[k] != nil, "owned_history_incomplete")
	}
	historyID(r["id"])
	return r
}
func historyParseGraph(v any) map[string]any {
	g := historyExact(v, historyTerminalTables)
	for _, table := range strings.Fields(historyTerminalTables) {
		a := historyArray(g[table])
		historyRequire(len(a) <= ownedHistoryMaxRows)
		prev := ""
		for _, v := range a {
			r := historyCompleteRow(v, table)
			id := historyString(r["id"])
			historyRequire(id > prev)
			prev = id
		}
	}
	return g
}
func historyParseCertificate(v any) map[string]any {
	keys := "operation_id task_id job_id upload_node_id legacy_account_id legacy_channel_profile_id"
	c := historyExact(v, keys+" classification retained_facts terminal_graph terminal_graph_sha256 transition_sha256 observed_at server_subject approval_reference")
	for i, k := range strings.Fields(keys) {
		historyRequire(historyID(c[k]) == historyRetiredTuple[i])
	}
	historyRequire(c["classification"] == "retired_unassigned_preupload")
	r := historyExact(c["retained_facts"], "operation task job upload_node account channel manual_seed source_assets")
	for _, p := range [][2]string{{"operation", "youtube_upload_operations"}, {"task", "production_tasks"}, {"job", "jobs"}, {"upload_node", "node_executions"}, {"account", "publishing_accounts"}, {"channel", "channel_profiles"}, {"manual_seed", "manual_seeds"}} {
		historyCompleteRow(r[p[0]], p[1])
	}
	sources := historyArray(r["source_assets"])
	historyRequire(len(sources) >= 1 && len(sources) <= 7)
	previous := ""
	for _, v := range sources {
		s := historyExact(v, "asset content_sha256")
		a := historyCompleteRow(s["asset"], "assets")
		id := historyString(a["id"])
		historyRequire(id > previous)
		previous = id
		historyHashValue(s["content_sha256"])
	}
	g := historyParseGraph(c["terminal_graph"])
	historyRequire(historyHash(g) == historyHashValue(c["terminal_graph_sha256"]) && historyHash(historyObject(r["task"])["transition_history_json"]) == historyHashValue(c["transition_sha256"]))
	historyUTC(c["observed_at"])
	historyText(c["server_subject"])
	historyText(c["approval_reference"])
	return c
}

func historyApprovedAuthority(rows map[string]any, now time.Time) (map[string]map[string]any, map[string]any, []any) {
	bindings := map[string]map[string]any{}
	bindingOrder := []string{}
	var cert map[string]any
	authority := []any{}
	for _, row := range historyRows(rows["owned_seed_inventories"]) {
		if row["approved_at"] == nil {
			continue
		}
		historyRequire(historyIs(row["state"], "approved", "held", "exhausted", "expired", "revoked") && historyBefore(historyTime(row["approved_at"]), now) && historyTruth(row["approved_by"]) && historyTruth(row["approval_reference"]), "owned_history_authority_invalid")
		decoded, err := decodeOwnedHistoryManifest([]byte(historyCanonical(row["manifest_json"])))
		historyRequire(err == nil, "owned_history_manifest_invalid")
		m := historyObject(historyDecode([]byte(decoded.document)))
		historyRequire(historyHash(m) == row["manifest_sha256"] && m["inventory_id"] == row["id"] && historyEqual(historyFields(m, "platform_channel_id target_account_id channel_profile_id"), historyFields(row, "platform_channel_id target_account_id channel_profile_id")), "owned_history_authority_invalid")
		if decoded.version == 1 {
			continue
		}
		legacy := historyObject(m["legacy_history"])
		if legacy["retired_unassigned_preupload"] != nil {
			c := historyObject(legacy["retired_unassigned_preupload"])
			historyRequire(historyBefore(historyUTC(c["observed_at"]), historyTime(row["approved_at"])), "owned_history_authority_invalid")
			historyRequire(cert == nil || historyBytesEqual(cert, c), "owned_history_authority_conflict")
			cert = c
		}
		for _, b := range historyRows(legacy["bindings"]) {
			historyRequire(historyBefore(historyUTC(historyObject(b["qualification"])["observed_at"]), historyTime(row["approved_at"])), "owned_history_authority_invalid")
			id := historyString(b["legacy_account_id"])
			old := bindings[id]
			historyRequire(old == nil || historyBytesEqual(old, b), "owned_history_authority_conflict")
			if old == nil {
				bindingOrder = append(bindingOrder, id)
			}
			bindings[id] = b
		}
		a := historyFields(row, "manifest_sha256 approved_at approved_by approval_reference")
		a["inventory_id"] = row["id"]
		a["legacy_history"] = legacy
		authority = append(authority, a)
	}
	accounts, tasks := historyIndex(rows["publishing_accounts"]), historyIndex(rows["production_tasks"])
	for _, id := range bindingOrder {
		b := bindings[id]
		a := accounts[id]
		historyRequire(a != nil && a["channel_profile_id"] == b["legacy_channel_profile_id"] && historyPlatform(a) == "youtube" && (a["platform_account_id"] == "" || a["platform_account_id"] == b["canonical_platform_channel_id"]) && historyHash(historyFields(a, "id channel_profile_id platform platform_account_id credential_ref platform_specific_config_json")) == b["account_descriptor_sha256"], "owned_history_binding_changed")
		ops := historySelect(rows["youtube_upload_operations"], func(o map[string]any) bool {
			return tasks[historyReference(o["production_task_id"])]["target_account_id"] == id
		})
		ids := []any{}
		taskIDs := map[string]bool{}
		for _, o := range historyRows(ops) {
			ids = append(ids, o["id"])
			taskIDs[historyString(o["production_task_id"])] = true
		}
		historyRequire(historyEqual(ids, b["qualified_operation_ids"]), "owned_history_membership_changed")
		members := historyIDs(historySelect(rows["production_tasks"], func(t map[string]any) bool { return t["target_account_id"] == id }))
		historyRequire(historySameSet(members, taskIDs), "owned_history_membership_changed")
		facts := historyRows(historyObject(b["qualification"])["sanitized_facts"])
		for i, o := range historyRows(ops) {
			f := facts[i]
			historyRequire(o["status"] == "succeeded" && o["manager_task_id"] == f["manager_task_id"] && o["platform_video_id"] == f["platform_video_id"] && historyBetween(historyUTC(f["observed_at"]), historyTime(o["completed_at"]), now) && historyHash(o) == f["operation_sha256"] && historyHash(o["receipt_json"]) == f["receipt_sha256"], "owned_history_qualification_changed")
		}
	}
	if cert != nil {
		historyRequire(bindings[historyString(cert["legacy_account_id"])] == nil, "owned_history_authority_conflict")
	}
	return bindings, cert, authority
}
func historySortedKeys[V any](m map[string]V) []string {
	keys := []string{}
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func historyPlatform(a map[string]any) any {
	if !historyTruth(a["platform"]) {
		return "youtube"
	}
	return a["platform"]
}

func historyTask(rows, task map[string]any) map[string]any {
	pubs := historySelect(rows["publication_records"], func(r map[string]any) bool { return r["production_task_id"] == task["id"] })
	pubIDs := historyIDs(pubs)
	metrics := historySelect(rows["publication_metric_schedules"], func(r map[string]any) bool { return pubIDs[historyReference(r["publication_id"])] })
	metricIDs := historyIDs(metrics)
	queues := historySelect(rows["channel_ops_queue_items"], func(q map[string]any) bool {
		p := historyDefaultObject(q, "payload_json")
		return p["production_task_id"] == task["id"] || pubIDs[historyReference(p["publication_id"])] || metricIDs[historyReference(p["metric_schedule_id"])]
	})
	for {
		ids := historyIDs(queues)
		parents := map[string]bool{}
		for _, q := range historyRows(queues) {
			if q["parent_queue_item_id"] != nil {
				parents[historyString(q["parent_queue_item_id"])] = true
			}
		}
		expanded := historySelect(rows["channel_ops_queue_items"], func(q map[string]any) bool {
			return ids[historyReference(q["id"])] || parents[historyReference(q["id"])] || ids[historyReference(q["parent_queue_item_id"])]
		})
		if len(expanded) == len(queues) {
			break
		}
		queues = expanded
	}
	jobs := historySelect(rows["jobs"], func(r map[string]any) bool { return r["id"] == task["job_id"] })
	job := map[string]any{}
	if len(jobs) > 0 {
		job = historyObject(jobs[0])
	}
	nodes := historySelect(rows["node_executions"], func(r map[string]any) bool { return r["job_id"] == task["job_id"] })
	outputs := map[string]bool{}
	for _, n := range historyRows(nodes) {
		if n["output_artifact_id"] != nil {
			outputs[historyString(n["output_artifact_id"])] = true
		}
	}
	return map[string]any{"task": task, "operations": historySelect(rows["youtube_upload_operations"], func(r map[string]any) bool { return r["production_task_id"] == task["id"] }), "job": job, "nodes": nodes, "artifacts": historySelect(rows["artifacts"], func(a map[string]any) bool {
		return a["job_id"] == task["job_id"] && outputs[historyReference(a["id"])]
	}), "publications": pubs, "queues": queues, "metrics": metrics, "feedback": historySelect(rows["feedback_snapshots"], func(f map[string]any) bool { return pubIDs[historyReference(f["publication_id"])] })}
}

func historyQueueClean(q map[string]any) bool {
	return q["last_error"] == nil && q["dead_letter_at"] == nil && historyInt(q["attempt_count"]) >= 0 && historyInt(q["attempt_count"]) <= 1
}
func historyQueueState(q map[string]any, states ...string) bool {
	if !historyQueueClean(q) || !historyIs(q["status"], states...) {
		return false
	}
	switch q["status"] {
	case "queued":
		return historyInt(q["attempt_count"]) == 0 && q["locked_at"] == nil && q["locked_by"] == nil
	case "running":
		return historyInt(q["attempt_count"]) == 1 && historyTruth(q["locked_by"]) && !historyTime(q["locked_at"]).IsZero()
	default:
		return historyInt(q["attempt_count"]) == 1 && q["locked_at"] == nil && q["locked_by"] == nil
	}
}
func historyReconciled(h, pub map[string]any, start time.Time) bool {
	reason := "owned_inventory_reconciliation"
	q := historyOne(historySelect(h["queues"], func(q map[string]any) bool { return q["kind"] == "reconcile_publication" }), reason)
	p := historyObject(q["payload_json"])
	task := historyObject(h["task"])
	historyRequire(historyTime(q["run_after"]).Equal(start.Add(30*time.Minute)) && q["idempotency_key"] == "reconcile_publication:"+historyString(pub["id"])+":"+historyZ(start) && p["publication_id"] == pub["id"] && q["channel_profile_id"] == task["channel_profile_id"] && historyQueueState(q, "queued", "running", "succeeded"), reason)
	parent := historyOne(historySelect(h["queues"], func(p map[string]any) bool { return p["id"] == q["parent_queue_item_id"] }), reason)
	pp := historyObject(parent["payload_json"])
	historyRequire(parent["kind"] == "promote_publication" && parent["channel_profile_id"] == q["channel_profile_id"] && pp["publication_id"] == pub["id"] && pp["target_visibility"] == "unlisted", reason)
	if historyQueueState(parent, "running") && q["status"] == "queued" {
		return false
	}
	historyRequire(historyQueueState(parent, "succeeded"), reason)
	return q["status"] == "succeeded"
}
func historyPendingPromotion(h, pub map[string]any, completed, now time.Time) (valid bool) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(ownedHistoryError); !ok {
				panic(p)
			}
			valid = false
		}
	}()
	uploaded := historyTime(pub["uploaded_at"])
	task := historyObject(h["task"])
	historyRequire(historyBetween(uploaded, completed, now) && pub["publish_status"] == "uploaded" && task["state"] == "uploaded_private" && len(historyArray(h["metrics"])) == 0 && len(historyArray(h["feedback"])) == 0)
	historyRequire(len(historySelect(h["queues"], func(q map[string]any) bool { return historyIs(q["kind"], "reconcile_publication", "collect_metrics") })) == 0)
	q := historyOne(historySelect(h["queues"], func(q map[string]any) bool { return q["kind"] == "promote_publication" }), "owned_history_invalid")
	p := historyObject(q["payload_json"])
	due := uploaded.Add(time.Hour)
	historyRequire(historyTime(q["run_after"]).Equal(due) && q["idempotency_key"] == "promote_publication:"+historyString(pub["id"])+":unlisted:"+historyZ(due) && q["channel_profile_id"] == task["channel_profile_id"] && p["publication_id"] == pub["id"] && p["target_visibility"] == "unlisted" && p["scheduled_at"] == historyZ(due) && historyQueueState(q, "queued", "running"))
	parent := historyOne(historySelect(h["queues"], func(p map[string]any) bool { return p["id"] == q["parent_queue_item_id"] }), "owned_history_invalid")
	return parent["kind"] == "publish_task" && parent["channel_profile_id"] == q["channel_profile_id"] && historyObject(parent["payload_json"])["production_task_id"] == task["id"] && historyQueueState(parent, "running", "succeeded")
}
func historySettledReplacement(h map[string]any) (result map[string]any) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(ownedHistoryError); !ok {
				panic(p)
			}
			result = nil
		}
	}()
	pub := historyOne(historyArray(h["publications"]), "owned_history_invalid")
	uploaded, start := historyTime(pub["uploaded_at"]), historyTime(pub["scheduled_publish_at"])
	historyRequire(historyBefore(uploaded, start) && pub["desired_privacy"] == "unlisted" && pub["current_privacy"] == "unlisted" && pub["public_at"] == nil)
	promotes := historySelect(h["queues"], func(q map[string]any) bool { return q["kind"] == "promote_publication" })
	auto := historyOne(historySelect(promotes, func(q map[string]any) bool { return q["status"] == "cancelled" }), "owned_history_invalid")
	manual := historyOne(historySelect(promotes, func(q map[string]any) bool { return q["status"] != "cancelled" }), "owned_history_invalid")
	historyID(auto["id"])
	historyID(manual["id"])
	task := historyObject(h["task"])
	channel := task["channel_profile_id"]
	due := uploaded.Add(time.Hour)
	p := historyObject(auto["payload_json"])
	mp := historyObject(manual["payload_json"])
	historyRequire(auto["id"] != manual["id"] && historyTime(auto["run_after"]).Equal(due) && historyBetween(historyTime(auto["dead_letter_at"]), uploaded, historyTime(manual["run_after"])) && historyBefore(historyTime(manual["run_after"]), start) && auto["last_error"] == "replaced_by_immediate_unlisted_canary_promotion" && historyEqual(auto["attempt_count"], json.Number("0")) && auto["locked_at"] == nil && auto["locked_by"] == nil && auto["channel_profile_id"] == channel && auto["idempotency_key"] == "promote_publication:"+historyString(pub["id"])+":unlisted:"+historyZ(due) && p["publication_id"] == pub["id"] && p["target_visibility"] == "unlisted" && p["scheduled_at"] == historyZ(due))
	historyRequire(historyQueueState(manual, "succeeded") && manual["parent_queue_item_id"] == nil && manual["channel_profile_id"] == channel && manual["idempotency_key"] == "promote_publication:"+historyString(pub["id"])+":unlisted:manual" && mp["publication_id"] == pub["id"] && mp["target_visibility"] == "unlisted" && mp["channel_profile_id"] == channel && mp["scheduled_at"] == nil)
	parent := historyOne(historySelect(h["queues"], func(q map[string]any) bool { return q["id"] == auto["parent_queue_item_id"] }), "owned_history_invalid")
	historyRequire(parent["kind"] == "publish_task" && parent["channel_profile_id"] == channel && historyObject(parent["payload_json"])["production_task_id"] == task["id"] && historyQueueState(parent, "succeeded") && historyReconciled(h, pub, start))
	reconcile := historyOne(historySelect(h["queues"], func(q map[string]any) bool { return q["kind"] == "reconcile_publication" }), "owned_history_invalid")
	historyRequire(reconcile["parent_queue_item_id"] == manual["id"])
	return map[string]any{"automatic": auto, "manual": manual, "reconcile": reconcile, "publish_parent": parent}
}

var historyMetricStages = []struct {
	name       string
	due, grace int
}{{"1h", 1, 3}, {"6h", 6, 12}, {"24h", 24, 30}, {"72h", 72, 84}, {"7d", 168, 192}}

func historyMetricsReady(h, pub map[string]any, start, now time.Time) bool {
	reason := "owned_inventory_metrics"
	metrics := historyRows(h["metrics"])
	historyRequire(len(metrics) == len(historyMetricStages), reason)
	ids := historyIDs(h["metrics"])
	task := historyObject(h["task"])
	for _, q := range historyRows(h["queues"]) {
		if q["kind"] == "collect_metrics" {
			historyRequire(ids[historyReference(historyObject(q["payload_json"])["metric_schedule_id"])], reason)
		}
	}
	wait := false
	for _, stage := range historyMetricStages {
		m := historyOne(historySelect(h["metrics"], func(m map[string]any) bool { return m["snapshot_stage"] == stage.name }), reason)
		due, grace := start.Add(time.Duration(stage.due)*time.Hour), start.Add(time.Duration(stage.grace)*time.Hour)
		limit := now
		if grace.Before(limit) {
			limit = grace
		}
		historyRequire(m["publication_id"] == pub["id"] && historyTime(m["effective_start_at"]).Equal(start) && historyTime(m["due_at"]).Equal(due) && historyTime(m["grace_until"]).Equal(grace), reason)
		succeeded := m["status"] == "succeeded"
		attempts := historyInt(m["attempt_count"])
		last := attempts
		if succeeded {
			last--
		}
		historyRequire(historyIs(m["status"], "pending", "succeeded") && last >= 0 && last <= 1024, reason)
		var expectedError any
		if !succeeded && attempts != 0 {
			expectedError = "metrics_unavailable"
		}
		historyRequire(m["last_error_code"] == expectedError, reason)
		if attempts != 0 {
			historyRequire(historyBetween(historyTime(m["last_attempt_at"]), due, limit), reason)
		} else {
			historyRequire(m["last_attempt_at"] == nil, reason)
		}
		chain := historySelect(h["queues"], func(q map[string]any) bool {
			return q["kind"] == "collect_metrics" && historyObject(q["payload_json"])["metric_schedule_id"] == m["id"]
		})
		historyRequire(int64(len(chain)) == last+1, reason)
		sort.Slice(chain, func(i, j int) bool {
			return historyInt(historyObject(historyObject(chain[i])["payload_json"])["metrics_poll_count"]) < historyInt(historyObject(historyObject(chain[j])["payload_json"])["metrics_poll_count"])
		})
		for i, v := range chain {
			q := historyObject(v)
			p := historyObject(q["payload_json"])
			historyID(q["id"])
			historyRequire(historyInt(p["metrics_poll_count"]) == int64(i) && p["publication_id"] == pub["id"] && p["snapshot_stage"] == stage.name && q["channel_profile_id"] == task["channel_profile_id"] && q["idempotency_key"] == "collect_metrics:"+historyString(pub["id"])+":stage:"+stage.name+":attempt:"+strconv.Itoa(i), reason)
			run := historyTime(q["run_after"])
			historyRequire(historyBefore(run, grace), reason)
			if i > 0 {
				prior := historyObject(chain[i-1])
				historyRequire(run.After(historyTime(prior["run_after"])) && q["parent_queue_item_id"] == prior["id"], reason)
			} else {
				historyRequire(run.Equal(due), reason)
				parent := historyOne(historySelect(h["queues"], func(p map[string]any) bool { return p["id"] == q["parent_queue_item_id"] }), reason)
				pp := historyObject(parent["payload_json"])
				historyRequire(parent["kind"] == "promote_publication" && historyQueueState(parent, "succeeded") && parent["channel_profile_id"] == q["channel_profile_id"] && pp["publication_id"] == pub["id"] && pp["target_visibility"] == "unlisted", reason)
			}
			historyRequire(historyQueueState(q, "queued", "running", "succeeded"), reason)
			if q["status"] == "succeeded" {
				historyRequire(int64(i) < last || succeeded, reason)
			} else {
				if int64(i) < last {
					historyRequire(int64(i) == last-1 && q["status"] == "running" && !succeeded, reason)
				}
				wait = wait || succeeded || int64(i) < last
			}
		}
		feedback := historySelect(h["feedback"], func(f map[string]any) bool { return f["snapshot_stage"] == stage.name })
		if succeeded {
			done := historyTime(m["completed_at"])
			historyRequire(done.Equal(historyTime(m["last_attempt_at"])) && historyBetween(done, due, limit) && !done.Before(historyTime(historyObject(chain[len(chain)-1])["run_after"])) && len(feedback) == 1, reason)
		} else {
			historyRequire(m["completed_at"] == nil && now.Before(grace) && len(feedback) == 0, reason)
			if attempts != 0 {
				historyRequire(historyTime(historyObject(chain[len(chain)-1])["run_after"]).After(historyTime(m["last_attempt_at"])), reason)
			}
			wait = wait || !now.Before(due)
		}
	}
	for _, f := range historyRows(h["feedback"]) {
		historyRequire(historyIs(f["snapshot_stage"], "1h", "6h", "24h", "72h", "7d"), reason)
	}
	return wait
}
func historyNormal(h, item map[string]any, now time.Time) (string, bool, any) {
	task := historyObject(h["task"])
	reserved := item != nil && item["state"] == "reserved"
	var replacement map[string]any
	if item == nil {
		replacement = historySettledReplacement(h)
	}
	historyRequire(historyEqual(task["retry_count"], json.Number("0")) && task["failure_reason"] == nil && task["blocked_by_guard"] == nil && historyIs(task["state"], "selected", "planning", "producing", "scheduled", "uploaded_private", "measured"), "owned_inventory_task_failed")
	for _, q := range historyRows(h["queues"]) {
		if replacement != nil && q["id"] == historyObject(replacement["automatic"])["id"] {
			continue
		}
		historyRequire(historyQueueClean(q) && historyIs(q["status"], "queued", "running", "succeeded") && q["channel_profile_id"] == task["channel_profile_id"], "owned_inventory_queue_failed")
	}
	job := historyObject(h["job"])
	historyRequire(len(job) == 0 || historyIs(job["status"], "SUCCEEDED", "RUNNING", "PENDING", "WAITING_WINDOW", "VALIDATING", "PLANNING"), "owned_inventory_job_outcome")
	if len(historyArray(h["operations"])) == 0 && reserved {
		return "owned_inventory_outstanding", false, nil
	}
	op := historyOne(historyArray(h["operations"]), "owned_inventory_operation_count")
	historyRequire(op["production_task_id"] == task["id"] && historyEqual(op["job_id"], task["job_id"]) && historySHA.MatchString(historyString(op["content_sha256"])) && op["error_message"] == nil, "owned_inventory_operation_identity")
	if op["status"] != "succeeded" {
		historyRequire(reserved && historyIs(op["status"], "reserved", "attempted", "submitted"), "owned_inventory_operation_unresolved")
		return "owned_inventory_outstanding", false, nil
	}
	attempted, completed := historyTime(op["request_attempted_at"]), historyTime(op["completed_at"])
	receipt := historyExact(op["receipt_json"], "video_id url title privacy tags quota_estimate")
	historyID(op["manager_task_id"])
	video := historyString(op["platform_video_id"])
	historyRequire(historyBetween(completed, attempted, now) && historyVideo.MatchString(video) && historyIs(op["privacy"], "private", "unlisted") && historyEqual(receipt["privacy"], op["privacy"]) && historyEqual(receipt["title"], op["title"]) && receipt["video_id"] == video && receipt["url"] == "https://www.youtube.com/watch?v="+video, "owned_inventory_receipt")
	if job["status"] == "RUNNING" && reserved {
		return "owned_inventory_outstanding", false, nil
	}
	historyRequire(job["status"] == "SUCCEEDED" && job["id"] == op["job_id"] && job["error_message"] == nil && historyBetween(historyTime(job["completed_at"]), completed, now), "owned_inventory_job_receipt")
	uploads := 0
	for _, n := range historyRows(h["nodes"]) {
		historyRequire(n["status"] == "SUCCEEDED" && n["job_id"] == job["id"] && n["error_message"] == nil, "owned_inventory_node_outcome")
		if n["node_type"] == "youtube_upload" {
			uploads++
			historyRequire(n["id"] == op["node_execution_id"] && historyEqual(n["input_artifact_ids"], []any{op["input_artifact_id"]}) && historyBetween(historyTime(n["completed_at"]), completed, historyTime(job["completed_at"])), "owned_inventory_node_receipt")
			historyOne(historySelect(h["artifacts"], func(a map[string]any) bool {
				return a["id"] == n["output_artifact_id"] && a["node_execution_id"] == n["id"] && a["job_id"] == job["id"] && historyEqual(historyObject(a["media_info"])["youtube"], receipt)
			}), "owned_inventory_output_receipt")
		}
	}
	historyRequire(uploads == 1, "owned_inventory_upload_node_count")
	if len(historyArray(h["publications"])) == 0 && reserved {
		return "owned_inventory_outstanding", false, nil
	}
	pub := historyOne(historyArray(h["publications"]), "owned_inventory_publication_count")
	historyRequire(pub["production_task_id"] == task["id"] && pub["account_id"] == task["target_account_id"] && pub["platform"] == "youtube" && pub["platform_content_id"] == video && pub["desired_privacy"] == "unlisted" && pub["public_at"] == nil, "owned_inventory_publication_identity")
	if pub["current_privacy"] == "private" && reserved {
		return "owned_inventory_outstanding", false, nil
	}
	historyRequire(pub["current_privacy"] == "unlisted" && historyIs(pub["publish_status"], "uploaded", "scheduled"), "owned_inventory_publication_privacy")
	if pub["scheduled_publish_at"] == nil && reserved && historyPendingPromotion(h, pub, completed, now) {
		return "owned_inventory_outstanding", false, nil
	}
	start := historyTime(pub["scheduled_publish_at"])
	historyRequire(historyBetween(start, completed, now), "owned_inventory_publication_time")
	if !historyReconciled(h, pub, start) {
		return "owned_inventory_outstanding", false, nil
	}
	wait := ""
	if historyMetricsReady(h, pub, start, now) {
		wait = "owned_inventory_metrics_pending"
	}
	if now.Sub(attempted) < 24*time.Hour || now.Sub(completed) < 24*time.Hour {
		wait = "owned_inventory_cooldown"
	}
	var replaced any
	if replacement != nil {
		replaced = replacement
	}
	stable := map[string]any{"task": historyFields(task, "id channel_profile_id target_account_id manual_seed_id job_id"), "operations": h["operations"], "job": job, "nodes": h["nodes"], "artifacts": h["artifacts"], "publication": historyFields(pub, "id production_task_id account_id platform platform_content_id desired_privacy current_privacy public_at uploaded_at scheduled_publish_at"), "settled_promotion_replacement": replaced}
	return wait, reserved, stable
}

func historyTerminalGraph(rows, c map[string]any) map[string]any {
	g := map[string]any{}
	for _, name := range strings.Fields(historyTerminalTables) {
		g[name] = []any{}
	}
	nodes := historySelect(rows["node_executions"], func(n map[string]any) bool { return n["job_id"] == c["job_id"] || n["id"] == c["upload_node_id"] })
	nodeIDs := historyIDs(nodes)
	g["node_executions"] = nodes
	artifactIDs := map[string]bool{}
	for _, n := range historyRows(nodes) {
		for _, id := range historyArray(n["input_artifact_ids"]) {
			artifactIDs[historyString(id)] = true
		}
		if n["output_artifact_id"] != nil {
			artifactIDs[historyString(n["output_artifact_id"])] = true
		}
	}
	g["artifacts"] = historySelect(rows["artifacts"], func(a map[string]any) bool {
		return a["job_id"] == c["job_id"] || artifactIDs[historyReference(a["id"])]
	})
	atts, receipts, dispatches := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for {
		old := len(atts) + len(receipts) + len(dispatches)
		for _, table := range strings.Fields("worker_task_dispatches worker_task_delivery_attestations worker_event_emissions registered_worker_event_receipts") {
			g[table] = historySelect(rows[table], func(r map[string]any) bool {
				return r["job_id"] == c["job_id"] || nodeIDs[historyReference(r["node_execution_id"])] || atts[historyReference(r["source_task_attestation_id"])] || dispatches[historyReference(r["dispatch_key"])] || receipts[historyReference(r["origin_receipt_id"])] || receipts[historyReference(r["id"])] || dispatches[historyReference(historyDefaultObject(r, "payload_json")["task_dispatch_key"])]
			})
		}
		for _, r := range historyRows(g["worker_task_dispatches"]) {
			dispatches[historyString(r["dispatch_key"])] = true
			if historyTruth(r["origin_receipt_id"]) {
				receipts[historyString(r["origin_receipt_id"])] = true
			}
		}
		for _, r := range historyRows(g["worker_task_delivery_attestations"]) {
			atts[historyString(r["id"])] = true
		}
		for _, table := range []string{"worker_event_emissions", "registered_worker_event_receipts"} {
			for _, r := range historyRows(g[table]) {
				atts[historyString(r["source_task_attestation_id"])] = true
				if table == "registered_worker_event_receipts" {
					receipts[historyString(r["id"])] = true
				}
			}
		}
		if old == len(atts)+len(receipts)+len(dispatches) {
			break
		}
	}
	eventIDs := map[string]bool{}
	for _, table := range []string{"worker_event_emissions", "registered_worker_event_receipts"} {
		for _, r := range historyRows(g[table]) {
			eventIDs[historyEventIdentity(r)] = true
		}
	}
	g["registered_worker_event_deliveries"] = historySelect(rows["registered_worker_event_deliveries"], func(r map[string]any) bool {
		return atts[historyReference(r["source_task_attestation_id"])] || receipts[historyReference(r["receipt_id"])] || eventIDs[historyEventIdentity(r)]
	})
	regIDs := map[string]bool{}
	for _, table := range []string{"node_executions", "worker_task_delivery_attestations", "worker_event_emissions", "registered_worker_event_receipts"} {
		for _, r := range historyRows(g[table]) {
			if r["worker_registration_id"] != nil {
				regIDs[historyString(r["worker_registration_id"])] = true
			}
		}
	}
	g["worker_registrations"] = historySelect(rows["worker_registrations"], func(r map[string]any) bool { return regIDs[historyReference(r["id"])] })
	historyRequire(historySameSet(regIDs, historyIDs(g["worker_registrations"])), "owned_history_retired_orphan")
	grantIDs := map[string]bool{}
	for _, r := range historyRows(g["worker_registrations"]) {
		grantIDs[historyString(r["grant_id"])] = true
	}
	g["worker_admission_grants"] = historySelect(rows["worker_admission_grants"], func(r map[string]any) bool { return grantIDs[historyReference(r["id"])] })
	historyRequire(historySameSet(grantIDs, historyIDs(g["worker_admission_grants"])), "owned_history_retired_orphan")
	g["legacy_worker_event_resolutions"] = historySelect(rows["legacy_worker_event_resolutions"], func(r map[string]any) bool {
		return r["job_id"] == c["job_id"] || nodeIDs[historyReference(r["node_execution_id"])] || eventIDs[historyEventIdentity(r)]
	})
	g["channel_ops_queue_items"] = historySelect(rows["channel_ops_queue_items"], func(r map[string]any) bool {
		if r["channel_profile_id"] == c["legacy_channel_profile_id"] {
			return true
		}
		p := historyDefaultObject(r, "payload_json")
		return p["production_task_id"] == c["task_id"] || p["job_id"] == c["job_id"]
	})
	sourceIDs := map[string]bool{}
	for _, table := range []string{"worker_task_dispatches", "worker_event_emissions"} {
		for _, r := range historyRows(g[table]) {
			sourceIDs[historyString(r["id"])] = true
		}
	}
	for _, table := range []string{"worker_redis_marker_cleanup_authorizations", "worker_redis_marker_repair_audits"} {
		g[table] = historySelect(rows[table], func(r map[string]any) bool { return sourceIDs[historyReference(r["source_id"])] })
	}
	for _, v := range g {
		historySortRows(historyArray(v))
	}
	return g
}
func historyEventIdentity(r map[string]any) string {
	return historyCanonical([]any{historyGet(r, "redis_stream"), historyGet(r, "consumer_group"), r["message_id"]})
}
func historyTerminalProjection(g map[string]any) map[string]any {
	out := historyObject(historyCopy(g))
	for _, r := range historyRows(out["worker_registrations"]) {
		for _, k := range strings.Fields("heartbeat_at lease_expires_at status revoked_at revoke_reason superseded_by") {
			delete(r, k)
		}
	}
	for _, r := range historyRows(out["worker_admission_grants"]) {
		for _, k := range strings.Fields("state revoked_at revoke_reason updated_at") {
			delete(r, k)
		}
	}
	return out
}
func historyRedisTerminal(observations []ownedHistoryRedisObservation, kind string, stream, group, message, key, sha any, now time.Time) {
	matches := []ownedHistoryRedisObservation{}
	for _, r := range observations {
		if r.kind == kind && r.stream == stream && r.group == group && historyOptional(r.message) == message && historyOptional(r.key) == key && r.sha == sha {
			matches = append(matches, r)
		}
	}
	historyRequire(len(matches) == 1, "owned_history_retired_redis_missing")
	r := matches[0]
	var marker any
	if kind == "task" {
		marker = message
	}
	age := now.Sub(r.observedAt)
	historyRequire(r.pending == "[]" && historyOptional(r.marker) == marker && age >= 0 && age <= 60*time.Second, "owned_history_retired_redis_changed")
}
func historyClaimEqual(a, b map[string]any) bool {
	return historyEqual(historyFields(a, "job_id node_execution_id worker_registration_id worker_lease_epoch worker_id"), historyFields(b, "job_id node_execution_id worker_registration_id worker_lease_epoch worker_id")) && historyTime(a["worker_started_at"]).Equal(historyTime(b["worker_started_at"]))
}
func historyRedisHash(v any) string {
	m := historyObject(v)
	for _, v := range m {
		historyString(v)
	}
	return historyHash(m)
}

// Same pure envelope parser as parse_registered_worker_event, including native
// normalized claim fields. No receipt is manufactured or written here.
func historyParsedReceipt(r, att, dispatch map[string]any) {
	reason := "owned_history_retired_receipt"
	p := historyObject(r["payload_json"])
	sha := historyRedisHash(p)
	historyRequire(historyIs(p["event"], "node_completed", "node_failed"))
	for _, key := range strings.Fields("worker_id started_at worker_lease_epoch task_stream task_group task_message_id task_payload_sha256 task_dispatch_key") {
		historyRequire(strings.TrimSpace(historyString(p[key])) != "")
	}
	historyHashValue(p["task_payload_sha256"])
	leaseRaw := historyString(p["worker_lease_epoch"])
	historyRequire(strings.Trim(leaseRaw, "0123456789") == "")
	lease := historyInteger(json.Number(leaseRaw))
	historyRequire(lease.Sign() > 0)
	started, aware := historyAwareTime(historyString(p["started_at"]))
	historyRequire(aware)
	facts := map[string]any{"source_task_attestation_id": att["id"], "redis_stream": strings.TrimSpace(historyString(r["redis_stream"])), "consumer_group": strings.TrimSpace(historyString(r["consumer_group"])), "message_id": strings.TrimSpace(historyString(r["message_id"])), "payload_sha256": sha, "payload_json": p, "event_type": p["event"], "job_id": historyEventUUID(p["job_id"]), "node_execution_id": historyEventUUID(p["node_execution_id"]), "worker_registration_id": historyEventUUID(p["worker_registration_id"]), "worker_lease_epoch": json.Number(lease.String()), "worker_id": strings.TrimSpace(historyString(p["worker_id"])), "source_task_stream": strings.TrimSpace(historyString(p["task_stream"])), "source_task_group": strings.TrimSpace(historyString(p["task_group"])), "source_task_message_id": strings.TrimSpace(historyString(p["task_message_id"]))}
	for k, v := range facts {
		historyRequire(historyEqual(historyGet(r, k), v), reason)
	}
	historyRequire(historyTime(r["worker_started_at"]).Equal(started) && p["task_payload_sha256"] == dispatch["payload_sha256"] && historyEventUUID(p["task_dispatch_key"]) == dispatch["dispatch_key"], reason)
}
func historyEventUUID(v any) string {
	s := strings.ToLower(historyString(v))
	s = strings.TrimPrefix(s, "urn:uuid:")
	s = strings.Trim(s, "{}")
	s = strings.ReplaceAll(s, "-", "")
	historyRequire(len(s) == 32 && strings.Trim(s, "0123456789abcdef") == "")
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func historyReceiptPath(d, att, g, node, c map[string]any, observations []ownedHistoryRedisObservation, now time.Time) {
	reason := "owned_history_retired_receipt"
	certTime := historyUTC(c["observed_at"])
	historyRequire(att["job_id"] == c["job_id"] && att["node_execution_id"] == node["id"] && att["redis_stream"] == d["redis_stream"] && att["consumer_group"] == d["consumer_group"] && att["message_id"] == d["redis_message_id"] && att["payload_sha256"] == d["payload_sha256"] && att["dispatch_key"] == d["dispatch_key"] && att["ack_state"] == "acknowledged" && historyTime(att["acknowledged_at"]).Equal(historyTime(d["acknowledged_at"])), reason)
	match := func(r map[string]any) bool { return r["source_task_attestation_id"] == att["id"] }
	e := historyOne(historySelect(g["worker_event_emissions"], match), reason)
	r := historyOne(historySelect(g["registered_worker_event_receipts"], match), reason)
	deliveries := historySelect(g["registered_worker_event_deliveries"], match)
	historyRequire(len(deliveries) > 0 && historyClaimEqual(att, e) && historyClaimEqual(att, r) && e["emission_state"] == "resolved" && r["application_state"] == "applied" && r["ack_state"] == "acknowledged" && r["source_task_ack_state"] == "acknowledged", reason)
	if att["ack_event_emission_id"] == nil {
		historyRequire(historyBefore(historyTime(r["applied_at"]), historyTime(att["acknowledged_at"])), reason)
	} else {
		historyRequire(att["ack_event_emission_id"] == e["id"], reason)
	}
	historyParsedReceipt(r, att, d)
	for _, k := range strings.Fields("redis_stream consumer_group message_id payload_sha256 payload_json event_type") {
		historyRequire(historyEqual(e[k], r[k]), reason)
	}
	historyRequire(r["redis_stream"] == "vp:events" && r["consumer_group"] == "orchestrator", reason)
	for _, k := range strings.Fields("prepared_at emitted_at resolved_at") {
		historyRequire(historyBefore(historyTime(e[k]), certTime), reason)
	}
	historyRequire(historyBetween(historyTime(e["emitted_at"]), historyTime(e["prepared_at"]), historyTime(e["resolved_at"])) && historyBetween(historyTime(r["applied_at"]), historyTime(r["accepted_at"]), historyTime(r["acknowledged_at"])) && historyBefore(historyTime(r["acknowledged_at"]), certTime) && historyTime(r["source_task_acknowledged_at"]).Equal(historyTime(att["acknowledged_at"])) && historyBetween(historyTime(att["attested_at"]), historyTime(att["worker_started_at"]), historyTime(att["acknowledged_at"])), reason)
	seen := false
	for _, delivery := range historyRows(deliveries) {
		historyRequire(delivery["receipt_id"] == r["id"] && delivery["resolution_state"] == "accepted" && delivery["reason_code"] == nil && delivery["ack_state"] == "acknowledged" && historyBetween(historyTime(delivery["acknowledged_at"]), historyTime(delivery["accepted_at"]), certTime) && delivery["payload_sha256"] == r["payload_sha256"] && delivery["redis_stream"] == r["redis_stream"] && delivery["consumer_group"] == r["consumer_group"], reason)
		historyRedisTerminal(observations, "event", delivery["redis_stream"], delivery["consumer_group"], delivery["message_id"], nil, delivery["payload_sha256"], now)
		seen = seen || delivery["message_id"] == r["message_id"]
	}
	historyRequire(seen, reason)
	reg := historyOne(historySelect(g["worker_registrations"], func(r map[string]any) bool { return r["id"] == att["worker_registration_id"] }), reason)
	grant := historyOne(historySelect(g["worker_admission_grants"], func(g map[string]any) bool { return g["id"] == reg["grant_id"] }), reason)
	historyRequire(reg["redis_consumer_id"] == att["worker_id"] && historyInteger(att["worker_lease_epoch"]).Sign() > 0 && historyInteger(att["worker_lease_epoch"]).Cmp(historyInteger(reg["lease_epoch"])) <= 0 && historyBetween(historyTime(reg["registered_at"]), historyTime(grant["activated_at"]), historyTime(att["worker_started_at"])) && historyEqual(historyFields(reg, "service_name worker_type worker_host capabilities_json image_identity database_principal"), historyFields(grant, "service_name worker_type worker_host capabilities_json image_identity database_principal")) && grant["redis_stream"] == d["redis_stream"] && grant["redis_group"] == d["consumer_group"], reason)
	for _, k := range strings.Fields("database_fingerprint redis_fingerprint storage_fingerprint") {
		historyHashValue(reg[k])
	}
	historyRequire(node["worker_registration_id"] == att["worker_registration_id"] && historyEqual(node["worker_lease_epoch"], att["worker_lease_epoch"]) && historyTime(node["started_at"]).Equal(historyTime(att["worker_started_at"])), reason)
	if r["event_type"] == "node_completed" {
		historyRequire(node["status"] == "SUCCEEDED" && node["worker_id"] == att["worker_id"] && historyObject(r["payload_json"])["output_artifact_id"] == node["output_artifact_id"], reason)
	} else {
		historyRequire(r["event_type"] == "node_failed" && node["id"] == c["upload_node_id"] && node["status"] == "CANCELLED" && node["worker_id"] == nil, reason)
	}
}

// Frozen NodeTypeRegistry routing, not worker capabilities or operator input.
func historyWorkerType(nodeType string) string {
	switch nodeType {
	case "source":
		return "none"
	case "trim", "transcode", "bgm", "concat_horizontal", "concat_vertical", "montage_assembler", "replace_audio", "export", "title_overlay", "concat_many", "concat_vertical_timeline", "concat_timeline", "watermark", "vertical_crop":
		return "ffmpeg_go"
	case "youtube_upload":
		return "youtube_publisher"
	case "smart_trim":
		return "vision"
	case "zip_records", "x_search", "youtube_search", "bilibili_search", "material_search", "xiaohongshu_search":
		return "planner"
	case "url_download", "x_upload", "xiaohongshu_upload", "material_library_ingest", "subtitle", "speech_to_subtitle", "subtitle_translate", "subtitle_to_speech":
		return "ffmpeg"
	}
	return ""
}

type historyPipeline struct {
	nodes map[string]map[string]any
	edges []map[string]any
}

func historyParsePipeline(v any, reason string) historyPipeline {
	m := historyObject(v)
	nodes := historyRows(historyGet(m, "nodes"))
	edges := historyRows(historyGet(m, "edges"))
	byName := map[string]map[string]any{}
	for _, n := range nodes {
		historyString(n["id"])
		historyString(n["type"])
		for _, x := range historyObject(n["position"]) {
			historyNumber(x)
		}
		data := historyObject(n["data"])
		if x, ok := data["label"]; ok {
			historyString(x)
		}
		if x, ok := data["asset_id"]; ok && x != nil {
			historyString(x)
		}
		if x, ok := data["config"]; ok {
			historyObject(x)
		}
		byName[historyString(n["id"])] = n
	}
	if viewport, ok := m["viewport"]; ok {
		for _, x := range historyObject(viewport) {
			historyNumber(x)
		}
	}
	degree := map[string]int{}
	seen := map[string]bool{}
	for name := range byName {
		degree[name] = 0
	}
	for _, e := range edges {
		for _, k := range strings.Fields("id source target sourceHandle targetHandle") {
			historyString(e[k])
		}
		id := historyString(e["id"])
		historyRequire(!seen[id] && byName[historyString(e["source"])] != nil && byName[historyString(e["target"])] != nil, reason)
		seen[id] = true
		degree[historyString(e["target"])]++
	}
	ready := []string{}
	for id, d := range degree {
		if d == 0 {
			ready = append(ready, id)
		}
	}
	count := 0
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		count++
		for _, e := range edges {
			if e["source"] == id {
				target := historyString(e["target"])
				degree[target]--
				if degree[target] == 0 {
					ready = append(ready, target)
				}
			}
		}
	}
	historyRequire(count == len(nodes), reason)
	return historyPipeline{byName, edges}
}
func historyNumber(v any) {
	switch v := v.(type) {
	case json.Number:
		_, err := strconv.ParseFloat(string(v), 64)
		historyRequire(err == nil)
	case string:
		_, err := strconv.ParseFloat(v, 64)
		historyRequire(err == nil)
	case bool:
	default:
		historyRequire(false)
	}
}
func historyUpstream(p historyPipeline, byName map[string]map[string]any, node map[string]any, reason string) map[string]any {
	out := map[string]any{}
	count := 0
	for _, e := range p.edges {
		if e["target"] == node["node_id"] {
			count++
			up := byName[historyString(e["source"])]
			historyRequire(up["status"] == "SUCCEEDED", reason)
			out[historyString(e["targetHandle"])] = up["output_artifact_id"]
		}
	}
	historyRequire(len(out) == count, reason)
	values := []string{}
	for _, v := range out {
		values = append(values, historyString(v))
	}
	sort.Strings(values)
	inputs := []string{}
	for _, v := range historyArray(node["input_artifact_ids"]) {
		inputs = append(inputs, historyString(v))
	}
	sort.Strings(inputs)
	historyRequire(slices.Equal(values, inputs), reason)
	return out
}

func historyAssessRetired(rows, c map[string]any, snapshot ownedHistorySnapshot, now time.Time) ([]string, []string, []ownedHistoryTerminalPath) {
	reason := "owned_history_retired_changed"
	certTime := historyUTC(c["observed_at"])
	historyRequire(historyBetween(snapshot.observedAt, certTime, now), reason)
	retained := historyObject(c["retained_facts"])
	current := map[string]map[string]any{}
	for _, p := range [][3]string{{"operation", "youtube_upload_operations", "operation_id"}, {"task", "production_tasks", "task_id"}, {"job", "jobs", "job_id"}, {"upload_node", "node_executions", "upload_node_id"}, {"account", "publishing_accounts", "legacy_account_id"}, {"channel", "channel_profiles", "legacy_channel_profile_id"}, {"manual_seed", "manual_seeds", ""}} {
		id := c[p[2]]
		if p[0] == "manual_seed" {
			id = historyObject(retained["task"])["manual_seed_id"]
		}
		r := historyOne(historySelect(rows[p[1]], func(r map[string]any) bool { return r["id"] == id }), "owned_history_retired_orphan")
		historyCompleteRow(r, p[1])
		historyRequire(historyBytesEqual(r, retained[p[0]]), reason)
		current[p[0]] = r
	}
	op, task, job, upload, account, channel, seed := current["operation"], current["task"], current["job"], current["upload_node"], current["account"], current["channel"], current["manual_seed"]
	historyRequire(op["production_task_id"] == task["id"] && op["job_id"] == job["id"] && task["job_id"] == job["id"] && op["node_execution_id"] == upload["id"] && task["target_account_id"] == account["id"] && task["channel_profile_id"] == channel["id"] && account["channel_profile_id"] == channel["id"] && historyPlatform(account) == "youtube" && account["platform_account_id"] == "", reason)
	historyRequire(op["status"] == "reserved" && op["privacy"] == "unlisted" && historyEqual(op["receipt_json"], map[string]any{}), reason)
	for _, k := range strings.Fields("request_attempted_at manager_task_id platform_video_id completed_at error_message") {
		historyRequire(op[k] == nil, reason)
	}
	historyHashValue(op["content_sha256"])
	historyRequire(task["state"] == "held" && task["blocked_by_guard"] == "operator_canary_failure" && task["failure_reason"] == "operator_canary_failure" && job["status"] == "CANCELLED" && upload["status"] == "CANCELLED" && job["error_message"] == "operator_canary_failure" && upload["error_message"] == "operator_canary_failure" && channel["halt_reason"] == "operator_canary_failure" && channel["intake_paused_at"] != nil && upload["worker_id"] == nil, reason)
	transitions := historyArray(task["transition_history_json"])
	historyRequire(len(transitions) > 0)
	transition := historyExact(transitions[len(transitions)-1], "from to actor at")
	cancelled := historyTime(job["completed_at"])
	historyRequire(transition["from"] == "producing" && transition["to"] == "held" && transition["actor"] == "operator_canary_failure" && historyTime(transition["at"]).Equal(cancelled) && historyTime(task["state_updated_at"]).Equal(cancelled) && historyTime(upload["completed_at"]).Equal(cancelled) && historyBetween(historyTime(channel["halted_at"]), historyTime(channel["intake_paused_at"]), cancelled) && historyBefore(cancelled, certTime), reason)
	for _, table := range []string{"publication_records", "publication_promotion_operations"} {
		historyRequire(len(historySelect(rows[table], func(p map[string]any) bool { return p["production_task_id"] == task["id"] })) == 0, reason)
	}
	historyRequire(len(historySelect(rows["jobs"], func(j map[string]any) bool {
		return j["id"] != job["id"] && historyGet(j, "parent_job_id") == job["id"]
	})) == 0 && len(historySelect(rows["production_tasks"], func(t map[string]any) bool {
		return t["id"] != task["id"] && (t["target_account_id"] == account["id"] || t["channel_profile_id"] == channel["id"] || t["job_id"] == job["id"])
	})) == 0 && len(historySelect(rows["runtime_schedules"], func(s map[string]any) bool { return s["guarded_job_id"] == job["id"] })) == 0, reason)
	g := historyTerminalGraph(rows, c)
	historyParseGraph(g)
	historyRequire(historyEqual(historyTerminalProjection(g), historyTerminalProjection(historyObject(c["terminal_graph"]))), reason)
	historyRequire(len(historyArray(g["legacy_worker_event_resolutions"])) == 0, "owned_history_retired_unsupported_legacy_resolution")
	historyRequire(len(historyArray(g["worker_redis_marker_cleanup_authorizations"])) == 0 && len(historyArray(g["worker_redis_marker_repair_audits"])) == 0, "owned_history_retired_marker_maintenance")
	queueIDs := historyIDs(g["channel_ops_queue_items"])
	for _, q := range historyRows(g["channel_ops_queue_items"]) {
		historyRequire(q["channel_profile_id"] == channel["id"] && q["locked_at"] == nil && q["locked_by"] == nil && (q["status"] == "succeeded" && historyQueueClean(q) && historyInt(q["attempt_count"]) == 1 || q["status"] == "dead_lettered" && q["last_error"] == "operator_canary_failure" && historyTime(q["dead_letter_at"]).Equal(cancelled) && historyInt(q["attempt_count"]) >= 0 && historyInt(q["attempt_count"]) <= 1), reason)
		historyRequire(q["parent_queue_item_id"] == nil || queueIDs[historyReference(q["parent_queue_item_id"])], reason)
	}
	nodes := historyIndex(g["node_executions"])
	pipeline := historyParsePipeline(job["pipeline_snapshot"], reason)
	historyRequire(len(nodes) == len(pipeline.nodes), reason)
	byName := map[string]map[string]any{}
	for _, n := range nodes {
		byName[historyString(n["node_id"])] = n
	}
	historyRequire(len(byName) == len(nodes), reason)
	for name := range byName {
		historyRequire(pipeline.nodes[name] != nil, reason)
	}
	sourceIDs := map[string]bool{}
	paths := []ownedHistoryTerminalPath{}
	artifacts := historyIndex(g["artifacts"])
	for _, id := range historySortedKeys(nodes) {
		n := nodes[id]
		specified := pipeline.nodes[historyString(n["node_id"])]
		data := historyObject(specified["data"])
		config := map[string]any{}
		if v, ok := data["config"]; ok {
			config = historyObject(historyCopy(v))
		}
		if historyTruth(data["asset_id"]) {
			config["asset_id"] = data["asset_id"]
		}
		historyRequire(n["job_id"] == job["id"] && n["node_type"] == specified["type"] && historyEqual(n["node_config"], config) && historyIs(n["status"], "SUCCEEDED", "CANCELLED") && historyBefore(historyTime(n["completed_at"]), cancelled), reason)
		if n["status"] == "CANCELLED" {
			historyRequire(n["worker_id"] == nil && n["error_message"] == "operator_canary_failure" && historyTime(n["completed_at"]).Equal(cancelled) && n["output_artifact_id"] == nil, reason)
		} else {
			historyRequire(n["error_message"] == nil && artifacts[historyReference(n["output_artifact_id"])] != nil && artifacts[historyReference(n["output_artifact_id"])]["node_execution_id"] == n["id"], reason)
		}
		related := historySelect(g["worker_task_dispatches"], func(d map[string]any) bool { return d["node_execution_id"] == n["id"] })
		historyUpstream(pipeline, byName, n, reason)
		if n["node_type"] == "source" {
			historyRequire(n["status"] == "SUCCEEDED" && len(related) == 0 && len(historyArray(n["input_artifact_ids"])) == 0 && n["worker_registration_id"] == nil && n["worker_lease_epoch"] == nil && n["worker_id"] == nil && historyBefore(historyTime(n["started_at"]), historyTime(n["completed_at"])), reason)
			assetID := historyString(historyObject(n["node_config"])["asset_id"])
			sourceIDs[assetID] = true
			s := historyOne(historySelect(retained["source_assets"], func(s map[string]any) bool { return historyObject(s["asset"])["id"] == assetID }), reason)
			source := historyObject(s["asset"])
			fresh := historyOne(historySelect(rows["assets"], func(a map[string]any) bool { return a["id"] == assetID }), reason)
			artifact := artifacts[historyString(n["output_artifact_id"])]
			historyRequire(historyEqual(fresh, source) && historyEqual(historyFields(artifact, "filename mime_type file_size storage_backend storage_path"), historyFields(source, "filename mime_type file_size storage_backend storage_path")) && historyObject(artifact["media_info"])["source_asset_id"] == assetID && historyObject(artifact["media_info"])["asset_id"] == assetID, reason)
			paths = append(paths, ownedHistoryTerminalPath{id, "synchronous_source"})
		} else {
			historyRequire(len(related) > 0, reason)
		}
	}
	retainedIDs := map[string]bool{}
	sourceHashes := map[string]bool{}
	for _, s := range historyRows(retained["source_assets"]) {
		retainedIDs[historyString(historyObject(s["asset"])["id"])] = true
		sourceHashes[historyString(s["content_sha256"])] = true
	}
	historyRequire(len(sourceIDs) > 0 && historySameSet(sourceIDs, retainedIDs) && seed["channel_profile_id"] == channel["id"] && seed["target_account_id"] == account["id"], reason)
	constraints := historyObject(seed["constraints_json"])
	seedAssets := map[string]bool{}
	if historyTruth(constraints["input_asset_ids"]) {
		seedAssets = historySet(historyArray(constraints["input_asset_ids"]))
	}
	if historyTruth(constraints["input_asset_id"]) {
		seedAssets[historyString(constraints["input_asset_id"])] = true
	}
	historyRequire(historySameSet(seedAssets, sourceIDs) && artifacts[historyReference(op["input_artifact_id"])] != nil && historyEqual(upload["input_artifact_ids"], []any{op["input_artifact_id"]}), reason)
	for _, a := range artifacts {
		historyRequire(a["job_id"] == job["id"] && nodes[historyReference(a["node_execution_id"])] != nil, reason)
	}
	dispatchKeys, origins := map[string]bool{}, map[string]bool{}
	for _, d := range historyRows(g["worker_task_dispatches"]) {
		key := historyString(d["dispatch_key"])
		historyRequire(!dispatchKeys[key], reason)
		dispatchKeys[key] = true
		if d["origin_receipt_id"] != nil {
			pair := historyCanonical([]any{d["origin_receipt_id"], d["node_execution_id"]})
			historyRequire(!origins[pair], reason)
			origins[pair] = true
		}
	}
	for _, a := range historyRows(g["worker_task_delivery_attestations"]) {
		historyRequire(dispatchKeys[historyReference(a["dispatch_key"])], reason)
	}
	attIDs := historyIDs(g["worker_task_delivery_attestations"])
	for _, table := range []string{"worker_event_emissions", "registered_worker_event_receipts", "registered_worker_event_deliveries"} {
		for _, r := range historyRows(g[table]) {
			historyRequire(attIDs[historyReference(r["source_task_attestation_id"])], reason)
		}
	}
	observations := snapshot.observations()
	expectedRedis := map[string]bool{}
	for _, d := range historyRows(g["worker_task_dispatches"]) {
		n := nodes[historyReference(d["node_execution_id"])]
		historyRequire(n != nil)
		p := historyObject(d["payload_json"])
		worker := historyWorkerType(historyString(n["node_type"]))
		historyRequire(worker != "" && d["job_id"] == job["id"] && d["redis_stream"] == "vp:tasks:"+worker && d["consumer_group"] == worker+"-workers" && p["job_id"] == job["id"] && p["node_execution_id"] == n["id"] && p["node_id"] == n["node_id"] && p["node_type"] == n["node_type"] && p["dispatch_key"] == d["dispatch_key"] && historyRedisHash(p) == d["payload_sha256"] && historyEqual(historyDecode([]byte(historyString(p["config"]))), n["node_config"]), reason)
		inputs := historyObject(historyDecode([]byte(historyString(p["input_artifacts"]))))
		expectedInputs := historyUpstream(pipeline, byName, n, reason)
		historyRequire(historyEqual(inputs, expectedInputs), reason)
		for _, id := range inputs {
			historyRequire(artifacts[historyReference(id)] != nil, reason)
		}
		atts := historySelect(g["worker_task_delivery_attestations"], func(a map[string]any) bool { return a["dispatch_key"] == d["dispatch_key"] })
		historyRequire(d["delivery_error"] == nil && historyBefore(historyTime(d["created_at"]), certTime), reason)
		path := ""
		if d["resolution_state"] == "cancelled" {
			historyRequire(len(atts) == 0 && n["status"] == "CANCELLED" && historyIs(d["delivery_state"], "pending", "cancelled") && historyBetween(historyTime(d["cancelled_at"]), cancelled, certTime), reason)
			for _, k := range strings.Fields("delivery_attempted_at redis_message_id delivered_at acknowledged_at") {
				historyRequire(d[k] == nil, reason)
			}
			for _, k := range strings.Fields("worker_registration_id worker_lease_epoch worker_id started_at") {
				historyRequire(n[k] == nil, reason)
			}
			path = "never_delivered_cancelled"
		} else {
			historyRequire(d["delivery_state"] == "delivered" && d["resolution_state"] == "acknowledged" && d["cancelled_at"] == nil && historyRedisID.MatchString(historyString(d["redis_message_id"])) && historyBetween(historyTime(d["delivered_at"]), historyTime(d["delivery_attempted_at"]), historyTime(d["acknowledged_at"])) && historyBefore(historyTime(d["acknowledged_at"]), certTime), reason)
			if len(atts) > 0 {
				historyReceiptPath(d, historyOne(atts, reason), g, n, c, observations, now)
				path = "receipt_backed"
			} else {
				origin := historyOne(historySelect(g["registered_worker_event_receipts"], func(r map[string]any) bool { return r["id"] == d["origin_receipt_id"] }), reason)
				historyRequire(n["id"] == c["upload_node_id"] && n["status"] == "CANCELLED" && origin["event_type"] == "node_failed" && origin["node_execution_id"] == n["id"] && origin["application_state"] == "applied" && origin["ack_state"] == "acknowledged" && !historyTime(d["acknowledged_at"]).Before(cancelled) && len(historySelect(g["worker_event_emissions"], func(e map[string]any) bool {
					return historyObject(e["payload_json"])["task_dispatch_key"] == d["dispatch_key"]
				})) == 0, reason)
				original := historyOne(historySelect(g["worker_task_delivery_attestations"], func(a map[string]any) bool { return a["id"] == origin["source_task_attestation_id"] }), reason)
				historyRequire(original["dispatch_key"] != d["dispatch_key"] && original["worker_registration_id"] == n["worker_registration_id"] && historyEqual(original["worker_lease_epoch"], n["worker_lease_epoch"]) && historyTime(original["worker_started_at"]).Equal(historyTime(n["started_at"])), reason)
				path = "delivered_cancelled_ack"
			}
		}
		paths = append(paths, ownedHistoryTerminalPath{historyString(d["id"]), path})
		historyRedisTerminal(observations, "task", d["redis_stream"], d["consumer_group"], d["redis_message_id"], d["dispatch_key"], d["payload_sha256"], now)
		expectedRedis[historyCanonical([]any{"task", d["redis_stream"], d["consumer_group"], d["redis_message_id"], d["dispatch_key"]})] = true
	}
	for _, d := range historyRows(g["registered_worker_event_deliveries"]) {
		expectedRedis[historyCanonical([]any{"event", d["redis_stream"], d["consumer_group"], d["message_id"], nil})] = true
	}
	actualRedis := map[string]bool{}
	for _, r := range observations {
		actualRedis[historyCanonical([]any{r.kind, r.stream, r.group, historyOptional(r.message), historyOptional(r.key)})] = true
	}
	historyRequire(historySameSet(expectedRedis, actualRedis) && len(expectedRedis) == len(observations), reason)
	return historySorted(sourceHashes), []string{historyString(op["content_sha256"])}, paths
}

func assessOwnedHistorySnapshot(snapshot ownedHistorySnapshot, now time.Time) ownedHistoryAssessment {
	return assessOwnedHistory(snapshot, now, "")
}

// Only the current task's cooldown/outstanding pass is excluded. Its operations
// remain in complete identity classification and retained-proof validation.
func assessOwnedProducerHistory(snapshot ownedHistorySnapshot, now time.Time, currentTaskID string) ownedHistoryAssessment {
	if currentTaskID == "" {
		reason := "owned_inventory_producer_missing"
		return historyEmptyAssessment(&reason)
	}
	return assessOwnedHistory(snapshot, now, currentTaskID)
}

func assessOwnedHistory(snapshot ownedHistorySnapshot, now time.Time, currentTaskID string) (result ownedHistoryAssessment) {
	result = historyEmptyAssessment(nil)
	defer func() {
		if p := recover(); p != nil {
			if e, ok := p.(ownedHistoryError); ok {
				code := string(e)
				result = historyEmptyAssessment(&code)
			} else {
				panic(p)
			}
		}
	}()
	age := now.Sub(snapshot.observedAt)
	historyRequire(age >= 0 && age <= 60*time.Second, "owned_history_observation_stale")
	rows := snapshot.rows()
	tasks, accounts, channels := historyIndex(rows["production_tasks"]), historyIndex(rows["publishing_accounts"]), historyIndex(rows["channel_profiles"])
	historyRequire(currentTaskID == "" || tasks[currentTaskID] != nil, "owned_inventory_producer_missing")
	bindings, cert, authority := historyApprovedAuthority(rows, now)
	if cert != nil {
		result.RetiredSourceSHA256, result.RetiredRenderSHA256, result.TerminalPaths = historyAssessRetired(rows, cert, snapshot, now)
	}
	for _, op := range historyRows(rows["youtube_upload_operations"]) {
		task := tasks[historyReference(op["production_task_id"])]
		account := accounts[historyReference(task["target_account_id"])]
		channel := channels[historyReference(task["channel_profile_id"])]
		historyRequire(task != nil && account != nil && channel != nil && account["channel_profile_id"] == channel["id"], "owned_history_orphan")
		classification := ownedHistoryClassification{OperationID: historyString(op["id"]), AccountID: historyString(account["id"])}
		if cert != nil && op["id"] == cert["operation_id"] {
			classification.Classification = "retired_unassigned_preupload"
		} else if b := bindings[historyString(account["id"])]; b != nil {
			classification.Classification = "history_only"
			classification.PlatformChannelID = historyNullableString(b["canonical_platform_channel_id"])
		} else {
			identity, ok := account["platform_account_id"].(string)
			historyRequire(historyPlatform(account) == "youtube" && ok && historyUC.MatchString(identity), "owned_history_unclassified")
			classification.Classification = "direct"
			classification.PlatformChannelID = &identity
		}
		result.Classifications = append(result.Classifications, classification)
	}
	members := map[string]bool{}
	for id, a := range accounts {
		if historyPlatform(a) == "youtube" && a["platform_account_id"] == snapshot.platformChannelID {
			members[id] = true
		}
	}
	for id, b := range bindings {
		if b["canonical_platform_channel_id"] == snapshot.platformChannelID {
			members[id] = true
		}
	}
	result.AccountIDs = historySorted(members)
	items := map[string]map[string]any{}
	itemCount := 0
	for _, i := range historyRows(rows["owned_seed_inventory_items"]) {
		if i["production_task_id"] != nil && i["platform_channel_id"] == snapshot.platformChannelID {
			items[historyString(i["production_task_id"])] = i
			itemCount++
		}
	}
	for id := range items {
		historyRequire(tasks[id] != nil, "owned_inventory_missing_task")
	}
	historyRequire(len(items) == itemCount, "owned_history_item_authority")
	for _, id := range historySortedKeys(items) {
		item := items[id]
		inv := historyOne(historySelect(rows["owned_seed_inventories"], func(r map[string]any) bool { return r["id"] == item["inventory_id"] && r["approved_at"] != nil }), "owned_history_item_authority")
		entry := historyOne(historySelect(historyObject(inv["manifest_json"])["entries"], func(e map[string]any) bool { return e["id"] == item["id"] }), "owned_history_item_authority")
		historyRequire(historyEqual(historyFields(entry, "manual_seed_id asset_id content_sha256"), historyFields(item, "manual_seed_id asset_id content_sha256")) && inv["platform_channel_id"] == snapshot.platformChannelID && inv["channel_profile_id"] == tasks[id]["channel_profile_id"] && inv["target_account_id"] == tasks[id]["target_account_id"], "owned_history_item_authority")
	}
	stable := []any{}
	for _, id := range historySortedKeys(tasks) {
		if id == currentTaskID {
			continue
		}
		task := tasks[id]
		item := items[id]
		if !members[historyReference(task["target_account_id"])] && item == nil {
			continue
		}
		h := historyTask(rows, task)
		if historyIs(task["state"], "held", "failed", "rejected") && len(historyArray(h["operations"])) == 0 && item == nil {
			continue
		}
		if item != nil {
			historyRequire(task["manual_seed_id"] == item["manual_seed_id"], "owned_inventory_history_identity")
		}
		wait, complete, effect := historyNormal(h, item, now)
		if wait != "" {
			result.WaitReason = &wait
		}
		if complete {
			historyRequire(item != nil)
			result.CompletedItemIDs = append(result.CompletedItemIDs, historyString(item["id"]))
		}
		if effect != nil {
			stable = append(stable, effect)
		}
	}
	result.AuthoritySHA256 = historyHash(authority)
	result.StableHistorySHA256 = historyHash(stable)
	return result
}
