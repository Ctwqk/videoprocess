package channelops

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var historyGoldenNames = []string{"direct", "history_only", "retired_unassigned", "pending_promotion", "metrics_pending_retry", "metrics_recovered", "promotion_replacement", "v1_unclassified"}

func historyGolden(t *testing.T, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "backend", "tests", "fixtures", "owned_seed_inventory_history", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	v, err := ownedDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	f := v.(map[string]any)
	if f["synthetic"] != true || f["contract_version"] != json.Number("1") {
		t.Fatal("not a synthetic v1 contract fixture")
	}
	return f
}

func historyTestRows(f map[string]any) map[string]any { return f["rows"].(map[string]any) }
func historyTestFirst(f map[string]any, table string) map[string]any {
	return historyTestRows(f)[table].([]any)[0].(map[string]any)
}
func historyTestManifest(f map[string]any) map[string]any {
	return historyTestFirst(f, "owned_seed_inventories")["manifest_json"].(map[string]any)
}
func historyTestUID(n int) string { return fmt.Sprintf("00000000-0000-0000-0000-%012d", n) }
func historyTestCopy(t *testing.T, v any) any {
	t.Helper()
	r, err := ownedDecode(historyJSON(t, v))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func historyTestHash(t *testing.T, v any) string {
	t.Helper()
	s, err := ownedHash(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func historyTestRehash(t *testing.T, f map[string]any) {
	t.Helper()
	inv := historyTestFirst(f, "owned_seed_inventories")
	inv["manifest_sha256"] = historyTestHash(t, inv["manifest_json"])
}
func historyTestAssess(t *testing.T, f map[string]any) ownedHistoryAssessment {
	t.Helper()
	return assessOwnedHistorySnapshot(historySnapshot(t, f), historyAt(t, f["now"]))
}
func historyTestReason(t *testing.T, f map[string]any, want string) {
	t.Helper()
	got := historyTestAssess(t, f)
	if got.BlockReason == nil {
		t.Fatalf("accepted invalid history; want %s", want)
	}
	if want != "" && *got.BlockReason != want {
		t.Fatalf("reason %s, want %s", *got.BlockReason, want)
	}
	if len(got.Classifications)+len(got.AccountIDs)+len(got.RetiredSourceSHA256)+len(got.RetiredRenderSHA256)+len(got.CompletedItemIDs)+len(got.TerminalPaths) != 0 || got.AuthoritySHA256 != "" || got.StableHistorySHA256 != "" || got.WaitReason != nil {
		t.Fatal("failure leaked partial assessment")
	}
}

func historyTestFullAssessment(t *testing.T, f map[string]any, want any) {
	t.Helper()
	defer func() {
		if p := recover(); p != nil {
			t.Fatalf("constructor/assessor panicked instead of returning an assessment: %v", p)
		}
	}()
	raw := mustHistoryAssessmentJSON(t, historyTestAssess(t, f))
	got, err := ownedDecode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !ownedEqual(got, want) {
		t.Fatalf("full assessment\ngot %s\nwant %s", raw, historyJSON(t, want))
	}
}

func historyTestRefusal(reason string) map[string]any {
	return map[string]any{
		"block_reason": reason, "classifications": []any{}, "account_ids": []any{},
		"retired_source_sha256": []any{}, "retired_render_sha256": []any{},
		"authority_sha256": "", "stable_history_sha256": "", "wait_reason": nil,
		"completed_item_ids": []any{}, "terminal_paths": []any{},
	}
}

func TestOwnedHistoryReviewR1StructuredJobReferences(t *testing.T) {
	for _, structured := range []bool{false, true} {
		t.Run(fmt.Sprint(structured), func(t *testing.T) {
			f := historyGolden(t, "direct")
			want := f["expected"]
			if structured {
				historyTestFirst(f, "production_tasks")["job_id"] = map[string]any{}
				historyTestFirst(f, "youtube_upload_operations")["job_id"] = map[string]any{}
				want = historyTestRefusal("owned_inventory_job_receipt")
			}
			historyTestFullAssessment(t, f, want)
		})
	}
}

func TestOwnedHistoryReviewR3NumericTruth(t *testing.T) {
	for _, field := range []string{"approved_by", "approval_reference"} {
		for _, variant := range []string{"control", "negative_zero", "one"} {
			t.Run(field+"/"+variant, func(t *testing.T) {
				f := historyGolden(t, "history_only")
				want := historyTestCopy(t, f["expected"]).(map[string]any)
				switch variant {
				case "negative_zero":
					historyTestFirst(f, "owned_seed_inventories")[field] = json.Number("-0.0")
					want = historyTestRefusal("owned_history_authority_invalid")
				case "one":
					historyTestFirst(f, "owned_seed_inventories")[field] = json.Number("1")
					// Complete frozen Python assessments differ only in this authority hash.
					want["authority_sha256"] = map[string]string{
						"approved_by":        "9ee1966f4e143a0e7b195964df6a4c44a8724917295e12e21dc673efdbb5caf2",
						"approval_reference": "a03002851b157dfe9c44756db98b6da9078503aae0f3fd36821e3dd7141f1751",
					}[field]
				}
				historyTestFullAssessment(t, f, want)
			})
		}
	}
	for _, negativeZero := range []bool{false, true} {
		t.Run("running/"+fmt.Sprint(negativeZero), func(t *testing.T) {
			f := historyGolden(t, "direct")
			q := historyTestFind(t, f, "channel_ops_queue_items", "kind", "reconcile_publication")
			q["status"], q["attempt_count"], q["locked_at"], q["locked_by"] = "running", json.Number("1"), f["now"], "fixture-owner"
			want := historyTestCopy(t, f["expected"]).(map[string]any)
			want["wait_reason"] = "owned_inventory_outstanding"
			want["stable_history_sha256"] = "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
			if negativeZero {
				q["locked_by"] = json.Number("-0.0")
				want = historyTestRefusal("owned_inventory_reconciliation")
			}
			historyTestFullAssessment(t, f, want)
		})
	}
}

func TestOwnedHistoryReviewR5OrdinaryTimestamp(t *testing.T) {
	for _, tc := range []struct{ name, timestamp, stable string }{
		{"control", "", ""},
		{"year_zero", "0000-09-10T06:59:00+00:00", ""},
		{"year_zero_naive", "0000-09-10T06:59:00", ""},
		{"utc_underflow", "0001-01-01T00:00:00+00:01", ""},
		{"utc_overflow", "9999-12-31T23:59:59-00:01", ""},
		{"basic_aware", "20260910T065900+00:00", "fe73b998df2833efab45eb422f86d71fb096abcb3706467e845bc95a6b5eb39e"},
		{"zone_under_24_hours", "20260910T065900+23:59", "887e96aff6777ac61c4de5929dc9f9ebf125cca4e5acea9f84821a5fba9b85a5"},
		{"zone_at_24_hours", "20260910T065900+24:00", ""},
		{"basic_date_extended_clock", "20260910T06:59:00+00:00", "4ce9eabf3e088d5e78d317bc7f88a80925848fedf3fae9e9c8a85250e1da1deb"},
		{"basic_naive", "20260910T065900", "5cedf8603d23d25a2ce5fd62140a05aaee64fcbb2040a67a44f1ba4cd52268f4"},
		{"basic_naive_space", "20260910 065900", "e630d0e2d54f2914d282f8b155f1a88240eccba7330319eb21202b09e85704bc"},
		{"basic_naive_minutes", "20260910T0659", "a43f37e24711e22ce1355191c1d7fdf5689e9b8d958750726bc3768d1545ed23"},
		{"extended_date_basic_clock", "2026-09-10T065900", "d2c978cb3412018aad2fdb22ee07ced03a76504a8eb7a8b0ea0b2837f2017843"},
		{"basic_date_only", "20260910", "aed1aa2e6be37d8a7f7f70fa2430f8e7d369ba91ea4747d7d161aa4e8e9c09e3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := historyGolden(t, "direct")
			want := historyTestCopy(t, f["expected"]).(map[string]any)
			if tc.timestamp != "" {
				historyTestFirst(f, "youtube_upload_operations")["request_attempted_at"] = tc.timestamp
				if tc.stable == "" {
					want = historyTestRefusal("owned_history_invalid")
				} else {
					// Observed from the frozen Python assessor, not computed by this port.
					want["stable_history_sha256"] = tc.stable
				}
			}
			historyTestFullAssessment(t, f, want)
		})
	}
	for _, basic := range []bool{false, true} {
		t.Run("authority/"+fmt.Sprint(basic), func(t *testing.T) {
			f := historyGolden(t, "history_only")
			want := f["expected"]
			if basic {
				historyTestManifest(f)["starts_at"] = "20260911T080000+00:00"
				historyTestRehash(t, f)
				want = historyTestRefusal("owned_history_manifest_invalid")
			}
			historyTestFullAssessment(t, f, want)
		})
	}
}

func TestOwnedHistoryReviewR6ReplacementEquality(t *testing.T) {
	for _, tc := range []struct {
		name, stable string
		value        any
	}{
		{"integer_zero", "490a318f97dcf9634c7b4adbc3b11a0a3c521d2fdebe05dd68fee54476b0b747", json.Number("0")},
		{"float_zero", "d7f982c715388fdafc1057004c32e1300b28b2897ad1e7d5992c2cda717a544f", json.Number("0.0")},
		{"false", "cfbb430790cf5696a5cc0c35b45b66d581ba178fea56101863c31ff01673937d", false},
		{"integer_one", "", json.Number("1")}, {"float_one", "", json.Number("1.0")}, {"true", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := historyGolden(t, "promotion_replacement")
			historyTestFind(t, f, "channel_ops_queue_items", "status", "cancelled")["attempt_count"] = tc.value
			want := historyTestCopy(t, f["expected"]).(map[string]any)
			if tc.stable == "" {
				want = historyTestRefusal("owned_inventory_queue_failed")
			} else {
				want["stable_history_sha256"] = tc.stable
			}
			historyTestFullAssessment(t, f, want)
		})
	}
}

func TestOwnedHistorySnapshotValidation(t *testing.T) {
	for _, bad := range []string{"missing", "extra", "duplicate", "duplicate_schedule", "overflow", "not_array", "not_object", "missing_id", "invalid_uc", "too_large", "bad_json", "bad_redis"} {
		t.Run(bad, func(t *testing.T) {
			f := historyGolden(t, "direct")
			rows := historyTestRows(f)
			uc := f["platform_channel_id"].(string)
			switch bad {
			case "missing":
				delete(rows, "registered_worker_event_deliveries")
			case "extra":
				rows["extra"] = []any{}
			case "duplicate":
				rows["jobs"] = append(rows["jobs"].([]any), historyTestFirst(f, "jobs"))
			case "duplicate_schedule":
				rows["runtime_schedules"] = []any{map[string]any{"service_name": "video"}, map[string]any{"service_name": "video"}}
			case "overflow":
				a := []any{}
				for i := 0; i < ownedHistoryMaxSnapshotRows+1; i++ {
					a = append(a, map[string]any{"id": historyTestUID(i)})
				}
				rows["jobs"] = a
			case "not_array":
				rows["jobs"] = nil
			case "not_object":
				rows["jobs"] = []any{"wrong"}
			case "missing_id":
				rows["jobs"] = []any{map[string]any{}}
			case "invalid_uc":
				uc = "caller-selected"
			}
			raw := historyJSON(t, rows)
			redis := historyJSON(t, f["redis_observations"])
			if bad == "too_large" {
				raw = bytes.Repeat([]byte(" "), 16*1024*1024+1)
			}
			if bad == "bad_json" {
				raw = []byte(`{"a":1,"a":2}`)
			}
			if bad == "bad_redis" {
				redis = []byte(`[{"kind":"task"}]`)
			}
			_, err := newOwnedHistorySnapshot(raw, uc, historyAt(t, f["observed_at"]), redis)
			if err == nil || !strings.HasPrefix(err.Error(), "owned_history_") {
				t.Fatalf("invalid snapshot: %v", err)
			}
		})
	}
}
func TestOwnedHistoryImmutableAndStable(t *testing.T) {
	for _, name := range historyGoldenNames {
		t.Run(name, func(t *testing.T) {
			f := historyGolden(t, name)
			raw := historyJSON(t, f["rows"])
			s, err := newOwnedHistorySnapshot(raw, f["platform_channel_id"].(string), historyAt(t, f["observed_at"]), historyJSON(t, f["redis_observations"]))
			if err != nil {
				t.Fatal(err)
			}
			before := s.snapshotSHA256()
			raw[0] = '!'
			copyRows := s.rows()
			copyRows["jobs"] = []any{}
			if before != s.snapshotSHA256() {
				t.Fatal("aliased snapshot")
			}
			original := historyTestAssess(t, f)
			for _, v := range historyTestRows(f) {
				a := v.([]any)
				for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
					a[i], a[j] = a[j], a[i]
				}
			}
			f["observed_at"] = historyISO(historyAt(t, f["observed_at"]).Add(time.Second))
			f["now"] = historyISO(historyAt(t, f["now"]).Add(time.Second))
			after := historyTestAssess(t, f)
			if !bytes.Equal(mustHistoryAssessmentJSON(t, original), mustHistoryAssessmentJSON(t, after)) {
				t.Fatal("row order/allowed time changed assessment")
			}
		})
	}
}
func mustHistoryAssessmentJSON(t *testing.T, v ownedHistoryAssessment) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestOwnedHistoryObservationWindow(t *testing.T) {
	for _, offset := range []time.Duration{-time.Nanosecond, 0, 60 * time.Second, 60*time.Second + time.Nanosecond} {
		t.Run(offset.String(), func(t *testing.T) {
			f := historyGolden(t, "direct")
			s := historySnapshot(t, f)
			got := assessOwnedHistorySnapshot(s, s.observedAt.Add(offset))
			if offset < 0 || offset > 60*time.Second {
				if got.BlockReason == nil || *got.BlockReason != "owned_history_observation_stale" {
					t.Fatal(got)
				}
			} else if got.BlockReason != nil {
				t.Fatal(*got.BlockReason)
			}
		})
	}
}
func TestOwnedHistoryCanonicalBoundary(t *testing.T) {
	for raw, want := range map[string]string{`{"s":"\ud83d\ude00\u4e2d","i":1,"f":1.0,"e":1e-07}`: `{"e":1e-07,"f":1.0,"i":1,"s":"\ud83d\ude00\u4e2d"}`, `{"z":-0,"f":-0.0}`: `{"f":-0.0,"z":0}`, `{"at":"2026-09-11T08:00:00.123456+00:00"}`: `{"at":"2026-09-11T08:00:00.123456+00:00"}`} {
		v, err := ownedDecode([]byte(raw))
		if err != nil || string(historyJSON(t, v)) != want {
			t.Fatal("canonical discrepancy", raw, err)
		}
	}
	for _, raw := range []string{`{"a":1,"a":2}`, `{"x":NaN}`, `{"x":Infinity}`, `{"x":"\ud800"}`, `{"x":"\udc00"}`, `{} {}`, `{"x":1e999}`} {
		v, err := ownedDecode([]byte(raw))
		if err == nil {
			_, err = ownedCanonical(v)
		}
		if err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	for _, v := range []any{math.Inf(1), math.NaN(), string([]byte{0xff})} {
		if _, err := ownedCanonical(v); err == nil {
			t.Fatal("accepted nonfinite/invalid Unicode")
		}
	}
}
func TestOwnedHistoryStrictManifest(t *testing.T) {
	for _, bad := range []string{"v1_history", "version", "version_float", "tick_bool", "tick_float", "privacy", "duration", "asset", "sha", "extra", "entry_extra", "provenance_extra", "ordinal", "byte_size", "storage_id", "storage_hash", "duplicate_seed", "duplicate_hash", "invalid_id", "utc_z", "utc_offset", "utc_naive", "utc_fraction", "subject", "binding_target", "binding_duplicate", "legacy_version", "qualified_id", "qualified_hash", "endpoint"} {
		t.Run(bad, func(t *testing.T) {
			f := historyGolden(t, "history_only")
			m := historyTestManifest(f)
			e := m["entries"].([]any)[0].(map[string]any)
			section := m["legacy_history"].(map[string]any)
			b := section["bindings"].([]any)[0].(map[string]any)
			switch bad {
			case "v1_history":
				m["version"] = json.Number("1")
			case "version":
				m["version"] = json.Number("3")
			case "version_float":
				m["version"] = json.Number("2.0")
			case "tick_bool":
				m["tick_interval_minutes"] = true
			case "tick_float":
				m["tick_interval_minutes"] = json.Number("1.0")
			case "privacy":
				m["privacy"] = "public"
			case "duration":
				m["expires_at"] = historyISO(historyAt(t, m["starts_at"]).Add(8 * 24 * time.Hour))
			case "asset":
				e["asset_id"] = m["entries"].([]any)[1].(map[string]any)["asset_id"]
			case "sha":
				e["provenance_sha256"] = strings.Repeat("f", 64)
			case "extra":
				m["safe"] = true
			case "entry_extra":
				e["safe"] = true
			case "provenance_extra":
				e["provenance_evidence"].(map[string]any)["safe"] = true
			case "ordinal":
				e["ordinal"] = json.Number("2")
			case "byte_size":
				e["byte_size"] = json.Number("67108865")
			case "storage_id":
				e["storage_descriptor"].(map[string]any)["id"] = historyTestUID(999)
			case "storage_hash":
				e["storage_descriptor"].(map[string]any)["media_info_sha256"] = "wrong"
			case "duplicate_seed":
				e["manual_seed_id"] = m["entries"].([]any)[1].(map[string]any)["manual_seed_id"]
			case "duplicate_hash":
				e["content_sha256"] = m["entries"].([]any)[1].(map[string]any)["content_sha256"]
			case "invalid_id":
				m["inventory_id"] = "11111111111111111111111111111111"
			case "utc_z":
				m["starts_at"] = strings.Replace(m["starts_at"].(string), "+00:00", "Z", 1)
			case "utc_offset":
				m["starts_at"] = "2026-09-11T09:00:00+01:00"
			case "utc_naive":
				m["starts_at"] = "2026-09-11T08:00:00"
			case "utc_fraction":
				m["starts_at"] = "2026-09-11T08:00:00.000+00:00"
			case "subject":
				m["server_subject"] = "caller"
			case "binding_target":
				b["legacy_account_id"] = m["target_account_id"]
			case "binding_duplicate":
				section["bindings"] = append(section["bindings"].([]any), historyTestCopy(t, b))
			case "legacy_version":
				section["version"] = json.Number("2")
			case "qualified_id":
				b["qualified_operation_ids"] = []any{historyTestUID(999)}
			case "qualified_hash":
				b["qualification"].(map[string]any)["facts_sha256"] = strings.Repeat("f", 64)
			case "endpoint":
				b["qualification"].(map[string]any)["manager_endpoint_identity"] = "https://caller"
			}
			_, err := decodeOwnedHistoryManifest(historyJSON(t, m))
			if err == nil || err.Error() != "owned_history_manifest_invalid" {
				t.Fatalf("manifest accepted: %v", err)
			}
		})
	}
	for _, version := range []string{"1", "2"} {
		f := historyGolden(t, "history_only")
		m := historyTestManifest(f)
		m["version"] = json.Number(version)
		if version == "1" {
			delete(m, "legacy_history")
		}
		raw := historyJSON(t, m)
		decoded, err := decodeOwnedHistoryManifest(raw)
		if err != nil {
			t.Fatal(err)
		}
		raw[0] = '!'
		if decoded.document != string(historyJSON(t, m)) {
			t.Fatal("mutable manifest")
		}
	}
}

func TestOwnedHistoryQualificationNegatives(t *testing.T) {
	for _, bad := range []string{"draft", "wrong_uc", "missing_fact", "duplicate_fact", "operation_hash", "receipt_hash", "outer_video", "descriptor", "new_operation", "submitted", "future", "unknown_field", "manager", "video", "receipt", "task", "new_held_task", "unapproved_state", "approval_future", "approved_by", "manifest_hash", "manifest_scope"} {
		t.Run(bad, func(t *testing.T) {
			f := historyGolden(t, "history_only")
			rows := historyTestRows(f)
			inv := historyTestFirst(f, "owned_seed_inventories")
			m := historyTestManifest(f)
			b := m["legacy_history"].(map[string]any)["bindings"].([]any)[0].(map[string]any)
			q := b["qualification"].(map[string]any)
			fact := q["sanitized_facts"].([]any)[0].(map[string]any)
			op := historyTestFirst(f, "youtube_upload_operations")
			switch bad {
			case "draft":
				inv["approved_at"] = nil
				inv["approved_by"] = nil
				inv["approval_reference"] = nil
				inv["state"] = "draft"
			case "wrong_uc":
				fact["actual_platform_channel_id"] = "UC" + strings.Repeat("b", 22)
			case "missing_fact":
				q["sanitized_facts"] = []any{}
			case "duplicate_fact":
				q["sanitized_facts"] = append(q["sanitized_facts"].([]any), historyTestCopy(t, fact))
			case "operation_hash":
				fact["operation_sha256"] = strings.Repeat("0", 64)
			case "receipt_hash":
				fact["receipt_sha256"] = strings.Repeat("0", 64)
			case "outer_video":
				q["platform_video_id"] = "other_video"
			case "descriptor":
				historyTestFirst(f, "publishing_accounts")["credential_ref"] = "changed-reference"
			case "new_operation":
				newOp := historyTestCopy(t, op).(map[string]any)
				newOp["id"] = historyTestUID(999)
				rows["youtube_upload_operations"] = append(rows["youtube_upload_operations"].([]any), newOp)
			case "submitted":
				op["status"] = "submitted"
				fact["operation_sha256"] = historyTestHash(t, op)
			case "future":
				fact["observed_at"] = historyISO(historyAt(t, f["now"]).Add(time.Second))
			case "unknown_field":
				q["safe"] = true
			case "manager":
				op["manager_task_id"] = historyTestUID(999)
			case "video":
				op["platform_video_id"] = "other_video"
			case "receipt":
				op["receipt_json"].(map[string]any)["privacy"] = "public"
			case "task":
				op["production_task_id"] = historyTestUID(999)
			case "new_held_task":
				n := historyTestCopy(t, historyTestFirst(f, "production_tasks")).(map[string]any)
				n["id"] = historyTestUID(999)
				n["state"] = "held"
				n["job_id"] = nil
				rows["production_tasks"] = append(rows["production_tasks"].([]any), n)
			case "unapproved_state":
				inv["state"] = "draft"
			case "approval_future":
				inv["approved_at"] = historyISO(historyAt(t, f["now"]).Add(time.Second))
			case "approved_by":
				inv["approved_by"] = ""
			case "manifest_scope":
				inv["target_account_id"] = historyTestUID(999)
			}
			q["facts_sha256"] = historyTestHash(t, q["sanitized_facts"])
			historyTestRehash(t, f)
			if bad == "manifest_hash" {
				inv["manifest_sha256"] = strings.Repeat("f", 64)
			}
			historyTestReason(t, f, "")
		})
	}
}
func TestOwnedHistoryLineage(t *testing.T) {
	for _, state := range []string{"approved", "held", "exhausted", "expired", "revoked"} {
		t.Run(state, func(t *testing.T) {
			f := historyGolden(t, "history_only")
			historyTestFirst(f, "owned_seed_inventories")["state"] = state
			before := historyJSON(t, f)
			result := historyTestAssess(t, f)
			if result.BlockReason != nil || len(result.AccountIDs) != 1 || result.Classifications[0].Classification != "history_only" {
				t.Fatal(result)
			}
			if !bytes.Equal(before, historyJSON(t, f)) {
				t.Fatal("history mutated")
			}
		})
	}
	f := historyGolden(t, "history_only")
	rows := historyTestRows(f)
	successor := historyTestCopy(t, historyTestFirst(f, "owned_seed_inventories")).(map[string]any)
	m := successor["manifest_json"].(map[string]any)
	successor["id"] = historyTestUID(105)
	m["inventory_id"] = successor["id"]
	other := "UC" + strings.Repeat("b", 22)
	m["platform_channel_id"] = other
	successor["platform_channel_id"] = other
	b := m["legacy_history"].(map[string]any)["bindings"].([]any)[0].(map[string]any)
	b["canonical_platform_channel_id"] = other
	q := b["qualification"].(map[string]any)
	q["actual_platform_channel_id"] = other
	q["sanitized_facts"].([]any)[0].(map[string]any)["actual_platform_channel_id"] = other
	q["facts_sha256"] = historyTestHash(t, q["sanitized_facts"])
	successor["manifest_sha256"] = historyTestHash(t, m)
	rows["owned_seed_inventories"] = append(rows["owned_seed_inventories"].([]any), successor)
	historyTestReason(t, f, "owned_history_authority_conflict")
}

func TestOwnedHistoryAuthorityDecisionOrder(t *testing.T) {
	f := historyGolden(t, "history_only")
	rows := historyTestRows(f)
	first := historyTestFirst(f, "owned_seed_inventories")
	second := historyTestCopy(t, first).(map[string]any)
	second["id"] = historyTestUID(105)
	secondManifest := second["manifest_json"].(map[string]any)
	secondManifest["inventory_id"] = second["id"]
	second["manifest_sha256"] = historyTestHash(t, secondManifest)
	first["manifest_json"].(map[string]any)["legacy_history"].(map[string]any)["bindings"].([]any)[0].(map[string]any)["legacy_account_id"] = historyTestUID(999)
	historyTestRehash(t, f)
	rows["owned_seed_inventories"] = append(rows["owned_seed_inventories"].([]any), second)
	extra := historyTestCopy(t, historyTestFirst(f, "youtube_upload_operations")).(map[string]any)
	extra["id"] = historyTestUID(777)
	rows["youtube_upload_operations"] = append(rows["youtube_upload_operations"].([]any), extra)
	// A1 checks bindings in approved-inventory insertion order, not account order.
	historyTestReason(t, f, "owned_history_binding_changed")
}
func TestOwnedHistoryGlobalOrphansAndItems(t *testing.T) {
	for _, bad := range []string{"task", "account", "channel", "foreign_channel", "blank", "foreign_platform", "unknown_operation", "item_duplicate", "item_manifest", "item_seed"} {
		t.Run(bad, func(t *testing.T) {
			name := "direct"
			if strings.HasPrefix(bad, "item_") {
				name = "pending_promotion"
			}
			f := historyGolden(t, name)
			r := historyTestRows(f)
			reason := "owned_history_orphan"
			switch bad {
			case "task":
				r["production_tasks"] = []any{}
			case "account":
				r["publishing_accounts"] = []any{}
			case "channel":
				r["channel_profiles"] = []any{}
			case "foreign_channel":
				historyTestFirst(f, "publishing_accounts")["channel_profile_id"] = historyTestUID(999)
			case "blank":
				historyTestFirst(f, "publishing_accounts")["platform_account_id"] = ""
				reason = "owned_history_unclassified"
			case "foreign_platform":
				historyTestFirst(f, "publishing_accounts")["platform"] = "vimeo"
				reason = "owned_history_unclassified"
			case "unknown_operation":
				r["youtube_upload_operations"] = append(r["youtube_upload_operations"].([]any), map[string]any{"id": historyTestUID(999), "production_task_id": historyTestUID(998)})
			case "item_duplicate":
				item := historyTestCopy(t, historyTestFirst(f, "owned_seed_inventory_items")).(map[string]any)
				item["id"] = historyTestUID(999)
				r["owned_seed_inventory_items"] = append(r["owned_seed_inventory_items"].([]any), item)
				reason = "owned_history_item_authority"
			case "item_manifest":
				r["owned_seed_inventories"] = []any{}
				reason = "owned_history_item_authority"
			case "item_seed":
				historyTestFirst(f, "production_tasks")["manual_seed_id"] = historyTestUID(999)
				reason = "owned_inventory_history_identity"
			}
			historyTestReason(t, f, reason)
		})
	}
	f := historyGolden(t, "direct")
	historyTestFirst(f, "publishing_accounts")["platform"] = ""
	if r := historyTestAssess(t, f); r.BlockReason != nil {
		t.Fatal(*r.BlockReason)
	}
}

func TestOwnedHistoryA1ScalarDecisionParity(t *testing.T) {
	// Expectations independently evaluated against frozen A1, not Go's legacy evaluator.
	for _, tc := range []struct {
		name, table, key string
		value            any
		reason           string
	}{
		{"large_count", "publication_metric_schedules", "attempt_count", json.Number("1000000000000000000000000000000"), "owned_inventory_metrics"},
		{"false_retry", "production_tasks", "retry_count", false, ""},
		{"fraction_retry", "production_tasks", "retry_count", json.Number("0.0"), ""},
		{"array_fk", "youtube_upload_operations", "production_task_id", []any{}, "owned_history_invalid"},
		{"array_kind", "channel_ops_queue_items", "kind", []any{}, "owned_inventory_reconciliation"},
		{"array_status", "production_tasks", "state", []any{}, "owned_history_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := historyGolden(t, "direct")
			historyTestFirst(f, tc.table)[tc.key] = tc.value
			if tc.reason != "" {
				historyTestReason(t, f, tc.reason)
			} else {
				result := historyTestAssess(t, f)
				if result.BlockReason != nil {
					t.Fatal(*result.BlockReason)
				}
			}
		})
	}
	f := historyGolden(t, "history_only")
	m := historyTestManifest(f)
	m["entries"].([]any)[0].(map[string]any)["storage_descriptor"].(map[string]any)["file_size"] = json.Number("10.0")
	if _, err := decodeOwnedHistoryManifest(historyJSON(t, m)); err != nil {
		t.Fatal("A1 numeric descriptor equality", err)
	}
}

func TestOwnedHistoryA1StructuredReceiptEquality(t *testing.T) {
	f := historyGolden(t, "direct")
	op := historyTestFirst(f, "youtube_upload_operations")
	op["title"] = map[string]any{"retained": true}
	receipt := op["receipt_json"].(map[string]any)
	receipt["title"] = historyTestCopy(t, op["title"])
	for _, v := range historyTestRows(f)["artifacts"].([]any) {
		a := v.(map[string]any)
		info := a["media_info"].(map[string]any)
		if _, ok := info["youtube"]; ok {
			info["youtube"] = historyTestCopy(t, receipt)
		}
	}
	if result := historyTestAssess(t, f); result.BlockReason != nil {
		t.Fatal(*result.BlockReason)
	}
}

func TestOwnedHistoryReviewR4UnrelatedProjectedQueue(t *testing.T) {
	for _, name := range []string{"direct", "retired_unassigned"} {
		for _, variant := range []string{"control", "missing", "null", "array"} {
			t.Run(name+"/"+variant, func(t *testing.T) {
				f := historyGolden(t, name)
				want := f["expected"]
				if variant != "control" {
					q := map[string]any{"id": historyTestUID(999)}
					if variant == "null" {
						q["payload_json"] = nil
					}
					if variant == "array" {
						q["payload_json"] = []any{}
					}
					if variant != "missing" {
						want = historyTestRefusal("owned_history_invalid")
					}
					rows := historyTestRows(f)
					rows["channel_ops_queue_items"] = append(rows["channel_ops_queue_items"].([]any), q)
				}
				historyTestFullAssessment(t, f, want)
			})
		}
	}
}

func TestOwnedHistoryNormalDriftAndRetry(t *testing.T) {
	for _, tc := range []struct {
		table, key string
		value      any
	}{
		{"youtube_upload_operations", "status", "submitted"}, {"youtube_upload_operations", "request_attempted_at", nil}, {"youtube_upload_operations", "manager_task_id", "unknown"}, {"youtube_upload_operations", "privacy", "public"}, {"jobs", "status", "CANCELLED"}, {"node_executions", "status", "RUNNING"}, {"production_tasks", "failure_reason", "failed"}, {"publication_records", "current_privacy", "private"}, {"publication_records", "current_privacy", "public"}, {"publication_records", "platform_content_id", "other_video"}, {"publication_records", "publish_status", "unknown"}, {"channel_ops_queue_items", "last_error", "failed"},
	} {
		t.Run(tc.table+"/"+tc.key+fmt.Sprint(tc.value), func(t *testing.T) {
			f := historyGolden(t, "direct")
			historyTestFirst(f, tc.table)[tc.key] = tc.value
			historyTestReason(t, f, "")
		})
	}
	for _, bad := range []string{"parent", "key", "payload", "late", "duplicate", "error"} {
		t.Run("metric/"+bad, func(t *testing.T) {
			f := historyGolden(t, "metrics_pending_retry")
			r := historyTestRows(f)
			retry := historyTestFind(t, f, "channel_ops_queue_items", "id", historyTestUID(61))
			switch bad {
			case "parent":
				retry["parent_queue_item_id"] = historyTestUID(999)
			case "key":
				retry["idempotency_key"] = retry["idempotency_key"].(string) + ":other"
			case "payload":
				retry["payload_json"].(map[string]any)["publication_id"] = historyTestUID(999)
			case "late":
				historyTestFind(t, f, "publication_metric_schedules", "snapshot_stage", "24h")["grace_until"] = historyISO(historyAt(t, f["now"]).Add(-time.Second))
			case "duplicate":
				q := historyTestCopy(t, retry).(map[string]any)
				q["id"] = historyTestUID(999)
				r["channel_ops_queue_items"] = append(r["channel_ops_queue_items"].([]any), q)
			case "error":
				retry["last_error"] = "uncertain"
			}
			historyTestReason(t, f, "")
		})
	}
	for _, name := range []string{"metrics_pending_retry", "metrics_recovered"} {
		t.Run("lease/"+name, func(t *testing.T) {
			f := historyGolden(t, name)
			id := historyTestUID(32)
			if name == "metrics_recovered" {
				id = historyTestUID(61)
			}
			q := historyTestFind(t, f, "channel_ops_queue_items", "id", id)
			q["status"] = "running"
			q["locked_at"] = f["now"]
			q["locked_by"] = "native-consumer"
			result := historyTestAssess(t, f)
			if result.BlockReason != nil || result.WaitReason == nil || *result.WaitReason != "owned_inventory_metrics_pending" {
				t.Fatal(result)
			}
		})
	}
	for _, bad := range []string{"manual_video", "manual_privacy", "parent", "outcome", "reason_only", "automatic_attempt"} {
		t.Run("replacement/"+bad, func(t *testing.T) {
			f := historyGolden(t, "promotion_replacement")
			manual := historyTestFind(t, f, "channel_ops_queue_items", "id", historyTestUID(14))
			auto := historyTestFind(t, f, "channel_ops_queue_items", "status", "cancelled")
			switch bad {
			case "manual_video":
				manual["payload_json"].(map[string]any)["publication_id"] = historyTestUID(999)
			case "manual_privacy":
				manual["payload_json"].(map[string]any)["target_visibility"] = "public"
			case "parent":
				historyTestFind(t, f, "channel_ops_queue_items", "kind", "reconcile_publication")["parent_queue_item_id"] = auto["id"]
			case "outcome":
				manual["status"] = "failed"
			case "reason_only":
				auto["idempotency_key"] = "something_else"
			case "automatic_attempt":
				auto["attempt_count"] = json.Number("1")
			}
			historyTestReason(t, f, "")
		})
	}
}

func historyJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := ownedCanonical(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func historyAt(t *testing.T, v any) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339Nano, v.(string))
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func historySnapshot(t *testing.T, f map[string]any) ownedHistorySnapshot {
	t.Helper()
	s, err := newOwnedHistorySnapshot(historyJSON(t, f["rows"]), f["platform_channel_id"].(string), historyAt(t, f["observed_at"]), historyJSON(t, f["redis_observations"]))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOwnedHistoryGolden(t *testing.T) {
	for _, name := range historyGoldenNames {
		t.Run(name, func(t *testing.T) {
			f := historyGolden(t, name)
			s := historySnapshot(t, f)
			got := assessOwnedHistorySnapshot(s, historyAt(t, f["now"]))
			raw, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := ownedDecode(raw)
			if err != nil {
				t.Fatal(err)
			}
			if !ownedEqual(decoded, f["expected"]) {
				t.Fatalf("assessment\ngot %s\nwant %s", raw, historyJSON(t, f["expected"]))
			}
			if s.snapshotSHA256() != f["snapshot_sha256"] {
				t.Fatalf("snapshot hash: %s", s.snapshotSHA256())
			}
			vector := f["canonical_vector"].(map[string]any)
			if string(historyJSON(t, vector["value"])) != vector["canonical_ascii"] {
				t.Fatal("canonical bytes differ")
			}
			sha, err := ownedHash(vector["value"])
			if err != nil || sha != vector["sha256"] {
				t.Fatal("canonical vector hash differs", err)
			}
		})
	}
}
