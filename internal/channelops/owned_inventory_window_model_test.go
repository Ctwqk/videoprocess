package channelops

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

type ownedWindowCase struct {
	Name                 string `json:"name"`
	PollSeconds          int    `json:"poll_seconds"`
	QueueSeconds         int    `json:"queue_seconds"`
	UploadSeconds        int    `json:"upload_seconds"`
	SettlementSeconds    int    `json:"settlement_seconds"`
	MissedDays           []int  `json:"missed_days"`
	AdmissionMinutes     []int  `json:"admission_minutes"`
	LastCompletionMinute int    `json:"last_completion_minute"`
	LastSettlementMinute int    `json:"last_settlement_minute"`
	FitsAssumptions      bool   `json:"fits_assumptions"`
}

func TestOwnedNativeSevenDayWindowModel(t *testing.T) {
	raw, err := os.ReadFile("../../backend/tests/fixtures/owned_inventory_window_model.json")
	if err != nil {
		t.Fatal(err)
	}
	var model struct {
		Cases []ownedWindowCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &model); err != nil {
		t.Fatal(err)
	}
	for _, c := range model.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if got := runOwnedWindowModel(t, c); !reflect.DeepEqual(got, c.AdmissionMinutes) {
				t.Fatalf("admission minutes = %v; want %v", got, c.AdmissionMinutes)
			}
		})
	}
}

type ownedWindowEvent struct {
	admitted, completed, settled time.Time
}

// These are synthetic normal lifecycle observations, not actual uploads or elapsed observation.
func ownedWindowHistory(t *testing.T, target ownedInventoryData, index int, event ownedWindowEvent, now time.Time) map[string]any {
	_, template, _ := ownedTestFixture(t)
	publicationAt := event.settled.Add(-30 * time.Minute)
	ownedTestCompletedHistory(t, &template, publicationAt)
	replacements := []string{ownedTestID(201), ownedString(target.Items[index].Seed["id"]), "abcdefghijk", fmt.Sprintf("modelvid%03d", index+1)}
	for id := 501; id <= 550; id++ {
		replacements = append(replacements, ownedTestID(id), ownedTestID(id+(index+1)*1000))
	}
	replacer := strings.NewReplacer(replacements...)
	var remap func(any) any
	remap = func(v any) any {
		switch v := v.(type) {
		case map[string]any:
			r := map[string]any{}
			for key, value := range v {
				r[key] = remap(value)
			}
			return r
		case []any:
			r := make([]any, len(v))
			for i, value := range v {
				r[i] = remap(value)
			}
			return r
		case string:
			return replacer.Replace(v)
		default:
			return v
		}
	}
	h := remap(template.Tasks[0]).(map[string]any)
	op := ownedMap(ownedArray(h["operations"])[0])
	op["request_attempted_at"], op["completed_at"] = ownedISO(event.admitted), ownedISO(event.completed)
	op["content_sha256"], op["privacy"] = fmt.Sprintf("%064x", index+101), "unlisted"
	ownedMap(op["receipt_json"])["privacy"] = "unlisted"
	ownedMap(h["job"])["completed_at"] = ownedISO(event.completed)
	ownedMap(ownedArray(h["nodes"])[0])["completed_at"] = ownedISO(event.completed)
	ownedMap(ownedMap(ownedArray(h["artifacts"])[0])["media_info"])["youtube"] = op["receipt_json"]
	ownedMap(ownedArray(h["publications"])[0])["uploaded_at"] = ownedISO(event.completed)
	h["feedback"] = []any{}
	for i, value := range ownedArray(h["metrics"]) {
		metric := ownedMap(value)
		due, _ := ownedTime(metric["due_at"])
		queue := ownedMap(ownedArray(h["queues"])[i+2])
		metric["status"], metric["attempt_count"], metric["completed_at"], metric["last_attempt_at"] = "pending", 0, nil, nil
		queue["status"], queue["attempt_count"] = "queued", 0
		if !now.Before(due) {
			metric["status"], metric["attempt_count"], metric["completed_at"], metric["last_attempt_at"] = "succeeded", 1, metric["due_at"], metric["due_at"]
			queue["status"], queue["attempt_count"] = "succeeded", 1
			h["feedback"] = append(ownedArray(h["feedback"]), map[string]any{"id": ownedTestID(540 + i + (index+1)*1000), "publication_id": metric["publication_id"], "snapshot_stage": metric["snapshot_stage"]})
		}
	}
	if now.Before(event.settled) {
		h["publications"], h["metrics"], h["feedback"], h["queues"] = []any{}, []any{}, []any{}, []any{}
		ownedMap(h["task"])["state"] = "producing"
	}
	if now.Before(event.completed) {
		op["status"], op["completed_at"], op["receipt_json"] = "submitted", nil, map[string]any{}
		ownedMap(h["job"])["status"], ownedMap(h["job"])["completed_at"] = "RUNNING", nil
		ownedMap(ownedArray(h["nodes"])[0])["status"] = "RUNNING"
	}
	return h
}

func runOwnedWindowModel(t *testing.T, c ownedWindowCase) []int {
	t.Helper()
	channel, data, start := ownedTestFixture(t)
	manifest := ownedMap(data.Inventory["manifest_json"])
	data.Inventory["starts_at"], data.Inventory["expires_at"] = ownedISO(start), ownedISO(start.Add(7*24*time.Hour))
	manifest["starts_at"], manifest["expires_at"] = data.Inventory["starts_at"], data.Inventory["expires_at"]
	data.Inventory["manifest_sha256"] = ownedTestHash(t, manifest)
	events, minutes := []ownedWindowEvent{}, []int{}
	probe := func(now time.Time, open bool) ownedTickState {
		data.Tasks = nil
		for i, event := range events {
			data.Tasks = append(data.Tasks, ownedWindowHistory(t, data, i, event, now))
		}
		data.RuntimeOpen = open
		state := ownedTestAssess(t, channel, data, now)
		if state.HoldReason != "" {
			t.Fatalf("at %s: %+v", now, state)
		}
		if !open && state.Candidate != nil {
			t.Fatal("admission outside OPEN")
		}
		return state
	}
	for day := 0; day < 7; day++ {
		opens := start.Add(time.Duration(day) * 24 * time.Hour)
		probe(opens.Add(-time.Second), false)
		eligible := opens
		if len(events) > 0 {
			last := events[len(events)-1]
			if last.settled.After(last.completed.Add(24*time.Hour)) && probe(last.completed.Add(24*time.Hour), true).Candidate != nil {
				t.Fatal("unsettled item released after cooldown")
			}
			if last.completed.Add(24 * time.Hour).After(eligible) {
				eligible = last.completed.Add(24 * time.Hour)
			}
			if last.settled.After(eligible) {
				eligible = last.settled
			}
		}
		polled := time.Unix((eligible.Unix()/int64(c.PollSeconds)+1)*int64(c.PollSeconds), 0).UTC()
		at := polled.Add(time.Duration(c.QueueSeconds) * time.Second)
		if !slices.Contains(c.MissedDays, day+1) && at.Before(opens.Add(5*time.Hour)) {
			if len(events) > 0 && eligible.Equal(events[len(events)-1].completed.Add(24*time.Hour)) {
				if probe(eligible.Add(-time.Microsecond), true).Candidate != nil {
					t.Fatal("completion floor rounded down")
				}
				if probe(eligible, true).Candidate == nil {
					t.Fatal("exact 24h remained blocked")
				}
			}
			state := probe(at, true)
			if state.Candidate == nil {
				t.Fatalf("at %s: %+v", at, state)
			}
			index := len(events)
			want := "owned_inventory:" + ownedString(data.Inventory["id"]) + ":" + ownedString(data.Items[index].Item["id"])
			if state.Candidate.CandidateID != want {
				t.Fatal("lowest item changed")
			}
			if channelSchedulerBucket(channel, polled) != polled.Format("2006-01-02-15-04") {
				t.Fatal("owned minute bucket changed")
			}
			if SchedulerBucket(polled, 1) != SchedulerBucket(polled, 15) {
				t.Fatal("ordinary floor changed")
			}
			for _, id := range state.CompleteItemIDs {
				for _, item := range data.Items {
					if item.Item["id"] == id {
						item.Item["state"] = "completed"
					}
				}
			}
			item := data.Items[index]
			item.Item["state"], item.Item["production_task_id"], item.Item["consumed_at"] = "reserved", ownedTestID(501+(index+1)*1000), ownedISO(at)
			item.Seed["status"] = "exhausted"
			reserved := 0
			for _, item := range data.Items {
				if item.Item["state"] == "reserved" {
					reserved++
				}
			}
			if reserved != 1 {
				t.Fatal("multiple outstanding items")
			}
			event := ownedWindowEvent{admitted: at, completed: at.Add(time.Duration(c.UploadSeconds) * time.Second)}
			event.settled = event.completed.Add(time.Duration(c.SettlementSeconds) * time.Second)
			if index > 0 {
				last := events[index-1]
				if at.Sub(last.admitted) < 24*time.Hour || at.Sub(last.completed) < 24*time.Hour || at.Before(last.settled) {
					t.Fatal("rolling spacing or outstanding guard bypassed")
				}
			}
			events, minutes = append(events, event), append(minutes, int(at.Sub(start)/time.Minute))
			if len(events) == 7 {
				data.Inventory["state"] = "exhausted"
			}
		}
		probe(opens.Add(5*time.Hour), false) // DRAINING
		probe(opens.Add(6*time.Hour), false) // CLOSED
	}
	last := events[len(events)-1]
	if int(last.completed.Sub(start)/time.Minute) != c.LastCompletionMinute || int(last.settled.Sub(start)/time.Minute) != c.LastSettlementMinute {
		t.Fatal("completion/settlement trace changed")
	}
	fits := len(events) == 7 && len(c.MissedDays) == 0 && c.PollSeconds+c.QueueSeconds <= 120 && c.UploadSeconds <= 1200 && c.SettlementSeconds <= 5700
	if fits != c.FitsAssumptions {
		t.Fatal("assumption qualification changed")
	}
	if fits && (!last.settled.Before(start.Add(6*24*time.Hour+5*time.Hour)) || last.settled.Sub(start) >= 168*time.Hour) {
		t.Fatal("nominal settlement missed DRAINING or evidence-span bound")
	}
	data.Tasks = nil
	for i, event := range events {
		data.Tasks = append(data.Tasks, ownedWindowHistory(t, data, i, event, start.Add(168*time.Hour)))
	}
	data.RuntimeOpen = true
	state := ownedTestAssess(t, channel, data, start.Add(168*time.Hour))
	if state.Candidate != nil || (len(events) < 7 && state.HoldReason != "owned_inventory_expired") {
		t.Fatalf("expiry permitted catch-up/replacement: %+v", state)
	}
	return minutes
}

func TestOwnedEachDailyWindowDeniesOtherwiseEligibleInventory(t *testing.T) {
	channel, data, start := ownedTestFixture(t)
	for day := 0; day < 7; day++ {
		for _, point := range []struct {
			offset time.Duration
			open   bool
		}{
			{-time.Second, false}, {0, true}, {5 * time.Hour, false}, {6 * time.Hour, false},
		} {
			data.RuntimeOpen = point.open
			state := ownedTestAssess(t, channel, data, start.Add(time.Duration(day)*24*time.Hour+point.offset))
			if (state.Candidate != nil) != point.open {
				t.Fatalf("day %d, offset %s: %+v", day+1, point.offset, state)
			}
		}
	}
}
