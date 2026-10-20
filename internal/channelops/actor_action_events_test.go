package channelops

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

type actorActionSchema struct {
	Required             []string `json:"required"`
	AdditionalProperties bool     `json:"additionalProperties"`
	Properties           map[string]struct {
		Type      string   `json:"type"`
		Const     string   `json:"const"`
		Enum      []string `json:"enum"`
		MinLength int      `json:"minLength"`
	} `json:"properties"`
}

func loadActorActionSchema(t *testing.T) actorActionSchema {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "services", "vp-feature-aggregator", "schemas", "vp.actor.actions.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema actorActionSchema
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

// requireSchemaValid checks the constraints vp.actor.actions.v1.json uses;
// the aggregator rejects anything else to its dead-letter topic.
func requireSchemaValid(t *testing.T, schema actorActionSchema, event map[string]any) {
	t.Helper()
	for _, key := range schema.Required {
		if _, ok := event[key]; !ok {
			t.Fatalf("event missing required %q: %#v", key, event)
		}
	}
	for key, value := range event {
		property, ok := schema.Properties[key]
		if !ok {
			if !schema.AdditionalProperties {
				t.Fatalf("event has property %q the schema forbids", key)
			}
			continue
		}
		switch property.Type {
		case "string":
			text, ok := value.(string)
			if !ok || len(text) < property.MinLength {
				t.Fatalf("event %q = %#v, want string of length >= %d", key, value, property.MinLength)
			}
			if key == "occurred_at" {
				if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
					t.Fatalf("occurred_at %q is not RFC 3339: %v", text, err)
				}
			}
		case "object":
			if _, ok := value.(map[string]any); !ok {
				t.Fatalf("event %q = %#v, want object", key, value)
			}
		}
		if property.Const != "" && value != property.Const {
			t.Fatalf("event %q = %#v, want const %q", key, value, property.Const)
		}
		if len(property.Enum) > 0 {
			found := false
			for _, allowed := range property.Enum {
				found = found || value == allowed
			}
			if !found {
				t.Fatalf("event %q = %#v is not in %v", key, value, property.Enum)
			}
		}
	}
}

func TestActorActionEventsMatchAggregatorSchema(t *testing.T) {
	schema := loadActorActionSchema(t)
	decision := PDSDecision{
		DecisionID:   "decision-1",
		Verdict:      "block",
		Score:        0.9,
		RulesVersion: "rules-1",
		Reasons:      []map[string]any{{"code": "velocity"}, {"code": ""}, {"detail": "no code"}},
		Metadata:     map[string]any{"warning": "pds_unavailable"},
	}
	for _, actionType := range []string{
		ActorActionCandidateAccepted,
		ActorActionCandidateBlocked,
		ActorActionCandidateFlagged,
		ActorActionPublicationPromotionAttempted,
		ActorActionPublicationPromotionBlocked,
		ActorActionPublicationScheduled,
	} {
		event := buildActorActionEvent(
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-000000000002",
			actionType,
			"youtube",
			pdsDecisionEventMetadata(decision),
			time.Date(2026, 10, 20, 12, 0, 0, 123456789, time.UTC),
		)
		// Round-trip through JSON exactly as the outbox row stores it.
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		requireSchemaValid(t, schema, decoded)
		if decoded["occurred_at"] != "2026-10-20T12:00:00.123456Z" {
			t.Fatalf("occurred_at = %v, want microsecond precision", decoded["occurred_at"])
		}
	}
	metadata := pdsDecisionEventMetadata(decision)
	if codes := metadata["reason_codes"].([]string); len(codes) != 1 || codes[0] != "velocity" {
		t.Fatalf("reason_codes = %#v", metadata["reason_codes"])
	}
	if metadata["warning"] != "pds_unavailable" {
		t.Fatalf("warning = %#v", metadata["warning"])
	}
	if roundTrip := pdsDecisionEventMetadata(pdsDecisionFromAuditJSON(pdsDecisionAuditJSON(decision))); !jsonEqual(t, roundTrip, metadata) {
		t.Fatalf("audit JSON round trip = %#v, want %#v", roundTrip, metadata)
	}
}

func jsonEqual(t *testing.T, a, b any) bool {
	t.Helper()
	left, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	right, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return string(left) == string(right)
}

type outboxEvent struct {
	ActionType string
	Payload    map[string]any
}

func (f *ChannelOpsFixture) actorActionEvents(ctx context.Context) []outboxEvent {
	f.T.Helper()
	rows, err := f.Store.Pool.Query(ctx, `
		SELECT payload::text FROM event_outbox
		WHERE topic = $1 AND key = $2
		ORDER BY created_at, id
	`, actorActionsTopic, f.AccountID)
	if err != nil {
		f.T.Fatalf("select event_outbox: %v", err)
	}
	defer rows.Close()
	schema := loadActorActionSchema(f.T)
	events := []outboxEvent{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			f.T.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			f.T.Fatal(err)
		}
		requireSchemaValid(f.T, schema, payload)
		events = append(events, outboxEvent{ActionType: payload["action_type"].(string), Payload: payload})
	}
	if err := rows.Err(); err != nil {
		f.T.Fatal(err)
	}
	return events
}

func actionTypes(events []outboxEvent) []string {
	types := make([]string, 0, len(events))
	for _, event := range events {
		types = append(types, event.ActionType)
	}
	sort.Strings(types)
	return types
}

func TestTickCandidatePDSVerdictsEmitActorActions(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	for _, tc := range []struct {
		verdict string
		want    string
	}{
		{"block", ActorActionCandidateBlocked},
		{"flag", ActorActionCandidateFlagged},
	} {
		t.Run(tc.verdict, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewChannelOpsFixture(t)
			defer fixture.Close(ctx)
			fixture.InsertChannelWithLaneAccountSeed(ctx)
			handler := fixture.HandlerService(PDSDecision{Verdict: tc.verdict, DecisionID: "tick-" + tc.verdict})
			if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
				t.Fatalf("RunTick: %v", err)
			}
			events := fixture.actorActionEvents(ctx)
			if len(events) == 0 {
				t.Fatal("PDS-rejected candidates emitted no actor actions")
			}
			for _, event := range events {
				if event.ActionType != tc.want {
					t.Fatalf("action types = %v, want only %s", actionTypes(events), tc.want)
				}
				metadata := event.Payload["metadata"].(map[string]any)
				if metadata["decision_id"] != "tick-"+tc.verdict || metadata["verdict"] != tc.verdict || metadata["lane_id"] != fixture.LaneID {
					t.Fatalf("metadata = %#v", metadata)
				}
				if event.Payload["actor_id"] != fixture.AccountID {
					t.Fatalf("actor_id = %v, want %s", event.Payload["actor_id"], fixture.AccountID)
				}
			}
		})
	}
}

func TestLiveFlowEmitsAcceptedAndScheduledExactlyOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	item := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	for attempt := 0; attempt < 2; attempt++ {
		if err := handler.HandlePromotePublication(ctx, item); err != nil {
			t.Fatalf("HandlePromotePublication attempt %d: %v", attempt, err)
		}
	}
	events := fixture.actorActionEvents(ctx)
	want := []string{ActorActionCandidateAccepted, ActorActionPublicationPromotionAttempted, ActorActionPublicationScheduled}
	if got := actionTypes(events); !jsonEqual(t, got, want) {
		t.Fatalf("action types = %v, want %v (replayed promotion must not re-emit)", got, want)
	}
	task := fixture.RequireSingleTask(ctx)
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	for _, event := range events {
		metadata := event.Payload["metadata"].(map[string]any)
		if metadata["task_id"] != task.ID {
			t.Fatalf("%s task_id = %v, want %s", event.ActionType, metadata["task_id"], task.ID)
		}
		if event.ActionType != ActorActionCandidateAccepted && metadata["publication_id"] != publicationID {
			t.Fatalf("%s publication_id = %v, want %s", event.ActionType, metadata["publication_id"], publicationID)
		}
	}
}

func TestPromotionPDSBlockEmitsAttemptedAndBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	item := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	blocking := handler
	blocking.PDS = fakePDS{decision: PDSDecision{Verdict: "block", DecisionID: "publish-block"}}
	if err := blocking.HandlePromotePublication(ctx, item); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	events := fixture.actorActionEvents(ctx)
	want := []string{ActorActionCandidateAccepted, ActorActionPublicationPromotionAttempted, ActorActionPublicationPromotionBlocked}
	if got := actionTypes(events); !jsonEqual(t, got, want) {
		t.Fatalf("action types = %v, want %v", got, want)
	}
	for _, event := range events {
		if event.ActionType != ActorActionPublicationPromotionBlocked {
			continue
		}
		metadata := event.Payload["metadata"].(map[string]any)
		if metadata["guard"] != "pds_blocked" || metadata["decision_id"] != "publish-block" || metadata["target_visibility"] == "" {
			t.Fatalf("blocked metadata = %#v", metadata)
		}
	}
	if task := fixture.RequireSingleTask(ctx); task.State != TaskHeld {
		t.Fatalf("task state = %s, want held", task.State)
	}
}
