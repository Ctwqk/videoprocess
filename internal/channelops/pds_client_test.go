package channelops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPDSFailPolicyByAction(t *testing.T) {
	cases := []struct {
		action string
		want   string
	}{
		{action: "candidate_accept", want: "allow"},
		{action: "plan_approval", want: "flag"},
		{action: "publish", want: "block"},
		{action: "promote_publication", want: "block"},
	}

	client := PDSClient{Enabled: false}
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			decision, err := client.Decide(context.Background(), PDSDecisionRequest{
				ActorID:    "channel-1",
				ActionType: tc.action,
			})
			if err != nil {
				t.Fatalf("Decide returned error: %v", err)
			}
			if decision.Verdict != tc.want {
				t.Fatalf("Verdict = %q, want %q", decision.Verdict, tc.want)
			}
			if decision.Metadata["warning"] != "pds_disabled" {
				t.Fatalf("warning metadata = %v", decision.Metadata["warning"])
			}
			if decision.Metadata["fail_policy"] != tc.want {
				t.Fatalf("fail_policy metadata = %v, want %q", decision.Metadata["fail_policy"], tc.want)
			}
		})
	}
}

func TestPDSDevAllowAll(t *testing.T) {
	client := PDSClient{Enabled: false, DevAllowAll: true}
	decision, err := client.Decide(context.Background(), PDSDecisionRequest{
		ActorID:    "channel-1",
		ActionType: "publish",
	})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if decision.Verdict != "allow" {
		t.Fatalf("Verdict = %q, want allow", decision.Verdict)
	}
	if decision.Metadata["warning"] != "dev_allow_all" {
		t.Fatalf("warning metadata = %v, want dev_allow_all", decision.Metadata["warning"])
	}
	if decision.Metadata["fail_policy"] != "allow" {
		t.Fatalf("fail_policy metadata = %v, want allow", decision.Metadata["fail_policy"])
	}
}

func TestPDSDevAllowAllOverridesEnabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("DevAllowAll should not call PDS")
	}))
	defer server.Close()

	client := PDSClient{Enabled: true, DevAllowAll: true, BaseURL: server.URL}
	decision, err := client.Decide(context.Background(), PDSDecisionRequest{
		ActorID:    "channel-1",
		ActionType: "publish",
	})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if decision.Verdict != "allow" {
		t.Fatalf("Verdict = %q, want allow", decision.Verdict)
	}
	if decision.Metadata["warning"] != "dev_allow_all" {
		t.Fatalf("warning metadata = %v, want dev_allow_all", decision.Metadata["warning"])
	}
	if decision.Metadata["fail_policy"] != "allow" {
		t.Fatalf("fail_policy metadata = %v, want allow", decision.Metadata["fail_policy"])
	}
}

func TestPDSHTTPAllow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/decide" {
			t.Fatalf("path = %q, want /v1/decide", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", r.Method)
		}
		if r.Header.Get("X-Client-Id") != "channelops-test" {
			t.Fatalf("X-Client-Id = %q", r.Header.Get("X-Client-Id"))
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		action, ok := payload["action"].(map[string]any)
		if !ok || action["type"] != "candidate_accept" || action["platform"] != "youtube" {
			t.Fatalf("unexpected action payload: %#v", payload["action"])
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"decision_id":"dec-1","verdict":"allow","score":0.42}`))
	}))
	defer server.Close()

	client := PDSClient{Enabled: true, BaseURL: server.URL, ClientID: "channelops-test"}
	decision, err := client.Decide(context.Background(), PDSDecisionRequest{
		ActorID:    "channel-1",
		ActionType: "candidate_accept",
		Platform:   "youtube",
		Content: map[string]any{
			"title": "sample",
		},
		Context: map[string]any{
			"task_id": "task-1",
		},
	})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if decision.DecisionID != "dec-1" {
		t.Fatalf("DecisionID = %q, want dec-1", decision.DecisionID)
	}
	if decision.Verdict != "allow" {
		t.Fatalf("Verdict = %q, want allow", decision.Verdict)
	}
	if decision.Score != 0.42 {
		t.Fatalf("Score = %v, want 0.42", decision.Score)
	}
}

func TestPDSHTTPServerErrorFallsBackToPlanApprovalFlag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unavailable", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := PDSClient{Enabled: true, BaseURL: server.URL}
	decision, err := client.Decide(context.Background(), PDSDecisionRequest{
		ActorID:    "channel-1",
		ActionType: "plan_approval",
	})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if decision.Verdict != "flag" {
		t.Fatalf("Verdict = %q, want flag", decision.Verdict)
	}
	if decision.Metadata["warning"] != "pds_unavailable" {
		t.Fatalf("warning metadata = %v, want pds_unavailable", decision.Metadata["warning"])
	}
}

func TestPDSHTTPClientErrorFallsBackToFailPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client := PDSClient{Enabled: true, BaseURL: server.URL}
	decision, err := client.Decide(context.Background(), PDSDecisionRequest{
		ActorID:    "channel-1",
		ActionType: "publish",
	})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if decision.Verdict != "block" {
		t.Fatalf("Verdict = %q, want block", decision.Verdict)
	}
	if decision.Metadata["warning"] != "pds_unavailable" {
		t.Fatalf("warning metadata = %v, want pds_unavailable", decision.Metadata["warning"])
	}
}

func TestPDSInvalidVerdictFallsBackToFailPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"decision_id":"dec-2","verdict":"review","score":0.9}`))
	}))
	defer server.Close()

	client := PDSClient{Enabled: true, BaseURL: server.URL}
	decision, err := client.Decide(context.Background(), PDSDecisionRequest{
		ActorID:    "channel-1",
		ActionType: "publish",
	})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if decision.Verdict != "block" {
		t.Fatalf("Verdict = %q, want block", decision.Verdict)
	}
	if decision.Metadata["warning"] != "pds_parse_failed" {
		t.Fatalf("warning metadata = %v, want pds_parse_failed", decision.Metadata["warning"])
	}
}
