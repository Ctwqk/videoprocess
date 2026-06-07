package channelops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type recordingAlertSink struct {
	payloads []AlertPayload
	err      error
}

func (s *recordingAlertSink) Send(ctx context.Context, alert AlertPayload) error {
	s.payloads = append(s.payloads, alert)
	return s.err
}

type errAlertSinkFailedForTest struct{}

func (errAlertSinkFailedForTest) Error() string { return "sink failed" }

func TestMultiAlertSinkContinuesAfterFailure(t *testing.T) {
	failing := &recordingAlertSink{err: errAlertSinkFailedForTest{}}
	successful := &recordingAlertSink{}
	sink := MultiAlertSink{failing, successful}

	err := sink.Send(context.Background(), AlertPayload{
		Kind:      "quota_low",
		Severity:  "warning",
		ChannelID: "channel-1",
		Message:   "quota below threshold",
	})

	if err == nil {
		t.Fatal("expected aggregate sink error")
	}
	if len(failing.payloads) != 1 {
		t.Fatalf("failing sink payload count = %d, want 1", len(failing.payloads))
	}
	if len(successful.payloads) != 1 {
		t.Fatalf("successful sink payload count = %d, want 1", len(successful.payloads))
	}
}

func TestSlackAlertSinkPostsMessageSeverityAndDetails(t *testing.T) {
	requests := []map[string]any{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode slack payload: %v", err)
		}
		requests = append(requests, payload)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink := SlackAlertSink{WebhookURL: server.URL, HTTPClient: server.Client()}
	err := sink.Send(context.Background(), AlertPayload{
		Kind:       "platform_rejected",
		Severity:   "critical",
		ResourceID: "publication-1",
		Message:    "platform rejected publication",
		Details:    map[string]any{"reason": "rejected"},
	})

	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if len(requests) != 1 {
		t.Fatalf("request count = %d, want 1", len(requests))
	}
	text, _ := requests[0]["text"].(string)
	for _, want := range []string{"platform_rejected", "critical", "platform rejected publication", "reason: rejected"} {
		if !strings.Contains(text, want) {
			t.Fatalf("slack text %q missing %q", text, want)
		}
	}
}

func TestHandleSendAlertUsesConfiguredSink(t *testing.T) {
	sink := &recordingAlertSink{}
	handler := HandlerService{Store: &Store{}, Alerts: sink}
	item := QueueItemRow{
		Kind: QueueSendAlert,
		PayloadJSON: map[string]any{
			"kind":        "pds_outage",
			"severity":    "warning",
			"channel_id":  "channel-1",
			"resource_id": "task-1",
			"message":     "PDS unavailable",
			"details":     map[string]any{"warning": "pds_unavailable"},
		},
	}

	if err := handler.HandleSendAlert(context.Background(), item); err != nil {
		t.Fatalf("HandleSendAlert: %v", err)
	}
	if len(sink.payloads) != 1 {
		t.Fatalf("payload count = %d, want 1", len(sink.payloads))
	}
	got := sink.payloads[0]
	if got.Kind != "pds_outage" || got.Severity != "warning" || got.ResourceID != "task-1" {
		t.Fatalf("alert payload = %#v", got)
	}
	if got.Details["warning"] != "pds_unavailable" {
		t.Fatalf("details = %#v", got.Details)
	}
}
