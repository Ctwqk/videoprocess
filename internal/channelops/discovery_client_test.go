package channelops

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiscoveryClientIngestPostsStrictRequest(t *testing.T) {
	request := discoveryRequestForTest()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/api/v1/channel-agent/internal/discovery/ingest" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q", got)
		}
		var got map[string]any
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		want := map[string]any{
			"queue_item_id":    request.QueueItemID,
			"channel_id":       request.ChannelID,
			"source":           "youtube_search",
			"scheduler_bucket": request.SchedulerBucket,
		}
		if !mapsEqual(got, want) {
			t.Fatalf("request body = %#v, want %#v", got, want)
		}
		writeDiscoveryResponse(t, w, request, nil)
	}))
	defer server.Close()

	client := HTTPDiscoveryClient{BaseURL: server.URL, Timeout: time.Second}
	observation, err := client.Ingest(context.Background(), request)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if observation.RunID != "00000000-0000-0000-0000-000000000003" {
		t.Fatalf("RunID = %q", observation.RunID)
	}
}

func TestDiscoveryClientIngestRejectsUnsafeOrInvalidResponses(t *testing.T) {
	request := discoveryRequestForTest()
	cases := []struct {
		name    string
		status  int
		body    string
		mutate  func(map[string]any)
		wantErr string
	}{
		{name: "non success", status: http.StatusBadGateway, body: `{"detail":"provider secret https://user:password@example.test/?title=hidden"}`, wantErr: "discovery ingest returned HTTP 502"},
		{name: "malformed json", status: http.StatusOK, body: `{`, wantErr: "discovery ingest response is invalid"},
		{name: "blank run id", status: http.StatusOK, mutate: func(payload map[string]any) { payload["run_id"] = "  " }, wantErr: "run_id"},
		{name: "invalid run id", status: http.StatusOK, mutate: func(payload map[string]any) { payload["run_id"] = "not-a-uuid" }, wantErr: "run_id"},
		{name: "channel mismatch", status: http.StatusOK, mutate: func(payload map[string]any) { payload["channel_id"] = "00000000-0000-0000-0000-000000000099" }, wantErr: "channel_id mismatch"},
		{name: "queue mismatch", status: http.StatusOK, mutate: func(payload map[string]any) { payload["queue_item_id"] = "00000000-0000-0000-0000-000000000099" }, wantErr: "queue_item_id mismatch"},
		{name: "source mismatch", status: http.StatusOK, mutate: func(payload map[string]any) { payload["source"] = "other" }, wantErr: "source mismatch"},
		{name: "bucket mismatch", status: http.StatusOK, mutate: func(payload map[string]any) { payload["scheduler_bucket"] = "other" }, wantErr: "scheduler_bucket mismatch"},
		{name: "not succeeded", status: http.StatusOK, mutate: func(payload map[string]any) { payload["status"] = "running" }, wantErr: "status must be succeeded"},
		{name: "negative query count", status: http.StatusOK, mutate: func(payload map[string]any) { payload["query_count"] = -1 }, wantErr: "query_count"},
		{name: "negative created count", status: http.StatusOK, mutate: func(payload map[string]any) { payload["created_count"] = -1 }, wantErr: "created_count"},
		{name: "negative refreshed count", status: http.StatusOK, mutate: func(payload map[string]any) { payload["refreshed_count"] = -1 }, wantErr: "refreshed_count"},
		{name: "negative expired count", status: http.StatusOK, mutate: func(payload map[string]any) { payload["expired_count"] = -1 }, wantErr: "expired_count"},
		{name: "negative quota units", status: http.StatusOK, mutate: func(payload map[string]any) { payload["quota_units_estimated"] = -1 }, wantErr: "quota_units_estimated"},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				if tt.body != "" {
					_, _ = w.Write([]byte(tt.body))
					return
				}
				writeDiscoveryResponse(t, w, request, tt.mutate)
			}))
			defer server.Close()

			_, err := (HTTPDiscoveryClient{BaseURL: server.URL, Timeout: time.Second}).Ingest(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Ingest error = %v, want %q", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "provider secret") || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "hidden") {
				t.Fatalf("Ingest leaked response body: %v", err)
			}
		})
	}
}

func TestDiscoveryClientIngestBoundsResponseBody(t *testing.T) {
	request := discoveryRequestForTest()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"run_id":"` + strings.Repeat("a", 1<<20) + `"}`))
	}))
	defer server.Close()

	_, err := (HTTPDiscoveryClient{BaseURL: server.URL, Timeout: time.Second}).Ingest(context.Background(), request)
	if err == nil || !strings.Contains(err.Error(), "response is too large") {
		t.Fatalf("Ingest error = %v, want bounded response error", err)
	}
}

func TestDiscoveryClientIngestEnforcesDedicatedTimeoutWithCustomClient(t *testing.T) {
	for _, tt := range []struct {
		name       string
		httpClient *http.Client
	}{
		{name: "zero client timeout", httpClient: &http.Client{}},
		{name: "long client timeout", httpClient: &http.Client{Timeout: 5 * time.Second}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
				case <-time.After(300 * time.Millisecond):
				}
			}))
			defer server.Close()

			started := time.Now()
			_, err := (HTTPDiscoveryClient{
				BaseURL: server.URL, Timeout: 25 * time.Millisecond, HTTPClient: tt.httpClient,
			}).Ingest(context.Background(), discoveryRequestForTest())
			if err == nil || err.Error() != "discovery ingest request failed" {
				t.Fatal("Ingest did not return the fixed request failure")
			}
			if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
				t.Fatalf("Ingest elapsed = %s, dedicated timeout was not enforced", elapsed)
			}
		})
	}
}

func TestDiscoveryClientIngestRejectsRedirectWithoutCallingTarget(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
	}{
		{name: "temporary redirect", status: http.StatusTemporaryRedirect},
		{name: "permanent redirect", status: http.StatusPermanentRedirect},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := discoveryRequestForTest()
			var targetCalls atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				targetCalls.Add(1)
				writeDiscoveryResponse(t, w, request, nil)
			}))
			defer target.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL, tt.status)
			}))
			defer origin.Close()

			_, err := (HTTPDiscoveryClient{
				BaseURL: origin.URL,
				Timeout: time.Second,
				HTTPClient: &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
					return nil
				}},
			}).Ingest(context.Background(), request)
			want := fmt.Sprintf("discovery ingest returned HTTP %d", tt.status)
			if err == nil || err.Error() != want {
				t.Fatal("Ingest did not reject the redirect response")
			}
			if targetCalls.Load() != 0 {
				t.Fatalf("redirect target calls = %d, want 0", targetCalls.Load())
			}
		})
	}
}

func TestDiscoveryClientIngestRejectsNonHTTPSchemes(t *testing.T) {
	for _, baseURL := range []string{"ftp://example.test", "gopher://example.test"} {
		t.Run(strings.Split(baseURL, ":")[0], func(t *testing.T) {
			_, err := (HTTPDiscoveryClient{BaseURL: baseURL, Timeout: time.Second}).Ingest(
				context.Background(), discoveryRequestForTest(),
			)
			if err == nil || err.Error() != "AUTOFLOW_BASE_URL is invalid for discovery ingestion" {
				t.Fatal("Ingest did not reject the non-HTTP base scheme")
			}
		})
	}
}

func discoveryRequestForTest() DiscoveryIngestRequest {
	return DiscoveryIngestRequest{
		QueueItemID:     "00000000-0000-0000-0000-000000000001",
		ChannelID:       "00000000-0000-0000-0000-000000000002",
		Source:          "youtube_search",
		SchedulerBucket: "2026-07-21-18",
	}
}

func writeDiscoveryResponse(t *testing.T, w http.ResponseWriter, request DiscoveryIngestRequest, mutate func(map[string]any)) {
	t.Helper()
	payload := map[string]any{
		"run_id":                "00000000-0000-0000-0000-000000000003",
		"channel_id":            request.ChannelID,
		"queue_item_id":         request.QueueItemID,
		"source":                request.Source,
		"scheduler_bucket":      request.SchedulerBucket,
		"status":                "succeeded",
		"query_count":           2,
		"created_count":         3,
		"refreshed_count":       4,
		"expired_count":         5,
		"quota_units_estimated": 200,
	}
	if mutate != nil {
		mutate(payload)
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func mapsEqual(left, right map[string]any) bool {
	leftJSON, _ := json.Marshal(left)
	rightJSON, _ := json.Marshal(right)
	return string(leftJSON) == string(rightJSON)
}
