package channelops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestYouTubeManagerSchedulePublishPostsSchedule(t *testing.T) {
	scheduledAt := time.Date(2026, 5, 21, 20, 0, 0, 0, time.UTC)
	attemptKey := "channelops-promotion:00000000-0000-0000-0000-000000000027"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.URL.Path != "/api/videos/yt-1/schedule" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if payload["privacy"] != "unlisted" {
			t.Fatalf("privacy = %#v", payload["privacy"])
		}
		if payload["scheduled_at"] != scheduledAt.Format(time.RFC3339) {
			t.Fatalf("scheduled_at = %#v", payload["scheduled_at"])
		}
		if got := r.Header.Get("Idempotency-Key"); got != attemptKey {
			t.Fatalf("Idempotency-Key = %q, want %q", got, attemptKey)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := YouTubeManagerClient{BaseURL: server.URL}
	if err := client.SchedulePublish(context.Background(), "yt-1", scheduledAt, "unlisted", attemptKey); err != nil {
		t.Fatalf("SchedulePublish returned error: %v", err)
	}
}

func TestYouTubeManagerSchedulePublishRejectsPublicBeforeHTTP(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		t.Fatal("public promotion must not reach YouTubeManager")
	}))
	defer server.Close()

	client := YouTubeManagerClient{BaseURL: server.URL}
	err := client.SchedulePublish(
		context.Background(),
		"yt-1",
		time.Date(2026, 5, 21, 20, 0, 0, 0, time.UTC),
		"public",
		"channelops-promotion:unsafe",
	)
	if err == nil {
		t.Fatal("public promotion should be rejected")
	}
	if calls != 0 {
		t.Fatalf("HTTP calls = %d, want 0", calls)
	}
}
