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
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := YouTubeManagerClient{BaseURL: server.URL}
	if err := client.SchedulePublish(context.Background(), "yt-1", scheduledAt, "unlisted"); err != nil {
		t.Fatalf("SchedulePublish returned error: %v", err)
	}
}
