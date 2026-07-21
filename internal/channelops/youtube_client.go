package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type YouTubeClient interface {
	AccountHealth(ctx context.Context, accountID string) (YouTubeAccountHealth, error)
	SchedulePublish(ctx context.Context, videoID string, scheduledAt time.Time, privacy string, idempotencyKey string) error
	PublicationStatus(ctx context.Context, videoID string) (YouTubePublicationStatus, error)
	FetchMetrics(ctx context.Context, videoID string) (map[string]any, error)
}

type YouTubeAccountHealth struct {
	Authenticated  bool
	QuotaRemaining int
	Raw            map[string]any
}

type YouTubePublicationStatus struct {
	VideoID       string
	PublishStatus string
	Privacy       string
	Permalink     string
	Raw           map[string]any
}

type YouTubeManagerClient struct {
	BaseURL    string
	Timeout    time.Duration
	HTTPClient *http.Client
}

func (c YouTubeManagerClient) AccountHealth(ctx context.Context, accountID string) (YouTubeAccountHealth, error) {
	payload, err := c.getJSON(ctx, "/api/auth/status")
	if err != nil {
		return YouTubeAccountHealth{}, err
	}
	quota := mapFromAny(payload["quota_estimate"])
	return YouTubeAccountHealth{
		Authenticated:  boolValue(payload["authenticated"]),
		QuotaRemaining: intOrDefault(quota["estimated_units_remaining"], -1),
		Raw:            payload,
	}, nil
}

func (c YouTubeManagerClient) SchedulePublish(ctx context.Context, videoID string, scheduledAt time.Time, privacy string, idempotencyKey string) error {
	if strings.TrimSpace(videoID) == "" {
		return fmt.Errorf("video_id is required")
	}
	if safePromotionVisibility(privacy) == "" {
		return fmt.Errorf("promotion privacy must be private or unlisted")
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return fmt.Errorf("promotion idempotency key is required")
	}
	payload := map[string]any{
		"scheduled_at": scheduledAt.UTC().Format(time.RFC3339),
		"privacy":      privacy,
	}
	_, err := c.postJSONWithIdempotency(
		ctx,
		"/api/videos/"+url.PathEscape(videoID)+"/schedule",
		payload,
		idempotencyKey,
	)
	return err
}

func (c YouTubeManagerClient) PublicationStatus(ctx context.Context, videoID string) (YouTubePublicationStatus, error) {
	if strings.TrimSpace(videoID) == "" {
		return YouTubePublicationStatus{}, fmt.Errorf("video_id is required")
	}
	payload, err := c.getJSON(ctx, "/api/videos/"+url.PathEscape(videoID)+"/status")
	if err != nil {
		return YouTubePublicationStatus{}, err
	}
	return YouTubePublicationStatus{
		VideoID:       stringOrFallback(payload["video_id"], videoID),
		PublishStatus: normalizedStatus(firstString(payload, "publish_status", "processing_state", "upload_status", "status")),
		Privacy:       observedPrivacy(firstString(payload, "privacy", "current_privacy")),
		Permalink:     firstString(payload, "permalink", "url"),
		Raw:           payload,
	}, nil
}

func (c YouTubeManagerClient) FetchMetrics(ctx context.Context, videoID string) (map[string]any, error) {
	if strings.TrimSpace(videoID) == "" {
		return map[string]any{}, fmt.Errorf("video_id is required")
	}
	payload, err := c.getJSON(ctx, "/api/videos/"+url.PathEscape(videoID)+"/metrics")
	if err != nil {
		return map[string]any{}, err
	}
	if metrics := mapFromAny(payload["metrics"]); len(metrics) > 0 {
		return metrics, nil
	}
	return payload, nil
}

func (c YouTubeManagerClient) getJSON(ctx context.Context, path string) (map[string]any, error) {
	return c.doJSON(ctx, http.MethodGet, path, nil, "")
}

func (c YouTubeManagerClient) postJSON(ctx context.Context, path string, payload map[string]any) (map[string]any, error) {
	return c.postJSONWithIdempotency(ctx, path, payload, "")
}

func (c YouTubeManagerClient) postJSONWithIdempotency(ctx context.Context, path string, payload map[string]any, idempotencyKey string) (map[string]any, error) {
	raw, err := json.Marshal(jsonObject(payload))
	if err != nil {
		return nil, err
	}
	return c.doJSON(ctx, http.MethodPost, path, bytes.NewReader(raw), idempotencyKey)
}

func (c YouTubeManagerClient) doJSON(ctx context.Context, method string, path string, body *bytes.Reader, idempotencyKey string) (map[string]any, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("YOUTUBE_MANAGER_URL is required for live ChannelOps runner mode")
	}
	var requestBody io.Reader
	if body != nil {
		requestBody = body
	}
	request, err := http.NewRequestWithContext(ctx, method, baseURL+path, requestBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if strings.TrimSpace(idempotencyKey) != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	client := c.HTTPClient
	if client == nil {
		timeout := c.Timeout
		if timeout <= 0 {
			timeout = 20 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("youtube-manager %s %s returned %s", method, path, response.Status)
	}
	var payload map[string]any
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return jsonObject(payload), nil
}
