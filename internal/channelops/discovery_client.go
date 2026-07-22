package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	discoveryIngestPath      = "/api/v1/channel-agent/internal/discovery/ingest"
	defaultDiscoveryTimeout  = 120 * time.Second
	maxDiscoveryResponseSize = 1 << 20
)

type DiscoveryClient interface {
	Ingest(ctx context.Context, request DiscoveryIngestRequest) (DiscoveryObservation, error)
}

type DiscoveryIngestRequest struct {
	QueueItemID     string `json:"queue_item_id"`
	ChannelID       string `json:"channel_id"`
	Source          string `json:"source"`
	SchedulerBucket string `json:"scheduler_bucket"`
}

type DiscoveryObservation struct {
	RunID               string
	ChannelID           string
	QueueItemID         string
	Source              string
	SchedulerBucket     string
	Status              string
	QueryCount          int
	CreatedCount        int
	RefreshedCount      int
	ExpiredCount        int
	QuotaUnitsEstimated int
}

type HTTPDiscoveryClient struct {
	BaseURL    string
	Timeout    time.Duration
	HTTPClient *http.Client
}

type discoveryIngestResponse struct {
	RunID               string `json:"run_id"`
	ChannelID           string `json:"channel_id"`
	QueueItemID         string `json:"queue_item_id"`
	Source              string `json:"source"`
	SchedulerBucket     string `json:"scheduler_bucket"`
	Status              string `json:"status"`
	QueryCount          int    `json:"query_count"`
	CreatedCount        int    `json:"created_count"`
	RefreshedCount      int    `json:"refreshed_count"`
	ExpiredCount        int    `json:"expired_count"`
	QuotaUnitsEstimated int    `json:"quota_units_estimated"`
}

func (c HTTPDiscoveryClient) Ingest(ctx context.Context, request DiscoveryIngestRequest) (DiscoveryObservation, error) {
	if err := validateDiscoveryRequest(request); err != nil {
		return DiscoveryObservation{}, err
	}
	endpoint, err := discoveryEndpoint(c.BaseURL)
	if err != nil {
		return DiscoveryObservation{}, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return DiscoveryObservation{}, errors.New("discovery ingest request could not be encoded")
	}
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout())
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return DiscoveryObservation{}, errors.New("discovery ingest request could not be created")
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient().Do(httpRequest)
	if err != nil {
		return DiscoveryObservation{}, errors.New("discovery ingest request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return DiscoveryObservation{}, fmt.Errorf("discovery ingest returned HTTP %d", response.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(response.Body, maxDiscoveryResponseSize+1))
	if err != nil {
		return DiscoveryObservation{}, errors.New("discovery ingest response could not be read")
	}
	if len(raw) > maxDiscoveryResponseSize {
		return DiscoveryObservation{}, errors.New("discovery ingest response is too large")
	}
	var payload discoveryIngestResponse
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return DiscoveryObservation{}, errors.New("discovery ingest response is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return DiscoveryObservation{}, errors.New("discovery ingest response is invalid")
	}
	observation := DiscoveryObservation{
		RunID:               payload.RunID,
		ChannelID:           payload.ChannelID,
		QueueItemID:         payload.QueueItemID,
		Source:              payload.Source,
		SchedulerBucket:     payload.SchedulerBucket,
		Status:              payload.Status,
		QueryCount:          payload.QueryCount,
		CreatedCount:        payload.CreatedCount,
		RefreshedCount:      payload.RefreshedCount,
		ExpiredCount:        payload.ExpiredCount,
		QuotaUnitsEstimated: payload.QuotaUnitsEstimated,
	}
	if err := validateDiscoveryObservation(request, observation); err != nil {
		return DiscoveryObservation{}, err
	}
	return observation, nil
}

func (c HTTPDiscoveryClient) httpClient() *http.Client {
	client := &http.Client{Timeout: c.timeout()}
	if c.HTTPClient != nil {
		clone := *c.HTTPClient
		client = &clone
	}
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client
}

func (c HTTPDiscoveryClient) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultDiscoveryTimeout
}

func discoveryEndpoint(baseURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("AUTOFLOW_BASE_URL is invalid for discovery ingestion")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + discoveryIngestPath
	return parsed.String(), nil
}

func validateDiscoveryRequest(request DiscoveryIngestRequest) error {
	if !canonicalDiscoveryUUID(request.QueueItemID) {
		return errors.New("discovery ingest queue_item_id is invalid")
	}
	if !canonicalDiscoveryUUID(request.ChannelID) {
		return errors.New("discovery ingest channel_id is invalid")
	}
	if request.Source != "youtube_search" {
		return errors.New("discovery ingest source must be youtube_search")
	}
	if strings.TrimSpace(request.SchedulerBucket) == "" || len(request.SchedulerBucket) > 64 {
		return errors.New("discovery ingest scheduler_bucket is invalid")
	}
	return nil
}

func validateDiscoveryObservation(request DiscoveryIngestRequest, observation DiscoveryObservation) error {
	if !canonicalDiscoveryUUID(observation.RunID) {
		return errors.New("discovery ingest response run_id is invalid")
	}
	if observation.ChannelID != request.ChannelID {
		return errors.New("discovery ingest response channel_id mismatch")
	}
	if observation.QueueItemID != request.QueueItemID {
		return errors.New("discovery ingest response queue_item_id mismatch")
	}
	if observation.Source != request.Source {
		return errors.New("discovery ingest response source mismatch")
	}
	if observation.SchedulerBucket != request.SchedulerBucket {
		return errors.New("discovery ingest response scheduler_bucket mismatch")
	}
	if observation.Status != "succeeded" {
		return errors.New("discovery ingest response status must be succeeded")
	}
	for _, counter := range []struct {
		name  string
		value int
	}{
		{name: "query_count", value: observation.QueryCount},
		{name: "created_count", value: observation.CreatedCount},
		{name: "refreshed_count", value: observation.RefreshedCount},
		{name: "expired_count", value: observation.ExpiredCount},
		{name: "quota_units_estimated", value: observation.QuotaUnitsEstimated},
	} {
		if counter.value < 0 {
			return fmt.Errorf("discovery ingest response %s must not be negative", counter.name)
		}
	}
	return nil
}

func canonicalDiscoveryUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}
