package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type AlertSink interface {
	Send(ctx context.Context, alert AlertPayload) error
}

type AlertPayload struct {
	Kind       string         `json:"kind"`
	Severity   string         `json:"severity"`
	ChannelID  string         `json:"channel_id,omitempty"`
	ResourceID string         `json:"resource_id,omitempty"`
	Message    string         `json:"message"`
	Details    map[string]any `json:"details,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	DedupeKey  string         `json:"dedupe_key,omitempty"`
}

type MultiAlertSink []AlertSink

func (s MultiAlertSink) Send(ctx context.Context, alert AlertPayload) error {
	var errs []error
	for _, sink := range s {
		if sink == nil {
			continue
		}
		if err := sink.Send(ctx, alert); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type SlackAlertSink struct {
	WebhookURL string
	HTTPClient *http.Client
}

func (s SlackAlertSink) Send(ctx context.Context, alert AlertPayload) error {
	webhookURL := strings.TrimSpace(s.WebhookURL)
	if webhookURL == "" {
		return nil
	}
	body, err := json.Marshal(map[string]any{"text": slackAlertText(alert)})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest {
		_, _ = io.Copy(io.Discard, response.Body)
		return fmt.Errorf("slack alert webhook returned %s", response.Status)
	}
	return nil
}

type LogAlertSink struct{}

func (LogAlertSink) Send(ctx context.Context, alert AlertPayload) error {
	slog.WarnContext(ctx, "channelops alert",
		"kind", alert.Kind,
		"severity", alert.Severity,
		"channel_id", alert.ChannelID,
		"resource_id", alert.ResourceID,
		"message", alert.Message,
		"details", alert.Details,
	)
	return nil
}

func NewAlertSink(cfg Config) AlertSink {
	sinks := MultiAlertSink{LogAlertSink{}}
	if strings.TrimSpace(cfg.SlackWebhookURL) != "" {
		sinks = append(sinks, SlackAlertSink{WebhookURL: cfg.SlackWebhookURL})
	}
	return sinks
}

func parseAlertPayload(payload map[string]any, now time.Time) (AlertPayload, error) {
	alert := AlertPayload{
		Kind:       firstNonBlankString(payload, "kind", "type"),
		Severity:   firstNonBlankString(payload, "severity"),
		ChannelID:  firstNonBlankString(payload, "channel_id"),
		ResourceID: firstNonBlankString(payload, "resource_id"),
		Message:    firstNonBlankString(payload, "message"),
		Details:    mapFromAny(payload["details"]),
		DedupeKey:  firstNonBlankString(payload, "dedupe_key"),
	}
	if alert.Kind == "" {
		return AlertPayload{}, errors.New("send_alert payload missing kind")
	}
	if alert.Severity == "" {
		alert.Severity = "warning"
	}
	if alert.Message == "" {
		alert.Message = alert.Kind
	}
	if createdAtRaw := firstNonBlankString(payload, "created_at"); createdAtRaw != "" {
		createdAt, err := time.Parse(time.RFC3339, createdAtRaw)
		if err != nil {
			return AlertPayload{}, fmt.Errorf("send_alert created_at: %w", err)
		}
		alert.CreatedAt = createdAt.UTC()
	}
	if alert.CreatedAt.IsZero() {
		alert.CreatedAt = now.UTC()
	}
	return alert, nil
}

func (a AlertPayload) queuePayload(now time.Time) map[string]any {
	alert := a
	if alert.Severity == "" {
		alert.Severity = "warning"
	}
	if alert.Message == "" {
		alert.Message = alert.Kind
	}
	if alert.CreatedAt.IsZero() {
		alert.CreatedAt = now.UTC()
	}
	if alert.DedupeKey == "" {
		alert.DedupeKey = alertDedupeKey(alert, now)
	}
	return map[string]any{
		"kind":        alert.Kind,
		"type":        alert.Kind,
		"severity":    alert.Severity,
		"channel_id":  alert.ChannelID,
		"resource_id": alert.ResourceID,
		"message":     alert.Message,
		"details":     jsonObject(alert.Details),
		"created_at":  alert.CreatedAt.UTC().Format(time.RFC3339),
		"dedupe_key":  alert.DedupeKey,
	}
}

func (s *Store) EnqueueAlert(ctx context.Context, alert AlertPayload, priority int, parentQueueItemID string) (string, error) {
	return s.enqueueAlert(ctx, s.db(), alert, priority, parentQueueItemID)
}

func (s *Store) enqueueAlert(ctx context.Context, db dbExecutor, alert AlertPayload, priority int, parentQueueItemID string) (string, error) {
	if priority == 0 {
		priority = 5
	}
	now := s.Now().UTC()
	payload := alert.queuePayload(now)
	parentID, err := optionalUUID("parent_queue_item_id", parentQueueItemID)
	if err != nil {
		return "", err
	}
	var channelID *string
	if strings.TrimSpace(alert.ChannelID) != "" {
		value := alert.ChannelID
		channelID = &value
	}
	return s.enqueue(ctx, db, EnqueueOptions{
		Kind:              QueueSendAlert,
		IdempotencyKey:    stringOrFallback(payload["dedupe_key"], alertDedupeKey(alert, now)),
		Payload:           payload,
		Priority:          priority,
		ChannelProfileID:  channelID,
		ParentQueueItemID: parentID,
	})
}

func maybePDSOutageAlert(decision PDSDecision, channelID string, resourceID string, actionType string) (AlertPayload, bool) {
	if !isPDSFailPolicyDecision(decision) {
		return AlertPayload{}, false
	}
	metadata := jsonObject(decision.Metadata)
	return AlertPayload{
		Kind:       "pds_outage",
		Severity:   "warning",
		ChannelID:  channelID,
		ResourceID: resourceID,
		Message:    "Policy Decision Service is unavailable or returning fail-policy decisions",
		Details: map[string]any{
			"action_type": actionType,
			"verdict":     decision.Verdict,
			"decision_id": decision.DecisionID,
			"warning":     stringOrFallback(metadata["warning"], ""),
			"fail_policy": stringOrFallback(metadata["fail_policy"], ""),
		},
	}, true
}

func isPDSFailPolicyDecision(decision PDSDecision) bool {
	metadata := jsonObject(decision.Metadata)
	warning := stringOrFallback(metadata["warning"], "")
	if stringOrFallback(metadata["fail_policy"], "") != "" {
		return true
	}
	switch warning {
	case "pds_disabled", "pds_unavailable", "pds_parse_failed":
		return true
	default:
		return false
	}
}

func pdsDecisionAuditJSON(decision PDSDecision) map[string]any {
	return map[string]any{
		"decision_id":     decision.DecisionID,
		"verdict":         decision.Verdict,
		"score":           decision.Score,
		"reasons":         decision.Reasons,
		"evaluated_rules": decision.EvaluatedRules,
		"rules_version":   decision.RulesVersion,
		"latency_ms":      decision.LatencyMS,
		"metadata":        jsonObject(decision.Metadata),
	}
}

func quotaLowAlert(channelID string, accountID string, remaining int) (AlertPayload, bool) {
	if remaining < 0 || remaining >= 2000 {
		return AlertPayload{}, false
	}
	severity := "warning"
	if remaining == 0 {
		severity = "critical"
	}
	return AlertPayload{
		Kind:       "quota_low",
		Severity:   severity,
		ChannelID:  channelID,
		ResourceID: accountID,
		Message:    "YouTube quota remaining is below the ChannelOps safety threshold",
		Details: map[string]any{
			"quota_remaining": remaining,
			"threshold":       2000,
		},
	}, true
}

func platformRejectedAlert(publication PublicationRow, channelID string, status YouTubePublicationStatus) AlertPayload {
	return AlertPayload{
		Kind:       "platform_rejected",
		Severity:   "critical",
		ChannelID:  channelID,
		ResourceID: publication.ID,
		Message:    "YouTube reported a severe publication status",
		Details: map[string]any{
			"production_task_id":  publication.ProductionTaskID,
			"platform_content_id": publication.PlatformContentID,
			"publish_status":      status.PublishStatus,
			"privacy":             status.Privacy,
			"permalink":           status.Permalink,
		},
	}
}

func materialLowSupplyAlert(channelID string, bucket string, accepted int, rejected int) AlertPayload {
	return AlertPayload{
		Kind:      "material_low_supply",
		Severity:  "warning",
		ChannelID: channelID,
		Message:   "ChannelOps tick produced no accepted candidates",
		Details: map[string]any{
			"bucket":   bucket,
			"accepted": accepted,
			"rejected": rejected,
		},
	}
}

func slackAlertText(alert AlertPayload) string {
	kind := stringOrFallback(alert.Kind, "channel_ops_alert")
	severity := stringOrFallback(alert.Severity, "info")
	resourceID := stringOrFallback(alert.ResourceID, "-")
	message := stringOrFallback(alert.Message, kind)
	text := fmt.Sprintf("[ChannelOps:%s] %s %s - %s", severity, kind, resourceID, message)
	for key, value := range jsonObject(alert.Details) {
		text += fmt.Sprintf("\n%s: %v", key, value)
	}
	return text
}

func alertDedupeKey(alert AlertPayload, now time.Time) string {
	resourceID := alert.ResourceID
	if resourceID == "" {
		resourceID = alert.ChannelID
	}
	if resourceID == "" {
		resourceID = "global"
	}
	channelID := alert.ChannelID
	if channelID == "" {
		channelID = "global"
	}
	return fmt.Sprintf("send_alert:%s:%s:%s:%s", alert.Kind, channelID, resourceID, UTCBucket(now))
}

func firstNonBlankString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(fmt.Sprint(values[key]))
		if value != "" && value != "<nil>" {
			return value
		}
	}
	return ""
}
