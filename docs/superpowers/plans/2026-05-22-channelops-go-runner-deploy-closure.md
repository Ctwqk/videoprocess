# ChannelOps Go Runner Deploy Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `channelops-runner-go` a complete experimental-deployment worker for alerts, retention, learning recompute, health probes, and metrics while making the FastAPI learning recompute endpoint real.

**Architecture:** Keep Go runner as the only long-running ChannelOps worker in the `channelops-go` profile. Add small focused Go units for alert delivery, retention cleanup, operations scheduling, runner HTTP probes, and Prometheus counters, then wire handlers through the existing queue boundary. Keep FastAPI as control/read surface and implement manual recompute with the same source-dimension aggregation semantics as Go.

**Tech Stack:** Go 1.25, pgx, net/http, prometheus/client_golang, FastAPI, SQLAlchemy async, pytest.

---

## Scope Check

The approved spec touches multiple deployment concerns, but they all close one runtime boundary: replacing Python-runner-only ChannelOps operations with Go-runner-owned operations. Keep this as one plan because each task lands a working slice and the final deployment is not complete until all slices are present.

## File Structure

- Modify `internal/channelops/types.go`: queue kind/status constants.
- Modify `internal/channelops/config.go` and `internal/channelops/config_test.go`: Go runner env/config for retention, metrics port, and quota denominator.
- Create `internal/channelops/alerts.go` and `internal/channelops/alerts_test.go`: Slack-compatible alert payload and delivery.
- Modify `internal/channelops/handlers.go` and `internal/channelops/handlers_test.go`: dispatch new queue kinds and alert helper.
- Create `internal/channelops/retention.go` and `internal/channelops/retention_test.go`: retention cleanup store logic.
- Modify `internal/channelops/queue.go`: add an idempotent enqueue helper that reports whether a row was created.
- Create `internal/channelops/ops_scheduler.go` and `internal/channelops/ops_scheduler_test.go`: daily cleanup and learning enqueue logic.
- Modify `internal/channelops/runner.go` and `internal/channelops/runner_test.go`: call ops scheduler and record queue metrics.
- Create `internal/channelops/runner_http.go` and `internal/channelops/runner_http_test.go`: `/healthz`, `/readyz`, and `/metrics`.
- Create `internal/channelops/runner_metrics.go`: Prometheus counters and histograms.
- Modify `cmd/channelops-runner/main.go`: start runner HTTP server after runner creation.
- Modify `docker-compose.yml`: expose the Go runner metrics/probe port for `channelops-go`.
- Create `backend/app/channel_agent/learning.py`: Python learning recompute service.
- Modify `backend/app/api/channel_agent.py`: call Python recompute service from the endpoint.
- Modify `backend/tests/channel_agent/test_api.py`: replace stub-presence test with real recompute/idempotency coverage.

## Task 1: Queue Constants And Runner Config

**Files:**
- Modify: `internal/channelops/types.go`
- Modify: `internal/channelops/config.go`
- Modify: `internal/channelops/config_test.go`
- Modify: `internal/channelops/handlers_test.go`

- [ ] **Step 1: Write failing tests for config defaults and claimable kinds**

Add this test to `internal/channelops/config_test.go`:

```go
func TestLoadConfigDeployClosureDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://vp:vp@localhost:5432/vp")
	t.Setenv("YOUTUBE_MANAGER_URL", "http://youtube:8899")
	t.Setenv("CHANNELOPS_METRICS_ADDR", ":9092")

	cfg := LoadConfig()

	if cfg.MetricsAddr != ":9092" {
		t.Fatalf("MetricsAddr = %q", cfg.MetricsAddr)
	}
	if cfg.RetentionQueueDays != 30 {
		t.Fatalf("RetentionQueueDays = %d, want 30", cfg.RetentionQueueDays)
	}
	if cfg.RetentionAuditDays != 90 {
		t.Fatalf("RetentionAuditDays = %d, want 90", cfg.RetentionAuditDays)
	}
	if cfg.RetentionFeedbackDays != 365 {
		t.Fatalf("RetentionFeedbackDays = %d, want 365", cfg.RetentionFeedbackDays)
	}
	if cfg.YouTubeDailyQuotaUnits != 10000 {
		t.Fatalf("YouTubeDailyQuotaUnits = %d, want 10000", cfg.YouTubeDailyQuotaUnits)
	}
}
```

Add this test to `internal/channelops/handlers_test.go`:

```go
func TestClaimableKindsIncludesDeployClosureOps(t *testing.T) {
	handler := HandlerService{
		Store:    &Store{},
		PDS:      fakePDS{decision: PDSDecision{Verdict: "allow"}},
		AutoFlow: fakeAutoFlow{},
		YouTube:  fakeYouTube{},
	}

	got := handler.ClaimableKinds()
	want := []string{
		QueueAgentTick,
		QueuePlanTask,
		QueueExecuteTask,
		QueueObserveJob,
		QueuePublishTask,
		QueuePromotePublication,
		QueueReconcilePublication,
		QueueCollectMetrics,
		QueueAccountHealth,
		QueueSendAlert,
		QueueCleanupExpired,
		QueueRecomputeLearning,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ClaimableKinds = %#v, want %#v", got, want)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestLoadConfigDeployClosureDefaults|TestClaimableKindsIncludesDeployClosureOps' -count=1
```

Expected: FAIL with missing `MetricsAddr`, `RetentionQueueDays`, `QueueSendAlert`, `QueueCleanupExpired`, or `QueueRecomputeLearning`.

- [ ] **Step 3: Add constants and config fields**

In `internal/channelops/types.go`, extend the constants:

```go
const (
	QueueAgentTick            = "agent_tick"
	QueuePlanTask             = "plan_task"
	QueueExecuteTask          = "execute_task"
	QueueObserveJob           = "observe_job"
	QueuePublishTask          = "publish_task"
	QueuePromotePublication   = "promote_publication"
	QueueReconcilePublication = "reconcile_publication"
	QueueCollectMetrics       = "collect_metrics"
	QueueAccountHealth        = "account_health"
	QueueSendAlert            = "send_alert"
	QueueCleanupExpired       = "cleanup_expired"
	QueueRecomputeLearning    = "recompute_learning"

	QueueStatusQueued       = "queued"
	QueueStatusRunning      = "running"
	QueueStatusSucceeded    = "succeeded"
	QueueStatusFailed       = "failed"
	QueueStatusDeadLettered = "dead_lettered"
	QueueStatusCancelled    = "cancelled"
)
```

In `internal/channelops/config.go`, add fields:

```go
	MetricsAddr            string
	RetentionQueueDays     int
	RetentionAuditDays     int
	RetentionFeedbackDays  int
	YouTubeDailyQuotaUnits int
```

Load them in `LoadConfig()`:

```go
		MetricsAddr:            env("CHANNELOPS_METRICS_ADDR", ""),
		RetentionQueueDays:     intEnv("CHANNEL_AGENT_RETENTION_QUEUE_DAYS", 30),
		RetentionAuditDays:     intEnv("CHANNEL_AGENT_RETENTION_AUDIT_DAYS", 90),
		RetentionFeedbackDays:  intEnv("CHANNEL_AGENT_RETENTION_FEEDBACK_DAYS", 365),
		YouTubeDailyQuotaUnits: intEnv("CHANNEL_AGENT_YOUTUBE_DAILY_QUOTA_UNITS", 10000),
```

Validate positive retention/quota values in `Validate()`:

```go
	if c.RetentionQueueDays <= 0 {
		return errors.New("CHANNEL_AGENT_RETENTION_QUEUE_DAYS must be positive")
	}
	if c.RetentionAuditDays <= 0 {
		return errors.New("CHANNEL_AGENT_RETENTION_AUDIT_DAYS must be positive")
	}
	if c.RetentionFeedbackDays <= 0 {
		return errors.New("CHANNEL_AGENT_RETENTION_FEEDBACK_DAYS must be positive")
	}
	if c.YouTubeDailyQuotaUnits <= 0 {
		return errors.New("CHANNEL_AGENT_YOUTUBE_DAILY_QUOTA_UNITS must be positive")
	}
```

Update `validConfig()` in `internal/channelops/config_test.go` with these values:

```go
		RetentionQueueDays:     30,
		RetentionAuditDays:     90,
		RetentionFeedbackDays:  365,
		YouTubeDailyQuotaUnits: 10000,
```

- [ ] **Step 4: Add new claimable kinds**

In `internal/channelops/handlers.go`, append new kinds after `QueueAccountHealth`:

```go
		QueueAccountHealth,
		QueueSendAlert,
		QueueCleanupExpired,
		QueueRecomputeLearning,
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
go test ./internal/channelops -run 'TestLoadConfigDeployClosureDefaults|TestClaimableKindsIncludesDeployClosureOps|TestValidateRejectsNonPositivePollsAndAttempts' -count=1
```

Expected: PASS.

Commit:

```bash
git add internal/channelops/types.go internal/channelops/config.go internal/channelops/config_test.go internal/channelops/handlers.go internal/channelops/handlers_test.go
git commit -m "feat: add channelops deploy closure queue config"
```

## Task 2: Alert Service And `send_alert` Handler

**Files:**
- Create: `internal/channelops/alerts.go`
- Create: `internal/channelops/alerts_test.go`
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/handlers_test.go`
- Modify: `internal/channelops/runner.go`

- [ ] **Step 1: Write failing alert service tests**

Create `internal/channelops/alerts_test.go`:

```go
package channelops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSlackMessageMatchesPythonAlphaFormat(t *testing.T) {
	msg := SlackMessage(map[string]any{
		"type":        "pds_outage",
		"severity":    "critical",
		"resource_id": "service:pds",
		"message":     "Policy Decision Service is unavailable",
		"details": map[string]any{
			"action_type": "publish",
			"warning":     "pds_unavailable",
		},
	})

	text := msg["text"]
	if !strings.Contains(text, "[ChannelOps:critical] pds_outage service:pds - Policy Decision Service is unavailable") {
		t.Fatalf("text = %q", text)
	}
	if !strings.Contains(text, "action_type: publish") || !strings.Contains(text, "warning: pds_unavailable") {
		t.Fatalf("details missing from text: %q", text)
	}
}

func TestAlertServiceRecordsWithoutWebhook(t *testing.T) {
	service := AlertService{}
	result, err := service.Send(context.Background(), map[string]any{"type": "quota_below_20pct"})
	if err != nil {
		t.Fatalf("Send returned error: %v", err)
	}
	if result.Status != "recorded" {
		t.Fatalf("Status = %q, want recorded", result.Status)
	}
}

func TestAlertServicePostsSlackWebhook(t *testing.T) {
	var body map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode slack payload: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	service := AlertService{SlackWebhookURL: server.URL}
	result, err := service.Send(context.Background(), map[string]any{
		"type":        "token_expiring_24h",
		"severity":    "warning",
		"resource_id": "account-1",
		"message":     "YouTube OAuth token refresh failed",
	})
	if err != nil {
		t.Fatalf("Send returned error: %v", err)
	}
	if result.Status != "sent" || result.SlackStatusCode != http.StatusOK {
		t.Fatalf("result = %#v", result)
	}
	if !strings.Contains(body["text"].(string), "token_expiring_24h") {
		t.Fatalf("slack body = %#v", body)
	}
}

func TestAlertServiceErrorsOnSlackFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusBadGateway)
	}))
	defer server.Close()

	service := AlertService{SlackWebhookURL: server.URL}
	_, err := service.Send(context.Background(), map[string]any{"type": "pds_outage"})
	if err == nil {
		t.Fatal("expected Slack failure to return error")
	}
	if !strings.Contains(err.Error(), "502") {
		t.Fatalf("error = %v", err)
	}
}
```

Add this handler test to `internal/channelops/handlers_test.go`:

```go
type recordingAlertSender struct {
	payloads []map[string]any
}

func (r *recordingAlertSender) Send(ctx context.Context, payload map[string]any) (AlertResult, error) {
	r.payloads = append(r.payloads, jsonObject(payload))
	return AlertResult{Status: "recorded", Type: firstString(payload, "type")}, nil
}

func TestHandleSendAlertUsesAlertSender(t *testing.T) {
	sender := &recordingAlertSender{}
	handler := HandlerService{Store: &Store{}, Alerts: sender}

	err := handler.Handle(context.Background(), QueueItemRow{
		Kind: QueueSendAlert,
		PayloadJSON: map[string]any{
			"type":        "quota_below_20pct",
			"severity":    "warning",
			"resource_id": "account-1",
			"message":     "quota low",
		},
	})
	if err != nil {
		t.Fatalf("Handle send_alert: %v", err)
	}
	if len(sender.payloads) != 1 || sender.payloads[0]["type"] != "quota_below_20pct" {
		t.Fatalf("payloads = %#v", sender.payloads)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestSlackMessage|TestAlertService|TestHandleSendAlertUsesAlertSender' -count=1
```

Expected: FAIL with missing `AlertService`, `SlackMessage`, `AlertResult`, `Alerts`, or `QueueSendAlert` dispatch.

- [ ] **Step 3: Implement alert service**

Create `internal/channelops/alerts.go`:

```go
package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type AlertSender interface {
	Send(ctx context.Context, payload map[string]any) (AlertResult, error)
}

type AlertResult struct {
	Status          string
	Type            string
	SlackStatusCode int
	EmailTo         string
}

type AlertService struct {
	SlackWebhookURL string
	EmailTo         string
	Timeout         time.Duration
	HTTPClient      *http.Client
}

func (s AlertService) Send(ctx context.Context, payload map[string]any) (AlertResult, error) {
	alertType := firstString(payload, "type")
	result := AlertResult{Status: "recorded", Type: alertType, EmailTo: strings.TrimSpace(s.EmailTo)}
	webhook := strings.TrimSpace(s.SlackWebhookURL)
	if webhook == "" {
		return result, nil
	}

	body, err := json.Marshal(SlackMessage(payload))
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook, bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := s.HTTPClient
	if client == nil {
		timeout := s.Timeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		client = &http.Client{Timeout: timeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	result.SlackStatusCode = resp.StatusCode
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return result, fmt.Errorf("slack webhook returned status %d", resp.StatusCode)
	}
	result.Status = "sent"
	return result, nil
}

func SlackMessage(payload map[string]any) map[string]string {
	alertType := stringOrFallback(payload["type"], "channel_ops_alert")
	severity := stringOrFallback(payload["severity"], "info")
	resourceID := stringOrFallback(payload["resource_id"], "-")
	message := stringOrFallback(payload["message"], alertType)
	text := fmt.Sprintf("[ChannelOps:%s] %s %s - %s", severity, alertType, resourceID, message)
	details := mapFromAny(payload["details"])
	if len(details) > 0 {
		keys := make([]string, 0, len(details))
		for key := range details {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		lines := make([]string, 0, len(keys))
		for _, key := range keys {
			lines = append(lines, fmt.Sprintf("%s: %v", key, details[key]))
		}
		text += "\n" + strings.Join(lines, "\n")
	}
	return map[string]string{"text": text}
}
```

- [ ] **Step 4: Wire handler and runner construction**

In `internal/channelops/handlers.go`, add field:

```go
	Alerts   AlertSender
```

Add case to `Handle()`:

```go
	case QueueSendAlert:
		return h.HandleSendAlert(ctx, item)
```

Add method:

```go
func (h HandlerService) HandleSendAlert(ctx context.Context, item QueueItemRow) error {
	sender := h.Alerts
	if sender == nil {
		sender = AlertService{
			SlackWebhookURL: h.Config.SlackWebhookURL,
			EmailTo:         h.Config.AlertEmailTo,
			Timeout:         10 * time.Second,
		}
	}
	result, err := sender.Send(ctx, jsonObject(item.PayloadJSON))
	if err != nil {
		return err
	}
	return nil
}
```

In `internal/channelops/runner.go`, update `newRunnerHandlerService()` return:

```go
	alerts := AlertService{
		SlackWebhookURL: cfg.SlackWebhookURL,
		EmailTo:         cfg.AlertEmailTo,
		Timeout:         10 * time.Second,
	}
	return HandlerService{Store: st, PDS: pds, AutoFlow: autoflow, YouTube: youtube, Alerts: alerts, Config: cfg}
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
go test ./internal/channelops -run 'TestSlackMessage|TestAlertService|TestHandleSendAlertUsesAlertSender' -count=1
```

Expected: PASS.

Commit:

```bash
git add internal/channelops/alerts.go internal/channelops/alerts_test.go internal/channelops/handlers.go internal/channelops/handlers_test.go internal/channelops/runner.go
git commit -m "feat: add channelops alert delivery"
```

## Task 3: Retention Cleanup, Idempotent Enqueue, And Ops Scheduler

**Files:**
- Modify: `internal/channelops/queue.go`
- Create: `internal/channelops/retention.go`
- Create: `internal/channelops/retention_test.go`
- Create: `internal/channelops/ops_scheduler.go`
- Create: `internal/channelops/ops_scheduler_test.go`
- Modify: `internal/channelops/runner.go`
- Modify: `internal/channelops/runner_test.go`

- [ ] **Step 1: Write failing retention and ops scheduler tests**

Create `internal/channelops/retention_test.go`:

```go
package channelops

import (
	"context"
	"testing"
	"time"
)

func TestCleanupExpiredDeletesOnlyExpiredOperationalRows(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)

	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	old := now.AddDate(0, 0, -40)
	recent := now.AddDate(0, 0, -1)

	_, err := fixture.Store.Pool.Exec(ctx, `
		INSERT INTO channel_ops_queue_items
			(id, kind, idempotency_key, payload_json, status, priority, run_after, attempt_count, max_attempts, channel_profile_id, created_at, updated_at)
		VALUES
			(gen_random_uuid(), 'collect_metrics', 'retention-old-terminal', '{}'::json, 'succeeded', 100, $2, 0, 3, $1::uuid, $2, $2),
			(gen_random_uuid(), 'collect_metrics', 'retention-old-running', '{}'::json, 'running', 100, $2, 1, 3, $1::uuid, $2, $2),
			(gen_random_uuid(), 'collect_metrics', 'retention-recent-terminal', '{}'::json, 'succeeded', 100, $3, 0, 3, $1::uuid, $3, $3)
	`, fixture.ChannelID, old, recent)
	if err != nil {
		t.Fatalf("insert queue rows: %v", err)
	}
	_, err = fixture.Store.Pool.Exec(ctx, `
		INSERT INTO agent_tick_audits (id, channel_profile_id, tick_id, started_at, completed_at, decision_summary_json)
		VALUES
			(gen_random_uuid(), $1::uuid, 'retention-old-audit', $2, $2, '{}'::json),
			(gen_random_uuid(), $1::uuid, 'retention-recent-audit', $3, $3, '{}'::json)
	`, fixture.ChannelID, old.AddDate(0, 0, -60), recent)
	if err != nil {
		t.Fatalf("insert audit rows: %v", err)
	}

	result, err := fixture.Store.CleanupExpired(ctx, now, RetentionConfig{
		QueueDays:    30,
		AuditDays:    90,
		FeedbackDays: 365,
	})
	if err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}
	if result.DeletedQueueItems != 1 || result.DeletedTickAudits != 1 {
		t.Fatalf("result = %#v", result)
	}
}
```

Create `internal/channelops/ops_scheduler_test.go`:

```go
package channelops

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestOpsSchedulerEnqueuesDailyCleanupAndLearning(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)

	scheduler := OpsScheduler{Store: fixture.Store, LearningWindowDays: 7}
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)

	result, err := scheduler.RunOnce(ctx, now)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.EnqueuedCleanup != 1 || result.EnqueuedLearning != 1 {
		t.Fatalf("result = %#v", result)
	}
	second, err := scheduler.RunOnce(ctx, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("RunOnce second: %v", err)
	}
	if second.EnqueuedCleanup != 0 || second.EnqueuedLearning != 0 {
		t.Fatalf("second result = %#v", second)
	}

	var count int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_ops_queue_items
		WHERE idempotency_key IN ($1, $2)
	`, "cleanup_expired:2026-05-22", fmt.Sprintf("recompute_learning:%s:7:2026-05-22", fixture.ChannelID)).Scan(&count); err != nil {
		t.Fatalf("count ops queue items: %v", err)
	}
	if count != 2 {
		t.Fatalf("ops queue item count = %d, want 2", count)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestCleanupExpiredDeletesOnlyExpiredOperationalRows|TestOpsSchedulerEnqueuesDailyCleanupAndLearning' -count=1
```

Expected: FAIL with missing `CleanupExpired`, `RetentionConfig`, or `OpsScheduler`.

- [ ] **Step 3: Add idempotent enqueue helper**

In `internal/channelops/queue.go`, add:

```go
func (s *Store) EnqueueIfAbsent(ctx context.Context, opts EnqueueOptions) (string, bool, error) {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = s.defaultMaxAttempts()
	}
	if opts.RunAfter.IsZero() {
		opts.RunAfter = s.Now().UTC()
	}
	payload, err := json.Marshal(jsonObject(opts.Payload))
	if err != nil {
		return "", false, err
	}

	var id string
	err = s.Pool.QueryRow(ctx, `
		INSERT INTO channel_ops_queue_items
			(id, kind, idempotency_key, payload_json, status, priority, run_after, attempt_count,
			 max_attempts, channel_profile_id, parent_queue_item_id)
		VALUES (gen_random_uuid(), $1, $2, $3::jsonb, $4, $5, $6, 0, $7, $8, $9)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id
	`, opts.Kind, opts.IdempotencyKey, payload, QueueStatusQueued, opts.Priority, opts.RunAfter,
		opts.MaxAttempts, opts.ChannelProfileID, opts.ParentQueueItemID).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", false, err
	}
	err = s.Pool.QueryRow(ctx, `
		SELECT id
		FROM channel_ops_queue_items
		WHERE idempotency_key = $1
	`, opts.IdempotencyKey).Scan(&id)
	return id, false, err
}
```

- [ ] **Step 4: Implement retention cleanup**

Create `internal/channelops/retention.go`:

```go
package channelops

import (
	"context"
	"time"
)

type RetentionConfig struct {
	QueueDays    int
	AuditDays    int
	FeedbackDays int
}

type RetentionResult struct {
	DeletedQueueItems      int64
	DeletedTickAudits      int64
	DeletedDecisionAudits  int64
	DeletedFeedback        int64
}

func (s *Store) CleanupExpired(ctx context.Context, now time.Time, cfg RetentionConfig) (RetentionResult, error) {
	if cfg.QueueDays <= 0 {
		cfg.QueueDays = 30
	}
	if cfg.AuditDays <= 0 {
		cfg.AuditDays = 90
	}
	if cfg.FeedbackDays <= 0 {
		cfg.FeedbackDays = 365
	}
	queueCutoff := now.UTC().AddDate(0, 0, -cfg.QueueDays)
	auditCutoff := now.UTC().AddDate(0, 0, -cfg.AuditDays)
	feedbackCutoff := now.UTC().AddDate(0, 0, -cfg.FeedbackDays)

	result := RetentionResult{}
	tag, err := s.Pool.Exec(ctx, `
		DELETE FROM channel_ops_queue_items
		WHERE status = ANY($1)
		  AND created_at < $2::timestamptz
	`, []string{QueueStatusSucceeded, QueueStatusDeadLettered, QueueStatusCancelled}, queueCutoff)
	if err != nil {
		return result, err
	}
	result.DeletedQueueItems = tag.RowsAffected()

	tag, err = s.Pool.Exec(ctx, `DELETE FROM decision_audit_entries WHERE created_at < $1::timestamptz`, auditCutoff)
	if err != nil {
		return result, err
	}
	result.DeletedDecisionAudits = tag.RowsAffected()

	tag, err = s.Pool.Exec(ctx, `DELETE FROM agent_tick_audits WHERE started_at < $1::timestamptz`, auditCutoff)
	if err != nil {
		return result, err
	}
	result.DeletedTickAudits = tag.RowsAffected()

	tag, err = s.Pool.Exec(ctx, `DELETE FROM feedback_snapshots WHERE collected_at < $1::timestamptz`, feedbackCutoff)
	if err != nil {
		return result, err
	}
	result.DeletedFeedback = tag.RowsAffected()
	return result, nil
}
```

- [ ] **Step 5: Implement ops scheduler**

Create `internal/channelops/ops_scheduler.go`:

```go
package channelops

import (
	"context"
	"fmt"
	"time"
)

type OpsScheduler struct {
	Store              *Store
	LearningWindowDays int
}

type OpsSchedulerResult struct {
	EnqueuedCleanup  int
	EnqueuedLearning int
}

func (s OpsScheduler) RunOnce(ctx context.Context, now time.Time) (OpsSchedulerResult, error) {
	result := OpsSchedulerResult{}
	if s.Store == nil {
		return result, nil
	}
	day := now.UTC().Format("2006-01-02")
	_, created, err := s.Store.EnqueueIfAbsent(ctx, EnqueueOptions{
		Kind:           QueueCleanupExpired,
		IdempotencyKey: "cleanup_expired:" + day,
		Payload:        map[string]any{"day": day},
		Priority:       20,
		RunAfter:       now.UTC(),
	})
	if err != nil {
		return result, err
	}
	if created {
		result.EnqueuedCleanup = 1
	}

	windowDays := s.LearningWindowDays
	if windowDays <= 0 {
		windowDays = 7
	}
	channels, err := s.Store.ListSchedulableChannels(ctx, now)
	if err != nil {
		return result, err
	}
	for _, channel := range channels {
		channelID := channel.ID
		_, created, err := s.Store.EnqueueIfAbsent(ctx, EnqueueOptions{
			Kind:             QueueRecomputeLearning,
			IdempotencyKey:   fmt.Sprintf("recompute_learning:%s:%d:%s", channel.ID, windowDays, day),
			Payload:          map[string]any{"channel_id": channel.ID, "window_days": windowDays},
			Priority:         90,
			RunAfter:         now.UTC(),
			ChannelProfileID: &channelID,
		})
		if err != nil {
			return result, err
		}
		if created {
			result.EnqueuedLearning++
		}
	}
	return result, nil
}
```

- [ ] **Step 6: Wire ops scheduler into runner**

In `internal/channelops/runner.go`, add field:

```go
	OpsScheduler     OpsScheduler
```

In `NewRunner()`:

```go
	runner.OpsScheduler = OpsScheduler{Store: st, LearningWindowDays: 7}
```

In `runOnce()` after existing scheduler `RunOnce`:

```go
		if r.OpsScheduler.Store != nil {
			if _, err := r.OpsScheduler.RunOnce(ctx, now); err != nil {
				return err
			}
		}
```

- [ ] **Step 7: Run tests and commit**

Run:

```bash
go test ./internal/channelops -run 'TestCleanupExpiredDeletesOnlyExpiredOperationalRows|TestOpsSchedulerEnqueuesDailyCleanupAndLearning' -count=1
```

Expected: PASS or SKIP when `DATABASE_URL` is not reachable.

Commit:

```bash
git add internal/channelops/queue.go internal/channelops/retention.go internal/channelops/retention_test.go internal/channelops/ops_scheduler.go internal/channelops/ops_scheduler_test.go internal/channelops/runner.go internal/channelops/runner_test.go
git commit -m "feat: schedule channelops maintenance work"
```

## Task 4: New Queue Handlers For Cleanup And Learning

**Files:**
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/handlers_test.go`
- Modify: `internal/channelops/integration_test.go`

- [ ] **Step 1: Write failing handler tests**

Add to `internal/channelops/handlers_test.go`:

```go
func TestHandleCleanupExpiredRequiresStore(t *testing.T) {
	err := (HandlerService{}).Handle(context.Background(), QueueItemRow{Kind: QueueCleanupExpired})
	if err == nil || !strings.Contains(err.Error(), "store is not configured") {
		t.Fatalf("error = %v", err)
	}
}

func TestHandleRecomputeLearningRequiresChannelID(t *testing.T) {
	handler := HandlerService{Store: &Store{}}
	err := handler.Handle(context.Background(), QueueItemRow{Kind: QueueRecomputeLearning, PayloadJSON: map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "channel_id") {
		t.Fatalf("error = %v", err)
	}
}
```

Add to `internal/channelops/integration_test.go`:

```go
func TestHandleRecomputeLearningWritesLearningState(t *testing.T) {
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
	promote := fixture.ProcessUntilQueueKind(ctx, handler, QueuePromotePublication)
	if err := handler.HandlePromotePublication(ctx, promote); err != nil {
		t.Fatalf("HandlePromotePublication: %v", err)
	}
	if err := fixture.Store.MarkQueueDone(ctx, promote.ID); err != nil {
		t.Fatalf("MarkQueueDone: %v", err)
	}
	collect := fixture.ProcessUntilQueueKind(ctx, handler, QueueCollectMetrics)
	collect.PayloadJSON["metrics"] = map[string]any{
		"views":                 1000,
		"likes":                 80,
		"comments":              12,
		"avg_view_duration_sec": 25,
	}
	if err := handler.HandleCollectMetrics(ctx, collect); err != nil {
		t.Fatalf("HandleCollectMetrics: %v", err)
	}
	if err := handler.Handle(ctx, QueueItemRow{
		Kind:        QueueRecomputeLearning,
		PayloadJSON: map[string]any{"channel_id": fixture.ChannelID, "window_days": 7},
	}); err != nil {
		t.Fatalf("Handle recompute_learning: %v", err)
	}

	var count int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM learning_states
		WHERE channel_profile_id = $1::uuid
	`, fixture.ChannelID).Scan(&count); err != nil {
		t.Fatalf("count learning states: %v", err)
	}
	if count != 1 {
		t.Fatalf("learning state count = %d, want 1", count)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestHandleCleanupExpiredRequiresStore|TestHandleRecomputeLearningRequiresChannelID|TestHandleRecomputeLearningWritesLearningState' -count=1
```

Expected: FAIL with unknown queue kind or missing handlers.

- [ ] **Step 3: Implement handlers**

In `internal/channelops/handlers.go`, add cases:

```go
	case QueueCleanupExpired:
		return h.HandleCleanupExpired(ctx, item)
	case QueueRecomputeLearning:
		return h.HandleRecomputeLearning(ctx, item)
```

Add methods:

```go
func (h HandlerService) HandleCleanupExpired(ctx context.Context, item QueueItemRow) error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	result, err := h.Store.CleanupExpired(ctx, h.Store.Now().UTC(), RetentionConfig{
		QueueDays:    h.Config.RetentionQueueDays,
		AuditDays:    h.Config.RetentionAuditDays,
		FeedbackDays: h.Config.RetentionFeedbackDays,
	})
	if err != nil {
		return err
	}
	return nil
}

func (h HandlerService) HandleRecomputeLearning(ctx context.Context, item QueueItemRow) error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	channelID := firstString(item.PayloadJSON, "channel_id")
	if channelID == "" {
		return errors.New("recompute_learning payload missing channel_id")
	}
	windowDays := intOrDefault(item.PayloadJSON["window_days"], 7)
	if windowDays <= 0 {
		windowDays = 7
	}
	return h.Store.RecomputeLearningState(ctx, channelID, windowDays)
}
```

- [ ] **Step 4: Run tests and commit**

Run:

```bash
go test ./internal/channelops -run 'TestHandleCleanupExpiredRequiresStore|TestHandleRecomputeLearningRequiresChannelID|TestHandleRecomputeLearningWritesLearningState' -count=1
```

Expected: PASS or SKIP for the integration test when `DATABASE_URL` is unreachable.

Commit:

```bash
git add internal/channelops/handlers.go internal/channelops/handlers_test.go internal/channelops/integration_test.go
git commit -m "feat: handle channelops maintenance queue kinds"
```

## Task 5: Runner HTTP Server And Prometheus Metrics

**Files:**
- Create: `internal/channelops/runner_metrics.go`
- Create: `internal/channelops/runner_http.go`
- Create: `internal/channelops/runner_http_test.go`
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/runner.go`
- Modify: `cmd/channelops-runner/main.go`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Write failing HTTP tests**

Create `internal/channelops/runner_http_test.go`:

```go
package channelops

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunnerHTTPHandlerHealthz(t *testing.T) {
	handler := RunnerHTTPHandler(&Runner{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"ok"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestRunnerHTTPHandlerReadyzFailsWithoutStore(t *testing.T) {
	handler := RunnerHTTPHandler(&Runner{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"not_ready"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestRunnerHTTPHandlerMetrics(t *testing.T) {
	handler := RunnerHTTPHandler(&Runner{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "vp_channelops_queue_items_total") {
		t.Fatalf("metrics body missing channelops counter: %s", rec.Body.String())
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestRunnerHTTPHandler' -count=1
```

Expected: FAIL with missing `RunnerHTTPHandler`.

- [ ] **Step 3: Implement metrics definitions**

Create `internal/channelops/runner_metrics.go`:

```go
package channelops

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	channelopsQueueItemsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_channelops_queue_items_total",
		Help: "Total ChannelOps queue items handled by the Go runner.",
	}, []string{"kind", "result"})
	channelopsQueueItemDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "vp_channelops_queue_item_duration_seconds",
		Help:    "Duration of ChannelOps queue item handling.",
		Buckets: prometheus.DefBuckets,
	}, []string{"kind"})
	channelopsSchedulerRunsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_channelops_scheduler_runs_total",
		Help: "Total ChannelOps scheduler runs.",
	}, []string{"scheduler", "result"})
	channelopsAlertsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_channelops_alerts_total",
		Help: "Total ChannelOps alerts handled.",
	}, []string{"type", "result"})
	channelopsRetentionDeletedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_channelops_retention_deleted_total",
		Help: "Total ChannelOps rows deleted by retention cleanup.",
	}, []string{"table"})
	channelopsLearningRecomputeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vp_channelops_learning_recompute_total",
		Help: "Total ChannelOps learning recompute attempts.",
	}, []string{"result"})
)

func observeQueueItem(kind string, result string, started time.Time) {
	if kind == "" {
		kind = "unknown"
	}
	channelopsQueueItemsTotal.WithLabelValues(kind, result).Inc()
	channelopsQueueItemDuration.WithLabelValues(kind).Observe(time.Since(started).Seconds())
}
```

- [ ] **Step 4: Implement runner HTTP handler**

Create `internal/channelops/runner_http.go`:

```go
package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunnerHTTPHandler(r *Runner) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		writeRunnerJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
		defer cancel()
		payload := map[string]string{"status": "ready"}
		status := http.StatusOK
		if err := r.ReadinessError(ctx); err != nil {
			payload["status"] = "not_ready"
			payload["error"] = err.Error()
			status = http.StatusServiceUnavailable
		}
		writeRunnerJSON(w, status, payload)
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func (r *Runner) ReadinessError(ctx context.Context) error {
	if r == nil || r.Store == nil || r.Store.Pool == nil {
		return errors.New("postgres store is not configured")
	}
	if err := r.Store.Pool.Ping(ctx); err != nil {
		return err
	}
	return r.Handlers.ReadinessError()
}

func writeRunnerJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func StartRunnerHTTPServer(ctx context.Context, addr string, r *Runner) func() {
	if addr == "" {
		return func() {}
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           RunnerHTTPHandler(r),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("channelops-runner-go http server stopped", "addr", addr, "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}
}
```

- [ ] **Step 5: Record handler-level metrics**

In `internal/channelops/handlers.go`, update `HandleSendAlert()` after successful send:

```go
	channelopsAlertsTotal.WithLabelValues(result.Type, result.Status).Inc()
```

Update `HandleCleanupExpired()` after successful cleanup:

```go
	channelopsRetentionDeletedTotal.WithLabelValues("channel_ops_queue_items").Add(float64(result.DeletedQueueItems))
	channelopsRetentionDeletedTotal.WithLabelValues("agent_tick_audits").Add(float64(result.DeletedTickAudits))
	channelopsRetentionDeletedTotal.WithLabelValues("decision_audit_entries").Add(float64(result.DeletedDecisionAudits))
	channelopsRetentionDeletedTotal.WithLabelValues("feedback_snapshots").Add(float64(result.DeletedFeedback))
```

Update `HandleRecomputeLearning()`:

```go
	if err := h.Store.RecomputeLearningState(ctx, channelID, windowDays); err != nil {
		channelopsLearningRecomputeTotal.WithLabelValues("failed").Inc()
		return err
	}
	channelopsLearningRecomputeTotal.WithLabelValues("succeeded").Inc()
	return nil
```

- [ ] **Step 6: Observe queue and scheduler results in runner**

In `internal/channelops/runner.go`, wrap item handling:

```go
	started := time.Now()
	if err := r.Handlers.Handle(ctx, *item); err != nil {
		observeQueueItem(item.Kind, "failed", started)
		if markErr := r.Store.MarkQueueFailedOrRetry(ctx, *item, err.Error()); markErr != nil {
			return markErr
		}
		if ShouldDeadLetter(item.AttemptCount, item.MaxAttempts) {
			channelopsQueueItemsTotal.WithLabelValues(item.Kind, "dead_lettered").Inc()
		}
		return nil
	}
	if err := r.Store.MarkQueueDone(ctx, item.ID); err != nil {
		observeQueueItem(item.Kind, "failed", started)
		return err
	}
	observeQueueItem(item.Kind, "succeeded", started)
	return nil
```

In `internal/channelops/runner.go`, update the ops scheduler block:

```go
		if r.OpsScheduler.Store != nil {
			if _, err := r.OpsScheduler.RunOnce(ctx, now); err != nil {
				channelopsSchedulerRunsTotal.WithLabelValues("ops", "failed").Inc()
				return err
			}
			channelopsSchedulerRunsTotal.WithLabelValues("ops", "succeeded").Inc()
		}
```

- [ ] **Step 7: Start HTTP server in `cmd/channelops-runner/main.go`**

After runner creation:

```go
	stopHTTP := channelops.StartRunnerHTTPServer(ctx, cfg.MetricsAddr, runner)
	defer stopHTTP()
```

- [ ] **Step 8: Update Compose**

In `docker-compose.yml`, under `channelops-runner-go.environment`, add:

```yaml
      CHANNELOPS_METRICS_ADDR: ${CHANNELOPS_METRICS_ADDR:-:9092}
      CHANNEL_AGENT_RETENTION_QUEUE_DAYS: ${CHANNEL_AGENT_RETENTION_QUEUE_DAYS:-30}
      CHANNEL_AGENT_RETENTION_AUDIT_DAYS: ${CHANNEL_AGENT_RETENTION_AUDIT_DAYS:-90}
      CHANNEL_AGENT_RETENTION_FEEDBACK_DAYS: ${CHANNEL_AGENT_RETENTION_FEEDBACK_DAYS:-365}
      CHANNEL_AGENT_YOUTUBE_DAILY_QUOTA_UNITS: ${CHANNEL_AGENT_YOUTUBE_DAILY_QUOTA_UNITS:-10000}
```

Add ports:

```yaml
    ports:
      - "${CHANNELOPS_METRICS_PORT:-19092}:9092"
```

- [ ] **Step 9: Run tests and commit**

Run:

```bash
go test ./internal/channelops -run 'TestRunnerHTTPHandler|TestRunnerRunOnceRejectsMissingHandlerDependencies' -count=1
go test ./cmd/channelops-runner ./internal/channelops -count=1
```

Expected: PASS.

Commit:

```bash
git add internal/channelops/runner_metrics.go internal/channelops/runner_http.go internal/channelops/runner_http_test.go internal/channelops/handlers.go internal/channelops/runner.go cmd/channelops-runner/main.go docker-compose.yml
git commit -m "feat: expose channelops runner probes and metrics"
```

## Task 6: Go-Owned Alert Sources

**Files:**
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/store_publications.go`
- Modify: `internal/channelops/handlers_test.go`
- Modify: `internal/channelops/integration_test.go`

- [ ] **Step 1: Write failing PDS outage and account alert tests**

Add to `internal/channelops/handlers_test.go`:

```go
func TestPDSFailPolicyDecisionDetection(t *testing.T) {
	if !IsPDSFailPolicyDecision(PDSDecision{Metadata: map[string]any{"warning": "pds_unavailable", "fail_policy": "block"}}) {
		t.Fatal("expected pds_unavailable fail-policy decision to be detected")
	}
	if IsPDSFailPolicyDecision(PDSDecision{Verdict: "allow", Metadata: map[string]any{"warning": "dev_allow_all", "fail_policy": "allow"}}) {
		t.Fatal("dev allow all must not be treated as outage")
	}
	if IsPDSFailPolicyDecision(PDSDecision{Verdict: "allow", Metadata: map[string]any{}}) {
		t.Fatal("healthy decision detected as fail-policy")
	}
}
```

Add to `internal/channelops/integration_test.go`:

```go
func TestPlanTaskPDSFailPolicyEnqueuesOutageAlert(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close(ctx)
	fixture.InsertChannelWithLaneAccountSeed(ctx)
	handler := fixture.HandlerService(PDSDecision{
		Verdict: "flag",
		Metadata: map[string]any{
			"warning":     "pds_unavailable",
			"fail_policy": "flag",
		},
	})
	handler.Config.PDSEnabled = true

	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, UTCBucket(fixture.Store.Now()), handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	item := fixture.ProcessUntilQueueKind(ctx, handler, QueuePlanTask)
	if err := handler.HandlePlanTask(ctx, item); err != nil {
		t.Fatalf("HandlePlanTask: %v", err)
	}

	var count int
	if err := fixture.Store.Pool.QueryRow(ctx, `
		SELECT count(*)
		FROM channel_ops_queue_items
		WHERE kind = $1
		  AND idempotency_key LIKE 'send_alert:pds_outage:service:pds:%'
	`, QueueSendAlert).Scan(&count); err != nil {
		t.Fatalf("count pds alerts: %v", err)
	}
	if count != 1 {
		t.Fatalf("pds alert count = %d, want 1", count)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestPDSFailPolicyDecisionDetection|TestPlanTaskPDSFailPolicyEnqueuesOutageAlert' -count=1
```

Expected: FAIL with missing `IsPDSFailPolicyDecision` or no alert queue row.

- [ ] **Step 3: Add alert payload helper and PDS wrapper**

In `internal/channelops/handlers.go`, add:

```go
func (h HandlerService) decidePDS(ctx context.Context, request PDSDecisionRequest) (PDSDecision, error) {
	decision, err := h.PDS.Decide(ctx, request)
	if err != nil {
		if h.Config.PDSEnabled {
			_ = h.enqueueAlert(ctx, "pds_outage", "service:pds", "critical", "Policy Decision Service is unavailable", map[string]any{
				"action_type": request.ActionType,
				"error":       err.Error(),
			}, nil)
		}
		return decision, err
	}
	if h.Config.PDSEnabled && IsPDSFailPolicyDecision(decision) {
		metadata := jsonObject(decision.Metadata)
		_ = h.enqueueAlert(ctx, "pds_outage", "service:pds", "critical", "Policy Decision Service is unavailable or returning fail-policy decisions", map[string]any{
			"action_type": request.ActionType,
			"verdict":     decision.Verdict,
			"decision_id": decision.DecisionID,
			"warning":     firstString(metadata, "warning"),
			"fail_policy": firstString(metadata, "fail_policy"),
		}, nil)
	}
	return decision, nil
}

func IsPDSFailPolicyDecision(decision PDSDecision) bool {
	metadata := jsonObject(decision.Metadata)
	warning := firstString(metadata, "warning")
	failPolicy := firstString(metadata, "fail_policy")
	if warning == "dev_allow_all" {
		return false
	}
	switch warning {
	case "pds_disabled", "pds_unavailable", "pds_parse_failed":
		return true
	}
	return warning != "" && failPolicy != "" && failPolicy == decision.Verdict
}

func (h HandlerService) enqueueAlert(ctx context.Context, alertType string, resourceID string, severity string, message string, details map[string]any, channelID *string) error {
	if h.Store == nil {
		return errors.New("channelops handler store is not configured")
	}
	now := h.Store.Now().UTC()
	bucket := UTCBucket(now)
	payload := map[string]any{
		"type":        alertType,
		"resource_id": resourceID,
		"severity":    severity,
		"message":     message,
		"details":     jsonObject(details),
		"created_at":  now.Format(time.RFC3339),
		"dedupe_key":  fmt.Sprintf("send_alert:%s:%s:%s", alertType, resourceID, bucket),
	}
	_, err := h.Store.Enqueue(ctx, EnqueueOptions{
		Kind:             QueueSendAlert,
		IdempotencyKey:   fmt.Sprintf("send_alert:%s:%s:%s", alertType, resourceID, bucket),
		Payload:          payload,
		Priority:         5,
		ChannelProfileID: channelID,
	})
	return err
}
```

Replace direct `h.PDS.Decide(...)` calls in `HandlePlanTask()` and `HandlePromotePublication()` with `h.decidePDS(ctx, request)`.

- [ ] **Step 4: Add account health and takedown alerts**

In `HandleAccountHealth()`, after `health` is fetched and before returning:

```go
	account, accountErr := h.Store.getPublishingAccount(ctx, accountID)
	if accountErr != nil {
		return accountErr
	}
	if err := h.Store.UpdateAccountHealth(ctx, accountID, health); err != nil {
		return err
	}
	channelID := account.ChannelProfileID
	if !health.Authenticated {
		if err := h.enqueueAlert(ctx, "token_expiring_24h", account.ID, "warning", "YouTube OAuth token refresh failed", map[string]any{
			"account_label": account.AccountLabel,
		}, &channelID); err != nil {
			return err
		}
	}
	if health.QuotaRemaining > 0 && h.Config.YouTubeDailyQuotaUnits > 0 {
		remaining := float64(health.QuotaRemaining) / float64(h.Config.YouTubeDailyQuotaUnits)
		if remaining < 0.2 {
			if err := h.enqueueAlert(ctx, "quota_below_20pct", account.ID, "warning", "YouTube quota remaining below 20%", map[string]any{
				"remaining_fraction": remaining,
				"quota_remaining":    health.QuotaRemaining,
			}, &channelID); err != nil {
				return err
			}
		}
	}
	return nil
```

In `HandleReconcilePublication()`, after severe status is marked:

```go
	if isSeverePublicationStatus(status.PublishStatus) {
		if err := h.Store.MarkPublicationSevereDedup(ctx, publication, status, h.Store.Now()); err != nil {
			return err
		}
		task, err := h.Store.GetProductionTask(ctx, publication.ProductionTaskID)
		if err != nil {
			return err
		}
		channelID := task.ChannelProfileID
		return h.enqueueAlert(ctx, "takedown_event_logged", publication.ID, "severe", "YouTube takedown event logged: "+status.PublishStatus, map[string]any{
			"event_type":     status.PublishStatus,
			"publication_id": publication.ID,
		}, &channelID)
	}
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
go test ./internal/channelops -run 'TestPDSFailPolicyDecisionDetection|TestPlanTaskPDSFailPolicyEnqueuesOutageAlert|TestFakeLiveFlowReachesMeasured' -count=1
```

Expected: PASS or SKIP for integration tests without Postgres.

Commit:

```bash
git add internal/channelops/handlers.go internal/channelops/store_publications.go internal/channelops/handlers_test.go internal/channelops/integration_test.go
git commit -m "feat: enqueue channelops operational alerts"
```

## Task 7: FastAPI Manual Learning Recompute

**Files:**
- Create: `backend/app/channel_agent/learning.py`
- Modify: `backend/app/api/channel_agent.py`
- Modify: `backend/tests/channel_agent/test_api.py`

- [ ] **Step 1: Replace stub test with failing real recompute test**

In `backend/tests/channel_agent/test_api.py`, replace `test_learning_recompute_endpoint_is_present` with:

```python
@pytest.mark.asyncio
async def test_learning_recompute_endpoint_writes_learning_state(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Learn"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={"account_label": "yt-main", "platform_account_id": "yt-1", "credential_ref": "youtube/main"},
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            title_seed="learn",
            prompt="learn prompt",
            state="measured",
            score_breakdown_json={},
            rationale_json={},
            transition_history_json=[],
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-learn",
            title="learn",
            compliance_disposition="assumed_fair_use",
        )
        api_session.add(publication)
        await api_session.flush()
        api_session.add(
            FeedbackSnapshot(
                publication_id=publication.id,
                snapshot_stage="24h",
                collected_at=datetime.now(timezone.utc),
                metrics_completeness_score=0.7,
                reward_score=0.72,
                reward_components_json={"views": 0.7},
                available_fields_json=["views", "likes"],
            )
        )
        await api_session.commit()

        response = await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/learning/recompute")

        assert response.status_code == 200
        body = response.json()
        assert body["channel_id"] == channel["id"]
        assert body["recomputed"] is True
        assert body["states_written"] == 1

        learning = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/learning")
        states = learning.json()["states"]
        assert len(states) == 1
        assert states[0]["dimension_type"] == "source"
        assert states[0]["dimension_key"] == "manual_seed"
        assert states[0]["avg_reward"] == 0.72

        second = await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/learning/recompute")
        assert second.status_code == 200
        learning_again = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/learning")
        assert len(learning_again.json()["states"]) == 1
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_api.py::test_learning_recompute_endpoint_writes_learning_state -q
```

Expected: FAIL because `states_written` is missing and no `LearningState` row is written.

- [ ] **Step 3: Implement Python learning service**

Create `backend/app/channel_agent/learning.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import FeedbackSnapshot, LearningState, ProductionTask, PublicationRecord


@dataclass(frozen=True)
class LearningRecomputeResult:
    states_written: int
    window_days: int


def learning_recommendation(sample_count: int, avg_reward: float) -> dict[str, Any]:
    action = "insufficient_data"
    if sample_count >= 10:
        action = "observe"
        if avg_reward >= 0.65:
            action = "promote_more"
        if avg_reward < 0.25:
            action = "cool_down"
    return {"action": action, "sample_count": sample_count, "avg_reward": avg_reward}


async def recompute_learning_state(db: AsyncSession, *, channel_id, window_days: int = 7) -> LearningRecomputeResult:
    if window_days <= 0:
        window_days = 7
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=window_days)
    rows = (
        await db.execute(
            select(
                func.coalesce(func.nullif(ProductionTask.source, ""), "unknown").label("dimension_key"),
                func.count(FeedbackSnapshot.id).label("sample_count"),
                func.avg(FeedbackSnapshot.reward_score).label("avg_reward"),
            )
            .join(PublicationRecord, PublicationRecord.production_task_id == ProductionTask.id)
            .join(FeedbackSnapshot, FeedbackSnapshot.publication_id == PublicationRecord.id)
            .where(ProductionTask.channel_profile_id == channel_id)
            .where(FeedbackSnapshot.collected_at >= since)
            .where(FeedbackSnapshot.metrics_completeness_score >= 0.4)
            .where(FeedbackSnapshot.reward_score.is_not(None))
            .group_by(func.coalesce(func.nullif(ProductionTask.source, ""), "unknown"))
        )
    ).all()

    await db.execute(
        delete(LearningState)
        .where(LearningState.channel_profile_id == channel_id)
        .where(LearningState.dimension_type == "source")
        .where(LearningState.window_days == window_days)
    )
    written = 0
    for dimension_key, sample_count, avg_reward in rows:
        reward = float(avg_reward or 0.0)
        if not isfinite(reward):
            reward = 0.0
        count = int(sample_count or 0)
        confidence = min(count / 20.0, 1.0)
        db.add(
            LearningState(
                channel_profile_id=channel_id,
                dimension_type="source",
                dimension_key=str(dimension_key or "unknown"),
                window_days=window_days,
                sample_count=count,
                avg_reward=reward,
                confidence=confidence,
                recommendation_json=learning_recommendation(count, reward),
                last_computed_at=now,
            )
        )
        written += 1
    await db.commit()
    return LearningRecomputeResult(states_written=written, window_days=window_days)
```

- [ ] **Step 4: Wire FastAPI endpoint**

In `backend/app/api/channel_agent.py`, add import:

```python
from app.channel_agent.learning import recompute_learning_state
```

Change the endpoint to:

```python
@router.post("/channels/{channel_id}/learning/recompute")
async def recompute_learning(channel_id: str, window_days: int = 7, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    result = await recompute_learning_state(db, channel_id=channel.id, window_days=window_days)
    return {
        "channel_id": str(channel.id),
        "recomputed": True,
        "window_days": result.window_days,
        "states_written": result.states_written,
    }
```

- [ ] **Step 5: Run tests and commit**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_api.py::test_learning_recompute_endpoint_writes_learning_state -q
```

Expected: PASS.

Commit:

```bash
git add backend/app/channel_agent/learning.py backend/app/api/channel_agent.py backend/tests/channel_agent/test_api.py
git commit -m "feat: recompute channelops learning from api"
```

## Task 8: Final Verification

**Files:**
- No planned edits.

- [ ] **Step 1: Run Go tests**

Run:

```bash
go test ./cmd/... ./internal/...
```

Expected: PASS. If integration tests skip because `DATABASE_URL` is unreachable, record the exact skip output.

- [ ] **Step 2: Run backend pytest**

Run:

```bash
cd backend
python3 -m pytest
```

Expected: PASS.

- [ ] **Step 3: Run optional backend linters**

Run:

```bash
cd backend
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: command completes. If `ruff` or `mypy` is unavailable, record the missing-module output.

- [ ] **Step 4: Check repository diff**

Run:

```bash
git status --short
git log --oneline -8
```

Expected: worktree clean after all task commits, with commits for the plan tasks visible at the top of history.

- [ ] **Step 5: Manual deploy smoke command**

Run after tests pass:

```bash
docker compose --profile channelops-go config >/tmp/channelops-go-compose.yml
grep -n "CHANNELOPS_METRICS_ADDR" /tmp/channelops-go-compose.yml
grep -n "19092" /tmp/channelops-go-compose.yml
```

Expected: generated compose contains `CHANNELOPS_METRICS_ADDR` and host port `19092`.
