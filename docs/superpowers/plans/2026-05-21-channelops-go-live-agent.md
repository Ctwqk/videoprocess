# ChannelOps Go Live Agent Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Go-owned ChannelOps live runner for Phase 0 and Phase A so scheduler, tick, queue handlers, publish, reconcile, and metrics can run without the legacy Python runner.

**Architecture:** Add `cmd/channelops-runner` and focused packages under `internal/channelops/`. Keep Python FastAPI, SQLAlchemy models, Alembic migrations, and AutoFlow internals where they already live; Go calls AutoFlow, PDS, and YouTubeManager over HTTP and owns the live queue/state machine. Live deployments must run Go ChannelOps runner only, with Python runner disabled.

**Tech Stack:** Go 1.25, pgx, net/http, Python FastAPI/SQLAlchemy/Alembic for existing APIs and migrations, pytest for Python seams, `go test` for Go runtime packages, docker compose for live runner deployment.

---

## File Structure

Create Go runtime files:

- `cmd/channelops-runner/main.go` - process entrypoint, config load, DB open, client wiring, runner start/shutdown.
- `internal/channelops/config.go` - runner-specific env parsing with defaults.
- `internal/channelops/types.go` - constants and row structs mirroring ChannelOps DB state.
- `internal/channelops/store.go` - store type and shared SQL helpers.
- `internal/channelops/queue.go` - claim, enqueue, mark done, retry, dead-letter.
- `internal/channelops/scheduler.go` - channel scanning and tick enqueue.
- `internal/channelops/pds_client.go` - PDS HTTP client, fail policy, dev allow-all.
- `internal/channelops/autoflow_client.go` - AutoFlow HTTP client interface and payload structs.
- `internal/channelops/youtube_client.go` - YouTubeManager HTTP client interface and payload structs.
- `internal/channelops/materials.go` - material reference extraction and repetition guard helpers.
- `internal/channelops/metrics.go` - feedback completeness and snapshot mapping helpers.
- `internal/channelops/tick.go` - candidate generation, dry-run audit, guard orchestration, task creation.
- `internal/channelops/handlers.go` - queue item dispatch and handler implementations.
- `internal/channelops/runner.go` - polling loop that combines scheduler and queue processing.
- `internal/channelops/live_smoke.go` - reusable smoke runner logic.
- `cmd/channelops-live-smoke/main.go` - operator CLI for live unlisted smoke.

Create Go tests:

- `internal/channelops/queue_test.go`
- `internal/channelops/scheduler_test.go`
- `internal/channelops/pds_client_test.go`
- `internal/channelops/materials_test.go`
- `internal/channelops/metrics_test.go`
- `internal/channelops/tick_test.go`
- `internal/channelops/handlers_test.go`
- `internal/channelops/integration_test.go`

Modify Python-owned seams:

- `backend/app/schemas/autoflow.py`
- `backend/app/autoflow/search_service.py`
- `backend/app/autoflow/service.py`
- `backend/app/autoflow/clip_ranker.py`
- `backend/app/channel_agent/material_usage.py`
- `backend/app/models/channel_agent.py`
- `backend/alembic/versions/019_channelops_go_live_phase0.py`

Modify deployment/docs:

- `backend/Dockerfile.channelops-runner-go`
- `docker-compose.yml`
- `docker-compose.gpu.yml` if it currently starts ChannelOps live services there.
- `docs/channelops-go-live-runner.md`

Do not delete Python ChannelOps runner in this plan. Only ensure live compose no longer starts it alongside Go.

---

## Task 1: Add Go Runner Config And Entrypoint Skeleton

**Files:**
- Create: `internal/channelops/config.go`
- Create: `cmd/channelops-runner/main.go`
- Test: `internal/channelops/config_test.go`
- Modify: `internal/config/config.go`

- [ ] **Step 1: Write failing config tests**

Create `internal/channelops/config_test.go`:

```go
package channelops

import "testing"

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgresql://vp:vp@localhost:5432/vp")
	t.Setenv("YOUTUBE_MANAGER_URL", "http://youtube:8899")
	cfg := LoadConfig()
	if cfg.DatabaseURL != "postgresql://vp:vp@localhost:5432/vp" {
		t.Fatalf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.YouTubeManagerURL != "http://youtube:8899" {
		t.Fatalf("YouTubeManagerURL = %q", cfg.YouTubeManagerURL)
	}
	if cfg.RunnerPollSeconds != 5 {
		t.Fatalf("RunnerPollSeconds = %v", cfg.RunnerPollSeconds)
	}
	if cfg.SchedulerPollSeconds != 60 {
		t.Fatalf("SchedulerPollSeconds = %v", cfg.SchedulerPollSeconds)
	}
	if cfg.DevAllowAllPDS {
		t.Fatal("DevAllowAllPDS default should be false")
	}
}

func TestValidateLiveRequiresYouTubeManagerURL(t *testing.T) {
	cfg := Config{DatabaseURL: "postgresql://vp:vp@localhost:5432/vp", LiveMode: true}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected Validate to reject missing YouTubeManagerURL")
	}
	cfg.YouTubeManagerURL = "http://youtube:8899"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestLoadConfigDefaults|TestValidateLiveRequiresYouTubeManagerURL' -count=1
```

Expected: fail because `internal/channelops` package and `LoadConfig` do not exist.

- [ ] **Step 3: Implement config**

Create `internal/channelops/config.go`:

```go
package channelops

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL             string
	YouTubeManagerURL       string
	PDSEnabled              bool
	PDSBaseURL              string
	PDSClientID             string
	PDSTimeout              time.Duration
	DevAllowAllPDS          bool
	RunnerPollSeconds       int
	SchedulerPollSeconds    int
	SlackWebhookURL         string
	AlertEmailTo            string
	LiveMode                bool
	MaxQueueAttempts        int
	MetricsPollMaxAttempts  int
	MetricsPollDelayMinutes int
}

func LoadConfig() Config {
	return Config{
		DatabaseURL:             env("DATABASE_URL", "postgresql://vp:vp_secret@localhost:5435/videoprocess"),
		YouTubeManagerURL:       env("YOUTUBE_MANAGER_URL", ""),
		PDSEnabled:              boolEnv("PDS_ENABLED", false),
		PDSBaseURL:              env("PDS_BASE_URL", "http://pds:8080"),
		PDSClientID:             env("PDS_CLIENT_ID", "videoprocess-channel-agent"),
		PDSTimeout:              time.Duration(floatEnv("PDS_TIMEOUT_SECONDS", 0.5) * float64(time.Second)),
		DevAllowAllPDS:          boolEnv("CHANNEL_AGENT_DEV_ALLOW_ALL_PDS", false),
		RunnerPollSeconds:       intEnv("CHANNELOPS_RUNNER_POLL_SECONDS", 5),
		SchedulerPollSeconds:    intEnv("CHANNELOPS_SCHEDULER_POLL_SECONDS", 60),
		SlackWebhookURL:         env("CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL", ""),
		AlertEmailTo:            env("CHANNEL_AGENT_ALERT_EMAIL_TO", ""),
		LiveMode:                boolEnv("CHANNELOPS_LIVE_MODE", true),
		MaxQueueAttempts:        intEnv("CHANNELOPS_QUEUE_MAX_ATTEMPTS", 5),
		MetricsPollMaxAttempts:  intEnv("CHANNELOPS_METRICS_MAX_POLLS", 24),
		MetricsPollDelayMinutes: intEnv("CHANNELOPS_METRICS_POLL_DELAY_MINUTES", 60),
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DatabaseURL) == "" {
		return errors.New("DATABASE_URL is required")
	}
	if c.LiveMode && strings.TrimSpace(c.YouTubeManagerURL) == "" {
		return errors.New("YOUTUBE_MANAGER_URL is required in live ChannelOps mode")
	}
	if c.RunnerPollSeconds <= 0 {
		return errors.New("CHANNELOPS_RUNNER_POLL_SECONDS must be positive")
	}
	if c.SchedulerPollSeconds <= 0 {
		return errors.New("CHANNELOPS_SCHEDULER_POLL_SECONDS must be positive")
	}
	if c.MaxQueueAttempts <= 0 {
		return errors.New("CHANNELOPS_QUEUE_MAX_ATTEMPTS must be positive")
	}
	return nil
}

func env(key string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes" || value == "on"
}

func intEnv(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func floatEnv(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
```

- [ ] **Step 4: Add runner entrypoint skeleton**

Create `cmd/channelops-runner/main.go`:

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Ctwqk/videoprocess/internal/channelops"
)

func main() {
	cfg := channelops.LoadConfig()
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid ChannelOps runner config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	runner, err := channelops.NewRunner(ctx, cfg)
	if err != nil {
		slog.Error("create ChannelOps runner", "error", err)
		os.Exit(1)
	}
	defer runner.Close()

	slog.Info("starting channelops-runner-go")
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("channelops-runner-go stopped", "error", err)
		os.Exit(1)
	}
	slog.Info("channelops-runner-go stopped cleanly", "at", time.Now().UTC())
}
```

- [ ] **Step 5: Add temporary runner stub**

Create `internal/channelops/runner.go` with a stub that compiles and will be expanded in later tasks:

```go
package channelops

import (
	"context"
	"time"
)

type Runner struct {
	Config Config
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	return &Runner{Config: cfg}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(r.Config.RunnerPollSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			return nil
		}
	}
}

func (r *Runner) Close() {}
```

- [ ] **Step 6: Run tests and build**

Run:

```bash
go test ./internal/channelops -run 'TestLoadConfigDefaults|TestValidateLiveRequiresYouTubeManagerURL' -count=1
go build ./cmd/channelops-runner
```

Expected: both pass.

- [ ] **Step 7: Commit**

```bash
git add internal/channelops/config.go internal/channelops/config_test.go internal/channelops/runner.go cmd/channelops-runner/main.go
git commit -m "feat: add channelops go runner skeleton"
```

---

## Task 2: Add ChannelOps Types And Store Foundation

**Files:**
- Create: `internal/channelops/types.go`
- Create: `internal/channelops/store.go`
- Test: `internal/channelops/store_test.go`
- Modify: `internal/channelops/runner.go`

- [ ] **Step 1: Write store foundation tests**

Create `internal/channelops/store_test.go`:

```go
package channelops

import (
	"testing"
	"time"
)

func TestUTCBucket(t *testing.T) {
	now := time.Date(2026, 5, 21, 10, 42, 33, 0, time.FixedZone("PDT", -7*3600))
	got := UTCBucket(now)
	if got != "2026-05-21-17" {
		t.Fatalf("UTCBucket = %q", got)
	}
}

func TestTransitionPayload(t *testing.T) {
	at := time.Date(2026, 5, 21, 17, 0, 0, 0, time.UTC)
	got := Transition("selected", "planning", "plan_task", at)
	if got["from"] != "selected" || got["to"] != "planning" || got["reason"] != "plan_task" {
		t.Fatalf("unexpected transition: %#v", got)
	}
	if got["at"] != "2026-05-21T17:00:00Z" {
		t.Fatalf("unexpected transition timestamp: %#v", got["at"])
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
go test ./internal/channelops -run 'TestUTCBucket|TestTransitionPayload' -count=1
```

Expected: fail because helpers are undefined.

- [ ] **Step 3: Add constants and row types**

Create `internal/channelops/types.go`:

```go
package channelops

import "time"

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

	TaskSelected  = "selected"
	TaskPlanning  = "planning"
	TaskProducing = "producing"
	TaskScheduled = "scheduled"
	TaskMeasured  = "measured"
	TaskHeld      = "held"
	TaskFailed    = "failed"
	TaskRejected  = "rejected"

	ApprovalAgent = "agent"
	ApprovalHuman = "human"

	SourceManualSeed = "manual_seed"
	SourceLaneSeed   = "lane_seed"
	SourceTrendYT    = "trend_youtube"
)

type ChannelProfileRow struct {
	ID                    string
	Enabled               bool
	DryRun                bool
	HaltedAt              *time.Time
	TickIntervalMinutes   int
	ConfigVersion         int
	RiskPolicyJSON        map[string]any
	CadencePolicyJSON     map[string]any
	ContentMixPolicyJSON  map[string]any
	DefaultAspectRatio    string
	ExternalAutoPublish   bool
	MaxPostsPerDay        int
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type TopicLaneRow struct {
	ID                   string
	ChannelProfileID     string
	Name                 string
	Description          string
	KeywordsJSON         []string
	Enabled              bool
	PausedUntil          *time.Time
	Weight               float64
	MaxPostsPerDay        int
	CooldownAfterPostMin  int
	MaxConsecutiveStreak  int
	CreatedAt            time.Time
}

type LaneFormatRow struct {
	ID                       string
	TopicLaneID              string
	FormatKey                string
	Enabled                  bool
	Weight                   float64
	TargetDurationSec        int
	DefaultPublishVisibility string
	TemplatePoolJSON         []string
	SourcePlatformsJSON      []string
	CreatedAt                time.Time
}

type PublishingAccountRow struct {
	ID                  string
	ChannelProfileID    string
	Platform            string
	AccountLabel        string
	PlatformAccountID   string
	Enabled             bool
	PausedUntil         *time.Time
	DefaultPrivacy      string
	QuotaUnitsRemaining int
	CreatedAt           time.Time
}

type ManualSeedRow struct {
	ID                     string
	ChannelProfileID       string
	TopicLaneID            *string
	TargetAccountID        *string
	Prompt                 string
	TitleSeed              string
	SourcePolicy           string
	SourcePlatformsJSON    []string
	MaterialLibraryIDsJSON []string
	ConstraintsJSON        map[string]any
	Status                 string
	CreatedAt              time.Time
}

type ProductionTaskRow struct {
	ID                         string
	ChannelProfileID           string
	TopicLaneID                *string
	LaneFormatID               *string
	TargetAccountID            string
	ManualSeedID               *string
	Source                     string
	TitleSeed                  string
	Prompt                     string
	RationaleJSON              map[string]any
	ScoreBreakdownJSON         map[string]any
	SourcePlatformsJSON        []string
	MaterialLibraryIDsJSON     []string
	ApprovalMode               string
	AutoFlowPlanID             *string
	AutoFlowRunID              *string
	JobID                      *string
	State                      string
	BlockedByGuard             *string
	FailureReason              *string
	TransitionHistoryJSON      []map[string]any
	ChannelConfigVersion       int
	ChannelConfigSnapshotJSON  map[string]any
	StateUpdatedAt             time.Time
}

type QueueItemRow struct {
	ID                string
	Kind              string
	IdempotencyKey    string
	PayloadJSON       map[string]any
	Priority          int
	Attempts          int
	MaxAttempts       int
	RunAfter          time.Time
	ChannelProfileID  *string
	ParentQueueItemID *string
}
```

- [ ] **Step 4: Add store helpers**

Create `internal/channelops/store.go`:

```go
package channelops

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	Pool *pgxpool.Pool
	Now  func() time.Time
}

func OpenStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{Pool: pool, Now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *Store) Close() {
	if s != nil && s.Pool != nil {
		s.Pool.Close()
	}
}

func UTCBucket(now time.Time) string {
	return now.UTC().Format("2006-01-02-15")
}

func Transition(from string, to string, reason string, at time.Time) map[string]any {
	return map[string]any{
		"from":   from,
		"to":     to,
		"reason": reason,
		"at":     at.UTC().Format(time.RFC3339),
	}
}

func jsonObject(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	return value
}

func stringSlice(value []string) []string {
	if value == nil {
		return []string{}
	}
	return value
}
```

- [ ] **Step 5: Wire runner to store open/close**

Modify `internal/channelops/runner.go`:

```go
type Runner struct {
	Config Config
	Store  *Store
}

func NewRunner(ctx context.Context, cfg Config) (*Runner, error) {
	st, err := OpenStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	return &Runner{Config: cfg, Store: st}, nil
}

func (r *Runner) Close() {
	if r.Store != nil {
		r.Store.Close()
	}
}
```

Keep the temporary `Run` loop from Task 1.

- [ ] **Step 6: Run tests**

```bash
go test ./internal/channelops -run 'TestUTCBucket|TestTransitionPayload' -count=1
go build ./cmd/channelops-runner
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add internal/channelops/types.go internal/channelops/store.go internal/channelops/store_test.go internal/channelops/runner.go
git commit -m "feat: add channelops go store foundation"
```

---

## Task 3: Implement Go Queue Claim, Enqueue, Retry, Dead-Letter

**Files:**
- Create: `internal/channelops/queue.go`
- Test: `internal/channelops/queue_test.go`
- Modify: `internal/channelops/runner.go`

- [ ] **Step 1: Write unit tests for pure queue backoff**

Create `internal/channelops/queue_test.go`:

```go
package channelops

import (
	"testing"
	"time"
)

func TestRetryDelayUsesExponentialBackoff(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 5 * time.Minute},
		{attempt: 2, want: 10 * time.Minute},
		{attempt: 3, want: 20 * time.Minute},
		{attempt: 4, want: 30 * time.Minute},
		{attempt: 9, want: 30 * time.Minute},
	}
	for _, tc := range cases {
		if got := RetryDelay(tc.attempt); got != tc.want {
			t.Fatalf("RetryDelay(%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

func TestShouldDeadLetter(t *testing.T) {
	if ShouldDeadLetter(4, 5) {
		t.Fatal("attempt 4 of 5 should retry")
	}
	if !ShouldDeadLetter(5, 5) {
		t.Fatal("attempt 5 of 5 should dead-letter")
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/channelops -run 'TestRetryDelayUsesExponentialBackoff|TestShouldDeadLetter' -count=1
```

Expected: fail because helpers are undefined.

- [ ] **Step 3: Implement queue helpers and SQL methods**

Create `internal/channelops/queue.go`:

```go
package channelops

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

func RetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 5 * time.Minute
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= 30*time.Minute {
			return 30 * time.Minute
		}
	}
	return delay
}

func ShouldDeadLetter(nextAttempt int, maxAttempts int) bool {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	return nextAttempt >= maxAttempts
}

type EnqueueOptions struct {
	Kind              string
	IdempotencyKey    string
	Payload           map[string]any
	Priority          int
	RunAfter          time.Time
	ChannelProfileID  *string
	ParentQueueItemID *string
	MaxAttempts       int
}

func (s *Store) Enqueue(ctx context.Context, opts EnqueueOptions) (string, error) {
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = 5
	}
	if opts.RunAfter.IsZero() {
		opts.RunAfter = s.Now().UTC()
	}
	payload, err := json.Marshal(jsonObject(opts.Payload))
	if err != nil {
		return "", err
	}
	var id string
	err = s.Pool.QueryRow(ctx, `
		INSERT INTO channel_ops_queue_items
			(kind, idempotency_key, payload_json, priority, run_after, max_attempts, channel_profile_id, parent_queue_item_id)
		VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, $8)
		ON CONFLICT (idempotency_key) DO UPDATE
		SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id
	`, opts.Kind, opts.IdempotencyKey, payload, opts.Priority, opts.RunAfter, opts.MaxAttempts, opts.ChannelProfileID, opts.ParentQueueItemID).Scan(&id)
	return id, err
}

func (s *Store) ClaimNext(ctx context.Context, workerID string) (*QueueItemRow, error) {
	row := s.Pool.QueryRow(ctx, `
		WITH picked AS (
			SELECT id
			FROM channel_ops_queue_items
			WHERE status = 'pending'
			  AND dead_letter_at IS NULL
			  AND run_after <= NOW()
			ORDER BY priority DESC, run_after ASC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE channel_ops_queue_items q
		SET status = 'running',
		    locked_by = $1,
		    locked_at = NOW(),
		    attempts = attempts + 1
		FROM picked
		WHERE q.id = picked.id
		RETURNING q.id, q.kind, q.idempotency_key, q.payload_json, q.priority, q.attempts,
		          q.max_attempts, q.run_after, q.channel_profile_id, q.parent_queue_item_id
	`, workerID)
	var item QueueItemRow
	var payloadBytes []byte
	if err := row.Scan(&item.ID, &item.Kind, &item.IdempotencyKey, &payloadBytes, &item.Priority,
		&item.Attempts, &item.MaxAttempts, &item.RunAfter, &item.ChannelProfileID, &item.ParentQueueItemID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(payloadBytes, &item.PayloadJSON); err != nil {
		return nil, err
	}
	return &item, nil
}

func (s *Store) MarkQueueDone(ctx context.Context, id string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = 'succeeded', completed_at = NOW(), locked_by = NULL, locked_at = NULL
		WHERE id = $1
	`, id)
	return err
}

func (s *Store) MarkQueueFailedOrRetry(ctx context.Context, item QueueItemRow, message string) error {
	if ShouldDeadLetter(item.Attempts, item.MaxAttempts) {
		_, err := s.Pool.Exec(ctx, `
			UPDATE channel_ops_queue_items
			SET status = 'failed', error_message = $2, completed_at = NOW(), dead_letter_at = NOW(),
			    locked_by = NULL, locked_at = NULL
			WHERE id = $1
		`, item.ID, message)
		return err
	}
	_, err := s.Pool.Exec(ctx, `
		UPDATE channel_ops_queue_items
		SET status = 'pending', error_message = $2, run_after = $3, locked_by = NULL, locked_at = NULL
		WHERE id = $1
	`, item.ID, message, s.Now().UTC().Add(RetryDelay(item.Attempts)))
	return err
}
```

- [ ] **Step 4: Add queue fields to runner loop**

Modify `internal/channelops/runner.go` so the loop claims one item per tick and marks unknown kinds as retryable errors until handlers exist:

```go
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(r.Config.RunnerPollSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		item, err := r.Store.ClaimNext(ctx, "channelops-runner-go")
		if err != nil {
			return err
		}
		if item != nil {
			_ = r.Store.MarkQueueFailedOrRetry(ctx, *item, "handler not registered yet: "+item.Kind)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./internal/channelops -run 'TestRetryDelayUsesExponentialBackoff|TestShouldDeadLetter' -count=1
go build ./cmd/channelops-runner
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add internal/channelops/queue.go internal/channelops/queue_test.go internal/channelops/runner.go
git commit -m "feat: implement channelops go queue primitives"
```

---

## Task 4: Implement PDS Client, Fail Policy, Dev Allow-All

**Files:**
- Create: `internal/channelops/pds_client.go`
- Test: `internal/channelops/pds_client_test.go`

- [ ] **Step 1: Write PDS tests**

Create `internal/channelops/pds_client_test.go`:

```go
package channelops

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPDSFailPolicyByAction(t *testing.T) {
	client := PDSClient{Enabled: false}
	cases := map[string]string{
		"candidate_accept":    "allow",
		"plan_approval":       "flag",
		"publish":             "block",
		"promote_publication": "block",
	}
	for action, want := range cases {
		got, err := client.Decide(context.Background(), PDSDecisionRequest{ActionType: action})
		if err != nil {
			t.Fatalf("Decide returned error: %v", err)
		}
		if got.Verdict != want {
			t.Fatalf("action %s verdict = %s, want %s", action, got.Verdict, want)
		}
	}
}

func TestPDSDevAllowAll(t *testing.T) {
	client := PDSClient{Enabled: false, DevAllowAll: true}
	got, err := client.Decide(context.Background(), PDSDecisionRequest{ActionType: "publish"})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if got.Verdict != "allow" {
		t.Fatalf("verdict = %s", got.Verdict)
	}
	if got.Metadata["warning"] != "dev_allow_all" {
		t.Fatalf("metadata = %#v", got.Metadata)
	}
}

func TestPDSHTTPAllow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/decide" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"decision_id":"d1","verdict":"allow","score":0.9}`))
	}))
	defer server.Close()
	client := PDSClient{Enabled: true, BaseURL: server.URL, ClientID: "test", Timeout: time.Second}
	got, err := client.Decide(context.Background(), PDSDecisionRequest{ActionType: "plan_approval"})
	if err != nil {
		t.Fatalf("Decide returned error: %v", err)
	}
	if got.DecisionID != "d1" || got.Verdict != "allow" {
		t.Fatalf("decision = %#v", got)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/channelops -run 'TestPDS' -count=1
```

Expected: fail because PDS types do not exist.

- [ ] **Step 3: Implement PDS client**

Create `internal/channelops/pds_client.go`:

```go
package channelops

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type PDSDecisionRequest struct {
	ActorID    string         `json:"actor_id"`
	ActionType string         `json:"action_type"`
	Platform   string         `json:"platform,omitempty"`
	Content    map[string]any `json:"content,omitempty"`
	Context    map[string]any `json:"context,omitempty"`
}

type PDSDecision struct {
	DecisionID string         `json:"decision_id"`
	Verdict    string         `json:"verdict"`
	Score      float64        `json:"score"`
	Reasons    []map[string]any `json:"reasons,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

type PDSClient struct {
	Enabled     bool
	DevAllowAll bool
	BaseURL     string
	ClientID    string
	Timeout     time.Duration
	HTTPClient  *http.Client
}

func (c PDSClient) Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error) {
	if c.DevAllowAll {
		return PDSDecision{
			DecisionID: "",
			Verdict:    "allow",
			Metadata:   map[string]any{"warning": "dev_allow_all", "fail_policy": "allow"},
		}, nil
	}
	if !c.Enabled {
		return failPolicyDecision(req.ActionType, "pds_disabled"), nil
	}
	payload := map[string]any{
		"actor_id": req.ActorID,
		"action": map[string]any{
			"type":     req.ActionType,
			"platform": req.Platform,
		},
		"content": req.Content,
		"context": req.Context,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return PDSDecision{}, err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/v1/decide"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return PDSDecision{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Client-Id", c.ClientID)
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return failPolicyDecision(req.ActionType, "pds_unavailable"), nil
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return failPolicyDecision(req.ActionType, "pds_unavailable"), nil
	}
	var decision PDSDecision
	if err := json.NewDecoder(resp.Body).Decode(&decision); err != nil {
		return failPolicyDecision(req.ActionType, "pds_parse_failed"), nil
	}
	decision.Verdict = normalizeVerdict(decision.Verdict, failPolicy(req.ActionType))
	if decision.Metadata == nil {
		decision.Metadata = map[string]any{}
	}
	return decision, nil
}

func failPolicyDecision(action string, warning string) PDSDecision {
	verdict := failPolicy(action)
	return PDSDecision{
		Verdict:  verdict,
		Metadata: map[string]any{"warning": warning, "fail_policy": verdict},
	}
}

func failPolicy(action string) string {
	switch action {
	case "candidate_accept":
		return "allow"
	case "plan_approval":
		return "flag"
	case "publish", "promote_publication":
		return "block"
	default:
		return "allow"
	}
}

func normalizeVerdict(value string, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "allow", "flag", "block":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return fallback
	}
}
```

- [ ] **Step 4: Run PDS tests**

```bash
go test ./internal/channelops -run 'TestPDS' -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/channelops/pds_client.go internal/channelops/pds_client_test.go
git commit -m "feat: add channelops go pds client"
```

---

## Task 5: Implement Material Reference Extraction And Metrics Completeness

**Files:**
- Create: `internal/channelops/materials.go`
- Create: `internal/channelops/metrics.go`
- Test: `internal/channelops/materials_test.go`
- Test: `internal/channelops/metrics_test.go`

- [ ] **Step 1: Write material tests**

Create `internal/channelops/materials_test.go`:

```go
package channelops

import "testing"

func TestExtractMaterialReferencesPrefersMaterialID(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{
		"clips": []any{map[string]any{"material_id": "mat-1", "asset_id": "asset-1", "start_sec": 1.5, "end_sec": 4.0}},
	})
	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
	if refs[0].MaterialID != "mat-1" || refs[0].AssetID != "asset-1" {
		t.Fatalf("ref = %#v", refs[0])
	}
	if refs[0].StartMS == nil || *refs[0].StartMS != 1500 {
		t.Fatalf("StartMS = %#v", refs[0].StartMS)
	}
}

func TestExtractMaterialReferencesFallsBackToAssetID(t *testing.T) {
	refs := ExtractMaterialReferences(map[string]any{"asset_id": "asset-legacy"})
	if len(refs) != 1 {
		t.Fatalf("refs len = %d", len(refs))
	}
	if refs[0].MaterialID != "asset-legacy" {
		t.Fatalf("MaterialID = %s", refs[0].MaterialID)
	}
}
```

- [ ] **Step 2: Write metrics tests**

Create `internal/channelops/metrics_test.go`:

```go
package channelops

import (
	"math"
	"testing"
)

func TestMetricsCompletenessPartial(t *testing.T) {
	score, fields := MetricsCompleteness(map[string]any{"views": 100, "likes": 9})
	if math.Abs(score-0.25) > 0.0001 {
		t.Fatalf("score = %f", score)
	}
	if len(fields) != 2 || fields[0] != "views" || fields[1] != "likes" {
		t.Fatalf("fields = %#v", fields)
	}
}

func TestMetricsCompletenessRetentionAndImpressions(t *testing.T) {
	score, fields := MetricsCompleteness(map[string]any{
		"retention_curve": []any{0.9, 0.7},
		"impressions":    500,
	})
	if math.Abs(score-0.35) > 0.0001 {
		t.Fatalf("score = %f fields=%#v", score, fields)
	}
}
```

- [ ] **Step 3: Run tests and verify they fail**

```bash
go test ./internal/channelops -run 'TestExtractMaterialReferences|TestMetricsCompleteness' -count=1
```

Expected: fail because helpers are undefined.

- [ ] **Step 4: Implement material helpers**

Create `internal/channelops/materials.go`:

```go
package channelops

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

type MaterialReference struct {
	MaterialID       string
	AssetID          string
	StartMS          *int
	EndMS            *int
	SegmentSignature string
	Metadata         map[string]any
}

func ExtractMaterialReferences(payloads ...map[string]any) []MaterialReference {
	seen := map[string]struct{}{}
	refs := []MaterialReference{}
	for _, payload := range payloads {
		walkMaps(payload, func(item map[string]any) {
			ref, ok := referenceFromMap(item)
			if !ok {
				return
			}
			key := ref.MaterialID + ":" + ref.SegmentSignature
			if _, exists := seen[key]; exists {
				return
			}
			seen[key] = struct{}{}
			refs = append(refs, ref)
		})
	}
	return refs
}

func referenceFromMap(item map[string]any) (MaterialReference, bool) {
	materialID := stringValue(item["material_id"])
	if materialID == "" {
		materialID = stringValue(item["materialId"])
	}
	assetID := stringValue(item["asset_id"])
	if assetID == "" {
		assetID = stringValue(item["assetId"])
	}
	if materialID == "" {
		materialID = assetID
	}
	if materialID == "" {
		return MaterialReference{}, false
	}
	startMS := millisValue(item["start_ms"], item["start_sec"])
	endMS := millisValue(item["end_ms"], item["end_sec"])
	signature := stringValue(item["segment_signature"])
	if signature == "" {
		signature = SegmentSignature(materialID, startMS, endMS)
	}
	return MaterialReference{
		MaterialID:       materialID,
		AssetID:          assetID,
		StartMS:          startMS,
		EndMS:            endMS,
		SegmentSignature: signature,
		Metadata:         item,
	}, true
}

func SegmentSignature(materialID string, startMS *int, endMS *int) string {
	start := ""
	end := ""
	if startMS != nil {
		start = strconv.Itoa(*startMS)
	}
	if endMS != nil {
		end = strconv.Itoa(*endMS)
	}
	sum := sha256.Sum256([]byte(materialID + ":" + start + ":" + end))
	return hex.EncodeToString(sum[:])
}

func walkMaps(value any, visit func(map[string]any)) {
	switch typed := value.(type) {
	case map[string]any:
		visit(typed)
		for _, child := range typed {
			walkMaps(child, visit)
		}
	case []any:
		for _, child := range typed {
			walkMaps(child, visit)
		}
	}
}

func millisValue(msValue any, secValue any) *int {
	if msValue != nil {
		if parsed, ok := intValue(msValue); ok {
			return &parsed
		}
	}
	if secValue != nil {
		if parsed, ok := floatValue(secValue); ok {
			ms := int(parsed * 1000)
			return &ms
		}
	}
	return nil
}

func stringValue(value any) string {
	return strings.TrimSpace(fmt.Sprint(value))
}

func intValue(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return 0, false
	}
}

func floatValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
```

- [ ] **Step 5: Implement metrics helpers**

Create `internal/channelops/metrics.go`:

```go
package channelops

var metricWeights = []struct {
	Key    string
	Aliases []string
	Weight float64
}{
	{Key: "views", Aliases: []string{"views"}, Weight: 0.15},
	{Key: "likes", Aliases: []string{"likes"}, Weight: 0.10},
	{Key: "comments", Aliases: []string{"comments"}, Weight: 0.05},
	{Key: "shares", Aliases: []string{"shares"}, Weight: 0.05},
	{Key: "avg_view_duration_sec", Aliases: []string{"avg_view_duration_sec"}, Weight: 0.20},
	{Key: "retention_curve_json", Aliases: []string{"retention_curve_json", "retention_curve"}, Weight: 0.20},
	{Key: "ctr", Aliases: []string{"ctr"}, Weight: 0.10},
	{Key: "impressions", Aliases: []string{"impressions"}, Weight: 0.15},
}

func MetricsCompleteness(metrics map[string]any) (float64, []string) {
	if metrics == nil {
		return 0, []string{}
	}
	score := 0.0
	fields := []string{}
	for _, item := range metricWeights {
		if hasAnyMetric(metrics, item.Aliases) {
			score += item.Weight
			fields = append(fields, item.Key)
		}
	}
	return score, fields
}

func HasRecognizedMetrics(metrics map[string]any) bool {
	_, fields := MetricsCompleteness(metrics)
	return len(fields) > 0
}

func hasAnyMetric(metrics map[string]any, aliases []string) bool {
	for _, alias := range aliases {
		if _, ok := metrics[alias]; ok {
			return true
		}
	}
	return false
}
```

- [ ] **Step 6: Run tests**

```bash
go test ./internal/channelops -run 'TestExtractMaterialReferences|TestMetricsCompleteness' -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add internal/channelops/materials.go internal/channelops/materials_test.go internal/channelops/metrics.go internal/channelops/metrics_test.go
git commit -m "feat: add channelops material and metrics helpers"
```

---

## Task 6: Add Python AutoFlow Material ID And Feedback/Takedown Migration

**Files:**
- Modify: `backend/app/schemas/autoflow.py`
- Modify: `backend/app/autoflow/search_service.py`
- Modify: `backend/app/autoflow/service.py`
- Modify: `backend/app/autoflow/clip_ranker.py`
- Modify: `backend/app/channel_agent/material_usage.py`
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/019_channelops_go_live_phase0.py`
- Test: `backend/tests/autoflow/test_material_id_propagation.py`
- Test: `backend/tests/channel_agent/test_material_usage.py`
- Test: `backend/tests/channel_agent/test_models_queue.py`

- [ ] **Step 1: Write failing AutoFlow material ID tests**

Create `backend/tests/autoflow/test_material_id_propagation.py`:

```python
from app.autoflow.search_service import _candidate_from_material_result
from app.schemas.autoflow import AutoFlowClipCandidate


def test_autoflow_clip_candidate_accepts_material_id():
    candidate = AutoFlowClipCandidate(
        id="clip-1",
        title="Clip",
        source_type="material",
        material_id="mat-1",
    )
    assert candidate.material_id == "mat-1"


def test_material_search_candidate_sets_material_id_and_metadata():
    candidate = _candidate_from_material_result(
        {
            "id": "clip-1",
            "material_id": "mat-1",
            "asset_id": "asset-materialized",
            "source_asset_id": "asset-source",
            "title": "Clip",
        },
        1,
    )
    assert candidate.material_id == "mat-1"
    assert candidate.metadata["material_id"] == "mat-1"
    assert candidate.metadata["asset_id"] == "asset-materialized"
```

- [ ] **Step 2: Write failing fallback test**

Append to `backend/tests/channel_agent/test_material_usage.py`:

```python
def test_extract_material_references_falls_back_to_asset_id():
    refs = extract_material_references(
        plan_payload={"clips": [{"asset_id": "asset-legacy", "start_sec": 1, "end_sec": 2}]},
        run_payload={},
        upload_metadata={},
    )
    assert len(refs) == 1
    assert refs[0].material_id == "asset-legacy"
```

- [ ] **Step 3: Write failing model column test**

Append to `backend/tests/channel_agent/test_models_queue.py`:

```python
def test_feedback_snapshot_completeness_columns_exist():
    assert "metrics_completeness_score" in FeedbackSnapshot.__table__.columns
    assert "available_fields_json" in FeedbackSnapshot.__table__.columns


def test_takedown_event_has_dedup_lookup_index():
    index_names = {index.name for index in TakedownEvent.__table__.indexes}
    assert "ix_takedown_events_publication_event_detected" in index_names
```

- [ ] **Step 4: Run failing tests**

```bash
cd backend
python3 -m pytest tests/autoflow/test_material_id_propagation.py tests/channel_agent/test_material_usage.py::test_extract_material_references_falls_back_to_asset_id tests/channel_agent/test_models_queue.py::test_feedback_snapshot_completeness_columns_exist tests/channel_agent/test_models_queue.py::test_takedown_event_has_dedup_lookup_index -q
```

Expected: fail because fields and fallback do not exist.

- [ ] **Step 5: Implement Python schema/model changes**

Make these exact changes:

In `backend/app/schemas/autoflow.py`, add to `AutoFlowClipCandidate`:

```python
    material_id: str | None = None
```

In `backend/app/autoflow/search_service.py`, inside `_candidate_from_material_result`:

```python
    material_id = _string_or_none(result.get("material_id")) or materialized_asset_id
```

and pass:

```python
        material_id=material_id,
        metadata=_material_metadata(result, material_id, materialized_asset_id, source_asset_id),
```

Change `_material_metadata` signature:

```python
def _material_metadata(
    result: dict[str, Any],
    material_id: str | None,
    materialized_asset_id: str | None,
    source_asset_id: str | None,
) -> dict[str, Any]:
```

and add:

```python
    _put_if_present(metadata, "material_id", material_id)
```

In `backend/app/channel_agent/material_usage.py`, change `_reference_from_dict`:

```python
    material_id = str(item.get("material_id") or item.get("materialId") or "").strip()
    asset_id = item.get("asset_id") or item.get("assetId")
    if not material_id and asset_id:
        material_id = str(asset_id).strip()
    if not material_id:
        return None
```

In `backend/app/models/channel_agent.py`, update `TakedownEvent`:

```python
class TakedownEvent(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "takedown_events"
    __table_args__ = (
        Index("ix_takedown_events_publication_event_detected", "publication_id", "event_type", "detected_at"),
    )
```

and update `FeedbackSnapshot`:

```python
    metrics_completeness_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    available_fields_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
```

- [ ] **Step 6: Add Alembic migration**

Create `backend/alembic/versions/019_channelops_go_live_phase0.py`:

```python
"""channelops go live phase0

Revision ID: 019_channelops_go_live_phase0
Revises: 018_go_orchestrator_owner
Create Date: 2026-05-21 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "019_channelops_go_live_phase0"
down_revision = "018_go_orchestrator_owner"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "feedback_snapshots",
        sa.Column("metrics_completeness_score", sa.Float(), nullable=False, server_default="0"),
    )
    op.add_column(
        "feedback_snapshots",
        sa.Column("available_fields_json", sa.JSON(), nullable=False, server_default=sa.text("'[]'::json")),
    )
    op.create_index(
        "ix_takedown_events_publication_event_detected",
        "takedown_events",
        ["publication_id", "event_type", "detected_at"],
    )
    op.alter_column("feedback_snapshots", "metrics_completeness_score", server_default=None)
    op.alter_column("feedback_snapshots", "available_fields_json", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_takedown_events_publication_event_detected", table_name="takedown_events")
    op.drop_column("feedback_snapshots", "available_fields_json")
    op.drop_column("feedback_snapshots", "metrics_completeness_score")
```

- [ ] **Step 7: Ensure selected candidates carry material ID**

In `backend/app/autoflow/service.py`, any direct `AutoFlowClipCandidate(...)` constructed for material/storyboard matches must set:

```python
material_id=str(match.get("material_id") or match.get("asset_id") or "")
```

only when the source is material-like. In `clip_ranker.py`, do not strip `material_id`; if it copies or serializes candidates manually, include:

```python
"material_id": candidate.material_id
```

- [ ] **Step 8: Run target Python tests**

```bash
cd backend
python3 -m pytest tests/autoflow/test_material_id_propagation.py tests/channel_agent/test_material_usage.py tests/channel_agent/test_models_queue.py -q
```

Expected: pass.

- [ ] **Step 9: Commit**

```bash
git add backend/app/schemas/autoflow.py backend/app/autoflow/search_service.py backend/app/autoflow/service.py backend/app/autoflow/clip_ranker.py backend/app/channel_agent/material_usage.py backend/app/models/channel_agent.py backend/alembic/versions/019_channelops_go_live_phase0.py backend/tests/autoflow/test_material_id_propagation.py backend/tests/channel_agent/test_material_usage.py backend/tests/channel_agent/test_models_queue.py
git commit -m "feat: add channelops live material and metric schema"
```

---

## Task 7: Implement Scheduler

**Files:**
- Create: `internal/channelops/scheduler.go`
- Test: `internal/channelops/scheduler_test.go`
- Modify: `internal/channelops/runner.go`

- [ ] **Step 1: Write scheduler pure helper tests**

Create `internal/channelops/scheduler_test.go`:

```go
package channelops

import (
	"testing"
	"time"
)

func TestChannelDueForTick(t *testing.T) {
	now := time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC)
	channel := ChannelProfileRow{Enabled: true, TickIntervalMinutes: 60}
	if !ChannelDueForTick(channel, now) {
		t.Fatal("enabled hourly channel should be due at hour boundary")
	}
	halted := now.Add(-time.Hour)
	channel.HaltedAt = &halted
	if ChannelDueForTick(channel, now) {
		t.Fatal("halted channel should not be due")
	}
}

func TestTickIdempotencyKey(t *testing.T) {
	got := TickIdempotencyKey("channel-1", "2026-05-21-18")
	if got != "agent_tick:channel-1:2026-05-21-18" {
		t.Fatalf("key = %s", got)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/channelops -run 'TestChannelDueForTick|TestTickIdempotencyKey' -count=1
```

Expected: fail because scheduler helpers are undefined.

- [ ] **Step 3: Implement scheduler helpers and store methods**

Create `internal/channelops/scheduler.go`:

```go
package channelops

import (
	"context"
	"fmt"
	"time"
)

func ChannelDueForTick(channel ChannelProfileRow, now time.Time) bool {
	if !channel.Enabled || channel.HaltedAt != nil {
		return false
	}
	interval := channel.TickIntervalMinutes
	if interval <= 0 {
		interval = 60
	}
	minute := now.UTC().Minute()
	return minute%interval == 0
}

func TickIdempotencyKey(channelID string, bucket string) string {
	return fmt.Sprintf("agent_tick:%s:%s", channelID, bucket)
}

type Scheduler struct {
	Store *Store
}

func (s Scheduler) RunOnce(ctx context.Context, now time.Time) (int, error) {
	channels, err := s.Store.ListSchedulableChannels(ctx, now)
	if err != nil {
		return 0, err
	}
	enqueued := 0
	bucket := UTCBucket(now)
	for _, channel := range channels {
		if !ChannelDueForTick(channel, now) {
			continue
		}
		created, err := s.Store.InsertSchedulerRun(ctx, channel.ID, bucket)
		if err != nil {
			return enqueued, err
		}
		if !created {
			continue
		}
		channelID := channel.ID
		_, err = s.Store.Enqueue(ctx, EnqueueOptions{
			Kind:             QueueAgentTick,
			IdempotencyKey:   TickIdempotencyKey(channel.ID, bucket),
			Payload:          map[string]any{"channel_id": channel.ID, "bucket": bucket},
			Priority:         100,
			ChannelProfileID: &channelID,
		})
		if err != nil {
			return enqueued, err
		}
		enqueued++
	}
	return enqueued, nil
}

func (s *Store) ListSchedulableChannels(ctx context.Context, now time.Time) ([]ChannelProfileRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id, enabled, dry_run, halted_at, tick_interval_minutes, config_version,
		       risk_policy_json, cadence_policy_json, content_mix_policy_json,
		       default_aspect_ratio, external_asset_auto_publish, max_posts_per_day,
		       created_at, updated_at
		FROM channel_profiles
		WHERE enabled = TRUE AND halted_at IS NULL
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []ChannelProfileRow{}
	for rows.Next() {
		var row ChannelProfileRow
		if err := rows.Scan(&row.ID, &row.Enabled, &row.DryRun, &row.HaltedAt, &row.TickIntervalMinutes,
			&row.ConfigVersion, &row.RiskPolicyJSON, &row.CadencePolicyJSON, &row.ContentMixPolicyJSON,
			&row.DefaultAspectRatio, &row.ExternalAutoPublish, &row.MaxPostsPerDay, &row.CreatedAt, &row.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (s *Store) InsertSchedulerRun(ctx context.Context, channelID string, bucket string) (bool, error) {
	tag, err := s.Pool.Exec(ctx, `
		INSERT INTO internal_scheduler_runs (channel_profile_id, bucket, status, metadata_json)
		VALUES ($1, $2, 'succeeded', '{}'::jsonb)
		ON CONFLICT (channel_profile_id, bucket) DO NOTHING
	`, channelID, bucket)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
```

- [ ] **Step 4: Wire scheduler into runner**

Modify `internal/channelops/runner.go`:

```go
type Runner struct {
	Config    Config
	Store     *Store
	Scheduler Scheduler
}
```

and in `NewRunner`:

```go
runner := &Runner{Config: cfg, Store: st}
runner.Scheduler = Scheduler{Store: st}
return runner, nil
```

In `Run`, before claiming queue items, call:

```go
_, _ = r.Scheduler.RunOnce(ctx, r.Store.Now())
```

Do not return scheduler errors from the loop yet; log them in Task 13 when the logger is wired.

- [ ] **Step 5: Run tests**

```bash
go test ./internal/channelops -run 'TestChannelDueForTick|TestTickIdempotencyKey' -count=1
go build ./cmd/channelops-runner
```

Expected: pass.

- [ ] **Step 6: Commit**

```bash
git add internal/channelops/scheduler.go internal/channelops/scheduler_test.go internal/channelops/runner.go
git commit -m "feat: add channelops go scheduler"
```

---

## Task 8: Implement Tick Candidate Generation, Guards, Audit, Task Creation

**Files:**
- Create: `internal/channelops/tick.go`
- Test: `internal/channelops/tick_test.go`
- Modify: `internal/channelops/store.go`
- Modify: `internal/channelops/store.go`

- [ ] **Step 1: Write tick behavior tests**

Create `internal/channelops/tick_test.go`:

```go
package channelops

import "testing"

func TestBuildCandidatesManualThenLaneDriven(t *testing.T) {
	channel := ChannelProfileRow{ID: "ch", MaxPostsPerDay: 3}
	laneID := "lane-1"
	account := PublishingAccountRow{ID: "acct-1", Enabled: true}
	manual := ManualSeedRow{ID: "seed-1", TopicLaneID: &laneID, Prompt: "manual", TitleSeed: "manual title", SourcePolicy: "remix_with_review"}
	lane := TopicLaneRow{ID: laneID, Enabled: true, MaxPostsPerDay: 3}
	format := LaneFormatRow{ID: "fmt-1", TopicLaneID: laneID, Enabled: true, SourcePlatformsJSON: []string{"youtube"}}
	candidates := BuildTickCandidates(channel, []TopicLaneRow{lane}, []PublishingAccountRow{account}, []ManualSeedRow{manual}, map[string][]LaneFormatRow{laneID: []LaneFormatRow{format}}, "2026-05-21-18")
	if len(candidates) != 2 {
		t.Fatalf("candidate count = %d", len(candidates))
	}
	if candidates[0].Source != SourceManualSeed {
		t.Fatalf("first source = %s", candidates[0].Source)
	}
	if candidates[1].Source != SourceLaneSeed {
		t.Fatalf("second source = %s", candidates[1].Source)
	}
}

func TestTrendYouTubeIsNotManualOverride(t *testing.T) {
	seed := ManualSeedRow{ID: "seed-1", SourcePolicy: SourceTrendYT}
	candidate := CandidateFromManualSeed(seed, nil, nil, nil, "bucket")
	if candidate.SourceKind != SourceTrendYT {
		t.Fatalf("SourceKind = %s", candidate.SourceKind)
	}
	if candidate.ManualMaterialOverride {
		t.Fatal("trend_youtube should not get manual material override")
	}
}

func TestDryRunAuditDoesNotCreateTasks(t *testing.T) {
	result := TickResult{DryRun: true, Accepted: []TickCandidate{{CandidateID: "c1"}}}
	if result.TasksToCreate() != 0 {
		t.Fatalf("TasksToCreate = %d", result.TasksToCreate())
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/channelops -run 'TestBuildCandidates|TestTrendYouTube|TestDryRunAudit' -count=1
```

Expected: fail because tick types do not exist.

- [ ] **Step 3: Implement candidate types and pure generation**

Create `internal/channelops/tick.go`:

```go
package channelops

import "fmt"

type TickCandidate struct {
	CandidateID             string
	Source                  string
	SourceKind              string
	Seed                    *ManualSeedRow
	Lane                    *TopicLaneRow
	LaneFormat              *LaneFormatRow
	Account                 *PublishingAccountRow
	Prompt                  string
	TitleSeed               string
	SourcePlatformsJSON     []string
	MaterialLibraryIDsJSON  []string
	ConstraintsJSON         map[string]any
	ManualMaterialOverride  bool
	Rejected                bool
	RejectionGuard          string
	RejectionReason         string
}

type TickResult struct {
	DryRun   bool
	Accepted []TickCandidate
	Rejected []TickCandidate
}

func (r TickResult) TasksToCreate() int {
	if r.DryRun {
		return 0
	}
	return len(r.Accepted)
}

func BuildTickCandidates(
	channel ChannelProfileRow,
	lanes []TopicLaneRow,
	accounts []PublishingAccountRow,
	seeds []ManualSeedRow,
	laneFormats map[string][]LaneFormatRow,
	bucket string,
) []TickCandidate {
	candidates := []TickCandidate{}
	if len(accounts) == 0 || len(lanes) == 0 {
		return candidates
	}
	fallbackLane := lanes[0]
	account := accounts[0]
	selectedByLane := map[string]int{}
	for _, seed := range seeds {
		lane := fallbackLane
		if seed.TopicLaneID != nil {
			for _, item := range lanes {
				if item.ID == *seed.TopicLaneID {
					lane = item
					break
				}
			}
		}
		formats := laneFormats[lane.ID]
		var laneFormat *LaneFormatRow
		if len(formats) > 0 {
			copy := formats[0]
			laneFormat = &copy
		}
		candidate := CandidateFromManualSeed(seed, &lane, laneFormat, &account, bucket)
		candidates = append(candidates, candidate)
		selectedByLane[lane.ID]++
	}
	for _, lane := range lanes {
		budget := lane.MaxPostsPerDay
		if budget <= 0 {
			budget = 1
		}
		if selectedByLane[lane.ID] >= budget {
			continue
		}
		for _, format := range laneFormats[lane.ID] {
			candidates = append(candidates, TickCandidate{
				CandidateID:         fmt.Sprintf("lane:%s:format:%s:%s", lane.ID, format.ID, bucket),
				Source:              SourceLaneSeed,
				SourceKind:          SourceLaneSeed,
				Lane:                &lane,
				LaneFormat:          &format,
				Account:             &account,
				Prompt:              LanePrompt(lane, format),
				TitleSeed:           lane.Name,
				SourcePlatformsJSON: stringSlice(format.SourcePlatformsJSON),
				ConstraintsJSON:     map[string]any{},
			})
			break
		}
	}
	return candidates
}

func CandidateFromManualSeed(seed ManualSeedRow, lane *TopicLaneRow, laneFormat *LaneFormatRow, account *PublishingAccountRow, bucket string) TickCandidate {
	sourceKind := SourceManualSeed
	if seed.SourcePolicy == SourceTrendYT {
		sourceKind = SourceTrendYT
	}
	laneID := "unassigned"
	if lane != nil {
		laneID = lane.ID
	}
	formatID := "none"
	if laneFormat != nil {
		formatID = laneFormat.ID
	}
	sourcePlatforms := stringSlice(seed.SourcePlatformsJSON)
	if len(sourcePlatforms) == 0 && laneFormat != nil {
		sourcePlatforms = stringSlice(laneFormat.SourcePlatformsJSON)
	}
	return TickCandidate{
		CandidateID:             fmt.Sprintf("manual_seed:%s:%s:%s:%s", laneID, formatID, seed.ID, bucket),
		Source:                  SourceManualSeed,
		SourceKind:              sourceKind,
		Seed:                    &seed,
		Lane:                    lane,
		LaneFormat:              laneFormat,
		Account:                 account,
		Prompt:                  seed.Prompt,
		TitleSeed:               seed.TitleSeed,
		SourcePlatformsJSON:     sourcePlatforms,
		MaterialLibraryIDsJSON:  stringSlice(seed.MaterialLibraryIDsJSON),
		ConstraintsJSON:         jsonObject(seed.ConstraintsJSON),
		ManualMaterialOverride:  sourceKind == SourceManualSeed,
	}
}

func LanePrompt(lane TopicLaneRow, format LaneFormatRow) string {
	duration := format.TargetDurationSec
	if duration <= 0 {
		duration = 30
	}
	return fmt.Sprintf("Create a %s video for the %q topic. Theme: %s. Keywords: %v. Target duration: %ds.", format.FormatKey, lane.Name, lane.Description, lane.KeywordsJSON, duration)
}
```

- [ ] **Step 4: Add store methods needed by tick**

Append to `internal/channelops/store.go` with the real SQL implementation:

```go
func (s *Store) LoadTickInputs(ctx context.Context, channelID string, now time.Time) (ChannelProfileRow, []TopicLaneRow, []PublishingAccountRow, []ManualSeedRow, map[string][]LaneFormatRow, error) {
	channel, err := s.GetChannelProfile(ctx, channelID)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, err
	}
	lanes, err := s.ListActiveLanes(ctx, channelID, now)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, err
	}
	accounts, err := s.ListActiveAccounts(ctx, channelID, now)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, err
	}
	seeds, err := s.ListActiveManualSeeds(ctx, channelID)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, err
	}
	formats, err := s.ListLaneFormats(ctx, lanes)
	if err != nil {
		return ChannelProfileRow{}, nil, nil, nil, nil, err
	}
	return channel, lanes, accounts, seeds, formats, nil
}
```

Add these helper methods in the same step and back them with explicit SQL:

```go
func (s *Store) GetChannelProfile(ctx context.Context, channelID string) (ChannelProfileRow, error)
func (s *Store) ListActiveLanes(ctx context.Context, channelID string, now time.Time) ([]TopicLaneRow, error)
func (s *Store) ListActiveAccounts(ctx context.Context, channelID string, now time.Time) ([]PublishingAccountRow, error)
func (s *Store) ListActiveManualSeeds(ctx context.Context, channelID string) ([]ManualSeedRow, error)
func (s *Store) ListLaneFormats(ctx context.Context, lanes []TopicLaneRow) (map[string][]LaneFormatRow, error)
```

Implementation requirements for those methods:

- Query one `channel_profiles` row by id.
- Query enabled/unpaused `topic_lanes` ordered by weight desc then creation asc.
- Query enabled/unpaused `publishing_accounts` ordered by creation asc.
- Query active `manual_seeds` ordered by creation asc.
- Query enabled `lane_format_matrix` for loaded lane ids ordered by weight desc then creation asc.
- Use pgx JSON scanning into `[]byte`, then `json.Unmarshal` into maps/slices.
- Keep this SQL in `store.go` or split to `store_tick.go` when `store.go` exceeds 300 lines.

- [ ] **Step 5: Add task/audit writing store methods**

Add methods with these signatures:

```go
func (s *Store) InsertTickAudit(ctx context.Context, channelID string, bucket string, result TickResult, summary map[string]any) (string, error)
func (s *Store) InsertProductionTask(ctx context.Context, channel ChannelProfileRow, candidate TickCandidate, now time.Time) (string, error)
```

`InsertProductionTask` must set:

- `state = selected`
- `source = candidate.Source`
- `rationale_json.source_kind = candidate.SourceKind`
- `approval_mode = human` only for manual seed where `SourceKind == manual_seed`; otherwise `agent`
- `channel_config_version_snapshot = channel.ConfigVersion`
- `channel_config_snapshot_json` containing cadence and risk policy snapshots

- [ ] **Step 6: Run tests**

```bash
go test ./internal/channelops -run 'TestBuildCandidates|TestTrendYouTube|TestDryRunAudit' -count=1
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add internal/channelops/tick.go internal/channelops/tick_test.go internal/channelops/store.go
git commit -m "feat: add channelops go tick candidate engine"
```

---

## Task 9: Implement Handler Dispatch, Plan, Execute, Observe

**Files:**
- Create: `internal/channelops/autoflow_client.go`
- Create: `internal/channelops/handlers.go`
- Test: `internal/channelops/handlers_test.go`
- Modify: `internal/channelops/runner.go`

- [ ] **Step 1: Write handler tests for PDS flag/block**

Create `internal/channelops/handlers_test.go` with fake clients:

```go
package channelops

import (
	"context"
	"testing"
)

type fakePDS struct{ decision PDSDecision }

func (f fakePDS) Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error) {
	return f.decision, nil
}

func TestPlanDecisionFlagHoldsTask(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-flag", Verdict: "flag"})
	if result.NextState != TaskHeld {
		t.Fatalf("NextState = %s", result.NextState)
	}
	if result.BlockedByGuard != "pds_flagged_for_review" {
		t.Fatalf("BlockedByGuard = %s", result.BlockedByGuard)
	}
	if result.EnqueueExecute {
		t.Fatal("flagged plan must not enqueue execute")
	}
}

func TestPlanDecisionBlockHoldsTask(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-block", Verdict: "block"})
	if result.NextState != TaskHeld || result.BlockedByGuard != "pds_blocked" || result.EnqueueExecute {
		t.Fatalf("result = %#v", result)
	}
}

func TestPlanDecisionAllowEnqueuesExecute(t *testing.T) {
	result := PlanDecisionResult(PDSDecision{DecisionID: "d-allow", Verdict: "allow"})
	if result.NextState != TaskPlanning || !result.EnqueueExecute {
		t.Fatalf("result = %#v", result)
	}
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/channelops -run 'TestPlanDecision' -count=1
```

Expected: fail because helpers do not exist.

- [ ] **Step 3: Implement AutoFlow interface**

Create `internal/channelops/autoflow_client.go`:

```go
package channelops

import "context"

type AutoFlowClient interface {
	PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error)
	ApprovePlan(ctx context.Context, planID string, evidence map[string]any) error
	ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error)
	GetJob(ctx context.Context, jobID string) (AutoFlowJobObservation, error)
}

type AutoFlowPlanObservation struct {
	PlanID          string
	UploadNodeCount int
	PlanPayload     map[string]any
}

type AutoFlowExecuteObservation struct {
	RunID string
	JobID string
	Status string
	RunPayload map[string]any
}

type AutoFlowJobObservation struct {
	Status string
	RunPayload map[string]any
	UploadMetadata map[string]any
	ErrorMessage string
}
```

- [ ] **Step 4: Implement handler decision helper**

Create `internal/channelops/handlers.go`:

```go
package channelops

import (
	"context"
	"fmt"
	"time"
)

type PDSDecider interface {
	Decide(ctx context.Context, req PDSDecisionRequest) (PDSDecision, error)
}

type HandlerService struct {
	Store    *Store
	PDS      PDSDecider
	AutoFlow AutoFlowClient
	YouTube  YouTubeClient
	Config   Config
}

type PlanResult struct {
	NextState      string
	BlockedByGuard string
	EnqueueExecute bool
}

func PlanDecisionResult(decision PDSDecision) PlanResult {
	switch decision.Verdict {
	case "allow":
		return PlanResult{NextState: TaskPlanning, EnqueueExecute: true}
	case "block":
		return PlanResult{NextState: TaskHeld, BlockedByGuard: "pds_blocked"}
	default:
		return PlanResult{NextState: TaskHeld, BlockedByGuard: "pds_flagged_for_review"}
	}
}

func (h HandlerService) Handle(ctx context.Context, item QueueItemRow) error {
	switch item.Kind {
	case QueueAgentTick:
		return h.HandleAgentTick(ctx, item)
	case QueuePlanTask:
		return h.HandlePlanTask(ctx, item)
	case QueueExecuteTask:
		return h.HandleExecuteTask(ctx, item)
	case QueueObserveJob:
		return h.HandleObserveJob(ctx, item)
	case QueuePublishTask, QueuePromotePublication:
		return h.HandlePublishTask(ctx, item)
	case QueueReconcilePublication:
		return h.HandleReconcilePublication(ctx, item)
	case QueueCollectMetrics:
		return h.HandleCollectMetrics(ctx, item)
	case QueueAccountHealth:
		return h.HandleAccountHealth(ctx, item)
	default:
		return fmt.Errorf("unknown ChannelOps queue kind: %s", item.Kind)
	}
}

func (h HandlerService) HandleAgentTick(ctx context.Context, item QueueItemRow) error {
	channelID, _ := item.PayloadJSON["channel_id"].(string)
	bucket, _ := item.PayloadJSON["bucket"].(string)
	return h.Store.RunTick(ctx, channelID, bucket, h)
}

func (h HandlerService) HandlePlanTask(ctx context.Context, item QueueItemRow) error {
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	observation, err := h.AutoFlow.PlanTask(ctx, task, AutoFlowRequestForTask(task))
	if err != nil {
		return err
	}
	if observation.UploadNodeCount != 1 {
		return h.Store.HoldTask(ctx, task.ID, "missing_youtube_upload_node", "AutoFlow plan must contain exactly one youtube_upload node", "plan_task")
	}
	if task.ApprovalMode == ApprovalAgent {
		decision, err := h.PDS.Decide(ctx, PDSDecisionRequest{
			ActorID: task.TargetAccountID,
			ActionType: "plan_approval",
			Platform: "youtube",
			Content: map[string]any{"title": task.TitleSeed, "description": task.Prompt},
			Context: map[string]any{"production_task_id": task.ID, "autoflow_plan_id": observation.PlanID},
		})
		if err != nil {
			return err
		}
		result := PlanDecisionResult(decision)
		if !result.EnqueueExecute {
			return h.Store.HoldTaskWithPDS(ctx, task.ID, result.BlockedByGuard, decision, "plan_task_pds")
		}
		if err := h.AutoFlow.ApprovePlan(ctx, observation.PlanID, map[string]any{"decision_id": decision.DecisionID, "verdict": decision.Verdict}); err != nil {
			return err
		}
	}
	return h.Store.MarkTaskPlanningAndEnqueueExecute(ctx, task.ID, observation.PlanID, item.ID)
}

func (h HandlerService) HandleExecuteTask(ctx context.Context, item QueueItemRow) error {
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	observation, err := h.AutoFlow.ExecuteTask(ctx, task, AutoFlowRequestForTask(task))
	if err != nil {
		return err
	}
	return h.Store.MarkTaskProducingAndEnqueueObserve(ctx, task.ID, observation.RunID, observation.JobID, item.ID)
}

func (h HandlerService) HandleObserveJob(ctx context.Context, item QueueItemRow) error {
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task.JobID == nil {
		return fmt.Errorf("task %s has no AutoFlow job id", task.ID)
	}
	observation, err := h.AutoFlow.GetJob(ctx, *task.JobID)
	if err != nil {
		return err
	}
	switch observation.Status {
	case "running", "queued", "pending":
		return h.Store.ReenqueueObserve(ctx, task.ID, item.ID, time.Minute)
	case "succeeded":
		return h.Store.MarkTaskReadyToPublish(ctx, task, observation, item.ID)
	default:
		return h.Store.FailTask(ctx, task.ID, observation.ErrorMessage, "observe_job")
	}
}
```

- [ ] **Step 5: Add store method stubs and then real SQL**

Add these signatures, then implement them with SQL in `internal/channelops/store.go` or `store_tasks.go`:

```go
func (s *Store) RunTick(ctx context.Context, channelID string, bucket string, h HandlerService) error
func (s *Store) GetProductionTask(ctx context.Context, taskID string) (ProductionTaskRow, error)
func (s *Store) HoldTask(ctx context.Context, taskID string, guard string, reason string, transitionReason string) error
func (s *Store) HoldTaskWithPDS(ctx context.Context, taskID string, guard string, decision PDSDecision, transitionReason string) error
func (s *Store) MarkTaskPlanningAndEnqueueExecute(ctx context.Context, taskID string, planID string, parentQueueItemID string) error
func (s *Store) MarkTaskProducingAndEnqueueObserve(ctx context.Context, taskID string, runID string, jobID string, parentQueueItemID string) error
func (s *Store) ReenqueueObserve(ctx context.Context, taskID string, parentQueueItemID string, delay time.Duration) error
func (s *Store) MarkTaskReadyToPublish(ctx context.Context, task ProductionTaskRow, observation AutoFlowJobObservation, parentQueueItemID string) error
func (s *Store) FailTask(ctx context.Context, taskID string, reason string, transitionReason string) error
```

`MarkTaskReadyToPublish` should enqueue `publish_task:<task_id>` and store plan/run payloads needed for material ledger in task rationale if there is no dedicated column.

- [ ] **Step 6: Wire handler into runner**

Modify `Runner`:

```go
type Runner struct {
	Config    Config
	Store     *Store
	Scheduler Scheduler
	Handlers  HandlerService
}
```

After clients are created in later tasks, `Handlers` must be populated. For this task, create a compile-safe constructor path using fake nil checks and keep `Run` returning an error if handlers are missing.

- [ ] **Step 7: Run tests**

```bash
go test ./internal/channelops -run 'TestPlanDecision' -count=1
go build ./cmd/channelops-runner
```

Expected: pass.

- [ ] **Step 8: Commit**

```bash
git add internal/channelops/autoflow_client.go internal/channelops/handlers.go internal/channelops/handlers_test.go internal/channelops/store.go internal/channelops/runner.go
git commit -m "feat: add channelops go plan execute observe handlers"
```

---

## Task 10: Implement YouTube Client, Publish, Reconcile, Metrics Handlers

**Files:**
- Create: `internal/channelops/youtube_client.go`
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/store.go`
- Test: `internal/channelops/handlers_test.go`

- [ ] **Step 1: Add reconcile and metrics tests**

Append to `internal/channelops/handlers_test.go`:

```go
func TestTakedownDedupKeyUsesPublicationEventDay(t *testing.T) {
	key := TakedownDedupKey("pub-1", "rejected", mustTime("2026-05-21T17:15:00Z"))
	if key != "pub-1:rejected:2026-05-21" {
		t.Fatalf("key = %s", key)
	}
}

func mustTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return parsed
}
```

- [ ] **Step 2: Run tests and verify they fail**

```bash
go test ./internal/channelops -run 'TestTakedownDedupKey' -count=1
```

Expected: fail because helper is undefined.

- [ ] **Step 3: Add YouTube client interface**

Create `internal/channelops/youtube_client.go`:

```go
package channelops

import "context"

type YouTubeClient interface {
	AccountHealth(ctx context.Context, accountID string) (YouTubeAccountHealth, error)
	PublicationStatus(ctx context.Context, videoID string) (YouTubePublicationStatus, error)
	FetchMetrics(ctx context.Context, videoID string) (map[string]any, error)
}

type YouTubeAccountHealth struct {
	Authenticated bool
	QuotaRemaining int
	Raw map[string]any
}

type YouTubePublicationStatus struct {
	VideoID string
	PublishStatus string
	Privacy string
	Permalink string
	Raw map[string]any
}
```

- [ ] **Step 4: Implement helper and handler methods**

Append to `internal/channelops/handlers.go`:

```go
func TakedownDedupKey(publicationID string, eventType string, at time.Time) string {
	return fmt.Sprintf("%s:%s:%s", publicationID, eventType, at.UTC().Format("2006-01-02"))
}

func (h HandlerService) HandlePublishTask(ctx context.Context, item QueueItemRow) error {
	taskID, _ := item.PayloadJSON["production_task_id"].(string)
	task, err := h.Store.GetProductionTask(ctx, taskID)
	if err != nil {
		return err
	}
	decision, err := h.PDS.Decide(ctx, PDSDecisionRequest{
		ActorID: task.TargetAccountID,
		ActionType: "publish",
		Platform: "youtube",
		Content: map[string]any{"title": task.TitleSeed, "description": task.Prompt},
		Context: map[string]any{"production_task_id": task.ID},
	})
	if err != nil {
		return err
	}
	if decision.Verdict != "allow" {
		guard := "pds_blocked"
		if decision.Verdict == "flag" {
			guard = "pds_flagged_for_review"
		}
		return h.Store.HoldTaskWithPDS(ctx, task.ID, guard, decision, "publish_task_pds")
	}
	return h.Store.CreateOrUpdatePublicationFromTask(ctx, task, item.ID)
}

func (h HandlerService) HandleReconcilePublication(ctx context.Context, item QueueItemRow) error {
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	publication, err := h.Store.GetPublication(ctx, publicationID)
	if err != nil {
		return err
	}
	status, err := h.YouTube.PublicationStatus(ctx, publication.PlatformContentID)
	if err != nil {
		return err
	}
	if isSeverePublicationStatus(status.PublishStatus) {
		return h.Store.MarkPublicationSevereDedup(ctx, publication, status, h.Store.Now())
	}
	return h.Store.UpdatePublicationStatus(ctx, publication.ID, status)
}

func (h HandlerService) HandleCollectMetrics(ctx context.Context, item QueueItemRow) error {
	publicationID, _ := item.PayloadJSON["publication_id"].(string)
	publication, err := h.Store.GetPublication(ctx, publicationID)
	if err != nil {
		return err
	}
	metrics := mapFromAny(item.PayloadJSON["metrics"])
	if !HasRecognizedMetrics(metrics) && publication.PlatformContentID != "" {
		fetched, err := h.YouTube.FetchMetrics(ctx, publication.PlatformContentID)
		if err == nil && HasRecognizedMetrics(fetched) {
			metrics = fetched
		}
	}
	if !HasRecognizedMetrics(metrics) {
		return h.Store.RequeueOrHoldMetrics(ctx, publication, item, h.Config.MetricsPollMaxAttempts)
	}
	score, fields := MetricsCompleteness(metrics)
	return h.Store.UpsertFeedbackSnapshot(ctx, publication, metrics, score, fields)
}

func (h HandlerService) HandleAccountHealth(ctx context.Context, item QueueItemRow) error {
	accountID, _ := item.PayloadJSON["account_id"].(string)
	health, err := h.YouTube.AccountHealth(ctx, accountID)
	if err != nil {
		return err
	}
	return h.Store.UpdateAccountHealth(ctx, accountID, health)
}

func isSeverePublicationStatus(status string) bool {
	switch status {
	case "rejected", "removed", "claim", "claimed", "takedown":
		return true
	default:
		return false
	}
}

func mapFromAny(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok {
		return typed
	}
	return map[string]any{}
}
```

- [ ] **Step 5: Implement store methods**

Add these SQL-backed methods:

```go
func (s *Store) CreateOrUpdatePublicationFromTask(ctx context.Context, task ProductionTaskRow, parentQueueItemID string) error
func (s *Store) GetPublication(ctx context.Context, publicationID string) (PublicationRow, error)
func (s *Store) UpdatePublicationStatus(ctx context.Context, publicationID string, status YouTubePublicationStatus) error
func (s *Store) MarkPublicationSevereDedup(ctx context.Context, publication PublicationRow, status YouTubePublicationStatus, now time.Time) error
func (s *Store) RequeueOrHoldMetrics(ctx context.Context, publication PublicationRow, item QueueItemRow, maxPolls int) error
func (s *Store) UpsertFeedbackSnapshot(ctx context.Context, publication PublicationRow, metrics map[string]any, score float64, fields []string) error
func (s *Store) UpdateAccountHealth(ctx context.Context, accountID string, health YouTubeAccountHealth) error
```

`MarkPublicationSevereDedup` must:

1. Compute UTC day start and next day.
2. Query existing `takedown_events` with same publication/event inside day.
3. Insert new row if absent.
4. Append repeat object to `auto_actions_taken_json` if present.
5. Set task state `held`.

`UpsertFeedbackSnapshot` must update the latest snapshot for the publication if one exists; otherwise insert one.

- [ ] **Step 6: Run tests**

```bash
go test ./internal/channelops -run 'TestTakedownDedupKey|TestMetricsCompleteness' -count=1
go build ./cmd/channelops-runner
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add internal/channelops/youtube_client.go internal/channelops/handlers.go internal/channelops/handlers_test.go internal/channelops/store.go
git commit -m "feat: add channelops go publication and metrics handlers"
```

---

## Task 11: Add Fake End-To-End Integration Test

**Files:**
- Create: `internal/channelops/integration_test.go`
- Modify: `internal/channelops/store.go`
- Modify: `internal/channelops/handlers.go`

- [ ] **Step 1: Write integration test outline**

Create `internal/channelops/integration_test.go`:

```go
package channelops

import (
	"context"
	"testing"
	"time"
)

func TestFakeLiveFlowReachesMeasured(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test skipped in short mode")
	}
	ctx := context.Background()
	fixture := NewChannelOpsFixture(t)
	defer fixture.Close()

	fixture.InsertChannelWithLaneAccountSeed()
	handler := fixture.HandlerService(PDSDecision{Verdict: "allow", DecisionID: "allow"})
	bucket := UTCBucket(time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC))

	if err := fixture.Store.RunTick(ctx, fixture.ChannelID, bucket, handler); err != nil {
		t.Fatalf("RunTick: %v", err)
	}
	fixture.ProcessAllQueueItems(ctx, handler)

	task := fixture.RequireSingleTask()
	if task.State != TaskMeasured {
		t.Fatalf("task state = %s", task.State)
	}
	if fixture.CountRows("publication_records") != 1 {
		t.Fatalf("publication count mismatch")
	}
	if fixture.CountRows("feedback_snapshots") != 1 {
		t.Fatalf("feedback snapshot count mismatch")
	}
	if fixture.CountRows("material_usage_ledger") == 0 {
		t.Fatalf("material ledger did not grow")
	}
	if fixture.CountRows("takedown_events") != 0 {
		t.Fatalf("unexpected takedown event")
	}
}
```

- [ ] **Step 2: Add fixture helpers**

In the same file, add fixture helpers that create an isolated test database. Use the repo's existing Go DB test pattern if one exists; if not, require `DATABASE_URL` and create data in a transaction with cleanup:

```go
type ChannelOpsFixture struct {
	T *testing.T
	Store *Store
	ChannelID string
}

func NewChannelOpsFixture(t *testing.T) *ChannelOpsFixture {
	t.Helper()
	cfg := LoadConfig()
	if cfg.DatabaseURL == "" {
		t.Fatal("DATABASE_URL is required for ChannelOps integration test")
	}
	store, err := OpenStore(context.Background(), cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	store.Now = func() time.Time { return time.Date(2026, 5, 21, 18, 0, 0, 0, time.UTC) }
	return &ChannelOpsFixture{T: t, Store: store}
}

func (f *ChannelOpsFixture) Close() {
	f.Store.Close()
}
```

Then implement:

- `InsertChannelWithLaneAccountSeed`
- `HandlerService`
- `ProcessAllQueueItems`
- `RequireSingleTask`
- `CountRows`

Each helper should use explicit SQL inserts into ChannelOps tables and delete rows by `channel_profile_id` during cleanup.

- [ ] **Step 3: Add fake clients**

In `integration_test.go`, define:

```go
type fakeAutoFlow struct{}

func (fakeAutoFlow) PlanTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowPlanObservation, error) {
	return AutoFlowPlanObservation{
		PlanID: "00000000-0000-0000-0000-000000000101",
		UploadNodeCount: 1,
		PlanPayload: map[string]any{"clips": []any{map[string]any{"material_id": "mat-1", "asset_id": "asset-1"}}},
	}, nil
}

func (fakeAutoFlow) ApprovePlan(ctx context.Context, planID string, evidence map[string]any) error { return nil }

func (fakeAutoFlow) ExecuteTask(ctx context.Context, task ProductionTaskRow, request map[string]any) (AutoFlowExecuteObservation, error) {
	return AutoFlowExecuteObservation{RunID: "00000000-0000-0000-0000-000000000201", JobID: "job-1", Status: "running"}, nil
}

func (fakeAutoFlow) GetJob(ctx context.Context, jobID string) (AutoFlowJobObservation, error) {
	return AutoFlowJobObservation{
		Status: "succeeded",
		RunPayload: map[string]any{"clips": []any{map[string]any{"material_id": "mat-1", "asset_id": "asset-1"}}},
		UploadMetadata: map[string]any{"video_id": "yt-1", "material_id": "mat-1", "asset_id": "asset-1"},
	}, nil
}

type fakeYouTube struct{}

func (fakeYouTube) AccountHealth(ctx context.Context, accountID string) (YouTubeAccountHealth, error) {
	return YouTubeAccountHealth{Authenticated: true, QuotaRemaining: 1000, Raw: map[string]any{"ok": true}}, nil
}

func (fakeYouTube) PublicationStatus(ctx context.Context, videoID string) (YouTubePublicationStatus, error) {
	return YouTubePublicationStatus{VideoID: videoID, PublishStatus: "scheduled", Privacy: "unlisted", Permalink: "https://youtu.be/" + videoID, Raw: map[string]any{"status": "scheduled"}}, nil
}

func (fakeYouTube) FetchMetrics(ctx context.Context, videoID string) (map[string]any, error) {
	return map[string]any{"views": 10, "likes": 2, "impressions": 100}, nil
}
```

- [ ] **Step 4: Run integration test**

```bash
go test ./internal/channelops -run TestFakeLiveFlowReachesMeasured -count=1
```

Expected: pass against local dev DB. If local DB is unavailable, start the repo's compose database and rerun.

- [ ] **Step 5: Commit**

```bash
git add internal/channelops/integration_test.go internal/channelops/store.go internal/channelops/handlers.go
git commit -m "test: cover channelops go fake live flow"
```

---

## Task 12: Add Live Smoke CLI

**Files:**
- Create: `internal/channelops/live_smoke.go`
- Create: `cmd/channelops-live-smoke/main.go`
- Test: `internal/channelops/live_smoke_test.go`

- [ ] **Step 1: Write smoke decision tests**

Create `internal/channelops/live_smoke_test.go`:

```go
package channelops

import "testing"

func TestSmokeResultRequiresLedgerAndNoTakedown(t *testing.T) {
	result := SmokeResult{TaskScheduled: true, PublicationUnlisted: true, MetricsWritten: true, LedgerRows: 1, TakedownRows: 0}
	if err := result.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	result.LedgerRows = 0
	if err := result.Validate(); err == nil {
		t.Fatal("expected missing ledger rows to fail validation")
	}
}
```

- [ ] **Step 2: Implement smoke result and runner shell**

Create `internal/channelops/live_smoke.go`:

```go
package channelops

import (
	"context"
	"errors"
)

type SmokeResult struct {
	TaskScheduled       bool
	PublicationUnlisted bool
	MetricsWritten      bool
	LedgerRows          int
	TakedownRows        int
}

func (r SmokeResult) Validate() error {
	if !r.TaskScheduled {
		return errors.New("no task reached scheduled")
	}
	if !r.PublicationUnlisted {
		return errors.New("publication was not confirmed unlisted")
	}
	if !r.MetricsWritten {
		return errors.New("metrics snapshot was not written")
	}
	if r.LedgerRows <= 0 {
		return errors.New("material_usage_ledger did not grow")
	}
	if r.TakedownRows != 0 {
		return errors.New("takedown_events is non-zero")
	}
	return nil
}

type LiveSmoke struct {
	Store *Store
	Handler HandlerService
}

func (s LiveSmoke) Run(ctx context.Context, channelID string) (SmokeResult, error) {
	return s.Store.RunLiveSmoke(ctx, channelID, s.Handler)
}
```

Create `cmd/channelops-live-smoke/main.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/Ctwqk/videoprocess/internal/channelops"
)

func main() {
	channelID := flag.String("channel-id", "", "ChannelProfile id to smoke")
	flag.Parse()
	if *channelID == "" {
		fmt.Fprintln(os.Stderr, "-channel-id is required")
		os.Exit(2)
	}
	cfg := channelops.LoadConfig()
	if err := cfg.Validate(); err != nil {
		slog.Error("invalid smoke config", "error", err)
		os.Exit(1)
	}
	runner, err := channelops.NewRunner(context.Background(), cfg)
	if err != nil {
		slog.Error("create runner", "error", err)
		os.Exit(1)
	}
	defer runner.Close()
	result, err := channelops.LiveSmoke{Store: runner.Store, Handler: runner.Handlers}.Run(context.Background(), *channelID)
	if err != nil {
		slog.Error("smoke failed", "error", err)
		os.Exit(1)
	}
	if err := result.Validate(); err != nil {
		slog.Error("smoke validation failed", "error", err)
		os.Exit(1)
	}
	fmt.Printf("channelops live smoke passed: %+v\n", result)
}
```

- [ ] **Step 3: Implement store smoke orchestration**

Add to `store.go`:

```go
func (s *Store) RunLiveSmoke(ctx context.Context, channelID string, handler HandlerService) (SmokeResult, error) {
	bucket := UTCBucket(s.Now())
	if err := s.RunTick(ctx, channelID, bucket, handler); err != nil {
		return SmokeResult{}, err
	}
	for i := 0; i < 100; i++ {
		item, err := s.ClaimNext(ctx, "channelops-live-smoke")
		if err != nil {
			return SmokeResult{}, err
		}
		if item == nil {
			break
		}
		if err := handler.Handle(ctx, *item); err != nil {
			_ = s.MarkQueueFailedOrRetry(ctx, *item, err.Error())
			return SmokeResult{}, err
		}
		if err := s.MarkQueueDone(ctx, item.ID); err != nil {
			return SmokeResult{}, err
		}
	}
	return s.SmokeResultForChannel(ctx, channelID)
}

func (s *Store) SmokeResultForChannel(ctx context.Context, channelID string) (SmokeResult, error) {
	var result SmokeResult
	err := s.Pool.QueryRow(ctx, `
		SELECT
			EXISTS(SELECT 1 FROM production_tasks WHERE channel_profile_id = $1 AND state IN ('scheduled', 'measured')),
			EXISTS(SELECT 1 FROM publication_records p JOIN production_tasks t ON t.id = p.production_task_id WHERE t.channel_profile_id = $1 AND p.current_privacy = 'unlisted'),
			EXISTS(SELECT 1 FROM feedback_snapshots f JOIN publication_records p ON p.id = f.publication_id JOIN production_tasks t ON t.id = p.production_task_id WHERE t.channel_profile_id = $1),
			(SELECT COUNT(*) FROM material_usage_ledger WHERE channel_profile_id = $1),
			(SELECT COUNT(*) FROM takedown_events e JOIN publication_records p ON p.id = e.publication_id JOIN production_tasks t ON t.id = p.production_task_id WHERE t.channel_profile_id = $1)
	`, channelID).Scan(&result.TaskScheduled, &result.PublicationUnlisted, &result.MetricsWritten, &result.LedgerRows, &result.TakedownRows)
	return result, err
}
```

- [ ] **Step 4: Run tests and build smoke CLI**

```bash
go test ./internal/channelops -run TestSmokeResultRequiresLedgerAndNoTakedown -count=1
go build ./cmd/channelops-live-smoke
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/channelops/live_smoke.go internal/channelops/live_smoke_test.go cmd/channelops-live-smoke/main.go internal/channelops/store.go
git commit -m "feat: add channelops live smoke cli"
```

---

## Task 13: Add Docker, Compose, And Runner Wiring

**Files:**
- Create: `backend/Dockerfile.channelops-runner-go`
- Modify: `docker-compose.yml`
- Modify: `docker-compose.gpu.yml` if it defines ChannelOps runtime services.
- Modify: `internal/channelops/runner.go`
- Modify: `cmd/channelops-runner/main.go`
- Create: `docs/channelops-go-live-runner.md`
- Test: `backend/tests/test_go_dockerfiles.py`

- [ ] **Step 1: Add Dockerfile test**

Modify `backend/tests/test_go_dockerfiles.py` so it includes:

```python
        ROOT / "backend" / "Dockerfile.channelops-runner-go",
```

in the dockerfile list.

- [ ] **Step 2: Run test and verify it fails**

```bash
python3 -m pytest backend/tests/test_go_dockerfiles.py -q
```

Expected: fail because `Dockerfile.channelops-runner-go` does not exist.

- [ ] **Step 3: Create Dockerfile**

Create `backend/Dockerfile.channelops-runner-go`:

```dockerfile
FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY cmd ./cmd
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/channelops-runner ./cmd/channelops-runner
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/channelops-live-smoke ./cmd/channelops-live-smoke

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/channelops-runner /usr/local/bin/channelops-runner
COPY --from=build /out/channelops-live-smoke /usr/local/bin/channelops-live-smoke
ENTRYPOINT ["channelops-runner"]
```

- [ ] **Step 4: Add compose service**

In `docker-compose.yml`, add service:

```yaml
  channelops-runner-go:
    build:
      context: .
      dockerfile: backend/Dockerfile.channelops-runner-go
    container_name: vp_channelops_runner_go
    environment:
      DATABASE_URL: postgresql://vp:vp_secret@postgres:5432/videoprocess
      YOUTUBE_MANAGER_URL: http://youtube-manager:8899
      PDS_ENABLED: ${PDS_ENABLED:-false}
      PDS_BASE_URL: ${PDS_BASE_URL:-http://pds:8080}
      PDS_CLIENT_ID: videoprocess-channel-agent
      PDS_TIMEOUT_SECONDS: ${PDS_TIMEOUT_SECONDS:-0.5}
      CHANNEL_AGENT_DEV_ALLOW_ALL_PDS: ${CHANNEL_AGENT_DEV_ALLOW_ALL_PDS:-false}
      CHANNELOPS_RUNNER_POLL_SECONDS: ${CHANNELOPS_RUNNER_POLL_SECONDS:-5}
      CHANNELOPS_SCHEDULER_POLL_SECONDS: ${CHANNELOPS_SCHEDULER_POLL_SECONDS:-60}
      CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL: ${CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL:-}
      CHANNEL_AGENT_ALERT_EMAIL_TO: ${CHANNEL_AGENT_ALERT_EMAIL_TO:-}
    depends_on:
      postgres:
        condition: service_healthy
      youtube-manager:
        condition: service_started
    profiles: ["channelops-go"]
    restart: unless-stopped
```

Ensure the existing Python `channel-agent-runner` service is not in the same `channelops-go` profile. If it has no profile and starts by default, move it to `profiles: ["channelops-python"]`.

- [ ] **Step 5: Add docs**

Create `docs/channelops-go-live-runner.md`:

```markdown
# ChannelOps Go Live Runner

Live ChannelOps mode uses `channelops-runner-go`.

Do not run `channelops-runner-go` and the legacy Python `channel-agent-runner` at the same time. Both consume `channel_ops_queue_items`.

Development smoke with no PDS:

```bash
CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true docker compose --profile channelops-go up -d --build channelops-runner-go
```

Production-like mode:

```bash
PDS_ENABLED=true CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=false docker compose --profile channelops-go up -d --build channelops-runner-go
```

Live smoke:

```bash
docker compose exec channelops-runner-go channelops-live-smoke -channel-id <channel_profile_id>
```
```

- [ ] **Step 6: Build images and run dockerfile test**

```bash
python3 -m pytest backend/tests/test_go_dockerfiles.py -q
docker compose --profile channelops-go build channelops-runner-go
```

Expected: pytest passes; Docker image builds.

- [ ] **Step 7: Commit**

```bash
git add backend/Dockerfile.channelops-runner-go docker-compose.yml docker-compose.gpu.yml docs/channelops-go-live-runner.md backend/tests/test_go_dockerfiles.py
git commit -m "build: add channelops go runner container"
```

---

## Task 14: Full Verification And Cutover Safety

**Files:**
- Modify only if verification finds a concrete bug in files touched above.

- [ ] **Step 1: Run Go tests**

```bash
go test ./internal/channelops ./internal/config ./internal/store ./internal/orchestrator ./internal/worker/...
```

Expected: pass.

- [ ] **Step 2: Run Python target tests**

```bash
cd backend
python3 -m pytest tests/autoflow/test_material_id_propagation.py tests/channel_agent/test_material_usage.py tests/channel_agent/test_models_queue.py -q
```

Expected: pass.

- [ ] **Step 3: Run backend full test suite**

```bash
cd backend
python3 -m pytest
```

Expected: pass. If failures are unrelated to ChannelOps Go live work, document exact failing tests and do not hide them.

- [ ] **Step 4: Run lint/type checks if tools are installed**

```bash
cd backend
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: either pass or print missing-module output. Record the output in the final handoff.

- [ ] **Step 5: Build Go binaries**

```bash
go build ./cmd/channelops-runner
go build ./cmd/channelops-live-smoke
go build ./cmd/vp-api
go build ./cmd/vp-ffmpeg-worker
```

Expected: pass.

- [ ] **Step 6: Build compose service**

```bash
docker compose --profile channelops-go build channelops-runner-go
```

Expected: pass.

- [ ] **Step 7: Run fake integration**

```bash
go test ./internal/channelops -run TestFakeLiveFlowReachesMeasured -count=1
```

Expected: pass and verify task measured, publication exists, feedback snapshot exists, material ledger grows, takedown count is zero.

- [ ] **Step 8: Optional live smoke**

Only run this when a real test channel and YouTubeManager credentials are present:

```bash
CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true docker compose --profile channelops-go up -d --build channelops-runner-go
docker compose exec channelops-runner-go channelops-live-smoke -channel-id <channel_profile_id>
```

Expected: CLI prints `channelops live smoke passed`.

- [ ] **Step 9: Confirm single-runner deployment**

Run:

```bash
docker compose ps channelops-runner-go channel-agent-runner
```

Expected: `channelops-runner-go` is up for Go live mode; Python `channel-agent-runner` is absent or not running in the same profile.

- [ ] **Step 10: Commit verification fixes if any**

If verification required code fixes:

```bash
git add <fixed-files>
git commit -m "fix: stabilize channelops go live verification"
```

If no fixes were needed, do not create an empty commit.

---

## Self-Review Against Spec

Spec coverage:

- Go-owned live scheduler/tick/queue/handlers: Tasks 1-3, 7-10, 13.
- AutoFlow `material_id`: Task 6.
- PDS plan flag/block held: Tasks 4 and 9.
- Dev allow-all PDS: Tasks 4 and 13.
- Takedown dedup: Tasks 6 and 10.
- `trend_youtube` no manual override: Task 8.
- Feedback completeness: Tasks 5, 6, 10.
- Fake E2E and live smoke: Tasks 11 and 12.
- Deployment single-runner rule: Task 13.

Scope exclusions preserved:

- No Phase B `DecisionAuditEntry` table.
- No Phase C `DiscoverySignal` table.
- No public auto-publish.
- No multi-platform publishing.
- No deletion of Python ChannelOps code.

Implementation order:

1. Foundation and compileable runner.
2. Pure helper behavior with tests.
3. Store and queue semantics.
4. Handler state machine.
5. Python schema/migration seams.
6. Integration and deployment.

Commit strategy:

- Commit after each task.
- Keep Python AutoFlow/migration commit separate from Go runtime commits.
- Keep Docker/compose commit separate from runtime code.
