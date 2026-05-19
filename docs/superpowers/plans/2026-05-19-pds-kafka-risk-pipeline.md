# PDS Kafka Risk Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the Go PDS feature/risk service, add a Kafka-backed actor feature aggregator, and connect VideoProcess Python ChannelOps to PDS with a fail-open client and event outbox.

**Architecture:** Work sequentially across three repos: PDS first, then the new `vp-feature-aggregator`, then VideoProcess. PDS owns decisions and audit, the aggregator owns Kafka schemas and feature facts, and VideoProcess only calls PDS plus emits actor/action events through a durable outbox.

**Tech Stack:** Go 1.25 PDS, chi, cel-go, pgx, go-redis, prometheus/client_golang, franz-go for Kafka, Python 3 FastAPI/httpx/aiokafka/asyncpg/redis/jsonschema for the aggregator, VideoProcess FastAPI/SQLAlchemy/Alembic/pytest, Docker Compose, Redpanda, Kubernetes YAML.

---

## Grounding

- Design spec: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/docs/superpowers/specs/2026-05-19-pds-kafka-risk-design.md`
- PDS repo: `/home/taiwei/Constructure-repos/policy-decision-service`
- Aggregator repo: `/home/taiwei/Constructure-repos/vp-feature-aggregator`
- VideoProcess worktree: `/home/taiwei/.codex/worktrees/d1d5/videoprocess`
- K8s repo area: `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess`
- Current PDS baseline already contains `/v1/decide`, rule loading, keyword/rate-limit/CEL/combiner rules, Postgres audit storage, Dockerfile, Makefile, and tests.
- Current VP branch already includes Go sidecar services. This plan does not change Go sidecar behavior.

## Execution Rules

- Work in the order listed. Do not start VideoProcess changes before PDS and aggregator tests pass.
- Keep commits repo-local. Commit PDS changes in the PDS repo, aggregator changes in the aggregator repo, and VP changes in the VP worktree.
- Before changing a repo, run `git status --short` in that repo and preserve unrelated local changes.
- Use TDD where a task introduces behavior: write or update tests first, run the focused failing test, implement, then run the focused passing test.
- After each task, run the task's verification commands and commit the touched files.

## File Map

### PDS Repo

- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/types.go`: add `EvalState`, feature facts, degraded metadata, and sink-facing decision event fields.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/engine.go`: evaluate rules with `EvalState`, call feature providers, and fan out to decision sinks.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/rule.go`: update rule interface to evaluate `EvalState`.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/cel.go`: expose `features` and degraded metadata to CEL.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/provider.go`: provider interface and fallback composition.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/http_provider.go`: HTTP client for the aggregator feature API.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/provider_test.go`: fail-open and merge behavior tests.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/sink.go`: decision sink interfaces and multi-sink.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/kafka.go`: async Kafka sink for `pds.decisions.v1`.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/kafka_test.go`: queue/drop and serialization tests.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/store/audit.go`: adapt to sink interface and expose queue/drop metrics.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/http.go`: add reload endpoint and metrics instrumentation.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/grpc.go`: complete gRPC adapter over the same engine.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/internal/config/config.go`: add feature provider, Kafka, reload, and timeout settings.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/cmd/server/main.go`: wire providers, sinks, reload, gRPC, and signal handling.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/config/rules.example.yaml`: add one feature-backed CEL rule.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/deploy/kubernetes.yaml`: PDS Deployment, Service, ConfigMap, and ServiceMonitor.
- Modify `/home/taiwei/Constructure-repos/policy-decision-service/README.md`: document feature provider, Kafka sink, reload, and smoke commands.

### Aggregator Repo

- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/pyproject.toml`: dependencies and pytest config.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/AGENTS.md`: repo checks and service boundaries.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/README.md`: local run and smoke instructions.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/schemas/vp.actor.actions.v1.json`: VP action event schema.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/schemas/pds.decisions.v1.json`: PDS decision event schema.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/config.py`: pydantic settings.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/schemas.py`: typed event and feature models.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/windows.py`: bucketed window aggregation logic.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/postgres.py`: long-window summary store.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/redis.py`: short-window and dedupe store.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/consumer.py`: aiokafka consumer loop with manual commit.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/main.py`: FastAPI app and lifecycle.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/`: schema, window, API, and consumer tests.
- Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/deploy/Dockerfile`: service image.

### VideoProcess Repo

- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/pds_client.py`: fail-open async PDS client and models.
- Modify `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/config.py`: add PDS and Kafka settings.
- Modify `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/clients.py`: add `PolicyDecisionClient` protocol and fake implementation.
- Modify `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/service.py`: inject PDS client and event outbox into candidate and promotion paths.
- Modify `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/runner.py`: wire real client when enabled.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/schemas.py`: VP event payload builders.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/outbox.py`: SQLAlchemy outbox writer.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/producer.py`: Kafka producer adapter.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/relay.py`: outbox relay loop.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/event_outbox_relay.py`: CLI entry point.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/alembic/versions/013_event_outbox.py`: event outbox migration, unless a newer revision exists at execution time.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/test_pds_client.py`: client behavior tests.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/events/test_outbox.py`: outbox tests.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/events/test_relay.py`: relay tests.
- Modify `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/channel_agent/test_service.py`: PDS and outbox integration tests.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/docker-compose.pds-kafka.yml`: Redpanda, PDS, aggregator, relay override.
- Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/docs/pds-kafka-smoke.md`: local smoke steps.

### K8s Repo

- Create `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/redpanda.yaml`: Redpanda StatefulSet and Service.
- Create `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/vp-feature-aggregator.yaml`: aggregator Deployment, Service, ConfigMap.
- Modify `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/kustomization.yaml`: include new manifests.

## Task 1: PDS EvalState And Feature Providers

**Files:**
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/types.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/engine.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/rule.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/cel.go`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/provider.go`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/http_provider.go`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/provider_test.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/rule_engine_test.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/cel_test.go`

- [ ] **Step 1: Write provider fail-open tests**

Add these tests to `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/provider_test.go`:

```go
package profile

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestFallbackProviderReturnsFeaturesFromFirstSuccessfulProvider(t *testing.T) {
	provider := FallbackProvider{
		Providers: []Provider{
			StaticProvider{Err: errors.New("redis unavailable")},
			StaticProvider{Features: ActorFeatures{Publishes1H: 3, Flags7D: 2}},
		},
	}

	features, degraded := provider.GetActorFeatures(context.Background(), "actor-1")

	if degraded {
		t.Fatalf("expected successful fallback without degraded=true")
	}
	if features.Publishes1H != 3 || features.Flags7D != 2 {
		t.Fatalf("unexpected features: %+v", features)
	}
}

func TestFallbackProviderFailsOpenWhenAllProvidersFail(t *testing.T) {
	provider := FallbackProvider{
		Providers: []Provider{
			StaticProvider{Err: errors.New("redis unavailable")},
			StaticProvider{Err: errors.New("http unavailable")},
		},
	}

	features, degraded := provider.GetActorFeatures(context.Background(), "actor-1")

	if !degraded {
		t.Fatalf("expected degraded=true")
	}
	if features != (ActorFeatures{}) {
		t.Fatalf("expected zero-value fail-open features, got %+v", features)
	}
}

func TestHTTPFeatureProviderParsesAggregatorResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/features/actor-1" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"actor_id":"actor-1","publishes_5m":1,"publishes_1h":4,"publishes_24h":9,"blocks_24h":2,"flags_7d":7,"comment_burst_1m":3,"as_of":"2026-05-19T00:00:00Z","from_cache":true}`))
	}))
	defer server.Close()

	provider := NewHTTPFeatureProvider(server.URL, 200*time.Millisecond, server.Client())
	features, degraded := provider.GetActorFeatures(context.Background(), "actor-1")

	if degraded {
		t.Fatalf("expected degraded=false")
	}
	if features.Publishes5M != 1 || features.Publishes1H != 4 || features.Blocks24H != 2 || !features.FromCache {
		t.Fatalf("unexpected features: %+v", features)
	}
}
```

- [ ] **Step 2: Run the provider tests and verify they fail**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go test ./internal/profile -run 'TestFallbackProvider|TestHTTPFeatureProvider' -count=1
```

Expected: FAIL because `internal/profile` does not exist.

- [ ] **Step 3: Implement feature provider types**

Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/provider.go`:

```go
package profile

import "context"

type ActorFeatures struct {
	ActorID        string `json:"actor_id,omitempty"`
	Publishes5M    int64  `json:"publishes_5m"`
	Publishes1H    int64  `json:"publishes_1h"`
	Publishes24H   int64  `json:"publishes_24h"`
	Blocks24H      int64  `json:"blocks_24h"`
	Flags7D        int64  `json:"flags_7d"`
	CommentBurst1M int64  `json:"comment_burst_1m"`
	AsOf           string `json:"as_of,omitempty"`
	FromCache      bool   `json:"from_cache"`
}

type Provider interface {
	GetActorFeatures(ctx context.Context, actorID string) (ActorFeatures, bool)
}

type StaticProvider struct {
	Features ActorFeatures
	Err      error
}

func (p StaticProvider) GetActorFeatures(context.Context, string) (ActorFeatures, bool) {
	if p.Err != nil {
		return ActorFeatures{}, true
	}
	return p.Features, false
}

type FallbackProvider struct {
	Providers []Provider
}

func (p FallbackProvider) GetActorFeatures(ctx context.Context, actorID string) (ActorFeatures, bool) {
	for _, provider := range p.Providers {
		if provider == nil {
			continue
		}
		features, degraded := provider.GetActorFeatures(ctx, actorID)
		if !degraded {
			return features, false
		}
	}
	return ActorFeatures{ActorID: actorID}, true
}
```

Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/http_provider.go`:

```go
package profile

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type HTTPFeatureProvider struct {
	baseURL string
	timeout time.Duration
	client  *http.Client
}

func NewHTTPFeatureProvider(baseURL string, timeout time.Duration, client *http.Client) *HTTPFeatureProvider {
	if client == nil {
		client = http.DefaultClient
	}
	if timeout <= 0 {
		timeout = 100 * time.Millisecond
	}
	return &HTTPFeatureProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		timeout: timeout,
		client:  client,
	}
}

func (p *HTTPFeatureProvider) GetActorFeatures(ctx context.Context, actorID string) (ActorFeatures, bool) {
	if p == nil || strings.TrimSpace(p.baseURL) == "" || strings.TrimSpace(actorID) == "" {
		return ActorFeatures{ActorID: actorID}, true
	}
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	endpoint := p.baseURL + "/v1/features/" + url.PathEscape(actorID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return ActorFeatures{ActorID: actorID}, true
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return ActorFeatures{ActorID: actorID}, true
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ActorFeatures{ActorID: actorID}, true
	}
	var features ActorFeatures
	if err := json.NewDecoder(resp.Body).Decode(&features); err != nil {
		return ActorFeatures{ActorID: actorID}, true
	}
	if features.ActorID == "" {
		features.ActorID = actorID
	}
	return features, false
}
```

- [ ] **Step 4: Add EvalState to engine types**

Update `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/types.go` with these types and response metadata:

```go
type ActorFeatures struct {
	ActorID        string `json:"actor_id,omitempty"`
	Publishes5M    int64  `json:"publishes_5m"`
	Publishes1H    int64  `json:"publishes_1h"`
	Publishes24H   int64  `json:"publishes_24h"`
	Blocks24H      int64  `json:"blocks_24h"`
	Flags7D        int64  `json:"flags_7d"`
	CommentBurst1M int64  `json:"comment_burst_1m"`
	AsOf           string `json:"as_of,omitempty"`
	FromCache      bool   `json:"from_cache"`
}

type EvalState struct {
	Request          DecideRequest
	Features         ActorFeatures
	FeatureDegraded  bool
	DegradedWarnings []string
}
```

Add this field to `DecideResponse`:

```go
Metadata map[string]any `json:"metadata,omitempty"`
```

- [ ] **Step 5: Update rule interfaces to use EvalState**

Change `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/engine.go`:

```go
type Rule interface {
	ID() string
	Evaluate(context.Context, EvalState) (RuleResult, error)
}

type ResultAwareRule interface {
	Rule
	Dependencies() []string
	EvaluateWithResults(context.Context, EvalState, map[string]RuleResult) (RuleResult, error)
}
```

Update `RuleEngine.Evaluate` to build an `EvalState` before the rule loop:

```go
state := EvalState{Request: req}
if e.featureProvider != nil {
	features, degraded := e.featureProvider.GetActorFeatures(ctx, req.ActorID)
	state.Features = ActorFeatures{
		ActorID:        features.ActorID,
		Publishes5M:    features.Publishes5M,
		Publishes1H:    features.Publishes1H,
		Publishes24H:   features.Publishes24H,
		Blocks24H:      features.Blocks24H,
		Flags7D:        features.Flags7D,
		CommentBurst1M: features.CommentBurst1M,
		AsOf:           features.AsOf,
		FromCache:      features.FromCache,
	}
	state.FeatureDegraded = degraded
	if degraded {
		state.DegradedWarnings = append(state.DegradedWarnings, "feature_provider_unavailable")
	}
}
```

Add a `FeatureProvider` interface in `engine.go`:

```go
type FeatureProvider interface {
	GetActorFeatures(context.Context, string) (ActorFeatures, bool)
}
```

Add `WithFeatureProvider(provider FeatureProvider) *RuleEngine`.

- [ ] **Step 6: Update CEL activation tests**

Add a CEL test in `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/cel_test.go`:

```go
func TestCELRuleCanMatchFeatureFacts(t *testing.T) {
	rule, err := NewCELRule(CELRuleConfig{
		ID:   "burst-publisher",
		Expr: "features.publishes_5m >= 3 && !degraded.feature_provider",
		OnMatch: RuleAction{
			Verdict: engine.VerdictFlag,
			Code:    "publishing_burst",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := rule.Evaluate(context.Background(), engine.EvalState{
		Request: engine.DecideRequest{ActorID: "actor-1", Action: engine.ActionContext{Type: "publish"}},
		Features: engine.ActorFeatures{
			Publishes5M: 3,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Matched || result.Verdict != engine.VerdictFlag {
		t.Fatalf("expected flag match, got %+v", result)
	}
}
```

- [ ] **Step 7: Update CEL activation implementation**

In `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/cel.go`, update the CEL environment variables:

```go
cel.Variable("actor", cel.DynType),
cel.Variable("action", cel.DynType),
cel.Variable("content", cel.DynType),
cel.Variable("context", cel.DynType),
cel.Variable("features", cel.DynType),
cel.Variable("degraded", cel.DynType),
```

Update `Evaluate` to accept `engine.EvalState` and evaluate with:

```go
req := state.Request
out, _, err := r.program.Eval(map[string]any{
	"actor": actorActivation(req),
	"action": map[string]any{
		"type":     req.Action.Type,
		"platform": req.Action.Platform,
	},
	"content": map[string]any{
		"title":       req.Content.Title,
		"description": req.Content.Description,
		"duration_s":  req.Content.DurationS,
		"tags":        req.Content.Tags,
	},
	"context": req.Context,
	"features": map[string]any{
		"publishes_5m":     state.Features.Publishes5M,
		"publishes_1h":     state.Features.Publishes1H,
		"publishes_24h":    state.Features.Publishes24H,
		"blocks_24h":       state.Features.Blocks24H,
		"flags_7d":         state.Features.Flags7D,
		"comment_burst_1m": state.Features.CommentBurst1M,
		"from_cache":       state.Features.FromCache,
	},
	"degraded": map[string]any{
		"feature_provider": state.FeatureDegraded,
	},
})
```

Apply the same `EvalState` signature to keyword, rate-limit, and combiner rules.

- [ ] **Step 8: Run PDS rule tests**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
gofmt -w internal/engine internal/rules internal/profile
go test ./internal/profile ./internal/engine ./internal/rules -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit PDS EvalState work**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
git status --short
git add internal/engine internal/rules internal/profile
git commit -m "feat: add feature-aware evaluation state"
```

Expected: commit contains only PDS EvalState, feature provider, and related tests.

## Task 2: PDS Decision Sink, Kafka Publisher, Metrics, And Reload

**Files:**
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/sink.go`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/kafka.go`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/kafka_test.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/store/audit.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/telemetry/metrics.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/http.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/http_test.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/config/config.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/cmd/server/main.go`

- [ ] **Step 1: Add sink tests**

Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/kafka_test.go`:

```go
package sink

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Ctwqk/policy-decision-service/internal/engine"
)

type recordingPublisher struct {
	payloads [][]byte
	err      error
}

func (p *recordingPublisher) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	p.payloads = append(p.payloads, append([]byte(nil), value...))
	return p.err
}

func TestKafkaDecisionSinkSerializesDecisionEvent(t *testing.T) {
	publisher := &recordingPublisher{}
	sink := NewKafkaDecisionSink(KafkaDecisionSinkConfig{
		Topic:     "pds.decisions.v1",
		QueueSize: 2,
		Publisher: publisher,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sink.Run(ctx)

	sink.Enqueue(ctx, engine.AuditRecord{
		DecisionID: "decision-1",
		ActorID:    "actor-1",
		ActionType: "publish",
		Platform:   "youtube",
		Verdict:    engine.VerdictBlock,
		Score:      0.9,
		Reasons:    []engine.Reason{{Code: "burst", Rule: "r1"}},
		Client:     "vp",
	})

	deadline := time.After(500 * time.Millisecond)
	for len(publisher.payloads) == 0 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for publish")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	var event DecisionEvent
	if err := json.Unmarshal(publisher.payloads[0], &event); err != nil {
		t.Fatal(err)
	}
	if event.TopicVersion != "pds.decisions.v1" || event.ActorID != "actor-1" || event.Verdict != "block" {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestKafkaDecisionSinkDropsWhenQueueFull(t *testing.T) {
	sink := NewKafkaDecisionSink(KafkaDecisionSinkConfig{
		Topic:     "pds.decisions.v1",
		QueueSize: 1,
		Publisher: &recordingPublisher{},
	})

	sink.Enqueue(context.Background(), engine.AuditRecord{DecisionID: "one"})
	sink.Enqueue(context.Background(), engine.AuditRecord{DecisionID: "two"})

	if sink.Dropped() != 1 {
		t.Fatalf("expected one dropped event, got %d", sink.Dropped())
	}
}
```

- [ ] **Step 2: Run sink tests and verify they fail**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go test ./internal/sink -count=1
```

Expected: FAIL because `internal/sink` does not exist.

- [ ] **Step 3: Implement sink interfaces and multi-sink**

Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/sink.go`:

```go
package sink

import (
	"context"

	"github.com/Ctwqk/policy-decision-service/internal/engine"
)

type DecisionSink interface {
	Enqueue(context.Context, engine.AuditRecord)
}

type MultiDecisionSink struct {
	Sinks []DecisionSink
}

func (s MultiDecisionSink) Enqueue(ctx context.Context, record engine.AuditRecord) {
	for _, sink := range s.Sinks {
		if sink != nil {
			sink.Enqueue(ctx, record)
		}
	}
}
```

- [ ] **Step 4: Implement Kafka decision sink**

Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/kafka.go`:

```go
package sink

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/Ctwqk/policy-decision-service/internal/engine"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
}

type DecisionEvent struct {
	EventID      string          `json:"event_id"`
	TopicVersion string         `json:"topic_version"`
	ActorID      string         `json:"actor_id"`
	ActionType   string         `json:"action_type"`
	Platform     string         `json:"platform,omitempty"`
	Verdict      string         `json:"verdict"`
	Score        float64        `json:"score"`
	Reasons      []engine.Reason `json:"reasons"`
	DecisionID   string         `json:"decision_id"`
	Client       string         `json:"client,omitempty"`
	OccurredAt   string         `json:"occurred_at"`
}

type KafkaDecisionSinkConfig struct {
	Topic     string
	QueueSize int
	Publisher Publisher
}

type KafkaDecisionSink struct {
	topic     string
	queue     chan engine.AuditRecord
	publisher Publisher
	dropped   atomic.Int64
}

func NewKafkaDecisionSink(cfg KafkaDecisionSinkConfig) *KafkaDecisionSink {
	if cfg.Topic == "" {
		cfg.Topic = "pds.decisions.v1"
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 10000
	}
	return &KafkaDecisionSink{
		topic:     cfg.Topic,
		queue:     make(chan engine.AuditRecord, cfg.QueueSize),
		publisher: cfg.Publisher,
	}
}

func (s *KafkaDecisionSink) Enqueue(ctx context.Context, record engine.AuditRecord) {
	if s == nil || s.publisher == nil {
		return
	}
	select {
	case s.queue <- record:
	default:
		s.dropped.Add(1)
	}
}

func (s *KafkaDecisionSink) Dropped() int64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}

func (s *KafkaDecisionSink) Run(ctx context.Context) {
	if s == nil || s.publisher == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case record := <-s.queue:
			_ = s.publish(ctx, record)
		}
	}
}

func (s *KafkaDecisionSink) publish(ctx context.Context, record engine.AuditRecord) error {
	event := DecisionEvent{
		EventID:      record.DecisionID,
		TopicVersion: "pds.decisions.v1",
		ActorID:      record.ActorID,
		ActionType:   record.ActionType,
		Platform:     record.Platform,
		Verdict:      string(record.Verdict),
		Score:        record.Score,
		Reasons:      record.Reasons,
		DecisionID:   record.DecisionID,
		Client:       record.Client,
		OccurredAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return s.publisher.Publish(ctx, s.topic, []byte(record.ActorID), payload)
}
```

- [ ] **Step 5: Add a franz-go publisher adapter**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go get github.com/twmb/franz-go/pkg/kgo
```

Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/sink/franz.go`:

```go
package sink

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type FranzPublisher struct {
	client *kgo.Client
}

func NewFranzPublisher(brokers []string, clientID string) (*FranzPublisher, error) {
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	if clientID != "" {
		opts = append(opts, kgo.ClientID(clientID))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &FranzPublisher{client: client}, nil
}

func (p *FranzPublisher) Publish(ctx context.Context, topic string, key []byte, value []byte) error {
	return p.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
}

func (p *FranzPublisher) Close() {
	if p != nil && p.client != nil {
		p.client.Close()
	}
}
```

- [ ] **Step 6: Add config values**

Add these fields to `/home/taiwei/Constructure-repos/policy-decision-service/internal/config/config.go`:

```go
FeatureProviderURL     string
FeatureProviderTimeout time.Duration
KafkaEnabled           bool
KafkaBrokers           []string
KafkaDecisionTopic     string
KafkaClientID          string
KafkaQueueSize         int
```

Add defaults:

```text
PDS_FEATURE_PROVIDER_URL=http://vp-feature-aggregator:8080
PDS_FEATURE_PROVIDER_TIMEOUT=100ms
PDS_KAFKA_ENABLED=false
PDS_KAFKA_BROKERS=redpanda:9092
PDS_KAFKA_DECISION_TOPIC=pds.decisions.v1
PDS_KAFKA_CLIENT_ID=pds
PDS_KAFKA_QUEUE_SIZE=10000
```

Use `time.ParseDuration` for duration values and `strings.Split` for broker lists.

- [ ] **Step 7: Add reload endpoint tests**

In `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/http_test.go`, add:

```go
func TestAdminReloadCallsReloadFunction(t *testing.T) {
	called := false
	router := NewRouter(Dependencies{
		Engine: engine.NewAllowEngine("test"),
		Reload: func(context.Context) error {
			called = true
			return nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/reload", nil)
	resp := httptest.NewRecorder()

	router.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.Code)
	}
	if !called {
		t.Fatalf("expected reload function to be called")
	}
}
```

- [ ] **Step 8: Implement reload dependency**

Add `Reload func(context.Context) error` to `api.Dependencies`. In `NewRouter`, register:

```go
r.Post("/v1/admin/reload", reload(deps.Reload))
```

Add handler:

```go
func reload(fn func(context.Context) error) http.HandlerFunc {
	if fn == nil {
		fn = func(context.Context) error { return nil }
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(r.Context()); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "reloaded"})
	}
}
```

- [ ] **Step 9: Wire provider and sinks in server**

In `/home/taiwei/Constructure-repos/policy-decision-service/cmd/server/main.go`, build:

```go
featureProvider := profile.NewHTTPFeatureProvider(cfg.FeatureProviderURL, cfg.FeatureProviderTimeout, nil)
decisionEngine := engine.NewRuleEngine(snapshot.Version, snapshot.Rules).WithFeatureProvider(featureProvider)

var sinks []sink.DecisionSink
if postgres != nil {
	auditWriter := store.NewAuditWriter(postgres.Pool(), cfg.AuditQueueSize, cfg.AuditBatchSize)
	go auditWriter.Run(ctx)
	sinks = append(sinks, auditWriter)
}
if cfg.KafkaEnabled {
	publisher, err := sink.NewFranzPublisher(cfg.KafkaBrokers, cfg.KafkaClientID)
	if err != nil {
		logger.Warn().Err(err).Msg("kafka decision sink disabled")
	} else {
		defer publisher.Close()
		kafkaSink := sink.NewKafkaDecisionSink(sink.KafkaDecisionSinkConfig{
			Topic:     cfg.KafkaDecisionTopic,
			QueueSize: cfg.KafkaQueueSize,
			Publisher: publisher,
		})
		go kafkaSink.Run(ctx)
		sinks = append(sinks, kafkaSink)
	}
}
decisionEngine.WithAuditSink(sink.MultiDecisionSink{Sinks: sinks})
```

Use an atomic `snapshot` holder for reload so `POST /v1/admin/reload` and SIGHUP replace the current rules without restarting.

- [ ] **Step 10: Run PDS tests**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
gofmt -w cmd internal
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 11: Commit PDS sink and reload work**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
git status --short
git add go.mod go.sum cmd internal
git commit -m "feat: add pds decision sinks and reload"
```

Expected: commit contains PDS sink, reload, metrics-adjacent wiring, and tests.

## Task 3: PDS gRPC, Kubernetes, And Docs

**Files:**
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/grpc.go`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/proto/pds/v1/pds.proto`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/buf.yaml`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/buf.gen.yaml`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/cmd/server/main.go`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/config/rules.example.yaml`
- Create: `/home/taiwei/Constructure-repos/policy-decision-service/deploy/kubernetes.yaml`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/README.md`

- [ ] **Step 1: Add protobuf contract**

Create `/home/taiwei/Constructure-repos/policy-decision-service/proto/pds/v1/pds.proto`:

```proto
syntax = "proto3";

package pds.v1;

option go_package = "github.com/Ctwqk/policy-decision-service/proto/gen/pds/v1;pdsv1";

service PolicyDecisionService {
  rpc Decide(DecideRequest) returns (DecideResponse);
}

message DecideRequest {
  string actor_id = 1;
  Action action = 2;
  Content content = 3;
  map<string, string> context = 4;
}

message Action {
  string type = 1;
  string platform = 2;
}

message Content {
  string title = 1;
  string description = 2;
  int32 duration_s = 3;
  repeated string tags = 4;
}

message Reason {
  string code = 1;
  string rule = 2;
  string detail = 3;
}

message DecideResponse {
  string decision_id = 1;
  string verdict = 2;
  double score = 3;
  repeated Reason reasons = 4;
  repeated string evaluated_rules = 5;
  string rules_version = 6;
  int64 latency_ms = 7;
}
```

Create `buf.yaml` and `buf.gen.yaml`:

```yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
breaking:
  use:
    - FILE
```

```yaml
version: v2
plugins:
  - local: protoc-gen-go
    out: proto/gen
    opt: paths=source_relative
  - local: protoc-gen-go-grpc
    out: proto/gen
    opt: paths=source_relative
```

- [ ] **Step 2: Generate protobuf code**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
$(go env GOPATH)/bin/buf generate
```

Expected: generated Go files appear under `proto/gen/pds/v1/`.

- [ ] **Step 3: Implement gRPC adapter**

Update `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/grpc.go` so the gRPC service converts protobuf requests into `engine.DecideRequest` and returns `engine.DecideResponse` fields. Use the same `DecisionEngine` interface as HTTP.

Core conversion shape:

```go
func grpcRequestToEngine(req *pdsv1.DecideRequest) engine.DecideRequest {
	contextMap := make(map[string]any, len(req.GetContext()))
	for key, value := range req.GetContext() {
		contextMap[key] = value
	}
	return engine.DecideRequest{
		ActorID: req.GetActorId(),
		Action: engine.ActionContext{
			Type:     req.GetAction().GetType(),
			Platform: req.GetAction().GetPlatform(),
		},
		Content: engine.ContentContext{
			Title:       req.GetContent().GetTitle(),
			Description: req.GetContent().GetDescription(),
			DurationS:   int(req.GetContent().GetDurationS()),
			Tags:        req.GetContent().GetTags(),
		},
		Context: contextMap,
		ClientID: "grpc",
	}
}
```

- [ ] **Step 4: Add a feature-backed sample rule**

Update `/home/taiwei/Constructure-repos/policy-decision-service/config/rules.example.yaml` with this CEL rule:

```yaml
  - id: burst_publish_feature_flag
    type: cel
    enabled: true
    expr: "action.type == 'publish' && features.publishes_5m >= 3 && !degraded.feature_provider"
    on_match:
      verdict: flag
      code: publishing_burst
```

- [ ] **Step 5: Add PDS Kubernetes manifest**

Create `/home/taiwei/Constructure-repos/policy-decision-service/deploy/kubernetes.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pds-config
  namespace: videoprocess
data:
  PDS_HTTP_ADDR: ":8080"
  PDS_GRPC_ADDR: ":9090"
  PDS_FEATURE_PROVIDER_URL: "http://vp-feature-aggregator:8080"
  PDS_KAFKA_ENABLED: "true"
  PDS_KAFKA_BROKERS: "redpanda:9092"
  PDS_KAFKA_DECISION_TOPIC: "pds.decisions.v1"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pds
  namespace: videoprocess
spec:
  replicas: 1
  selector:
    matchLabels:
      app: pds
  template:
    metadata:
      labels:
        app: pds
    spec:
      containers:
        - name: pds
          image: policy-decision-service:local
          ports:
            - containerPort: 8080
              name: http
            - containerPort: 9090
              name: grpc
          envFrom:
            - configMapRef:
                name: pds-config
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
---
apiVersion: v1
kind: Service
metadata:
  name: pds
  namespace: videoprocess
spec:
  selector:
    app: pds
  ports:
    - name: http
      port: 8080
      targetPort: http
    - name: grpc
      port: 9090
      targetPort: grpc
```

- [ ] **Step 6: Run full PDS verification**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
gofmt -w cmd internal proto
go test ./... -count=1
go build ./cmd/server
```

Expected: PASS.

- [ ] **Step 7: Commit PDS deployment work**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
git status --short
git add README.md buf.yaml buf.gen.yaml cmd config deploy internal proto go.mod go.sum
git commit -m "feat: add pds grpc and deployment assets"
```

Expected: commit contains gRPC, sample rule, PDS manifest, and docs.

## Task 4: Create Aggregator Repo, Schemas, And API Skeleton

**Files:**
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/pyproject.toml`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/AGENTS.md`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/README.md`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/schemas/vp.actor.actions.v1.json`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/schemas/pds.decisions.v1.json`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/config.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/schemas.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/main.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_schemas.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_api.py`

- [ ] **Step 1: Initialize the repo**

Run:

```bash
mkdir -p /home/taiwei/Constructure-repos/vp-feature-aggregator
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
git init
mkdir -p app store schemas tests deploy
touch app/__init__.py
```

Expected: new git repo with package folders.

- [ ] **Step 2: Create pyproject**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/pyproject.toml`:

```toml
[project]
name = "vp-feature-aggregator"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
  "aiokafka>=0.10.0",
  "asyncpg>=0.29.0",
  "fastapi>=0.111.0",
  "httpx>=0.27.0",
  "jsonschema>=4.22.0",
  "pydantic>=2.7.0",
  "pydantic-settings>=2.3.0",
  "redis>=5.0.0",
  "uvicorn>=0.30.0",
]

[project.optional-dependencies]
dev = [
  "pytest>=8.2.0",
  "pytest-asyncio>=0.23.0",
  "ruff>=0.4.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
pythonpath = ["."]
testpaths = ["tests"]

[tool.ruff]
line-length = 100
```

- [ ] **Step 3: Write schema tests first**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_schemas.py`:

```python
from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator


SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"


def _schema(name: str) -> dict:
    return json.loads((SCHEMA_DIR / name).read_text())


def test_vp_actor_action_schema_accepts_candidate_event():
    schema = _schema("vp.actor.actions.v1.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(
        {
            "event_id": "event-1",
            "topic_version": "vp.actor.actions.v1",
            "actor_id": "account-1",
            "action_type": "candidate_accepted",
            "platform": "youtube",
            "occurred_at": "2026-05-19T00:00:00Z",
            "source": "videoprocess.channel_ops",
            "metadata": {"candidate_id": "candidate-1"},
        }
    )


def test_pds_decision_schema_accepts_block_event():
    schema = _schema("pds.decisions.v1.json")
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(
        {
            "event_id": "decision-1",
            "topic_version": "pds.decisions.v1",
            "actor_id": "account-1",
            "action_type": "publish",
            "platform": "youtube",
            "verdict": "block",
            "score": 0.9,
            "reasons": [{"code": "burst", "rule": "r1"}],
            "decision_id": "decision-1",
            "occurred_at": "2026-05-19T00:00:00Z",
        }
    )
```

- [ ] **Step 4: Run schema tests and verify they fail**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
python3 -m pytest tests/test_schemas.py -q
```

Expected: FAIL because schema files do not exist.

- [ ] **Step 5: Add versioned JSON schemas**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/schemas/vp.actor.actions.v1.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "vp.actor.actions.v1",
  "type": "object",
  "required": ["event_id", "topic_version", "actor_id", "action_type", "occurred_at", "source"],
  "properties": {
    "event_id": {"type": "string", "minLength": 1},
    "topic_version": {"const": "vp.actor.actions.v1"},
    "actor_id": {"type": "string", "minLength": 1},
    "action_type": {
      "type": "string",
      "enum": [
        "candidate_accepted",
        "candidate_blocked",
        "candidate_flagged",
        "publication_promotion_attempted",
        "publication_promotion_blocked",
        "publication_scheduled"
      ]
    },
    "platform": {"type": "string"},
    "occurred_at": {"type": "string", "format": "date-time"},
    "source": {"const": "videoprocess.channel_ops"},
    "metadata": {"type": "object", "additionalProperties": true}
  },
  "additionalProperties": false
}
```

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/schemas/pds.decisions.v1.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "pds.decisions.v1",
  "type": "object",
  "required": ["event_id", "topic_version", "actor_id", "action_type", "verdict", "score", "decision_id", "occurred_at"],
  "properties": {
    "event_id": {"type": "string", "minLength": 1},
    "topic_version": {"const": "pds.decisions.v1"},
    "actor_id": {"type": "string", "minLength": 1},
    "action_type": {"type": "string", "minLength": 1},
    "platform": {"type": "string"},
    "verdict": {"type": "string", "enum": ["allow", "flag", "block"]},
    "score": {"type": "number"},
    "reasons": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "code": {"type": "string"},
          "rule": {"type": "string"},
          "detail": {"type": "string"}
        },
        "required": ["code"],
        "additionalProperties": false
      }
    },
    "decision_id": {"type": "string", "minLength": 1},
    "client": {"type": "string"},
    "occurred_at": {"type": "string", "format": "date-time"}
  },
  "additionalProperties": false
}
```

- [ ] **Step 6: Add config and response model**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/config.py`:

```python
from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    http_host: str = "0.0.0.0"
    http_port: int = 8080
    kafka_brokers: str = "redpanda:9092"
    kafka_group_id: str = "vp-feature-aggregator"
    vp_actions_topic: str = "vp.actor.actions.v1"
    pds_decisions_topic: str = "pds.decisions.v1"
    dead_letter_topic: str = "risk.events.dlq.v1"
    database_url: str = "postgresql://vp:vp_secret@localhost:5435/videoprocess"
    redis_url: str = "redis://localhost:6380/2"
    dedupe_ttl_seconds: int = 604800

    model_config = {"env_prefix": "AGG_", "case_sensitive": False}


settings = Settings()
```

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/schemas.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field


class FeatureResponse(BaseModel):
    actor_id: str
    publishes_5m: int = 0
    publishes_1h: int = 0
    publishes_24h: int = 0
    blocks_24h: int = 0
    flags_7d: int = 0
    comment_burst_1m: int = 0
    as_of: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    from_cache: bool = False


class VPActorActionEvent(BaseModel):
    event_id: str
    topic_version: Literal["vp.actor.actions.v1"]
    actor_id: str
    action_type: str
    platform: str | None = None
    occurred_at: datetime
    source: Literal["videoprocess.channel_ops"]
    metadata: dict[str, Any] = Field(default_factory=dict)


class PDSDecisionEvent(BaseModel):
    event_id: str
    topic_version: Literal["pds.decisions.v1"]
    actor_id: str
    action_type: str
    platform: str | None = None
    verdict: Literal["allow", "flag", "block"]
    score: float
    reasons: list[dict[str, Any]] = Field(default_factory=list)
    decision_id: str
    client: str | None = None
    occurred_at: datetime
```

- [ ] **Step 7: Add FastAPI skeleton tests**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_api.py`:

```python
from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import create_app


def test_healthz():
    client = TestClient(create_app())
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_features_returns_zero_defaults_for_unknown_actor():
    client = TestClient(create_app())
    response = client.get("/v1/features/actor-unknown")
    assert response.status_code == 200
    payload = response.json()
    assert payload["actor_id"] == "actor-unknown"
    assert payload["publishes_5m"] == 0
    assert payload["blocks_24h"] == 0
    assert payload["from_cache"] is False
```

- [ ] **Step 8: Implement FastAPI skeleton**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/main.py`:

```python
from __future__ import annotations

from fastapi import FastAPI

from app.schemas import FeatureResponse


def create_app() -> FastAPI:
    app = FastAPI(title="VP Feature Aggregator")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict[str, str]:
        return {"status": "ready"}

    @app.get("/v1/features/{actor_id}", response_model=FeatureResponse)
    async def get_features(actor_id: str) -> FeatureResponse:
        return FeatureResponse(actor_id=actor_id)

    return app


app = create_app()
```

- [ ] **Step 9: Run aggregator skeleton tests**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
python3 -m pytest -q
python3 -m ruff check . || true
```

Expected: pytest PASS.

- [ ] **Step 10: Commit aggregator skeleton**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
git status --short
git add AGENTS.md README.md pyproject.toml app schemas tests
git commit -m "feat: scaffold vp feature aggregator"
```

Expected: commit contains initial aggregator app, schemas, and tests.

## Task 5: Aggregator Window Logic And Stores

**Files:**
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/windows.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/__init__.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/memory.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/redis.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/postgres.py`
- Modify: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/main.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_windows.py`
- Modify: `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_api.py`

- [ ] **Step 1: Write window aggregation tests**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_windows.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone

from app.schemas import PDSDecisionEvent, VPActorActionEvent
from app.windows import WindowAggregator


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)


def test_action_events_increment_publish_windows():
    aggregator = WindowAggregator(now=lambda: NOW)
    event = VPActorActionEvent(
        event_id="event-1",
        topic_version="vp.actor.actions.v1",
        actor_id="actor-1",
        action_type="publication_scheduled",
        platform="youtube",
        occurred_at=NOW,
        source="videoprocess.channel_ops",
    )

    aggregator.apply_vp_action(event)
    features = aggregator.features_for("actor-1")

    assert features.publishes_5m == 1
    assert features.publishes_1h == 1
    assert features.publishes_24h == 1


def test_decision_events_increment_block_and_flag_windows():
    aggregator = WindowAggregator(now=lambda: NOW)
    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="decision-1",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="block",
            score=0.8,
            decision_id="decision-1",
            occurred_at=NOW,
        )
    )
    aggregator.apply_pds_decision(
        PDSDecisionEvent(
            event_id="decision-2",
            topic_version="pds.decisions.v1",
            actor_id="actor-1",
            action_type="publish",
            platform="youtube",
            verdict="flag",
            score=0.7,
            decision_id="decision-2",
            occurred_at=NOW,
        )
    )

    features = aggregator.features_for("actor-1")

    assert features.blocks_24h == 1
    assert features.flags_7d == 1
```

- [ ] **Step 2: Run window tests and verify they fail**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
python3 -m pytest tests/test_windows.py -q
```

Expected: FAIL because `app/windows.py` does not exist.

- [ ] **Step 3: Implement in-memory window aggregator**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/windows.py`:

```python
from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from app.schemas import FeatureResponse, PDSDecisionEvent, VPActorActionEvent


PUBLISH_ACTIONS = {"candidate_accepted", "publication_scheduled"}


@dataclass
class ActorCounters:
    publishes: list[datetime] = field(default_factory=list)
    blocks: list[datetime] = field(default_factory=list)
    flags: list[datetime] = field(default_factory=list)


class WindowAggregator:
    def __init__(self, *, now: Callable[[], datetime] | None = None) -> None:
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._actors: dict[str, ActorCounters] = defaultdict(ActorCounters)

    def apply_vp_action(self, event: VPActorActionEvent) -> None:
        if event.action_type in PUBLISH_ACTIONS:
            self._actors[event.actor_id].publishes.append(event.occurred_at)

    def apply_pds_decision(self, event: PDSDecisionEvent) -> None:
        if event.verdict == "block":
            self._actors[event.actor_id].blocks.append(event.occurred_at)
        elif event.verdict == "flag":
            self._actors[event.actor_id].flags.append(event.occurred_at)

    def features_for(self, actor_id: str) -> FeatureResponse:
        now = self._now()
        counters = self._actors[actor_id]
        return FeatureResponse(
            actor_id=actor_id,
            publishes_5m=_count_since(counters.publishes, now - timedelta(minutes=5)),
            publishes_1h=_count_since(counters.publishes, now - timedelta(hours=1)),
            publishes_24h=_count_since(counters.publishes, now - timedelta(hours=24)),
            blocks_24h=_count_since(counters.blocks, now - timedelta(hours=24)),
            flags_7d=_count_since(counters.flags, now - timedelta(days=7)),
            comment_burst_1m=0,
            as_of=now,
            from_cache=False,
        )


def _count_since(values: list[datetime], since: datetime) -> int:
    return sum(1 for value in values if value >= since)
```

- [ ] **Step 4: Add store protocol and memory store**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/memory.py`:

```python
from __future__ import annotations

from app.schemas import FeatureResponse, PDSDecisionEvent, VPActorActionEvent
from app.windows import WindowAggregator


class MemoryFeatureStore:
    def __init__(self, aggregator: WindowAggregator | None = None) -> None:
        self.aggregator = aggregator or WindowAggregator()
        self.seen_event_ids: set[str] = set()

    async def apply_vp_action(self, event: VPActorActionEvent) -> bool:
        if event.event_id in self.seen_event_ids:
            return False
        self.seen_event_ids.add(event.event_id)
        self.aggregator.apply_vp_action(event)
        return True

    async def apply_pds_decision(self, event: PDSDecisionEvent) -> bool:
        if event.event_id in self.seen_event_ids:
            return False
        self.seen_event_ids.add(event.event_id)
        self.aggregator.apply_pds_decision(event)
        return True

    async def features_for(self, actor_id: str) -> FeatureResponse:
        return self.aggregator.features_for(actor_id)
```

- [ ] **Step 5: Use store in API**

Update `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/main.py`:

```python
from app.store.memory import MemoryFeatureStore


def create_app(store: MemoryFeatureStore | None = None) -> FastAPI:
    app = FastAPI(title="VP Feature Aggregator")
    feature_store = store or MemoryFeatureStore()

    @app.get("/v1/features/{actor_id}", response_model=FeatureResponse)
    async def get_features(actor_id: str) -> FeatureResponse:
        return await feature_store.features_for(actor_id)
```

- [ ] **Step 6: Add Redis and Postgres store shells**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/redis.py` with dedupe and short-window method names:

```python
from __future__ import annotations

import redis.asyncio as redis


class RedisWindowStore:
    def __init__(self, client: redis.Redis, *, dedupe_ttl_seconds: int) -> None:
        self.client = client
        self.dedupe_ttl_seconds = dedupe_ttl_seconds

    async def mark_seen(self, event_id: str) -> bool:
        key = f"risk:dedupe:{event_id}"
        created = await self.client.set(key, "1", nx=True, ex=self.dedupe_ttl_seconds)
        return bool(created)

    async def increment_bucket(self, actor_id: str, metric: str, bucket: str) -> None:
        await self.client.hincrby(f"risk:actor:{actor_id}:{metric}", bucket, 1)
```

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/store/postgres.py`:

```python
from __future__ import annotations

import asyncpg


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS actor_feature_summaries (
  actor_id text PRIMARY KEY,
  publishes_24h integer NOT NULL DEFAULT 0,
  blocks_24h integer NOT NULL DEFAULT 0,
  flags_7d integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
)
"""


class PostgresSummaryStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def ensure_schema(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_SQL)
```

- [ ] **Step 7: Run aggregator tests**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
python3 -m pytest -q
python3 -m ruff check . || true
```

Expected: pytest PASS.

- [ ] **Step 8: Commit aggregator window work**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
git status --short
git add app tests
git commit -m "feat: add actor feature windows"
```

Expected: commit contains window logic, store shells, and tests.

## Task 6: Aggregator Kafka Consumer, Container, And K8s Assets

**Files:**
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/consumer.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_consumer.py`
- Create: `/home/taiwei/Constructure-repos/vp-feature-aggregator/deploy/Dockerfile`
- Modify: `/home/taiwei/Constructure-repos/vp-feature-aggregator/README.md`
- Create: `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/redpanda.yaml`
- Create: `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/vp-feature-aggregator.yaml`
- Modify: `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/kustomization.yaml`

- [ ] **Step 1: Write consumer tests**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/tests/test_consumer.py`:

```python
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from app.consumer import EventConsumer
from app.store.memory import MemoryFeatureStore


class FakeMessage:
    def __init__(self, topic: str, value: bytes) -> None:
        self.topic = topic
        self.value = value


@pytest.mark.asyncio
async def test_consumer_applies_vp_action_event():
    store = MemoryFeatureStore()
    consumer = EventConsumer(store=store, dead_letter_producer=None)
    payload = {
        "event_id": "event-1",
        "topic_version": "vp.actor.actions.v1",
        "actor_id": "actor-1",
        "action_type": "publication_scheduled",
        "platform": "youtube",
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "source": "videoprocess.channel_ops",
    }

    await consumer.process_message(FakeMessage("vp.actor.actions.v1", json.dumps(payload).encode()))

    features = await store.features_for("actor-1")
    assert features.publishes_5m == 1


@pytest.mark.asyncio
async def test_consumer_sends_bad_json_to_dead_letter():
    sent: list[tuple[str, bytes]] = []

    async def send(topic: str, value: bytes) -> None:
        sent.append((topic, value))

    store = MemoryFeatureStore()
    consumer = EventConsumer(store=store, dead_letter_producer=send)

    await consumer.process_message(FakeMessage("vp.actor.actions.v1", b"{bad-json"))

    assert sent
    assert sent[0][0] == "risk.events.dlq.v1"
```

- [ ] **Step 2: Run consumer tests and verify they fail**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
python3 -m pytest tests/test_consumer.py -q
```

Expected: FAIL because `app/consumer.py` does not exist.

- [ ] **Step 3: Implement consumer message processor**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/app/consumer.py`:

```python
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from aiokafka import AIOKafkaConsumer

from app.config import settings
from app.schemas import PDSDecisionEvent, VPActorActionEvent


DeadLetterProducer = Callable[[str, bytes], Awaitable[None]]


class EventConsumer:
    def __init__(
        self,
        *,
        store: Any,
        dead_letter_producer: DeadLetterProducer | None,
        dead_letter_topic: str = "risk.events.dlq.v1",
    ) -> None:
        self.store = store
        self.dead_letter_producer = dead_letter_producer
        self.dead_letter_topic = dead_letter_topic

    async def process_message(self, message: Any) -> None:
        try:
            payload = json.loads(message.value.decode("utf-8"))
            if message.topic == settings.vp_actions_topic:
                await self.store.apply_vp_action(VPActorActionEvent.model_validate(payload))
            elif message.topic == settings.pds_decisions_topic:
                await self.store.apply_pds_decision(PDSDecisionEvent.model_validate(payload))
        except Exception:
            if self.dead_letter_producer is not None:
                await self.dead_letter_producer(self.dead_letter_topic, bytes(message.value))


async def run_consumer(store: Any) -> None:
    consumer = AIOKafkaConsumer(
        settings.vp_actions_topic,
        settings.pds_decisions_topic,
        bootstrap_servers=settings.kafka_brokers.split(","),
        group_id=settings.kafka_group_id,
        enable_auto_commit=False,
    )
    event_consumer = EventConsumer(store=store, dead_letter_producer=None)
    await consumer.start()
    try:
        async for message in consumer:
            await event_consumer.process_message(message)
            await consumer.commit()
    finally:
        await consumer.stop()
```

- [ ] **Step 4: Add container image**

Create `/home/taiwei/Constructure-repos/vp-feature-aggregator/deploy/Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY app ./app
COPY schemas ./schemas

RUN pip install --no-cache-dir .

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

- [ ] **Step 5: Add k8s manifests**

Create `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/redpanda.yaml`:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redpanda
  namespace: videoprocess
spec:
  serviceName: redpanda
  replicas: 1
  selector:
    matchLabels:
      app: redpanda
  template:
    metadata:
      labels:
        app: redpanda
    spec:
      containers:
        - name: redpanda
          image: redpandadata/redpanda:v24.3.5
          args:
            - redpanda
            - start
            - --overprovisioned
            - --smp=1
            - --memory=512M
            - --reserve-memory=0M
            - --node-id=0
            - --check=false
            - --kafka-addr=0.0.0.0:9092
            - --advertise-kafka-addr=redpanda:9092
          ports:
            - containerPort: 9092
              name: kafka
---
apiVersion: v1
kind: Service
metadata:
  name: redpanda
  namespace: videoprocess
spec:
  selector:
    app: redpanda
  ports:
    - name: kafka
      port: 9092
      targetPort: kafka
```

Create `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/vp-feature-aggregator.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vp-feature-aggregator
  namespace: videoprocess
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vp-feature-aggregator
  template:
    metadata:
      labels:
        app: vp-feature-aggregator
    spec:
      containers:
        - name: vp-feature-aggregator
          image: vp-feature-aggregator:local
          ports:
            - name: http
              containerPort: 8080
          env:
            - name: AGG_KAFKA_BROKERS
              value: redpanda:9092
            - name: AGG_REDIS_URL
              value: redis://redis:6379/2
          readinessProbe:
            httpGet:
              path: /readyz
              port: http
---
apiVersion: v1
kind: Service
metadata:
  name: vp-feature-aggregator
  namespace: videoprocess
spec:
  selector:
    app: vp-feature-aggregator
  ports:
    - name: http
      port: 8080
      targetPort: http
```

Add these resources to `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess/kustomization.yaml`:

```yaml
  - redpanda.yaml
  - vp-feature-aggregator.yaml
```

- [ ] **Step 6: Run aggregator and k8s checks**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
python3 -m pytest -q
python3 -m ruff check . || true
docker build -f deploy/Dockerfile -t vp-feature-aggregator:local .

cd /home/taiwei/k8s-Constructure/k8s-constructure
kubectl kustomize videoprocess >/tmp/vp-kustomize.yaml
```

Expected: pytest PASS, Docker build succeeds, kustomize renders manifests.

- [ ] **Step 7: Commit aggregator and k8s work**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
git status --short
git add README.md app deploy tests
git commit -m "feat: add aggregator kafka consumer"

cd /home/taiwei/k8s-Constructure/k8s-constructure
git status --short
git add videoprocess/redpanda.yaml videoprocess/vp-feature-aggregator.yaml videoprocess/kustomization.yaml
git commit -m "feat: add videoprocess risk kafka services"
```

Expected: one aggregator commit and one k8s commit.

## Task 7: VideoProcess PDS Client

**Files:**
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/pds_client.py`
- Modify: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/config.py`
- Modify: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/clients.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/test_pds_client.py`

- [ ] **Step 1: Write PDS client tests**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/test_pds_client.py`:

```python
from __future__ import annotations

import httpx
import pytest

from app.pds_client import PDSClient, PDSDecision, PDSDecisionRequest


@pytest.mark.asyncio
async def test_pds_client_returns_block_decision():
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Client-Id"] == "videoprocess-channel-agent"
        return httpx.Response(
            200,
            json={
                "decision_id": "decision-1",
                "verdict": "block",
                "score": 0.8,
                "reasons": [{"code": "burst", "rule": "r1"}],
                "evaluated_rules": ["r1"],
                "rules_version": "sha256:test",
                "latency_ms": 3,
            },
        )

    client = PDSClient(
        base_url="http://pds",
        client_id="videoprocess-channel-agent",
        timeout_seconds=0.5,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    decision = await client.decide(
        PDSDecisionRequest(actor_id="actor-1", action_type="publish", platform="youtube")
    )

    assert decision.verdict == "block"
    assert decision.decision_id == "decision-1"
    assert decision.reasons[0]["code"] == "burst"


@pytest.mark.asyncio
async def test_pds_client_fails_open_on_500():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "down"})

    client = PDSClient(
        base_url="http://pds",
        client_id="videoprocess-channel-agent",
        timeout_seconds=0.5,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
    )

    decision = await client.decide(PDSDecisionRequest(actor_id="actor-1", action_type="publish"))

    assert decision.verdict == "allow"
    assert decision.metadata["warning"] == "pds_unavailable"
```

- [ ] **Step 2: Run client tests and verify they fail**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/test_pds_client.py -q
```

Expected: FAIL because `app.pds_client` does not exist.

- [ ] **Step 3: Implement PDS client**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/pds_client.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx


@dataclass(frozen=True)
class PDSDecisionRequest:
    actor_id: str
    action_type: str
    platform: str = ""
    content: dict[str, Any] = field(default_factory=dict)
    context: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PDSDecision:
    decision_id: str
    verdict: str
    score: float = 0.0
    reasons: list[dict[str, Any]] = field(default_factory=list)
    evaluated_rules: list[str] = field(default_factory=list)
    rules_version: str = ""
    latency_ms: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class PolicyDecisionClient(Protocol):
    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        ...


class NoopPDSClient:
    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        return PDSDecision(decision_id="", verdict="allow", metadata={"warning": "pds_disabled"})


class PDSClient:
    def __init__(
        self,
        *,
        base_url: str,
        client_id: str,
        timeout_seconds: float,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.timeout_seconds = timeout_seconds
        self._http_client = http_client

    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        payload = {
            "actor_id": request.actor_id,
            "action": {"type": request.action_type, "platform": request.platform},
            "content": request.content,
            "context": request.context,
        }
        try:
            if self._http_client is not None:
                response = await self._http_client.post(
                    f"{self.base_url}/v1/decide",
                    json=payload,
                    headers={"X-Client-Id": self.client_id},
                    timeout=self.timeout_seconds,
                )
            else:
                async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                    response = await client.post(
                        f"{self.base_url}/v1/decide",
                        json=payload,
                        headers={"X-Client-Id": self.client_id},
                    )
            if response.status_code >= 500:
                return _fail_open("pds_unavailable")
            response.raise_for_status()
            data = response.json()
            return PDSDecision(
                decision_id=str(data.get("decision_id") or ""),
                verdict=str(data.get("verdict") or "allow"),
                score=float(data.get("score") or 0.0),
                reasons=list(data.get("reasons") or []),
                evaluated_rules=list(data.get("evaluated_rules") or []),
                rules_version=str(data.get("rules_version") or ""),
                latency_ms=int(data.get("latency_ms") or 0),
                metadata=dict(data.get("metadata") or {}),
            )
        except Exception:
            return _fail_open("pds_unavailable")


def _fail_open(warning: str) -> PDSDecision:
    return PDSDecision(decision_id="", verdict="allow", metadata={"warning": warning})
```

- [ ] **Step 4: Add config values**

Add to `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/config.py`:

```python
    # Policy Decision Service
    pds_enabled: bool = False
    pds_base_url: str = "http://pds:8080"
    pds_client_id: str = "videoprocess-channel-agent"
    pds_timeout_seconds: float = 0.5

    # Risk event Kafka
    risk_kafka_brokers: str = "redpanda:9092"
    risk_vp_actions_topic: str = "vp.actor.actions.v1"
```

- [ ] **Step 5: Expose protocol in ChannelOps clients**

In `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/clients.py`, import and re-export the PDS protocol:

```python
from app.pds_client import NoopPDSClient, PDSDecision, PDSDecisionRequest, PolicyDecisionClient
```

- [ ] **Step 6: Run VP client tests**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/test_pds_client.py -q
python3 -m ruff check app/pds_client.py tests/test_pds_client.py || true
python3 -m mypy app || true
```

Expected: pytest PASS.

- [ ] **Step 7: Commit VP PDS client**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
git status --short
git add backend/app/config.py backend/app/pds_client.py backend/app/channel_agent/clients.py backend/tests/test_pds_client.py
git commit -m "feat: add fail-open pds client"
```

Expected: commit contains only VP PDS client and config wiring.

## Task 8: VideoProcess Event Outbox And Relay

**Files:**
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/__init__.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/schemas.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/outbox.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/producer.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/relay.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/event_outbox_relay.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/alembic/versions/013_event_outbox.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/events/test_outbox.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/events/test_relay.py`

- [ ] **Step 1: Check migration revision**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
ls alembic/versions
```

Expected: if `013_event_outbox.py` already exists or a newer ChannelOps migration was added, choose the next unused revision number and update all file names in this task before editing.

- [ ] **Step 2: Write outbox tests**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/events/test_outbox.py`:

```python
from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.events.outbox import EventOutbox


CREATE_TABLE = """
CREATE TABLE event_outbox (
  id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  key TEXT NOT NULL,
  payload JSON NOT NULL,
  created_at TEXT NOT NULL,
  delivered_at TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
)
"""


@pytest.mark.asyncio
async def test_outbox_writes_undelivered_event():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(text(CREATE_TABLE))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        outbox = EventOutbox()
        event_id = await outbox.enqueue(
            session,
            topic="vp.actor.actions.v1",
            key="actor-1",
            payload={"event_id": "event-1", "actor_id": "actor-1"},
        )
        await session.commit()
        rows = (await session.execute(text("SELECT id, topic, key, delivered_at FROM event_outbox"))).all()

    assert rows == [(event_id, "vp.actor.actions.v1", "actor-1", None)]
    await engine.dispose()
```

- [ ] **Step 3: Run outbox test and verify it fails**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/events/test_outbox.py -q
```

Expected: FAIL because `app.events.outbox` does not exist.

- [ ] **Step 4: Add migration**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/alembic/versions/013_event_outbox.py`:

```python
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "013_event_outbox"
down_revision = "012_channel_ops_hardening"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_outbox",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
    )
    op.create_index("ix_event_outbox_undelivered", "event_outbox", ["delivered_at", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_event_outbox_undelivered", table_name="event_outbox")
    op.drop_table("event_outbox")
```

- [ ] **Step 5: Implement event schemas**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/schemas.py`:

```python
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any


TOPIC_VP_ACTIONS = "vp.actor.actions.v1"


def build_actor_action_event(
    *,
    actor_id: str,
    action_type: str,
    platform: str = "",
    metadata: dict[str, Any] | None = None,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    occurred_at = occurred_at or datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "topic_version": TOPIC_VP_ACTIONS,
        "actor_id": actor_id,
        "action_type": action_type,
        "platform": platform,
        "occurred_at": occurred_at.isoformat(),
        "source": "videoprocess.channel_ops",
        "metadata": dict(metadata or {}),
    }
```

- [ ] **Step 6: Implement outbox writer**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/outbox.py`:

```python
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession


event_outbox_table = sa.Table(
    "event_outbox",
    sa.MetaData(),
    sa.Column("id", sa.String(length=64), primary_key=True),
    sa.Column("topic", sa.String(length=255), nullable=False),
    sa.Column("key", sa.String(length=255), nullable=False),
    sa.Column("payload", sa.JSON(), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("attempt_count", sa.Integer(), nullable=False),
    sa.Column("last_error", sa.Text(), nullable=True),
)


class EventOutbox:
    async def enqueue(
        self,
        db: AsyncSession,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
    ) -> str:
        event_id = str(payload.get("event_id") or uuid.uuid4())
        await db.execute(
            sa.insert(event_outbox_table).values(
                id=event_id,
                topic=topic,
                key=key,
                payload=payload,
                created_at=datetime.now(timezone.utc),
                attempt_count=0,
            )
        )
        return event_id
```

- [ ] **Step 7: Write relay tests**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/events/test_relay.py`:

```python
from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.events.relay import EventOutboxRelay


CREATE_TABLE = """
CREATE TABLE event_outbox (
  id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  key TEXT NOT NULL,
  payload JSON NOT NULL,
  created_at TEXT NOT NULL,
  delivered_at TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
)
"""


class RecordingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict]] = []

    async def send(self, *, topic: str, key: str, payload: dict) -> None:
        self.sent.append((topic, key, payload))


@pytest.mark.asyncio
async def test_relay_marks_event_delivered_after_send():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(text(CREATE_TABLE))
        await conn.execute(
            text(
                "INSERT INTO event_outbox (id, topic, key, payload, created_at) VALUES ('event-1', 'vp.actor.actions.v1', 'actor-1', '{\"event_id\":\"event-1\"}', '2026-05-19T00:00:00Z')"
            )
        )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    producer = RecordingProducer()
    async with session_factory() as session:
        relay = EventOutboxRelay(producer=producer)
        delivered = await relay.run_once(session, batch_size=10)
        await session.commit()
        row = (
            await session.execute(text("SELECT delivered_at, attempt_count FROM event_outbox WHERE id = 'event-1'"))
        ).one()

    assert delivered == 1
    assert producer.sent[0][0] == "vp.actor.actions.v1"
    assert row.delivered_at is not None
    assert row.attempt_count == 1
    await engine.dispose()
```

- [ ] **Step 8: Implement producer and relay**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/producer.py`:

```python
from __future__ import annotations

import json
from typing import Any, Protocol

from aiokafka import AIOKafkaProducer


class EventProducer(Protocol):
    async def send(self, *, topic: str, key: str, payload: dict[str, Any]) -> None:
        ...


class KafkaEventProducer:
    def __init__(self, *, brokers: str) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=brokers.split(","))

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, *, topic: str, key: str, payload: dict[str, Any]) -> None:
        await self._producer.send_and_wait(
            topic,
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            key=key.encode("utf-8"),
        )
```

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/events/relay.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.events.outbox import event_outbox_table
from app.events.producer import EventProducer


class EventOutboxRelay:
    def __init__(self, *, producer: EventProducer) -> None:
        self.producer = producer

    async def run_once(self, db: AsyncSession, *, batch_size: int = 100) -> int:
        rows = (
            await db.execute(
                sa.select(
                    event_outbox_table.c.id,
                    event_outbox_table.c.topic,
                    event_outbox_table.c.key,
                    event_outbox_table.c.payload,
                )
                .where(event_outbox_table.c.delivered_at.is_(None))
                .order_by(event_outbox_table.c.created_at.asc())
                .limit(batch_size)
            )
        ).mappings().all()

        delivered = 0
        for row in rows:
            try:
                await self.producer.send(topic=row["topic"], key=row["key"], payload=dict(row["payload"]))
                await db.execute(
                    sa.update(event_outbox_table)
                    .where(event_outbox_table.c.id == row["id"])
                    .values(
                        delivered_at=datetime.now(timezone.utc),
                        attempt_count=event_outbox_table.c.attempt_count + 1,
                        last_error=None,
                    )
                )
                delivered += 1
            except Exception as exc:
                await db.execute(
                    sa.update(event_outbox_table)
                    .where(event_outbox_table.c.id == row["id"])
                    .values(
                        attempt_count=event_outbox_table.c.attempt_count + 1,
                        last_error=str(exc),
                    )
                )
        return delivered
```

- [ ] **Step 9: Add relay entry point**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/event_outbox_relay.py`:

```python
from __future__ import annotations

import asyncio

from app.config import settings
from app.db import async_session
from app.events.producer import KafkaEventProducer
from app.events.relay import EventOutboxRelay


async def run_forever() -> None:
    producer = KafkaEventProducer(brokers=settings.risk_kafka_brokers)
    await producer.start()
    try:
        relay = EventOutboxRelay(producer=producer)
        while True:
            async with async_session() as db:
                await relay.run_once(db)
                await db.commit()
            await asyncio.sleep(1.0)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(run_forever())
```

- [ ] **Step 10: Run event tests**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/events/test_outbox.py tests/events/test_relay.py -q
python3 -m ruff check app/events event_outbox_relay.py tests/events || true
```

Expected: pytest PASS.

- [ ] **Step 11: Commit VP outbox**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
git status --short
git add backend/app/events backend/event_outbox_relay.py backend/alembic/versions/013_event_outbox.py backend/tests/events
git commit -m "feat: add risk event outbox relay"
```

Expected: commit contains outbox, relay, migration, and tests.

## Task 9: VideoProcess ChannelOps PDS Gates And Compose Override

**Files:**
- Modify: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/service.py`
- Modify: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/runner.py`
- Modify: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/channel_agent/test_service.py`
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/docker-compose.pds-kafka.yml`

- [ ] **Step 1: Add fake PDS helper to ChannelOps tests**

In `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/channel_agent/test_service.py`, add:

```python
from app.pds_client import PDSDecision, PDSDecisionRequest


class FakePDSClient:
    def __init__(self, decision: PDSDecision | None = None) -> None:
        self.decision = decision or PDSDecision(decision_id="decision-allow", verdict="allow")
        self.requests: list[PDSDecisionRequest] = []

    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        self.requests.append(request)
        return self.decision
```

Update the `_service` test helper signature:

```python
def _service(*, clock=None, autoflow=None, youtube=None, minimax=None, pds=None, event_outbox=None) -> ChannelAgentService:
    clock = clock or FakeClock(datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc))
    queue = ChannelOpsQueueService(clock=clock)
    return ChannelAgentService(
        queue=queue,
        clock=clock,
        autoflow_client=autoflow or FakeAutoFlowClient(),
        youtube_client=youtube or FakeYouTubeClient(),
        minimax_client=minimax or FakeMiniMaxClient(),
        pds_client=pds,
        event_outbox=event_outbox,
    )
```

- [ ] **Step 2: Write candidate PDS block test**

Add to `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/tests/channel_agent/test_service.py`:

```python
@pytest.mark.asyncio
async def test_tick_rejects_candidate_when_pds_blocks(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    seed = ManualSeed(channel_profile_id=channel.id, topic_lane_id=lane.id, title="risky", status="active")
    service_session.add(seed)
    await service_session.commit()
    pds = FakePDSClient(
        PDSDecision(
            decision_id="decision-block",
            verdict="block",
            reasons=[{"code": "publishing_burst", "rule": "burst_publish_feature_flag"}],
        )
    )

    audit = await _service(pds=pds).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.tasks_rejected == 1
    assert pds.requests
    assert audit.guards_triggered_json[0]["guard"] == "pds_blocked"
```

- [ ] **Step 3: Write promotion PDS block test**

Add:

```python
@pytest.mark.asyncio
async def test_promote_publication_holds_when_pds_blocks(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="publish risky video",
        state=TASK_UPLOADED_PRIVATE,
    )
    service_session.add(task)
    await service_session.flush()
    publication = _publication_for_task(task, account, publish_status="uploaded", title="risky")
    service_session.add(publication)
    await service_session.flush()
    item = ChannelOpsQueueItem(
        kind="promote_publication",
        payload_json={"publication_id": str(publication.id), "target_visibility": "unlisted"},
    )
    service_session.add(item)
    await service_session.commit()
    pds = FakePDSClient(PDSDecision(decision_id="decision-block", verdict="block", reasons=[{"code": "burst"}]))

    result = await _service(pds=pds).handle_promote_publication(service_session, item)

    assert result.publish_status == "held"
    assert "pds_blocked:decision-block" in list(result.warnings_json or [])
```

- [ ] **Step 4: Run ChannelOps tests and verify they fail**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/channel_agent/test_service.py -q
```

Expected: FAIL because `ChannelAgentService` does not accept PDS or outbox dependencies yet.

- [ ] **Step 5: Inject PDS client and outbox into service**

In `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/service.py`, add imports:

```python
from app.events.outbox import EventOutbox
from app.events.schemas import TOPIC_VP_ACTIONS, build_actor_action_event
from app.pds_client import NoopPDSClient, PDSDecision, PDSDecisionRequest, PolicyDecisionClient
```

Update `__init__`:

```python
        pds_client: PolicyDecisionClient | None = None,
        event_outbox: EventOutbox | None = None,
```

Set fields:

```python
        self.pds_client = pds_client or NoopPDSClient()
        self.event_outbox = event_outbox or EventOutbox()
```

- [ ] **Step 6: Add PDS candidate guard helper**

Add to `ChannelAgentService`:

```python
    async def _pds_candidate_guard(self, candidate: dict[str, Any]) -> dict[str, Any] | None:
        account = candidate.get("account")
        if account is None:
            return None
        lane = candidate.get("lane")
        decision = await self.pds_client.decide(
            PDSDecisionRequest(
                actor_id=str(account.id),
                action_type="candidate_accept",
                platform=str(getattr(account, "platform", "") or "youtube"),
                content={
                    "title": str(candidate.get("title") or candidate.get("prompt") or ""),
                    "description": str(candidate.get("prompt") or ""),
                },
                context={
                    "lane_id": str(lane.id) if lane is not None else "",
                    "candidate_id": str(candidate.get("candidate_id") or ""),
                },
            )
        )
        if decision.verdict == "block":
            return _candidate_rejection(
                candidate,
                guard="pds_blocked",
                reason=f"PDS blocked candidate: {decision.decision_id}",
            )
        if decision.verdict == "flag":
            return _candidate_rejection(
                candidate,
                guard="pds_flagged_for_review",
                reason=f"PDS flagged candidate: {decision.decision_id}",
            )
        return None
```

Call this helper in `_evaluate_candidate_guards` after local account/concurrency checks and before lane cadence:

```python
        pds_rejection = await self._pds_candidate_guard(candidate)
        if pds_rejection is not None:
            return pds_rejection
```

- [ ] **Step 7: Add promotion PDS gate**

In `handle_promote_publication`, before `youtube_client.schedule_publish`, call:

```python
        decision = await self.pds_client.decide(
            PDSDecisionRequest(
                actor_id=str(publication.account_id),
                action_type="publish",
                platform=publication.platform,
                content={"title": publication.title},
                context={"publication_id": str(publication.id), "task_id": str(task.id)},
            )
        )
        if decision.verdict in {"block", "flag"}:
            publication.publish_status = "held"
            previous_state = task.state
            task.state = TASK_HELD
            task.state_updated_at = self.clock.now()
            task.transition_history_json = [
                *list(task.transition_history_json or []),
                _transition(previous_state, TASK_HELD, "pds_gate", self.clock.now()),
            ]
            marker = "pds_blocked" if decision.verdict == "block" else "pds_flagged_for_review"
            publication.warnings_json = [
                *list(publication.warnings_json or []),
                f"{marker}:{decision.decision_id}",
            ]
            await db.commit()
            await db.refresh(publication)
            return publication
```

- [ ] **Step 8: Emit VP outbox events**

After candidate acceptance, enqueue:

```python
payload = build_actor_action_event(
    actor_id=str(candidate["account"].id),
    action_type="candidate_accepted",
    platform=str(getattr(candidate["account"], "platform", "") or "youtube"),
    metadata={"lane_id": str(candidate["lane"].id) if candidate.get("lane") is not None else ""},
)
await self.event_outbox.enqueue(db, topic=TOPIC_VP_ACTIONS, key=payload["actor_id"], payload=payload)
```

For PDS candidate block or flag, use `candidate_blocked` or `candidate_flagged`. For promotion, emit `publication_promotion_attempted`, `publication_promotion_blocked`, and `publication_scheduled` at the corresponding state transitions.

- [ ] **Step 9: Wire runner**

In `/home/taiwei/.codex/worktrees/d1d5/videoprocess/backend/app/channel_agent/runner.py`, build the client:

```python
from app.config import settings
from app.pds_client import NoopPDSClient, PDSClient


def _build_pds_client():
    if not settings.pds_enabled:
        return NoopPDSClient()
    return PDSClient(
        base_url=settings.pds_base_url,
        client_id=settings.pds_client_id,
        timeout_seconds=settings.pds_timeout_seconds,
    )
```

Pass `pds_client=_build_pds_client()` into `ChannelAgentService`.

- [ ] **Step 10: Add compose override**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/docker-compose.pds-kafka.yml`:

```yaml
services:
  redpanda:
    image: redpandadata/redpanda:v24.3.5
    command:
      - redpanda
      - start
      - --overprovisioned
      - --smp=1
      - --memory=512M
      - --reserve-memory=0M
      - --node-id=0
      - --check=false
      - --kafka-addr=0.0.0.0:9092
      - --advertise-kafka-addr=redpanda:9092
    ports:
      - "${VP_REDPANDA_PORT:-19092}:9092"
    networks:
      - vp_internal

  pds:
    build:
      context: /home/taiwei/Constructure-repos/policy-decision-service
      dockerfile: deploy/Dockerfile
    environment:
      PDS_HTTP_ADDR: ":8080"
      PDS_DATABASE_URL: ${PDS_DATABASE_URL:-postgres://vp:${VP_POSTGRES_PASSWORD:-vp_secret}@host.docker.internal:5435/videoprocess?sslmode=disable}
      PDS_REDIS_URL: ${PDS_REDIS_URL:-redis://host.docker.internal:6380/1}
      PDS_FEATURE_PROVIDER_URL: http://vp-feature-aggregator:8080
      PDS_KAFKA_ENABLED: "true"
      PDS_KAFKA_BROKERS: redpanda:9092
    depends_on:
      - redpanda
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - vp_internal

  vp-feature-aggregator:
    build:
      context: /home/taiwei/Constructure-repos/vp-feature-aggregator
      dockerfile: deploy/Dockerfile
    environment:
      AGG_KAFKA_BROKERS: redpanda:9092
      AGG_REDIS_URL: ${VP_REDIS_URL:-redis://host.docker.internal:6380/2}
      AGG_DATABASE_URL: ${AGG_DATABASE_URL:-postgresql://vp:${VP_POSTGRES_PASSWORD:-vp_secret}@host.docker.internal:5435/videoprocess}
    depends_on:
      - redpanda
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - vp_internal

  event-outbox-relay:
    build:
      context: ./backend
      dockerfile: Dockerfile.api
    command: ["python", "event_outbox_relay.py"]
    environment:
      DATABASE_URL: ${VP_DATABASE_URL:-postgresql+asyncpg://vp:${VP_POSTGRES_PASSWORD:-vp_secret}@host.docker.internal:5435/videoprocess}
      RISK_KAFKA_BROKERS: redpanda:9092
      RISK_VP_ACTIONS_TOPIC: vp.actor.actions.v1
    depends_on:
      - redpanda
    extra_hosts:
      - "host.docker.internal:host-gateway"
    networks:
      - vp_internal

  channel-agent-runner:
    environment:
      PDS_ENABLED: "true"
      PDS_BASE_URL: http://pds:8080
      PDS_CLIENT_ID: videoprocess-channel-agent
```

- [ ] **Step 11: Run VP checks**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/test_pds_client.py tests/events/test_outbox.py tests/events/test_relay.py tests/channel_agent/test_service.py -q
python3 -m ruff check app/pds_client.py app/events app/channel_agent tests/test_pds_client.py tests/events tests/channel_agent/test_service.py || true
python3 -m mypy app || true

cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml config >/tmp/vp-pds-kafka-compose.yaml
```

Expected: pytest PASS, compose config renders.

- [ ] **Step 12: Commit VP ChannelOps integration**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
git status --short
git add backend/app/channel_agent/service.py backend/app/channel_agent/runner.py backend/tests/channel_agent/test_service.py docker-compose.pds-kafka.yml
git commit -m "feat: gate channel ops with pds"
```

Expected: commit contains ChannelOps gate behavior and compose override.

## Task 10: End-To-End Smoke And Final Verification

**Files:**
- Create: `/home/taiwei/.codex/worktrees/d1d5/videoprocess/docs/pds-kafka-smoke.md`
- Modify: `/home/taiwei/Constructure-repos/policy-decision-service/README.md`
- Modify: `/home/taiwei/Constructure-repos/vp-feature-aggregator/README.md`

- [ ] **Step 1: Write smoke document**

Create `/home/taiwei/.codex/worktrees/d1d5/videoprocess/docs/pds-kafka-smoke.md`:

````markdown
# PDS Kafka Smoke

Run from `/home/taiwei/.codex/worktrees/d1d5/videoprocess`.

```bash
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml up -d --build redpanda pds vp-feature-aggregator event-outbox-relay
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/readyz || true
curl -fsS http://localhost:8080/v1/decide \
  -H 'Content-Type: application/json' \
  -H 'X-Client-Id: videoprocess-channel-agent' \
  -d '{"actor_id":"smoke-actor","action":{"type":"publish","platform":"youtube"},"content":{"title":"smoke"},"context":{}}'
curl -fsS http://localhost:8080/metrics | head
```

The full workflow is proven when a ChannelOps event writes `event_outbox`, the relay marks it delivered, the aggregator returns non-zero features for the actor, and PDS returns a decision with a stable `decision_id`.
````

- [ ] **Step 2: Run PDS full tests**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go test ./... -count=1
go build ./cmd/server
git status --short
```

Expected: tests PASS, build PASS, only expected README/doc changes remain.

- [ ] **Step 3: Run aggregator full tests**

Run:

```bash
cd /home/taiwei/Constructure-repos/vp-feature-aggregator
python3 -m pytest -q
python3 -m ruff check . || true
docker build -f deploy/Dockerfile -t vp-feature-aggregator:local .
git status --short
```

Expected: pytest PASS, Docker build PASS, only expected README/doc changes remain.

- [ ] **Step 4: Run VideoProcess backend checks**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: pytest PASS. Ruff and mypy may report existing warnings because AGENTS marks them non-blocking with `|| true`; capture any new PDS/Kafka-specific errors and fix them before final commit.

- [ ] **Step 5: Run compose smoke**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml config >/tmp/vp-pds-kafka-compose.yaml
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml up -d --build redpanda pds vp-feature-aggregator event-outbox-relay
curl -fsS http://localhost:18080/healthz || true
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml logs --tail=80 pds vp-feature-aggregator event-outbox-relay
```

Expected: compose services build and start. If the API health URL differs in the current compose configuration, use the port rendered in `/tmp/vp-pds-kafka-compose.yaml`.

- [ ] **Step 6: Commit docs and verification updates**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
git status --short
git add docs/pds-kafka-smoke.md
git commit -m "docs: add pds kafka smoke runbook"

cd /home/taiwei/Constructure-repos/policy-decision-service
git status --short
git add README.md
git commit -m "docs: update pds kafka operations"

cd /home/taiwei/Constructure-repos/vp-feature-aggregator
git status --short
git add README.md
git commit -m "docs: add aggregator operations"
```

Expected: all three repos have the final doc commits, unless a README had no changes.

## Final Completion Checklist

- [ ] PDS `go test ./... -count=1` passes.
- [ ] PDS `go build ./cmd/server` passes.
- [ ] Aggregator `python3 -m pytest -q` passes.
- [ ] Aggregator Docker image builds.
- [ ] VP `cd backend && python3 -m pytest` passes.
- [ ] VP `cd backend && python3 -m ruff check . || true` was run.
- [ ] VP `cd backend && python3 -m mypy app || true` was run.
- [ ] Compose override renders with `docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml config`.
- [ ] Local compose smoke attempted and logs captured.
- [ ] PDS, aggregator, VP, and k8s repos each have clean `git status --short` or only explicitly reported unrelated user changes.
