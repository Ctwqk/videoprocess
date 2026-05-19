# Policy Decision Service Go Risk Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-style Go Policy Decision Service (PDS) as a new sibling repo and integrate it into VideoProcess as a fail-open pre-flight risk gate before platform publication actions.

**Architecture:** Create `/home/taiwei/Constructure-repos/policy-decision-service` as a single Go binary with thin HTTP and gRPC transports over one deterministic rule engine. Store mutable state in Redis/Postgres, load rules from YAML, persist every decision audit row, expose Prometheus metrics, and keep VP integration narrow through a typed Python client plus channel-agent pre-flight calls.

**Tech Stack:** Go 1.24.4 locally, chi, grpc-go, pgxpool, go-redis, zerolog, prometheus/client_golang, yaml.v3, cel-go, Aho-Corasick, golang-migrate, pytest/httpx for VP integration tests, Docker Compose, Kubernetes manifests, k6 for load testing.

---

## Grounding Notes

- Source spec: `/home/taiwei/code/job-prep/PDS_PLAN.md`.
- Phase rule from Taiwei: one day equals one phase.
- Current VP worktree: `/home/taiwei/.codex/worktrees/d1d5/videoprocess`.
- Canonical VP repo: `/home/taiwei/Constructure-repos/videoprocess`; current worktree origin is `git@github.com:Ctwqk/videoprocess.git`.
- PDS target repo does not exist yet: `/home/taiwei/Constructure-repos/policy-decision-service`.
- Local tool check on 2026-05-19: `go version go1.24.4 linux/amd64`, `kubectl` client exists, `protoc` and `k6` are not installed.
- Current worktree is detached HEAD. Before implementation, create or switch to a named branch for the VP changes, for example `codex/pds-go-risk-gate`.

## File Map

### New PDS Repo

- Create `/home/taiwei/Constructure-repos/policy-decision-service/go.mod`: Go module metadata.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/Makefile`: local build, test, lint, migrate, run, and load-test commands.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/cmd/server/main.go`: config load, dependencies, signal handling, HTTP/gRPC startup.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/config/config.go`: env-based config and defaults.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/http.go`: chi router and HTTP endpoints.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/grpc.go`: grpc-go service adapter.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/api/middleware.go`: request id, panic recovery, logging, metrics.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/types.go`: request, response, verdict, reason, and rule result types.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/engine.go`: `Evaluate(ctx, req)`.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/engine/combine.go`: `block > flag > allow` precedence and reason aggregation.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/loader.go`: YAML loading, hashing, validation, atomic snapshot construction.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/rule.go`: common rule interfaces and eval result.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/rate_limit.go`: Redis-backed sliding-window rule.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/keyword.go`: keyword rule with automaton built at load time.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/cel.go`: compiled CEL expression rule.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/rules/combiner.go`: topological combiner rule.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/store/postgres.go`: pgxpool setup and health check.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/store/redis.go`: go-redis setup, Lua scripts, health check.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/store/audit.go`: async bounded audit writer.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/profile/cache.go`: actor profile read-through cache backed by Postgres.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/telemetry/metrics.go`: Prometheus collectors.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/internal/telemetry/logging.go`: zerolog setup.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/proto/pds/v1/pds.proto`: gRPC API.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/buf.yaml` and `buf.gen.yaml`: proto generation without requiring system `protoc`.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/migrations/0001_init.up.sql`: `pds.decisions` and `pds.actor_profile_cache`.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/migrations/0001_init.down.sql`: drop PDS schema objects.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/config/rules.example.yaml`: runnable sample policy.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/config/blocklist.example.txt`: keyword fixture.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/config/server.example.env`: local env template.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/deploy/Dockerfile`: static multi-stage container build.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/deploy/kubernetes.yaml`: ConfigMap, Deployment, Service, ServiceMonitor.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/deploy/grafana-dashboard.json`: PDS dashboard panels.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/tests/load/decide.js`: k6 load script.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/tests/load/README.md`: load-test instructions.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/tests/load/RESULTS.md`: measured results, filled only after running k6.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/README.md`: architecture, quickstart, operational notes.
- Create `/home/taiwei/Constructure-repos/policy-decision-service/AGENTS.md`: agent onboarding and required checks for the new repo.

### VideoProcess Integration

- Create `backend/app/pds_client.py`: fail-open async HTTP PDS client and decision models.
- Modify `backend/app/config.py`: add `pds_enabled`, `pds_base_url`, `pds_client_id`, and timeout settings.
- Modify `backend/app/channel_agent/clients.py`: add a `PolicyDecisionClient` protocol and fake client for tests.
- Modify `backend/app/channel_agent/service.py`: inject PDS client, call it during candidate guard evaluation and before promotion, persist decision ids in guard summaries.
- Modify `backend/app/channel_agent/runner.py`: wire real `PDSClient` when enabled.
- Modify `docker-compose.yml`: add `pds` service and PDS env vars for `api`, `channel-agent-runner`, and workers where needed.
- Create `backend/tests/test_pds_client.py`: client timeout, block, flag, allow, and malformed-response tests.
- Modify `backend/tests/channel_agent/test_service.py`: PDS block/flag/fail-open scenarios.
- Optionally modify `docs/constructure/services/videoprocess.md` or `/home/taiwei/Constructure-repos/constructure-runtime/docs/services/videoprocess.md`: document the new sibling service once deployment is real.

## Phase 0: Execution Prep

Do this immediately before Phase 1 if implementing from this worktree.

- [ ] **Step 1: Create a named VP branch**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
git switch -c codex/pds-go-risk-gate
```

Expected: worktree is on `codex/pds-go-risk-gate`, not detached HEAD.

- [ ] **Step 2: Create the sibling PDS repo directory**

Run:

```bash
mkdir -p /home/taiwei/Constructure-repos/policy-decision-service
cd /home/taiwei/Constructure-repos/policy-decision-service
git init
```

Expected: empty git repo initialized for PDS.

- [ ] **Step 3: Decide proto generation path**

Use `buf` so the plan does not depend on the currently missing system `protoc`.

Run:

```bash
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Expected: `buf`, `protoc-gen-go`, and `protoc-gen-go-grpc` are available on `PATH` or under `$(go env GOPATH)/bin`.

## Phase 1: Skeleton And Plumbing

**Day goal:** PDS is a runnable Go HTTP service with config, health/readiness/metrics, Postgres and Redis health checks, and an always-allow `POST /v1/decide`.

**Files:**
- Create the PDS repo skeleton listed above through `internal/engine`.
- Test in the PDS repo with `go test ./...`.

- [ ] **Step 1: Write the first HTTP contract tests**

Create tests for:

- `GET /healthz` returns 200 without dependencies.
- `GET /readyz` returns 503 when rules or dependencies are unavailable.
- `POST /v1/decide` validates `X-Client-Id`.
- A valid decide request returns `verdict=allow`, one `decision_id`, `rules_version`, `latency_ms`, and an empty reason list.

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go test ./...
```

Expected: FAIL because packages and handlers do not exist yet.

- [ ] **Step 2: Initialize module and dependency shell**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go mod init github.com/Ctwqk/policy-decision-service
go get github.com/go-chi/chi/v5 github.com/rs/zerolog github.com/prometheus/client_golang
go get github.com/jackc/pgx/v5/pgxpool github.com/redis/go-redis/v9 gopkg.in/yaml.v3
go get github.com/stretchr/testify
```

Expected: `go.mod` and `go.sum` are created.

- [ ] **Step 3: Implement config and telemetry basics**

Implement `internal/config.Config` with these env defaults:

```text
PDS_HTTP_ADDR=:8080
PDS_GRPC_ADDR=:9090
PDS_METRICS_ADDR=:8081
PDS_DATABASE_URL=postgres://vp:vp_secret@localhost:5435/videoprocess?sslmode=disable
PDS_REDIS_URL=redis://localhost:6380/1
PDS_RULES_PATH=config/rules.example.yaml
PDS_BLOCKLIST_PATH=config/blocklist.example.txt
PDS_AUDIT_QUEUE_SIZE=10000
PDS_AUDIT_BATCH_SIZE=100
PDS_FAIL_OPEN=true
```

Keep this service fail-open by default.

- [ ] **Step 4: Add engine request/response models**

Define typed Go structs that match the spec:

```go
type Verdict string

const (
    VerdictAllow Verdict = "allow"
    VerdictFlag  Verdict = "flag"
    VerdictBlock Verdict = "block"
)

type DecideRequest struct {
    ActorID string         `json:"actor_id"`
    Action  ActionContext  `json:"action"`
    Content ContentContext `json:"content"`
    Context map[string]any `json:"context"`
}

type DecideResponse struct {
    DecisionID     string   `json:"decision_id"`
    Verdict        Verdict  `json:"verdict"`
    Score          float64  `json:"score"`
    Reasons        []Reason `json:"reasons"`
    EvaluatedRules []string `json:"evaluated_rules"`
    RulesVersion   string   `json:"rules_version"`
    LatencyMS      int64    `json:"latency_ms"`
}
```

- [ ] **Step 5: Implement always-allow engine**

`engine.Evaluate(ctx, req)` should generate a UUID decision id, set verdict `allow`, include `rules_version="bootstrap"`, and return no reasons.

- [ ] **Step 6: Implement chi routes**

Implement:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `POST /v1/decide`

Return `400` for malformed JSON and `400` for missing `actor_id`, `action.type`, or `X-Client-Id`.

- [ ] **Step 7: Add Postgres/Redis wrappers**

Add `store.NewPostgres(ctx, cfg)` and `store.NewRedis(ctx, cfg)`. Phase 1 only needs health pings and readiness wiring; no writes yet.

- [ ] **Step 8: Add Dockerfile and Makefile**

`Makefile` commands:

```makefile
.PHONY: test build run docker-build fmt
test:
	go test ./...
build:
	CGO_ENABLED=0 go build -o bin/pds ./cmd/server
run:
	go run ./cmd/server
docker-build:
	docker build -f deploy/Dockerfile -t policy-decision-service:local .
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './proto/gen/*')
```

- [ ] **Step 9: Verify end-of-day demo**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
make test
make build
go run ./cmd/server
curl -sS -H 'X-Client-Id: vp-local' \
  -H 'Content-Type: application/json' \
  -d '{"actor_id":"channel_demo","action":{"type":"publish_video","platform":"youtube"},"content":{"title":"demo","duration_s":30,"tags":[]},"context":{"session_id":"local"}}' \
  http://127.0.0.1:8080/v1/decide
```

Expected: JSON response has `verdict:"allow"` and a non-empty `decision_id`.

- [ ] **Step 10: Commit Phase 1**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
git add .
git commit -m "feat: scaffold policy decision service"
```

## Phase 2: Rule Engine Core And Audit Persistence

**Day goal:** PDS loads YAML rules, evaluates `rate_limit` and `keyword_match`, combines verdicts, writes decision audit rows, and has table-driven unit tests.

**Files:**
- Create/modify `internal/rules/*`, `internal/engine/*`, `internal/store/*`, `migrations/*`, `config/*`.
- Add unit tests beside each package.

- [ ] **Step 1: Write loader and validation tests**

Cover:

- unknown rule type fails at load time;
- duplicate rule ids fail;
- combiner references to missing rules fail;
- disabled rules do not evaluate;
- rules hash changes when YAML changes.

Run: `go test ./internal/rules -run TestLoader -v`

Expected: FAIL before loader exists.

- [ ] **Step 2: Implement YAML rule schema**

Support the v1 schema from `PDS_PLAN.md`:

- `version`
- `defaults.on_eval_error`
- `rules[].id`
- `rules[].type`
- `rules[].enabled`
- type-specific fields
- `on_exceed` and `on_match`

Do all validation at load time.

- [ ] **Step 3: Write engine precedence tests**

Cases:

- no matches -> allow;
- one flag -> flag;
- one block -> block;
- block plus flag -> block with all reasons;
- per-rule error -> allow for that rule and increments error metric.

Run: `go test ./internal/engine -run TestCombine -v`

Expected: FAIL until combiner exists.

- [ ] **Step 4: Implement `Rule` interface and result types**

Use one common interface:

```go
type Rule interface {
    ID() string
    Evaluate(ctx context.Context, req engine.DecideRequest, state EvalState) (Result, error)
}
```

`EvalState` should carry Redis, actor profile, sub-rule results, and request-scoped cache values.

- [ ] **Step 5: Implement Redis rate limit rule**

Use two adjacent fixed windows for sliding-window approximation. Use a Lua script so increment, TTL, and reads are atomic.

Tests must cover:

- first `limit` requests allow;
- request `limit+1` returns configured verdict and code;
- actor+action scope isolates different action types;
- Redis error fail-opens at rule level and increments error metric.

- [ ] **Step 6: Implement keyword match rule**

Build automaton at rule load. Tests must cover:

- title match blocks;
- description match can be configured separately;
- empty keyword file loads as no-op;
- keyword file missing fails startup for enabled keyword rules.

- [ ] **Step 7: Add Postgres migrations**

Create `migrations/0001_init.up.sql` and `down.sql` for:

- schema `pds`;
- table `pds.decisions`;
- indexes on `(actor_id, ts desc)`, `(verdict, ts desc)`, `(action_type, ts desc)`;
- table `pds.actor_profile_cache`.

Use the exact schema from the source plan unless implementation finds an incompatibility.

- [ ] **Step 8: Implement bounded async audit writer**

Behavior:

- every successful engine decision enqueues one audit write;
- writer batches by count or timer;
- if queue is full, drop the audit row and increment `pds_postgres_writes_total{result="dropped"}`;
- request path must not block on slow audit writes.

- [ ] **Step 9: Add integration smoke with real Postgres and Redis**

Prefer Docker Compose for local dependency smoke first. If `testcontainers-go` is added, keep it behind an integration tag so normal `go test ./...` stays fast.

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go test ./... -v
```

Expected: unit tests pass without requiring live containers; integration tests are opt-in.

- [ ] **Step 10: Verify end-of-day demo**

Run PDS with sample rules and real Redis/Postgres. Submit:

- 10 `publish_video` requests for one actor -> allow;
- 11th request -> block with `daily_publish_quota_exceeded`;
- a title containing a sample blocklist keyword -> block with `title_blocked_keyword`;
- query `pds.decisions` and confirm rows include reasons and evaluated rules.

- [ ] **Step 11: Commit Phase 2**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
git add .
git commit -m "feat: add rule engine and audit storage"
```

## Phase 3: CEL, Combiners, Hot Reload, Observability, And gRPC

**Day goal:** PDS supports CEL rules, combiner rules, hot reload, Prometheus metrics on all paths, gRPC parity, and actor profile lookup.

**Files:**
- Modify `internal/rules/cel.go`, `internal/rules/combiner.go`, `internal/profile/cache.go`, `internal/api/grpc.go`, `proto/*`, `internal/telemetry/*`.
- Add tests for CEL, combiner, reload, gRPC, and metrics.

- [ ] **Step 1: Write CEL rule tests**

Cover:

- `actor.age_days < 7 && action.type == "publish_video"` flags new actors;
- missing actor profile fields evaluate as zero values only when documented;
- CEL compile errors fail at rule load;
- CEL runtime errors fail-open per rule and increment error metrics.

Run: `go test ./internal/rules -run TestCEL -v`

Expected: FAIL before CEL implementation.

- [ ] **Step 2: Implement actor profile cache**

Load `age_days`, `flags_24h`, and `blocks_7d` from `pds.actor_profile_cache` once per request. Cache the request-scoped profile in `EvalState`.

- [ ] **Step 3: Implement CEL rules with compiled programs**

Expose typed maps:

- `actor`
- `action`
- `content`
- `context`

Compile once on load; never compile per request.

- [ ] **Step 4: Write combiner tests**

Cover:

- `all` matches only when every referenced sub-rule matched;
- `any` matches when at least one referenced sub-rule matched;
- cycle detection fails load;
- combiner reuses sub-results and does not re-run referenced rules.

Run: `go test ./internal/rules -run TestCombiner -v`

Expected: FAIL before combiner implementation.

- [ ] **Step 5: Implement combiner topological validation**

At load time, sort dependency order and reject cycles. At eval time, read sub-rule results from the same request.

- [ ] **Step 6: Wire Prometheus collectors**

Expose these metrics:

- `pds_decisions_total{verdict,action_type,client}`
- `pds_decision_latency_seconds{action_type}`
- `pds_rule_evaluations_total{rule_id,matched}`
- `pds_rule_eval_errors_total{rule_id,error_type}`
- `pds_rules_loaded`
- `pds_rules_reload_total{result}`
- `pds_redis_ops_total{op,result}`
- `pds_postgres_writes_total{table,result}`
- `pds_audit_queue_depth`

- [ ] **Step 7: Add hot reload**

Implement:

- `SIGHUP` reload;
- `POST /v1/admin/reload` with an admin header;
- fsnotify reload for mounted ConfigMap files.

Reload must build a full new snapshot and swap atomically only after validation succeeds.

- [ ] **Step 8: Generate gRPC code with buf**

Create `proto/pds/v1/pds.proto`, `buf.yaml`, and `buf.gen.yaml`.

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
buf generate
```

Expected: generated Go stubs compile.

- [ ] **Step 9: Implement gRPC handler parity**

Both HTTP and gRPC must call the same `engine.Evaluate(ctx, req)`. Tests should compare HTTP and gRPC responses for the same seeded rules.

- [ ] **Step 10: Verify end-of-day demo**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
go test ./... -v
curl -sS http://127.0.0.1:8081/metrics | grep pds_decisions_total
```

Then modify `config/rules.example.yaml`, trigger reload, and confirm a new rule takes effect without process restart.

- [ ] **Step 11: Commit Phase 3**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
git add .
git commit -m "feat: add advanced rules observability and grpc"
```

## Phase 4: VideoProcess Integration And Deployment

**Day goal:** VP calls PDS before selecting/publishing platform tasks, fail-opens on PDS outage, blocks/flags with auditable decision ids, and docker-compose can run the local combined stack.

**Files:**
- PDS: `deploy/Dockerfile`, `deploy/kubernetes.yaml`, `README.md`.
- VP: `backend/app/pds_client.py`, `backend/app/config.py`, `backend/app/channel_agent/clients.py`, `backend/app/channel_agent/service.py`, `backend/app/channel_agent/runner.py`, `docker-compose.yml`, `backend/tests/test_pds_client.py`, `backend/tests/channel_agent/test_service.py`.

- [ ] **Step 1: Write VP PDS client tests**

Create `backend/tests/test_pds_client.py` covering:

- `allow` response maps to `Decision(verdict="allow")`;
- `flag` response preserves reason code, rule, and decision id;
- `block` response preserves reason code, rule, and decision id;
- timeout returns `allow` with reason code `pds_unavailable`;
- 5xx returns `allow` with reason code `pds_unavailable`;
- malformed JSON returns `allow` with reason code `pds_unavailable`.

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/test_pds_client.py -q
```

Expected: FAIL before client exists.

- [ ] **Step 2: Implement VP fail-open client**

Create `backend/app/pds_client.py` with:

- dataclasses or Pydantic models for `PolicyDecision` and `PolicyReason`;
- `PDSClient.decide(...)`;
- `NoopPDSClient` for disabled config;
- timeout from settings;
- no exception escapes for timeout, connection, HTTP 5xx, or invalid payload.

The fallback decision must be explicit:

```python
PolicyDecision(
    verdict="allow",
    decision_id="",
    reasons=[PolicyReason(code="pds_unavailable", rule="pds_client")],
    evaluated_rules=[],
    rules_version="unavailable",
    latency_ms=0,
)
```

- [ ] **Step 3: Add VP settings**

Modify `backend/app/config.py`:

```python
pds_enabled: bool = False
pds_base_url: str = "http://pds:8080"
pds_client_id: str = "videoprocess-channel-agent"
pds_timeout_seconds: float = 0.5
```

- [ ] **Step 4: Write channel-agent integration tests**

Add tests in `backend/tests/channel_agent/test_service.py`:

- candidate PDS `block` rejects the candidate with guard `pds_blocked` and does not enqueue `plan_task`;
- candidate PDS `flag` rejects or holds the candidate with guard `pds_flagged_for_review` because there is no human-review queue in this alpha path;
- PDS fail-open `allow` still creates a task;
- promotion PDS `block` holds an uploaded publication before scheduling public/unlisted promotion;
- decision id and PDS reason codes are preserved in `AgentTickAudit.guards_triggered_json` or task failure metadata.

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/channel_agent/test_service.py -q
```

Expected: FAIL before service integration exists.

- [ ] **Step 5: Add PDS protocol and fake client to ChannelAgent**

Modify `backend/app/channel_agent/clients.py`:

- add `PolicyDecisionClient` protocol with `decide(...)`;
- add `FakePolicyDecisionClient` that can return allow/flag/block by action type;
- keep fake default allow so existing tests remain deterministic.

- [ ] **Step 6: Inject PDS client into ChannelAgentService**

Modify `ChannelAgentService.__init__` to accept `pds_client`. Use `NoopPDSClient` when omitted.

- [ ] **Step 7: Add candidate pre-flight call**

In `_evaluate_candidate_guards`, after local account/concurrency/upload-failure/lane cadence checks and before accepting the candidate, call PDS with:

- `actor_id`: channel id or account id; prefer `str(candidate["account"].id)` for platform action limits;
- `action.type`: `publish_video`;
- `action.platform`: account platform, usually `youtube`;
- `content.title`: candidate title seed;
- `content.description`: candidate prompt;
- `content.duration_s`: lane format target duration if present;
- `content.tags`: lane keywords when available;
- `context.channel_id`, `context.lane_id`, `context.candidate_id`, `context.source_platforms`.

Decision handling:

- `block`: return `_candidate_rejection(..., guard="pds_blocked", reason=<codes>)`;
- `flag`: return `_candidate_rejection(..., guard="pds_flagged_for_review", reason=<codes>)` for v1 because there is no separate review queue yet;
- `allow`: continue.

- [ ] **Step 8: Add promotion pre-flight call**

In `handle_promote_publication`, call PDS before `youtube_client.schedule_publish(...)` with:

- `actor_id`: publication account id;
- `action.type`: `promote_publication`;
- `action.platform`: publication platform;
- `content.title`, `content.description`, `content.tags`;
- `context.publication_id`, `context.production_task_id`, `context.target_visibility`.

If `block` or `flag`, set:

- `publication.publish_status = "held"`;
- `task.state = TASK_HELD`;
- `task.blocked_by_guard = "pds_blocked"` or `"pds_flagged_for_review"`;
- `task.failure_reason` includes decision id and reason codes;
- do not call YouTube schedule.

- [ ] **Step 9: Wire real client in runner**

Modify `backend/app/channel_agent/runner.py` so `ChannelAgentRunner` passes:

- `PDSClient(settings.pds_base_url, settings.pds_client_id, settings.pds_timeout_seconds)` when `settings.pds_enabled`;
- `NoopPDSClient()` otherwise.

- [ ] **Step 10: Add docker-compose service**

Modify `docker-compose.yml`:

- add `pds` service built from `../policy-decision-service` or `/home/taiwei/Constructure-repos/policy-decision-service`;
- mount `../policy-decision-service/config:/etc/pds:ro`;
- use shared Postgres and Redis;
- expose local `18081:8080`, `19090:9090`, and `18082:8081` if ports are available;
- set `PDS_ENABLED=true`, `PDS_BASE_URL=http://pds:8080`, and `PDS_CLIENT_ID=videoprocess-channel-agent` for `channel-agent-runner`.

Keep VP publication privacy private/unlisted only; do not introduce a public default.

- [ ] **Step 11: Add PDS K8s manifests**

In the PDS repo, add `deploy/kubernetes.yaml` with:

- namespace-compatible Deployment;
- ConfigMap for rules and blocklist;
- ClusterIP service;
- readiness/liveness probes;
- optional ServiceMonitor;
- resource limits around `200m` CPU and `256Mi` memory for resume-friendly measurement.

- [ ] **Step 12: Verify VP tests**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest tests/test_pds_client.py tests/channel_agent/test_service.py -q
python3 -m pytest -q
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: targeted tests and full pytest pass. Ruff/mypy output is recorded even if optional tools are missing.

- [ ] **Step 13: Verify local combined smoke**

Run PDS and VP stack, then seed a rule that blocks after the first `publish_video` action. Trigger a channel-agent tick and confirm:

- first task is selected;
- next matching task is rejected with `pds_blocked`;
- PDS audit table contains both decisions;
- VP logs include the PDS decision id.

- [ ] **Step 14: Commit Phase 4**

Run:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
git add backend/app/pds_client.py backend/app/config.py backend/app/channel_agent/clients.py backend/app/channel_agent/service.py backend/app/channel_agent/runner.py backend/tests/test_pds_client.py backend/tests/channel_agent/test_service.py docker-compose.yml docs/superpowers/plans/2026-05-19-policy-decision-service-go-risk-gate.md
git commit -m "feat: integrate policy decision service preflight"

cd /home/taiwei/Constructure-repos/policy-decision-service
git add .
git commit -m "feat: add deployment assets"
```

## Phase 5: Load Test, Polish, Docs, And Resume Evidence

**Day goal:** PDS has measured load-test results, documented operational behavior, clean checks, and resume bullets backed by actual numbers.

**Files:**
- PDS: `tests/load/decide.js`, `tests/load/RESULTS.md`, `deploy/grafana-dashboard.json`, `README.md`, `AGENTS.md`.
- VP: docs only if needed.

- [ ] **Step 1: Install or containerize k6**

Because local `k6` is currently missing, use one of:

```bash
brew install k6
```

or:

```bash
docker run --rm -i grafana/k6 run - < tests/load/decide.js
```

Use Docker if host install is not desired.

- [ ] **Step 2: Write load script**

`tests/load/decide.js` should:

- ramp from 0 to 2000 RPS if the host can sustain it;
- mix `publish_video`, `post_comment`, and `promote_publication`;
- include 80 percent allow, 15 percent flag, 5 percent block distribution;
- set `X-Client-Id`;
- assert status 200 and valid verdict.

- [ ] **Step 3: Run baseline load test**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
make build
./bin/pds
k6 run tests/load/decide.js
```

Record:

- CPU/memory;
- P50/P95/P99 latency;
- achieved RPS;
- error rate;
- git SHA;
- rules hash.

- [ ] **Step 4: Tune only if measurement demands it**

Tune in this order:

- Redis pool size;
- audit batch size and flush interval;
- pgxpool max connections;
- `GOMAXPROCS`;
- number of rule goroutines, avoiding unbounded fan-out.

Do not claim 1000 RPS/P99 < 10ms unless measured.

- [ ] **Step 5: Fill `tests/load/RESULTS.md`**

Document the final measured result with exact command, environment, git SHA, and numbers.

- [ ] **Step 6: Add Grafana dashboard**

Dashboard must show:

- decision rate by verdict;
- latency histogram/P99;
- rule eval errors;
- audit queue depth;
- Redis/Postgres operation errors.

- [ ] **Step 7: Finish README**

README sections:

- architecture diagram;
- quickstart;
- rule examples;
- fail-open design;
- VP integration;
- local Compose;
- Kubernetes deployment;
- metrics and alerting;
- known v2 scope.

- [ ] **Step 8: Add new repo `AGENTS.md`**

Include:

- Go package layout;
- required checks: `go test ./...`, `gofmt`, `go vet ./...`;
- integration/load-test commands;
- fail-open rule;
- audit persistence requirement.

- [ ] **Step 9: Run final PDS verification**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
gofmt -w $(find . -name '*.go' -not -path './proto/gen/*')
go test ./...
go vet ./...
make docker-build
```

Expected: pass.

- [ ] **Step 10: Run final VP verification**

Because VP files changed, run the required backend checks and frontend build if `docker-compose.yml` or UI-related docs changed:

```bash
cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true

cd /home/taiwei/.codex/worktrees/d1d5/videoprocess/frontend
npm install
npm run build
npm run lint || true
```

Expected: pytest and frontend build pass; optional quality outputs recorded.

- [ ] **Step 11: Update resume bullets only with measured numbers**

Update `/home/taiwei/code/job-prep/PDS_PLAN.md` or the resume source only after Phase 5 measurements exist:

- If no load test has been run, do not add a latency/RPS bullet.
- If the measured run misses the original `1000 RPS, P99 < 10ms` target, write the actual lower number.
- Include the command, git SHA, resource limit, P99 latency, achieved RPS, and error rate from `tests/load/RESULTS.md`.

- [ ] **Step 12: Commit Phase 5**

Run:

```bash
cd /home/taiwei/Constructure-repos/policy-decision-service
git add .
git commit -m "docs: record pds load test and operations guide"

cd /home/taiwei/.codex/worktrees/d1d5/videoprocess
git add docs/superpowers/plans/2026-05-19-policy-decision-service-go-risk-gate.md
git commit -m "docs: plan pds risk gate implementation"
```

## Acceptance Criteria

- PDS repo exists at `/home/taiwei/Constructure-repos/policy-decision-service`.
- `go test ./...` passes in PDS.
- PDS exposes HTTP `/v1/decide`, `/healthz`, `/readyz`, `/metrics`, `/v1/rules`, and `/v1/admin/reload`.
- PDS exposes gRPC `PolicyDecision.Decide`.
- Rules load from YAML and support `rate_limit`, `keyword_match`, `cel`, and `combiner`.
- Rule reload is atomic and does not activate invalid rules.
- Every successful decision attempts an audit write to `pds.decisions`.
- PDS is fail-open at rule-error level; VP is fail-open for PDS outage.
- VP rejects or holds actions on PDS `block` and `flag`, with decision id and reasons preserved.
- Default publication privacy remains `private` or `unlisted`; no public default is introduced.
- External platform assets are not publicly published without existing VP human-review/approval behavior.
- Local Compose can run VP plus PDS.
- K8s manifests are present and readiness/liveness probes work.
- Load-test result is documented with measured P99 and RPS.

## Risks And Mitigations

- **PDS scope is larger than one week:** Ship HTTP plus core rules first; keep gRPC and CEL contained to Phase 3 and avoid UI work.
- **`protoc` missing locally:** Use `buf` generation path or install `protobuf-compiler` before Phase 3.
- **`k6` missing locally:** Use Dockerized `grafana/k6` for Phase 5 if host install is undesirable.
- **PDS blocks VP incorrectly:** Keep PDS disabled by default in VP config, fail-open on service errors, and start with `flag`/`block` test rules in local smoke only.
- **Audit writes hurt latency:** Keep writes async and bounded; record drops with metrics rather than blocking decision latency.
- **VP integration creates duplicate policy systems:** Treat PDS as a pre-flight service for platform actions. Existing VP cadence/privacy/rights guards remain authoritative local safeguards.

## Execution Handoff

Plan complete. Recommended execution is one phase per day:

1. Phase 1: PDS skeleton.
2. Phase 2: core rules and audit.
3. Phase 3: CEL, combiner, reload, metrics, gRPC.
4. Phase 4: VP integration and deployment.
5. Phase 5: load test, docs, and resume evidence.

Use subagent-driven execution per phase only if write scopes are kept separate: PDS core, PDS deployment/docs, and VP integration should not edit the same files concurrently.
