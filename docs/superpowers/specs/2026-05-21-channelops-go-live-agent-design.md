# ChannelOps Go Live Agent Design

Date: 2026-05-21
Source spec: `Design/videoprocess_channelops_live_agent_spec.md`

## Goal

Move ChannelOps from a Python-runner beta path to a Go-owned live runner for the Phase 0 and Phase A scope:

- Phase 0 P0 fixes:
  - AutoFlow material candidates expose `material_id` so `material_usage_ledger` and repetition guard work.
  - PDS `plan_approval` `flag` / `block` holds the task instead of entering execute.
  - Dev/staging can explicitly use allow-all PDS while production remains fail-closed.
  - `TakedownEvent` writes are deduplicated per publication, event type, and UTC day.
- Phase A small fixes:
  - `trend_youtube` candidates do not receive manual-seed material-usage override.
  - `FeedbackSnapshot` records metrics completeness and available fields.
- Live execution path:
  - `scheduler -> tick -> queue -> plan -> execute -> observe -> publish -> reconcile -> metrics` is owned by Go.
  - Python ChannelOps runner remains legacy/fallback but is not enabled in live mode.

Out of scope for this implementation:

- Phase B candidate-level audit table and failure-category API expansion.
- Phase C `DiscoverySignal` migration.
- Public auto-publishing.
- Multi-platform publishing.
- Full deletion of Python ChannelOps code.

## Architecture Boundary

Add a new Go process:

```text
cmd/channelops-runner
internal/channelops/
  config/
  store/
  queue/
  scheduler/
  tick/
  guards/
  handlers/
  clients/
  materials/
  metrics/
  reconcile/
  alerts/
```

The Go runner owns the live ChannelOps runtime:

- `scheduler` scans enabled, unhalted channels and writes `internal_scheduler_runs`.
- `tick` generates manual-seed and lane-driven candidates, evaluates guards, writes audits, creates `production_tasks`, and enqueues `plan_task`.
- `queue` claims `channel_ops_queue_items` with PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`.
- `handlers` process `plan_task`, `execute_task`, `observe_job`, `publish_task`, `reconcile_publication`, `collect_metrics`, and `account_health`.
- `clients` call AutoFlow HTTP APIs, YouTubeManager HTTP APIs, PDS, and alert sinks.
- `store` centralizes SQL for ChannelOps tables so business code does not scatter raw SQL.

Python remains responsible for:

- FastAPI APIs and existing dashboard endpoints.
- SQLAlchemy models and Alembic migrations.
- AutoFlow internal planning/execution services.
- The source AutoFlow schema fix for `material_id`, because the schema and material search pipeline are Python code.

Deployment must run only one live ChannelOps runner. In live mode, start `channelops-runner-go` and do not start the legacy Python `channel-agent-runner`.

## Data Flow

### Scheduler

The Go scheduler loops every `CHANNELOPS_SCHEDULER_POLL_SECONDS`.

For every channel where:

- `enabled = true`
- `halted_at is null`
- tick cadence says the current bucket is due

it inserts or reuses an `internal_scheduler_runs` row keyed by `(channel_profile_id, bucket)`, then enqueues:

```text
kind = agent_tick
idempotency_key = agent_tick:<channel_id>:<bucket>
channel_profile_id = <channel_id>
```

Duplicate scheduler runs for the same bucket must not enqueue duplicate work.

### Tick

The Go tick handler loads:

- `ChannelProfile`
- enabled and unpaused `TopicLane`
- enabled and unpaused `PublishingAccount`
- active `ManualSeed`
- enabled `LaneFormatMatrix`

Candidate generation order:

1. Manual seeds first.
2. Lane-driven candidates then fill remaining budget.
3. Each enabled `(lane x format)` may emit at most one lane-driven candidate per tick.
4. Guard-rejected candidates do not consume the tick budget.

Manual seed behavior:

- `ManualSeed.source_policy = trend_youtube` is treated as a trend candidate.
- The created task rationale must include `source_kind = trend_youtube`.
- Trend candidates do not get manual material-usage override.

Dry-run behavior:

- Candidate generation and guards still run.
- `agent_tick_audits` records selected/rejected decisions.
- No `production_tasks` and no queue items are created.

Accepted candidates create `production_tasks` with existing state names and enqueue `plan_task:<task_id>`.

### Plan

The Go `plan_task` handler calls AutoFlow plan over HTTP and requires exactly one `youtube_upload` node.

If the task uses `approval_mode = agent`, it calls PDS `plan_approval`:

- `allow`: approve the AutoFlow plan, set task state to `planning`, enqueue `execute_task`.
- `flag`: set task state to `held`, `blocked_by_guard = pds_flagged_for_review`, do not enqueue execute.
- `block`: set task state to `held`, `blocked_by_guard = pds_blocked`, do not enqueue execute.

If `approval_mode = human`, the handler preserves the existing human-review behavior and does not agent-approve the plan.

### Execute And Observe

`execute_task` calls AutoFlow execute over HTTP, records `autoflow_run_id` and `job_id`, sets task state to `producing`, and enqueues `observe_job`.

`observe_job` polls AutoFlow job status:

- Running: re-enqueue `observe_job` with exponential backoff capped at five minutes.
- Succeeded: parse the `youtube_upload` node output, then continue to publication handling.
- Failed: set task state to `failed` and record the reason.

The Go runner is the live source of truth for this progression. Python service behavior may remain for legacy tests but must not compete for queue items in live deployment.

### Publish, Reconcile, Metrics

Publication remains YouTube-only and `private` / `unlisted` only for this phase.

`publish_task` and `promote_publication`:

- Call PDS `publish` / `promote_publication`.
- Respect fail-policy.
- Never public-publish external platform assets in this phase.
- Write or update `publication_records`.

`reconcile_publication`:

- Calls YouTubeManager status.
- Updates `publication_records`.
- On severe status, sets task to `held`, records guard/reason, and writes a deduplicated `takedown_events` row.

`collect_metrics`:

- Calls YouTubeManager metrics when payload metrics are absent.
- Requeues until max poll count if no recognized metrics are available.
- Creates or updates a single latest `FeedbackSnapshot` per publication.
- Computes `metrics_completeness_score` and `available_fields_json`.
- Sets task state to `measured` when metrics are written.

## Go Clients

### PDS

Implement a Go PDS client with the existing action fail policy:

| Action | PDS unavailable default |
| - | - |
| `candidate_accept` | `allow` |
| `plan_approval` | `flag` |
| `publish` | `block` |
| `promote_publication` | `block` |

Add explicit dev allow-all:

```text
CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true
```

When enabled, all decisions return `allow` and include metadata:

```json
{"warning":"dev_allow_all","fail_policy":"allow"}
```

Production and staging deployments must not enable this flag unless the operator is intentionally running a dev smoke.

### AutoFlow

The Go runner calls AutoFlow through HTTP APIs rather than importing Python.

The minimal client interface:

- `PlanTask(task, request) -> plan_id, upload_node_count, plan_payload`
- `ApprovePlan(plan_id, evidence)`
- `ExecuteTask(task, request) -> run_id, job_id, status`
- `GetJob(job_id) -> status, output payload`

Fake AutoFlow HTTP servers must be used in Go integration tests.

### YouTubeManager

The Go YouTube client wraps the existing manager endpoints needed by live ChannelOps:

- auth/health status
- quota/account health
- upload/status or publication status
- metrics

OAuth, quota, or takedown-like failures must hold the relevant task/account rather than loop indefinitely.

## Data Model And Migrations

Python Alembic remains the migration system.

Required changes:

1. AutoFlow schema only:
   - Add `material_id: str | None = None` to `AutoFlowClipCandidate`.
   - Add `material_id` into material metadata output.
   - Ensure selected candidates and AutoFlow run artifacts carry `material_id`.
   - Add fallback in `extract_material_references`: if `material_id` is missing, use `asset_id` as a compatibility material id.

2. `FeedbackSnapshot`:
   - `metrics_completeness_score: float default 0.0 not null`
   - `available_fields_json: JSON list default [] not null`

3. `TakedownEvent`:
   - Add index over `publication_id`, `event_type`, `detected_at`.
   - Runtime dedup uses UTC day; if a matching event exists, append repeat details to `auto_actions_taken_json`.

No Phase B/C tables are added in this implementation.

## Guards

The Go tick and publish path must implement the existing live guard set:

- Account concurrency guard.
- Quota/account health guard.
- Lane cadence guard based on `PublicationRecord`, not `ProductionTask.created_at`.
- Consecutive upload failure guard using recent-window semantics.
- Material usage/repetition guard.
- External asset publication guard.

Material usage behavior:

- Extract references from AutoFlow plan payload, run payload, and upload metadata.
- Prefer explicit `material_id`.
- Fallback to `asset_id` only for older payloads.
- Manual seeds may override repetition guard only when `source_kind != trend_youtube`.
- Trend and lane-driven candidates do not override repetition guard.

## Error Handling

Queue item handling:

- Claim with `FOR UPDATE SKIP LOCKED`.
- Preserve `idempotency_key` uniqueness.
- Retry transient failures with exponential backoff:

```text
delay = min(5m * 2^(attempt - 1), 30m)
```

- When attempts exceed `max_attempts`, set `dead_letter_at` and stop retrying.

Non-transient failures:

- Missing YouTube upload node: task `held`, guard `missing_youtube_upload_node`.
- PDS flag/block: task `held`, no execute enqueue.
- OAuth/quota severe: account/task held and alert.
- YouTube severe status: task held and deduplicated takedown event.
- Metrics unavailable after max polls: task held.

Transition history must be appended on every state change using the existing JSON shape.

## Deployment

Add:

- `cmd/channelops-runner`
- `backend/Dockerfile.channelops-runner-go` or extend existing Go Dockerfile pattern.
- Compose service `channelops-runner-go`.

Required environment:

```text
DATABASE_URL
CHANNELOPS_RUNNER_POLL_SECONDS
CHANNELOPS_SCHEDULER_POLL_SECONDS
YOUTUBE_MANAGER_URL
PDS_ENABLED
PDS_BASE_URL
PDS_CLIENT_ID
PDS_TIMEOUT_SECONDS
CHANNEL_AGENT_DEV_ALLOW_ALL_PDS
CHANNEL_AGENT_ALERT_SLACK_WEBHOOK_URL
CHANNEL_AGENT_ALERT_EMAIL_TO
```

Live compose profile:

- Start `channelops-runner-go`.
- Do not start Python `channel-agent-runner`.

The runner must fail fast if `YOUTUBE_MANAGER_URL` is empty in live mode.

## Tests

### Go Unit Tests

Add targeted tests under `internal/channelops/...`:

- Queue claim, idempotency, retry backoff, and dead-letter.
- Scheduler bucket dedup.
- Tick candidate generation:
  - manual seed priority
  - lane-driven budget fill
  - dry-run audit without side effects
  - guard-rejected candidates do not consume budget
  - `trend_youtube` does not receive manual override
- PDS client:
  - fail-policy by action
  - dev allow-all
  - timeout/unavailable handling
- Material guard:
  - explicit `material_id`
  - fallback to `asset_id`
  - repetition rejection
- Reconcile:
  - takedown dedup appends repeats
- Metrics:
  - partial completeness score
  - update existing snapshot

### Python Target Tests

Keep Python tests for Python-owned seams:

- AutoFlow candidate/search/ranker/service emits `material_id`.
- `extract_material_references` falls back to `asset_id`.
- Alembic migrations upgrade.
- Existing API serialization remains compatible with new fields.

### Integration Tests

Add a Go integration test with fake HTTP servers for AutoFlow, YouTubeManager, and PDS:

```text
scheduler -> tick -> plan -> execute -> observe -> publish -> reconcile -> collect_metrics
```

Expected final data:

- one task reaches `measured`
- one publication is scheduled/unlisted
- one feedback snapshot exists with completeness score
- material usage ledger has at least one row
- no severe takedown

### Live Smoke

Add a runnable smoke command, preferably Go:

```text
cmd/channelops-live-smoke
```

It checks:

- YouTubeManager authenticated and quota-readable.
- PDS health or explicit dev allow-all.
- A channel tick can be triggered or observed.
- At least one task reaches scheduled/unlisted.
- Reconcile confirms unlisted.
- Metrics snapshot is written.
- Material ledger grows.
- Takedown count remains zero.

If real YouTube credentials are missing, live smoke must skip only the real YouTube section and still require fake integration tests to pass.

## Acceptance Criteria

This implementation is complete when:

- Go `channelops-runner` can replace Python `channel-agent-runner` in compose for live ChannelOps.
- Phase 0 four P0 items pass tests.
- Phase A two small fixes pass tests.
- Fake end-to-end Go integration test passes.
- AutoFlow `material_id` appears in generated candidate/run payloads.
- `material_usage_ledger` receives rows during the integration test.
- PDS `flag` and `block` plan approvals hold tasks and do not enqueue execute.
- Takedown dedup stores one row per publication/event/day and appends repeats.
- `FeedbackSnapshot` records `metrics_completeness_score` and `available_fields_json`.
- Documentation states live deployments must not run Go and Python ChannelOps runners at the same time.

## Rollback

Rollback is deployment-level first:

- Stop `channelops-runner-go`.
- Restart legacy Python `channel-agent-runner` only after ensuring no Go runner is consuming queue.

Schema rollback:

- New FeedbackSnapshot fields and takedown index are additive and can remain during rollback.
- AutoFlow `material_id` schema field is backward-compatible.

Operational rollback trigger examples:

- Held task ratio exceeds 30% after Go runner cutover.
- Dead-letter queue item ratio exceeds 1%.
- Scheduler tick success rate falls below 95% over 24 hours.
- Any severe takedown appears during unlisted soak.
