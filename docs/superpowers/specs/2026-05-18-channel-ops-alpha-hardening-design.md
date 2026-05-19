# ChannelOps Alpha Hardening Design

Date: 2026-05-18
Status: Approved design, ready for implementation planning
Base branch: `codex/channel-ops-agent-alpha`
Related specs:

- `docs/superpowers/specs/2026-05-18-channel-ops-agent-design-v2.md`
- `docs/superpowers/specs/2026-05-18-channel-ops-agent-implementation-design.md`

## 1. Problem Statement

The current ChannelOps alpha has strong model, migration, queue, MiniMax,
frontend, and Compose foundations, but it is not yet ready for 7-day dry-run
acceptance. Three gaps block the alpha:

- The runner acknowledges `execute_task`, `observe_job`, and `collect_metrics`
  as no-ops, so a task can stop forever at `planning`.
- `tick()` only consumes `ManualSeed` rows. Without manual seeds, the agent has
  no lane-driven autonomy.
- Several alpha guards and operational APIs are missing or misleading,
  especially cadence, account concurrency, upload failure handling, queue
  scoping, and funnel aggregation.

This hardening pass must turn the current scaffold into a local-safe,
end-to-end alpha loop that can be evaluated in dry-run and then promoted by an
operator.

## 2. Scope

This pass implements the selected C scope:

- P0: runner closure, lane-driven candidate generation, guard completion.
- P1: real funnel aggregation, missing control APIs, AutoFlow request
  configuration, and alert strategy alignment.
- P2 necessary hardening: queue channel scoping, idempotent enqueue race
  handling, and exponential retry backoff.

This pass does not implement:

- Real SMTP delivery. Alpha push alert delivery is Slack-only.
- Automatic held-task TTL. It is documented as a deferred decision.
- Learning, bandits, or trend/news ingestion.
- Multi-platform upload beyond YouTube.
- A large bridge refactor. Existing service boundaries are kept, with small
  helper/client extensions where needed.

## 3. Implementation Approach

Use service-first hardening. Keep `ChannelAgentService`,
`ChannelOpsQueueService`, and `backend/app/api/channel_agent.py` as the main
extension points. Add helper methods and small client protocol extensions, but
avoid introducing a large adapter layer in this pass.

The service must expose this queue state machine:

```text
agent_tick
  -> plan_task
  -> execute_task
  -> observe_job
  -> publish_task
  -> promote_publication
  -> collect_metrics
```

Each handler must be idempotent. If a record already exists, the handler must
return the existing record or enqueue the next missing queue item instead of
duplicating work.

## 4. Runner Closure

`ChannelAgentRunner.handle_item()` must route the following kinds to real
service handlers:

- `execute_task`
- `observe_job`
- `collect_metrics`

Only unsupported future kinds may remain no-op. The three kinds above are part
of the alpha critical path and must never be acknowledged without doing work.

### 4.1 AutoFlow Client Contract

Extend the fake-friendly `AutoFlowClient` protocol with:

- `execute_task(task, request_or_plan_id) -> AutoFlowExecutionObservation`
- `observe_job(task, run_id, job_id) -> AutoFlowJobObservation`

Observation data:

- `run_id`
- `pipeline_id`
- `job_id`
- `status`: `pending`, `running`, `succeeded`, `failed`
- `error_message`, nullable
- `youtube`, nullable dict containing at least `video_id` when upload succeeded

The default fake client must be deterministic:

- `plan_task()` returns one `youtube_upload` node when configured to include
  upload.
- `execute_task()` returns stable run, pipeline, and job identifiers.
- `observe_job()` can return `running` once if configured, then `succeeded`
  with `youtube.video_id`.

### 4.2 Local AutoFlow Observation

The real local-safe path must call existing AutoFlow service methods instead
of HTTP:

- plan through existing planning path.
- execute through `autoflow_service.execute()`.
- observe by reading `AutoFlowRun`, `Job`, `NodeExecution`, and terminal
  `Artifact.media_info`.

The current worker stores handler results in `Artifact.media_info`. The
`youtube_upload` handler returns `{"youtube": {...}}`, so observation must
read the `Artifact.media_info.youtube.video_id` from the artifact attached to
the successful `youtube_upload` node. If the job succeeds but no YouTube video
id can be found, the task must be held with
`blocked_by_guard="missing_youtube_observation"` instead of silently succeeding.

### 4.3 Handler Semantics

`handle_execute_task`:

- Requires `ProductionTask.autoflow_plan_id`.
- Calls `AutoFlowClient.execute_task`.
- Writes `autoflow_run_id`, `pipeline_id`, `job_id`.
- Sets task state to `producing` when execution is accepted.
- Enqueues `observe_job:<production_task_id>:<run_id>`.
- On execution failure, sets `state="failed"` and records `failure_reason`.

`handle_observe_job`:

- Reads task run/job identifiers.
- If running, re-enqueues itself with backoff. Backoff parameters: base
  30 seconds, multiplier `2 ** observe_count`, capped at 5 minutes. Track
  `observe_count` in `payload_json` so each requeue advances the wait.
- If failed, marks task failed and records failure.
- If succeeded with `youtube.video_id`, enqueues
  `publish_task:<production_task_id>` and writes the observed
  `youtube` block into the publish_task payload so `handle_publish_task`
  does not need to re-read job artifacts.
- If succeeded without upload observation, marks task held with
  `missing_youtube_observation`.

`handle_collect_metrics`:

- Creates or updates one `FeedbackSnapshot` for the publication/window.
- Updates `PublicationRecord.last_metrics_polled_at`.
- Moves the task to `measured` after metrics are collected.
- Alpha may use fake metrics. Real YouTube analytics can replace the client
  later without changing the queue contract.

## 5. Tick And Candidate Generation

`tick()` must execute candidate generation and guard evaluation in both dry-run
and live mode. Dry-run differs only in side effects:

- It must not create `ProductionTask`.
- It must not enqueue `plan_task`.
- It must still write candidate counts, guard rejections, and rationale to
  `AgentTickAudit`.

### 5.1 Candidate Order

Candidate generation order is fixed:

1. Active `ManualSeed` rows, oldest first.
2. Lane-driven candidates for remaining budget.

Manual seeds have priority, but they do not suppress lane-driven work. After
manual seeds are evaluated, the service computes remaining budget and fills it
with lane-driven candidates when possible.

### 5.2 Budget

Budget is based on actual publication cadence, not task creation count.

- Channel/lane daily budget is reduced by `PublicationRecord` rows in the last
  24 hours where `publish_status in ("public", "scheduled")`.
- Current-tick selections also reduce remaining budget.
- Guard-rejected candidates do not consume budget.

### 5.3 Lane-Driven Candidates

Each enabled `(TopicLane x LaneFormatMatrix)` may produce at most one
lane-driven candidate per tick, subject to remaining budget and guards.

Lane-driven task defaults:

- `source="lane_seed"`
- `manual_seed_id=None`
- `uses_external_assets=True`
- `source_platforms_json` comes from `LaneFormatMatrix.source_platforms_json`.
- If the format has no source platforms, use
  `ChannelProfile.risk_policy_json.default_source_platforms`.
- If neither is configured, use `["youtube"]`.
- Publication compliance defaults to `known_risk_accepted`.

`target_account_id` resolution for lane-driven candidates:

- If the channel has exactly one enabled `PublishingAccount`, use that account.
- If the channel has multiple enabled accounts, alpha picks the first by
  `created_at` ascending. Round-robin or load-aware routing is deferred and
  listed in §13.
- If the channel has zero enabled accounts, the candidate is rejected with
  guard reason `no_enabled_account` and recorded in `rejected_candidates`.
  Lane-driven generation must not raise.

`LaneFormatMatrix` must gain:

- `source_platforms_json: list[str]`, default `[]`

This requires an Alembic migration and schema/API updates.

### 5.4 Prompt Template

Lane-driven prompts must use a structured template instead of string
concatenation. Add a small helper module, for example
`backend/app/channel_agent/lane_prompts.py`.

Default English template:

```text
Create a {format_key} video for the "{lane_name}" topic.
Theme: {lane_description}
Keywords: {keywords}
Target duration: {duration_sec}s, aspect ratio {aspect_ratio}.
```

The helper must keep room for future language-specific prompt templates, but
this pass only needs the default template.

## 6. Guards

Guard results must be represented consistently in audit output.

`AgentTickAudit.decision_summary_json` must include:

```json
{
  "per_lane_eligible_count": {},
  "rejected_candidates": [
    {
      "candidate_id": "lane:<lane_id>:format:<format_id>:<bucket>",
      "lane_id": "...",
      "format_id": "...",
      "account_id": "...",
      "guard": "account_concurrency",
      "reason": "Account abc has 1 active task in state=held"
    }
  ]
}
```

`guards_triggered_json` may keep a compact list for quick dashboard display,
but the detailed schema above is the source of truth for operator diagnosis.

### 6.1 AccountConcurrencyGuard

An account may have at most one active task. Active states:

- `selected`
- `planning`
- `producing`
- `uploaded_private`
- `held`
- `scheduled`

Terminal/inactive states:

- `failed`
- `rejected`
- `cancelled`
- `published`
- `measured`

`held` counts as active because it requires operator intervention. Otherwise
the agent can keep filling the same account with work while a prior task is
blocked.

Consequence: on a channel with multiple lanes but only one enabled account,
this guard serializes lane-driven candidates across the account. Within a
single tick at most one lane will produce a new task; subsequent lanes will be
rejected with `account_concurrency`. This is intentional for alpha. Multi-lane
parallelism on shared accounts is part of the multi-account routing deferred
in §13.

### 6.2 ConsecutiveUploadFailureGuard

This guard uses a window, not a lifetime cumulative count.

Rule:

- Look at the same account's most recent 5 tasks.
- If at least 3 failed, and the oldest task in that 5-task window is within
  the last 24 hours, block new candidates for the account.
- Count only upload/publication-related failures.

Failure keyword set for alpha:

- `upload`
- `publish`
- `youtube`
- `quota`
- `oauth`
- `video_id`
- `thumbnail`

When triggered, enqueue a warning alert with an operator action in the message:

```text
3 of last 5 uploads failed in 24h. Suggested action: pause the account via
POST /api/v1/channel-agent/accounts/<id>/pause or inspect failed tasks in
/api/v1/channel-agent/channels/<channel_id>/tasks.
```

Future migration: add `ProductionTask.failure_category` enum with values such
as `network`, `upload`, `auth`, `quota`, `validation`, and `other`.

### 6.3 LaneCadenceGuard

Cadence measures actual distribution, not task creation.

Rules:

- `max_posts_per_day`: count same-lane `PublicationRecord` rows in the last
  24 hours where `publish_status in ("public", "scheduled")`.
- `cooldown_after_post_minutes`: use the same lane's most recent
  `scheduled_publish_at` or `public_at`.
- `max_consecutive_streak`: inspect recent publication records in descending
  publication time and block if the lane would exceed the configured streak.

Because `PublicationRecord` does not currently store `topic_lane_id`, the
implementation must derive lane by joining through `ProductionTask` using
`publication.production_task_id`.

## 7. AutoFlow Request Configuration

`_autoflow_request()` must be built from the task snapshot and lane format
configuration.

Required fields:

- `duration_sec`: from `LaneFormatMatrix.target_duration_sec`.
- `aspect_ratio`: from `ChannelProfile.default_aspect_ratio`.
- `source_strategy`: from manual seed constraints or channel defaults, fallback
  `auto`.
- `planning_mode`: from manual seed constraints or channel defaults, fallback
  `auto`.
- `publish_mode`: alpha default `private_upload`. Lane/account configuration
  may opt into `public_after_review` when `external_asset_auto_publish=true`
  on the account. Never hard-code `public` at the AutoFlow request layer; that
  decision belongs to the promote step.
- `material_library_ids`: from task/manual seed.
- `source_platforms`: from manual seed or lane format source platforms.
- `constraints`: include `template_pool_json`, `lane_id`, `lane_format_id`,
  and any manual seed constraints.

Do not hard-code `duration_sec=None` or `aspect_ratio="9:16"`.

Privacy must fail safe:

- `PublicationRecord.current_privacy` always starts as `private` until the
  promote step runs.
- `PublicationRecord.desired_privacy` is the target visibility the promote
  step will apply. Its fallback when no lane/account/format config is found
  must be `unlisted`, not `public`. `_desired_privacy()` must encode this
  fallback explicitly.
- If lane/account configuration asks for public visibility, the publish step
  must still go through promotion/public approval rules.
- Missing config must never resolve to `public` anywhere in the chain.

## 8. Queue Hardening

### 8.1 Channel Scope

Add nullable `channel_profile_id` to `ChannelOpsQueueItem` with an index.

All alpha queue kinds must fill `channel_profile_id`. Every kind in scope for
this pass has a channel context: `agent_tick`, `plan_task`, `execute_task`,
`observe_job`, `publish_task`, `promote_publication`, `collect_metrics`,
`account_health`, and `send_alert` (alert resource is always derivable to a
channel for the four alpha alert types). The field is left nullable on the
column only so legacy rows from before this migration can stay claimable; new
code must always populate it.

Channel dashboard endpoints must filter by `channel_profile_id`. Legacy/null
queue rows must not appear in `/channels/{id}/queue` or health counts. They
may remain claimable by the runner if otherwise valid.

### 8.2 Enqueue Race Handling

`ChannelOpsQueueService.enqueue()` already checks for an existing idempotency
key before insert. It must also catch unique constraint `IntegrityError`,
rollback, then query the existing row and return it. If the row still cannot be
found, re-raise.

### 8.3 Backoff

`mark_failed_or_retry()` must use exponential backoff:

- base: 5 minutes
- multiplier: `2 ** (attempt_count - 1)`
- cap: 30 minutes

Explicit `retry_delay` may still override the default.

## 9. API And Operator Controls

Add missing control/read endpoints:

- `POST /api/v1/channel-agent/publications/{id}/promote`
- `POST /api/v1/channel-agent/publications/{id}/reject`
- `POST /api/v1/channel-agent/accounts/{id}/pause`
- `POST /api/v1/channel-agent/accounts/{id}/resume`
- `POST /api/v1/channel-agent/lanes/{id}/pause`
- `POST /api/v1/channel-agent/lanes/{id}/resume`
- `GET /api/v1/channel-agent/channels/{id}/ticks`

Endpoint behavior:

- `promote`: enqueue `promote_publication` for held/uploaded publication.
- `reject`: mark publication rejected and task rejected or held-cancelled,
  without deleting records.
- account pause/resume: set `enabled`/`paused_until` in a way compatible with
  existing patch APIs.
- lane pause/resume: set `paused_until` and optionally `enabled`.
- ticks: return recent `AgentTickAudit` rows including decision summaries.

Existing patch APIs remain for broad configuration updates.

## 10. Funnel Metrics

`GET /channels/{id}/metrics/funnel` must aggregate real data for the requested
window.

Inputs:

- `ProductionTask` rows for the channel created or updated in the window.
- `PublicationRecord` rows joined through `ProductionTask`.

Source data for each key (alpha implementation; all counts scoped to the
channel and the window `created_at >= now() - days`):

- `seeded`: count of `ManualSeed` rows with `status='active'` in the window
  plus `ProductionTask` rows currently in state `seeded`.
- `selected`, `planning`, `producing`, `uploaded_private`, `scheduled`,
  `published`, `measured`, `failed`, `held`: count of `ProductionTask` rows
  whose **current** `state` equals that bucket. The funnel reports current
  state distribution, not lifetime transitions. Transition history is
  available per task via `transition_history_json` for deeper analysis.

Required output keys (renamed to match `ProductionTask.state` exactly so the
backend SQL is a single `GROUP BY state` query):

- `seeded`
- `selected`
- `planning`
- `producing`
- `uploaded_private`
- `scheduled`
- `published`
- `measured`
- `failed`
- `held`

The endpoint must not return hard-coded zeros. Frontend display labels can
remap `uploaded_private -> uploaded` and `planning -> planned` for brevity.

## 11. Alert Strategy

Alpha push alert delivery is Slack-only.

`CHANNEL_AGENT_ALERT_EMAIL_TO` remains a deferred config field and must not be
presented as a working email transport. The spec and docs must say clearly:

- Slack webhook configured: push alerts are sent.
- Slack webhook missing: alerts are durable queue records and dashboard-visible,
  but no push notification is delivered.
- Email/SMTP delivery is future work.

## 12. Tests

Add focused tests before implementation:

- Lane-driven tick creates a task when no manual seed exists.
- Manual seed is consumed first, then lane-driven fills remaining budget.
- Dry-run evaluates guards and writes rejected candidates without creating
  tasks or queue items.
- AccountConcurrencyGuard blocks when the account has a held task.
- ConsecutiveUploadFailureGuard blocks when 3 of last 5 same-account tasks
  failed within 24h and emits an actionable alert.
- LaneCadenceGuard counts `PublicationRecord`, not `ProductionTask.created_at`.
- `execute_task -> observe_job -> publish_task -> promote -> collect_metrics`
  progresses using fake clients.
- AutoFlow request uses lane format duration, channel aspect ratio,
  source_strategy, planning_mode, and source platform configuration.
- Funnel endpoint aggregates real task/publication state.
- Control APIs pause/resume accounts and lanes, promote/reject publications,
  and list ticks.
- Queue endpoints and health counts are channel-scoped.
- Queue enqueue handles duplicate idempotency races.
- Queue retry delay uses exponential backoff.

Existing full backend tests and frontend build remain required checks.

## 13. Deferred Decisions

These are explicitly not part of this pass:

- Held task TTL: when a task or publication is held for more than N hours,
  automatically move it to failed/cancelled or make it a dashboard severity.
- `ProductionTask.failure_category` enum migration.
- SMTP/email alert transport.
- Real YouTube Analytics collection beyond alpha-safe snapshots.
- Multi-account load balancing beyond the single-active-task guard.
- Full AutoFlow bridge refactor into separate adapter classes.

## 14. Acceptance Criteria

The hardening pass is complete when:

- No alpha-critical queue kind is acknowledged as a no-op.
- A fake-client e2e test starts from `agent_tick` and reaches publication
  scheduling plus metrics snapshot.
- A channel with no manual seeds can generate lane-driven candidates.
- Dry-run audit contains real candidate and guard decisions.
- The three missing guards block and explain unsafe selections.
- Channel queue/health/tick/funnel APIs no longer show cross-channel or fake
  zero data.
- Operator controls exist for account/lane pause/resume and publication
  promote/reject.
- Queue race and retry behavior are covered by tests.
- Full backend tests pass, and frontend build still passes.
