# ChannelOps Agent Implementation Design

Date: 2026-05-18
Status: Approved design, ready for implementation planning
Source design: `docs/superpowers/specs/2026-05-18-channel-ops-agent-design-v2.md`

## 1. Scope Decision

This spec turns the v2 ChannelOps Agent design into the first implementation
slice for VideoProcess.

The first alpha is queue-first and operator-visible:

- Material sourcing may use multiple sources, including local material libraries
  and existing AutoFlow external platform search/download paths.
- Upload and publication target only YouTube.
- The system can automatically schedule public or unlisted YouTube publication
  with `publishAt`, according to lane/account configuration.
- Content ID claims and monetization redirection do not block publication.
- Real strikes, invalid credentials, quota exhaustion, and repeated upload
  failures can block, pause, or hold work.
- Thumbnail generation uses MiniMax image generation as the primary strategy.
  Thumbnail failure is warning-only.
- Metrics are collected and stored for observability only. They do not update
  weights in the alpha.
- Push alerts are mandatory for token expiry, quota pressure, and takedown/strike
  events. A status panel alone is not enough for a long-running agent.
- The UI is an operational status panel with key charts plus dry-run and
  halt/resume controls. It is not a full configuration or review console.
- New channels start in dry-run and require operator promotion after a 7-day
  review window before real task creation/publication is enabled.
- Runtime target is Docker Compose plus external cron/API enqueue. Direct
  production-node rollout is a later phase.

Deferred but explicitly planned:

- Trend/news/RSS ingestion.
- Epsilon-greedy and bandit learning.
- Multi-platform publication beyond YouTube.
- Dedicated production rollout across Mac/NVIDIA nodes and live cron/env sync.

## 2. Architecture

The alpha uses a durable queue boundary instead of FastAPI background scheduling.
FastAPI remains a request and inspection surface. Long-running work is consumed
by a separate runner process.

Primary namespaces:

- `backend/app/channel_agent/`: models, schemas, stores, decision logic, guards,
  queue handlers, publication scheduling, metrics collection, health aggregation.
- `backend/app/api/channel_agent.py`: configuration, control, and operational
  read APIs.
- `backend/channel_agent_runner.py` or equivalent CLI module: consumes queue
  items and advances bounded work steps.
- `frontend/src/pages/ChannelOpsStatus.tsx` and supporting API client/types:
  operational status panel.

Runtime components:

```text
External cron or API
        |
        v
FastAPI enqueue/read APIs
        |
        v
channel_ops_queue_items
        |
        v
channel-agent-runner
        |
        +--> AutoFlow plan/execute/job observation
        +--> MiniMax thumbnail generation
        +--> YouTube upload observation/publishAt/metrics
        +--> Push alerts for token/quota/takedown events
        +--> PublicationRecord and FeedbackSnapshot writes
```

The runner can be started as a Compose service. FastAPI must not depend on
lifespan background tasks for ChannelOps progress.

## 3. Data Model

The implementation adds ChannelOps models instead of overloading
`AutoFlowRun.publish_json`. AutoFlow remains the media planning and execution
record; ChannelOps owns decision, schedule, publication, and feedback history.

### 3.1 Configuration

`ChannelProfile`

- `id`
- `operator_id` or `owner_user_id`, nullable for the single-operator alpha
- `name`
- `positioning`
- `language`
- `default_aspect_ratio`
- `risk_policy_json`
- `content_mix_policy_json`
- `cadence_policy_json`
- `alert_policy_json`, Slack/email delivery config references and thresholds
- `enabled`
- `dry_run`, default `true` for new channels
- `halted_at`, nullable
- `halt_reason`, nullable
- `config_version`
- timestamps

`TopicLane`

- `id`
- `channel_profile_id`
- `name`
- `description`
- `weight`
- `learned_weight`, nullable and unused by alpha decisions
- `keywords_json`
- `negative_keywords_json`
- `min_posts_per_week`
- `max_posts_per_day`
- `max_consecutive_streak`
- `cooldown_after_post_minutes`
- `enabled`
- `paused_until`
- timestamps

`PublishingAccount`

- `id`
- `channel_profile_id`
- `platform`, alpha value is `youtube`
- `account_label`
- `platform_account_id`
- `credential_ref`
- `platform_specific_config_json`
- `default_privacy`, default `public`
- `external_asset_auto_publish`, default `false`
- `enabled`
- `paused_until`
- `last_token_check_at`
- `last_token_check_status`
- timestamps

`LaneFormatMatrix`

- `id`
- `topic_lane_id`
- `format_key`, for example `shorts_9x16`
- `enabled`
- `weight`
- `target_duration_sec`
- `template_pool_json`
- `default_publish_visibility`, `public` or `unlisted`, default `public`
- timestamps

### 3.2 Queue And Audit

`ChannelOpsQueueItem`

- `id`
- `kind`: `agent_tick`, `plan_task`, `execute_task`, `observe_job`,
  `publish_task`, `collect_metrics`, `promote_publication`, `account_health`,
  `send_alert`
- `idempotency_key`, unique
- `priority`, default `100`; lower values run first
- `parent_queue_item_id`, nullable, for work spawned by another queue item
- `payload_json`
- `status`: `queued`, `running`, `succeeded`, `failed`, `held`, `cancelled`,
  `dead_lettered`
- `run_after`
- `locked_at`
- `locked_by`
- `attempt_count`
- `max_attempts`
- `last_error`
- `dead_letter_at`, set when attempts exceed `max_attempts`
- timestamps

`AgentTickAudit`

- `id`
- `channel_profile_id`
- `queue_item_id`
- `tick_id`
- `started_at`
- `finished_at`
- `dry_run`, copied from the channel at tick time
- `ideas_discovered`
- `candidates_scored`
- `tasks_selected`
- `tasks_rejected`
- `guards_triggered_json`
- `decision_summary_json`
  - must include `per_lane_eligible_count` and per-lane rejection counts
- `error_message`

### 3.3 Production

`ManualSeed`

- `id`
- `channel_profile_id`
- `topic_lane_id`, nullable
- `target_account_id`, nullable
- `prompt`
- `title_seed`
- `source_policy`
- `source_platforms_json`
- `material_library_ids_json`
- `constraints_json`
- `status`: `active`, `exhausted`, `cancelled`
- timestamps

A single manual seed may fan out to multiple `ProductionTask` rows, for example
different format variants. Do not mark a seed exhausted until the configured
fan-out budget is spent or the operator cancels it.

`ProductionTask`

- `id`
- `task_group_id`, nullable
- `channel_profile_id`
- `topic_lane_id`, nullable
- `lane_format_id`, nullable
- `target_account_id`
- `manual_seed_id`, nullable
- `source`: `manual_seed` or `lane_seed` in the alpha
- `title_seed`
- `prompt`
- `rationale_json`
- `score_breakdown_json`
- `portfolio_bucket`: `exploit`, `explore`, or `wildcard`
- `source_platforms_json`
- `material_library_ids_json`
- `uses_external_assets`, default `false` until known
- `autoflow_plan_id`
- `autoflow_run_id`
- `pipeline_id`
- `job_id`
- `scheduled_at`
- `priority`
- `state`
- `state_updated_at`
- `failure_reason`
- `retry_count`
- `blocked_by_guard`
- `channel_config_version_snapshot`
- `channel_config_snapshot_json`, including at least cadence policy, risk policy,
  content mix policy, lane settings, account publish settings, and lane-format
  settings used when the task was selected
- `transition_history_json`
- timestamps

State machine:

```text
seeded -> selected -> planning -> producing -> uploaded_private -> scheduled -> published -> measured
```

Additional states:

- `held`: needs operator or configuration intervention.
- `failed`: terminal until retry policy or manual reset.
- `cancelled`: terminal admin action.
- `rejected`: terminal selection rejection.

Each transition records actor, queue item id, timestamp, previous state, next
state, and reason in `transition_history_json`. Do not introduce a separate
transition table in the alpha migration.

`MaterialUsageLedger`

- `id`
- `material_id`
- `asset_id`
- `channel_profile_id`
- `topic_lane_id`
- `publishing_account_id`
- `publication_id`, nullable until publication exists
- `used_at`
- `segment_signature`
- `metadata_json`

### 3.4 Distribution And Feedback

`PublicationRecord`

- `id`
- `production_task_id`
- `platform`, alpha value `youtube`
- `account_id`
- `platform_content_id`
- `permalink`
- `title`
- `description`
- `tags_json`
- `thumbnail_storage_path`
- `desired_privacy`: `public` or `unlisted`
- `current_privacy`: starts as `private`
- `publish_status`: `uploaded`, `scheduled`, `public`, `unlisted`, `held`,
  `removed`, `failed`
- `uploaded_at`
- `scheduled_publish_at`
- `public_at`
- `compliance_disposition`, non-null
  - `manual_seed` default: `assumed_fair_use`
  - `lane_seed` default: `known_risk_accepted`
- `quota_units_estimated`
- `last_metrics_polled_at`
- `warnings_json`
- timestamps

`TakedownEvent`

- `id`
- `publication_id`
- `event_type`: `content_id_claim`, `strike`, `restriction`, `takedown`
- `detected_at`
- `severity`: `info`, `warning`, `severe`
- `raw_payload_json`
- `auto_actions_taken_json`

`FeedbackSnapshot`

- `id`
- `publication_id`
- `collected_at`
- `views`
- `likes`
- `comments`
- `shares`
- `avg_view_duration_sec`
- `retention_curve_json`
- `ctr`
- `impressions`
- `virality_score`
- `raw_json`

### 3.5 Deferred Schema Slots

Do not create these tables in the alpha migration, but keep the design slots so
future phases do not reinterpret the model:

`AccountFingerprint`

- `publishing_account_id`
- `last_upload_at`
- `upload_timing_jitter_seconds`
- `template_pool_recent_json`
- `daily_upload_count_json`
- `last_ip_class`

`MaterialInventoryForecast`

- `channel_profile_id`
- `topic_lane_id`
- `lane_format_id`
- `eligible_material_count`
- `weekly_consumption_rate`
- `days_of_supply`
- `last_computed_at`

## 4. Queue Runner Design

The runner polls queue rows whose `status=queued` and `run_after <= now`,
ordered by `priority` then `created_at`. On PostgreSQL it claims work with
`SELECT ... FOR UPDATE SKIP LOCKED` inside a short transaction. SQLite tests may
use a deterministic single-runner fallback, but production should not require a
Redis lock for the first alpha.

Each claimed row executes one bounded handler, then becomes `succeeded`,
`held`, `failed`, or `dead_lettered`. Rows that exceed `max_attempts` move to
dead letter with `dead_letter_at` set instead of staying in an ambiguous failed
loop.

Queue item kinds:

- `agent_tick`: create selected `ProductionTask` rows from manual seeds and
  conservative lane-generated candidates unless the channel is in dry-run.
  Dry-run ticks write `AgentTickAudit` only.
- `plan_task`: call AutoFlow planning and persist `autoflow_plan_id`.
- `execute_task`: call AutoFlow execution and persist run/job ids.
- `observe_job`: observe media job completion and extract artifact/upload output.
- `publish_task`: create or update `PublicationRecord`, generate thumbnail,
  apply YouTube thumbnail when possible, and enqueue `promote_publication` when
  guards allow.
- `collect_metrics`: poll YouTube metrics and write `FeedbackSnapshot`.
- `promote_publication`: apply YouTube `publishAt` or visibility changes.
  Default creation is automatic: `publish_task` enqueues it after private upload
  observation and guard approval. Manual promote APIs enqueue the same kind for
  held publications.
- `account_health`: check token/quota state when supported.
- `send_alert`: deliver Slack/email alerts with retry and idempotency.

Idempotency requirements:

- `agent_tick:<channel_id>:<YYYY-MM-DD-HH>` for scheduled ticks, or
  `agent_tick:<channel_id>:manual:<request_id>` for manual enqueue.
- `plan_task:<production_task_id>` must not create multiple AutoFlow plans.
- `execute_task:<production_task_id>` must not create multiple jobs for the same
  plan unless a retry policy explicitly cancels the old attempt.
- `observe_job:<production_task_id>:<job_id>` must not regress task state after
  a job has already been observed as terminal.
- `publish_task:<production_task_id>` must not upload duplicate videos when a
  `PublicationRecord.platform_content_id` already exists.
- `promote_publication:<publication_id>:<target_visibility>:<scheduled_at_iso>`
  must not create duplicate scheduling calls for the same target.
- `collect_metrics:<publication_id>:<YYYY-MM-DD-HH>` uses a UTC hour bucket and
  may run repeatedly, but must append snapshots predictably and avoid corrupting
  existing data.
- `account_health:<account_id>:<YYYY-MM-DD-HH>` uses a UTC hour bucket.
- `send_alert:<alert_type>:<resource_id>:<YYYY-MM-DD-HH>` suppresses duplicate
  alert storms while preserving one alert per resource/type/hour.

Retry handling:

- Network/API transient failures retry with backoff.
- Missing credentials, invalid OAuth, exhausted quota, and validation failures
  become held or failed with explicit reasons.
- MiniMax thumbnail failure records a warning and continues.
- Content ID claims and monetization redirection record events but do not block.
- Strikes and repeated upload failures may pause an account or lane.

## 5. Selection And AutoFlow Boundary

The alpha has two candidate sources:

1. `ManualSeed`: operator/API provided prompt, lane, account, materials, and
   constraints.
2. Lane-driven conservative seeds: generated from `TopicLane.keywords_json`,
   `LaneFormatMatrix.template_pool_json`, and channel defaults when no manual
   seed is available.

Every tick must compute `per_lane_eligible_count` before selection and write it
to `AgentTickAudit.decision_summary_json`. If a lane has fewer than the
configured candidate threshold for three consecutive ticks, the agent sends a
push alert and marks the lane as supply-constrained in health output. The alpha
does not need a separate `MaterialInventoryForecast` table.

Automated trend/news/RSS ingestion is not part of the alpha. Existing
`TrendSignal` rows may be read opportunistically if present, but the alpha does
not own trend ingestion.

ChannelOps compiles a `ProductionTask` into an `AutoFlowRequest`. It must not
let LLM output directly define arbitrary workflow graphs. AutoFlow continues to
own candidate selection, clip ranking, template/graph construction, repair, and
`validate_pipeline()` compliance.

The alpha must pass through these AutoFlow fields where applicable:

- `prompt`
- `target_platforms`, alpha upload target `youtube`
- `source_platforms`
- `duration_sec`
- `aspect_ratio`
- `source_policy`
- `publish_mode`
- `material_library_ids`
- `source_strategy`
- `constraints`
- `planning_mode`

Publish mode behavior:

- Alpha must use AutoFlow's existing `youtube_upload` node as the only upload
  path. ChannelOps compiles tasks with an upload-capable mode such as
  `private_upload` or `public_after_review`, then validates that the generated
  pipeline contains exactly one YouTube upload node.
- ChannelOps must not duplicate uploads. It observes AutoFlow job outputs for
  the YouTube video id and upload metadata.
- If an alpha plan omits the upload node, `plan_task` or `publish_task` fails or
  holds the task with `missing_youtube_upload_node`. Do not fall back to an
  independent upload path in the alpha. Any non-AutoFlow upload path is a future
  extension only.

External assets:

- External material sourcing can be used through existing AutoFlow source
  platform paths.
- If `uses_external_assets=true`, automatic `publishAt` requires explicit
  `external_asset_auto_publish=true` on the account or equivalent channel risk
  policy.
- Without that explicit acceptance, the video can upload private but the
  publication becomes `held` before public scheduling.

## 6. YouTube Publication

YouTube is the only alpha publication target. Multi-platform metadata and
publication are deferred.

Publication flow:

1. AutoFlow executes a workflow that contains one `youtube_upload` node.
2. Runner observes the completed AutoFlow run/job and reads the YouTube upload
   result from job artifacts or node output.
3. Runner creates `PublicationRecord` with `current_privacy=private`.
4. Runner requests MiniMax thumbnail generation.
5. If thumbnail succeeds, runner stores the image artifact/path and calls
   YouTube `thumbnails.set`.
6. Runner chooses target visibility from account/lane config, default `public`.
7. If guards allow, `publish_task` automatically enqueues `promote_publication`.
8. `promote_publication` schedules publication with YouTube `publishAt`.
9. Runner records quota estimates, warnings, and publication status.

The current worker `YouTubeUploadHandler` handles private/unlisted upload and
quota estimation. The alpha must keep that as the only video upload path. It may
add a service-level YouTube publication client for thumbnail updates and
`publishAt` scheduling, but it must not add a second video upload path.

Blocking and non-blocking outcomes:

- Non-blocking: MiniMax failure, Content ID claim, monetization redirection,
  partial metrics unavailability.
- Held/failed: invalid OAuth, missing credential ref, quota exhaustion, upload
  failure after retries, missing explicit external asset risk acceptance, strike
  threshold.

## 7. MiniMax Thumbnail Generation

Thumbnail generation uses MiniMax as the primary strategy. The runner reads
`MINIMAX_API_KEY` from runtime environment/config and never stores or logs the
secret.

Implementation boundary:

- New thumbnail service under `backend/app/channel_agent/thumbnail.py` or
  equivalent.
- Input: title, prompt, storyboard/metadata, lane/account style hints, and
  optional frame/reference paths if available.
- Output: stored image artifact/path plus provider metadata.
- Failure: append warning to `PublicationRecord.warnings_json` and continue.

The current MiniMax image generation API endpoint documented by MiniMax is
`https://api.minimaxi.com/v1/image_generation`. Model choice should be
configurable, with `image-01` as the initial default unless runtime testing
shows a better model for thumbnails. Implementation must re-check the latest
MiniMax documentation before coding the client because provider APIs and model
names can drift.

Initial operational limits:

- Timeout: 30 seconds.
- Retry: 1 retry for transient failures.
- Concurrency cap: 2 requests per second per runner process.
- Secrets: `MINIMAX_API_KEY` only from runtime config/env; never logs or repo.

## 8. Metrics And Learning

The alpha collects metrics but does not learn from them automatically.

Metrics collector:

- Polls YouTube content metrics for recent publications.
- Writes `FeedbackSnapshot`.
- Updates `PublicationRecord.last_metrics_polled_at`.
- Records API failures in queue item errors and health output.

Suggested cadence:

- Content younger than 24 hours: frequent polling, for example hourly.
- Content 1 to 7 days old: moderate polling, for example every 6 hours.
- Content 7 to 30 days old: daily polling.
- Older content: stop or run only on explicit request.

Learning is deferred:

- Alpha does not update `TopicLane.learned_weight`.
- Alpha does not run epsilon-greedy or bandit selection.
- Later phases should use collected `FeedbackSnapshot` history as the input
  dataset for epsilon-greedy and then Thompson sampling.

## 9. Push Alerts

The alpha must include push alerts. A read-only dashboard is insufficient for
failures that require immediate operator action.

Minimum alert delivery:

- Slack webhook if `CHANNEL_AGENT_SLACK_WEBHOOK_URL` is configured.
- Email if SMTP settings and `CHANNEL_AGENT_ALERT_EMAIL_TO` are configured.
- If both are configured, send to both.

Minimum alpha alert types:

- `token_expiring_24h`: account token expires within 24 hours or refresh fails.
- `quota_below_20pct`: estimated remaining YouTube quota falls below 20%.
- `takedown_event_logged`: any `TakedownEvent` is inserted; severity controls
  wording, not whether the alert is sent.
- `material_supply_low`: a lane has fewer than the configured eligible
  candidates for three consecutive ticks.

Alert sending should be a queue item (`send_alert`) so transient Slack/SMTP
failures can retry without blocking the originating tick/publish/health item.
Secrets such as Slack webhook URLs and SMTP passwords are runtime-only.

## 10. API Surface

Use prefix `/api/v1/channel-agent` unless a repo convention suggests a better
name during implementation.

Configuration APIs:

- `POST /channels`
- `GET /channels`
- `GET /channels/{channel_id}`
- `PATCH /channels/{channel_id}`
- `POST /channels/{channel_id}/lanes`
- `PATCH /channels/{channel_id}/lanes/{lane_id}`
- `POST /channels/{channel_id}/accounts`
- `PATCH /channels/{channel_id}/accounts/{account_id}`
- `POST /lanes/{lane_id}/formats`
- `PATCH /lane-formats/{lane_format_id}`

Control APIs:

- `POST /channels/{channel_id}/manual-seeds`
- `POST /channels/{channel_id}/enqueue-tick`
- `POST /channels/{channel_id}/halt`
- `POST /channels/{channel_id}/resume`
- `PATCH /channels/{channel_id}/dry-run`
- `POST /publications/{publication_id}/enqueue-metrics`
- `POST /accounts/{account_id}/pause`
- `POST /accounts/{account_id}/resume`
- `POST /lanes/{lane_id}/pause`
- `POST /lanes/{lane_id}/resume`
- `POST /publications/{publication_id}/promote`
- `POST /publications/{publication_id}/reject`

Read APIs:

- `GET /channels/{channel_id}/health`
- `GET /channels/{channel_id}/queue`
- `GET /channels/{channel_id}/ticks`
- `GET /channels/{channel_id}/tasks`
- `GET /tasks/{task_id}`
- `GET /channels/{channel_id}/publications`
- `GET /publications/{publication_id}`
- `GET /channels/{channel_id}/metrics/funnel?days=7`
- `GET /channels/{channel_id}/lanes/health`
- `GET /channels/{channel_id}/accounts/health`

Auth is out of scope for the self-use alpha. Do not build full multi-tenant
auth. Keep model fields flexible enough for future user isolation.

## 11. Frontend

Add an operational ChannelOps status panel. It should not include full
configuration editing or a complete review console in the alpha, but it must
include the minimal controls needed to stop unsafe automation.

Required views:

- Health cards: queue status, active tasks, publications today, recent failures.
- 7-day funnel: seeded/scored/selected/planned/produced/uploaded/scheduled/
  published/measured.
- Account status: YouTube account label, quota estimate, token status,
  paused state, recent failures.
- Lane trend: publication count and failure count by lane.
- Tables: queue items, production tasks, publications.
- Detail drawer or page: raw JSON details for a selected task/publication/tick.
- Dry-run flag visibility and a control to flip it only after the dry-run SOP.
- Halt/resume controls for the channel, with required reason text on halt.

The page should use existing React/Vite patterns and API client conventions.

## 12. Guards

The alpha guards are operational, not IP-preventive.

Required guards:

- Idempotency guard: prevent duplicate queue/tick/task execution.
- Account concurrency guard: one active `ProductionTask` per YouTube account.
- Quota guard: hold or delay when quota estimate is too high.
- Token health guard: hold when OAuth is invalid or missing.
- External asset risk acceptance guard: require explicit config before
  auto-scheduling external-source publications.
- Strike guard: pause account or lane when strike threshold is reached.
- Consecutive upload failure guard: pause account after repeated upload failures.
- Lane cadence/streak guard: avoid exceeding configured lane cadence.
- Material supply guard: alert when a lane is repeatedly below candidate
  threshold; do not require a separate forecast table in the alpha.

Claim and monetization restrictions should be recorded but should not block
publication by themselves.

## 13. Operational SLAs And Halt Policy

Alpha SLAs are observation and alert thresholds. They do not automatically halt
the whole agent unless a specific guard says so. Beta can promote these into
automatic halt rules.

Initial targets:

| Metric | Alpha target | Alpha action |
|---|---:|---|
| Tick success rate | >= 99% | alert if below target |
| Selected -> uploaded success | >= 90% | alert if below target |
| Upload failure rate per account-day | <= 5% | alert; pause account at guard threshold |
| Metrics collection lag | < 2h to first snapshot | alert if above target |
| Mean tick wall-clock duration | < 60s | alert if above target |
| Dashboard page load | < 1s | observe only |

Automatic pause thresholds in alpha:

- Account strike threshold reached.
- Repeated upload failures on one account.
- Token refresh failure for an account.
- Quota guard says an upload would exceed the configured budget.

## 14. Testing

Add focused tests for every new service and state boundary.

Backend tests:

- SQLAlchemy model and migration smoke tests.
- ProductionTask state transition tests.
- Queue claim, retry, lock, and idempotency tests.
- Agent tick selection tests for manual seeds and lane-driven candidates.
- Account concurrency guard tests.
- External asset auto-publish config tests.
- AutoFlow request compilation tests.
- PublicationRecord creation from observed AutoFlow upload output.
- MiniMax client success/failure tests with a fake client.
- YouTube publication client success/failure tests with a fake client.
- Metrics collector tests with a fake YouTube metrics client.
- API tests for configuration, manual seed, enqueue, health, and read surfaces.
- FakeClock support for metrics cadence, `paused_until`, dry-run windows, and
  retry/backoff assertions.
- Push alert tests for Slack/email payload creation and idempotency.

Required CI scenarios:

```text
manual seed -> enqueue tick -> ProductionTask selected -> AutoFlow plan stub
-> execute stub -> observed upload result -> MiniMax thumbnail warning or success
-> PublicationRecord scheduled -> metrics snapshot collected
```

- Strike auto-pause: inject `TakedownEvent(severity=severe)` and assert the
  account is paused and the same lane/account does not enqueue more publish
  work.
- Quota exhaustion: simulate quota at 95% and assert `publish_task` becomes
  held instead of retrying uploads until failure.
- Token invalid: simulate OAuth refresh failure and assert `account_health`
  holds the account and enqueues a push alert.
- Dry-run: run a 7-day FakeClock scenario and assert ticks write audits without
  creating real tasks until the operator flips `dry_run=false`.

Frontend tests/build:

- `npm run build`
- Lint if available, non-blocking per repo instructions.
- Component/API smoke coverage where the existing frontend test setup supports it.

## 15. Runtime And Deployment

Alpha runtime target:

- Docker Compose can run API, media worker, and `channel-agent-runner`.
- External cron or manual API calls enqueue tick and metrics work.
- Runner can run continuously or as a bounded one-shot command for cron.
- Secrets are runtime-only: YouTube credentials, OAuth tokens, MiniMax API key.

Do not hard-code secrets. Do not print secrets. Do not commit local credential
paths or tokens.

Production-node rollout is deferred. The later deployment phase must update
live cron, env files, Compose overrides, and Mac/NVIDIA node responsibilities
only after the Compose alpha is verified.

Dry-run SOP:

- New channels default to `dry_run=true`.
- Before Phase 7 verification is considered complete, a new channel must run
  dry-run ticks for 7 calendar days.
- Each day the operator reviews `AgentTickAudit`, held publications, alert
  history, and per-lane decision rationale.
- Only an explicit `PATCH /channels/{id}/dry-run` to `false` enables real
  `ProductionTask` creation and downstream queueing.
- Halt/resume remains available after dry-run is disabled.

## 16. Implementation Phases

Phase 1a: Config model migration

- Add `ChannelProfile`, `TopicLane`, `PublishingAccount`, `LaneFormatMatrix`.
- Add Pydantic schemas and store tests.

Phase 1b: Queue and audit model migration

- Add `ChannelOpsQueueItem` and `AgentTickAudit`.
- Add queue claim/retry/dead-letter tests.

Phase 1c: Production, distribution, and feedback model migration

- Add `ManualSeed`, `ProductionTask`, `MaterialUsageLedger`,
  `PublicationRecord`, `TakedownEvent`, `FeedbackSnapshot`.
- Add state transition helpers and store tests.

Phase 2: Queue and runner

- Implement queue services on top of `ChannelOpsQueueItem`.
- Add claim/retry/idempotency/dead-letter logic.
- Add `SELECT ... FOR UPDATE SKIP LOCKED` claim path for PostgreSQL.
- Add runner CLI and Compose service wiring.
- Add queue tests.

Phase 3: Config, seed, and read APIs

- Add API router.
- Add manual seed APIs.
- Add channel/lane/account/lane-format APIs.
- Add health/read APIs with basic aggregates.
- Add dry-run, halt, and resume controls.

Phase 4: Agent tick and AutoFlow bridge

- Implement manual seed and lane-driven candidate generation.
- Implement scoring and account concurrency guard.
- Compile tasks into `AutoFlowRequest`.
- Plan/execute/observe AutoFlow through queue handlers.

Phase 5: YouTube publication and MiniMax thumbnail

- Observe existing YouTube upload outputs from AutoFlow jobs.
- Add publication client support for thumbnail and `publishAt`.
- Add MiniMax thumbnail service.
- Add `PublicationRecord` lifecycle.
- Add push alerts for token, quota, takedown, and low material supply.

Phase 6: Metrics collection and operational frontend

- Add YouTube metrics collector and fake test client.
- Add frontend status panel with health cards, funnel, account/lane trends,
  task/publication/queue tables, dry-run/halt controls, and JSON detail view.

Phase 7: Compose alpha verification

- Add or update Compose service for `channel-agent-runner`.
- Document env vars and cron examples.
- Run backend and frontend checks.
- Run an end-to-end fake-client scenario.
- Run the 7-day dry-run SOP with FakeClock in CI and document the real-operator
  checklist.

Later phases:

- Trend/news/RSS ingester writing `TrendSignal`.
- Epsilon-greedy and bandit learning using `FeedbackSnapshot`.
- Multi-platform metadata and publication.
- Full operator configuration/review UI.
- Live production node rollout and cron/env sync.
- Deferred migrations for `AccountFingerprint` and `MaterialInventoryForecast`.

## 17. Deferred Decisions

These decisions are intentionally not solved in the alpha. If implementation
hits one of them, pause and update the design instead of making an ad hoc
choice in code:

- Multi-tenant deployment and auth boundaries.
- A/B variant generation for one `task_group_id`.
- Cross-channel material sharing rules.
- Bilibili, X, and Xiaohongshu publication parity.
- Manual override priority and whether override outcomes feed learning.
- Localization, including whether zh/en variants are one task or siblings.
- Full operator UX beyond the alpha status/control panel.
- Cost ceilings for LLM, MiniMax, storage, and API calls.
- Account fingerprinting strategy across multiple YouTube accounts.

## 18. Acceptance Criteria

The alpha is complete when:

- A channel with lanes, account config, and lane formats can be created via API
  or seed script.
- A manual seed can enqueue a tick and create a selected `ProductionTask`.
- The runner can progress the task through AutoFlow planning/execution using
  fake or local-safe clients in tests.
- A completed task can create a `PublicationRecord` by observing AutoFlow's
  `youtube_upload` result, with no second video upload path.
- MiniMax thumbnail failure does not block scheduling.
- Automatic `publishAt` respects target visibility and external asset risk
  acceptance config.
- Push alerts fire for token expiry/refresh failure, quota below 20%, takedown
  events, and repeated material shortage.
- Strike, quota exhaustion, and token invalid CI scenarios pass.
- A new channel can complete the 7-day dry-run SOP and requires explicit
  operator promotion before real task creation.
- YouTube metrics snapshots can be collected with a fake client and exposed in
  read APIs.
- The operational status panel shows health, funnel, account status, lane trends,
  dry-run/halt status, tasks, queue items, publications, and JSON details.
- Docker Compose can run the API, worker, and channel-agent-runner.
- Backend tests for the new services pass.
- AutoFlow-generated workflows still pass `validate_pipeline()`.
- Default publication privacy remains private until ChannelOps intentionally
  schedules or promotes the video according to lane/account policy.
