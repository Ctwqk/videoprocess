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
- The UI is a read-only status panel with key charts, not a full configuration
  or review console.
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
  queue handlers, publication bridge, metrics collection, health aggregation.
- `backend/app/api/channel_agent.py`: configuration, control, and read-only APIs.
- `backend/channel_agent_runner.py` or equivalent CLI module: consumes queue
  items and advances bounded work steps.
- `frontend/src/pages/ChannelOpsStatus.tsx` and supporting API client/types:
  read-only operational panel.

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
        +--> YouTube upload/publishAt/metrics
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
- `enabled`
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
  `publish_task`, `collect_metrics`, `promote_publication`, `account_health`
- `idempotency_key`, unique
- `payload_json`
- `status`: `queued`, `running`, `succeeded`, `failed`, `held`, `cancelled`
- `run_after`
- `locked_at`
- `locked_by`
- `attempt_count`
- `last_error`
- timestamps

`AgentTickAudit`

- `id`
- `channel_profile_id`
- `queue_item_id`
- `tick_id`
- `started_at`
- `finished_at`
- `dry_run`, default `false` for this alpha once enabled
- `ideas_discovered`
- `candidates_scored`
- `tasks_selected`
- `tasks_rejected`
- `guards_triggered_json`
- `decision_summary_json`
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
- `status`: `open`, `consumed`, `rejected`, `cancelled`
- timestamps

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
state, and reason in a transition table or transition JSON audit trail.

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
- `compliance_disposition`
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

## 4. Queue Runner Design

The runner polls queue rows whose `status=queued` and `run_after <= now`,
claims them with a lock, executes one bounded handler, then marks the row
succeeded, held, or failed.

Queue item kinds:

- `agent_tick`: create selected `ProductionTask` rows from manual seeds and
  conservative lane-generated candidates.
- `plan_task`: call AutoFlow planning and persist `autoflow_plan_id`.
- `execute_task`: call AutoFlow execution and persist run/job ids.
- `observe_job`: observe media job completion and extract artifact/upload output.
- `publish_task`: create or update `PublicationRecord`, generate thumbnail,
  apply YouTube thumbnail, and schedule publication.
- `collect_metrics`: poll YouTube metrics and write `FeedbackSnapshot`.
- `promote_publication`: manually or automatically apply publish visibility
  changes when needed.
- `account_health`: check token/quota state when supported.

Idempotency requirements:

- `plan_task:<production_task_id>` must not create multiple AutoFlow plans.
- `execute_task:<production_task_id>` must not create multiple jobs for the same
  plan unless a retry policy explicitly cancels the old attempt.
- `publish_task:<production_task_id>` must not upload duplicate videos when a
  `PublicationRecord.platform_content_id` already exists.
- `collect_metrics:<publication_id>:<window>` may run repeatedly but must append
  snapshots predictably and avoid corrupting existing data.

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

- AutoFlow should use an upload-capable mode such as `private_upload` or
  `public_after_review` so its existing `youtube_upload` node can produce a
  private upload when appropriate.
- ChannelOps must not duplicate uploads. It first observes AutoFlow job outputs
  for YouTube upload results. If a future plan omits an upload node, a separate
  publication bridge may upload the final artifact, still under the same
  idempotency key.

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

1. Runner observes a completed AutoFlow run/job and final artifact.
2. Runner reads any YouTube upload result from job artifacts or node output.
3. Runner creates `PublicationRecord` with `current_privacy=private`.
4. Runner requests MiniMax thumbnail generation.
5. If thumbnail succeeds, runner stores the image artifact/path and calls
   YouTube `thumbnails.set`.
6. Runner chooses target visibility from account/lane config, default `public`.
7. If guards allow, runner schedules publication with YouTube `publishAt`.
8. Runner records quota estimates, warnings, and publication status.

The current worker `YouTubeUploadHandler` handles private/unlisted upload and
quota estimation. The alpha should extend YouTube publication support without
breaking existing AutoFlow APIs. Scheduling and thumbnail updates can be
implemented as a service-level YouTube publication client used by the runner,
or as compatible worker-handler extensions if that fits the existing execution
path better.

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
shows a better model for thumbnails.

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

## 9. API Surface

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

## 10. Frontend

Add a read-only ChannelOps status panel. It should not include full
configuration editing or a complete review console in the alpha.

Required views:

- Health cards: queue status, active tasks, publications today, recent failures.
- 7-day funnel: seeded/scored/selected/planned/produced/uploaded/scheduled/
  published/measured.
- Account status: YouTube account label, quota estimate, token status,
  paused state, recent failures.
- Lane trend: publication count and failure count by lane.
- Tables: queue items, production tasks, publications.
- Detail drawer or page: raw JSON details for a selected task/publication/tick.

The page should use existing React/Vite patterns and API client conventions.

## 11. Guards

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

Claim and monetization restrictions should be recorded but should not block
publication by themselves.

## 12. Testing

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

Integration scenario:

```text
manual seed -> enqueue tick -> ProductionTask selected -> AutoFlow plan stub
-> execute stub -> observed upload result -> MiniMax thumbnail warning or success
-> PublicationRecord scheduled -> metrics snapshot collected
```

Frontend tests/build:

- `npm run build`
- Lint if available, non-blocking per repo instructions.
- Component/API smoke coverage where the existing frontend test setup supports it.

## 13. Runtime And Deployment

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

## 14. Implementation Phases

Phase 1: Data model, migrations, schemas, stores

- Add ChannelOps models and Alembic migration.
- Add Pydantic schemas.
- Add state transition helpers and store tests.

Phase 2: Queue and runner

- Add `ChannelOpsQueueItem`.
- Add claim/retry/idempotency logic.
- Add runner CLI and Compose service wiring.
- Add queue tests.

Phase 3: Config, seed, and read APIs

- Add API router.
- Add manual seed APIs.
- Add channel/lane/account/lane-format APIs.
- Add health/read APIs with basic aggregates.

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

Phase 6: Metrics collection and read-only frontend

- Add YouTube metrics collector and fake test client.
- Add frontend status panel with health cards, funnel, account/lane trends,
  task/publication/queue tables, and JSON detail view.

Phase 7: Compose alpha verification

- Add or update Compose service for `channel-agent-runner`.
- Document env vars and cron examples.
- Run backend and frontend checks.
- Run an end-to-end fake-client scenario.

Later phases:

- Trend/news/RSS ingester writing `TrendSignal`.
- Epsilon-greedy and bandit learning using `FeedbackSnapshot`.
- Multi-platform metadata and publication.
- Full operator configuration/review UI.
- Live production node rollout and cron/env sync.

## 15. Acceptance Criteria

The alpha is complete when:

- A channel with lanes, account config, and lane formats can be created via API
  or seed script.
- A manual seed can enqueue a tick and create a selected `ProductionTask`.
- The runner can progress the task through AutoFlow planning/execution using
  fake or local-safe clients in tests.
- A completed task can create a `PublicationRecord` for YouTube without
  duplicate upload behavior.
- MiniMax thumbnail failure does not block scheduling.
- Automatic `publishAt` respects target visibility and external asset risk
  acceptance config.
- YouTube metrics snapshots can be collected with a fake client and exposed in
  read APIs.
- The read-only status panel shows health, funnel, account status, lane trends,
  tasks, queue items, publications, and JSON details.
- Docker Compose can run the API, worker, and channel-agent-runner.
- Backend tests for the new services pass.
- AutoFlow-generated workflows still pass `validate_pipeline()`.
- Default publication privacy remains private until ChannelOps intentionally
  schedules or promotes the video according to lane/account policy.
