# ChannelOps Live Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut ChannelOps over from fake-client alpha scaffolding to a guarded live YouTube loop that can upload, promote, reconcile, measure, avoid repeated material, and eventually schedule its own work.

**Architecture:** Execute this as gated sprint slices, not as one monolithic branch. Keep OAuth and YouTube Data API ownership in the existing `youtube-manager` service; ChannelOps talks to it through a typed HTTP client. Keep AutoFlow graph execution deterministic and review-gated: agent approval can satisfy review-required upload gates only when PDS explicitly allows it, while public publication remains manual.

**Tech Stack:** FastAPI, SQLAlchemy async ORM, Alembic, httpx, Pydantic v2, pytest + aiosqlite/httpx MockTransport, React/Vite/TypeScript, Docker Compose, existing `youtube-manager` FastAPI service.

---

## Source Spec

- `docs/superpowers/specs/2026-05-19-channel-ops-live-cutover-design.md`

## Scope Decision

This spec covers several coupled but independently releasable subsystems:

- Sprint 0: live manual smoke, no code required unless the smoke finds bugs.
- Sprint 1: live YouTube client wiring, metrics fetch, audit uniqueness, live-loop indicator.
- Sprint 2: agent approval bridge, PDS fail-closed publish behavior, status reconciliation.
- Sprint 3: material usage ledger writes and repetition guards.
- Sprint 4: internal scheduler, one YouTube trend source, score writeback, retention cleanup.

The implementation must keep these as separate PR-sized branches or commits. Sprint 0 blocks all code merge. Sprint 1 blocks Sprint 2 because metrics/status client contracts must be proven before publish reconciliation can be trusted. Sprint 3 can start after Sprint 1 but should merge after Sprint 2 so guard outcomes and funnel semantics are stable. Sprint 4 starts only after the live loop has at least one zero-incident run.

## Approach Options

**Recommended: gate-driven sprint split.** Implement Sprint 0, then Sprint 1, then Sprint 2-4 in order. This minimizes live-account risk, produces useful acceptance gates, and avoids guessing around YouTubeManager contracts before the first real smoke.

**Alternative: backend-only batch.** Implement Sprints 1-3 before any live smoke. This is faster on paper but likely hides OAuth, endpoint, and data-shape drift until a larger change set is harder to debug.

**Alternative: runtime patch only.** Wire `YouTubeManagerClient` and metrics fetch, skip approval/PDS/ledger/scheduler. This can prove one upload quickly but does not solve unattended operation and leaves known fail-open/repetition holes.

Use the recommended approach.

## File Map

### Sprint 0

- Read/write through existing API routes in `backend/app/api/channel_agent.py`.
- Use queue runner entrypoint `backend/channel_agent_runner.py`.
- Use Docker services in `docker-compose.yml`.
- Use `youtube-manager` source at `${PLATFORM_UPLOAD_ROOT:-../constructure-platform-upload}/YouTubeManager`; the verified runtime source for this smoke is `/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager`.

### Sprint 1

- Modify `backend/app/channel_agent/clients.py`
  - Extend `YouTubeClient` with `fetch_metrics()` and `fetch_status()`.
  - Add `YouTubeManagerClient` using `httpx.AsyncClient`.
  - Extend `FakeYouTubeClient` for metrics/status tests.
- Modify `backend/app/channel_agent/runner.py`
  - Inject `YouTubeManagerClient`, `MiniMaxImageClient`, `LocalAutoFlowClient`, and PDS client explicitly.
  - Fail startup when `YOUTUBE_MANAGER_URL` is empty in live runner mode.
- Modify `backend/app/channel_agent/service.py`
  - Fetch metrics from YouTube when queue payload lacks real metrics.
  - Preserve existing `_MAX_METRICS_POLLS` hold behavior.
- Modify `backend/app/models/channel_agent.py`
  - Add unique constraint to `AgentTickAudit`.
- Create `backend/alembic/versions/014_channel_ops_live_loop.py`
  - Add `uq_agent_tick_audit_channel_tick`.
- Modify `backend/app/api/channel_agent.py`
  - Add `last_successful_measured_at` to channel health/summary response.
- Modify `backend/app/schemas/channel_agent.py`
  - Add the field to `HealthSummary`.
- Modify `frontend/src/api/channelAgent.ts`
  - Type `last_successful_measured_at`.
- Modify `frontend/src/pages/ChannelOpsStatusPage.tsx`
  - Render amber/red live-loop recency pill.
- If the actual YouTubeManager source is present, modify `${PLATFORM_UPLOAD_ROOT}/YouTubeManager/app/main.py`
  - Add quota, schedule, token refresh, metrics, and status endpoints.
  - If absent in this worktree, modify `_archive/YouTubeManager/app/main.py` and later port the patch to the runtime source before live deployment.
- Test `backend/tests/channel_agent/test_service.py`
- Test `backend/tests/channel_agent/test_runner.py`
- Test `backend/tests/channel_agent/test_api.py`
- Test `backend/tests/test_pds_client.py` only if shared PDS types are touched.

### Sprint 2

- Modify `backend/app/models/channel_agent.py`
  - Add `ProductionTask.approval_mode`.
  - Add `ProductionTask.agent_approval_evidence_json`.
- Modify `backend/app/models/autoflow.py`
  - Add `AutoFlowPlan.agent_approved_by`.
- Create `backend/alembic/versions/015_channel_ops_approval_bridge.py`
  - Add the three new columns.
- Modify `backend/app/autoflow/service.py`
  - Add `approve_internal(plan_id, approved_by, evidence, db)`.
  - Treat `agent_approved_by` as equivalent to `review_approved_at` only for review-required upload execution.
  - Keep `public_approved_at` as the only public publication gate.
- Modify `backend/app/channel_agent/service.py`
  - In `handle_plan_task`, call PDS with `action_type="plan_approval"`.
  - Auto-approve only when `task.approval_mode == "agent"` and PDS verdict is `allow`.
  - Add `handle_reconcile_publication`.
- Modify `backend/app/channel_agent/runner.py`
  - Route `reconcile_publication`.
  - Add PDS outage alert bookkeeping or delegate it to a helper.
- Create `backend/app/channel_agent/pds_health.py`
  - Track last non-fail PDS decision and enqueue hourly `pds_outage` alerts after a 5 minute business-hours gap.
- Modify `backend/app/pds_client.py`
  - Add per-action fail policy.
  - Return `block` on publish/promote failures, `flag` on plan approval failures, and `allow` for candidate accept failures.
- Test `backend/tests/channel_agent/test_service.py`
- Test `backend/tests/autoflow/test_autoflow_api.py`
- Test `backend/tests/autoflow/test_validation_repair.py`
- Test `backend/tests/test_pds_client.py`

### Sprint 3

- Modify `backend/app/models/channel_agent.py`
  - Add index for `(channel_profile_id, topic_lane_id, segment_signature, used_at)`.
- Create `backend/alembic/versions/016_channel_ops_material_ledger.py`
  - Add the material ledger indexes.
- Create `backend/app/channel_agent/material_usage.py`
  - Extract selected material references from `AutoFlowRun.artifacts_json`, `AutoFlowPlan.candidates_json`, and upload-node media metadata.
  - Compute deterministic `segment_signature` from `material_id:start_ms:end_ms` when AutoFlow did not provide one.
  - Query recent usage windows for repetition and cross-account checks.
- Modify `backend/app/channel_agent/service.py`
  - Evaluate repetition guards during candidate selection.
  - Write ledger rows once a `PublicationRecord` exists in `handle_publish_task`; this matches the current code path better than writing in `handle_observe_job`, because publication creation currently happens in `handle_publish_task`.
  - Annotate manual overrides in `rationale_json`.
- Modify `backend/app/api/channel_agent.py`
  - Add `repetition_rejected` and `cross_account_rejected` funnel counts.
- Modify `frontend/src/pages/ChannelOpsStatusPage.tsx`
  - Render the new funnel slices without special casing.
- Test `backend/tests/channel_agent/test_service.py`
- Test `backend/tests/channel_agent/test_api.py`

### Sprint 4

- Modify `backend/app/models/channel_agent.py`
  - Add `ChannelProfile.tick_interval_minutes`.
  - Add `InternalSchedulerRun`.
  - Add any status/TTL fields required for trend-generated `ManualSeed` reuse.
- Create `backend/alembic/versions/017_channel_ops_self_driving.py`
  - Add scheduler, retention, and scoring columns/tables.
- Create `backend/app/channel_agent/scheduler.py`
  - Enqueue `agent_tick` per enabled channel using idempotency key `agent_tick:{channel_id}:{utc_hour_bucket}`.
  - Enforce minimum interval of 15 minutes.
- Create `backend/app/channel_agent/trend_ingesters/youtube_search.py`
  - Use `YouTubeManagerClient` to call YouTube search through youtube-manager; do not introduce a second Google OAuth/client library path in ChannelOps.
  - Materialize accepted results as `ManualSeed(source_policy="trend_youtube")`.
- Create `backend/app/channel_agent/candidate_scoring.py`
  - Compute observe-only score breakdown and write it to `ProductionTask.score_breakdown_json`.
- Create `backend/app/channel_agent/retention.py`
  - Cleanup queue/audit/feedback records by configured age.
- Modify `backend/app/config.py`
  - Add retention threshold settings.
- Modify `backend/app/channel_agent/runner.py`
  - Run queue consumer and scheduler loop together.
  - Route `cleanup_expired`.
- Test `backend/tests/channel_agent/test_service.py`
- Create `backend/tests/channel_agent/test_scheduler.py`
- Create `backend/tests/channel_agent/test_trend_ingester.py`
- Create `backend/tests/channel_agent/test_retention.py`

## Task 0: Sprint 0 Manual Live Smoke

**Files:**
- Read: `docker-compose.yml`
- Read: `backend/app/api/channel_agent.py`
- Read: `backend/channel_agent_runner.py`

- [ ] **Step 1: Verify service topology**

Run:

```bash
docker compose ps
docker compose logs --tail=80 api channel-agent-runner youtube-manager
curl -fsS http://localhost:18080/api/v1/health || curl -fsS http://localhost:8080/api/v1/health
curl -fsS http://localhost:18999/api/auth/status
```

Expected:

- API is reachable.
- `youtube-manager` reports `authenticated: true`.
- `channel-agent-runner` is either running or can be started with the current image.

- [ ] **Step 2: Create one production-shaped channel**

Use existing endpoints:

```bash
API=http://localhost:18080/api/v1/channel-agent
CHANNEL_ID=$(curl -fsS -X POST "$API/channels" \
  -H 'Content-Type: application/json' \
  -d '{"name":"live-cutover-smoke","positioning":"single-account private upload smoke","language":"zh","default_aspect_ratio":"9:16"}' | jq -r .id)
LANE_ID=$(curl -fsS -X POST "$API/channels/$CHANNEL_ID/lanes" \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke","description":"private upload smoke lane","keywords_json":["ai"],"max_posts_per_day":1}' | jq -r .id)
ACCOUNT_ID=$(curl -fsS -X POST "$API/channels/$CHANNEL_ID/accounts" \
  -H 'Content-Type: application/json' \
  -d '{"account_label":"youtube-smoke","platform":"youtube","platform_account_id":"smoke","credential_ref":"youtube-manager/default","default_privacy":"private","external_asset_auto_publish":true}' | jq -r .id)
curl -fsS -X POST "$API/lanes/$LANE_ID/formats" \
  -H 'Content-Type: application/json' \
  -d '{"format_key":"shorts_9x16","target_duration_sec":30,"template_pool_json":["material_library_remix"],"source_platforms_json":["youtube"],"default_publish_visibility":"private"}'
```

Expected: all commands return IDs and the lane format has `default_publish_visibility=private`.

- [ ] **Step 3: Seed and enqueue**

Run:

```bash
curl -fsS -X POST "$API/channels/$CHANNEL_ID/manual-seeds" \
  -H 'Content-Type: application/json' \
  -d '{"topic_lane_id":"'"$LANE_ID"'","target_account_id":"'"$ACCOUNT_ID"'","prompt":"Create a short private smoke video from the local material library.","title_seed":"ChannelOps private smoke","source_platforms_json":["youtube"],"material_library_ids_json":[]}'
curl -fsS -X PATCH "$API/channels/$CHANNEL_ID/dry-run" \
  -H 'Content-Type: application/json' \
  -d '{"dry_run":false}'
curl -fsS -X POST "$API/channels/$CHANNEL_ID/enqueue-tick"
```

Expected: a queued `agent_tick` exists.

- [ ] **Step 4: Observe the chain**

Run the queue runner until the queue drains:

```bash
docker compose up -d --build api channel-agent-runner youtube-manager
watch -n 10 "curl -fsS $API/channels/$CHANNEL_ID/queue | jq 'map({kind,status,last_error})'"
```

Expected chain:

```text
agent_tick -> plan_task -> execute_task -> observe_job -> publish_task -> promote_publication -> collect_metrics
```

- [ ] **Step 5: Verify database and platform evidence**

Run:

```bash
curl -fsS "$API/channels/$CHANNEL_ID/tasks" | jq 'map({id,state,blocked_by_guard,failure_reason})'
curl -fsS "$API/channels/$CHANNEL_ID/publications" | jq 'map({id,platform_content_id,permalink,desired_privacy,current_privacy,publish_status,warnings_json})'
curl -fsS "$API/channels/$CHANNEL_ID/metrics/funnel?days=1" | jq .
```

Expected:

- One `PublicationRecord` has non-empty `platform_content_id`.
- YouTube URL resolves in the authenticated account.
- Privacy is `private` or `unlisted`.
- A `FeedbackSnapshot` exists if metrics are available; if not, file the metrics gap before Sprint 1.

## Task 1: Sprint 1 Live YouTube Loop

**Files:**
- Modify: `backend/app/channel_agent/clients.py`
- Modify: `backend/app/channel_agent/runner.py`
- Modify: `backend/app/channel_agent/service.py`
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/014_channel_ops_live_loop.py`
- Modify: `backend/app/api/channel_agent.py`
- Modify: `backend/app/schemas/channel_agent.py`
- Modify: `frontend/src/api/channelAgent.ts`
- Modify: `frontend/src/pages/ChannelOpsStatusPage.tsx`
- Modify when available: `${PLATFORM_UPLOAD_ROOT}/YouTubeManager/app/main.py`
- Test: `backend/tests/channel_agent/test_service.py`
- Test: `backend/tests/channel_agent/test_runner.py`
- Test: `backend/tests/channel_agent/test_api.py`

- [ ] **Step 1: Add red tests for YouTube client contract**

Add tests that assert:

- `YouTubeManagerClient.quota_remaining_fraction()` reads `{remaining_fraction}`.
- `schedule_publish()` posts `scheduled_at` and `privacy`.
- `refresh_token()` reads `{ok}`.
- `fetch_metrics()` returns recognized metrics.
- `fetch_status()` returns privacy, processing state, permalink, and optional error message.
- `ChannelAgentRunner` uses `YouTubeManagerClient`, not `FakeYouTubeClient`.

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_runner.py tests/channel_agent/test_service.py -q
```

Expected: fail because the protocol/client methods and runner injection are missing.

- [ ] **Step 2: Implement `YouTubeManagerClient`**

In `backend/app/channel_agent/clients.py`, add the protocol methods from the spec and implement HTTP calls under `settings.youtube_manager_url`. Normalize endpoint errors into actionable `RuntimeError` messages that include the endpoint path and status code.

- [ ] **Step 3: Wire the runner**

In `backend/app/channel_agent/runner.py`, construct:

```python
ChannelAgentService(
    queue=self.queue,
    autoflow_client=LocalAutoFlowClient(),
    youtube_client=YouTubeManagerClient(base_url=settings.youtube_manager_url),
    minimax_client=MiniMaxImageClient(),
    pds_client=_build_pds_client(),
)
```

Fail startup when the runner is in live mode and `settings.youtube_manager_url` is empty.

- [ ] **Step 4: Fetch live metrics**

In `handle_collect_metrics`, if payload metrics are absent or empty, load the `PublishingAccount`, call `youtube_client.fetch_metrics(account, publication.platform_content_id)`, append `metrics_fetch_failed:<message>` warnings on exceptions, and keep the existing requeue ceiling.

- [ ] **Step 5: Add audit uniqueness**

Add `UniqueConstraint("channel_profile_id", "tick_id", name="uq_agent_tick_audit_channel_tick")` to `AgentTickAudit.__table_args__`. Create Alembic revision `014` after current revision `013_event_outbox`.

- [ ] **Step 6: Add live-loop recency indicator**

Backend: return `last_successful_measured_at` from `/channels/{id}/health` by querying the latest `ProductionTask.state == "measured"` for the channel.

Frontend: render:

- green when measured within 24h,
- amber when older than 24h,
- red when older than 72h or absent.

- [ ] **Step 7: Add YouTubeManager endpoints**

In the actual YouTubeManager source, add:

- `GET /accounts/{account_id}/quota`
- `POST /accounts/{account_id}/videos/{video_id}/schedule`
- `POST /accounts/{account_id}/token/refresh`
- `GET /accounts/{account_id}/videos/{video_id}/metrics`
- `GET /accounts/{account_id}/videos/{video_id}/status`

Keep credential lookup inside YouTubeManager. Do not read `token.json` from ChannelOps.

- [ ] **Step 8: Verify Sprint 1**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_runner.py tests/channel_agent/test_service.py tests/channel_agent/test_api.py -q
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
cd ../frontend
npm install
npm run build
npm run lint || true
```

Expected: tests pass; lint/type commands either pass or report known non-blocking findings.

## Task 2: Sprint 2 Approval Bridge And PDS Closure

**Files:**
- Modify: `backend/app/models/channel_agent.py`
- Modify: `backend/app/models/autoflow.py`
- Create: `backend/alembic/versions/015_channel_ops_approval_bridge.py`
- Modify: `backend/app/autoflow/service.py`
- Modify: `backend/app/channel_agent/service.py`
- Modify: `backend/app/channel_agent/runner.py`
- Create: `backend/app/channel_agent/pds_health.py`
- Modify: `backend/app/pds_client.py`
- Test: `backend/tests/channel_agent/test_service.py`
- Test: `backend/tests/autoflow/test_autoflow_api.py`
- Test: `backend/tests/autoflow/test_validation_repair.py`
- Test: `backend/tests/test_pds_client.py`

- [ ] **Step 1: Add red approval bridge tests**

Cover:

- lane-seed tasks default `approval_mode="agent"`.
- manual-seed tasks default `approval_mode="human"`.
- `handle_plan_task` sets `agent_approved_by="channel_agent"` only when PDS returns `allow`.
- review-required AutoFlow plans execute with `agent_approved_by`.
- public execution still requires `public_approved_at`.

- [ ] **Step 2: Add red PDS fail-policy tests**

Cover:

- network/5xx/parse failure on `candidate_accept` returns `allow`.
- failure on `plan_approval` returns `flag`.
- failure on `publish` and `promote_publication` returns `block`.
- metadata retains `warning` so UI/events can show `pds_unavailable` or `pds_parse_failed`.

- [ ] **Step 3: Add schema and migration**

Add the model columns and Alembic revision after `014`.

- [ ] **Step 4: Implement AutoFlow internal approval**

Add `approve_internal()` to avoid HTTP round-trips from `ChannelAgentService`. It must persist `agent_approved_by`, update rights metadata with evidence, and avoid setting `public_approved_at`.

- [ ] **Step 5: Implement PDS health monitor**

Track the last successful, non-fail-policy PDS decision. If no such decision occurs for more than 5 minutes during business hours, enqueue one hourly `send_alert` item with type `pds_outage`.

- [ ] **Step 6: Add publication reconciler**

Add queue kind `reconcile_publication`. Enqueue it after successful `promote_publication` with `run_after = scheduled_publish_at + 30 minutes`. The handler calls `fetch_status`, updates `current_privacy`, records explicit failure reasons, and writes `TakedownEvent` for rejection/removal/claim states.

- [ ] **Step 7: Verify Sprint 2**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py tests/autoflow/test_autoflow_api.py tests/autoflow/test_validation_repair.py tests/test_pds_client.py -q
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: review-required agent tasks no longer stall at execution; forced PDS outage blocks publish.

## Task 3: Sprint 3 Material Ledger And Repetition Guards

**Files:**
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/016_channel_ops_material_ledger.py`
- Create: `backend/app/channel_agent/material_usage.py`
- Modify: `backend/app/channel_agent/service.py`
- Modify: `backend/app/api/channel_agent.py`
- Modify: `frontend/src/pages/ChannelOpsStatusPage.tsx`
- Test: `backend/tests/channel_agent/test_service.py`
- Test: `backend/tests/channel_agent/test_api.py`

- [ ] **Step 1: Add red ledger extraction tests**

Create tests where a completed AutoFlow plan/run contains selected candidates with `material_id`, `asset_id`, `start_ms`, and `end_ms`. Assert one `MaterialUsageLedger` row per selected material after publish task handling.

- [ ] **Step 2: Add red repetition guard tests**

Cover:

- same segment in the same lane within 7 days rejects lane-seed candidate.
- same material in the same account within 14 days rejects lane-seed candidate.
- sibling account use within 30 days rejects lane-seed candidate.
- manual seed override is allowed but annotated in `rationale_json`.

- [ ] **Step 3: Implement ledger helper**

Centralize extraction and query logic in `material_usage.py`. Service code should call narrow helper functions, not inline JSON traversal through `service.py`.

- [ ] **Step 4: Write ledger rows after publication creation**

Current code creates `PublicationRecord` in `handle_publish_task`, not `handle_observe_job`. Write ledger rows immediately after the publication exists and before the method commits.

- [ ] **Step 5: Add funnel slices**

Return `repetition_rejected` and `cross_account_rejected` from `/channels/{id}/metrics/funnel`. The current frontend renders arbitrary funnel entries, so it should need little or no custom rendering beyond label order if desired.

- [ ] **Step 6: Verify Sprint 3**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py tests/channel_agent/test_api.py -q
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
cd ../frontend
npm run build
npm run lint || true
```

Expected: five consecutive ticks against a tiny material library do not select the same segment.

## Task 4: Sprint 4 Self-Driving Loop

**Files:**
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/017_channel_ops_self_driving.py`
- Create: `backend/app/channel_agent/scheduler.py`
- Create: `backend/app/channel_agent/trend_ingesters/youtube_search.py`
- Create: `backend/app/channel_agent/candidate_scoring.py`
- Create: `backend/app/channel_agent/retention.py`
- Modify: `backend/app/config.py`
- Modify: `backend/app/channel_agent/runner.py`
- Test: `backend/tests/channel_agent/test_scheduler.py`
- Test: `backend/tests/channel_agent/test_trend_ingester.py`
- Test: `backend/tests/channel_agent/test_retention.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Add scheduler red tests**

Assert enabled non-halted channels enqueue at most one tick per hour bucket, `tick_interval_minutes` floors to 15, and manual `/enqueue-tick` still works.

- [ ] **Step 2: Add trend ingester red tests**

Use `httpx.MockTransport` for YouTubeManager search response. Assert candidates above the view floor become active `ManualSeed` rows with `source_policy="trend_youtube"` and stale seeds are expired.

- [ ] **Step 3: Add scoring red tests**

Assert `score_breakdown_json` includes lane weight, material fit, freshness, account fit, timing, novelty, repetition risk, compliance risk, and total score for every selected task.

- [ ] **Step 4: Add retention red tests**

Assert old queue/audit/feedback rows are deleted according to settings and recent rows are retained.

- [ ] **Step 5: Implement scheduler and retention**

Keep scheduler logic separate from the queue consumer so `run_once()` tests remain deterministic.

- [ ] **Step 6: Implement trend ingestion through YouTubeManager**

Do not add a Google API client to ChannelOps. If YouTubeManager lacks search endpoint support, extend YouTubeManager first and keep ChannelOps as an HTTP client.

- [ ] **Step 7: Verify Sprint 4**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_scheduler.py tests/channel_agent/test_trend_ingester.py tests/channel_agent/test_retention.py tests/channel_agent/test_service.py -q
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: internal scheduler can produce queued work without external cron and every new `ProductionTask` has non-empty `score_breakdown_json`.

## Live Acceptance Gates

- Sprint 0 gate: one private/unlisted YouTube upload exists with non-empty `platform_content_id`; any discovered OAuth/payload/idempotency/storage drift is fixed before Sprint 1 merges.
- Sprint 1 gate: one real channel produces a `FeedbackSnapshot` without manual metrics injection within 24h.
- Sprint 2 gate: review-required agent plans execute after PDS allow, and PDS outage holds publish within one tick.
- Sprint 3 gate: five consecutive ticks against a small material library never reuse the same segment; funnel shows rejection slices.
- Sprint 4 gate: one channel runs for 7 days without operator action beyond dashboard inspection.

## Verification Checklist

Run this before closing each sprint:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
cd ../frontend
npm install
npm run build
npm run lint || true
```

For Sprint 1 and later, also rerun the Sprint 0 smoke against live services after deployment.
