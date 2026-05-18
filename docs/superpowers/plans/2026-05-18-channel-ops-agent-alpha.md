# ChannelOps Agent Alpha Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first queue-first ChannelOps alpha: channel config, durable queue, dry-run tick, AutoFlow observation, YouTube publication scheduling, alerts, metrics snapshots, API, runner, and an operational status panel.

**Architecture:** Add a focused `backend/app/channel_agent/` package around SQLAlchemy models, queue services, stores, orchestration handlers, fake-friendly external clients, and health aggregation. FastAPI exposes configuration/control/read APIs; `backend/channel_agent_runner.py` consumes durable queue items. The frontend adds a compact operational status page without full configuration editing.

**Tech Stack:** FastAPI, SQLAlchemy async ORM, Alembic, Pydantic v2, pytest + httpx ASGI tests, React/Vite/TypeScript, Docker Compose.

---

## File Map

- Create `backend/app/models/channel_agent.py`: ChannelOps SQLAlchemy models.
- Modify `backend/app/models/__init__.py`: export ChannelOps models.
- Create `backend/alembic/versions/009_channel_agent_config.py`: config tables.
- Create `backend/alembic/versions/010_channel_agent_queue.py`: queue/audit tables.
- Create `backend/alembic/versions/011_channel_agent_production.py`: production/distribution/feedback tables.
- Create `backend/app/schemas/channel_agent.py`: API schemas.
- Create `backend/app/channel_agent/constants.py`: state/kind constants.
- Create `backend/app/channel_agent/clock.py`: injectable clock/FakeClock helper.
- Create `backend/app/channel_agent/store.py`: async DB store methods.
- Create `backend/app/channel_agent/queue.py`: enqueue/claim/succeed/fail/dead-letter logic.
- Create `backend/app/channel_agent/alerts.py`: Slack/email alert service and payload builder.
- Create `backend/app/channel_agent/clients.py`: fake-friendly YouTube/MiniMax client protocols and default no-op clients.
- Create `backend/app/channel_agent/service.py`: tick, plan, execute, observe, publish, promote, metrics, health handlers.
- Create `backend/app/channel_agent/runner.py`: queue runner.
- Create `backend/channel_agent_runner.py`: CLI entrypoint.
- Create `backend/app/api/channel_agent.py`: FastAPI router.
- Modify `backend/app/main.py`: include router.
- Modify `backend/app/config.py`: alert/MiniMax/runner settings.
- Modify `docker-compose.yml`: add `channel-agent-runner` service.
- Create `backend/tests/channel_agent/test_models_queue.py`: model and queue tests.
- Create `backend/tests/channel_agent/test_service.py`: service scenarios.
- Create `backend/tests/channel_agent/test_api.py`: API tests.
- Create `frontend/src/api/channelAgent.ts`: API client.
- Create `frontend/src/pages/ChannelOpsStatusPage.tsx`: operational panel.
- Create `frontend/src/pages/ChannelOpsStatusPage.css`: panel styling.
- Modify `frontend/src/components/layout/Sidebar.tsx`: navigation link.
- Modify `frontend/src/App.tsx`: route/page wiring.

## Task 1: Models, Migrations, And Schemas

**Files:**
- Create: `backend/app/models/channel_agent.py`
- Modify: `backend/app/models/__init__.py`
- Create: `backend/alembic/versions/009_channel_agent_config.py`
- Create: `backend/alembic/versions/010_channel_agent_queue.py`
- Create: `backend/alembic/versions/011_channel_agent_production.py`
- Create: `backend/app/schemas/channel_agent.py`
- Test: `backend/tests/channel_agent/test_models_queue.py`

- [x] **Step 1: Write failing model smoke tests**

Create `backend/tests/channel_agent/test_models_queue.py` with tests that create the new tables in SQLite, insert a channel with `dry_run=True`, insert queue rows with `priority`, `parent_queue_item_id`, `dead_letter_at`, and insert a `PublicationRecord` with non-null `compliance_disposition`.

- [x] **Step 2: Run red test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_models_queue.py -q`

Expected: import fails because `app.models.channel_agent` does not exist.

- [x] **Step 3: Add models and migrations**

Implement the models and migration columns from `docs/superpowers/specs/2026-05-18-channel-ops-agent-implementation-design.md`, preserving JSON fields and nullable future slots exactly as specified.

- [x] **Step 4: Add Pydantic schemas**

Define create/update/read schemas for channels, lanes, accounts, lane formats, manual seeds, queue items, tasks, publications, feedback snapshots, health summaries, and control requests.

- [x] **Step 5: Run green test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_models_queue.py -q`

Expected: pass.

## Task 2: Queue Service, Clock, Alerts, And Clients

**Files:**
- Create: `backend/app/channel_agent/constants.py`
- Create: `backend/app/channel_agent/clock.py`
- Create: `backend/app/channel_agent/queue.py`
- Create: `backend/app/channel_agent/alerts.py`
- Create: `backend/app/channel_agent/clients.py`
- Test: `backend/tests/channel_agent/test_models_queue.py`

- [x] **Step 1: Add failing queue/alert tests**

Extend `test_models_queue.py` with tests for idempotent enqueue, priority ordering, dead-letter after max attempts, UTC hour idempotency key helpers, and alert payload creation for `token_expiring_24h`, `quota_below_20pct`, `takedown_event_logged`, and `material_supply_low`.

- [x] **Step 2: Run red test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_models_queue.py -q`

Expected: imports for `app.channel_agent.queue` and `alerts` fail.

- [x] **Step 3: Implement queue and alert helpers**

Implement `ChannelOpsQueueService.enqueue()`, `claim_next()`, `mark_succeeded()`, `mark_failed_or_retry()`, idempotency key builders, `Clock`/`FakeClock`, and `AlertService` with fake-friendly Slack/email delivery.

- [x] **Step 4: Run green test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_models_queue.py -q`

Expected: pass.

## Task 3: Store And Channel Agent Service

**Files:**
- Create: `backend/app/channel_agent/store.py`
- Create: `backend/app/channel_agent/service.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [x] **Step 1: Write failing service tests**

Create tests for:

- 7-day dry-run writes `AgentTickAudit` but creates no `ProductionTask`.
- `dry_run=false` manual seed creates a selected task and enqueues `plan_task`.
- consecutive low `per_lane_eligible_count` enqueues `material_supply_low`.
- `plan_task` holds when the AutoFlow plan has no `youtube_upload` node.
- `publish_task` observes an uploaded YouTube video id, creates `PublicationRecord`, and enqueues `promote_publication`.
- `promote_publication` uses fake YouTube client to schedule `publishAt`.
- severe `TakedownEvent` pauses the account and enqueues a takedown alert.
- quota at 95% holds publish instead of retrying upload.
- token refresh failure holds the account and enqueues alert.

- [x] **Step 2: Run red test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_service.py -q`

Expected: imports fail because service modules do not exist.

- [x] **Step 3: Implement store and service**

Implement store methods and service handlers using fake-friendly clients. Keep real network calls behind injected clients; default alpha clients should fail safe or no-op unless configured.

- [x] **Step 4: Run green test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_service.py -q`

Expected: pass.

## Task 4: API Router And Runner

**Files:**
- Create: `backend/app/api/channel_agent.py`
- Modify: `backend/app/main.py`
- Create: `backend/app/channel_agent/runner.py`
- Create: `backend/channel_agent_runner.py`
- Test: `backend/tests/channel_agent/test_api.py`

- [x] **Step 1: Write failing API tests**

Create API tests for channel creation, manual seed creation, enqueue tick, dry-run patch, halt/resume, health, queue, tasks, publications, and metrics/funnel read endpoints.

- [x] **Step 2: Run red test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_api.py -q`

Expected: 404 or import failure for the new router.

- [x] **Step 3: Implement router and runner**

Add router endpoints under `/api/v1/channel-agent`. Add runner loop and one-shot CLI that claims and handles queue items with `ChannelAgentService`.

- [x] **Step 4: Run green test**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_api.py -q`

Expected: pass.

## Task 5: Frontend Status Panel

**Files:**
- Create: `frontend/src/api/channelAgent.ts`
- Create: `frontend/src/pages/ChannelOpsStatusPage.tsx`
- Create: `frontend/src/pages/ChannelOpsStatusPage.css`
- Modify: `frontend/src/components/layout/Sidebar.tsx`
- Modify: `frontend/src/App.tsx`

- [x] **Step 1: Add API client and page**

Implement a status page with health cards, 7-day funnel, account/lane summaries, queue/task/publication tables, dry-run status, and halt/resume controls.

- [x] **Step 2: Wire navigation**

Add a sidebar entry and route to render the page.

- [x] **Step 3: Build frontend**

Run: `cd frontend && npm run build`

Expected: build succeeds.

## Task 6: Compose, Settings, And Verification

**Files:**
- Modify: `backend/app/config.py`
- Modify: `docker-compose.yml`
- Update: `docs/superpowers/plans/2026-05-18-channel-ops-agent-alpha.md` checkboxes as work completes.

- [x] **Step 1: Add settings**

Add MiniMax, alert, and runner settings without exposing secrets.

- [x] **Step 2: Add Compose service**

Add `channel-agent-runner` service using backend image, `python channel_agent_runner.py run`, and same database/storage env as API.

- [x] **Step 3: Run backend focused tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent tests/autoflow/test_autoflow_api.py -q
```

Expected: pass.

- [x] **Step 4: Run required checks**

Run:

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

Expected: pytest/build pass; ruff/mypy/lint may report non-blocking diagnostics per repo instructions.
