# ChannelOps Remaining Sprints Batch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete ChannelOps Sprint 2, Sprint 3, and Sprint 4 so the live loop can approve agent-owned plans, fail closed on publication risk, reconcile platform state, avoid repeated material, and run on an internal scheduler with trend ingestion and retention.

**Architecture:** Keep OAuth and YouTube API ownership in YouTubeManager. ChannelOps owns orchestration, queueing, PDS policy, ledger/guard decisions, and dashboard health. Implement in sprint order: approval/reconcile first, material ledger/guards second, scheduler/trends/scoring/retention last.

**Tech Stack:** FastAPI, SQLAlchemy async ORM, Alembic, Pydantic v2, httpx, pytest, React/Vite/TypeScript, Docker Compose, external YouTubeManager FastAPI runtime.

---

## Source Specs

- `docs/superpowers/specs/2026-05-19-channel-ops-live-cutover-design.md`
- `docs/superpowers/specs/2026-05-19-channel-ops-remaining-sprints-batch-design.md`

## Existing State To Preserve

- Sprint 0/1 live smoke already proved a private YouTube upload and live metrics collection.
- Existing uncommitted Sprint 1 implementation must remain intact.
- External YouTubeManager may be modified at `/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager`.
- Do not introduce a second Google OAuth path in ChannelOps.
- Do not allow automatic public publication.

## File Map

### Sprint 2

- Modify: `backend/app/models/channel_agent.py`
- Modify: `backend/app/models/autoflow.py`
- Create: `backend/alembic/versions/015_channel_ops_approval_bridge.py`
- Modify: `backend/app/autoflow/service.py`
- Modify: `backend/app/channel_agent/service.py`
- Modify: `backend/app/channel_agent/runner.py`
- Create: `backend/app/channel_agent/pds_health.py`
- Modify: `backend/app/pds_client.py`
- Modify tests: `backend/tests/channel_agent/test_service.py`
- Modify tests: `backend/tests/channel_agent/test_runner.py`
- Modify tests: `backend/tests/autoflow/test_autoflow_api.py`
- Modify tests: `backend/tests/test_pds_client.py`

### Sprint 3

- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/016_channel_ops_material_ledger.py`
- Create: `backend/app/channel_agent/material_usage.py`
- Modify: `backend/app/channel_agent/service.py`
- Modify: `backend/app/api/channel_agent.py`
- Modify: `frontend/src/pages/ChannelOpsStatusPage.tsx`
- Modify tests: `backend/tests/channel_agent/test_service.py`
- Modify tests: `backend/tests/channel_agent/test_api.py`

### Sprint 4

- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/017_channel_ops_self_driving.py`
- Create: `backend/app/channel_agent/scheduler.py`
- Create: `backend/app/channel_agent/trend_ingesters/youtube_search.py`
- Create: `backend/app/channel_agent/candidate_scoring.py`
- Create: `backend/app/channel_agent/retention.py`
- Modify: `backend/app/channel_agent/clients.py`
- Modify: `backend/app/channel_agent/runner.py`
- Modify: `backend/app/config.py`
- Modify external: `/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager/app/main.py`
- Create tests: `backend/tests/channel_agent/test_scheduler.py`
- Create tests: `backend/tests/channel_agent/test_trend_ingester.py`
- Create tests: `backend/tests/channel_agent/test_retention.py`
- Modify tests: `backend/tests/channel_agent/test_service.py`

## Task 1: Sprint 2 Approval Bridge And PDS Closure

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
- Test: `backend/tests/channel_agent/test_runner.py`
- Test: `backend/tests/autoflow/test_autoflow_api.py`
- Test: `backend/tests/test_pds_client.py`

- [ ] **Step 1: Add focused failing tests**

  Add tests for these observable behaviors:

  ```python
  async def test_lane_seed_task_auto_approves_review_required_plan_when_pds_allows(...):
      assert task.approval_mode == "agent"
      assert plan.agent_approved_by == "channel_agent"
      assert task.agent_approval_evidence_json["verdict"] == "allow"

  async def test_manual_seed_task_does_not_agent_approve_review_required_plan(...):
      assert task.approval_mode == "human"
      assert plan.agent_approved_by is None

  async def test_pds_failure_policy_blocks_publish_and_flags_plan_approval(...):
      assert publish_decision.verdict == "block"
      assert plan_decision.verdict == "flag"
      assert plan_decision.metadata["warning"] in {"pds_unavailable", "pds_parse_failed"}

  async def test_reconcile_publication_updates_privacy_and_platform_status(...):
      assert publication.current_privacy == "private"
      assert publication.publish_status in {"scheduled", "published", "processed"}
  ```

  Run:

  ```bash
  cd backend
  python3 -m pytest tests/channel_agent/test_service.py tests/autoflow/test_autoflow_api.py tests/test_pds_client.py -q
  ```

  Expected: at least one failure showing the missing schema/service behavior.

- [ ] **Step 2: Add schema and migration**

  Add `ProductionTask.approval_mode`, `ProductionTask.agent_approval_evidence_json`, and `AutoFlowPlan.agent_approved_by`. Create Alembic revision `015_channel_ops_approval_bridge.py` after `014_channel_ops_live_loop`.

- [ ] **Step 3: Add AutoFlow internal approval**

  Add an internal function in `backend/app/autoflow/service.py`:

  ```python
  async def approve_internal(
      db: AsyncSession,
      plan_id: str,
      *,
      approved_by: str,
      evidence: dict[str, Any],
  ) -> AutoFlowPlan:
      ...
  ```

  It must set `agent_approved_by`, preserve evidence in rights metadata, and never set `public_approved_at`.

- [ ] **Step 4: Implement PDS fail policy and health tracking**

  Add per-action fail policy in `backend/app/pds_client.py`. Create `backend/app/channel_agent/pds_health.py` with deterministic helpers for deciding when to enqueue one hourly `pds_outage` alert.

- [ ] **Step 5: Wire plan approval and reconcile handler**

  In `ChannelAgentService.handle_plan_task`, call PDS with `action_type="plan_approval"` and agent-approve only for `approval_mode="agent"` plus `verdict="allow"`. Add `handle_reconcile_publication` and route it in the runner.

- [ ] **Step 6: Verify Sprint 2**

  Run:

  ```bash
  cd backend
  python3 -m pytest tests/channel_agent/test_service.py tests/autoflow/test_autoflow_api.py tests/test_pds_client.py -q
  python3 -m pytest tests/channel_agent -q
  ```

  Expected: focused tests pass and channel-agent suite remains green.

## Task 2: Sprint 3 Material Ledger And Repetition Guards

**Files:**
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/016_channel_ops_material_ledger.py`
- Create: `backend/app/channel_agent/material_usage.py`
- Modify: `backend/app/channel_agent/service.py`
- Modify: `backend/app/api/channel_agent.py`
- Modify: `frontend/src/pages/ChannelOpsStatusPage.tsx`
- Test: `backend/tests/channel_agent/test_service.py`
- Test: `backend/tests/channel_agent/test_api.py`

- [ ] **Step 1: Add focused failing tests**

  Add tests that create plan/run metadata with selected clips and assert:

  ```python
  assert ledger.material_id == "mat-1"
  assert ledger.segment_signature
  assert task.blocked_by_guard == "repetition_rejected"
  assert "manual_override" in task.rationale_json
  assert funnel["repetition_rejected"] >= 1
  assert funnel["cross_account_rejected"] >= 1
  ```

- [ ] **Step 2: Implement `material_usage.py`**

  Add helper functions:

  ```python
  def segment_signature(material_id: str, start_ms: int | None, end_ms: int | None) -> str: ...
  def extract_material_references(plan_payload: dict, run_payload: dict, upload_metadata: dict) -> list[MaterialReference]: ...
  async def recent_usage_flags(db: AsyncSession, *, channel_id: str, lane_id: str | None, account_id: str | None, references: list[MaterialReference], now: datetime) -> UsageGuardResult: ...
  ```

- [ ] **Step 3: Write ledger rows during publish**

  After `PublicationRecord` exists in `handle_publish_task`, write one `MaterialUsageLedger` row per selected material reference and avoid duplicate rows for the same publication/reference pair.

- [ ] **Step 4: Apply repetition guards during candidate selection**

  Lane-generated tasks hard-reject repeated material. Manual seeds are allowed but annotate `rationale_json` with guard details.

- [ ] **Step 5: Add funnel slices and frontend display**

  Include `repetition_rejected` and `cross_account_rejected` in `/metrics/funnel`. Keep the React rendering generic so new slices do not require special-case cards.

- [ ] **Step 6: Verify Sprint 3**

  Run:

  ```bash
  cd backend
  python3 -m pytest tests/channel_agent/test_service.py tests/channel_agent/test_api.py -q
  python3 -m pytest tests/channel_agent -q
  cd ../frontend
  npm run build
  npm run lint || true
  ```

  Expected: focused tests pass, frontend build passes, lint has no blocking failure.

## Task 3: Sprint 4 Self-Driving Loop

**Files:**
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/017_channel_ops_self_driving.py`
- Create: `backend/app/channel_agent/scheduler.py`
- Create: `backend/app/channel_agent/trend_ingesters/youtube_search.py`
- Create: `backend/app/channel_agent/candidate_scoring.py`
- Create: `backend/app/channel_agent/retention.py`
- Modify: `backend/app/channel_agent/clients.py`
- Modify: `backend/app/channel_agent/runner.py`
- Modify: `backend/app/config.py`
- Modify external: `/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager/app/main.py`
- Test: `backend/tests/channel_agent/test_scheduler.py`
- Test: `backend/tests/channel_agent/test_trend_ingester.py`
- Test: `backend/tests/channel_agent/test_retention.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Add scheduler, trend, scoring, and retention tests**

  Tests must assert:

  ```python
  assert enqueued.kind == "agent_tick"
  assert channel.tick_interval_minutes >= 15
  assert seed.source_policy == "trend_youtube"
  assert task.score_breakdown_json["total_score"] is not None
  assert old_queue_row_deleted is True
  assert recent_feedback_row_deleted is False
  ```

- [ ] **Step 2: Implement scheduler**

  Add `ChannelOpsScheduler.run_once(db, now)` that enqueues idempotent ticks for enabled, non-halted channels and records `InternalSchedulerRun`.

- [ ] **Step 3: Implement trend ingestion through YouTubeManager**

  Extend `YouTubeClient`/`YouTubeManagerClient` with `search_videos`. Add YouTubeManager endpoint support if missing. Materialize accepted search results as `ManualSeed(source_policy="trend_youtube")`.

- [ ] **Step 4: Implement observe-only candidate scoring**

  Compute score components for selected tasks and persist them without changing selection order.

- [ ] **Step 5: Implement retention cleanup**

  Add settings for queue/audit/feedback retention days. Add `cleanup_expired` handler and route it in the runner.

- [ ] **Step 6: Verify Sprint 4**

  Run:

  ```bash
  cd backend
  python3 -m pytest tests/channel_agent/test_scheduler.py tests/channel_agent/test_trend_ingester.py tests/channel_agent/test_retention.py tests/channel_agent/test_service.py -q
  python3 -m pytest
  ```

  Expected: full backend suite passes.

## Task 4: Live Rebuild And Final Verification

**Files:**
- Read: `docker-compose.yml`
- Read: `.env` files if referenced by Compose
- Verify external: `/home/taiwei/Constructure-repos/constructure-platform-upload/YouTubeManager/app/main.py`

- [ ] **Step 1: Run full local checks**

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

- [ ] **Step 2: Rebuild live services**

  ```bash
  PLATFORM_UPLOAD_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload \
  PLATFORM_UPLOAD_RUNTIME_ROOT=/home/taiwei/Constructure-repos/constructure-platform-upload \
  VP_STORAGE_ROOT=/home/taiwei/Constructure-repos/videoprocess/k8s-data/storage \
  docker compose up -d --build --no-deps api channel-agent-runner youtube-manager frontend
  ```

- [ ] **Step 3: Smoke live endpoints**

  ```bash
  curl -fsS http://localhost:18080/health
  curl -fsS http://localhost:18999/api/auth/status
  curl -fsS http://localhost:3001/channel-ops -I
  ```

  Expected: services return healthy responses.

- [ ] **Step 4: Report remaining operational gates**

  Report whether the 7-day self-driving gate has merely been implemented or actually observed. Do not claim the 7-day gate has passed unless it has run for seven calendar days.
