# ChannelOps Alpha Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the ChannelOps alpha loop so a channel can dry-run meaningful decisions, create lane-driven work, pass safety guards, execute AutoFlow, observe YouTube uploads, schedule publication, collect metrics, and expose accurate operator controls.

**Architecture:** Keep the current service-first shape. Harden `ChannelAgentService`, `ChannelOpsQueueService`, and the ChannelOps API with focused helpers instead of a broad adapter rewrite. Use fake-friendly client protocols for deterministic tests and a local AutoFlow client that reads existing AutoFlow/job/artifact records.

**Tech Stack:** FastAPI, SQLAlchemy async ORM, Alembic, Pydantic v2, pytest + aiosqlite/httpx, React/Vite TypeScript for build verification.

---

## Scope Check

The approved hardening spec touches queue infrastructure, service orchestration, guards, API controls, and funnel metrics. These are coupled by the same queue state machine and must be implemented together for alpha acceptance. Frontend redesign is out of scope; existing status panel build compatibility remains part of final verification.

Source spec: `docs/superpowers/specs/2026-05-18-channel-ops-alpha-hardening-design.md`

## File Map

- Modify `backend/app/models/channel_agent.py`
  - Add `ChannelOpsQueueItem.channel_profile_id`.
  - Add `LaneFormatMatrix.source_platforms_json`.
- Create `backend/alembic/versions/012_channel_ops_hardening.py`
  - Migration for the two new columns and indexes.
- Modify `backend/app/schemas/channel_agent.py`
  - Add lane format source platforms and tick read schemas if missing.
- Modify `backend/app/channel_agent/queue.py`
  - Add channel-scoped enqueue, IntegrityError race recovery, exponential backoff.
- Create `backend/app/channel_agent/lane_prompts.py`
  - Structured lane-driven prompt builder.
- Modify `backend/app/channel_agent/constants.py`
  - Add active/terminal task state sets and lane prompt constants if useful.
- Modify `backend/app/channel_agent/clients.py`
  - Add AutoFlow execution/job observation dataclasses.
  - Extend fake AutoFlow client.
  - Add local AutoFlow client that reads AutoFlow/job/artifact state.
- Modify `backend/app/channel_agent/service.py`
  - Add lane-driven candidate generation, dry-run guard evaluation, guards, execute/observe/metrics handlers, safer AutoFlow requests and privacy fallback.
- Modify `backend/app/channel_agent/runner.py`
  - Route `execute_task`, `observe_job`, `collect_metrics` to real handlers.
- Modify `backend/app/api/channel_agent.py`
  - Add control APIs, ticks endpoint, real funnel aggregation, channel-scoped queue/health.
- Modify `backend/tests/channel_agent/test_models_queue.py`
  - Queue/schema/backoff/race tests.
- Modify `backend/tests/channel_agent/test_service.py`
  - Candidate generation, dry-run, guards, request config, runner-flow tests.
- Create `backend/tests/channel_agent/test_local_autoflow_client.py`
  - Local observation reads `Artifact.media_info.youtube.video_id`.
- Modify `backend/tests/channel_agent/test_api.py`
  - New control/funnel/channel-scope API tests.

## Task 1: Schema And Queue Hardening

**Files:**
- Modify: `backend/app/models/channel_agent.py`
- Create: `backend/alembic/versions/012_channel_ops_hardening.py`
- Modify: `backend/app/schemas/channel_agent.py`
- Modify: `backend/app/channel_agent/queue.py`
- Test: `backend/tests/channel_agent/test_models_queue.py`

- [ ] **Step 1: Add failing schema and queue tests**

Append tests equivalent to this in `backend/tests/channel_agent/test_models_queue.py`:

```python
@pytest.mark.asyncio
async def test_lane_format_source_platforms_and_queue_channel_scope(channel_agent_session):
    channel = ChannelProfile(name="Scope", language="zh")
    channel_agent_session.add(channel)
    await channel_agent_session.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="tech", keywords_json=["ai"])
    channel_agent_session.add(lane)
    await channel_agent_session.flush()
    lane_format = LaneFormatMatrix(
        topic_lane_id=lane.id,
        format_key="shorts_9x16",
        target_duration_sec=45,
        source_platforms_json=["bilibili", "youtube"],
        template_pool_json=["material_library_remix"],
    )
    channel_agent_session.add(lane_format)
    await channel_agent_session.commit()

    queue = ChannelOpsQueueService()
    item = await queue.enqueue(
        channel_agent_session,
        kind="agent_tick",
        idempotency_key=f"agent_tick:{channel.id}:2026-05-18-10",
        payload={"channel_id": str(channel.id)},
        channel_profile_id=channel.id,
        priority=20,
    )

    assert lane_format.source_platforms_json == ["bilibili", "youtube"]
    assert item.channel_profile_id == channel.id
```

Append a race recovery test using a monkeypatched first `get_by_key()` result:

```python
@pytest.mark.asyncio
async def test_enqueue_recovers_from_idempotency_integrity_race(channel_agent_session, monkeypatch):
    channel = ChannelProfile(name="Race", language="zh")
    channel_agent_session.add(channel)
    await channel_agent_session.flush()
    existing = ChannelOpsQueueItem(
        kind="agent_tick",
        idempotency_key=f"agent_tick:{channel.id}:race",
        channel_profile_id=channel.id,
        payload_json={"channel_id": str(channel.id)},
    )
    channel_agent_session.add(existing)
    await channel_agent_session.commit()

    queue = ChannelOpsQueueService()
    original_get_by_key = queue.get_by_key
    calls = {"count": 0}

    async def racing_get_by_key(db, idempotency_key):
        calls["count"] += 1
        if calls["count"] == 1:
            return None
        return await original_get_by_key(db, idempotency_key)

    monkeypatch.setattr(queue, "get_by_key", racing_get_by_key)

    item = await queue.enqueue(
        channel_agent_session,
        kind="agent_tick",
        idempotency_key=f"agent_tick:{channel.id}:race",
        payload={"channel_id": str(channel.id)},
        channel_profile_id=channel.id,
    )

    assert item.id == existing.id
    assert calls["count"] == 2
```

Append a backoff test:

```python
@pytest.mark.asyncio
async def test_queue_retry_uses_exponential_backoff(channel_agent_session):
    clock = FakeClock(datetime(2026, 5, 18, 8, 0, tzinfo=timezone.utc))
    queue = ChannelOpsQueueService(clock=clock)
    item = await queue.enqueue(
        channel_agent_session,
        kind="observe_job",
        idempotency_key="observe_job:task:run",
        payload={"production_task_id": str(uuid.uuid4())},
        max_attempts=4,
    )
    item = await queue.claim_next(channel_agent_session, worker_id="worker")
    await queue.mark_failed_or_retry(channel_agent_session, item, "not ready")
    assert item.status == "queued"
    assert item.run_after == datetime(2026, 5, 18, 8, 5, tzinfo=timezone.utc)

    clock.advance(timedelta(minutes=5))
    item = await queue.claim_next(channel_agent_session, worker_id="worker")
    await queue.mark_failed_or_retry(channel_agent_session, item, "not ready")
    assert item.run_after == datetime(2026, 5, 18, 8, 15, tzinfo=timezone.utc)
```

Add import:

```python
from datetime import timedelta
```

- [ ] **Step 2: Run red tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_models_queue.py -q
```

Expected: fail because `LaneFormatMatrix.source_platforms_json`, `ChannelOpsQueueItem.channel_profile_id`, queue `channel_profile_id` parameter, race handling, or backoff behavior is missing.

- [ ] **Step 3: Add model and migration fields**

In `backend/app/models/channel_agent.py`, update `LaneFormatMatrix`:

```python
source_platforms_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
```

Update `ChannelOpsQueueItem`:

```python
channel_profile_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
```

Add indexes in `__table_args__`:

```python
Index("ix_channel_ops_queue_channel_profile_id", "channel_profile_id"),
Index("ix_channel_ops_queue_channel_status", "channel_profile_id", "status"),
```

Create `backend/alembic/versions/012_channel_ops_hardening.py`:

```python
"""channel ops hardening fields

Revision ID: 012_channel_ops_hardening
Revises: 011_channel_agent_production
Create Date: 2026-05-18
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "012_channel_ops_hardening"
down_revision = "011_channel_agent_production"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "lane_format_matrix",
        sa.Column(
            "source_platforms_json",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::json"),
        ),
    )
    op.add_column(
        "channel_ops_queue_items",
        sa.Column("channel_profile_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_index(
        "ix_channel_ops_queue_channel_profile_id",
        "channel_ops_queue_items",
        ["channel_profile_id"],
    )
    op.create_index(
        "ix_channel_ops_queue_channel_status",
        "channel_ops_queue_items",
        ["channel_profile_id", "status"],
    )
    op.alter_column("lane_format_matrix", "source_platforms_json", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_channel_ops_queue_channel_status", table_name="channel_ops_queue_items")
    op.drop_index("ix_channel_ops_queue_channel_profile_id", table_name="channel_ops_queue_items")
    op.drop_column("channel_ops_queue_items", "channel_profile_id")
    op.drop_column("lane_format_matrix", "source_platforms_json")
```

If SQLite migration compatibility is needed for tests, keep tests creating SQLAlchemy tables directly; Alembic smoke is covered by import/metadata.

- [ ] **Step 4: Update schemas**

In `backend/app/schemas/channel_agent.py`, update lane format create/read models to include:

```python
source_platforms_json: list[str] = Field(default_factory=list)
```

Ensure any `_lane_format()` serializer in `backend/app/api/channel_agent.py` later returns this field; API serializer wiring is completed in Task 6.

- [ ] **Step 5: Harden queue service**

In `backend/app/channel_agent/queue.py`, import:

```python
from sqlalchemy.exc import IntegrityError
```

Change `enqueue()` signature:

```python
async def enqueue(
    self,
    db: AsyncSession,
    *,
    kind: str,
    idempotency_key: str,
    payload: dict[str, Any] | None = None,
    priority: int = 100,
    run_after: datetime | None = None,
    parent_queue_item_id=None,
    channel_profile_id=None,
    max_attempts: int = 3,
) -> ChannelOpsQueueItem:
```

Set the field when creating the row:

```python
item = ChannelOpsQueueItem(
    kind=kind,
    idempotency_key=idempotency_key,
    channel_profile_id=channel_profile_id,
    payload_json=dict(payload or {}),
    priority=priority,
    run_after=run_after or self.clock.now(),
    parent_queue_item_id=parent_queue_item_id,
    max_attempts=max_attempts,
)
```

Wrap commit:

```python
try:
    await db.commit()
except IntegrityError:
    await db.rollback()
    existing = await self.get_by_key(db, idempotency_key)
    if existing is None:
        raise
    return existing
```

Add helper:

```python
def _retry_delay_for_attempt(attempt_count: int) -> timedelta:
    base = timedelta(minutes=5)
    cap = timedelta(minutes=30)
    multiplier = 2 ** max(0, attempt_count - 1)
    return min(base * multiplier, cap)
```

In `mark_failed_or_retry()`, replace the fixed 5-minute default with:

```python
delay = retry_delay or _retry_delay_for_attempt(int(item.attempt_count or 1))
item.run_after = self.clock.now() + delay
```

- [ ] **Step 6: Run green tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_models_queue.py -q
```

Expected: all `test_models_queue.py` tests pass.

- [ ] **Step 7: Commit**

```bash
git add backend/app/models/channel_agent.py backend/alembic/versions/012_channel_ops_hardening.py backend/app/schemas/channel_agent.py backend/app/channel_agent/queue.py backend/tests/channel_agent/test_models_queue.py
git commit -m "feat: harden channel ops queue schema"
```

## Task 2: AutoFlow Request Configuration And Safe Privacy

**Files:**
- Modify: `backend/app/channel_agent/service.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Add failing request configuration test**

Append to `backend/tests/channel_agent/test_service.py`:

```python
def test_autoflow_request_uses_task_snapshot_configuration():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a tech short",
        title_seed="AI news",
        source_platforms_json=["bilibili", "youtube"],
        material_library_ids_json=["library-1"],
        uses_external_assets=True,
        channel_config_snapshot_json={
            "channel": {
                "id": "channel-1",
                "default_aspect_ratio": "16:9",
                "risk_policy_json": {
                    "source_strategy": "external_search",
                    "planning_mode": "ai_graph",
                },
            },
            "lane": {"id": "lane-1", "name": "Tech"},
            "lane_format": {
                "id": "format-1",
                "format_key": "long_16x9",
                "target_duration_sec": 60,
                "template_pool_json": ["news_remix"],
                "default_publish_visibility": "unlisted",
            },
            "manual_seed": {"constraints_json": {"tone": "calm"}},
        },
    )
    request = _service()._autoflow_request(task)

    assert request["duration_sec"] == 60
    assert request["aspect_ratio"] == "16:9"
    assert request["source_platforms"] == ["bilibili", "youtube"]
    assert request["source_strategy"] == "external_search"
    assert request["planning_mode"] == "ai_graph"
    assert request["constraints"]["template_pool_json"] == ["news_remix"]
    assert request["constraints"]["tone"] == "calm"
```

Append safe privacy test:

```python
def test_desired_privacy_falls_back_to_unlisted_not_public():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a short",
        channel_config_snapshot_json={"lane_format": {}},
    )
    account = PublishingAccount(
        channel_profile_id=uuid.uuid4(),
        account_label="main",
        platform_account_id="yt",
        credential_ref="youtube/main",
        default_privacy="public",
    )

    assert _service()._desired_privacy(task, account) == "unlisted"
```

- [ ] **Step 2: Run red tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py::test_autoflow_request_uses_task_snapshot_configuration tests/channel_agent/test_service.py::test_desired_privacy_falls_back_to_unlisted_not_public -q
```

Expected: fail because `_autoflow_request()` still hard-codes duration/aspect and `_desired_privacy()` can fall back to public.

- [ ] **Step 3: Expand snapshot shape**

In `_snapshot()` in `backend/app/channel_agent/service.py`, include channel and lane format fields:

```python
"channel": {
    "id": str(channel.id),
    "dry_run": channel.dry_run,
    "default_aspect_ratio": channel.default_aspect_ratio,
    "risk_policy_json": dict(channel.risk_policy_json or {}),
    "cadence_policy_json": dict(channel.cadence_policy_json or {}),
    "content_mix_policy_json": dict(channel.content_mix_policy_json or {}),
},
"lane_format": {
    "id": str(lane_format.id) if lane_format else None,
    "format_key": lane_format.format_key if lane_format else "",
    "default_publish_visibility": lane_format.default_publish_visibility if lane_format else "private",
    "target_duration_sec": lane_format.target_duration_sec if lane_format else 30,
    "template_pool_json": list(lane_format.template_pool_json or []) if lane_format else [],
    "source_platforms_json": list(lane_format.source_platforms_json or []) if lane_format else [],
},
```

When creating a task from a manual seed, include:

```python
"manual_seed": {"constraints_json": dict(seed.constraints_json or {})}
```

- [ ] **Step 4: Implement request builder**

Replace `_autoflow_request()` with logic equivalent to:

```python
def _autoflow_request(self, task: ProductionTask) -> dict[str, Any]:
    snapshot = dict(task.channel_config_snapshot_json or {})
    channel = dict(snapshot.get("channel") or {})
    lane_format = dict(snapshot.get("lane_format") or {})
    manual_seed = dict(snapshot.get("manual_seed") or {})
    risk_policy = dict(channel.get("risk_policy_json") or {})
    constraints = {
        "lane_id": (snapshot.get("lane") or {}).get("id"),
        "lane_format_id": lane_format.get("id"),
        "template_pool_json": list(lane_format.get("template_pool_json") or []),
    }
    constraints.update(dict(manual_seed.get("constraints_json") or {}))

    return {
        "prompt": task.prompt,
        "target_platforms": ["youtube"],
        "source_platforms": list(task.source_platforms_json or lane_format.get("source_platforms_json") or []),
        "duration_sec": int(lane_format.get("target_duration_sec") or 30),
        "aspect_ratio": str(channel.get("default_aspect_ratio") or "9:16"),
        "source_policy": "remix_with_review" if task.uses_external_assets else "owned_only",
        "publish_mode": self._autoflow_publish_mode(task),
        "material_library_ids": list(task.material_library_ids_json or []),
        "source_strategy": str(manual_seed.get("source_strategy") or risk_policy.get("source_strategy") or "auto"),
        "planning_mode": str(manual_seed.get("planning_mode") or risk_policy.get("planning_mode") or "auto"),
        "constraints": constraints,
    }
```

Add `_autoflow_publish_mode()`:

```python
def _autoflow_publish_mode(self, task: ProductionTask) -> str:
    privacy = self._desired_privacy_from_snapshot(task)
    if privacy == "unlisted":
        return "unlisted_upload"
    return "private_upload"
```

Change `_desired_privacy()` to allow only `private` or `unlisted` as automatic upload visibility:

```python
def _desired_privacy_from_snapshot(self, task: ProductionTask) -> str:
    snapshot = dict(task.channel_config_snapshot_json or {})
    lane_format = dict(snapshot.get("lane_format") or {})
    desired = str(lane_format.get("default_publish_visibility") or "").lower()
    if desired in {"private", "unlisted"}:
        return desired
    return "unlisted"

def _desired_privacy(self, task: ProductionTask, account: PublishingAccount) -> str:
    snapshot_privacy = self._desired_privacy_from_snapshot(task)
    account_privacy = str(account.default_privacy or "").lower()
    if snapshot_privacy in {"private", "unlisted"}:
        return snapshot_privacy
    if account_privacy in {"private", "unlisted"}:
        return account_privacy
    return "unlisted"
```

- [ ] **Step 5: Run green tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py::test_autoflow_request_uses_task_snapshot_configuration tests/channel_agent/test_service.py::test_desired_privacy_falls_back_to_unlisted_not_public -q
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```bash
git add backend/app/channel_agent/service.py backend/tests/channel_agent/test_service.py
git commit -m "fix: build channel ops autoflow requests from config"
```

## Task 3: Lane-Driven Candidates And Dry-Run Decisions

**Files:**
- Create: `backend/app/channel_agent/lane_prompts.py`
- Modify: `backend/app/channel_agent/service.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Add failing lane prompt test**

Append:

```python
def test_lane_prompt_template_is_structured():
    from app.channel_agent.lane_prompts import build_lane_prompt

    prompt = build_lane_prompt(
        lane_name="Tech News",
        lane_description="Daily AI product updates",
        keywords=["AI", "robots"],
        format_key="shorts_9x16",
        duration_sec=30,
        aspect_ratio="9:16",
    )

    assert 'Create a shorts_9x16 video for the "Tech News" topic.' in prompt
    assert "Theme: Daily AI product updates" in prompt
    assert "Keywords: AI, robots" in prompt
    assert "Target duration: 30s, aspect ratio 9:16." in prompt
```

- [ ] **Step 2: Add failing lane-driven service tests**

Append:

```python
@pytest.mark.asyncio
async def test_lane_driven_tick_creates_task_without_manual_seed(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.description = "Daily AI updates"
    lane.keywords_json = ["AI"]
    lane_format.source_platforms_json = ["bilibili", "youtube"]
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.source == "lane_seed"
    assert task.manual_seed_id is None
    assert task.topic_lane_id == lane.id
    assert task.lane_format_id == lane_format.id
    assert task.source_platforms_json == ["bilibili", "youtube"]
    assert "Daily AI updates" in task.prompt
```

Append a mixed manual/lane budget test using two accounts:

```python
@pytest.mark.asyncio
async def test_manual_seed_consumes_first_then_lane_driven_fills_budget(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 3
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
        external_asset_auto_publish=True,
    )
    service_session.add(second_account)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=second_account.id,
            prompt="manual short",
            title_seed="manual",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)
    tasks = (await service_session.execute(select(ProductionTask).order_by(ProductionTask.created_at.asc()))).scalars().all()

    assert audit.tasks_selected == 2
    assert [task.source for task in tasks] == ["manual_seed", "lane_seed"]
    assert {task.target_account_id for task in tasks} == {account.id, second_account.id}
```

Append a dry-run test:

```python
@pytest.mark.asyncio
async def test_dry_run_evaluates_candidates_without_creating_tasks_or_queue(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=True)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt="held task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="dry run candidate",
            title_seed="dry",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]

    assert len(tasks) == 1
    assert queue_items == []
    assert audit.candidates_scored >= 1
    assert rejected[0]["guard"] == "account_concurrency"
```

- [ ] **Step 3: Run red tests**

Run:

```bash
cd backend
python3 -m pytest \
  tests/channel_agent/test_service.py::test_lane_prompt_template_is_structured \
  tests/channel_agent/test_service.py::test_lane_driven_tick_creates_task_without_manual_seed \
  tests/channel_agent/test_service.py::test_manual_seed_consumes_first_then_lane_driven_fills_budget \
  tests/channel_agent/test_service.py::test_dry_run_evaluates_candidates_without_creating_tasks_or_queue \
  -q
```

Expected: fail because prompt helper and lane-driven/dry-run candidate evaluation are missing.

- [ ] **Step 4: Add lane prompt helper**

Create `backend/app/channel_agent/lane_prompts.py`:

```python
from __future__ import annotations


PROMPT_TEMPLATE = """Create a {format_key} video for the "{lane_name}" topic.
Theme: {lane_description}
Keywords: {keywords}
Target duration: {duration_sec}s, aspect ratio {aspect_ratio}.
"""


def build_lane_prompt(
    *,
    lane_name: str,
    lane_description: str,
    keywords: list[str],
    format_key: str,
    duration_sec: int,
    aspect_ratio: str,
) -> str:
    keyword_text = ", ".join(keyword for keyword in keywords if keyword) or lane_name
    return PROMPT_TEMPLATE.format(
        lane_name=lane_name,
        lane_description=lane_description or lane_name,
        keywords=keyword_text,
        format_key=format_key,
        duration_sec=duration_sec,
        aspect_ratio=aspect_ratio,
    ).strip()
```

- [ ] **Step 5: Add candidate helpers**

In `backend/app/channel_agent/service.py`, define internal candidate helpers near the service class:

```python
class CandidateSelection(dict):
    pass


def _candidate_id(*, source: str, lane_id, format_id, bucket: str) -> str:
    return f"{source}:lane:{lane_id}:format:{format_id}:{bucket}"
```

Implement helper methods with this shape:

```python
async def _manual_seed_candidates(
    self,
    db: AsyncSession,
    *,
    channel: ChannelProfile,
    lanes: list[TopicLane],
    accounts: list[PublishingAccount],
    seeds: list[ManualSeed],
    bucket: str,
) -> list[dict[str, Any]]:
    lane_by_id = {lane.id: lane for lane in lanes}
    candidates: list[dict[str, Any]] = []
    used_account_ids: set[Any] = set()
    for seed in seeds:
        lane = lane_by_id.get(seed.topic_lane_id) if seed.topic_lane_id else (lanes[0] if lanes else None)
        lane_format = await self._resolve_lane_format(db, lane.id if lane else None)
        account = await self._resolve_account_for_candidate(
            db,
            explicit_account_id=seed.target_account_id,
            accounts=accounts,
            used_account_ids=used_account_ids,
        )
        if lane is None or account is None:
            continue
        used_account_ids.add(account.id)
        candidates.append(
            {
                "candidate_id": _candidate_id(
                    source="manual_seed",
                    lane_id=lane.id,
                    format_id=lane_format.id if lane_format else "default",
                    bucket=bucket,
                ),
                "source": "manual_seed",
                "seed": seed,
                "lane": lane,
                "lane_format": lane_format,
                "account": account,
                "prompt": seed.prompt,
                "title_seed": seed.title_seed,
                "source_platforms_json": list(seed.source_platforms_json or []),
                "material_library_ids_json": list(seed.material_library_ids_json or []),
                "constraints_json": dict(seed.constraints_json or {}),
            }
        )
    return candidates


async def _lane_driven_candidates(
    self,
    db: AsyncSession,
    *,
    channel: ChannelProfile,
    lanes: list[TopicLane],
    accounts: list[PublishingAccount],
    used_account_ids: set[Any],
    bucket: str,
) -> list[dict[str, Any]]:
    from app.channel_agent.lane_prompts import build_lane_prompt

    candidates: list[dict[str, Any]] = []
    for lane in lanes:
        for lane_format in await self._lane_formats(db, lane.id):
            account = await self._resolve_account_for_candidate(
                db,
                explicit_account_id=None,
                accounts=accounts,
                used_account_ids=used_account_ids,
            )
            if account is None:
                continue
            used_account_ids.add(account.id)
            prompt = build_lane_prompt(
                lane_name=lane.name,
                lane_description=lane.description,
                keywords=list(lane.keywords_json or []),
                format_key=lane_format.format_key,
                duration_sec=lane_format.target_duration_sec,
                aspect_ratio=channel.default_aspect_ratio,
            )
            candidates.append(
                {
                    "candidate_id": _candidate_id(
                        source="lane_seed",
                        lane_id=lane.id,
                        format_id=lane_format.id,
                        bucket=bucket,
                    ),
                    "source": "lane_seed",
                    "seed": None,
                    "lane": lane,
                    "lane_format": lane_format,
                    "account": account,
                    "prompt": prompt,
                    "title_seed": f"{lane.name} {lane_format.format_key}",
                    "source_platforms_json": self._source_platforms_for_lane_format(channel, lane_format),
                    "material_library_ids_json": [],
                    "constraints_json": {},
                }
            )
    return candidates


async def _lane_formats(self, db: AsyncSession, lane_id) -> list[LaneFormatMatrix]:
    result = await db.execute(
        select(LaneFormatMatrix)
        .where(LaneFormatMatrix.topic_lane_id == lane_id)
        .where(LaneFormatMatrix.enabled.is_(True))
        .order_by(LaneFormatMatrix.weight.desc(), LaneFormatMatrix.created_at.asc())
    )
    return list(result.scalars().all())

def _source_platforms_for_lane_format(self, channel: ChannelProfile, lane_format: LaneFormatMatrix) -> list[str]:
    if lane_format.source_platforms_json:
        return list(lane_format.source_platforms_json)
    defaults = (channel.risk_policy_json or {}).get("default_source_platforms") or []
    return [str(value) for value in defaults] or ["youtube"]
```

Candidates should contain:

```python
{
    "candidate_id": "manual_seed:lane:<lane_id>:format:<format_id>:<bucket>",
    "source": "manual_seed" or "lane_seed",
    "seed": seed_or_none,
    "lane": lane,
    "lane_format": lane_format,
    "account": account,
    "prompt": "Create a shorts_9x16 video for the Tech topic.",
    "title_seed": "Tech shorts_9x16",
    "source_platforms_json": ["youtube"],
    "material_library_ids_json": [],
    "constraints_json": {},
}
```

Account selection rules:

- Explicit manual seed `target_account_id` wins.
- Lane-driven candidates pick the first enabled account by `created_at` that is not already selected in the same tick.
- If every enabled account is already selected, return the first enabled account anyway so `AccountConcurrencyGuard` records a rejection instead of silently dropping the candidate.

- [ ] **Step 6: Rewrite tick to evaluate before side effects**

Change `tick()` flow:

```python
bucket = utc_hour_bucket(self.clock.now())
candidates = []
manual_candidates = await self._manual_seed_candidates(
    db,
    channel=channel,
    lanes=lanes,
    accounts=accounts,
    seeds=seeds,
    bucket=bucket,
)
candidates.extend(manual_candidates)
used_account_ids = {candidate["account"].id for candidate in manual_candidates}
lane_candidates = await self._lane_driven_candidates(
    db,
    channel=channel,
    lanes=lanes,
    accounts=accounts,
    used_account_ids=used_account_ids,
    bucket=bucket,
)
candidates.extend(lane_candidates)

accepted: list[dict[str, Any]] = []
rejected: list[dict[str, Any]] = []
for candidate in candidates:
    rejection = await self._evaluate_candidate_guards(db, channel=channel, candidate=candidate, accepted=accepted)
    if rejection:
        rejected.append(rejection)
    else:
        accepted.append(candidate)
```

For dry-run or halted channel:

```python
audit.ideas_discovered = len(candidates)
audit.candidates_scored = len(candidates)
audit.tasks_selected = len(accepted)
audit.tasks_rejected = len(rejected)
audit.decision_summary_json = {
    "per_lane_eligible_count": per_lane,
    "rejected_candidates": rejected,
}
audit.guards_triggered_json = [{"guard": item["guard"], "candidate_id": item["candidate_id"]} for item in rejected]
await db.commit()
```

For live mode, create `ProductionTask` only for accepted candidates and enqueue `plan_task` with `channel_profile_id=channel.id`.

- [ ] **Step 7: Run green tests**

Run:

```bash
cd backend
python3 -m pytest \
  tests/channel_agent/test_service.py::test_lane_prompt_template_is_structured \
  tests/channel_agent/test_service.py::test_lane_driven_tick_creates_task_without_manual_seed \
  tests/channel_agent/test_service.py::test_manual_seed_consumes_first_then_lane_driven_fills_budget \
  tests/channel_agent/test_service.py::test_dry_run_evaluates_candidates_without_creating_tasks_or_queue \
  -q
```

Expected: all four tests pass.

- [ ] **Step 8: Commit**

```bash
git add backend/app/channel_agent/lane_prompts.py backend/app/channel_agent/service.py backend/tests/channel_agent/test_service.py
git commit -m "feat: add channel ops lane-driven candidates"
```

## Task 4: Guards

**Files:**
- Modify: `backend/app/channel_agent/constants.py`
- Modify: `backend/app/channel_agent/service.py`
- Test: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Add failing guard tests**

Append account concurrency test if Task 3 only covered dry-run:

```python
@pytest.mark.asyncio
async def test_account_concurrency_guard_blocks_active_account(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt="held task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new task",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.tasks_rejected == 1
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "account_concurrency"
```

Append upload failure window test:

```python
@pytest.mark.asyncio
async def test_consecutive_upload_failure_guard_uses_recent_window_and_alerts(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    for index, reason in enumerate(["youtube upload failed", "ok", "quota exhausted", "thumbnail failed", "publish failed"]):
        state = "failed" if index != 1 else "measured"
        task = ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt=f"task {index}",
            state=state,
            failure_reason=reason if state == "failed" else None,
            channel_config_snapshot_json={},
            created_at=clock.now() - timedelta(hours=5 - index),
        )
        service_session.add(task)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="blocked",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    rejected = audit.decision_summary_json["rejected_candidates"][0]
    assert rejected["guard"] == "consecutive_upload_failure"
    alert = (await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert"))).scalar_one()
    assert "pause the account" in alert.payload_json["message"]
```

Append lane cadence publication-based test:

```python
@pytest.mark.asyncio
async def test_lane_cadence_guard_counts_publications_not_created_tasks(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    await service_session.flush()
    for index in range(4):
        service_session.add(
            ProductionTask(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                lane_format_id=lane_format.id,
                target_account_id=account.id,
                source="manual_seed",
                prompt=f"held {index}",
                state="held",
                channel_config_snapshot_json={},
            )
        )
    published_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="published",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(published_task)
    await service_session.flush()
    service_session.add(
        PublicationRecord(
            production_task_id=published_task.id,
            platform="youtube",
            account_id=account.id,
            platform_content_id="yt-1",
            title="published",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="scheduled",
            scheduled_publish_at=clock.now() - timedelta(hours=1),
            compliance_disposition="assumed_fair_use",
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "lane_cadence"
```

- [ ] **Step 2: Run red tests**

Run:

```bash
cd backend
python3 -m pytest \
  tests/channel_agent/test_service.py::test_account_concurrency_guard_blocks_active_account \
  tests/channel_agent/test_service.py::test_consecutive_upload_failure_guard_uses_recent_window_and_alerts \
  tests/channel_agent/test_service.py::test_lane_cadence_guard_counts_publications_not_created_tasks \
  -q
```

Expected: guard tests fail until helpers are implemented.

- [ ] **Step 3: Add constants**

In `backend/app/channel_agent/constants.py`, add:

```python
TASK_PRODUCING = "producing"
TASK_PUBLISHED = "published"
TASK_MEASURED = "measured"
TASK_REJECTED = "rejected"
TASK_CANCELLED = "cancelled"

ACTIVE_TASK_STATES = {
    TASK_SELECTED,
    TASK_PLANNING,
    TASK_PRODUCING,
    TASK_UPLOADED_PRIVATE,
    TASK_HELD,
    TASK_SCHEDULED,
}

TERMINAL_TASK_STATES = {
    TASK_FAILED,
    TASK_REJECTED,
    TASK_CANCELLED,
    TASK_PUBLISHED,
    TASK_MEASURED,
}

UPLOAD_FAILURE_KEYWORDS = {
    "upload",
    "publish",
    "youtube",
    "quota",
    "oauth",
    "video_id",
    "thumbnail",
}
```

- [ ] **Step 4: Implement guard evaluator**

In `ChannelAgentService`, add:

```python
async def _evaluate_candidate_guards(
    self,
    db: AsyncSession,
    *,
    channel: ChannelProfile,
    candidate: dict[str, Any],
    accepted: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for guard in (
        self._account_concurrency_guard,
        self._consecutive_upload_failure_guard,
        self._lane_cadence_guard,
    ):
        rejection = await guard(db, channel=channel, candidate=candidate, accepted=accepted)
        if rejection:
            return rejection
    return None
```

Rejection shape:

```python
def _rejection(candidate: dict[str, Any], *, guard: str, reason: str) -> dict[str, Any]:
    lane = candidate.get("lane")
    lane_format = candidate.get("lane_format")
    account = candidate.get("account")
    return {
        "candidate_id": candidate["candidate_id"],
        "lane_id": str(lane.id) if lane else None,
        "format_id": str(lane_format.id) if lane_format else None,
        "account_id": str(account.id) if account else None,
        "guard": guard,
        "reason": reason,
    }
```

Account concurrency guard:

```python
async def _account_concurrency_guard(self, db, *, channel, candidate, accepted):
    account = candidate["account"]
    if any(item["account"].id == account.id for item in accepted):
        return _rejection(candidate, guard="account_concurrency", reason=f"Account {account.account_label} already selected in this tick")
    count = await self._active_task_count_for_account(db, account.id)
    if count > 0:
        return _rejection(candidate, guard="account_concurrency", reason=f"Account {account.account_label} has {count} active task")
    return None
```

Upload failure guard:

```python
async def _consecutive_upload_failure_guard(self, db, *, channel, candidate, accepted):
    account = candidate["account"]
    failures = await self._recent_upload_failure_window(db, account.id)
    if failures["blocked"]:
        await self._enqueue_alert(
            db,
            "consecutive_upload_failure",
            resource_id=str(account.id),
            severity="warning",
            message=(
                f"3 of last 5 uploads failed in 24h for account {account.account_label}. "
                f"Suggested action: pause the account via POST /api/v1/channel-agent/accounts/{account.id}/pause "
                f"or inspect failed tasks in /api/v1/channel-agent/channels/{channel.id}/tasks."
            ),
            details={"channel_id": str(channel.id), "account_label": account.account_label},
            channel_profile_id=channel.id,
        )
        return _rejection(candidate, guard="consecutive_upload_failure", reason=failures["reason"])
    return None
```

Lane cadence guard must query `PublicationRecord` joined to `ProductionTask` by `production_task_id`, with statuses `public` and `scheduled`.

- [ ] **Step 5: Run green tests**

Run:

```bash
cd backend
python3 -m pytest \
  tests/channel_agent/test_service.py::test_account_concurrency_guard_blocks_active_account \
  tests/channel_agent/test_service.py::test_consecutive_upload_failure_guard_uses_recent_window_and_alerts \
  tests/channel_agent/test_service.py::test_lane_cadence_guard_counts_publications_not_created_tasks \
  -q
```

Expected: all guard tests pass.

- [ ] **Step 6: Run service test suite**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py -q
```

Expected: all service tests pass.

- [ ] **Step 7: Commit**

```bash
git add backend/app/channel_agent/constants.py backend/app/channel_agent/service.py backend/tests/channel_agent/test_service.py
git commit -m "feat: add channel ops selection guards"
```

## Task 5: Runner Closure, AutoFlow Observation, And Metrics

**Files:**
- Modify: `backend/app/channel_agent/clients.py`
- Modify: `backend/app/channel_agent/service.py`
- Modify: `backend/app/channel_agent/runner.py`
- Create: `backend/tests/channel_agent/test_local_autoflow_client.py`
- Modify: `backend/tests/channel_agent/test_service.py`

- [ ] **Step 1: Add failing fake e2e flow test**

Append to `backend/tests/channel_agent/test_service.py`:

```python
@pytest.mark.asyncio
async def test_queue_flow_reaches_scheduled_publication_and_metrics(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    account.external_asset_auto_publish = True
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="make a test short",
            title_seed="test short",
        )
    )
    await service_session.commit()
    queue = ChannelOpsQueueService(clock=clock)
    service = ChannelAgentService(
        queue=queue,
        clock=clock,
        autoflow_client=FakeAutoFlowClient(include_upload=True, youtube_video_id="yt-video-1"),
        youtube_client=FakeYouTubeClient(),
        minimax_client=FakeMiniMaxClient(),
    )

    await service.tick(service_session, channel_id=channel.id)
    plan_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_plan_task(service_session, plan_item)
    execute_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_execute_task(service_session, execute_item)
    observe_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_observe_job(service_session, observe_item)
    publish_item = await queue.claim_next(service_session, worker_id="test")
    publication = await service.handle_publish_task(service_session, publish_item)
    promote_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_promote_publication(service_session, promote_item)
    metrics_item = await queue.claim_next(service_session, worker_id="test")
    snapshot = await service.handle_collect_metrics(service_session, metrics_item)

    await service_session.refresh(publication)
    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.state == "measured"
    assert publication.publish_status == "scheduled"
    assert publication.platform_content_id == "yt-video-1"
    assert snapshot.publication_id == publication.id
```

- [ ] **Step 2: Add failing local AutoFlow observation test**

Create `backend/tests/channel_agent/test_local_autoflow_client.py`:

```python
from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.clients import LocalAutoFlowClient
from app.models.artifact import Artifact, ArtifactKind
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.pipeline import Pipeline  # registers the referenced table in metadata


@pytest.mark.asyncio
async def test_local_autoflow_client_reads_youtube_video_id_from_artifact_media_info():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.exec_driver_sql("CREATE TABLE pipelines (id CHAR(32) PRIMARY KEY)")
        await conn.run_sync(Job.__table__.create)
        await conn.run_sync(NodeExecution.__table__.create)
        await conn.run_sync(Artifact.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as db:
        job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.SUCCEEDED)
        db.add(job)
        await db.flush()
        node = NodeExecution(
            job_id=job.id,
            node_id="youtube_upload_1",
            node_type="youtube_upload",
            node_config={},
            status=NodeStatus.SUCCEEDED,
        )
        db.add(node)
        await db.flush()
        artifact = Artifact(
            job_id=job.id,
            node_execution_id=node.id,
            kind=ArtifactKind.FINAL,
            filename="upload.mp4",
            storage_backend="local",
            storage_path="/tmp/upload.mp4",
            media_info={"youtube": {"video_id": "yt-local-1"}},
        )
        db.add(artifact)
        await db.flush()
        node.output_artifact_id = artifact.id
        await db.commit()

        observation = await LocalAutoFlowClient().observe_job(db, run_id=str(uuid.uuid4()), job_id=str(job.id))

    await engine.dispose()
    assert observation.status == "succeeded"
    assert observation.youtube["video_id"] == "yt-local-1"
```

- [ ] **Step 3: Run red tests**

Run:

```bash
cd backend
python3 -m pytest \
  tests/channel_agent/test_service.py::test_queue_flow_reaches_scheduled_publication_and_metrics \
  tests/channel_agent/test_local_autoflow_client.py \
  -q
```

Expected: fail because handlers/client classes are missing.

- [ ] **Step 4: Extend clients**

In `backend/app/channel_agent/clients.py`, add dataclasses:

```python
@dataclass(frozen=True)
class AutoFlowExecutionObservation:
    run_id: str
    pipeline_id: str | None
    job_id: str | None
    status: str
    error_message: str | None = None


@dataclass(frozen=True)
class AutoFlowJobObservation:
    run_id: str
    pipeline_id: str | None
    job_id: str | None
    status: str
    error_message: str | None = None
    youtube: dict[str, Any] | None = None
```

Extend `AutoFlowClient` protocol:

```python
async def execute_task(self, task, request: dict[str, Any]) -> AutoFlowExecutionObservation:
    raise NotImplementedError

async def observe_job(self, db, *, run_id: str, job_id: str) -> AutoFlowJobObservation:
    raise NotImplementedError
```

Extend `FakeAutoFlowClient.__init__`:

```python
def __init__(self, *, include_upload: bool = True, youtube_video_id: str = "yt-fake-1", observe_running_once: bool = False):
```

Implement fake methods:

```python
async def execute_task(self, task, request: dict[str, Any]) -> AutoFlowExecutionObservation:
    run_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"run:{task.id}"))
    pipeline_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"pipeline:{task.id}"))
    job_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"job:{task.id}"))
    return AutoFlowExecutionObservation(run_id=run_id, pipeline_id=pipeline_id, job_id=job_id, status="running")

async def observe_job(self, db, *, run_id: str, job_id: str) -> AutoFlowJobObservation:
    return AutoFlowJobObservation(
        run_id=run_id,
        pipeline_id=None,
        job_id=job_id,
        status="succeeded",
        youtube={"video_id": self.youtube_video_id},
    )
```

Add `LocalAutoFlowClient` with `execute_task()` calling `autoflow_service.execute()` and `observe_job()` reading `AutoFlowRun`, `Job`, `NodeExecution`, and `Artifact.media_info`.

- [ ] **Step 5: Implement service handlers**

In `ChannelAgentService`, add:

```python
async def handle_execute_task(self, db: AsyncSession, item: ChannelOpsQueueItem) -> ProductionTask:
    task = await self._task_from_item(db, item)
    observation = await self.autoflow_client.execute_task(task, self._autoflow_request(task))
    if observation.status == "failed":
        task.state = TASK_FAILED
        task.failure_reason = observation.error_message or "AutoFlow execution failed"
    else:
        task.autoflow_run_id = _uuid(observation.run_id)
        if observation.pipeline_id:
            task.pipeline_id = _uuid(observation.pipeline_id)
        if observation.job_id:
            task.job_id = _uuid(observation.job_id)
        task.state = TASK_PRODUCING
        await self.queue.enqueue(
            db,
            kind="observe_job",
            idempotency_key=f"observe_job:{task.id}:{observation.run_id}",
            payload={"production_task_id": str(task.id), "run_id": observation.run_id, "job_id": observation.job_id},
            priority=65,
            parent_queue_item_id=item.id,
            channel_profile_id=task.channel_profile_id,
        )
    task.state_updated_at = self.clock.now()
    await db.commit()
    await db.refresh(task)
    return task
```

Add `handle_observe_job()`:

```python
async def handle_observe_job(self, db: AsyncSession, item: ChannelOpsQueueItem) -> ProductionTask:
    task = await self._task_from_item(db, item)
    observation = await self.autoflow_client.observe_job(
        db,
        run_id=str(item.payload_json.get("run_id") or task.autoflow_run_id),
        job_id=str(item.payload_json.get("job_id") or task.job_id),
    )
    if observation.status in {"pending", "running"}:
        observe_count = int(item.payload_json.get("observe_count") or 0) + 1
        delay_seconds = min(30 * (2 ** max(0, observe_count - 1)), 300)
        await self.queue.enqueue(
            db,
            kind="observe_job",
            idempotency_key=f"observe_job:{task.id}:{observation.run_id}:{observe_count}",
            payload={**dict(item.payload_json), "observe_count": observe_count},
            priority=65,
            run_after=self.clock.now() + timedelta(seconds=delay_seconds),
            parent_queue_item_id=item.id,
            channel_profile_id=task.channel_profile_id,
        )
    elif observation.status == "failed":
        task.state = TASK_FAILED
        task.failure_reason = observation.error_message or "AutoFlow job failed"
    elif observation.youtube and observation.youtube.get("video_id"):
        await self.queue.enqueue(
            db,
            kind="publish_task",
            idempotency_key=f"publish_task:{task.id}",
            payload={"production_task_id": str(task.id), "youtube": observation.youtube},
            priority=70,
            parent_queue_item_id=item.id,
            channel_profile_id=task.channel_profile_id,
        )
    else:
        task.state = TASK_HELD
        task.blocked_by_guard = "missing_youtube_observation"
        task.failure_reason = "AutoFlow job succeeded without youtube.video_id"
    task.state_updated_at = self.clock.now()
    await db.commit()
    await db.refresh(task)
    return task
```

Modify `handle_promote_publication()` to enqueue metrics:

```python
await self.queue.enqueue(
    db,
    kind="collect_metrics",
    idempotency_key=f"collect_metrics:{publication.id}:{utc_hour_bucket(self.clock.now())}",
    payload={"publication_id": str(publication.id)},
    priority=90,
    parent_queue_item_id=item.id,
    channel_profile_id=task.channel_profile_id if task else None,
)
```

Add `handle_collect_metrics()`:

```python
async def handle_collect_metrics(self, db: AsyncSession, item: ChannelOpsQueueItem) -> FeedbackSnapshot:
    publication = await db.get(PublicationRecord, _uuid(item.payload_json["publication_id"]))
    if publication is None:
        raise ValueError("Publication not found")
    snapshot = FeedbackSnapshot(
        publication_id=publication.id,
        collected_at=self.clock.now(),
        views=int(item.payload_json.get("views") or 0),
        likes=int(item.payload_json.get("likes") or 0),
        comments=int(item.payload_json.get("comments") or 0),
        shares=int(item.payload_json.get("shares") or 0),
        raw_json=dict(item.payload_json),
    )
    db.add(snapshot)
    publication.last_metrics_polled_at = self.clock.now()
    task = await db.get(ProductionTask, publication.production_task_id)
    if task is not None:
        task.state = TASK_MEASURED
        task.state_updated_at = self.clock.now()
    await db.commit()
    await db.refresh(snapshot)
    return snapshot
```

- [ ] **Step 6: Route runner kinds**

In `backend/app/channel_agent/runner.py`, replace the no-op block:

```python
elif item.kind == "execute_task":
    await self.service.handle_execute_task(db, item)
elif item.kind == "observe_job":
    await self.service.handle_observe_job(db, item)
elif item.kind == "collect_metrics":
    await self.service.handle_collect_metrics(db, item)
```

Leave only future unsupported kinds out of the alpha critical path.

- [ ] **Step 7: Run green tests**

Run:

```bash
cd backend
python3 -m pytest \
  tests/channel_agent/test_service.py::test_queue_flow_reaches_scheduled_publication_and_metrics \
  tests/channel_agent/test_local_autoflow_client.py \
  -q
```

Expected: tests pass.

- [ ] **Step 8: Run channel agent service tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_service.py tests/channel_agent/test_local_autoflow_client.py -q
```

Expected: all selected tests pass.

- [ ] **Step 9: Commit**

```bash
git add backend/app/channel_agent/clients.py backend/app/channel_agent/service.py backend/app/channel_agent/runner.py backend/tests/channel_agent/test_service.py backend/tests/channel_agent/test_local_autoflow_client.py
git commit -m "feat: close channel ops runner flow"
```

## Task 6: API Controls, Channel Scope, Ticks, And Funnel

**Files:**
- Modify: `backend/app/api/channel_agent.py`
- Modify: `backend/app/schemas/channel_agent.py`
- Test: `backend/tests/channel_agent/test_api.py`

- [ ] **Step 1: Add failing API tests**

Extend `backend/tests/channel_agent/test_api.py` with tests equivalent to:

```python
@pytest.mark.asyncio
async def test_channel_queue_and_health_are_channel_scoped(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        first = (await client.post("/api/v1/channel-agent/channels", json={"name": "A"})).json()
        second = (await client.post("/api/v1/channel-agent/channels", json={"name": "B"})).json()
        await client.post(f"/api/v1/channel-agent/channels/{first['id']}/enqueue-tick")
        await client.post(f"/api/v1/channel-agent/channels/{second['id']}/enqueue-tick")

        first_queue = (await client.get(f"/api/v1/channel-agent/channels/{first['id']}/queue")).json()
        first_health = (await client.get(f"/api/v1/channel-agent/channels/{first['id']}/health")).json()

        assert len(first_queue) == 1
        assert first_queue[0]["payload_json"]["channel_id"] == first["id"]
        assert first_health["queued_items"] == 1
```

Add control endpoint test:

```python
@pytest.mark.asyncio
async def test_pause_resume_lane_account_and_publication_controls(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Ops"})).json()
        lane = (await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/lanes", json={"name": "Tech"})).json()
        account = (await client.post(
            f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
            json={"account_label": "main", "platform_account_id": "yt", "credential_ref": "youtube/main"},
        )).json()

        paused_account = (await client.post(f"/api/v1/channel-agent/accounts/{account['id']}/pause", json={"reason": "operator"})).json()
        resumed_account = (await client.post(f"/api/v1/channel-agent/accounts/{account['id']}/resume")).json()
        paused_lane = (await client.post(f"/api/v1/channel-agent/lanes/{lane['id']}/pause", json={"reason": "operator"})).json()
        resumed_lane = (await client.post(f"/api/v1/channel-agent/lanes/{lane['id']}/resume")).json()

        assert paused_account["enabled"] is False
        assert resumed_account["enabled"] is True
        assert paused_lane["paused_until"] is not None
        assert resumed_lane["paused_until"] is None
```

Add ticks and funnel tests:

```python
@pytest.mark.asyncio
async def test_ticks_and_funnel_return_real_data(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Metrics"})).json()
        await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/enqueue-tick")

        ticks = (await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/ticks")).json()
        funnel = (await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/metrics/funnel?days=7")).json()

        assert isinstance(ticks, list)
        assert "selected" in funnel
        assert "scheduled" in funnel
```

- [ ] **Step 2: Run red API tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_api.py -q
```

Expected: fail because endpoints/scoping/funnel are incomplete.

- [ ] **Step 3: Update enqueue and serializers**

In `enqueue_tick()`, pass:

```python
channel_profile_id=_uuid(channel_id)
```

Update `_queue()` response to include:

```python
channel_profile_id=str(row.channel_profile_id) if row.channel_profile_id else None
```

Update `_lane_format()` to include:

```python
"source_platforms_json": row.source_platforms_json,
```

- [ ] **Step 4: Scope queue and health endpoints**

In `channel_health()`:

```python
queued = (
    await db.execute(
        select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.channel_profile_id == channel.id)
    )
).scalars().all()
```

In `channel_queue()`:

```python
result = await db.execute(
    select(ChannelOpsQueueItem)
    .where(ChannelOpsQueueItem.channel_profile_id == _uuid(channel_id))
    .order_by(ChannelOpsQueueItem.created_at.desc())
)
```

In `channel_publications()`, join through `ProductionTask`:

```python
result = await db.execute(
    select(PublicationRecord)
    .join(ProductionTask, PublicationRecord.production_task_id == ProductionTask.id)
    .where(ProductionTask.channel_profile_id == _uuid(channel_id))
    .order_by(PublicationRecord.created_at.desc())
)
```

- [ ] **Step 5: Add control endpoints**

Add request model:

```python
class PauseRequest(BaseModel):
    reason: str = "operator"
    until: datetime | None = None
```

Add account endpoints:

```python
@router.post("/accounts/{account_id}/pause")
async def pause_account(account_id: str, data: PauseRequest, db: AsyncSession = Depends(get_db)):
    account = await db.get(PublishingAccount, _uuid(account_id))
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account.enabled = False
    account.paused_until = data.until
    await db.commit()
    await db.refresh(account)
    return _account(account)


@router.post("/accounts/{account_id}/resume")
async def resume_account(account_id: str, db: AsyncSession = Depends(get_db)):
    account = await db.get(PublishingAccount, _uuid(account_id))
    if account is None:
        raise HTTPException(status_code=404, detail="Account not found")
    account.enabled = True
    account.paused_until = None
    await db.commit()
    await db.refresh(account)
    return _account(account)
```

Add lane endpoints similarly:

```python
@router.post("/lanes/{lane_id}/pause")
async def pause_lane(lane_id: str, data: PauseRequest, db: AsyncSession = Depends(get_db)):
    lane = await db.get(TopicLane, _uuid(lane_id))
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found")
    lane.paused_until = data.until or datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(lane)
    return _lane(lane)


@router.post("/lanes/{lane_id}/resume")
async def resume_lane(lane_id: str, db: AsyncSession = Depends(get_db)):
    lane = await db.get(TopicLane, _uuid(lane_id))
    if lane is None:
        raise HTTPException(status_code=404, detail="Lane not found")
    lane.paused_until = None
    await db.commit()
    await db.refresh(lane)
    return _lane(lane)
```

Add publication promote/reject:

```python
@router.post("/publications/{publication_id}/promote")
async def promote_publication(publication_id: str, db: AsyncSession = Depends(get_db)):
    publication = await db.get(PublicationRecord, _uuid(publication_id))
    if publication is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    task = await db.get(ProductionTask, publication.production_task_id)
    item = await ChannelOpsQueueService().enqueue(
        db,
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:{publication.desired_privacy}:manual",
        payload={"publication_id": str(publication.id), "target_visibility": publication.desired_privacy},
        priority=10,
        channel_profile_id=task.channel_profile_id if task else None,
    )
    return _queue(item)
```

Reject:

```python
@router.post("/publications/{publication_id}/reject")
async def reject_publication(publication_id: str, db: AsyncSession = Depends(get_db)):
    publication = await db.get(PublicationRecord, _uuid(publication_id))
    if publication is None:
        raise HTTPException(status_code=404, detail="Publication not found")
    publication.publish_status = "rejected"
    task = await db.get(ProductionTask, publication.production_task_id)
    if task is not None:
        task.state = "rejected"
        task.state_updated_at = datetime.now(timezone.utc)
    await db.commit()
    await db.refresh(publication)
    return _publication(publication)
```

- [ ] **Step 6: Add ticks endpoint and real funnel**

Add ticks:

```python
@router.get("/channels/{channel_id}/ticks")
async def channel_ticks(channel_id: str, limit: int = 50, db: AsyncSession = Depends(get_db)):
    await _require_channel(db, channel_id)
    result = await db.execute(
        select(AgentTickAudit)
        .where(AgentTickAudit.channel_profile_id == _uuid(channel_id))
        .order_by(AgentTickAudit.started_at.desc())
        .limit(min(limit, 100))
    )
    return [_tick(row) for row in result.scalars().all()]
```

Add `_tick()` serializer:

```python
def _tick(row: AgentTickAudit) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "tick_id": row.tick_id,
        "dry_run": row.dry_run,
        "started_at": row.started_at,
        "finished_at": row.finished_at,
        "ideas_discovered": row.ideas_discovered,
        "candidates_scored": row.candidates_scored,
        "tasks_selected": row.tasks_selected,
        "tasks_rejected": row.tasks_rejected,
        "guards_triggered_json": row.guards_triggered_json,
        "decision_summary_json": row.decision_summary_json,
        "error_message": row.error_message,
    }
```

Replace funnel zeros with:

```python
since = datetime.now(timezone.utc) - timedelta(days=max(1, min(days, 90)))
tasks = (
    await db.execute(
        select(ProductionTask)
        .where(ProductionTask.channel_profile_id == _uuid(channel_id))
        .where(ProductionTask.created_at >= since)
    )
).scalars().all()
publications = (
    await db.execute(
        select(PublicationRecord)
        .join(ProductionTask, PublicationRecord.production_task_id == ProductionTask.id)
        .where(ProductionTask.channel_profile_id == _uuid(channel_id))
        .where(PublicationRecord.created_at >= since)
    )
).scalars().all()
return {
    "days": days,
    "seeded": sum(1 for task in tasks if task.source in {"manual_seed", "lane_seed"}),
    "selected": sum(1 for task in tasks if task.state == "selected"),
    "planning": sum(1 for task in tasks if task.state == "planning"),
    "producing": sum(1 for task in tasks if task.state == "producing"),
    "uploaded_private": sum(1 for task in tasks if task.state == "uploaded_private"),
    "scheduled": sum(1 for publication in publications if publication.publish_status == "scheduled"),
    "published": sum(1 for publication in publications if publication.publish_status == "published"),
    "measured": sum(1 for task in tasks if task.state == "measured"),
    "failed": sum(1 for task in tasks if task.state == "failed"),
    "held": sum(1 for task in tasks if task.state == "held"),
}
```

- [ ] **Step 7: Run green API tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_api.py -q
```

Expected: API tests pass.

- [ ] **Step 8: Commit**

```bash
git add backend/app/api/channel_agent.py backend/app/schemas/channel_agent.py backend/tests/channel_agent/test_api.py
git commit -m "feat: add channel ops operator controls"
```

## Task 7: Integration Verification And Cleanup

**Files:**
- Modify: `docs/superpowers/plans/2026-05-18-channel-ops-alpha-hardening.md`

- [ ] **Step 1: Run focused backend tests**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent tests/autoflow/test_autoflow_api.py -q
```

Expected: pass.

- [ ] **Step 2: Run full backend pytest**

Run:

```bash
cd backend
python3 -m pytest
```

Expected: pass.

- [ ] **Step 3: Run backend non-blocking static checks**

Run:

```bash
cd backend
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: command may report missing `ruff` or `mypy` in the local environment; record the output in the final implementation summary.

- [ ] **Step 4: Run frontend required checks**

Run:

```bash
cd frontend
npm install
npm run build
npm run lint || true
```

Expected: `npm run build` passes. Existing Vite/lightningcss warnings are acceptable if unchanged.

- [ ] **Step 5: Validate Compose config**

Run:

```bash
docker compose config >/tmp/vp-compose-config.out
tail -n 20 /tmp/vp-compose-config.out
```

Expected: command exits 0.

- [ ] **Step 6: Update plan checkboxes**

Mark completed items in this plan as `- [x]` as each task is actually completed. Do not mark a task complete before its verification commands have passed.

- [ ] **Step 7: Commit plan checkbox updates**

```bash
git add docs/superpowers/plans/2026-05-18-channel-ops-alpha-hardening.md
git commit -m "docs: track channel ops hardening execution"
```

## Execution Order

Execute tasks in this order:

1. Task 1: Schema and queue hardening.
2. Task 2: AutoFlow request configuration and safe privacy.
3. Task 3: Lane-driven candidates and dry-run decisions.
4. Task 4: Guards.
5. Task 5: Runner closure, AutoFlow observation, and metrics.
6. Task 6: API controls, channel scope, ticks, and funnel.
7. Task 7: Integration verification and cleanup.

This order keeps pure local work before external/client glue, then finishes with API and full verification.
