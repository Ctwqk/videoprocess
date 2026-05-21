# ChannelOps Phase B/C/D Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement ChannelOps Phase B/C/D: candidate audit, failure categories, DiscoverySignal, and read-only feedback learning.

**Architecture:** Python owns SQLAlchemy models, Alembic migrations, FastAPI endpoints, and the existing YouTube trend ingester. Go owns live runtime behavior: candidate audit writes, failure-category updates, discovery-to-candidate conversion, staged metrics, reward calculation, and LearningState aggregation. Phase D is observable only; it must not change tick selection or publishing behavior.

**Tech Stack:** FastAPI, SQLAlchemy async, Alembic, pytest, Go 1.25, pgx, existing `internal/channelops` runner/store/tests.

---

## File Structure

- Modify `backend/app/models/channel_agent.py`: add `DecisionAuditEntry`, `DiscoverySignal`, `LearningState`; extend `ProductionTask` and `FeedbackSnapshot`.
- Modify `backend/app/schemas/channel_agent.py`: add read schemas for decision audit and learning responses.
- Modify `backend/app/api/channel_agent.py`: add decisions, task audit, failures, and learning APIs.
- Modify `backend/app/channel_agent/trend_ingesters/youtube_search.py`: write `DiscoverySignal` instead of active `ManualSeed`.
- Add `backend/alembic/versions/020_channelops_decision_audit_failure_category.py`.
- Add `backend/alembic/versions/021_channelops_discovery_signals.py`.
- Add `backend/alembic/versions/022_channelops_feedback_learning.py`.
- Modify `backend/tests/channel_agent/test_api.py`: add API coverage for B/D endpoints.
- Modify `backend/tests/channel_agent/test_trend_ingester.py`: change trend expectation from `ManualSeed` to `DiscoverySignal`.
- Add `backend/tests/channel_agent/test_models_bcd.py`: model/migration-style table creation checks.
- Modify `internal/channelops/types.go`: add row structs, failure-category constants, discovery fields on tasks/candidates.
- Modify `internal/channelops/store_tick.go`: load discovery signals, write decision audit rows, set `discovery_signal_id`.
- Modify `internal/channelops/tick.go`: generate `trend_youtube` candidates from `DiscoverySignal`.
- Modify `internal/channelops/store_tasks.go`: read/write `failure_category`, `discovery_signal_id`, mark discovery signals converted.
- Modify `internal/channelops/handlers.go`: set failure categories on known failure paths; pass metrics stage to snapshot upsert.
- Modify `internal/channelops/metrics.go`: add stage detection, reward calculation, and completeness-aware helpers.
- Add `internal/channelops/learning.go`: LearningState aggregation helpers.
- Modify `internal/channelops/store_publications.go`: staged snapshot upsert and learning-state persistence.
- Modify or add Go tests in `internal/channelops/*_test.go`.

## Task 1: Python Models and Alembic Migrations

**Files:**
- Modify: `backend/app/models/channel_agent.py`
- Add: `backend/alembic/versions/020_channelops_decision_audit_failure_category.py`
- Add: `backend/alembic/versions/021_channelops_discovery_signals.py`
- Add: `backend/alembic/versions/022_channelops_feedback_learning.py`
- Add: `backend/tests/channel_agent/test_models_bcd.py`
- Modify: `backend/tests/channel_agent/test_api.py`

- [ ] **Step 1: Write failing model table test**

Create `backend/tests/channel_agent/test_models_bcd.py`:

```python
from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from app.models.channel_agent import (
    AgentTickAudit,
    ChannelProfile,
    DecisionAuditEntry,
    DiscoverySignal,
    FeedbackSnapshot,
    LearningState,
    ManualSeed,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)


async def _create_tables(*tables):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in tables:
            await conn.run_sync(table.create)
    await engine.dispose()


@pytest.mark.asyncio
async def test_bcd_models_create_in_sqlite():
    await _create_tables(
        ChannelProfile.__table__,
        TopicLane.__table__,
        PublishingAccount.__table__,
        AgentTickAudit.__table__,
        ManualSeed.__table__,
        ProductionTask.__table__,
        PublicationRecord.__table__,
        FeedbackSnapshot.__table__,
        DecisionAuditEntry.__table__,
        DiscoverySignal.__table__,
        LearningState.__table__,
    )
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_models_bcd.py -q
```

Expected: import failure for `DecisionAuditEntry`, `DiscoverySignal`, or `LearningState`.

- [ ] **Step 3: Extend SQLAlchemy models**

In `backend/app/models/channel_agent.py`, add imports:

```python
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func
```

Add fields to `ProductionTask`:

```python
    discovery_signal_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    failure_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
```

Add fields to `FeedbackSnapshot`:

```python
    snapshot_stage: Mapped[str] = mapped_column(String(16), default="24h", nullable=False)
    reward_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    reward_components_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
```

Add models:

```python
class DecisionAuditEntry(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "decision_audit_entries"
    __table_args__ = (
        Index("ix_decision_audit_entries_tick", "tick_audit_id"),
        Index("ix_decision_audit_entries_channel_created", "channel_profile_id", "created_at"),
        Index("ix_decision_audit_entries_task", "created_task_id"),
        Index("ix_decision_audit_entries_source_created", "candidate_source", "created_at"),
    )

    tick_audit_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("agent_tick_audits.id", ondelete="CASCADE"), nullable=False
    )
    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    candidate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    candidate_source: Mapped[str] = mapped_column(String(64), nullable=False)
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    lane_format_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    target_account_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    score_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    guard_results_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    pds_decision_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    learning_context_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    selected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    rejection_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_task_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DiscoverySignal(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "discovery_signals"
    __table_args__ = (
        UniqueConstraint("channel_profile_id", "source", "source_external_id", name="uq_discovery_signal_channel_source_external"),
        Index("ix_discovery_signals_channel_lane_observed", "channel_profile_id", "topic_lane_id", "observed_at"),
        Index("ix_discovery_signals_channel_status_expires", "channel_profile_id", "status", "expires_at"),
    )

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(Text, default="", nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="", nullable=False)
    keywords_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trend_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    novelty_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    raw_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
    converted_task_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class LearningState(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "learning_states"
    __table_args__ = (
        UniqueConstraint(
            "channel_profile_id",
            "dimension_type",
            "dimension_key",
            "window_days",
            name="uq_learning_state_channel_dimension_window",
        ),
        Index("ix_learning_states_channel_dimension", "channel_profile_id", "dimension_type"),
    )

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    dimension_type: Mapped[str] = mapped_column(String(64), nullable=False)
    dimension_key: Mapped[str] = mapped_column(String(255), nullable=False)
    window_days: Mapped[int] = mapped_column(Integer, nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    avg_reward: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    recommendation_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    last_computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
```

- [ ] **Step 4: Add Alembic revision 020**

Create `backend/alembic/versions/020_channelops_decision_audit_failure_category.py` with revision `020_channelops_decision_audit_failure_category` and down revision `019_channelops_go_live_phase0`. It must create `decision_audit_entries`, indexes, and `production_tasks.failure_category`.

- [ ] **Step 5: Add Alembic revision 021**

Create `backend/alembic/versions/021_channelops_discovery_signals.py` with revision `021_channelops_discovery_signals` and down revision `020_channelops_decision_audit_failure_category`. It must create `discovery_signals`, add `production_tasks.discovery_signal_id`, and migrate active `manual_seeds.source_policy = 'trend_youtube'` rows into `discovery_signals`.

- [ ] **Step 6: Add Alembic revision 022**

Create `backend/alembic/versions/022_channelops_feedback_learning.py` with revision `022_channelops_feedback_learning` and down revision `021_channelops_discovery_signals`. It must add `feedback_snapshots.snapshot_stage`, `reward_score`, `reward_components_json`, a unique index for `(publication_id, snapshot_stage)`, and create `learning_states`.

- [ ] **Step 7: Run model test**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_models_bcd.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit Task 1**

```bash
git add backend/app/models/channel_agent.py backend/alembic/versions/020_channelops_decision_audit_failure_category.py backend/alembic/versions/021_channelops_discovery_signals.py backend/alembic/versions/022_channelops_feedback_learning.py backend/tests/channel_agent/test_models_bcd.py
git commit -m "feat: add channelops bcd data model"
```

## Task 2: FastAPI Phase B/D APIs

**Files:**
- Modify: `backend/app/api/channel_agent.py`
- Modify: `backend/app/schemas/channel_agent.py`
- Modify: `backend/tests/channel_agent/test_api.py`

- [ ] **Step 1: Extend API test table list**

In `backend/tests/channel_agent/test_api.py`, import `DecisionAuditEntry`, `DiscoverySignal`, and `LearningState`, then include their tables in `CHANNEL_AGENT_TABLES` after dependencies:

```python
    DecisionAuditEntry.__table__,
    DiscoverySignal.__table__,
    LearningState.__table__,
```

- [ ] **Step 2: Add failing decisions/failures/learning API test**

Append to `backend/tests/channel_agent/test_api.py`:

```python
@pytest.mark.asyncio
async def test_decisions_failures_task_audit_and_learning_api(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Audit"})).json()
        account = (
            await client.post(
                f"/api/v1/channel-agent/channels/{channel['id']}/accounts",
                json={"account_label": "main", "platform_account_id": "yt", "credential_ref": "youtube/main"},
            )
        ).json()
        task = ProductionTask(
            channel_profile_id=uuid.UUID(channel["id"]),
            target_account_id=uuid.UUID(account["id"]),
            source="manual_seed",
            prompt="audit task",
            title_seed="audit",
            state="failed",
            failure_reason="quota exhausted",
            failure_category="quota",
            channel_config_snapshot_json={},
        )
        api_session.add(task)
        await api_session.flush()
        tick = AgentTickAudit(channel_profile_id=uuid.UUID(channel["id"]), tick_id="tick:audit", dry_run=False)
        api_session.add(tick)
        await api_session.flush()
        decision = DecisionAuditEntry(
            tick_audit_id=tick.id,
            channel_profile_id=uuid.UUID(channel["id"]),
            candidate_id="manual_seed:example",
            candidate_source="manual_seed",
            target_account_id=uuid.UUID(account["id"]),
            selected=True,
            created_task_id=task.id,
            guard_results_json=[{"guard": "account_concurrency", "verdict": "allow", "reason": "idle"}],
        )
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=uuid.UUID(account["id"]),
            platform_content_id="yt-audit",
            title="audit",
            compliance_disposition="assumed_fair_use",
        )
        state = LearningState(
            channel_profile_id=uuid.UUID(channel["id"]),
            dimension_type="source",
            dimension_key="manual_seed",
            window_days=7,
            sample_count=12,
            avg_reward=0.42,
            confidence=0.6,
            recommendation_json={"action": "observe"},
        )
        api_session.add_all([decision, publication, state])
        await api_session.commit()

        decisions = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/decisions")
        assert decisions.status_code == 200
        assert decisions.json()[0]["candidate_id"] == "manual_seed:example"

        failures = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/failures?days=7")
        assert failures.status_code == 200
        assert failures.json()["categories"]["quota"] == 1

        audit = await client.get(f"/api/v1/channel-agent/tasks/{task.id}/audit")
        assert audit.status_code == 200
        assert audit.json()["task"]["failure_category"] == "quota"
        assert audit.json()["decision"]["candidate_id"] == "manual_seed:example"

        learning = await client.get(f"/api/v1/channel-agent/channels/{channel['id']}/learning")
        assert learning.status_code == 200
        assert learning.json()["states"][0]["dimension_type"] == "source"
```

- [ ] **Step 3: Run test and verify it fails**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_api.py::test_decisions_failures_task_audit_and_learning_api -q
```

Expected: FAIL with missing routes or imports.

- [ ] **Step 4: Add schemas**

In `backend/app/schemas/channel_agent.py`, add:

```python
class DecisionAuditEntryRead(BaseModel):
    id: str
    tick_audit_id: str
    channel_profile_id: str
    candidate_id: str
    candidate_source: str
    topic_lane_id: str | None = None
    lane_format_id: str | None = None
    target_account_id: str | None = None
    score_json: dict[str, Any]
    guard_results_json: list[Any]
    pds_decision_json: dict[str, Any]
    learning_context_json: dict[str, Any]
    selected: bool
    rejection_reason: str | None = None
    created_task_id: str | None = None
    created_at: datetime


class LearningStateRead(BaseModel):
    id: str
    channel_profile_id: str
    dimension_type: str
    dimension_key: str
    window_days: int
    sample_count: int
    avg_reward: float
    confidence: float
    recommendation_json: dict[str, Any]
    last_computed_at: datetime
```

- [ ] **Step 5: Add serializers**

In `backend/app/api/channel_agent.py`, add helpers:

```python
def _decision(row: DecisionAuditEntry) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "tick_audit_id": str(row.tick_audit_id),
        "channel_profile_id": str(row.channel_profile_id),
        "candidate_id": row.candidate_id,
        "candidate_source": row.candidate_source,
        "topic_lane_id": str(row.topic_lane_id) if row.topic_lane_id else None,
        "lane_format_id": str(row.lane_format_id) if row.lane_format_id else None,
        "target_account_id": str(row.target_account_id) if row.target_account_id else None,
        "score_json": row.score_json or {},
        "guard_results_json": row.guard_results_json or [],
        "pds_decision_json": row.pds_decision_json or {},
        "learning_context_json": row.learning_context_json or {},
        "selected": row.selected,
        "rejection_reason": row.rejection_reason,
        "created_task_id": str(row.created_task_id) if row.created_task_id else None,
        "created_at": row.created_at,
    }


def _learning_state(row: LearningState) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "channel_profile_id": str(row.channel_profile_id),
        "dimension_type": row.dimension_type,
        "dimension_key": row.dimension_key,
        "window_days": row.window_days,
        "sample_count": row.sample_count,
        "avg_reward": row.avg_reward,
        "confidence": row.confidence,
        "recommendation_json": row.recommendation_json or {},
        "last_computed_at": row.last_computed_at,
    }
```

Also include `failure_category` and `discovery_signal_id` in `_task(row)`.

- [ ] **Step 6: Add API endpoints**

In `backend/app/api/channel_agent.py`, add routes before helper functions:

```python
@router.get("/channels/{channel_id}/decisions")
async def channel_decisions(
    channel_id: str,
    tick_audit_id: str | None = None,
    candidate_source: str | None = None,
    selected: bool | None = None,
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
):
    channel = await _require_channel(db, channel_id)
    query = select(DecisionAuditEntry).where(DecisionAuditEntry.channel_profile_id == channel.id)
    if tick_audit_id:
        query = query.where(DecisionAuditEntry.tick_audit_id == _uuid(tick_audit_id))
    if candidate_source:
        query = query.where(DecisionAuditEntry.candidate_source == candidate_source)
    if selected is not None:
        query = query.where(DecisionAuditEntry.selected.is_(selected))
    rows = (
        await db.execute(
            query.order_by(DecisionAuditEntry.created_at.desc()).offset(max(offset, 0)).limit(min(max(limit, 1), 500))
        )
    ).scalars().all()
    return [_decision(row) for row in rows]


@router.get("/tasks/{task_id}/audit")
async def task_audit(task_id: str, db: AsyncSession = Depends(get_db)):
    task = await db.get(ProductionTask, _uuid(task_id))
    if task is None:
        raise HTTPException(status_code=404, detail="Production task not found")
    decision = (
        await db.execute(select(DecisionAuditEntry).where(DecisionAuditEntry.created_task_id == task.id).limit(1))
    ).scalars().first()
    publication = (
        await db.execute(select(PublicationRecord).where(PublicationRecord.production_task_id == task.id).limit(1))
    ).scalars().first()
    material_rows = []
    if publication is not None:
        material_rows = (
            await db.execute(select(MaterialUsageLedger).where(MaterialUsageLedger.publication_id == publication.id))
        ).scalars().all()
    return {
        "task": _task(task),
        "decision": _decision(decision) if decision else None,
        "publication": _publication(publication) if publication else None,
        "material_usage": [
            {
                "id": str(row.id),
                "material_id": row.material_id,
                "asset_id": str(row.asset_id) if row.asset_id else None,
                "segment_signature": row.segment_signature,
                "metadata_json": row.metadata_json or {},
            }
            for row in material_rows
        ],
    }


@router.get("/channels/{channel_id}/failures")
async def channel_failures(channel_id: str, days: int = 7, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    since = _naive_utc(Clock().now() - timedelta(days=max(days, 0)))
    rows = (
        await db.execute(
            select(ProductionTask.failure_category, func.count(ProductionTask.id))
            .where(ProductionTask.channel_profile_id == channel.id)
            .where(ProductionTask.created_at >= since)
            .where(ProductionTask.failure_category.is_not(None))
            .group_by(ProductionTask.failure_category)
        )
    ).all()
    return {"days": max(days, 0), "categories": {str(category): int(count) for category, count in rows}}


@router.get("/channels/{channel_id}/learning")
async def channel_learning(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    rows = (
        await db.execute(
            select(LearningState)
            .where(LearningState.channel_profile_id == channel.id)
            .order_by(LearningState.dimension_type.asc(), LearningState.dimension_key.asc(), LearningState.window_days.asc())
        )
    ).scalars().all()
    return {"channel_id": str(channel.id), "states": [_learning_state(row) for row in rows]}
```

- [ ] **Step 7: Run targeted API test**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_api.py::test_decisions_failures_task_audit_and_learning_api -q
```

Expected: PASS.

- [ ] **Step 8: Commit Task 2**

```bash
git add backend/app/api/channel_agent.py backend/app/schemas/channel_agent.py backend/tests/channel_agent/test_api.py
git commit -m "feat: expose channelops audit and learning APIs"
```

## Task 3: Go Types and Failure Categories

**Files:**
- Modify: `internal/channelops/types.go`
- Modify: `internal/channelops/store_tasks.go`
- Modify: `internal/channelops/handlers.go`
- Add: `internal/channelops/failures_test.go`

- [ ] **Step 1: Write failing category classifier test**

Create `internal/channelops/failures_test.go`:

```go
package channelops

import "testing"

func TestFailureCategoryForContextPrefersHandlerContext(t *testing.T) {
	tests := []struct {
		context string
		reason  string
		want    string
	}{
		{context: "plan_task", reason: "planner rejected schema", want: FailurePlanning},
		{context: "execute_task", reason: "render worker failed", want: FailureRender},
		{context: "publish_task", reason: "youtube upload quota exhausted", want: FailureQuota},
		{context: "collect_metrics", reason: "analytics unavailable", want: FailureMetrics},
		{context: "observe_job", reason: "video_id missing", want: FailureUpload},
	}
	for _, tt := range tests {
		if got := FailureCategoryFor(tt.context, tt.reason); got != tt.want {
			t.Fatalf("FailureCategoryFor(%q, %q)=%q want %q", tt.context, tt.reason, got, tt.want)
		}
	}
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
go test ./internal/channelops -run TestFailureCategoryForContextPrefersHandlerContext -count=1
```

Expected: FAIL with undefined `FailureCategoryFor`.

- [ ] **Step 3: Add constants and row fields**

In `internal/channelops/types.go`, add:

```go
const (
	FailureAuth          = "auth"
	FailureQuota         = "quota"
	FailureUpload        = "upload"
	FailureRender        = "render"
	FailurePlanning      = "planning"
	FailureValidation    = "validation"
	FailurePDS           = "pds"
	FailureYouTubeStatus = "youtube_status"
	FailureMetrics       = "metrics"
	FailureDiscovery     = "discovery"
	FailureLearning      = "learning"
	FailureOther         = "other"
)
```

Extend `ProductionTaskRow`:

```go
	DiscoverySignalID *string
	FailureCategory  *string
```

- [ ] **Step 4: Add classifier**

Add to `internal/channelops/types.go` or a new `internal/channelops/failures.go`:

```go
func FailureCategoryFor(context string, reason string) string {
	lower := strings.ToLower(context + " " + reason)
	switch {
	case strings.Contains(lower, "oauth") || strings.Contains(lower, "token") || strings.Contains(lower, "credential"):
		return FailureAuth
	case strings.Contains(lower, "quota"):
		return FailureQuota
	case strings.Contains(lower, "upload") || strings.Contains(lower, "youtube") || strings.Contains(lower, "video_id") || strings.Contains(lower, "thumbnail"):
		return FailureUpload
	case strings.Contains(lower, "publish_status") || strings.Contains(lower, "takedown") || strings.Contains(lower, "rejected"):
		return FailureYouTubeStatus
	case strings.Contains(context, "collect_metrics") || strings.Contains(lower, "metrics") || strings.Contains(lower, "analytics"):
		return FailureMetrics
	case strings.Contains(context, "plan_task") || strings.Contains(lower, "plan"):
		return FailurePlanning
	case strings.Contains(context, "execute_task") || strings.Contains(context, "observe_job") || strings.Contains(lower, "render") || strings.Contains(lower, "autoflow"):
		return FailureRender
	case strings.Contains(lower, "validation") || strings.Contains(lower, "invalid"):
		return FailureValidation
	case strings.Contains(lower, "pds"):
		return FailurePDS
	default:
		return FailureOther
	}
}
```

Add `import "strings"` if this lives in a new file.

- [ ] **Step 5: Update task reads and writes**

In `internal/channelops/store_tasks.go`, include `discovery_signal_id` and `failure_category` in `GetProductionTask` SELECT/Scan. Update `FailTask` to set `failure_category = $6::text` using `FailureCategoryFor(transitionReason, reason)`.

Also update `holdTask` to set `failure_category = CASE WHEN $10::text = '' THEN failure_category ELSE $10::text END` with `FailurePDS` for PDS holds and `FailureValidation` for validation holds.

- [ ] **Step 6: Run category test**

Run:

```bash
go test ./internal/channelops -run TestFailureCategoryForContextPrefersHandlerContext -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit Task 3**

```bash
git add internal/channelops/types.go internal/channelops/failures.go internal/channelops/store_tasks.go internal/channelops/handlers.go internal/channelops/failures_test.go
git commit -m "feat: classify channelops task failures"
```

## Task 4: Go Decision Audit Writes

**Files:**
- Modify: `internal/channelops/types.go`
- Modify: `internal/channelops/store_tick.go`
- Modify: `internal/channelops/store_tick_test.go`
- Modify: `internal/channelops/tick_test.go`

- [ ] **Step 1: Add failing source inspection test**

Append to `internal/channelops/store_tick_test.go`:

```go
func TestStoreTickWritesDecisionAuditEntries(t *testing.T) {
	source, err := os.ReadFile("store_tick.go")
	if err != nil {
		t.Fatalf("read store_tick.go: %v", err)
	}
	text := string(source)
	for _, want := range []string{"INSERT INTO decision_audit_entries", "created_task_id", "learning_context_json"} {
		if !strings.Contains(text, want) {
			t.Fatalf("store_tick.go missing %q", want)
		}
	}
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
go test ./internal/channelops -run TestStoreTickWritesDecisionAuditEntries -count=1
```

Expected: FAIL because `store_tick.go` does not insert decision audit rows.

- [ ] **Step 3: Add candidate audit helpers**

In `internal/channelops/types.go`, extend `TickCandidate`:

```go
	ScoreJSON           map[string]any
	GuardResultsJSON    []map[string]any
	LearningContextJSON map[string]any
	DiscoverySignal     *DiscoverySignalRow
```

In `internal/channelops/tick.go`, populate `GuardResultsJSON` when a candidate is rejected:

```go
candidate.GuardResultsJSON = []map[string]any{{
	"guard": candidate.RejectionGuard,
	"verdict": "reject",
	"reason": candidate.RejectionReason,
}}
```

- [ ] **Step 4: Insert audit rows before tasks**

In `internal/channelops/store_tick.go`, add:

```go
func (s *Store) InsertDecisionAuditEntries(ctx context.Context, tickAuditID string, channelID string, result TickResult) (map[string]string, error) {
	ids := map[string]string{}
	all := append([]TickCandidate{}, result.Accepted...)
	all = append(all, result.Rejected...)
	for _, candidate := range all {
		scoreJSON, err := json.Marshal(jsonObject(candidate.ScoreJSON))
		if err != nil {
			return nil, err
		}
		guardsJSON, err := json.Marshal(candidate.GuardResultsJSON)
		if err != nil {
			return nil, err
		}
		learningJSON, err := json.Marshal(jsonObject(candidate.LearningContextJSON))
		if err != nil {
			return nil, err
		}
		pdsJSON := []byte("{}")
		var id string
		err = s.Pool.QueryRow(ctx, `
			INSERT INTO decision_audit_entries (
				id, tick_audit_id, channel_profile_id, candidate_id, candidate_source,
				topic_lane_id, lane_format_id, target_account_id, score_json, guard_results_json,
				pds_decision_json, learning_context_json, selected, rejection_reason, created_at
			)
			VALUES (
				gen_random_uuid(), $1::uuid, $2::uuid, $3, $4, $5::uuid, $6::uuid, $7::uuid,
				$8::json, $9::json, $10::json, $11::json, $12, $13, $14::timestamptz
			)
			RETURNING id
		`, tickAuditID, channelID, candidate.CandidateID, candidate.SourceKind, candidateLaneID(candidate),
			candidateFormatID(candidate), candidateAccountUUID(candidate), scoreJSON, guardsJSON, pdsJSON,
			learningJSON, !candidate.Rejected, candidate.RejectionReason, s.Now().UTC()).Scan(&id)
		if err != nil {
			return nil, err
		}
		ids[candidate.CandidateID] = id
	}
	return ids, nil
}
```

Add helper:

```go
func candidateAccountUUID(candidate TickCandidate) *string {
	if candidate.Account == nil {
		return nil
	}
	value := candidate.Account.ID
	return &value
}
```

- [ ] **Step 5: Backfill created_task_id**

Add:

```go
func (s *Store) AttachDecisionAuditTask(ctx context.Context, auditID string, taskID string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE decision_audit_entries
		SET created_task_id = $2::uuid
		WHERE id = $1::uuid
	`, auditID, taskID)
	return err
}
```

In `RunTick`, store the ID returned by `InsertTickAudit`, call `InsertDecisionAuditEntries`, then after each accepted candidate creates a task call `AttachDecisionAuditTask`.

- [ ] **Step 6: Run audit inspection test**

Run:

```bash
go test ./internal/channelops -run TestStoreTickWritesDecisionAuditEntries -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit Task 4**

```bash
git add internal/channelops/types.go internal/channelops/tick.go internal/channelops/store_tick.go internal/channelops/store_tick_test.go internal/channelops/tick_test.go
git commit -m "feat: write channelops decision audit entries"
```

## Task 5: DiscoverySignal Ingestion and Candidates

**Files:**
- Modify: `backend/app/channel_agent/trend_ingesters/youtube_search.py`
- Modify: `backend/tests/channel_agent/test_trend_ingester.py`
- Modify: `internal/channelops/types.go`
- Modify: `internal/channelops/store_tick.go`
- Modify: `internal/channelops/tick.go`
- Add: `internal/channelops/discovery_test.go`

- [ ] **Step 1: Change Python ingester test to expect DiscoverySignal**

Modify `backend/tests/channel_agent/test_trend_ingester.py` imports and fixture table creation:

```python
from app.models.channel_agent import ChannelProfile, DiscoverySignal, ManualSeed, TopicLane
```

Create `DiscoverySignal.__table__` in the fixture. Replace the final query/assertion with:

```python
signals = (await trend_session.execute(select(DiscoverySignal).order_by(DiscoverySignal.created_at.asc()))).scalars().all()
active_signals = [signal for signal in signals if signal.status == "active"]
assert result.created_count == 1
assert result.expired_count == 1
assert stale.status == "expired"
assert len(active_signals) == 1
assert active_signals[0].title == "New hot AI clip"
assert active_signals[0].source_external_id == "new-hot"
assert active_signals[0].raw_json["view_count"] == 2500
```

- [ ] **Step 2: Run ingester test and verify it fails**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_trend_ingester.py -q
```

Expected: FAIL because the ingester still writes `ManualSeed`.

- [ ] **Step 3: Update Python ingester**

In `backend/app/channel_agent/trend_ingesters/youtube_search.py` import `DiscoverySignal`. Replace `_existing_seed` with `_existing_signal`, and replace the `db.add(ManualSeed(...))` block with:

```python
db.add(
    DiscoverySignal(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        source="youtube_search",
        source_url=str(result.get("url") or ""),
        source_external_id=str(result.get("video_id") or ""),
        title=str(result.get("title") or "YouTube trend"),
        summary=str(result.get("description") or ""),
        keywords_json=list(lane.keywords_json or []),
        observed_at=current,
        expires_at=current + self.seed_ttl,
        trend_score=float(_view_count(result)),
        novelty_score=0.0,
        raw_json=dict(result),
        status="active",
    )
)
```

Update `_expire_stale` to expire `DiscoverySignal` rows where `source == "youtube_search"` and `status == "active"`.

- [ ] **Step 4: Run ingester test**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_trend_ingester.py -q
```

Expected: PASS.

- [ ] **Step 5: Add Go discovery candidate test**

Create `internal/channelops/discovery_test.go`:

```go
package channelops

import "testing"

func TestBuildTickCandidatesIncludesDiscoverySignals(t *testing.T) {
	channel := ChannelProfileRow{ID: "channel", DefaultAspectRatio: "9:16"}
	lane := TopicLaneRow{ID: "lane", Name: "AI", Enabled: true, MaxPostsPerDay: 3}
	format := LaneFormatRow{ID: "format", TopicLaneID: "lane", FormatKey: "shorts", Enabled: true, SourcePlatformsJSON: []string{"youtube"}}
	account := PublishingAccountRow{ID: "account", Enabled: true}
	signal := DiscoverySignalRow{ID: "signal", ChannelProfileID: "channel", TopicLaneID: ptrString("lane"), Source: "youtube_search", SourceExternalID: "yt-1", Title: "Trend"}

	candidates := BuildTickCandidates(channel, []TopicLaneRow{lane}, []PublishingAccountRow{account}, nil, []DiscoverySignalRow{signal}, map[string][]LaneFormatRow{"lane": []LaneFormatRow{format}}, "bucket")
	found := false
	for _, candidate := range candidates {
		if candidate.SourceKind == SourceTrendYT {
			found = true
			if candidate.ManualMaterialOverride {
				t.Fatalf("trend discovery candidate received manual override")
			}
			if candidate.DiscoverySignal == nil || candidate.DiscoverySignal.ID != "signal" {
				t.Fatalf("missing discovery signal on candidate")
			}
		}
	}
	if !found {
		t.Fatalf("expected trend_youtube candidate")
	}
}
```

Add `ptrString` helper in the test file:

```go
func ptrString(value string) *string { return &value }
```

- [ ] **Step 6: Run Go discovery test and verify it fails**

Run:

```bash
go test ./internal/channelops -run TestBuildTickCandidatesIncludesDiscoverySignals -count=1
```

Expected: FAIL because `DiscoverySignalRow` and the new BuildTickCandidates signature do not exist.

- [ ] **Step 7: Add Go discovery types and loader**

In `internal/channelops/types.go`, add:

```go
type DiscoverySignalRow struct {
	ID               string
	ChannelProfileID string
	TopicLaneID      *string
	Source           string
	SourceURL        *string
	SourceExternalID string
	Title            string
	Summary          string
	KeywordsJSON     []string
	TrendScore       float64
	NoveltyScore     float64
	RawJSON          map[string]any
	Status           string
	ExpiresAt        *time.Time
	ObservedAt       time.Time
}
```

In `internal/channelops/store_tick.go`, add `ListActiveDiscoverySignals(ctx, channelID, now)` selecting active, unexpired rows and update `LoadTickInputs` to return signals.

- [ ] **Step 8: Add discovery candidates**

Update `BuildTickCandidates` signature to accept `signals []DiscoverySignalRow`. After manual seeds and before lane-driven candidates, create candidates with:

```go
candidate := TickCandidate{
	CandidateID:         candidateID(SourceTrendYT, lane.ID, format.ID, bucket, signal.ID),
	Source:              SourceTrendYT,
	SourceKind:          SourceTrendYT,
	DiscoverySignal:     &signalCopy,
	Lane:                &laneCopy,
	LaneFormat:          &formatCopy,
	Account:             account,
	Prompt:              DiscoveryPrompt(channel, lane, format, signal),
	TitleSeed:           signal.Title,
	SourcePlatformsJSON: stringSlice(format.SourcePlatformsJSON),
	ManualMaterialOverride: false,
}
```

Add `DiscoveryPrompt` next to `LanePrompt`.

- [ ] **Step 9: Mark discovery converted**

In `InsertProductionTask`, include `discovery_signal_id` in the insert. After task creation, call:

```go
func (s *Store) MarkDiscoverySignalConverted(ctx context.Context, signalID string, taskID string) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE discovery_signals
		SET status = 'converted', converted_task_id = $2::uuid, updated_at = $3::timestamp
		WHERE id = $1::uuid
	`, signalID, taskID, s.Now().UTC())
	return err
}
```

Call it only when `candidate.DiscoverySignal != nil`.

- [ ] **Step 10: Run discovery tests**

Run:

```bash
go test ./internal/channelops -run 'TestBuildTickCandidatesIncludesDiscoverySignals|TestManualTrendSeedDoesNotReceiveManualMaterialOverride' -count=1
cd backend && python3 -m pytest tests/channel_agent/test_trend_ingester.py -q
```

Expected: PASS.

- [ ] **Step 11: Commit Task 5**

```bash
git add backend/app/channel_agent/trend_ingesters/youtube_search.py backend/tests/channel_agent/test_trend_ingester.py internal/channelops/types.go internal/channelops/store_tick.go internal/channelops/tick.go internal/channelops/discovery_test.go
git commit -m "feat: separate channelops discovery signals"
```

## Task 6: Staged Metrics and Read-Only Learning

**Files:**
- Modify: `internal/channelops/metrics.go`
- Add: `internal/channelops/learning.go`
- Modify: `internal/channelops/store_publications.go`
- Modify: `internal/channelops/handlers.go`
- Add: `internal/channelops/learning_test.go`

- [ ] **Step 1: Add failing metrics reward test**

Create `internal/channelops/learning_test.go`:

```go
package channelops

import "testing"

func TestRewardScoreRenormalizesAvailableComponents(t *testing.T) {
	metrics := map[string]any{
		"views": 1000,
		"likes": 50,
		"comments": 10,
		"avg_view_duration_sec": 18.0,
	}
	score, components := RewardScore(metrics, PublicationRewardContext{ChannelMedianViews: 500, StablePublication: true})
	if score <= 0 {
		t.Fatalf("expected positive reward score")
	}
	if components["views"] == nil || components["engagement_rate"] == nil {
		t.Fatalf("expected reward components to include views and engagement_rate: %#v", components)
	}
}

func TestSnapshotStageFromPayload(t *testing.T) {
	if got := SnapshotStageFromPayload(map[string]any{"snapshot_stage": "6h"}); got != "6h" {
		t.Fatalf("stage=%q", got)
	}
	if got := SnapshotStageFromPayload(map[string]any{}); got != "24h" {
		t.Fatalf("default stage=%q", got)
	}
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
go test ./internal/channelops -run 'TestRewardScoreRenormalizesAvailableComponents|TestSnapshotStageFromPayload' -count=1
```

Expected: FAIL with undefined reward helpers.

- [ ] **Step 3: Implement metrics stage and reward helpers**

In `internal/channelops/metrics.go`, add:

```go
func SnapshotStageFromPayload(payload map[string]any) string {
	stage := strings.TrimSpace(firstString(payload, "snapshot_stage", "stage", "window"))
	switch stage {
	case "1h", "6h", "24h", "72h", "7d":
		return stage
	default:
		return "24h"
	}
}

type PublicationRewardContext struct {
	ChannelMedianViews float64
	StablePublication  bool
}

func RewardScore(metrics map[string]any, context PublicationRewardContext) (float64, map[string]any) {
	components := map[string]any{}
	totalWeight := 0.0
	weighted := 0.0
	add := func(name string, weight float64, value float64) {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return
		}
		if value < 0 {
			value = 0
		}
		if value > 1 {
			value = 1
		}
		components[name] = value
		totalWeight += weight
		weighted += weight * value
	}
	views := floatFromAny(metrics["views"])
	if views > 0 {
		median := context.ChannelMedianViews
		if median <= 0 {
			median = views
		}
		add("views", 0.20, views/(median*2))
	}
	likes := floatFromAny(metrics["likes"])
	comments := floatFromAny(metrics["comments"])
	shares := floatFromAny(metrics["shares"])
	if views > 0 && likes+comments+shares > 0 {
		add("engagement_rate", 0.20, ((likes+comments*2+shares*3)/views)*10)
	}
	if ctr := floatFromAny(metrics["ctr"]); ctr > 0 {
		add("ctr", 0.20, ctr/0.12)
	}
	if duration := floatFromAny(metrics["avg_view_duration_sec"]); duration > 0 {
		add("avg_view_duration_sec", 0.30, duration/45)
	}
	if context.StablePublication {
		add("publish_stability", 0.10, 1)
	}
	if totalWeight == 0 {
		return 0, components
	}
	components["total_weight"] = totalWeight
	return weighted / totalWeight, components
}
```

Add `floatFromAny` helper handling `int`, `int64`, `float32`, `float64`, and numeric strings.

- [ ] **Step 4: Update snapshot upsert**

In `internal/channelops/store_publications.go`, change `UpsertFeedbackSnapshot` signature to:

```go
func (s *Store) UpsertFeedbackSnapshot(ctx context.Context, publication PublicationRow, metrics map[string]any, stage string, score float64, fields []string, reward float64, rewardComponents map[string]any) error
```

Use `ON CONFLICT (publication_id, snapshot_stage) DO UPDATE` and set `reward_score`, `reward_components_json`, `metrics_completeness_score`, and `available_fields_json`.

- [ ] **Step 5: Update collect metrics handler**

In `HandleCollectMetrics`, compute:

```go
stage := SnapshotStageFromPayload(item.PayloadJSON)
score, fields := MetricsCompleteness(metrics)
reward, components := RewardScore(metrics, PublicationRewardContext{StablePublication: true})
return h.Store.UpsertFeedbackSnapshot(ctx, publication, metrics, stage, score, fields, reward, components)
```

- [ ] **Step 6: Add LearningState aggregation**

Create `internal/channelops/learning.go`:

```go
package channelops

import "context"

type LearningStateInput struct {
	ChannelID     string
	DimensionType string
	DimensionKey  string
	WindowDays    int
	SampleCount   int
	AvgReward     float64
}

func LearningRecommendation(sampleCount int, avgReward float64) map[string]any {
	action := "insufficient_data"
	if sampleCount >= 10 {
		action = "observe"
		if avgReward >= 0.65 {
			action = "promote_more"
		}
		if avgReward < 0.25 {
			action = "cool_down"
		}
	}
	return map[string]any{"action": action, "sample_count": sampleCount, "avg_reward": avgReward}
}

func (s *Store) RecomputeLearningState(ctx context.Context, channelID string, windowDays int) error {
	return s.RecomputeLearningStateForSources(ctx, channelID, windowDays)
}
```

Add `RecomputeLearningStateForSources` in `store_publications.go` by grouping `production_tasks.source` joined through `publication_records` and `feedback_snapshots` where `metrics_completeness_score >= 0.4` and `reward_score IS NOT NULL`.

- [ ] **Step 7: Run learning tests**

Run:

```bash
go test ./internal/channelops -run 'TestRewardScoreRenormalizesAvailableComponents|TestSnapshotStageFromPayload' -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit Task 6**

```bash
git add internal/channelops/metrics.go internal/channelops/learning.go internal/channelops/store_publications.go internal/channelops/handlers.go internal/channelops/learning_test.go
git commit -m "feat: compute channelops learning state"
```

## Task 7: API Recompute and Cross-Surface Tests

**Files:**
- Modify: `backend/app/api/channel_agent.py`
- Modify: `backend/tests/channel_agent/test_api.py`
- Add: `internal/channelops/learning_influence_test.go`

- [ ] **Step 1: Add failing recompute endpoint test**

Append to `backend/tests/channel_agent/test_api.py`:

```python
@pytest.mark.asyncio
async def test_learning_recompute_endpoint_is_present(api_session):
    async with AsyncClient(transport=ASGITransport(app=_app(api_session)), base_url="http://test") as client:
        channel = (await client.post("/api/v1/channel-agent/channels", json={"name": "Learn"})).json()
        response = await client.post(f"/api/v1/channel-agent/channels/{channel['id']}/learning/recompute")
        assert response.status_code == 200
        assert response.json()["channel_id"] == channel["id"]
        assert response.json()["recomputed"] is True
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_api.py::test_learning_recompute_endpoint_is_present -q
```

Expected: FAIL because endpoint is missing.

- [ ] **Step 3: Add recompute API as Python-side placeholder recompute**

In `backend/app/api/channel_agent.py`, add:

```python
@router.post("/channels/{channel_id}/learning/recompute")
async def recompute_learning(channel_id: str, db: AsyncSession = Depends(get_db)):
    channel = await _require_channel(db, channel_id)
    return {"channel_id": str(channel.id), "recomputed": True}
```

This endpoint is an operator trigger surface for Go/local backfill flows. It does not mutate production behavior.

- [ ] **Step 4: Add Go source inspection test for learning not affecting tick**

Create `internal/channelops/learning_influence_test.go`:

```go
package channelops

import (
	"os"
	"strings"
	"testing"
)

func TestLearningStateDoesNotAffectCandidateSelection(t *testing.T) {
	source, err := os.ReadFile("tick.go")
	if err != nil {
		t.Fatalf("read tick.go: %v", err)
	}
	text := string(source)
	if strings.Contains(text, "LearningState") && strings.Contains(text, "sort.Slice") {
		t.Fatalf("tick.go must not sort candidates by LearningState in Phase D")
	}
}
```

- [ ] **Step 5: Run targeted tests**

Run:

```bash
cd backend && python3 -m pytest tests/channel_agent/test_api.py::test_learning_recompute_endpoint_is_present -q
go test ./internal/channelops -run TestLearningStateDoesNotAffectCandidateSelection -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit Task 7**

```bash
git add backend/app/api/channel_agent.py backend/tests/channel_agent/test_api.py internal/channelops/learning_influence_test.go
git commit -m "feat: add channelops learning recompute surface"
```

## Task 8: Full Verification and Final Review

**Files:**
- Review all changed files from Tasks 1-7.

- [ ] **Step 1: Run Go ChannelOps tests**

```bash
go test ./internal/channelops ./internal/config ./internal/store ./internal/orchestrator ./internal/worker/...
```

Expected: PASS.

- [ ] **Step 2: Run backend tests**

```bash
cd backend && python3 -m pytest
```

Expected: PASS.

- [ ] **Step 3: Run backend optional static checks**

```bash
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Expected: either PASS or missing-tool output. Record missing-tool output in final notes if modules are absent.

- [ ] **Step 4: Build Go binaries touched by ChannelOps**

```bash
go build ./cmd/channelops-runner
go build ./cmd/channelops-live-smoke
```

Expected: PASS. Remove generated local binaries if the commands create untracked files.

- [ ] **Step 5: Inspect diff for scope**

```bash
git status --short
git diff --check
git log --oneline -8
```

Expected: no whitespace errors; changes limited to B/C/D files and docs/plans.

- [ ] **Step 6: Commit final cleanup if any files changed after prior commits**

```bash
git add <changed-files>
git commit -m "test: verify channelops phase bcd"
```

Skip this commit if the worktree is clean after verification.

## Self-Review Checklist

- Phase B coverage: Task 1 adds model/migration, Task 2 adds APIs, Task 4 writes runtime candidate audit, Task 3 adds failure category.
- Phase C coverage: Task 1 adds model/migration, Task 5 changes ingester and Go tick candidate generation.
- Phase D coverage: Task 1 adds schema, Task 6 adds staged metrics/reward/learning, Task 2 and Task 7 expose read-only APIs.
- E/F safety: Task 6 and Task 7 explicitly keep learning from affecting tick selection; no task changes public publishing or bandit behavior.
- Required checks: Task 8 runs Go and backend checks, with optional ruff/mypy tolerated only if local modules are absent.
