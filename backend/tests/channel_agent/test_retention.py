from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.retention import cleanup_expired
from app.models.channel_agent import AgentTickAudit, ChannelOpsQueueItem, FeedbackSnapshot


@pytest.fixture
async def retention_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(ChannelOpsQueueItem.__table__.create)
        await conn.run_sync(AgentTickAudit.__table__.create)
        await conn.run_sync(FeedbackSnapshot.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


@pytest.mark.asyncio
async def test_cleanup_expired_deletes_old_terminal_rows_and_preserves_recent(retention_session):
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    old_queue = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key="old",
        status="succeeded",
        run_after=now - timedelta(days=40),
        created_at=now - timedelta(days=40),
    )
    recent_queue = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key="recent",
        status="succeeded",
        run_after=now - timedelta(days=1),
        created_at=now - timedelta(days=1),
    )
    running_old_queue = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key="running-old",
        status="running",
        run_after=now - timedelta(days=40),
        created_at=now - timedelta(days=40),
    )
    old_audit = AgentTickAudit(
        channel_profile_id=uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"),
        tick_id="old",
        started_at=now - timedelta(days=100),
    )
    recent_audit = AgentTickAudit(
        channel_profile_id=uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"),
        tick_id="recent",
        started_at=now - timedelta(days=10),
    )
    old_feedback = FeedbackSnapshot(
        publication_id=uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
        snapshot_stage="24h",
        collected_at=now - timedelta(days=400),
    )
    recent_feedback = FeedbackSnapshot(
        publication_id=uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
        snapshot_stage="7d",
        collected_at=now - timedelta(days=10),
    )
    retention_session.add_all(
        [old_queue, recent_queue, running_old_queue, old_audit, recent_audit, old_feedback, recent_feedback]
    )
    await retention_session.commit()

    result = await cleanup_expired(
        retention_session,
        now=now,
        queue_retention_days=30,
        audit_retention_days=90,
        feedback_retention_days=365,
    )

    queue_count = await retention_session.scalar(select(func.count()).select_from(ChannelOpsQueueItem))
    audit_count = await retention_session.scalar(select(func.count()).select_from(AgentTickAudit))
    feedback_count = await retention_session.scalar(select(func.count()).select_from(FeedbackSnapshot))
    assert result.deleted_queue_items == 1
    assert result.deleted_audits == 1
    assert result.deleted_feedback == 1
    assert queue_count == 2
    assert audit_count == 1
    assert feedback_count == 1
