from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.scheduler import ChannelOpsScheduler, scheduler_bucket
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, InternalSchedulerRun


@pytest.fixture
async def scheduler_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(ChannelProfile.__table__.create)
        await conn.run_sync(ChannelOpsQueueItem.__table__.create)
        await conn.run_sync(InternalSchedulerRun.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


@pytest.mark.asyncio
async def test_scheduler_enqueues_one_tick_per_channel_bucket_and_floors_interval(scheduler_session):
    now = datetime(2026, 5, 19, 12, 5, tzinfo=timezone.utc)
    channel = ChannelProfile(name="scheduled", dry_run=True, enabled=True, tick_interval_minutes=5)
    halted = ChannelProfile(name="halted", dry_run=True, enabled=True, halted_at=now)
    disabled = ChannelProfile(name="disabled", dry_run=True, enabled=False)
    scheduler_session.add_all([channel, halted, disabled])
    await scheduler_session.commit()

    scheduler = ChannelOpsScheduler()
    first = await scheduler.run_once(scheduler_session, now=now)
    second = await scheduler.run_once(scheduler_session, now=now)

    assert first.enqueued_count == 1
    assert second.enqueued_count == 0
    await scheduler_session.refresh(channel)
    assert channel.tick_interval_minutes == 15
    queue_count = await scheduler_session.scalar(select(func.count()).select_from(ChannelOpsQueueItem))
    run_count = await scheduler_session.scalar(select(func.count()).select_from(InternalSchedulerRun))
    item = (await scheduler_session.execute(select(ChannelOpsQueueItem))).scalar_one()
    assert queue_count == 1
    assert run_count == 1
    assert item.kind == "agent_tick"
    assert item.payload_json["channel_id"] == str(channel.id)
    assert item.idempotency_key == f"agent_tick:{channel.id}:2026-05-19-12-00"


def test_scheduler_bucket_respects_tick_interval_minutes():
    now = datetime(2026, 5, 19, 10, 37, tzinfo=timezone.utc)

    assert scheduler_bucket(now, 15) == "2026-05-19-10-30"
    assert scheduler_bucket(now, 30) == "2026-05-19-10-30"
    assert scheduler_bucket(now, 60) == "2026-05-19-10"
    assert scheduler_bucket(now, 240) == "2026-05-19-08"


@pytest.mark.asyncio
async def test_scheduler_uses_interval_bucket_in_idempotency_key(scheduler_session):
    now = datetime(2026, 5, 19, 10, 37, tzinfo=timezone.utc)
    channel = ChannelProfile(name="slow", dry_run=True, enabled=True, tick_interval_minutes=240)
    scheduler_session.add(channel)
    await scheduler_session.commit()

    result = await ChannelOpsScheduler().run_once(scheduler_session, now=now)

    assert result.enqueued_count == 1
    item = (await scheduler_session.execute(select(ChannelOpsQueueItem))).scalar_one()
    run = (await scheduler_session.execute(select(InternalSchedulerRun))).scalar_one()
    assert item.idempotency_key == f"agent_tick:{channel.id}:2026-05-19-08"
    assert item.payload_json["scheduler_bucket"] == "2026-05-19-08"
    assert run.bucket == "2026-05-19-08"
