from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.channel_agent.runner as runner_module
from app.channel_agent.clock import FakeClock
from app.channel_agent.queue import ChannelOpsQueueService
from app.channel_agent.runner import ChannelAgentRunner
from app.events.outbox import EventOutbox, event_outbox_table
from app.events.schemas import TOPIC_VP_ACTIONS, build_actor_action_event
from app.models.channel_agent import ChannelOpsQueueItem


@pytest.fixture
async def runner_session_factory():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(ChannelOpsQueueItem.__table__.create)
        await conn.run_sync(event_outbox_table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    yield session_factory
    await engine.dispose()


class FailingSideEffectRunner(ChannelAgentRunner):
    async def handle_item(self, db, item) -> None:
        payload = build_actor_action_event(
            actor_id="actor-1",
            action_type="runner_side_effect",
            platform="youtube",
        )
        await EventOutbox().enqueue(
            db,
            topic=TOPIC_VP_ACTIONS,
            key=payload["actor_id"],
            payload=payload,
        )
        raise RuntimeError("handler boom")


class NoDbRunner(ChannelAgentRunner):
    def __init__(self, *, original_sleep):
        super().__init__(worker_id="test-runner")
        self.original_sleep = original_sleep
        self.run_once_scheduler_flags: list[bool] = []

    async def run_once(self, *, run_scheduler_when_idle: bool = True) -> bool:
        self.run_once_scheduler_flags.append(run_scheduler_when_idle)
        await self.original_sleep(0)
        return False


def test_live_runner_requires_youtube_manager_url(monkeypatch):
    monkeypatch.setattr(runner_module.settings, "youtube_manager_url", "")

    with pytest.raises(RuntimeError, match="YOUTUBE_MANAGER_URL"):
        ChannelAgentRunner(worker_id="test-runner")


@pytest.mark.asyncio
async def test_runner_rolls_back_handler_side_effects_before_retry_mark(
    runner_session_factory,
    monkeypatch,
):
    clock = FakeClock(datetime(2026, 5, 19, 9, 0, tzinfo=timezone.utc))
    queue = ChannelOpsQueueService(clock=clock)
    async with runner_session_factory() as session:
        item = await queue.enqueue(
            session,
            kind="agent_tick",
            idempotency_key="agent_tick:rollback:2026-05-19-09",
            payload={"channel_id": "channel-1"},
            run_after=clock.now(),
        )
        item_id = item.id

    monkeypatch.setattr(runner_module, "async_session", runner_session_factory)
    runner = FailingSideEffectRunner(worker_id="test-runner")
    runner.queue = queue

    handled = await runner.run_once()

    assert handled is True
    async with runner_session_factory() as session:
        queued_item = await session.get(ChannelOpsQueueItem, item_id)
        outbox_count = await session.scalar(select(func.count()).select_from(event_outbox_table))

    assert queued_item is not None
    assert queued_item.status == "queued"
    assert queued_item.last_error == "handler boom"
    assert outbox_count == 0


@pytest.mark.asyncio
async def test_run_forever_starts_scheduler_loop_independently(monkeypatch):
    original_sleep = asyncio.sleep
    runner = NoDbRunner(original_sleep=original_sleep)
    calls = {"scheduler": 0}

    async def fake_scheduler_loop(*, poll_seconds: float) -> None:
        calls["scheduler"] += 1
        await original_sleep(3600)

    async def stop_after_first_idle_sleep(seconds: float) -> None:
        raise StopAsyncIteration

    runner._run_scheduler_forever = fake_scheduler_loop
    monkeypatch.setattr(asyncio, "sleep", stop_after_first_idle_sleep)

    with pytest.raises(StopAsyncIteration):
        await runner.run_forever(poll_seconds=0.01)

    assert calls["scheduler"] == 1
    assert runner.run_once_scheduler_flags == [False]
