from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.events.outbox import EventOutbox
from app.events.schemas import TOPIC_VP_ACTIONS, build_actor_action_event


CREATE_TABLE = """
CREATE TABLE event_outbox (
  id TEXT PRIMARY KEY,
  topic TEXT NOT NULL,
  key TEXT NOT NULL,
  payload JSON NOT NULL,
  created_at TEXT NOT NULL,
  delivered_at TEXT,
  claimed_at TEXT,
  claim_token TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT
)
"""


@pytest.mark.asyncio
async def test_outbox_writes_undelivered_event() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(CREATE_TABLE))
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session:
            outbox = EventOutbox()
            event_id = await outbox.enqueue(
                session,
                topic="vp.actor.actions.v1",
                key="actor-1",
                payload={"event_id": "event-1", "actor_id": "actor-1"},
            )
            await session.commit()
            rows = (await session.execute(text("SELECT id, topic, key, delivered_at FROM event_outbox"))).all()

        assert rows == [(event_id, "vp.actor.actions.v1", "actor-1", None)]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_outbox_synthesizes_matching_event_id_into_payload() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(CREATE_TABLE))
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session:
            outbox = EventOutbox()
            event_id = await outbox.enqueue(
                session,
                topic="vp.actor.actions.v1",
                key="actor-1",
                payload={"actor_id": "actor-1"},
            )
            await session.commit()
            row = (await session.execute(text("SELECT id, payload FROM event_outbox"))).one()

        payload = _payload_as_dict(row.payload)
        assert row.id == event_id
        assert payload["event_id"] == event_id
    finally:
        await engine.dispose()


def test_build_actor_action_event_returns_versioned_schema() -> None:
    occurred_at = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)

    event = build_actor_action_event(
        actor_id="actor-1",
        action_type="publication_scheduled",
        platform="youtube",
        metadata={"task_id": "task-1"},
        occurred_at=occurred_at,
    )

    assert set(event) == {
        "event_id",
        "topic_version",
        "actor_id",
        "action_type",
        "platform",
        "occurred_at",
        "source",
        "metadata",
    }
    assert uuid.UUID(event["event_id"])
    assert event["topic_version"] == TOPIC_VP_ACTIONS
    assert event["actor_id"] == "actor-1"
    assert event["action_type"] == "publication_scheduled"
    assert event["platform"] == "youtube"
    assert event["occurred_at"] == "2026-05-19T12:00:00+00:00"
    assert event["source"] == "videoprocess.channel_ops"
    assert event["metadata"] == {"task_id": "task-1"}


def _payload_as_dict(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
    return dict(payload)
