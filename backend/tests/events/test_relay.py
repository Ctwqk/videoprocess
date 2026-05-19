from __future__ import annotations

import sys
import types
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.events.producer import KafkaEventProducer
from app.events.relay import EventOutboxRelay


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


class RecordingProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    async def send(self, *, topic: str, key: str, payload: dict[str, Any]) -> None:
        self.sent.append((topic, key, payload))


class FailingFirstProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, Any]]] = []

    async def send(self, *, topic: str, key: str, payload: dict[str, Any]) -> None:
        if key == "actor-1":
            raise RuntimeError("kafka down")
        self.sent.append((topic, key, payload))


class FailingEventProducer:
    def __init__(self, *, event_id: str) -> None:
        self.event_id = event_id
        self.attempted: list[tuple[str, str, dict[str, Any]]] = []

    async def send(self, *, topic: str, key: str, payload: dict[str, Any]) -> None:
        self.attempted.append((topic, key, payload))
        if payload["event_id"] == self.event_id:
            raise RuntimeError("kafka down")


@pytest.mark.asyncio
async def test_kafka_event_producer_serializes_deterministically(monkeypatch: pytest.MonkeyPatch) -> None:
    records: list[tuple[str, bytes, bytes]] = []

    class FakeAIOKafkaProducer:
        def __init__(self, *, bootstrap_servers: list[str]) -> None:
            self.bootstrap_servers = bootstrap_servers

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def send_and_wait(self, topic: str, value: bytes, *, key: bytes) -> None:
            records.append((topic, value, key))

    fake_aiokafka = types.SimpleNamespace(AIOKafkaProducer=FakeAIOKafkaProducer)
    monkeypatch.setitem(sys.modules, "aiokafka", fake_aiokafka)

    producer = KafkaEventProducer(brokers="redpanda:9092, localhost:9092")
    await producer.send(topic="vp.actor.actions.v1", key="actor-1", payload={"b": 2, "a": 1})

    assert producer._producer.bootstrap_servers == ["redpanda:9092", "localhost:9092"]
    assert records == [("vp.actor.actions.v1", b'{"a":1,"b":2}', b"actor-1")]


@pytest.mark.asyncio
async def test_relay_marks_event_delivered_after_send() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(CREATE_TABLE))
            await conn.execute(
                text(
                    "INSERT INTO event_outbox (id, topic, key, payload, created_at) "
                    "VALUES ('event-1', 'vp.actor.actions.v1', 'actor-1', "
                    "'{\"event_id\":\"event-1\"}', '2026-05-19T00:00:00Z')"
                )
            )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        producer = RecordingProducer()
        async with session_factory() as session:
            relay = EventOutboxRelay(producer=producer)
            result = await relay.run_once(session, batch_size=10)
            await session.commit()
            row = (
                await session.execute(
                    text("SELECT delivered_at, attempt_count, last_error FROM event_outbox WHERE id = 'event-1'")
                )
            ).one()

        assert result.delivered == 1
        assert result.errors == 0
        assert producer.sent == [("vp.actor.actions.v1", "actor-1", {"event_id": "event-1"})]
        assert row.delivered_at is not None
        assert row.attempt_count == 1
        assert row.last_error is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_relay_records_send_failure_and_continues() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(CREATE_TABLE))
            await conn.execute(
                text(
                    "INSERT INTO event_outbox (id, topic, key, payload, created_at) VALUES "
                    "('event-1', 'vp.actor.actions.v1', 'actor-1', "
                    "'{\"event_id\":\"event-1\"}', '2026-05-19T00:00:00Z'), "
                    "('event-2', 'vp.actor.actions.v1', 'actor-2', "
                    "'{\"event_id\":\"event-2\"}', '2026-05-19T00:00:01Z')"
                )
            )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        producer = FailingFirstProducer()
        async with session_factory() as session:
            relay = EventOutboxRelay(producer=producer)
            result = await relay.run_once(session, batch_size=10)
            await session.commit()
            failed_row = (
                await session.execute(
                    text("SELECT delivered_at, attempt_count, last_error FROM event_outbox WHERE id = 'event-1'")
                )
            ).one()
            delivered_row = (
                await session.execute(
                    text("SELECT delivered_at, attempt_count, last_error FROM event_outbox WHERE id = 'event-2'")
                )
            ).one()

        assert result.delivered == 1
        assert result.errors >= 1
        assert failed_row.delivered_at is None
        assert failed_row.attempt_count == 1
        assert "kafka down" in failed_row.last_error
        assert delivered_row.delivered_at is not None
        assert delivered_row.attempt_count == 1
        assert delivered_row.last_error is None
        assert producer.sent == [("vp.actor.actions.v1", "actor-2", {"event_id": "event-2"})]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_relay_skips_rows_claimed_by_another_relay() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(CREATE_TABLE))
            await conn.execute(
                text(
                    "INSERT INTO event_outbox "
                    "(id, topic, key, payload, created_at, claimed_at, claim_token) VALUES "
                    "('event-claimed', 'vp.actor.actions.v1', 'actor-1', "
                    "'{\"event_id\":\"event-claimed\"}', '2026-05-19T00:00:00Z', "
                    "'2026-05-19T00:00:01Z', 'other-relay'), "
                    "('event-free', 'vp.actor.actions.v1', 'actor-2', "
                    "'{\"event_id\":\"event-free\"}', '2026-05-19T00:00:02Z', NULL, NULL)"
                )
            )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        producer = RecordingProducer()
        async with session_factory() as session:
            relay = EventOutboxRelay(producer=producer)
            result = await relay.run_once(session, batch_size=10)
            await session.commit()
            rows = (
                await session.execute(
                    text(
                        "SELECT id, delivered_at, claimed_at, claim_token, attempt_count "
                        "FROM event_outbox ORDER BY id"
                    )
                )
            ).all()

        assert result.delivered == 1
        assert result.errors == 0
        assert producer.sent == [("vp.actor.actions.v1", "actor-2", {"event_id": "event-free"})]
        assert rows[0].id == "event-claimed"
        assert rows[0].delivered_at is None
        assert rows[0].claimed_at == "2026-05-19T00:00:01Z"
        assert rows[0].claim_token == "other-relay"
        assert rows[0].attempt_count == 0
        assert rows[1].id == "event-free"
        assert rows[1].delivered_at is not None
        assert rows[1].claimed_at is None
        assert rows[1].claim_token is None
        assert rows[1].attempt_count == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_relay_preserves_per_key_order_after_failure() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.execute(text(CREATE_TABLE))
            await conn.execute(
                text(
                    "INSERT INTO event_outbox (id, topic, key, payload, created_at) VALUES "
                    "('event-1', 'vp.actor.actions.v1', 'actor-1', "
                    "'{\"event_id\":\"event-1\"}', '2026-05-19T00:00:00Z'), "
                    "('event-2', 'vp.actor.actions.v1', 'actor-1', "
                    "'{\"event_id\":\"event-2\"}', '2026-05-19T00:00:01Z'), "
                    "('event-3', 'vp.actor.actions.v1', 'actor-2', "
                    "'{\"event_id\":\"event-3\"}', '2026-05-19T00:00:02Z')"
                )
            )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        producer = FailingEventProducer(event_id="event-1")
        async with session_factory() as session:
            relay = EventOutboxRelay(producer=producer)
            result = await relay.run_once(session, batch_size=10)
            await session.commit()
            rows = (
                await session.execute(
                    text(
                        "SELECT id, delivered_at, claimed_at, claim_token, attempt_count, last_error "
                        "FROM event_outbox ORDER BY id"
                    )
                )
            ).all()

        assert result.delivered == 1
        assert result.errors == 1
        assert [attempt[2]["event_id"] for attempt in producer.attempted] == ["event-1", "event-3"]
        assert rows[0].id == "event-1"
        assert rows[0].delivered_at is None
        assert rows[0].claimed_at is None
        assert rows[0].claim_token is None
        assert rows[0].attempt_count == 1
        assert "kafka down" in rows[0].last_error
        assert rows[1].id == "event-2"
        assert rows[1].delivered_at is None
        assert rows[1].claimed_at is None
        assert rows[1].claim_token is None
        assert rows[1].attempt_count == 0
        assert rows[1].last_error is None
        assert rows[2].id == "event-3"
        assert rows[2].delivered_at is not None
        assert rows[2].claimed_at is None
        assert rows[2].claim_token is None
        assert rows[2].attempt_count == 1
        assert rows[2].last_error is None
    finally:
        await engine.dispose()
