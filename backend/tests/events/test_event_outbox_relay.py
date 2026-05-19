from __future__ import annotations

from typing import Any

import pytest

import event_outbox_relay
from app.events.relay import RelayCycleResult


class StopRelay(Exception):
    pass


@pytest.mark.asyncio
async def test_run_forever_retries_producer_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    started: list[FakeProducer] = []
    stopped: list[FakeProducer] = []
    sleep_delays: list[float] = []

    class FakeProducer:
        def __init__(self, *, brokers: str) -> None:
            self.brokers = brokers

        async def start(self) -> None:
            started.append(self)
            if len(started) == 1:
                raise RuntimeError("redpanda starting")

        async def stop(self) -> None:
            stopped.append(self)

    class FakeRelay:
        def __init__(self, *, producer: FakeProducer) -> None:
            self.producer = producer

        async def run_once(self, db: Any, *, batch_size: int) -> RelayCycleResult:
            return RelayCycleResult(delivered=0, errors=0)

    class FakeDb:
        async def commit(self) -> None:
            pass

    class FakeSession:
        async def __aenter__(self) -> FakeDb:
            return FakeDb()

        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
            pass

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        if len(started) >= 2:
            raise StopRelay

    async def fake_refresh_unsent_gauge(db: Any) -> None:
        pass

    monkeypatch.setattr(event_outbox_relay.settings, "risk_kafka_brokers", "redpanda:9092")
    monkeypatch.setattr(event_outbox_relay.settings, "risk_outbox_batch_size", 5)
    monkeypatch.setattr(event_outbox_relay.settings, "risk_outbox_poll_seconds", 0.01)
    monkeypatch.setattr(event_outbox_relay.settings, "risk_outbox_max_backoff_seconds", 0.05)
    monkeypatch.setattr(event_outbox_relay.settings, "risk_outbox_metrics_port", 19101)
    monkeypatch.setattr(event_outbox_relay, "start_http_server", lambda port: None)
    monkeypatch.setattr(event_outbox_relay, "KafkaEventProducer", FakeProducer)
    monkeypatch.setattr(event_outbox_relay, "EventOutboxRelay", FakeRelay)
    monkeypatch.setattr(event_outbox_relay, "async_session", lambda: FakeSession())
    monkeypatch.setattr(event_outbox_relay, "_refresh_unsent_gauge", fake_refresh_unsent_gauge)
    monkeypatch.setattr(event_outbox_relay.asyncio, "sleep", fake_sleep)

    with pytest.raises(StopRelay):
        await event_outbox_relay.run_forever()

    assert [producer.brokers for producer in started] == ["redpanda:9092", "redpanda:9092"]
    assert sleep_delays == [0.01, 0.01]
    assert stopped == [started[1]]
