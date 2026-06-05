from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from feature_aggregator.consumer import EventConsumer, run_consumer
from feature_aggregator.store.memory import MemoryFeatureStore


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)


@dataclass
class FakeMessage:
    topic: str
    value: bytes | None


class FakeKafkaConsumer:
    def __init__(
        self,
        messages: list[FakeMessage],
        *,
        start_error: BaseException | None = None,
    ) -> None:
        self.messages = messages
        self.start_error = start_error
        self.start_calls = 0
        self.stop_calls = 0
        self.commit_count = 0
        self._next_index = 0

    async def start(self) -> None:
        self.start_calls += 1
        if self.start_error is not None:
            raise self.start_error

    async def stop(self) -> None:
        self.stop_calls += 1

    async def commit(self) -> None:
        self.commit_count += 1

    def __aiter__(self) -> FakeKafkaConsumer:
        return self

    async def __anext__(self) -> FakeMessage:
        if self._next_index >= len(self.messages):
            raise StopAsyncIteration
        message = self.messages[self._next_index]
        self._next_index += 1
        return message


class FakeKafkaProducer:
    def __init__(self, *, send_error: BaseException | None = None) -> None:
        self.send_error = send_error
        self.start_calls = 0
        self.stop_calls = 0
        self.sent: list[tuple[str, bytes]] = []

    async def start(self) -> None:
        self.start_calls += 1

    async def stop(self) -> None:
        self.stop_calls += 1

    async def send_and_wait(self, topic: str, value: bytes) -> None:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append((topic, value))


class FailingStore:
    async def apply_vp_action(self, event) -> bool:
        raise RuntimeError("store down")

    async def apply_pds_decision(self, event) -> bool:
        raise RuntimeError("store down")

    async def features_for(self, actor_id: str):
        raise NotImplementedError


def vp_action_payload(
    *,
    event_id: str = "event-1",
    actor_id: str = "actor-1",
    action_type: str = "publication_scheduled",
) -> bytes:
    return (
        "{"
        f'"event_id":"{event_id}",'
        '"topic_version":"vp.actor.actions.v1",'
        f'"actor_id":"{actor_id}",'
        f'"action_type":"{action_type}",'
        '"platform":"youtube",'
        f'"occurred_at":"{NOW.isoformat()}",'
        '"source":"videoprocess.channel_ops"'
        "}"
    ).encode()


def pds_decision_payload(
    *,
    event_id: str = "decision-event-1",
    actor_id: str = "actor-1",
    verdict: str = "block",
    decision_id: str = "decision-1",
) -> bytes:
    return (
        "{"
        f'"event_id":"{event_id}",'
        '"topic_version":"pds.decisions.v1",'
        f'"actor_id":"{actor_id}",'
        '"action_type":"publish",'
        '"platform":"youtube",'
        f'"verdict":"{verdict}",'
        '"score":0.8,'
        f'"decision_id":"{decision_id}",'
        f'"occurred_at":"{NOW.isoformat()}"'
        "}"
    ).encode()


async def test_vp_action_event_increments_store_window():
    store = MemoryFeatureStore(now=lambda: NOW)
    consumer = EventConsumer(store)

    result = await consumer.process_message("vp.actor.actions.v1", vp_action_payload())

    features = await store.features_for("actor-1")
    assert result.applied is True
    assert result.dlq_topic is None
    assert features.publishes_5m == 1
    assert features.publishes_1h == 1
    assert features.publishes_24h == 1


async def test_pds_decision_events_increment_block_and_flag_windows():
    store = MemoryFeatureStore(now=lambda: NOW)
    consumer = EventConsumer(store)

    block_result = await consumer.process_message(
        "pds.decisions.v1",
        pds_decision_payload(
            event_id="decision-event-block",
            verdict="block",
            decision_id="decision-block",
        ),
    )
    flag_result = await consumer.process_message(
        "pds.decisions.v1",
        pds_decision_payload(
            event_id="decision-event-flag",
            verdict="flag",
            decision_id="decision-flag",
        ),
    )

    features = await store.features_for("actor-1")
    assert block_result.applied is True
    assert flag_result.applied is True
    assert features.blocks_24h == 1
    assert features.flags_7d == 1


async def test_bad_json_sends_original_bytes_to_dlq():
    store = MemoryFeatureStore(now=lambda: NOW)
    consumer = EventConsumer(store)
    payload = b'{"event_id":'

    result = await consumer.process_message("vp.actor.actions.v1", payload)

    assert result.applied is False
    assert result.dlq_topic == "risk.events.dlq.v1"
    assert result.dlq_value == payload


async def test_unknown_topics_are_ignored_without_applying_or_dlq():
    store = MemoryFeatureStore(now=lambda: NOW)
    consumer = EventConsumer(store)
    payload = vp_action_payload()

    result = await consumer.process_message("unknown.topic.v1", payload)

    features = await store.features_for("actor-1")
    assert result.applied is False
    assert result.dlq_topic is None
    assert features.publishes_24h == 0


async def test_invalid_known_topic_payload_goes_to_dlq():
    store = MemoryFeatureStore(now=lambda: NOW)
    consumer = EventConsumer(store)
    payload = vp_action_payload(action_type="not-a-real-action")

    result = await consumer.process_message("vp.actor.actions.v1", payload)

    features = await store.features_for("actor-1")
    assert result.applied is False
    assert result.dlq_topic == "risk.events.dlq.v1"
    assert result.dlq_value == payload
    assert features.publishes_24h == 0


async def test_run_consumer_valid_message_applies_and_commits_once():
    store = MemoryFeatureStore(now=lambda: NOW)
    kafka_consumer = FakeKafkaConsumer(
        [FakeMessage("vp.actor.actions.v1", vp_action_payload())]
    )
    kafka_producer = FakeKafkaProducer()

    await run_consumer(
        store,
        consumer_factory=lambda *args, **kwargs: kafka_consumer,
        producer_factory=lambda **kwargs: kafka_producer,
    )

    features = await store.features_for("actor-1")
    assert features.publishes_24h == 1
    assert kafka_consumer.commit_count == 1
    assert kafka_consumer.stop_calls == 1
    assert kafka_producer.stop_calls == 1


async def test_run_consumer_malformed_known_topic_sends_dlq_then_commits_once():
    store = MemoryFeatureStore(now=lambda: NOW)
    payload = b'{"event_id":'
    kafka_consumer = FakeKafkaConsumer([FakeMessage("vp.actor.actions.v1", payload)])
    kafka_producer = FakeKafkaProducer()

    await run_consumer(
        store,
        consumer_factory=lambda *args, **kwargs: kafka_consumer,
        producer_factory=lambda **kwargs: kafka_producer,
    )

    assert kafka_producer.sent == [("risk.events.dlq.v1", payload)]
    assert kafka_consumer.commit_count == 1
    assert kafka_consumer.stop_calls == 1
    assert kafka_producer.stop_calls == 1


async def test_run_consumer_dlq_send_failure_does_not_commit_and_propagates():
    store = MemoryFeatureStore(now=lambda: NOW)
    kafka_consumer = FakeKafkaConsumer(
        [FakeMessage("vp.actor.actions.v1", b'{"event_id":')]
    )
    kafka_producer = FakeKafkaProducer(send_error=RuntimeError("dlq down"))

    with pytest.raises(RuntimeError, match="dlq down"):
        await run_consumer(
            store,
            consumer_factory=lambda *args, **kwargs: kafka_consumer,
            producer_factory=lambda **kwargs: kafka_producer,
        )

    assert kafka_consumer.commit_count == 0
    assert kafka_consumer.stop_calls == 1
    assert kafka_producer.stop_calls == 1


async def test_run_consumer_store_failure_does_not_commit_and_propagates():
    kafka_consumer = FakeKafkaConsumer(
        [FakeMessage("vp.actor.actions.v1", vp_action_payload())]
    )
    kafka_producer = FakeKafkaProducer()

    with pytest.raises(RuntimeError, match="store down"):
        await run_consumer(
            FailingStore(),
            consumer_factory=lambda *args, **kwargs: kafka_consumer,
            producer_factory=lambda **kwargs: kafka_producer,
        )

    assert kafka_consumer.commit_count == 0
    assert kafka_consumer.stop_calls == 1
    assert kafka_producer.stop_calls == 1


async def test_run_consumer_stops_started_producer_when_consumer_start_fails():
    kafka_consumer = FakeKafkaConsumer([], start_error=RuntimeError("start failed"))
    kafka_producer = FakeKafkaProducer()

    with pytest.raises(RuntimeError, match="start failed"):
        await run_consumer(
            MemoryFeatureStore(now=lambda: NOW),
            consumer_factory=lambda *args, **kwargs: kafka_consumer,
            producer_factory=lambda **kwargs: kafka_producer,
        )

    assert kafka_producer.start_calls == 1
    assert kafka_producer.stop_calls == 1
    assert kafka_consumer.commit_count == 0
