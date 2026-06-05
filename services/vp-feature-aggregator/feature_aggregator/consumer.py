from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from pydantic import ValidationError

from feature_aggregator.config import settings
from feature_aggregator.schemas import PDSDecisionEvent, VPActorActionEvent
from feature_aggregator.store import FeatureStore


logger = logging.getLogger(__name__)

DLQProducer = Callable[[str, bytes], Awaitable[None]]


class KafkaMessage(Protocol):
    topic: str
    value: bytes | None


class KafkaConsumerClient(Protocol):
    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def commit(self) -> None:
        ...

    def __aiter__(self) -> AsyncIterator[KafkaMessage]:
        ...


class KafkaProducerClient(Protocol):
    async def start(self) -> None:
        ...

    async def stop(self) -> None:
        ...

    async def send_and_wait(self, topic: str, value: bytes) -> object:
        ...


KafkaConsumerFactory = Callable[..., KafkaConsumerClient]
KafkaProducerFactory = Callable[..., KafkaProducerClient]


@dataclass(frozen=True)
class ProcessResult:
    applied: bool
    dlq_topic: str | None = None
    dlq_value: bytes | None = None
    error: str | None = None


class EventConsumer:
    def __init__(
        self,
        store: FeatureStore,
        *,
        vp_actions_topic: str = settings.vp_actions_topic,
        pds_decisions_topic: str = settings.pds_decisions_topic,
        dead_letter_topic: str = settings.dead_letter_topic,
    ) -> None:
        self.store = store
        self.vp_actions_topic = vp_actions_topic
        self.pds_decisions_topic = pds_decisions_topic
        self.dead_letter_topic = dead_letter_topic

    async def process_message(self, topic: str, value: bytes) -> ProcessResult:
        if topic == self.vp_actions_topic:
            try:
                event = VPActorActionEvent.model_validate_json(value)
            except ValidationError as exc:
                return self._dlq_result(value, exc)
            applied = await self.store.apply_vp_action(event)
            return ProcessResult(applied=applied)

        if topic == self.pds_decisions_topic:
            try:
                event = PDSDecisionEvent.model_validate_json(value)
            except ValidationError as exc:
                return self._dlq_result(value, exc)
            applied = await self.store.apply_pds_decision(event)
            return ProcessResult(applied=applied)

        logger.info("Ignoring unsupported Kafka topic %s", topic)
        return ProcessResult(applied=False)

    def _dlq_result(self, value: bytes, exc: ValidationError) -> ProcessResult:
        return ProcessResult(
            applied=False,
            dlq_topic=self.dead_letter_topic,
            dlq_value=value,
            error=str(exc),
        )


async def run_consumer(
    store: FeatureStore,
    *,
    brokers: str = settings.kafka_brokers,
    group_id: str = settings.kafka_group_id,
    vp_actions_topic: str = settings.vp_actions_topic,
    pds_decisions_topic: str = settings.pds_decisions_topic,
    dead_letter_topic: str = settings.dead_letter_topic,
    topics: Sequence[str] | None = None,
    dlq_producer: DLQProducer | None = None,
    consumer_factory: KafkaConsumerFactory | None = None,
    producer_factory: KafkaProducerFactory | None = None,
) -> None:
    """Run the Kafka consumer loop.

    Offsets are committed only after a message is applied, ignored, or sent to the DLQ.
    If DLQ send fails, the exception is allowed to stop the loop so the offset is not
    committed and the message can be retried.
    """

    if consumer_factory is None or producer_factory is None:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

        if consumer_factory is None:
            consumer_factory = AIOKafkaConsumer
        if producer_factory is None:
            producer_factory = AIOKafkaProducer

    subscribed_topics = tuple(topics or (vp_actions_topic, pds_decisions_topic))
    consumer = consumer_factory(
        *subscribed_topics,
        bootstrap_servers=brokers,
        group_id=group_id,
        enable_auto_commit=False,
    )
    kafka_dlq_producer: KafkaProducerClient | None = None
    active_dlq_producer = dlq_producer
    consumer_started = False
    producer_started = False
    event_consumer = EventConsumer(
        store,
        vp_actions_topic=vp_actions_topic,
        pds_decisions_topic=pds_decisions_topic,
        dead_letter_topic=dead_letter_topic,
    )
    try:
        if active_dlq_producer is None:
            kafka_dlq_producer = producer_factory(bootstrap_servers=brokers)
            await kafka_dlq_producer.start()
            producer_started = True

            async def send_to_kafka_dlq(topic: str, value: bytes) -> None:
                assert kafka_dlq_producer is not None
                await kafka_dlq_producer.send_and_wait(topic, value)

            active_dlq_producer = send_to_kafka_dlq

        await consumer.start()
        consumer_started = True

        async for message in consumer:
            value = message.value or b""
            result = await event_consumer.process_message(message.topic, value)
            if result.dlq_topic is not None:
                assert result.dlq_value is not None
                logger.warning(
                    "Routing malformed Kafka message from %s to %s: %s",
                    message.topic,
                    result.dlq_topic,
                    result.error,
                )
                await active_dlq_producer(result.dlq_topic, result.dlq_value)
            await consumer.commit()
    except asyncio.CancelledError:
        raise
    finally:
        try:
            if consumer_started:
                await consumer.stop()
        finally:
            if producer_started and kafka_dlq_producer is not None:
                await kafka_dlq_producer.stop()
