from __future__ import annotations

import json
from typing import Any, Protocol


class EventProducer(Protocol):
    async def send(self, *, topic: str, key: str, payload: dict[str, Any]) -> None:
        ...


class KafkaEventProducer:
    def __init__(self, *, brokers: str) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(bootstrap_servers=_split_brokers(brokers))

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def send(self, *, topic: str, key: str, payload: dict[str, Any]) -> None:
        await self._producer.send_and_wait(
            topic,
            json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"),
            key=key.encode("utf-8"),
        )


def _split_brokers(brokers: str) -> list[str]:
    return [broker.strip() for broker in brokers.split(",") if broker.strip()]
