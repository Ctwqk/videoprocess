from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.events.outbox import event_outbox_table
from app.events.producer import EventProducer


@dataclass(frozen=True)
class RelayCycleResult:
    delivered: int
    errors: int


class EventOutboxRelay:
    def __init__(self, *, producer: EventProducer) -> None:
        self.producer = producer

    async def run_once(self, db: AsyncSession, *, batch_size: int = 100) -> RelayCycleResult:
        rows = (
            await db.execute(
                sa.select(
                    event_outbox_table.c.id,
                    event_outbox_table.c.topic,
                    event_outbox_table.c.key,
                    event_outbox_table.c.payload,
                )
                .where(event_outbox_table.c.delivered_at.is_(None))
                .order_by(event_outbox_table.c.created_at.asc())
                .limit(batch_size)
            )
        ).mappings().all()

        delivered = 0
        errors = 0
        for row in rows:
            try:
                await self.producer.send(
                    topic=str(row["topic"]),
                    key=str(row["key"]),
                    payload=_payload_as_dict(row["payload"]),
                )
                await db.execute(
                    sa.update(event_outbox_table)
                    .where(event_outbox_table.c.id == row["id"])
                    .values(
                        delivered_at=datetime.now(timezone.utc),
                        attempt_count=event_outbox_table.c.attempt_count + 1,
                        last_error=None,
                    )
                )
                delivered += 1
            except Exception as exc:
                errors += 1
                await db.execute(
                    sa.update(event_outbox_table)
                    .where(event_outbox_table.c.id == row["id"])
                    .values(
                        attempt_count=event_outbox_table.c.attempt_count + 1,
                        last_error=str(exc),
                    )
                )
        return RelayCycleResult(delivered=delivered, errors=errors)


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
