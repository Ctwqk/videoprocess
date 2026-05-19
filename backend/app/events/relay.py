from __future__ import annotations

import json
import uuid
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
        """Claim and deliver one batch.

        The caller owns transaction commit/rollback. Delivery is at-least-once:
        if Kafka send succeeds but the surrounding DB commit fails, the row can
        be sent again and consumers must tolerate duplicate event IDs.
        """
        claim_token = str(uuid.uuid4())
        await self._claim_batch(db, claim_token=claim_token, batch_size=batch_size)
        rows = (
            await db.execute(
                sa.select(
                    event_outbox_table.c.id,
                    event_outbox_table.c.topic,
                    event_outbox_table.c.key,
                    event_outbox_table.c.payload,
                )
                .where(event_outbox_table.c.claim_token == claim_token)
                .where(event_outbox_table.c.delivered_at.is_(None))
                .order_by(event_outbox_table.c.created_at.asc(), event_outbox_table.c.id.asc())
                .limit(batch_size)
            )
        ).mappings().all()

        delivered = 0
        errors = 0
        blocked_keys: set[str] = set()
        for row in rows:
            row_id = str(row["id"])
            key = str(row["key"])
            if key in blocked_keys:
                await self._clear_claim(db, row_id=row_id, claim_token=claim_token)
                continue
            try:
                await self.producer.send(
                    topic=str(row["topic"]),
                    key=key,
                    payload=_payload_as_dict(row["payload"]),
                )
                await db.execute(
                    sa.update(event_outbox_table)
                    .where(event_outbox_table.c.id == row_id)
                    .where(event_outbox_table.c.claim_token == claim_token)
                    .values(
                        delivered_at=datetime.now(timezone.utc),
                        claimed_at=None,
                        claim_token=None,
                        attempt_count=event_outbox_table.c.attempt_count + 1,
                        last_error=None,
                    )
                )
                delivered += 1
            except Exception as exc:
                errors += 1
                blocked_keys.add(key)
                await db.execute(
                    sa.update(event_outbox_table)
                    .where(event_outbox_table.c.id == row_id)
                    .where(event_outbox_table.c.claim_token == claim_token)
                    .values(
                        claimed_at=None,
                        claim_token=None,
                        attempt_count=event_outbox_table.c.attempt_count + 1,
                        last_error=str(exc),
                    )
                )
        return RelayCycleResult(delivered=delivered, errors=errors)

    async def _claim_batch(self, db: AsyncSession, *, claim_token: str, batch_size: int) -> None:
        claimed_at = datetime.now(timezone.utc)
        candidate_ids = (
            sa.select(event_outbox_table.c.id)
            .where(event_outbox_table.c.delivered_at.is_(None))
            .where(event_outbox_table.c.claimed_at.is_(None))
            .where(event_outbox_table.c.claim_token.is_(None))
            .order_by(event_outbox_table.c.created_at.asc(), event_outbox_table.c.id.asc())
            .limit(batch_size)
        )
        await db.execute(
            sa.update(event_outbox_table)
            .where(event_outbox_table.c.id.in_(candidate_ids))
            .where(event_outbox_table.c.delivered_at.is_(None))
            .where(event_outbox_table.c.claimed_at.is_(None))
            .where(event_outbox_table.c.claim_token.is_(None))
            .values(claimed_at=claimed_at, claim_token=claim_token)
        )

    async def _clear_claim(self, db: AsyncSession, *, row_id: str, claim_token: str) -> None:
        await db.execute(
            sa.update(event_outbox_table)
            .where(event_outbox_table.c.id == row_id)
            .where(event_outbox_table.c.claim_token == claim_token)
            .values(claimed_at=None, claim_token=None)
        )


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
