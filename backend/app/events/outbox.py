from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession


event_outbox_table = sa.Table(
    "event_outbox",
    sa.MetaData(),
    sa.Column("id", sa.String(length=64), primary_key=True),
    sa.Column("topic", sa.String(length=255), nullable=False),
    sa.Column("key", sa.String(length=255), nullable=False),
    sa.Column("payload", sa.JSON(), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("attempt_count", sa.Integer(), nullable=False),
    sa.Column("last_error", sa.Text(), nullable=True),
)


class EventOutbox:
    async def enqueue(
        self,
        db: AsyncSession,
        *,
        topic: str,
        key: str,
        payload: dict[str, Any],
    ) -> str:
        event_id = str(payload.get("event_id") or uuid.uuid4())
        await db.execute(
            sa.insert(event_outbox_table).values(
                id=event_id,
                topic=topic,
                key=key,
                payload=payload,
                created_at=datetime.now(timezone.utc),
                attempt_count=0,
            )
        )
        return event_id
