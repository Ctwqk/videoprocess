from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.clock import Clock
from app.channel_agent.constants import (
    QUEUE_DEAD_LETTERED,
    QUEUE_FAILED,
    QUEUE_QUEUED,
    QUEUE_RUNNING,
    QUEUE_SUCCEEDED,
)
from app.models.channel_agent import ChannelOpsQueueItem


def utc_hour_bucket(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    value = value.astimezone(timezone.utc)
    return value.strftime("%Y-%m-%d-%H")


class ChannelOpsQueueService:
    def __init__(self, clock: Clock | None = None) -> None:
        self.clock = clock or Clock()

    async def enqueue(
        self,
        db: AsyncSession,
        *,
        kind: str,
        idempotency_key: str,
        payload: dict[str, Any] | None = None,
        priority: int = 100,
        run_after: datetime | None = None,
        channel_profile_id=None,
        parent_queue_item_id=None,
        max_attempts: int = 3,
    ) -> ChannelOpsQueueItem:
        existing = await self.get_by_key(db, idempotency_key)
        if existing is not None:
            return existing

        item = ChannelOpsQueueItem(
            kind=kind,
            idempotency_key=idempotency_key,
            channel_profile_id=channel_profile_id,
            payload_json=dict(payload or {}),
            priority=priority,
            run_after=run_after or self.clock.now(),
            parent_queue_item_id=parent_queue_item_id,
            max_attempts=max_attempts,
        )
        db.add(item)
        try:
            await db.commit()
        except IntegrityError:
            await db.rollback()
            existing = await self.get_by_key(db, idempotency_key)
            if existing is None:
                raise
            await db.refresh(existing)
            return existing
        await db.refresh(item)
        return item

    async def get_by_key(self, db: AsyncSession, idempotency_key: str) -> ChannelOpsQueueItem | None:
        result = await db.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.idempotency_key == idempotency_key)
        )
        return result.scalar_one_or_none()

    async def claim_next(self, db: AsyncSession, *, worker_id: str) -> ChannelOpsQueueItem | None:
        now = self.clock.now()
        stmt = (
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.status == QUEUE_QUEUED)
            .where(ChannelOpsQueueItem.run_after <= now)
            .order_by(ChannelOpsQueueItem.priority.asc(), ChannelOpsQueueItem.created_at.asc())
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        result = await db.execute(stmt)
        item = result.scalar_one_or_none()
        if item is None:
            return None
        item.status = QUEUE_RUNNING
        item.locked_by = worker_id
        item.locked_at = now
        item.attempt_count = int(item.attempt_count or 0) + 1
        await db.commit()
        await db.refresh(item)
        return item

    async def mark_succeeded(self, db: AsyncSession, item: ChannelOpsQueueItem) -> ChannelOpsQueueItem:
        item.status = QUEUE_SUCCEEDED
        item.last_error = None
        await db.commit()
        await db.refresh(item)
        return item

    async def mark_failed_or_retry(
        self,
        db: AsyncSession,
        item: ChannelOpsQueueItem,
        error_message: str,
        *,
        max_attempts: int | None = None,
        retry_delay: timedelta | None = None,
    ) -> ChannelOpsQueueItem:
        limit = max_attempts if max_attempts is not None else int(item.max_attempts or 3)
        item.last_error = error_message
        if int(item.attempt_count or 0) >= limit:
            item.status = QUEUE_DEAD_LETTERED
            item.dead_letter_at = self.clock.now()
        else:
            item.status = QUEUE_QUEUED
            if retry_delay is None:
                retry_delay = timedelta(minutes=min(5 * (2 ** (int(item.attempt_count or 1) - 1)), 30))
            item.run_after = self.clock.now() + retry_delay
            item.locked_at = None
            item.locked_by = None
        await db.commit()
        await db.refresh(item)
        return item

    async def mark_failed(self, db: AsyncSession, item: ChannelOpsQueueItem, error_message: str) -> ChannelOpsQueueItem:
        item.status = QUEUE_FAILED
        item.last_error = error_message
        await db.commit()
        await db.refresh(item)
        return item
