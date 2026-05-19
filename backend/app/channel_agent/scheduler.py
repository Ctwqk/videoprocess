from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.queue import ChannelOpsQueueService
from app.models.channel_agent import ChannelProfile, InternalSchedulerRun


@dataclass(frozen=True)
class SchedulerResult:
    enqueued_count: int
    skipped_count: int


class ChannelOpsScheduler:
    def __init__(self, *, queue: ChannelOpsQueueService | None = None) -> None:
        self.queue = queue or ChannelOpsQueueService()

    async def run_once(self, db: AsyncSession, *, now: datetime | None = None) -> SchedulerResult:
        current = _as_utc(now or datetime.now(timezone.utc))
        channels = (
            await db.execute(
                select(ChannelProfile)
                .where(ChannelProfile.enabled.is_(True))
                .where(ChannelProfile.halted_at.is_(None))
                .order_by(ChannelProfile.created_at.asc())
            )
        ).scalars().all()
        enqueued = 0
        skipped = 0
        for channel in channels:
            interval_minutes = _normalized_interval_minutes(channel.tick_interval_minutes)
            if channel.tick_interval_minutes != interval_minutes:
                channel.tick_interval_minutes = interval_minutes
            bucket = scheduler_bucket(current, interval_minutes)
            key = f"agent_tick:{channel.id}:{bucket}"
            if await self.queue.get_by_key(db, key) is not None:
                skipped += 1
                continue
            item = await self.queue.enqueue(
                db,
                kind="agent_tick",
                idempotency_key=key,
                payload={"channel_id": str(channel.id), "scheduler_bucket": bucket},
                priority=40,
                run_after=current,
                channel_profile_id=channel.id,
                commit=False,
            )
            db.add(
                InternalSchedulerRun(
                    channel_profile_id=channel.id,
                    bucket=bucket,
                    enqueued_queue_item_id=item.id,
                    ran_at=current,
                    status="enqueued",
                )
            )
            enqueued += 1
        await db.commit()
        return SchedulerResult(enqueued_count=enqueued, skipped_count=skipped)


def scheduler_bucket(value: datetime, interval_minutes: int | None) -> str:
    current = _as_utc(value)
    interval = _normalized_interval_minutes(interval_minutes)
    minutes_since_midnight = current.hour * 60 + current.minute
    bucket_start = (minutes_since_midnight // interval) * interval
    bucket_hour = bucket_start // 60
    bucket_minute = bucket_start % 60
    bucket_time = current.replace(
        hour=bucket_hour,
        minute=bucket_minute,
        second=0,
        microsecond=0,
    )
    if interval >= 60 and bucket_minute == 0:
        return bucket_time.strftime("%Y-%m-%d-%H")
    return bucket_time.strftime("%Y-%m-%d-%H-%M")


def _normalized_interval_minutes(value: int | None) -> int:
    try:
        interval = int(value or 60)
    except (TypeError, ValueError):
        interval = 60
    return max(interval, 15)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
