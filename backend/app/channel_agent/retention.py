from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.constants import QUEUE_CANCELLED, QUEUE_DEAD_LETTERED, QUEUE_SUCCEEDED
from app.models.channel_agent import AgentTickAudit, ChannelOpsQueueItem, FeedbackSnapshot


@dataclass(frozen=True)
class RetentionResult:
    deleted_queue_items: int
    deleted_audits: int
    deleted_feedback: int


async def cleanup_expired(
    db: AsyncSession,
    *,
    now: datetime,
    queue_retention_days: int,
    audit_retention_days: int,
    feedback_retention_days: int,
) -> RetentionResult:
    current = _as_utc(now)
    queue_cutoff = current - timedelta(days=max(queue_retention_days, 0))
    audit_cutoff = current - timedelta(days=max(audit_retention_days, 0))
    feedback_cutoff = current - timedelta(days=max(feedback_retention_days, 0))

    queue_rows = (
        await db.execute(
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.status.in_([QUEUE_SUCCEEDED, QUEUE_DEAD_LETTERED, QUEUE_CANCELLED]))
            .where(ChannelOpsQueueItem.created_at < queue_cutoff)
        )
    ).scalars().all()
    audit_rows = (
        await db.execute(select(AgentTickAudit).where(AgentTickAudit.started_at < audit_cutoff))
    ).scalars().all()
    feedback_rows = (
        await db.execute(select(FeedbackSnapshot).where(FeedbackSnapshot.collected_at < feedback_cutoff))
    ).scalars().all()

    for row in [*queue_rows, *audit_rows, *feedback_rows]:
        await db.delete(row)
    await db.commit()
    return RetentionResult(
        deleted_queue_items=len(queue_rows),
        deleted_audits=len(audit_rows),
        deleted_feedback=len(feedback_rows),
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
