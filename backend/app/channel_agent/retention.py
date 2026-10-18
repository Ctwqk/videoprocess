from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import String, cast, func, or_, select, union
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.constants import QUEUE_CANCELLED, QUEUE_DEAD_LETTERED, QUEUE_SUCCEEDED
from app.models.channel_agent import (
    AgentTickAudit, ChannelOpsQueueItem, FeedbackSnapshot,
    PublicationMetricSchedule, PublicationRecord,
)
from app.models.youtube_upload_operation import YouTubeUploadOperation


@dataclass(frozen=True)
class RetentionResult:
    deleted_queue_items: int
    deleted_audits: int
    deleted_feedback: int


def _publication_evidence():
    """Retain the same parent/child component consumed by owned task history."""
    tasks = union(
        select(PublicationRecord.production_task_id).where(
            func.lower(PublicationRecord.platform) == "youtube"
        ),
        select(YouTubeUploadOperation.production_task_id).where(
            YouTubeUploadOperation.production_task_id.is_not(None)
        ),
    ).cte("publication_evidence_tasks")
    publications = select(PublicationRecord.id).where(
        PublicationRecord.production_task_id.in_(select(tasks.c.production_task_id))
    ).cte("publication_evidence_publications")
    metrics = select(PublicationMetricSchedule.id).where(
        PublicationMetricSchedule.publication_id.in_(select(publications.c.id))
    )

    def reference(value):
        # SQLite stores UUID columns without separators; PostgreSQL uses canonical
        # UUID text. Compare text safely without casting untrusted JSON to UUID.
        return func.replace(cast(value, String), "-", "")

    queue = ChannelOpsQueueItem.__table__
    related = select(queue.c.id, queue.c.parent_queue_item_id).where(or_(
        reference(queue.c.payload_json["production_task_id"].as_string()).in_(
            select(reference(tasks.c.production_task_id))
        ),
        reference(queue.c.payload_json["publication_id"].as_string()).in_(
            select(reference(publications.c.id))
        ),
        reference(queue.c.payload_json["metric_schedule_id"].as_string()).in_(
            metrics.with_only_columns(reference(PublicationMetricSchedule.id))
        ),
    )).cte("publication_evidence_queues", recursive=True)
    # UNION, not UNION ALL: corrupt cycles must terminate and remain evidence.
    related = related.union(select(queue.c.id, queue.c.parent_queue_item_id).join(
        related, or_(queue.c.id == related.c.parent_queue_item_id,
                     queue.c.parent_queue_item_id == related.c.id),
    ))
    return publications, related


async def cleanup_expired(
    db: AsyncSession,
    *,
    now: datetime,
    queue_retention_days: int,
    audit_retention_days: int,
    feedback_retention_days: int,
) -> RetentionResult:
    current = _as_utc(now)
    # TimestampMixin stores UTC without a timezone; asyncpg rejects aware binds.
    queue_cutoff = (current - timedelta(days=max(queue_retention_days, 0))).replace(tzinfo=None)
    audit_cutoff = current - timedelta(days=max(audit_retention_days, 0))
    feedback_cutoff = current - timedelta(days=max(feedback_retention_days, 0))
    publications, protected_queues = _publication_evidence()

    queue_rows = (
        await db.execute(
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.status.in_([QUEUE_SUCCEEDED, QUEUE_DEAD_LETTERED, QUEUE_CANCELLED]))
            .where(ChannelOpsQueueItem.created_at < queue_cutoff)
            .where(ChannelOpsQueueItem.id.not_in(select(protected_queues.c.id)))
        )
    ).scalars().all()
    audit_rows = (
        await db.execute(
            select(AgentTickAudit)
            .where(AgentTickAudit.started_at < audit_cutoff)
            .where(AgentTickAudit.replay_status == "legacy_unreplayable")
        )
    ).scalars().all()
    feedback_rows = (
        await db.execute(
            select(FeedbackSnapshot).where(FeedbackSnapshot.collected_at < feedback_cutoff)
            .where(FeedbackSnapshot.publication_id.not_in(select(publications.c.id)))
        )
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
