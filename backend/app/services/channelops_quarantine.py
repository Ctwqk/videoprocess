from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent.constants import TERMINAL_TASK_STATES
from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    ProductionTask,
    PublicationRecord,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.schedule import RuntimeSchedule
from app.services.schedule_service import VIDEO_SCHEDULE_SERVICE, VideoScheduleState


QUARANTINE_REASON = "operator_quarantine_before_unlisted_canary"
_NONTERMINAL_JOB_STATUSES = {
    JobStatus.PENDING,
    JobStatus.WAITING_WINDOW,
    JobStatus.VALIDATING,
    JobStatus.PLANNING,
    JobStatus.RUNNING,
}
_NONTERMINAL_NODE_STATUSES = {
    NodeStatus.PENDING,
    NodeStatus.QUEUED,
    NodeStatus.RUNNING,
}
_NONTERMINAL_QUEUE_STATUSES = {"queued", "running"}


class UnknownChannelError(ValueError):
    """Raised when an operator names a channel that does not exist."""


async def quarantine_channelops_backlog(
    db: AsyncSession,
    channel_id: uuid.UUID | str,
    *,
    apply: bool = False,
    now: datetime | None = None,
    reason: str = QUARANTINE_REASON,
    close_schedule: bool = False,
) -> dict[str, Any]:
    """Report or quarantine one channel's runnable backlog in one transaction."""
    if not reason or len(reason) > 255:
        raise ValueError("reason must be between 1 and 255 characters")

    resolved_channel_id = _uuid(channel_id)
    changed_at = now or datetime.now(timezone.utc)

    async with db.begin():
        channel_stmt = select(ChannelProfile).where(ChannelProfile.id == resolved_channel_id)
        if apply:
            channel_stmt = channel_stmt.with_for_update()
        channel = (await db.execute(channel_stmt)).scalar_one_or_none()
        if channel is None:
            raise UnknownChannelError(f"Unknown channel: {resolved_channel_id}")

        task_stmt = select(ProductionTask).where(ProductionTask.channel_profile_id == channel.id)
        queue_stmt = select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.channel_profile_id == channel.id)
        if apply:
            task_stmt = task_stmt.with_for_update()
            queue_stmt = queue_stmt.with_for_update()
        tasks = list((await db.execute(task_stmt)).scalars().all())
        queue_items = list((await db.execute(queue_stmt)).scalars().all())

        task_ids = {task.id for task in tasks}
        publications = await _publications_for_tasks(db, task_ids, apply=apply)
        feedback = await _feedback_for_publications(
            db,
            {publication.id for publication in publications},
            apply=apply,
        )

        changed_tasks = [
            task
            for task in tasks
            if task.state not in TERMINAL_TASK_STATES
            and not _already_quarantined(task, reason)
        ]
        changed_task_ids = {task.id for task in changed_tasks}
        retained_tasks = [task for task in tasks if task.id not in changed_task_ids]

        linked_job_ids = {task.job_id for task in tasks if task.job_id is not None}
        protected_job_ids = {task.job_id for task in retained_tasks if task.job_id is not None}
        jobs = await _jobs_by_id(db, linked_job_ids, apply=apply)
        changed_jobs = [
            job
            for job in jobs
            if job.id not in protected_job_ids and job.status in _NONTERMINAL_JOB_STATUSES
        ]
        changed_job_ids = {job.id for job in changed_jobs}
        retained_jobs = [job for job in jobs if job.id not in changed_job_ids]

        nodes = await _nodes_for_jobs(db, linked_job_ids, apply=apply)
        changed_nodes = [
            node
            for node in nodes
            if node.job_id in changed_job_ids and node.status in _NONTERMINAL_NODE_STATUSES
        ]
        changed_node_ids = {node.id for node in changed_nodes}
        retained_nodes = [node for node in nodes if node.id not in changed_node_ids]
        changed_queue_items = [item for item in queue_items if item.status in _NONTERMINAL_QUEUE_STATUSES]
        changed_queue_ids = {item.id for item in changed_queue_items}
        retained_queue_items = [item for item in queue_items if item.id not in changed_queue_ids]

        schedule = None
        previous_schedule_state = None
        if close_schedule:
            if apply:
                schedule, schedule_created = await _create_or_lock_runtime_schedule(
                    db,
                    reason=reason,
                    changed_at=changed_at,
                )
                previous_schedule_state = None if schedule_created else schedule.state
            else:
                schedule_stmt = select(RuntimeSchedule).where(
                    RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE
                )
                schedule = (await db.execute(schedule_stmt)).scalar_one_or_none()
                previous_schedule_state = schedule.state if schedule is not None else None
        schedule_changed = (
            close_schedule and previous_schedule_state != VideoScheduleState.CLOSED.value
        )
        final_schedule_state = (
            VideoScheduleState.CLOSED.value if close_schedule else previous_schedule_state
        )

        channel_changed = channel.halted_at is None or channel.halt_reason != reason
        if apply:
            _apply_channel_halt(channel, changed_at, reason)
            for task in changed_tasks:
                _hold_task(task, changed_at, reason)
            for job in changed_jobs:
                _cancel_job(job, changed_at, reason)
            for node in changed_nodes:
                _cancel_node(node, changed_at, reason)
            for item in changed_queue_items:
                _dead_letter_queue_item(item, changed_at, reason)
            if close_schedule:
                assert schedule is not None
                schedule.state = VideoScheduleState.CLOSED.value
                schedule.updated_by = reason
                schedule.updated_at = changed_at

        changed_ids = {
            "channel_ids": [str(channel.id)] if channel_changed else [],
            "task_ids": _sorted_ids(changed_tasks),
            "job_ids": _sorted_ids(changed_jobs),
            "node_execution_ids": _sorted_ids(changed_nodes),
            "queue_item_ids": _sorted_ids(changed_queue_items),
        }
        retained_ids = {
            "task_ids": _sorted_ids(retained_tasks),
            "job_ids": _sorted_ids(retained_jobs),
            "node_execution_ids": _sorted_ids(retained_nodes),
            "queue_item_ids": _sorted_ids(retained_queue_items),
            "publication_ids": _sorted_ids(publications),
            "feedback_snapshot_ids": _sorted_ids(feedback),
        }
        return {
            "channel_id": str(channel.id),
            "applied": apply,
            "reason": reason,
            "generated_at": changed_at.isoformat(),
            "schedule": {
                "requested_close": close_schedule,
                "changed": schedule_changed,
                "previous_state": previous_schedule_state,
                "final_state": final_schedule_state,
            },
            "changed_ids": changed_ids,
            "retained_ids": retained_ids,
            "counts": {
                "changed": {key: len(value) for key, value in changed_ids.items()},
                "retained": {key: len(value) for key, value in retained_ids.items()},
            },
        }


async def _create_or_lock_runtime_schedule(
    db: AsyncSession,
    *,
    reason: str,
    changed_at: datetime,
) -> tuple[RuntimeSchedule, bool]:
    dialect_name = db.get_bind().dialect.name
    insert_values = {
        "service_name": VIDEO_SCHEDULE_SERVICE,
        "state": VideoScheduleState.CLOSED.value,
        "updated_by": reason,
        "updated_at": changed_at,
    }
    if dialect_name == "postgresql":
        insert_stmt = postgresql_insert(RuntimeSchedule).values(**insert_values)
    elif dialect_name == "sqlite":
        insert_stmt = sqlite_insert(RuntimeSchedule).values(**insert_values)
    else:
        raise RuntimeError(f"Unsupported runtime schedule dialect: {dialect_name}")

    result = await db.execute(
        insert_stmt.on_conflict_do_nothing(index_elements=[RuntimeSchedule.service_name])
    )
    schedule_stmt = (
        select(RuntimeSchedule)
        .where(RuntimeSchedule.service_name == VIDEO_SCHEDULE_SERVICE)
        .with_for_update()
    )
    schedule = (await db.execute(schedule_stmt)).scalar_one()
    return schedule, result.rowcount == 1


async def _publications_for_tasks(
    db: AsyncSession,
    task_ids: set[uuid.UUID],
    *,
    apply: bool,
) -> list[PublicationRecord]:
    if not task_ids:
        return []
    stmt = select(PublicationRecord).where(PublicationRecord.production_task_id.in_(task_ids))
    if apply:
        stmt = stmt.with_for_update()
    return list((await db.execute(stmt)).scalars().all())


async def _feedback_for_publications(
    db: AsyncSession,
    publication_ids: set[uuid.UUID],
    *,
    apply: bool,
) -> list[FeedbackSnapshot]:
    if not publication_ids:
        return []
    stmt = select(FeedbackSnapshot).where(FeedbackSnapshot.publication_id.in_(publication_ids))
    if apply:
        stmt = stmt.with_for_update()
    return list((await db.execute(stmt)).scalars().all())


async def _jobs_by_id(db: AsyncSession, job_ids: set[uuid.UUID], *, apply: bool) -> list[Job]:
    if not job_ids:
        return []
    stmt = select(Job).where(Job.id.in_(job_ids))
    if apply:
        stmt = stmt.with_for_update()
    return list((await db.execute(stmt)).scalars().all())


async def _nodes_for_jobs(
    db: AsyncSession,
    job_ids: set[uuid.UUID],
    *,
    apply: bool,
) -> list[NodeExecution]:
    if not job_ids:
        return []
    stmt = select(NodeExecution).where(NodeExecution.job_id.in_(job_ids))
    if apply:
        stmt = stmt.with_for_update()
    return list((await db.execute(stmt)).scalars().all())


def _already_quarantined(task: ProductionTask, reason: str) -> bool:
    return (
        task.state == "held"
        and task.blocked_by_guard == reason
        and task.failure_reason == reason
    )


def _apply_channel_halt(channel: ChannelProfile, now: datetime, reason: str) -> None:
    if channel.halted_at is None:
        channel.halted_at = now
    channel.halt_reason = reason


def _hold_task(task: ProductionTask, now: datetime, reason: str) -> None:
    previous_state = task.state
    task.state = "held"
    task.state_updated_at = now
    task.blocked_by_guard = reason
    task.failure_reason = reason
    task.transition_history_json = [
        *list(task.transition_history_json or []),
        {
            "from": previous_state,
            "to": "held",
            "actor": reason,
            "at": now.isoformat(),
        },
    ]


def _cancel_job(job: Job, now: datetime, reason: str = QUARANTINE_REASON) -> None:
    job.status = JobStatus.CANCELLED
    job.completed_at = _naive_utc(now)
    job.error_message = reason


def _cancel_node(node: NodeExecution, now: datetime, reason: str = QUARANTINE_REASON) -> None:
    node.status = NodeStatus.CANCELLED
    node.completed_at = _naive_utc(now)
    node.worker_id = None
    node.error_message = reason


def _dead_letter_queue_item(item: ChannelOpsQueueItem, now: datetime, reason: str) -> None:
    item.status = "dead_lettered"
    item.last_error = reason
    item.dead_letter_at = now
    item.locked_at = None
    item.locked_by = None


def _uuid(value: uuid.UUID | str) -> uuid.UUID:
    return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _sorted_ids(rows: list[Any]) -> list[str]:
    return sorted(str(row.id) for row in rows)
