from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.schedule import RuntimeSchedule
from app.services.schedule_service import VideoScheduleState, get_or_create_and_lock_runtime_schedule


class JobExecutionAuthorityBlocked(RuntimeError):
    """The durable job/node authority no longer permits execution."""


@dataclass(frozen=True)
class LockedJobExecutionAuthority:
    channel: ChannelProfile | None
    schedule: RuntimeSchedule
    task: ProductionTask | None
    job: Job
    node: NodeExecution | None


def require_active_execution_authority(
    authority: LockedJobExecutionAuthority,
    *,
    job_statuses: set[JobStatus],
    node_statuses: set[NodeStatus] | None = None,
) -> None:
    if authority.channel is not None and (
        not authority.channel.enabled or authority.channel.halted_at is not None
    ):
        raise JobExecutionAuthorityBlocked("channel execution is blocked")
    if authority.task is not None and authority.task.state != "producing":
        raise JobExecutionAuthorityBlocked("production task is not producing")
    if authority.schedule.state != VideoScheduleState.OPEN.value:
        raise JobExecutionAuthorityBlocked("runtime schedule is not open")
    try:
        guarded_job_id = authority.schedule.guarded_job_id
    except AttributeError as exc:
        raise JobExecutionAuthorityBlocked(
            "runtime schedule guarded authority is unavailable"
        ) from exc
    if guarded_job_id is not None and authority.job.id != guarded_job_id:
        raise JobExecutionAuthorityBlocked("job does not hold guarded schedule authority")
    if authority.job.status not in job_statuses:
        raise JobExecutionAuthorityBlocked("job status no longer permits execution")
    if node_statuses is not None:
        if authority.node is None or authority.node.status not in node_statuses:
            raise JobExecutionAuthorityBlocked("node status no longer permits execution")


async def lock_job_execution_authority(
    db: AsyncSession,
    job_id: uuid.UUID,
    *,
    node_execution_id: uuid.UUID | None = None,
    lock_all_nodes: bool = False,
) -> LockedJobExecutionAuthority:
    """Lock shared execution authority in quarantine-compatible order.

    ChannelOps jobs lock channel -> schedule -> task -> job -> node. Jobs that
    are not linked to ChannelOps lock schedule -> job -> node.
    """

    task_refs = list(
        (
            await db.execute(
                select(ProductionTask.id, ProductionTask.channel_profile_id)
                .where(ProductionTask.job_id == job_id)
                .order_by(ProductionTask.id)
                .limit(2)
            )
        ).all()
    )
    if len(task_refs) > 1:
        raise JobExecutionAuthorityBlocked("job is linked to multiple production tasks")

    channel = None
    task = None
    if task_refs:
        task_id, discovered_channel_id = task_refs[0]
        channel = (
            await db.execute(
                select(ChannelProfile)
                .where(ChannelProfile.id == discovered_channel_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if channel is None:
            raise JobExecutionAuthorityBlocked("production task channel was not found")

    schedule, _created = await get_or_create_and_lock_runtime_schedule(db)

    if task_refs:
        task_id, discovered_channel_id = task_refs[0]
        task = (
            await db.execute(
                select(ProductionTask)
                .where(ProductionTask.id == task_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if (
            task is None
            or task.job_id != job_id
            or task.channel_profile_id != discovered_channel_id
            or channel is None
            or task.channel_profile_id != channel.id
        ):
            raise JobExecutionAuthorityBlocked("production task authority changed while locking")

    job = (
        await db.execute(
            select(Job)
            .where(Job.id == job_id)
            .options(selectinload(Job.node_executions))
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()
    if job is None:
        raise JobExecutionAuthorityBlocked("job was not found")

    node = None
    if lock_all_nodes:
        nodes = list(
            (
                await db.execute(
                    select(NodeExecution)
                    .where(NodeExecution.job_id == job.id)
                    .order_by(NodeExecution.id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars().all()
        )
        if node_execution_id is not None:
            node = next((item for item in nodes if item.id == node_execution_id), None)
            if node is None:
                raise JobExecutionAuthorityBlocked("node execution authority changed while locking")
        await db.refresh(job, attribute_names=["node_executions"])
    elif node_execution_id is not None:
        node = (
            await db.execute(
                select(NodeExecution)
                .where(NodeExecution.id == node_execution_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if node is None or node.job_id != job.id:
            raise JobExecutionAuthorityBlocked("node execution authority changed while locking")

    return LockedJobExecutionAuthority(
        channel=channel,
        schedule=schedule,
        task=task,
        job=job,
        node=node,
    )
