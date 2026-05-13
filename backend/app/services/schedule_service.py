from __future__ import annotations

import enum

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.schedule import RuntimeSchedule
from app.schemas.schedule import VideoScheduleStatusResponse

VIDEO_SCHEDULE_SERVICE = "videoprocess"


class VideoScheduleState(str, enum.Enum):
    OPEN = "OPEN"
    DRAINING = "DRAINING"
    CLOSED = "CLOSED"


ACTIVE_JOB_STATUSES = {
    JobStatus.PENDING,
    JobStatus.VALIDATING,
    JobStatus.PLANNING,
    JobStatus.RUNNING,
}
ACTIVE_NODE_STATUSES = {NodeStatus.PENDING, NodeStatus.QUEUED, NodeStatus.RUNNING}


def default_video_schedule_state() -> VideoScheduleState:
    raw = (settings.video_schedule_default_state or VideoScheduleState.OPEN.value).upper()
    try:
        return VideoScheduleState(raw)
    except ValueError:
        return VideoScheduleState.OPEN


def is_job_fresh_submission(job: Job) -> bool:
    if job.started_at is not None:
        return False
    return all(node.status == NodeStatus.PENDING for node in job.node_executions)


def should_defer_job_start(job: Job, state: VideoScheduleState) -> bool:
    if state == VideoScheduleState.OPEN:
        return False
    if state == VideoScheduleState.CLOSED:
        return True
    if job.status == JobStatus.WAITING_WINDOW:
        return True
    return is_job_fresh_submission(job)


async def _get_or_create_runtime_schedule(
    db: AsyncSession,
    service_name: str,
    default_state: VideoScheduleState,
) -> RuntimeSchedule:
    schedule = await db.get(RuntimeSchedule, service_name)
    if schedule:
        return schedule

    schedule = RuntimeSchedule(
        service_name=service_name,
        state=default_state.value,
        updated_by="system",
    )
    db.add(schedule)
    await db.commit()
    await db.refresh(schedule)
    return schedule


async def get_video_schedule_record(db: AsyncSession) -> RuntimeSchedule:
    return await _get_or_create_runtime_schedule(
        db,
        VIDEO_SCHEDULE_SERVICE,
        default_video_schedule_state(),
    )


async def get_video_schedule_state(db: AsyncSession) -> VideoScheduleState:
    schedule = await get_video_schedule_record(db)
    try:
        return VideoScheduleState(schedule.state)
    except ValueError:
        return default_video_schedule_state()


async def set_video_schedule_state(
    db: AsyncSession,
    state: VideoScheduleState,
    *,
    updated_by: str = "system",
) -> RuntimeSchedule:
    schedule = await get_video_schedule_record(db)
    schedule.state = state.value
    schedule.updated_by = updated_by
    await db.commit()
    await db.refresh(schedule)
    return schedule


async def park_job_for_window(db: AsyncSession, job: Job) -> Job:
    job.status = JobStatus.WAITING_WINDOW
    job.error_message = None
    job.completed_at = None
    await db.commit()
    await db.refresh(job, attribute_names=["node_executions"])
    return job


async def park_jobs_for_window(db: AsyncSession, jobs: list[Job]) -> list[Job]:
    if not jobs:
        return []
    for job in jobs:
        job.status = JobStatus.WAITING_WINDOW
        job.error_message = None
        job.completed_at = None
    await db.commit()
    for job in jobs:
        await db.refresh(job, attribute_names=["node_executions"])
    return jobs


async def defer_job_until_next_window(db: AsyncSession, job: Job) -> Job:
    for node in job.node_executions:
        if node.status in ACTIVE_NODE_STATUSES:
            node.status = NodeStatus.PENDING
            node.worker_id = None
            node.queued_at = None
            node.started_at = None
            node.completed_at = None
            node.progress = 0
            node.error_message = None
            node.error_trace = None
    job.status = JobStatus.WAITING_WINDOW
    job.error_message = None
    job.completed_at = None
    await db.commit()
    await db.refresh(job, attribute_names=["node_executions"])
    return job


async def list_waiting_video_jobs(db: AsyncSession) -> list[Job]:
    stmt = (
        select(Job)
        .where(Job.status == JobStatus.WAITING_WINDOW)
        .options(selectinload(Job.node_executions))
        .order_by(Job.submitted_at.asc(), Job.id.asc())
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def release_waiting_video_jobs(db: AsyncSession) -> list[str]:
    jobs = await list_waiting_video_jobs(db)
    if not jobs:
        return []

    for job in jobs:
        job.status = JobStatus.PENDING
        job.error_message = None
        job.completed_at = None
    await db.commit()
    return [str(job.id) for job in jobs]


async def load_video_jobs_for_recovery(db: AsyncSession) -> list[Job]:
    stmt = (
        select(Job)
        .where(
            Job.status.in_(
                [
                    JobStatus.PENDING,
                    JobStatus.WAITING_WINDOW,
                    JobStatus.RUNNING,
                    JobStatus.PLANNING,
                ]
            )
        )
        .options(selectinload(Job.node_executions))
    )
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def build_video_schedule_status(
    db: AsyncSession,
    *,
    released_jobs: int = 0,
) -> VideoScheduleStatusResponse:
    schedule = await get_video_schedule_record(db)

    waiting_jobs = int(
        (await db.execute(select(func.count()).select_from(Job).where(Job.status == JobStatus.WAITING_WINDOW))).scalar()
        or 0
    )
    active_jobs = int(
        (
            await db.execute(
                select(func.count()).select_from(Job).where(Job.status.in_(list(ACTIVE_JOB_STATUSES)))
            )
        ).scalar()
        or 0
    )
    queued_nodes = int(
        (
            await db.execute(
                select(func.count()).select_from(NodeExecution).where(NodeExecution.status == NodeStatus.QUEUED)
            )
        ).scalar()
        or 0
    )
    running_nodes = int(
        (
            await db.execute(
                select(func.count()).select_from(NodeExecution).where(NodeExecution.status == NodeStatus.RUNNING)
            )
        ).scalar()
        or 0
    )

    return VideoScheduleStatusResponse(
        service_name=schedule.service_name,
        state=schedule.state,
        waiting_jobs=waiting_jobs,
        active_jobs=active_jobs,
        queued_nodes=queued_nodes,
        running_nodes=running_nodes,
        updated_at=schedule.updated_at,
        updated_by=schedule.updated_by,
        released_jobs=released_jobs,
    )
