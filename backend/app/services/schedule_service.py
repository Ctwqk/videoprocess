from __future__ import annotations

import enum
import uuid
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import CursorResult
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
GUARDED_OPEN_JOB_STATUSES = {*ACTIVE_JOB_STATUSES, JobStatus.WAITING_WINDOW}
GUARDED_OPEN_NODE_STATUSES = {NodeStatus.QUEUED, NodeStatus.RUNNING}


class GuardedScheduleOpenConflict(RuntimeError):
    pass


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


def should_defer_job_start(
    job: Job,
    state: VideoScheduleState,
    guarded_job_id: uuid.UUID | None = None,
) -> bool:
    if state == VideoScheduleState.OPEN:
        return guarded_job_id is not None and job.id != guarded_job_id
    if state == VideoScheduleState.CLOSED:
        return True
    if job.status == JobStatus.WAITING_WINDOW:
        return True
    return is_job_fresh_submission(job)


async def _get_or_create_runtime_schedule(
    db: AsyncSession,
    service_name: str,
    default_state: VideoScheduleState,
    *,
    commit: bool = True,
) -> RuntimeSchedule:
    schedule, _created = await get_or_create_and_lock_runtime_schedule(
        db,
        service_name=service_name,
        default_state=default_state,
    )
    if not commit:
        return schedule
    await db.commit()
    await db.refresh(schedule)
    return schedule


async def get_or_create_and_lock_runtime_schedule(
    db: AsyncSession,
    *,
    service_name: str = VIDEO_SCHEDULE_SERVICE,
    default_state: VideoScheduleState | None = None,
) -> tuple[RuntimeSchedule, bool]:
    """Create if needed and lock schedule authority without committing.

    Lock order: an existing channel authority fence owns channel first; Python
    then locks schedule followed by task/job-specific rows. Callers that may be
    linked to ChannelOps must acquire the channel fence before this helper.
    """
    resolved_default = default_state or default_video_schedule_state()
    values = {
        "service_name": service_name,
        "state": resolved_default.value,
        "updated_by": "system",
    }
    dialect_name = db.get_bind().dialect.name
    if dialect_name == "postgresql":
        insert_result = cast(
            CursorResult[Any],
            await db.execute(
                postgresql_insert(RuntimeSchedule)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[RuntimeSchedule.service_name])
            ),
        )
    elif dialect_name == "sqlite":
        insert_result = cast(
            CursorResult[Any],
            await db.execute(
                sqlite_insert(RuntimeSchedule)
                .values(**values)
                .on_conflict_do_nothing(index_elements=[RuntimeSchedule.service_name])
            ),
        )
    else:
        raise RuntimeError(f"Unsupported runtime schedule dialect: {dialect_name}")
    schedule = (
        await db.execute(
            select(RuntimeSchedule)
            .where(RuntimeSchedule.service_name == service_name)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    return schedule, insert_result.rowcount == 1


async def get_video_schedule_record(db: AsyncSession, *, commit: bool = True) -> RuntimeSchedule:
    return await _get_or_create_runtime_schedule(
        db,
        VIDEO_SCHEDULE_SERVICE,
        default_video_schedule_state(),
        commit=commit,
    )


async def get_video_schedule_state(db: AsyncSession, *, commit: bool = True) -> VideoScheduleState:
    schedule = await get_video_schedule_record(db, commit=commit)
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
    schedule, _created = await get_or_create_and_lock_runtime_schedule(db)
    schedule.state = state.value
    schedule.guarded_job_id = None
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


async def park_jobs_for_window(
    db: AsyncSession,
    jobs: list[Job],
    *,
    commit: bool = True,
) -> list[Job]:
    if not jobs:
        return []
    for job in jobs:
        job.status = JobStatus.WAITING_WINDOW
        job.error_message = None
        job.completed_at = None
    if not commit:
        await db.flush()
        return jobs
    await db.commit()
    for job in jobs:
        await db.refresh(job, attribute_names=["node_executions"])
    return jobs


async def defer_job_until_next_window(
    db: AsyncSession,
    job: Job,
    *,
    commit: bool = True,
) -> Job:
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
    if not commit:
        await db.flush()
        return job
    await db.commit()
    await db.refresh(job, attribute_names=["node_executions"])
    return job


async def list_waiting_video_jobs(db: AsyncSession) -> list[Job]:
    stmt = (
        select(Job)
        .where(Job.status == JobStatus.WAITING_WINDOW)
        .where(Job.orchestrator_owner == "python")
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


async def open_video_schedule_for_job(
    db: AsyncSession,
    expected_job_id: uuid.UUID,
) -> list[uuid.UUID]:
    async with db.begin():
        schedule, _created = await get_or_create_and_lock_runtime_schedule(db)
        if schedule.state != VideoScheduleState.CLOSED.value:
            raise GuardedScheduleOpenConflict("video schedule is not closed")

        jobs = list(
            (
                await db.execute(
                    select(Job)
                    .where(Job.status.in_(GUARDED_OPEN_JOB_STATUSES))
                    .order_by(Job.id.asc())
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
        active_nodes = list(
            (
                await db.execute(
                    select(NodeExecution)
                    .where(NodeExecution.status.in_(GUARDED_OPEN_NODE_STATUSES))
                    .order_by(NodeExecution.id.asc())
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            ).scalars()
        )
        expected_jobs = [
            job
            for job in jobs
            if job.id == expected_job_id
            and job.status == JobStatus.WAITING_WINDOW
            and job.orchestrator_owner == "python"
        ]
        if len(expected_jobs) != 1 or len(jobs) != 1 or active_nodes:
            raise GuardedScheduleOpenConflict("guarded schedule open authority mismatch")

        expected_job = expected_jobs[0]
        schedule.state = VideoScheduleState.OPEN.value
        schedule.guarded_job_id = expected_job_id
        schedule.updated_by = "internal_api_guarded"
        expected_job.status = JobStatus.PENDING
        expected_job.error_message = None
        expected_job.completed_at = None

    return [expected_job_id]


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
        .where(Job.orchestrator_owner == "python")
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
        guarded_job_id=str(schedule.guarded_job_id) if schedule.guarded_job_id else None,
        waiting_jobs=waiting_jobs,
        active_jobs=active_jobs,
        queued_nodes=queued_nodes,
        running_nodes=running_nodes,
        updated_at=schedule.updated_at,
        updated_by=schedule.updated_by,
        released_jobs=released_jobs,
    )
