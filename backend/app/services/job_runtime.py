from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.artifact import Artifact
from app.models.job import Job
from app.schemas.job import JobDetailResponse, JobResponse, NodeExecutionResponse
from app.services.schedule_service import (
    VideoScheduleState,
    get_video_schedule_state,
    park_jobs_for_window,
)


async def start_jobs_background(job_ids: Iterable[uuid.UUID]) -> None:
    from app.orchestrator.engine import engine

    for job_id in job_ids:
        asyncio.create_task(engine.start_job(uuid.UUID(str(job_id))))


async def start_or_defer_jobs(db: AsyncSession, jobs: Iterable[Job]) -> VideoScheduleState:
    materialized_jobs = list(jobs)
    if not materialized_jobs:
        return VideoScheduleState.OPEN

    schedule_state = await get_video_schedule_state(db)
    if schedule_state == VideoScheduleState.OPEN:
        await start_jobs_background(job.id for job in materialized_jobs)
        return schedule_state

    await park_jobs_for_window(db, materialized_jobs)
    return schedule_state


def to_job_response(job) -> JobResponse:
    return JobResponse(
        id=str(job.id),
        pipeline_id=str(job.pipeline_id),
        status=job.status.value,
        submitted_at=job.submitted_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error_message=job.error_message,
        submitted_by=job.submitted_by,
        retry_count=job.retry_count,
    )


async def _load_output_artifacts(db: AsyncSession, job) -> dict[uuid.UUID, Artifact]:
    artifact_ids = [ne.output_artifact_id for ne in job.node_executions if ne.output_artifact_id]
    if not artifact_ids:
        return {}

    result = await db.execute(select(Artifact).where(Artifact.id.in_(artifact_ids)))
    artifacts = result.scalars().all()
    return {artifact.id: artifact for artifact in artifacts}


async def to_job_detail_response(db: AsyncSession, job) -> JobDetailResponse:
    artifacts_by_id = await _load_output_artifacts(db, job)
    return JobDetailResponse(
        **to_job_response(job).model_dump(),
        pipeline_snapshot=job.pipeline_snapshot,
        execution_plan=job.execution_plan,
        node_executions=[
            NodeExecutionResponse(
                id=str(ne.id),
                node_id=ne.node_id,
                node_type=ne.node_type,
                node_label=ne.node_label,
                status=ne.status.value,
                progress=ne.progress,
                worker_id=ne.worker_id,
                queued_at=ne.queued_at,
                started_at=ne.started_at,
                completed_at=ne.completed_at,
                error_message=ne.error_message,
                input_artifact_ids=[str(a) for a in (ne.input_artifact_ids or [])],
                output_artifact_id=str(ne.output_artifact_id) if ne.output_artifact_id else None,
                output_artifact_filename=(
                    artifacts_by_id[ne.output_artifact_id].filename
                    if ne.output_artifact_id and ne.output_artifact_id in artifacts_by_id
                    else None
                ),
                output_artifact_media_info=(
                    artifacts_by_id[ne.output_artifact_id].media_info
                    if ne.output_artifact_id and ne.output_artifact_id in artifacts_by_id
                    else None
                ),
            )
            for ne in job.node_executions
        ],
    )
