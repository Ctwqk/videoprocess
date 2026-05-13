from __future__ import annotations

import uuid
from typing import Any

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job
from app.services.job_service import create_job


async def create_jobs_or_400(
    db: AsyncSession,
    pipeline_id: uuid.UUID,
    input_sets: list[dict[str, Any]],
) -> list[Job]:
    jobs: list[Job] = []
    for input_overrides in input_sets:
        try:
            job = await create_job(db, pipeline_id, input_overrides=input_overrides)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        jobs.append(job)
    return jobs
