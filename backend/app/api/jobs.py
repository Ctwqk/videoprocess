from __future__ import annotations
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.db import get_db
from app.api.job_helpers import create_jobs_or_400
from app.schemas.job import (
    JobCreate, BatchJobCreate, JobResponse, JobDetailResponse, JobListResponse,
)
from app.services.job_service import (
    create_job, create_job_from_snapshot, get_job, list_jobs, cancel_job, delete_job,
)
from app.services.job_runtime import (
    start_or_defer_jobs,
    to_job_detail_response,
    to_job_response,
)

router = APIRouter(prefix="/api/v1", tags=["jobs"])


@router.post("/jobs", response_model=JobDetailResponse, status_code=201)
async def submit_job(data: JobCreate, db: AsyncSession = Depends(get_db)):
    try:
        job = await create_job(db, uuid.UUID(data.pipeline_id), input_overrides=data.inputs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    await start_or_defer_jobs(db, [job])
    return await to_job_detail_response(db, job)


@router.get("/jobs", response_model=JobListResponse)
async def list_all(
    skip: int = 0,
    limit: int = Query(default=50, le=100),
    pipeline_id: str | None = None,
    status: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    pid = uuid.UUID(pipeline_id) if pipeline_id else None
    items, total = await list_jobs(db, skip, limit, pid, status)
    return JobListResponse(
        items=[to_job_response(j) for j in items],
        total=total,
    )


@router.get("/jobs/{job_id}", response_model=JobDetailResponse)
async def get_one(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    job = await get_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return await to_job_detail_response(db, job)


@router.post("/jobs/{job_id}/cancel", response_model=JobDetailResponse)
async def cancel(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    job = await cancel_job(db, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return await to_job_detail_response(db, job)


@router.post("/jobs/batch", response_model=list[JobDetailResponse], status_code=201)
async def submit_batch(data: BatchJobCreate, db: AsyncSession = Depends(get_db)):
    """Submit multiple jobs for the same pipeline with different inputs."""
    jobs = await create_jobs_or_400(db, uuid.UUID(data.pipeline_id), data.inputs)

    await start_or_defer_jobs(db, jobs)
    return [await to_job_detail_response(db, job) for job in jobs]


@router.post("/jobs/{job_id}/rerun", response_model=JobDetailResponse, status_code=201)
async def rerun(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    """Re-run a job by creating a new job from the same pipeline."""
    old_job = await get_job(db, job_id)
    if not old_job:
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        new_job = await create_job_from_snapshot(db, old_job.pipeline_id, old_job.pipeline_snapshot)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    await start_or_defer_jobs(db, [new_job])
    return await to_job_detail_response(db, new_job)


@router.delete("/jobs/{job_id}", status_code=200)
async def delete_one(job_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    try:
        deleted = await delete_job(db, job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if not deleted:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"status": "deleted"}
