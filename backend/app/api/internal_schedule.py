from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.schemas.schedule import VideoScheduleStatusResponse
from app.services.job_runtime import start_jobs_background
from app.services.schedule_service import (
    GuardedScheduleOpenConflict,
    VideoScheduleState,
    build_video_schedule_status,
    open_video_schedule_for_job,
    release_waiting_video_jobs,
    set_video_schedule_state,
)

router = APIRouter(prefix="/internal/schedule/video", tags=["internal-schedule"])


@router.get("/status", response_model=VideoScheduleStatusResponse)
async def video_schedule_status(db: AsyncSession = Depends(get_db)):
    return await build_video_schedule_status(db)


@router.post("/open", response_model=VideoScheduleStatusResponse)
async def video_schedule_open(
    expected_job_id: uuid.UUID | None = None,
    db: AsyncSession = Depends(get_db),
):
    released_job_ids: list[uuid.UUID]
    if expected_job_id is None:
        await set_video_schedule_state(db, VideoScheduleState.OPEN, updated_by="internal_api")
        released_job_ids = [uuid.UUID(job_id) for job_id in await release_waiting_video_jobs(db)]
    else:
        try:
            released_job_ids = await open_video_schedule_for_job(db, expected_job_id)
        except GuardedScheduleOpenConflict as exc:
            raise HTTPException(status_code=409, detail="guarded_schedule_open_conflict") from exc
    if released_job_ids:
        await start_jobs_background(released_job_ids)
    return await build_video_schedule_status(db, released_jobs=len(released_job_ids))


@router.post("/drain", response_model=VideoScheduleStatusResponse)
async def video_schedule_drain(db: AsyncSession = Depends(get_db)):
    await set_video_schedule_state(db, VideoScheduleState.DRAINING, updated_by="internal_api")
    return await build_video_schedule_status(db)


@router.post("/close", response_model=VideoScheduleStatusResponse)
async def video_schedule_close(db: AsyncSession = Depends(get_db)):
    await set_video_schedule_state(db, VideoScheduleState.CLOSED, updated_by="internal_api")
    return await build_video_schedule_status(db)
