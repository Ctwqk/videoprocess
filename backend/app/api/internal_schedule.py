from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.schemas.schedule import VideoScheduleStatusResponse
from app.services.job_runtime import start_jobs_background
from app.services.schedule_service import (
    VideoScheduleState,
    build_video_schedule_status,
    release_waiting_video_jobs,
    set_video_schedule_state,
)

router = APIRouter(prefix="/internal/schedule/video", tags=["internal-schedule"])


@router.get("/status", response_model=VideoScheduleStatusResponse)
async def video_schedule_status(db: AsyncSession = Depends(get_db)):
    return await build_video_schedule_status(db)


@router.post("/open", response_model=VideoScheduleStatusResponse)
async def video_schedule_open(db: AsyncSession = Depends(get_db)):
    await set_video_schedule_state(db, VideoScheduleState.OPEN, updated_by="internal_api")
    released_job_ids = await release_waiting_video_jobs(db)
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
