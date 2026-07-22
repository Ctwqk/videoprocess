import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.artifacts import router as artifacts_router
from app.api.assets import router as assets_router
from app.api.autoflow import router as autoflow_router
from app.api.channel_agent import router as channel_agent_router
from app.api.internal_schedule import router as internal_schedule_router
from app.api.jobs import router as jobs_router
from app.api.llm import router as llm_router
from app.api.materials import router as materials_router
from app.api.node_types import router as node_types_router
from app.api.pipelines import router as pipelines_router
from app.config import settings
from app.db import async_session
from app.models.job import Job, JobStatus, NodeStatus
from app.orchestrator.engine import engine
from app.orchestrator.event_listener import event_listener
from app.services.schedule_service import (
    VideoScheduleState,
    default_video_schedule_state,
    defer_job_until_next_window,
    get_video_schedule_record,
    load_video_jobs_for_recovery,
    should_defer_job_start,
)

logger = logging.getLogger(__name__)
STALE_NODE_RECOVERY_THRESHOLD = timedelta(minutes=10)


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def _prepare_job_for_recovery(db, job) -> bool:
    """Reset clearly abandoned QUEUED/RUNNING nodes so startup recovery can redispatch them."""

    now = datetime.now(timezone.utc)
    changed = False

    for node in job.node_executions:
        if node.status not in (NodeStatus.QUEUED, NodeStatus.RUNNING):
            continue

        reference_time = _ensure_utc(node.started_at or node.queued_at or job.started_at or job.submitted_at)
        if not reference_time or (now - reference_time) < STALE_NODE_RECOVERY_THRESHOLD:
            continue

        logger.warning(
            "Resetting stale node %s for job %s from %s to PENDING during startup recovery",
            node.node_id, job.id, node.status.value,
        )
        node.status = NodeStatus.PENDING
        node.worker_id = None
        node.queued_at = None
        node.started_at = None
        node.completed_at = None
        node.progress = 0
        node.error_message = None
        node.input_artifact_ids = []
        changed = True

    if changed and job.status in (JobStatus.RUNNING, JobStatus.PLANNING):
        job.status = JobStatus.PENDING
        job.error_message = None
        job.completed_at = None

    return changed


async def _recover_stale_jobs():
    """On startup, find PENDING/RUNNING jobs and restart them."""
    async with async_session() as db:
        schedule = await get_video_schedule_record(db)
        try:
            schedule_state = VideoScheduleState(schedule.state)
        except ValueError:
            schedule_state = default_video_schedule_state()
        stale_jobs = await load_video_jobs_for_recovery(db)
        jobs_to_restart: list[Job] = []
        for job in stale_jobs:
            if should_defer_job_start(job, schedule_state, schedule.guarded_job_id):
                await defer_job_until_next_window(db, job)
                continue

            changed = await _prepare_job_for_recovery(db, job)
            if changed or job.status in (JobStatus.PENDING, JobStatus.WAITING_WINDOW):
                jobs_to_restart.append(job)
                continue
            await engine._maybe_finalize_job(db, job)
        await db.commit()

    for job in jobs_to_restart:
        logger.info(f"Recovering stale job {job.id} (status={job.status.value})")
        asyncio.create_task(engine.start_job(job.id))


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = None
    if settings.event_listener_enabled:
        task = asyncio.create_task(event_listener())
        logger.info("Orchestrator event listener background task started")
    else:
        logger.info("Orchestrator event listener disabled by configuration")

    if settings.startup_recovery_enabled:
        await _recover_stale_jobs()
    else:
        logger.info("Startup job recovery disabled by configuration")

    yield
    if task is not None:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info("Orchestrator event listener stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="VideoProcess API",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(node_types_router)
    app.include_router(autoflow_router)
    app.include_router(channel_agent_router)
    app.include_router(pipelines_router)
    app.include_router(assets_router)
    app.include_router(artifacts_router)
    app.include_router(jobs_router)
    app.include_router(llm_router)
    app.include_router(materials_router)
    app.include_router(internal_schedule_router)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


app = create_app()
