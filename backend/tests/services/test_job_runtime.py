from __future__ import annotations

import asyncio
import uuid

import pytest

from app.services.job_runtime import start_jobs_background


@pytest.mark.asyncio
async def test_start_jobs_background_waits_for_durable_launch_handoff(monkeypatch):
    job_id = uuid.uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def blocking_start(observed_job_id: uuid.UUID) -> None:
        assert observed_job_id == job_id
        started.set()
        try:
            await release.wait()
        finally:
            finished.set()

    monkeypatch.setattr("app.orchestrator.engine.engine.start_job", blocking_start)
    launch = asyncio.create_task(start_jobs_background([job_id]))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        assert not launch.done()
    finally:
        release.set()
        await asyncio.wait_for(launch, timeout=1)
        await asyncio.wait_for(finished.wait(), timeout=1)


@pytest.mark.asyncio
async def test_start_jobs_background_shields_launch_handoff_from_caller_cancellation(monkeypatch):
    job_id = uuid.uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    child_cancelled = asyncio.Event()

    async def blocking_start(observed_job_id: uuid.UUID) -> None:
        assert observed_job_id == job_id
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            child_cancelled.set()
            raise

    monkeypatch.setattr("app.orchestrator.engine.engine.start_job", blocking_start)
    launch = asyncio.create_task(start_jobs_background([job_id]))
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        launch.cancel()
        await asyncio.sleep(0)

        assert not launch.done()
        assert not child_cancelled.is_set()

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(launch, timeout=1)
        assert not child_cancelled.is_set()
    finally:
        release.set()
        if not launch.done():
            launch.cancel()
        await asyncio.gather(launch, return_exceptions=True)
