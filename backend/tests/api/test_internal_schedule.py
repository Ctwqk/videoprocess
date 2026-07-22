from __future__ import annotations

import uuid
from types import SimpleNamespace

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import app.api.internal_schedule as internal_schedule_api
from app.api.internal_schedule import router
from app.db import get_db
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.schedule import RuntimeSchedule


SCHEDULE_PATH = "/internal/schedule/video/open"
SCHEDULE_TABLES = (
    Job.__table__,
    NodeExecution.__table__,
    RuntimeSchedule.__table__,
)


@pytest.fixture
async def schedule_api():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(
            lambda sync_connection: RuntimeSchedule.metadata.create_all(
                sync_connection,
                tables=SCHEDULE_TABLES,
            )
        )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as seed_session:
        app = FastAPI()
        app.include_router(router)

        async def get_request_db():
            async with session_factory() as request_session:
                yield request_session

        app.dependency_overrides[get_db] = get_request_db
        yield SimpleNamespace(
            app=app,
            seed_session=seed_session,
            session_factory=session_factory,
        )
    await engine.dispose()


def _job(*, status: JobStatus, owner: str = "python") -> Job:
    return Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={"version": "1.0", "nodes": [], "edges": []},
        status=status,
        orchestrator_owner=owner,
    )


async def _closed_schedule(session: AsyncSession) -> None:
    session.add(
        RuntimeSchedule(
            service_name="videoprocess",
            state="CLOSED",
            updated_by="test",
        )
    )
    await session.commit()


@pytest.mark.anyio
async def test_legacy_schedule_open_without_expected_job_id_is_unchanged(
    schedule_api,
    monkeypatch: pytest.MonkeyPatch,
):
    session = schedule_api.seed_session
    await _closed_schedule(session)
    first = _job(status=JobStatus.WAITING_WINDOW)
    second = _job(status=JobStatus.WAITING_WINDOW)
    go_job = _job(status=JobStatus.WAITING_WINDOW, owner="go")
    session.add_all((first, second, go_job))
    await session.commit()
    started: list[uuid.UUID] = []

    async def record_started(job_ids):
        started.extend(uuid.UUID(str(job_id)) for job_id in job_ids)

    monkeypatch.setattr(internal_schedule_api, "start_jobs_background", record_started)

    async with AsyncClient(
        transport=ASGITransport(app=schedule_api.app),
        base_url="http://test",
    ) as client:
        response = await client.post(SCHEDULE_PATH)

    assert response.status_code == 200
    assert response.json()["state"] == "OPEN"
    assert response.json()["released_jobs"] == 2
    assert set(started) == {first.id, second.id}
    await session.refresh(go_job)
    assert go_job.status == JobStatus.WAITING_WINDOW


@pytest.mark.anyio
async def test_guarded_schedule_open_releases_and_starts_only_expected_python_job_after_commit(
    schedule_api,
    monkeypatch: pytest.MonkeyPatch,
):
    session = schedule_api.seed_session
    await _closed_schedule(session)
    expected = _job(status=JobStatus.WAITING_WINDOW)
    session.add(expected)
    await session.commit()
    observed_committed_state: list[tuple[str, JobStatus]] = []

    async def record_started(job_ids):
        assert [uuid.UUID(str(job_id)) for job_id in job_ids] == [expected.id]
        async with schedule_api.session_factory() as verifier:
            schedule = await verifier.get(RuntimeSchedule, "videoprocess")
            job = await verifier.get(Job, expected.id)
            assert schedule is not None and job is not None
            observed_committed_state.append((schedule.state, job.status))

    monkeypatch.setattr(internal_schedule_api, "start_jobs_background", record_started)

    async with AsyncClient(
        transport=ASGITransport(app=schedule_api.app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            SCHEDULE_PATH,
            params={"expected_job_id": str(expected.id)},
        )

    assert response.status_code == 200
    assert response.json()["state"] == "OPEN"
    assert response.json()["released_jobs"] == 1
    assert observed_committed_state == [("OPEN", JobStatus.PENDING)]


@pytest.mark.anyio
async def test_guarded_schedule_open_mismatch_returns_409_without_mutation_or_start(
    schedule_api,
    monkeypatch: pytest.MonkeyPatch,
):
    session = schedule_api.seed_session
    await _closed_schedule(session)
    waiting = _job(status=JobStatus.WAITING_WINDOW)
    session.add(waiting)
    await session.commit()
    waiting_id = waiting.id
    started = False

    async def record_started(_job_ids):
        nonlocal started
        started = True

    monkeypatch.setattr(internal_schedule_api, "start_jobs_background", record_started)

    async with AsyncClient(
        transport=ASGITransport(app=schedule_api.app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            SCHEDULE_PATH,
            params={"expected_job_id": str(uuid.uuid4())},
        )

    assert response.status_code == 409
    assert started is False
    session.expire_all()
    schedule = await session.get(RuntimeSchedule, "videoprocess")
    waiting = await session.get(Job, waiting_id)
    assert schedule is not None and schedule.state == "CLOSED"
    assert waiting is not None and waiting.status == JobStatus.WAITING_WINDOW


@pytest.mark.anyio
@pytest.mark.parametrize("blocker", ("waiting_job", "active_job", "queued_node", "running_node"))
async def test_guarded_schedule_open_rejects_other_runnable_work(
    schedule_api,
    monkeypatch: pytest.MonkeyPatch,
    blocker: str,
):
    session = schedule_api.seed_session
    await _closed_schedule(session)
    expected = _job(status=JobStatus.WAITING_WINDOW)
    session.add(expected)
    await session.flush()
    if blocker == "waiting_job":
        session.add(_job(status=JobStatus.WAITING_WINDOW))
    elif blocker == "active_job":
        session.add(_job(status=JobStatus.RUNNING))
    else:
        session.add(
            NodeExecution(
                job_id=expected.id,
                node_id=f"{blocker}_1",
                node_type="source",
                status=NodeStatus.QUEUED if blocker == "queued_node" else NodeStatus.RUNNING,
            )
        )
    await session.commit()
    expected_id = expected.id
    started = False

    async def record_started(_job_ids):
        nonlocal started
        started = True

    monkeypatch.setattr(internal_schedule_api, "start_jobs_background", record_started)

    async with AsyncClient(
        transport=ASGITransport(app=schedule_api.app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            SCHEDULE_PATH,
            params={"expected_job_id": str(expected_id)},
        )

    assert response.status_code == 409
    assert started is False
    session.expire_all()
    schedule = await session.get(RuntimeSchedule, "videoprocess")
    expected = await session.get(Job, expected_id)
    assert schedule is not None and schedule.state == "CLOSED"
    assert expected is not None and expected.status == JobStatus.WAITING_WINDOW
