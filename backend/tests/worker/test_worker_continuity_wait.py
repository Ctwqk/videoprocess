"""Startup waits only for the exact in-progress PostgreSQL continuity result."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from asyncpg import PostgresError
from sqlalchemy.dialects.postgresql.asyncpg import AsyncAdapt_asyncpg_dbapi
from sqlalchemy.exc import DBAPIError

from app.services.worker_registration import WorkerRegistrationError
from worker import main as worker_main
from worker.registration import PythonWorkerRegistration


def database_error(message="worker_redis_continuity_running", *, adapted=True, state="P0001"):
    error = PostgresError.new({"C": state, "M": message})
    if adapted:
        wrapper = AsyncAdapt_asyncpg_dbapi.Error(str(error))
        wrapper.__cause__ = error
        error = wrapper
    return DBAPIError(None, None, error)


def sessions(monkeypatch, outcomes):
    attempts, closed = [], []

    @asynccontextmanager
    async def factory():
        index = len(attempts)

        class Session:
            async def execute(self, statement, parameters):
                assert len(closed) == index
                attempts.append((str(statement), parameters))
                outcome = outcomes[min(index, len(outcomes) - 1)]
                if isinstance(outcome, Exception):
                    raise outcome
                if callable(outcome):
                    await outcome()

        try:
            yield Session()
        finally:
            closed.append(index)

    monkeypatch.setattr(worker_main, "get_worker_session", lambda: factory)
    monkeypatch.setattr(worker_main, "WORKER_REDIS_CONTINUITY_RETRY_SECONDS", 0.001, raising=False)
    monkeypatch.setattr(worker_main, "WORKER_REDIS_CONTINUITY_WAIT_SECONDS", 0.03, raising=False)
    return attempts, closed


@pytest.mark.asyncio
@pytest.mark.parametrize("adapted", [False, True])
async def test_running_then_ready_rechecks_gate_in_fresh_session(monkeypatch, adapted):
    attempts, closed = sessions(monkeypatch, [database_error(adapted=adapted), None])
    await worker_main._require_worker_redis_continuity()
    assert len(attempts) == len(closed) == 2
    assert all("vp_require_worker_redis_continuity" in sql and params == {"max_age_seconds": 90}
               for sql, params in attempts)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    database_error("worker_redis_continuity_stale"),
    database_error("worker_redis_continuity_error"),
    database_error("worker_redis_continuity_missing"),
    database_error("database_principal_mismatch"),
    database_error("worker_redis_continuity_running extra"),
    database_error(state="08006"),
    RuntimeError("worker_redis_continuity_running"),
])
async def test_other_failures_are_immediate_and_sanitized(monkeypatch, error):
    attempts, closed = sessions(monkeypatch, [error, None])
    with pytest.raises(WorkerRegistrationError, match="^worker_redis_continuity_unready$"):
        await worker_main._require_worker_redis_continuity()
    assert len(attempts) == len(closed) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("blocked_query", [False, True])
async def test_budget_bounds_repeated_running_and_database_call(monkeypatch, blocked_query):
    async def block():
        await asyncio.Event().wait()

    attempts, closed = sessions(monkeypatch, [block if blocked_query else database_error()])
    with pytest.raises(WorkerRegistrationError, match="^worker_redis_continuity_unready$"):
        await asyncio.wait_for(worker_main._require_worker_redis_continuity(), 0.5)
    assert attempts and len(closed) == len(attempts)
    assert len(attempts) == 1 if blocked_query else len(attempts) > 1


@pytest.mark.asyncio
@pytest.mark.parametrize("lease_loss", [False, True])
async def test_cancellation_or_registration_loss_interrupts_database_wait(monkeypatch, lease_loss):
    entered = asyncio.Event()

    async def block():
        entered.set()
        await asyncio.Event().wait()

    attempts, closed = sessions(monkeypatch, [block])
    monkeypatch.setattr(worker_main, "WORKER_REDIS_CONTINUITY_WAIT_SECONDS", 10, raising=False)
    registration = PythonWorkerRegistration(SimpleNamespace(), SimpleNamespace(), "test-token")
    task = asyncio.create_task(worker_main._require_worker_redis_continuity(registration))
    await asyncio.wait_for(entered.wait(), 0.5)
    if lease_loss:
        registration._mark_lost(WorkerRegistrationError("lease_fenced"))
        expected, match = WorkerRegistrationError, "lease_fenced"
    else:
        task.cancel()
        expected, match = asyncio.CancelledError, None
    with pytest.raises(expected, match=match):
        await asyncio.wait_for(task, 0.5)
    assert len(attempts) == len(closed) == 1
