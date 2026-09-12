from __future__ import annotations

import uuid

import pytest

from app.services.staging_janitor_status import (
    STAGING_JANITOR_MAX_AGE_SECONDS,
    STAGING_JANITOR_STALE_RUN_SECONDS,
    StagingJanitorStatusStore,
)


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class _Session:
    def __init__(self, calls):
        self.calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    def begin(self):
        return _Transaction()

    async def scalar(self, statement, parameters):
        sql = str(statement)
        self.calls.append((sql, dict(parameters)))
        if "vp_begin_staging_janitor_run" in sql:
            return "started"
        if "vp_finish_staging_janitor_run" in sql:
            return True
        if "vp_staging_janitor_readiness" in sql:
            return "ready"
        raise AssertionError(sql)


@pytest.mark.asyncio
async def test_status_store_uses_durable_begin_finish_and_readiness_functions() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def session_factory():
        return _Session(calls)

    store = StagingJanitorStatusStore(session_factory)
    run_id = uuid.uuid4()
    result = {
        "scanned": 3,
        "deleted": 1,
        "protected": 1,
        "too_young": 1,
        "invalid": 0,
        "errors": 0,
    }

    assert await store.begin(run_id, runner_id="ccttww-lap") == "started"
    await store.finish(run_id, result=result, succeeded=True)
    assert await store.readiness() == "ready"

    assert "vp_begin_staging_janitor_run" in calls[0][0]
    assert calls[0][1] == {
        "run_id": run_id,
        "runner_id": "ccttww-lap",
        "stale_run_seconds": STAGING_JANITOR_STALE_RUN_SECONDS,
    }
    assert "vp_finish_staging_janitor_run" in calls[1][0]
    assert calls[1][1]["run_id"] == run_id
    assert calls[1][1]["succeeded"] is True
    assert "vp_staging_janitor_readiness" in calls[2][0]
    assert calls[2][1] == {
        "max_age_seconds": STAGING_JANITOR_MAX_AGE_SECONDS,
        "stale_run_seconds": STAGING_JANITOR_STALE_RUN_SECONDS,
    }
