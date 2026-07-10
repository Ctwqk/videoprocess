from __future__ import annotations

import pytest

from app.services.worker_admission import WorkerAdmissionError
from worker import main as worker_main


def test_worker_database_is_not_configured_at_import() -> None:
    assert worker_main.engine_db is None
    assert worker_main.worker_session is None


@pytest.mark.asyncio
async def test_worker_admission_runs_before_database_and_redis(monkeypatch) -> None:
    events: list[str] = []

    class StopStartup(RuntimeError):
        pass

    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        lambda: events.append("admission"),
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "configure_worker_database",
        lambda: events.append("database"),
        raising=False,
    )

    def stop_at_redis() -> None:
        events.append("redis")
        raise StopStartup

    monkeypatch.setattr(worker_main, "_redis", stop_at_redis)

    with pytest.raises(StopStartup):
        await worker_main.main()

    assert events == ["admission", "database", "redis"]


@pytest.mark.asyncio
async def test_denied_worker_stops_before_database_or_redis(monkeypatch) -> None:
    touched: list[str] = []

    def deny_worker() -> None:
        raise WorkerAdmissionError("unsafe worker configuration")

    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        deny_worker,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "configure_worker_database",
        lambda: touched.append("database"),
        raising=False,
    )
    monkeypatch.setattr(worker_main, "_redis", lambda: touched.append("redis"))

    with pytest.raises(SystemExit) as exc:
        await worker_main.main()

    assert exc.value.code == 2
    assert touched == []
