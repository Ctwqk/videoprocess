from __future__ import annotations

import json
import stat
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.channel_agent import legacy_event_resolution_cli as cli
from app.services.legacy_worker_event_resolution import (
    LegacyEventCandidate,
    LegacyEventResolutionError,
    LegacyEventResolutionReport,
)


MESSAGE_ID = "1785034608101-0"
PAYLOAD_SHA256 = "a" * 64
MANIFEST = f"{MESSAGE_ID}={PAYLOAD_SHA256}"


def report(*, applied: bool) -> LegacyEventResolutionReport:
    return LegacyEventResolutionReport(
        applied=applied,
        candidates=(
            LegacyEventCandidate(
                message_id=MESSAGE_ID,
                payload_sha256=PAYLOAD_SHA256,
                job_id=uuid.UUID("871ec6e9-1ac0-458c-8870-15e7684cf49f"),
                node_execution_id=uuid.UUID(
                    "2f387cc9-fd8b-451d-a7eb-9d4df500501a"
                ),
            ),
        ),
        resolution_ids=(uuid.UUID("4f789fff-eb56-44ca-9ea4-440257ffd715"),)
        if applied
        else (),
        xack_count=1 if applied else 0,
        final_pending=0 if applied else 1,
    )


@pytest.mark.anyio
async def test_run_defaults_to_dry_run_and_writes_sanitized_mode_0600_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    database_url = "postgresql+asyncpg://user:database-secret@db/vp"
    redis_url = "redis://:redis-secret@cache/0"
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("REDIS_URL", redis_url)
    observed_request = None

    async def execute_request(_database_url, _redis_url, request):
        nonlocal observed_request
        assert _database_url == database_url
        assert _redis_url == redis_url
        observed_request = request
        return report(applied=False)

    monkeypatch.setattr(cli, "execute_request", execute_request)
    evidence = tmp_path / "dry-run.json"

    exit_code = await cli.run(
        [
            "--expected-event",
            MANIFEST,
            "--evidence",
            str(evidence),
        ]
    )

    assert exit_code == 0
    assert observed_request is not None
    assert observed_request.apply is False
    assert observed_request.operator_id == "dry-run"
    payload = json.loads(evidence.read_text())
    assert payload["mode"] == "dry_run"
    assert payload["status"] == "succeeded"
    assert payload["xack_count"] == 0
    assert payload["final_pending"] == 1
    assert payload["candidates"] == [
        {
            "job_id": "871ec6e9-1ac0-458c-8870-15e7684cf49f",
            "message_id": MESSAGE_ID,
            "node_execution_id": "2f387cc9-fd8b-451d-a7eb-9d4df500501a",
            "payload_sha256": PAYLOAD_SHA256,
        }
    ]
    serialized = json.dumps(payload)
    assert "database-secret" not in serialized
    assert "redis-secret" not in serialized
    assert stat.S_IMODE(evidence.stat().st_mode) == 0o600


@pytest.mark.anyio
async def test_run_apply_requires_operator_before_opening_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    called = False

    async def execute_request(*_args):
        nonlocal called
        called = True
        return report(applied=True)

    monkeypatch.setattr(cli, "execute_request", execute_request)

    exit_code = await cli.run(
        [
            "--expected-event",
            MANIFEST,
            "--apply",
            "--evidence",
            str(tmp_path / "apply.json"),
        ]
    )

    assert exit_code == 2
    assert called is False


@pytest.mark.anyio
async def test_run_apply_passes_operator_and_records_resolution_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://db/vp")
    monkeypatch.setenv("REDIS_URL", "redis://cache/0")
    observed_request = None

    async def execute_request(_database_url, _redis_url, request):
        nonlocal observed_request
        observed_request = request
        return report(applied=True)

    monkeypatch.setattr(cli, "execute_request", execute_request)
    evidence = tmp_path / "apply.json"

    exit_code = await cli.run(
        [
            "--expected-event",
            MANIFEST,
            "--operator",
            "operator:wenjieliu",
            "--apply",
            "--evidence",
            str(evidence),
        ]
    )

    assert exit_code == 0
    assert observed_request.apply is True
    assert observed_request.operator_id == "operator:wenjieliu"
    payload = json.loads(evidence.read_text())
    assert payload["mode"] == "apply"
    assert payload["resolution_ids"] == [
        "4f789fff-eb56-44ca-9ea4-440257ffd715"
    ]
    assert payload["xack_count"] == 1
    assert payload["final_pending"] == 0


@pytest.mark.anyio
async def test_run_sanitizes_known_and_unknown_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:secret@db/vp")
    monkeypatch.setenv("REDIS_URL", "redis://:secret@cache/0")
    evidence = tmp_path / "failed.json"

    async def known_failure(*_args):
        raise LegacyEventResolutionError("expected events changed")

    monkeypatch.setattr(cli, "execute_request", known_failure)
    known_exit = await cli.run(
        [
            "--expected-event",
            MANIFEST,
            "--evidence",
            str(evidence),
        ]
    )
    known_payload = json.loads(evidence.read_text())

    async def unknown_failure(*_args):
        raise RuntimeError("postgresql://user:secret@db/vp")

    monkeypatch.setattr(cli, "execute_request", unknown_failure)
    unknown_exit = await cli.run(
        [
            "--expected-event",
            MANIFEST,
            "--evidence",
            str(evidence),
        ]
    )
    unknown_payload = json.loads(evidence.read_text())

    assert known_exit == 3
    assert known_payload["failure"] == {
        "type": "LegacyEventResolutionError",
        "message": "expected events changed",
    }
    assert unknown_exit == 3
    assert unknown_payload["failure"] == {
        "type": "RuntimeError",
        "message": "internal dependency failure",
    }
    assert "secret" not in json.dumps(unknown_payload)


@pytest.mark.anyio
async def test_execute_request_closes_redis_and_database_on_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    disposed = False
    redis_closed = False
    fake_engine = SimpleNamespace()
    fake_redis = SimpleNamespace()
    fake_db = object()

    class SessionContext:
        async def __aenter__(self):
            return fake_db

        async def __aexit__(self, *_args):
            return None

    def session_factory(_engine):
        return lambda: SessionContext()

    async def dispose():
        nonlocal disposed
        disposed = True

    async def aclose():
        nonlocal redis_closed
        redis_closed = True

    async def fail(_db, _redis, _request):
        assert _db is fake_db
        assert _redis is fake_redis
        raise RuntimeError("failure")

    fake_engine.dispose = dispose
    fake_redis.aclose = aclose
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: fake_engine)
    monkeypatch.setattr(cli, "create_redis_client", lambda _url: fake_redis)
    monkeypatch.setattr(cli, "create_session_factory", session_factory)
    monkeypatch.setattr(cli, "resolve_legacy_worker_events", fail)

    with pytest.raises(RuntimeError, match="failure"):
        await cli.execute_request(
            "postgresql+asyncpg://db/vp",
            "redis://cache/0",
            SimpleNamespace(),
        )

    assert redis_closed is True
    assert disposed is True


@pytest.mark.anyio
async def test_execute_request_disposes_database_when_redis_creation_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    disposed = False
    fake_engine = SimpleNamespace()

    async def dispose():
        nonlocal disposed
        disposed = True

    fake_engine.dispose = dispose
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: fake_engine)

    def fail_redis(_url):
        raise RuntimeError("redis creation failed")

    monkeypatch.setattr(cli, "create_redis_client", fail_redis)

    with pytest.raises(RuntimeError, match="redis creation failed"):
        await cli.execute_request(
            "postgresql+asyncpg://db/vp",
            "redis://cache/0",
            SimpleNamespace(),
        )

    assert disposed is True


@pytest.mark.anyio
async def test_execute_request_disposes_database_when_redis_close_fails(
    monkeypatch: pytest.MonkeyPatch,
):
    disposed = False
    fake_engine = SimpleNamespace()
    fake_redis = SimpleNamespace()

    async def dispose():
        nonlocal disposed
        disposed = True

    async def fail_close():
        raise RuntimeError("redis close failed")

    class SessionContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return None

    async def resolve(*_args):
        return report(applied=False)

    fake_engine.dispose = dispose
    fake_redis.aclose = fail_close
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: fake_engine)
    monkeypatch.setattr(cli, "create_redis_client", lambda _url: fake_redis)
    monkeypatch.setattr(
        cli,
        "create_session_factory",
        lambda _engine: lambda: SessionContext(),
    )
    monkeypatch.setattr(cli, "resolve_legacy_worker_events", resolve)

    with pytest.raises(RuntimeError, match="redis close failed"):
        await cli.execute_request(
            "postgresql+asyncpg://db/vp",
            "redis://cache/0",
            SimpleNamespace(),
        )

    assert disposed is True
