from __future__ import annotations

import json
import uuid
from types import SimpleNamespace

import pytest

from app.services import worker_redis_marker_repair_cli as cli
from app.services.worker_redis_marker_control import MarkerControlConfig


@pytest.mark.asyncio
async def test_repair_cli_defaults_to_read_only_audit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine = SimpleNamespace()
    redis = SimpleNamespace()

    async def dispose():
        return None

    async def aclose():
        return None

    engine.dispose = dispose
    redis.aclose = aclose
    monkeypatch.setattr(cli, "load_marker_control_config", lambda *, production: MarkerControlConfig("postgresql://repair", "redis://repair"))
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: engine)
    monkeypatch.setattr(cli, "create_redis_client", lambda _url: redis)

    class SessionContext:
        async def __aenter__(self):
            return "repair-session"

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(cli, "create_session_factory", lambda _engine: lambda: SessionContext())

    async def audit(database, redis_client):
        assert database == "repair-session"
        assert redis_client is redis
        return [{"source_id": str(uuid.uuid4()), "code": "marker_absent"}]

    monkeypatch.setattr(cli, "audit_worker_redis_markers", audit)

    assert await cli.run(["audit"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "ok"
    assert payload["count"] == 1
    assert payload["markers"][0]["code"] == "marker_absent"
    assert uuid.UUID(payload["markers"][0]["source_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("command", "source_option"),
    [("restore-marker", "--source-id"), ("promote-prepared", "--emission-id")],
)
async def test_repair_cli_dry_runs_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    command: str,
    source_option: str,
) -> None:
    source_id = uuid.uuid4()
    monkeypatch.setattr(cli, "load_marker_control_config", lambda *, production: MarkerControlConfig("postgresql://repair", "redis://repair"))
    engine = SimpleNamespace()
    redis = SimpleNamespace()

    async def dispose():
        return None

    async def aclose():
        return None

    engine.dispose = dispose
    redis.aclose = aclose
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: engine)
    monkeypatch.setattr(cli, "create_redis_client", lambda _url: redis)

    class SessionContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(cli, "create_session_factory", lambda _engine: lambda: SessionContext())
    mutations: list[bool] = []

    async def repair(_database, _redis, _source_id, *, apply):
        mutations.append(apply)
        return "dry_run_ready"

    monkeypatch.setattr(cli, "restore_worker_redis_marker" if command == "restore-marker" else "promote_prepared_worker_event", repair)

    assert await cli.run([command, source_option, str(source_id)]) == 0
    assert mutations == [False]
    assert json.loads(capsys.readouterr().out) == {"code": "dry_run_ready", "status": "ok"}


@pytest.mark.asyncio
async def test_repair_cli_requires_apply_for_mutating_path(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "load_marker_control_config", lambda *, production: (_ for _ in ()).throw(AssertionError("clients must not be created")))

    assert await cli.run(["restore-marker", "--source-id", str(uuid.uuid4()), "--apply"]) == 3
    assert json.loads(capsys.readouterr().out) == {"code": "marker_control_config_invalid", "status": "failed"}
