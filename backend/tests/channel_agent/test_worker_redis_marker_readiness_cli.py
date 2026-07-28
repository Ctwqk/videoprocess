from __future__ import annotations

import json
import uuid
from types import SimpleNamespace

import pytest

from app.channel_agent import worker_redis_marker_readiness_cli as cli
from app.services.worker_redis_marker_control import ContinuityResult, MarkerControlConfig


def test_marker_cli_rejects_environment_credentials_before_clients(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    client_factory_calls: list[str] = []
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret")
    monkeypatch.setenv("REDIS_URL", "redis://secret")
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: client_factory_calls.append("database"))
    monkeypatch.setattr(cli, "create_redis_client", lambda _url: client_factory_calls.append("redis"))

    assert cli.main(["check"]) == 3

    assert client_factory_calls == []
    assert json.loads(capsys.readouterr().out) == {"code": "marker_control_config_invalid", "status": "failed"}


@pytest.mark.asyncio
async def test_readiness_cli_constructs_clients_after_config_and_emits_stable_result(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []
    engine = SimpleNamespace()
    redis = SimpleNamespace()

    async def dispose():
        calls.append("dispose")

    async def aclose():
        calls.append("close")

    engine.dispose = dispose
    redis.aclose = aclose
    monkeypatch.setattr(cli, "load_marker_control_config", lambda *, production: MarkerControlConfig("postgresql://marker", "redis://marker"))
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: calls.append("database") or engine)
    monkeypatch.setattr(cli, "create_redis_client", lambda _url: calls.append("redis") or redis)

    class SessionContext:
        async def __aenter__(self):
            return "database-session"

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(cli, "create_session_factory", lambda _engine: lambda: SessionContext())

    async def check(database, redis_client, expected_user):
        assert database == "database-session"
        assert redis_client is redis
        assert expected_user == "vp-marker-readiness"
        return ContinuityResult(uuid.uuid4(), "error", "active_marker_missing", None, 2, 1)

    monkeypatch.setattr(cli, "check_worker_redis_continuity", check)

    assert await cli.run(["check"]) == 3
    assert calls == ["database", "redis", "close", "dispose"]
    assert json.loads(capsys.readouterr().out) == {"checked_count": 1, "code": "active_marker_missing", "expected_count": 2, "status": "failed"}


@pytest.mark.asyncio
async def test_readiness_cli_sanitizes_dependency_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(cli, "load_marker_control_config", lambda *, production: (_ for _ in ()).throw(RuntimeError("redis://:secret@host/0")))

    assert await cli.run(["check"]) == 3

    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"code": "marker_control_config_invalid", "status": "failed"}
    assert "secret" not in captured.out + captured.err
