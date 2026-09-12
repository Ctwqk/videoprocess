from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from app.channel_agent import worker_redis_marker_janitor_cli as cli
from app.services.worker_redis_marker_control import MarkerControlConfig


@pytest.mark.asyncio
async def test_janitor_cli_uses_separate_config_and_sanitized_counts(
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
    monkeypatch.setattr(cli, "load_marker_control_config", lambda *, production: MarkerControlConfig("postgresql://janitor", "redis://janitor"))
    monkeypatch.setattr(cli, "create_database_engine", lambda _url: calls.append("database") or engine)
    monkeypatch.setattr(cli, "create_redis_client", lambda _url: calls.append("redis") or redis)

    class SessionContext:
        async def __aenter__(self):
            return "janitor-session"

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(cli, "create_session_factory", lambda _engine: lambda: SessionContext())

    async def janitor(database, redis_client, run_id):
        assert database == "janitor-session"
        assert redis_client is redis
        assert run_id.version == 4
        return {"claimed": 3, "deleted": 1, "absent": 1, "conflict": 1}

    monkeypatch.setattr(cli, "run_worker_redis_marker_janitor", janitor)

    assert await cli.run(["run"]) == 3
    assert calls == ["database", "redis", "close", "dispose"]
    assert json.loads(capsys.readouterr().out) == {"absent": 1, "claimed": 3, "code": "marker_cleanup_conflict", "conflict": 1, "deleted": 1, "status": "failed"}


@pytest.mark.asyncio
async def test_janitor_cli_rejects_sensitive_unknown_arguments_before_clients(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(cli, "load_marker_control_config", lambda *, production: calls.append("config"))

    assert await cli.run(["run", "--redis-url", "redis://:secret@host/0"]) == 3

    captured = capsys.readouterr()
    assert calls == []
    assert json.loads(captured.out) == {"code": "invalid_arguments", "status": "failed"}
    assert "secret" not in captured.out + captured.err
