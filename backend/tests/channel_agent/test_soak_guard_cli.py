from __future__ import annotations

import json
import subprocess
import sys
import uuid
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from app.channel_agent import soak_guard_cli
from app.services.channelops_soak_guard import (
    SOAK_GUARD_REASON,
    SoakGuardAssessment,
)


CHANNEL_ID = uuid.UUID("d6c5ee75-734a-42a4-b758-9d7d0ec89532")
STARTED_AT = "2026-07-16T18:00:00Z"


class _SessionContext:
    def __init__(self, session, exits):
        self.session = session
        self.exits = exits

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, traceback):
        self.exits.append(self.session)
        return False


class _SessionFactory:
    def __init__(self, *sessions):
        self.sessions = list(sessions)
        self.calls = 0
        self.exits = []

    def __call__(self):
        session = self.sessions[self.calls]
        self.calls += 1
        return _SessionContext(session, self.exits)


def _arguments(*extra: str) -> list[str]:
    return [
        "--channel-id",
        str(CHANNEL_ID),
        "--started-at",
        STARTED_AT,
        *extra,
    ]


def _payload(capsys) -> dict:
    output = capsys.readouterr()
    assert output.err == ""
    assert output.out.count("\n") == 1
    return json.loads(output.out)


def test_module_import_does_not_import_settings_or_database_module():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import app.channel_agent.soak_guard_cli; "
                "print('app.config' in sys.modules, 'app.db' in sys.modules)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout == "False False\n"


@pytest.mark.asyncio
async def test_healthy_assessment_returns_zero_and_one_json_object(monkeypatch, capsys):
    read_session = object()
    factory = _SessionFactory(read_session)
    assess = AsyncMock(
        return_value=SoakGuardAssessment(
            critical_codes=(),
            metrics={"channel_id": str(CHANNEL_ID), "publication_count": 1},
        )
    )
    quarantine = AsyncMock()
    monkeypatch.setattr(soak_guard_cli, "get_session_factory", lambda: factory)
    monkeypatch.setattr(soak_guard_cli, "assess_channelops_soak", assess)
    monkeypatch.setattr(soak_guard_cli, "quarantine_channelops_backlog", quarantine)

    exit_code = await soak_guard_cli.run(_arguments())

    assert exit_code == 0
    assert factory.calls == 1
    assert factory.exits == [read_session]
    policy = assess.await_args.args[1]
    assert policy.channel_id == CHANNEL_ID
    assert policy.max_publications_per_24h == 1
    assert policy.upload_stale_minutes == 45
    assert policy.feedback_grace_hours == 30
    quarantine.assert_not_awaited()
    assert _payload(capsys) == {
        "critical_codes": [],
        "metrics": {"channel_id": str(CHANNEL_ID), "publication_count": 1},
        "status": "healthy",
    }


@pytest.mark.asyncio
async def test_critical_assessment_returns_twenty_without_apply(monkeypatch, capsys):
    factory = _SessionFactory(object())
    assess = AsyncMock(
        return_value=SoakGuardAssessment(
            critical_codes=("service_unhealthy",),
            metrics={"channel_id": str(CHANNEL_ID), "external_condition_count": 1},
        )
    )
    quarantine = AsyncMock()
    monkeypatch.setattr(soak_guard_cli, "get_session_factory", lambda: factory)
    monkeypatch.setattr(soak_guard_cli, "assess_channelops_soak", assess)
    monkeypatch.setattr(soak_guard_cli, "quarantine_channelops_backlog", quarantine)

    exit_code = await soak_guard_cli.run(
        _arguments("--external-condition", "service_unhealthy")
    )

    assert exit_code == 20
    quarantine.assert_not_awaited()
    payload = _payload(capsys)
    assert payload["status"] == "critical"
    assert payload["critical_codes"] == ["service_unhealthy"]
    assert assess.await_args.kwargs["external_conditions"] == ("service_unhealthy",)


@pytest.mark.asyncio
async def test_apply_uses_fresh_session_and_exact_fail_closed_arguments(
    monkeypatch,
    capsys,
):
    read_session = object()
    AnyAsyncSession = object()
    factory = _SessionFactory(read_session, AnyAsyncSession)
    assess = AsyncMock(
        return_value=SoakGuardAssessment(
            critical_codes=("unsafe_account_privacy",),
            metrics={"channel_id": str(CHANNEL_ID), "unsafe_account_privacy_count": 1},
        )
    )
    quarantine = AsyncMock(
        return_value={
            "counts": {
                "changed": {"channel_ids": 1, "task_ids": 2},
                "retained": {"publication_ids": 1},
            }
        }
    )
    monkeypatch.setattr(soak_guard_cli, "get_session_factory", lambda: factory)
    monkeypatch.setattr(soak_guard_cli, "assess_channelops_soak", assess)
    monkeypatch.setattr(soak_guard_cli, "quarantine_channelops_backlog", quarantine)

    exit_code = await soak_guard_cli.run(_arguments("--apply"))

    assert exit_code == 20
    assert factory.calls == 2
    assert factory.exits == [read_session, AnyAsyncSession]
    quarantine.assert_awaited_once_with(
        AnyAsyncSession,
        CHANNEL_ID,
        apply=True,
        reason=SOAK_GUARD_REASON,
        close_schedule=True,
        now=ANY,
    )
    assert _payload(capsys) == {
        "critical_codes": ["unsafe_account_privacy"],
        "metrics": {
            "channel_id": str(CHANNEL_ID),
            "unsafe_account_privacy_count": 1,
        },
        "quarantine_counts": {
            "changed": {"channel_ids": 1, "task_ids": 2},
            "retained": {"publication_ids": 1},
        },
        "status": "quarantined",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        ["--channel-id", "not-a-uuid", "--started-at", STARTED_AT],
        ["--channel-id", str(CHANNEL_ID), "--started-at", "not-a-timestamp"],
        ["--channel-id", str(CHANNEL_ID), "--started-at", "2026-07-16T18:00:00"],
        _arguments("--max-publications-per-24h", "0"),
        _arguments("--upload-stale-minutes", "-1"),
        _arguments("--feedback-grace-hours", "not-an-int"),
        _arguments("--external-condition", "postgresql://secret"),
    ],
)
async def test_invalid_arguments_return_two_without_opening_database(
    monkeypatch,
    capsys,
    arguments,
):
    get_factory = Mock(side_effect=AssertionError("database must not be opened"))
    monkeypatch.setattr(soak_guard_cli, "get_session_factory", get_factory)

    exit_code = await soak_guard_cli.run(arguments)

    assert exit_code == 2
    get_factory.assert_not_called()
    assert _payload(capsys) == {
        "critical_codes": [],
        "metrics": {},
        "status": "invalid_arguments",
    }


@pytest.mark.asyncio
async def test_database_failure_returns_three_without_exception_or_environment_values(
    monkeypatch,
    capsys,
):
    secret_url = "postgresql+asyncpg://operator:super-secret@database/videoprocess"
    secret_token = "environment-token-must-not-leak"
    monkeypatch.setenv("DATABASE_URL", secret_url)
    monkeypatch.setenv("PRIVATE_SERVICE_TOKEN", secret_token)
    factory = _SessionFactory(object())
    assess = AsyncMock(side_effect=RuntimeError(f"connection failed: {secret_url} {secret_token}"))
    monkeypatch.setattr(soak_guard_cli, "get_session_factory", lambda: factory)
    monkeypatch.setattr(soak_guard_cli, "assess_channelops_soak", assess)

    exit_code = await soak_guard_cli.run(_arguments())

    assert exit_code == 3
    output = capsys.readouterr().out
    assert secret_url not in output
    assert secret_token not in output
    assert "RuntimeError" not in output
    assert "connection failed" not in output
    assert json.loads(output) == {
        "critical_codes": [],
        "metrics": {},
        "status": "database_error",
    }


@pytest.mark.asyncio
async def test_quarantine_failure_returns_three_without_claiming_protection(
    monkeypatch,
    capsys,
):
    factory = _SessionFactory(object(), object())
    assess = AsyncMock(
        return_value=SoakGuardAssessment(
            critical_codes=("service_missing",),
            metrics={"channel_id": str(CHANNEL_ID), "external_condition_count": 1},
        )
    )
    quarantine = AsyncMock(side_effect=RuntimeError("postgresql://secret failure"))
    monkeypatch.setattr(soak_guard_cli, "get_session_factory", lambda: factory)
    monkeypatch.setattr(soak_guard_cli, "assess_channelops_soak", assess)
    monkeypatch.setattr(soak_guard_cli, "quarantine_channelops_backlog", quarantine)

    exit_code = await soak_guard_cli.run(_arguments("--apply"))

    assert exit_code == 3
    output = capsys.readouterr().out
    assert "secret" not in output
    assert json.loads(output)["status"] == "quarantine_error"
