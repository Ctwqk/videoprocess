from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from app.channel_agent import worker_storage_readiness_cli as cli
from app.services.worker_storage_readiness import ReadinessFailure


READY_RESULT = {
    "status": "ready",
    "components": {
        "scratch": "ready",
        "minio": "ready",
        "artifact_api": "not_required",
    },
}
READY_STDOUT = (
    '{"components":{"artifact_api":"not_required","minio":"ready",'
    '"scratch":"ready"},"status":"ready"}\n'
)
MINIO_FAILURE_STDOUT = '{"code":"minio_unavailable","status":"failed"}\n'
INVALID_ARGUMENTS_STDOUT = '{"code":"invalid_arguments","status":"failed"}\n'
UNEXPECTED_FAILURE_STDOUT = '{"code":"unexpected_failure","status":"failed"}\n'


def _payload(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.count("\n") == 1
    return json.loads(captured.out)


@pytest.mark.asyncio
async def test_run_emits_ready_payload_by_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    probe = AsyncMock(return_value=READY_RESULT)
    monkeypatch.setattr(cli, "probe_worker_storage", probe)

    assert await cli.run([]) == 0

    probe.assert_awaited_once_with(os.environ, require_artifact_api=False)
    captured = capsys.readouterr()
    assert captured.out == READY_STDOUT
    assert captured.err == ""


@pytest.mark.asyncio
async def test_run_requires_artifact_api_when_flag_is_present(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    probe = AsyncMock(
        return_value={
            **READY_RESULT,
            "components": {
                **READY_RESULT["components"],
                "artifact_api": "ready",
            },
        }
    )
    monkeypatch.setattr(cli, "probe_worker_storage", probe)

    assert await cli.run(["--require-artifact-api"]) == 0

    probe.assert_awaited_once_with(os.environ, require_artifact_api=True)
    assert _payload(capsys)["components"]["artifact_api"] == "ready"


@pytest.mark.asyncio
async def test_run_sanitizes_readiness_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    probe = AsyncMock(side_effect=ReadinessFailure("minio_unavailable"))
    monkeypatch.setattr(cli, "probe_worker_storage", probe)

    assert await cli.run([]) == 3

    captured = capsys.readouterr()
    assert captured.out == MINIO_FAILURE_STDOUT
    assert captured.err == ""


@pytest.mark.asyncio
async def test_run_rejects_unknown_sensitive_arguments(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sensitive_url = "https://user:password@example.test/private"
    probe = AsyncMock()
    monkeypatch.setattr(cli, "probe_worker_storage", probe)

    assert await cli.run(["--unsupported", sensitive_url]) == 3

    captured = capsys.readouterr()
    assert captured.out == INVALID_ARGUMENTS_STDOUT
    assert captured.err == ""
    assert sensitive_url not in captured.out + captured.err
    assert "password" not in captured.out + captured.err
    probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_sanitizes_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    url = "https://user:password@example.test/private"
    probe = AsyncMock(side_effect=RuntimeError(f"request failed: {url}"))
    monkeypatch.setattr(cli, "probe_worker_storage", probe)

    assert await cli.run([]) == 3

    captured = capsys.readouterr()
    assert captured.out == UNEXPECTED_FAILURE_STDOUT
    assert captured.err == ""
    assert url not in captured.out + captured.err
    assert "password" not in captured.out + captured.err


def test_module_rejects_unknown_sensitive_arguments_without_leaking() -> None:
    sensitive_url = "https://user:password@example.test/private"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "app.channel_agent.worker_storage_readiness_cli",
            "--unsupported",
            sensitive_url,
        ],
        cwd=Path(__file__).resolve().parents[2],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 3
    assert result.stdout == INVALID_ARGUMENTS_STDOUT
    assert result.stderr == ""
    assert sensitive_url not in result.stdout + result.stderr
    assert "password" not in result.stdout + result.stderr
