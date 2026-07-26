from __future__ import annotations

import json
import os
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
    assert _payload(capsys) == READY_RESULT


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

    assert _payload(capsys) == {
        "status": "failed",
        "code": "minio_unavailable",
    }


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
    assert json.loads(captured.out) == {
        "status": "failed",
        "code": "unexpected_failure",
    }
    assert captured.err == ""
    assert url not in captured.out + captured.err
    assert "password" not in captured.out + captured.err
