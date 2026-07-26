from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.channel_agent import staging_object_janitor_cli as cli


@pytest.mark.asyncio
async def test_cli_runs_exact_janitor_and_reports_counts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    status_file = tmp_path / "status.json"
    calls: list[dict[str, object]] = []

    class Janitor:
        def __init__(self, session_factory, **kwargs):
            calls.append(
                {
                    "session_factory": session_factory,
                    **kwargs,
                }
            )

        async def run_once(self):
            return {
                "scanned": 2,
                "deleted": 1,
                "protected": 1,
                "too_young": 0,
                "invalid": 0,
                "errors": 0,
            }

    storage = SimpleNamespace(client=object(), bucket="videoprocess")
    monkeypatch.setattr(
        cli,
        "get_storage",
        lambda name, *, create_bucket: (
            storage
            if (name, create_bucket) == ("minio", False)
            else None
        ),
    )
    monkeypatch.setattr(cli, "MinioStorageBackend", SimpleNamespace)
    monkeypatch.setattr(cli, "StagingObjectJanitor", Janitor)

    assert await cli.run(["--status-file", str(status_file)]) == 0
    assert calls[0]["status_file"] == status_file
    assert calls[0]["grace_seconds"] == 86400
    assert '"status":"ok"' in capsys.readouterr().out


@pytest.mark.asyncio
async def test_cli_fails_closed_without_leaking_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail(*args, **kwargs):
        raise RuntimeError("secret")

    monkeypatch.setattr(cli, "get_storage", fail)

    assert await cli.run([]) == 3
    assert capsys.readouterr().out == (
        '{"code":"janitor_failed","status":"failed"}\n'
    )
