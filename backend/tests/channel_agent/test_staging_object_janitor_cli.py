from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.channel_agent import staging_object_janitor_cli as cli


class _Engine:
    def __init__(self) -> None:
        self.disposed = False

    async def dispose(self) -> None:
        self.disposed = True


def _install_database_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_Engine, object]:
    engine = _Engine()
    session_factory = object()
    monkeypatch.setattr(
        cli,
        "_database_resources",
        lambda env: (engine, session_factory),
        raising=False,
    )
    monkeypatch.setattr(
        cli,
        "_load_minio_credentials",
        lambda env: None,
        raising=False,
    )
    return engine, session_factory


@pytest.mark.asyncio
async def test_cli_runs_exact_janitor_and_reports_counts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    status_file = tmp_path / "status.json"
    calls: list[dict[str, object]] = []
    status_calls: list[tuple[str, object]] = []
    engine, session_factory = _install_database_resources(monkeypatch)

    class StatusStore:
        def __init__(self, actual_session_factory):
            assert actual_session_factory is session_factory

        async def begin(self, run_id, *, runner_id):
            status_calls.append(("begin", (run_id, runner_id)))
            return "started"

        async def finish(
            self,
            run_id,
            *,
            result,
            succeeded,
        ):
            status_calls.append(
                ("finish", (run_id, dict(result), succeeded))
            )
            return succeeded

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
    monkeypatch.setattr(
        cli,
        "StagingJanitorStatusStore",
        StatusStore,
        raising=False,
    )
    monkeypatch.setenv(
        "VP_STAGING_JANITOR_RUNNER_ID",
        "ccttww-lap",
    )

    assert await cli.run(["--status-file", str(status_file)]) == 0
    assert engine.disposed
    assert calls[0]["status_file"] == status_file
    assert calls[0]["grace_seconds"] == 86400
    assert status_calls[0][0] == "begin"
    run_id, runner_id = status_calls[0][1]
    assert runner_id == "ccttww-lap"
    assert status_calls[1] == (
        "finish",
        (
            run_id,
            {
                "scanned": 2,
                "deleted": 1,
                "protected": 1,
                "too_young": 0,
                "invalid": 0,
                "errors": 0,
            },
            True,
        ),
    )
    assert '"status":"ok"' in capsys.readouterr().out


@pytest.mark.asyncio
async def test_cli_fails_closed_without_leaking_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    finishes: list[tuple[dict[str, int], bool]] = []
    engine, session_factory = _install_database_resources(monkeypatch)

    class StatusStore:
        def __init__(self, actual_session_factory):
            assert actual_session_factory is session_factory

        async def begin(self, run_id, *, runner_id):
            return "started"

        async def finish(
            self,
            run_id,
            *,
            result,
            succeeded,
        ):
            finishes.append((dict(result), succeeded))

    def fail(*args, **kwargs):
        raise RuntimeError("secret")

    monkeypatch.setattr(cli, "get_storage", fail)
    monkeypatch.setattr(
        cli,
        "StagingJanitorStatusStore",
        StatusStore,
        raising=False,
    )
    monkeypatch.setenv(
        "VP_STAGING_JANITOR_RUNNER_ID",
        "ccttww-lap",
    )

    assert await cli.run([]) == 3
    assert engine.disposed
    assert finishes == [
        (
            {
                "scanned": 0,
                "deleted": 0,
                "protected": 0,
                "too_young": 0,
                "invalid": 0,
                "errors": 1,
            },
            False,
        )
    ]
    assert capsys.readouterr().out == (
        '{"code":"janitor_failed","status":"failed"}\n'
    )


@pytest.mark.asyncio
async def test_cli_skips_overlapping_run_without_constructing_storage(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine, session_factory = _install_database_resources(monkeypatch)

    class StatusStore:
        def __init__(self, actual_session_factory):
            assert actual_session_factory is session_factory

        async def begin(self, run_id, *, runner_id):
            return "overlap"

    def unexpected_storage(*args, **kwargs):
        raise AssertionError("overlap must not construct MinIO")

    monkeypatch.setattr(cli, "get_storage", unexpected_storage)
    monkeypatch.setattr(
        cli,
        "StagingJanitorStatusStore",
        StatusStore,
        raising=False,
    )
    monkeypatch.setenv(
        "VP_STAGING_JANITOR_RUNNER_ID",
        "ccttww-lap",
    )

    assert await cli.run([]) == 0
    assert engine.disposed
    assert capsys.readouterr().out == (
        '{"status":"overlap_skipped"}\n'
    )


def test_database_resources_require_mode_0400_role_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _Engine()
    session_factory = object()
    reads: list[tuple[str, str]] = []

    def read_secret(path, *, label):
        reads.append((path, label))
        return "postgresql+asyncpg://janitor:secret@database/videoprocess"

    monkeypatch.setattr(cli, "read_mode_0400_secret", read_secret)
    monkeypatch.setattr(
        cli,
        "create_async_engine",
        lambda url, **kwargs: (
            engine
            if (
                url
                == "postgresql+asyncpg://janitor:secret@database/videoprocess"
                and kwargs == {"pool_pre_ping": True}
            )
            else None
        ),
    )
    monkeypatch.setattr(
        cli,
        "async_sessionmaker",
        lambda actual_engine, **kwargs: (
            session_factory
            if actual_engine is engine
            and kwargs == {"expire_on_commit": False}
            else None
        ),
    )

    actual_engine, actual_factory = cli._database_resources(
        {
            "VP_STAGING_JANITOR_DATABASE_URL_FILE": (
                "/run/secrets/vp-staging-janitor-database-url"
            )
        }
    )

    assert actual_engine is engine
    assert actual_factory is session_factory
    assert reads == [
        (
            "/run/secrets/vp-staging-janitor-database-url",
            "staging janitor database URL",
        )
    ]

    with pytest.raises(RuntimeError):
        cli._database_resources(
            {
                "DATABASE_URL": (
                    "postgresql+asyncpg://owner:secret@database/videoprocess"
                ),
                "VP_STAGING_JANITOR_DATABASE_URL_FILE": (
                    "/run/secrets/vp-staging-janitor-database-url"
                ),
            }
        )


def test_minio_credentials_require_independent_mode_0400_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reads: list[tuple[str, str]] = []

    def read_secret(path, *, label):
        reads.append((path, label))
        return {
            "staging janitor MinIO access key": "access-secret",
            "staging janitor MinIO secret key": "password-secret",
        }[label]

    monkeypatch.setattr(cli, "read_mode_0400_secret", read_secret)
    cli._load_minio_credentials(
        {
            "VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE": (
                "/run/secrets/vp-staging-janitor-minio-access-key"
            ),
            "VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE": (
                "/run/secrets/vp-staging-janitor-minio-secret-key"
            ),
        }
    )

    assert cli.settings.minio_access_key == "access-secret"
    assert cli.settings.minio_secret_key == "password-secret"
    assert reads == [
        (
            "/run/secrets/vp-staging-janitor-minio-access-key",
            "staging janitor MinIO access key",
        ),
        (
            "/run/secrets/vp-staging-janitor-minio-secret-key",
            "staging janitor MinIO secret key",
        ),
    ]

    with pytest.raises(RuntimeError):
        cli._load_minio_credentials(
            {
                "MINIO_ACCESS_KEY": "environment-secret",
                "VP_STAGING_JANITOR_MINIO_ACCESS_KEY_FILE": "/run/a",
                "VP_STAGING_JANITOR_MINIO_SECRET_KEY_FILE": "/run/b",
            }
        )
