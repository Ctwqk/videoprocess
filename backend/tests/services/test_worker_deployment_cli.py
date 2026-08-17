from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

import pytest

from app.services import worker_deployment_cli as cli
from app.services.worker_runtime_role_cli import (
    role_names_for_generation,
    write_generation_state,
)


@pytest.mark.asyncio
async def test_migrate_uses_only_mode_0400_migrator_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    url_file = tmp_path / "deploy-migrator-url"
    url_file.write_text(
        "postgresql+asyncpg://deploy_migrator:database-secret@"
        "db/videoprocess\n"
    )
    url_file.chmod(0o400)
    monkeypatch.setenv(cli.DEPLOY_MIGRATOR_URL_FILE_ENV, str(url_file))
    monkeypatch.delenv("DATABASE_URL", raising=False)
    calls: list[str] = []

    def upgrade(database_url: str) -> None:
        calls.append(database_url)

    monkeypatch.setattr(cli, "_upgrade_database", upgrade)

    assert await cli.run(["migrate"]) == 0
    assert calls == [
        "postgresql+asyncpg://deploy_migrator:database-secret@"
        "db/videoprocess"
    ]
    output = capsys.readouterr().out
    assert json.loads(output) == {
        "code": "worker_deployment_migrated",
        "status": "ok",
    }
    assert "database-secret" not in output
    assert "environment-secret" not in output


@pytest.mark.asyncio
async def test_migrate_runs_alembic_upgrade_without_an_active_event_loop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url_file = tmp_path / "deploy-migrator-url"
    url_file.write_text(
        "postgresql+asyncpg://deploy_migrator:database-secret@"
        "db/videoprocess\n"
    )
    url_file.chmod(0o400)
    monkeypatch.setenv(cli.DEPLOY_MIGRATOR_URL_FILE_ENV, str(url_file))
    monkeypatch.delenv("DATABASE_URL", raising=False)

    def upgrade(_database_url: str) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        raise AssertionError("Alembic upgrade inherited the CLI event loop")

    monkeypatch.setattr(cli, "_upgrade_database", upgrade)

    assert await cli.run(["migrate"]) == 0


@pytest.mark.asyncio
async def test_migrate_rejects_ambient_database_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    url_file = tmp_path / "deploy-migrator-url"
    url_file.write_text(
        "postgresql+asyncpg://deploy_migrator:file-secret@db/videoprocess\n"
    )
    url_file.chmod(0o400)
    monkeypatch.setenv(cli.DEPLOY_MIGRATOR_URL_FILE_ENV, str(url_file))
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://leaked:environment-secret@db/videoprocess",
    )

    assert await cli.run(["migrate"]) == 3
    output = capsys.readouterr().out
    assert json.loads(output)["code"] == (
        "worker_deployment_migrator_url_invalid"
    )
    assert "file-secret" not in output
    assert "environment-secret" not in output


@pytest.mark.asyncio
async def test_verify_head_uses_deploy_read_url_and_exact_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    url_file = tmp_path / "deploy-read-url"
    url_file.write_text(
        "postgresql://deploy_read:database-secret@db/videoprocess\n"
    )
    url_file.chmod(0o400)
    monkeypatch.setenv(cli.DEPLOY_READ_URL_FILE_ENV, str(url_file))
    queries: list[str] = []

    class Connection:
        async def fetch(self, query):
            queries.append(query)
            return [{"version_num": cli.EXPECTED_MIGRATION_HEAD}]

        async def close(self):
            pass

    async def connect(url):
        assert "deploy_read:database-secret" in url
        return Connection()

    monkeypatch.setattr(cli.asyncpg, "connect", connect)

    assert await cli.run(["verify-head"]) == 0
    assert queries == [
        "SELECT version_num FROM public.alembic_version "
        "ORDER BY version_num"
    ]
    output = capsys.readouterr().out
    assert json.loads(output) == {
        "code": "worker_deployment_migration_head_verified",
        "status": "ok",
    }
    assert "database-secret" not in output


@pytest.mark.asyncio
async def test_render_request_uses_versioned_state_and_canonical_topology(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service_name = "vp-vision-worker-swarm"
    generation = 42
    state_dir = tmp_path / "state"
    role_names = role_names_for_generation(service_name, generation)
    write_generation_state(
        state_dir,
        service_name,
        generation,
        role_names,
        database_url=(
            f"postgresql+asyncpg://{role_names.versioned}:secret@"
            "db-claimed:5435/claimed"
            "?host=vp-postgres&port=5544&database=videoprocess"
        ),
        admission_token="admission-secret",
    )
    request_file = (
        tmp_path / "requests" / str(generation) / "upsert.json"
    )

    assert await cli.run(
        [
            "render-request",
            "--service-name",
            service_name,
            "--generation",
            str(generation),
            "--release-commit",
            "0123456789abcdef0123456789abcdef01234567",
            "--image-identity",
            "vp-ffmpeg-worker-python:deploy-0123456789ab",
            "--state-dir",
            str(state_dir),
            "--request-file",
            str(request_file),
            "--redis-host",
            "vp-redis",
            "--redis-port",
            "6379",
            "--redis-database",
            "0",
            "--storage-host",
            "10.0.0.150",
            "--storage-port",
            "9000",
            "--storage-bucket",
            "videoprocess",
        ]
    ) == 0

    assert request_file.stat().st_mode & 0o777 == 0o600
    payload = json.loads(request_file.read_text())
    assert payload == {
        "version": 1,
        "service_name": service_name,
        "generation": generation,
        "worker_type": "vision",
        "worker_host": "150-vision",
        "capabilities": ["vision_gpu"],
        "release_commit": (
            "0123456789abcdef0123456789abcdef01234567"
        ),
        "image_identity": (
            "vp-ffmpeg-worker-python:deploy-0123456789ab"
        ),
        "database_principal": (
            role_names.versioned
        ),
        "redis_stream": "vp:tasks:vision",
        "redis_group": "vision-workers",
        "endpoint_bindings": {
            "database": {
                "driver": "postgresql",
                "host": "vp-postgres",
                "port": 5544,
                "database": "videoprocess",
            },
            "redis": {
                "scheme": "redis",
                "host": "vp-redis",
                "port": 6379,
                "database": 0,
            },
            "storage": {
                "backend": "minio",
                "host": "10.0.0.150",
                "port": 9000,
                "bucket": "videoprocess",
            },
        },
        "token_sha256": (
            "f3d66d8d07123105b2526662841766db2ed796e3cca87e"
            "9ff24d35cd7f7da529"
        ),
        "issued_by": "vp-deploy-controller",
    }
    assert "secret" not in capsys.readouterr().out


@pytest.mark.asyncio
async def test_readiness_uses_mode_0400_deploy_read_url_and_is_sanitized(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    url_file = tmp_path / "deploy-read-url"
    url_file.write_text(
        "postgresql://deploy_read:database-secret@db/videoprocess\n"
    )
    url_file.chmod(0o400)
    monkeypatch.setenv(cli.DEPLOY_READ_URL_FILE_ENV, str(url_file))
    calls: list[tuple[str, tuple[object, ...]]] = []

    class Connection:
        async def fetchval(self, query, *arguments):
            calls.append((query, arguments))
            return True

        async def close(self):
            pass

    async def connect(url):
        assert "deploy_read:database-secret" in url
        return Connection()

    monkeypatch.setattr(cli.asyncpg, "connect", connect)

    assert await cli.run(
        [
            "readiness",
            "--service-name",
            "vp-vision-worker-swarm",
            "--generation",
            "42",
        ]
    ) == 0
    assert calls[0][1] == ("vp-vision-worker-swarm", 42)
    query = " ".join(calls[0][0].split())
    for copied_fact in (
        "registration.worker_type = grant_record.worker_type",
        "registration.worker_host = grant_record.worker_host",
        "registration.capabilities_json = grant_record.capabilities_json",
        "registration.image_identity = grant_record.image_identity",
        "registration.database_principal = grant_record.database_principal",
        "grant_record.state = 'active'",
        "registration.redis_consumer_id =",
        "vp_worker_endpoint_fingerprints",
    ):
        assert copied_fact in query
    output = capsys.readouterr().out
    assert json.loads(output)["code"] == "worker_deployment_ready"
    assert "database-secret" not in output


@pytest.mark.asyncio
async def test_retirement_candidates_return_only_canonical_registration_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    url_file = tmp_path / "deploy-read-url"
    url_file.write_text(
        "postgresql://deploy_read:database-secret@db/videoprocess\n"
    )
    url_file.chmod(0o400)
    monkeypatch.setenv(cli.DEPLOY_READ_URL_FILE_ENV, str(url_file))
    first = uuid.uuid4()
    second = uuid.uuid4()

    class Connection:
        async def fetch(self, query, *arguments):
            assert "worker_registrations" in query
            assert arguments == ("vp-vision-worker-swarm", 41)
            return [
                {"registration_id": first},
                {"registration_id": second},
            ]

        async def close(self):
            pass

    async def connect(url):
        return Connection()

    monkeypatch.setattr(cli.asyncpg, "connect", connect)

    assert await cli.run(
        [
            "retirement-candidates",
            "--service-name",
            "vp-vision-worker-swarm",
            "--generation",
            "41",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "code": "worker_deployment_retirement_candidates",
        "generation": 41,
        "registration_ids": [str(first), str(second)],
        "service_name": "vp-vision-worker-swarm",
        "status": "ok",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("database_state", "expected_state"),
    [
        (None, "absent"),
        ("pending", "pending"),
        ("active", "active"),
        ("revoked", "revoked"),
    ],
)
async def test_generation_state_is_sanitized_and_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    database_state: str | None,
    expected_state: str,
) -> None:
    url_file = tmp_path / "deploy-read-url"
    url_file.write_text(
        "postgresql://deploy_read:database-secret@db/videoprocess\n"
    )
    url_file.chmod(0o400)
    monkeypatch.setenv(cli.DEPLOY_READ_URL_FILE_ENV, str(url_file))

    class Connection:
        async def fetchval(self, query, *arguments):
            assert "worker_admission_grants" in query
            assert arguments == ("vp-vision-worker-swarm", 41)
            return database_state

        async def close(self):
            pass

    async def connect(url):
        assert "database-secret" in url
        return Connection()

    monkeypatch.setattr(cli.asyncpg, "connect", connect)

    assert await cli.run(
        [
            "generation-state",
            "--service-name",
            "vp-vision-worker-swarm",
            "--generation",
            "41",
        ]
    ) == 0
    output_text = capsys.readouterr().out
    assert json.loads(output_text) == {
        "code": "worker_deployment_generation_state",
        "generation": 41,
        "grant_state": expected_state,
        "service_name": "vp-vision-worker-swarm",
        "status": "ok",
    }
    assert "database-secret" not in output_text
