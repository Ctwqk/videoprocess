from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.services.worker_registration import (
    WorkerLease,
    WorkerRegistrationClaims,
    WorkerRegistrationError,
)
from worker import registration as worker_registration
from worker.registration import PythonWorkerRegistration, build_worker_registration_claims
from worker.secret_config import (
    WorkerSecretError,
    load_worker_admission_token,
    load_worker_database_url,
    load_worker_minio_credentials,
    load_worker_redis_url,
    read_mode_0400_secret,
)
from worker import secret_config


def _lease(*, heartbeat_interval_seconds: float = 0.01) -> WorkerLease:
    return WorkerLease(
        registration_id=uuid.uuid4(),
        grant_id=uuid.uuid4(),
        service_name="vp-vision-worker-swarm",
        worker_instance_id=uuid.uuid4(),
        worker_slot=1,
        redis_consumer_id="vision-worker@127:1:instance",
        lease_epoch=7,
        lease_secret="lease-secret",
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=180),
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )


def _claims() -> WorkerRegistrationClaims:
    return WorkerRegistrationClaims(
        service_name="vp-vision-worker-swarm",
        generation=4,
        worker_type="vision",
        worker_host="127",
        worker_instance_id=uuid.uuid4(),
        worker_slot=1,
        redis_consumer_id="vision-worker@127:1:instance",
        capabilities=("vision_gpu",),
        release_commit="0123456789abcdef0123456789abcdef01234567",
        image_identity="vp-vision-worker:deploy-0123456789ab",
        redis_stream="vp:tasks:vision",
        redis_group="vision-workers",
        endpoint_bindings={
            "database": {
                "driver": "postgresql",
                "host": "vp-postgres",
                "port": 5432,
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
                "host": "vp-minio",
                "port": 9000,
                "bucket": "videoprocess",
            },
        },
        database_fingerprint="a" * 64,
        redis_fingerprint="b" * 64,
        storage_fingerprint="c" * 64,
    )


def _write_secret(path: Path, value: str, *, mode: int = 0o400) -> None:
    if path.exists():
        path.chmod(0o600)
    path.write_text(value, encoding="utf-8")
    path.chmod(mode)


def test_mode_0400_secret_read_is_bounded_and_strips_one_trailing_newline(
    tmp_path: Path,
) -> None:
    secret = tmp_path / "secret"
    _write_secret(secret, "sensitive-value\n")

    assert read_mode_0400_secret(secret, label="worker secret") == "sensitive-value"

    _write_secret(secret, "x" * 4097)
    with pytest.raises(WorkerSecretError, match="worker secret is too large"):
        read_mode_0400_secret(secret, label="worker secret")


def test_secret_reader_handles_short_regular_file_reads(
    monkeypatch,
    tmp_path: Path,
) -> None:
    secret = tmp_path / "secret"
    _write_secret(secret, "complete-secret")
    original_read = os.read
    first_read = True

    def short_read(descriptor: int, maximum: int) -> bytes:
        nonlocal first_read
        if first_read:
            first_read = False
            return original_read(descriptor, 3)
        return original_read(descriptor, maximum)

    monkeypatch.setattr(secret_config.os, "read", short_read)

    assert (
        read_mode_0400_secret(secret, label="worker secret")
        == "complete-secret"
    )


@pytest.mark.parametrize(
    ("mutation", "replacement"),
    [
        ("truncate", b"XYZ"),
        ("overwrite", b"UVWXYZ"),
        ("grow", b"abcdef-extra"),
    ],
)
def test_secret_reader_rejects_in_place_mutation_during_read(
    monkeypatch,
    tmp_path: Path,
    mutation: str,
    replacement: bytes,
) -> None:
    secret = tmp_path / "secret"
    _write_secret(secret, "abcdef")
    original_read = os.read
    first_read = True

    def mutate_after_first_read(descriptor: int, maximum: int) -> bytes:
        nonlocal first_read
        chunk = original_read(
            descriptor,
            min(maximum, 3) if first_read else maximum,
        )
        if first_read:
            first_read = False
            secret.chmod(0o600)
            if mutation == "grow":
                with secret.open("ab") as handle:
                    handle.write(replacement[6:])
            else:
                secret.write_bytes(replacement)
            secret.chmod(0o400)
        return chunk

    monkeypatch.setattr(
        secret_config.os,
        "read",
        mutate_after_first_read,
    )

    with pytest.raises(WorkerSecretError, match="changed"):
        read_mode_0400_secret(secret, label="worker secret")


@pytest.mark.parametrize("mode", (0o000, 0o440, 0o600, 0o644))
def test_secret_reader_rejects_any_mode_other_than_0400(
    tmp_path: Path,
    mode: int,
) -> None:
    secret = tmp_path / "secret"
    _write_secret(secret, "sensitive-value", mode=mode)

    with pytest.raises(WorkerSecretError, match="mode 0400"):
        read_mode_0400_secret(secret, label="worker secret")


def test_production_database_url_requires_file_and_rejects_environment_value(
    tmp_path: Path,
) -> None:
    database_secret = tmp_path / "database-url"
    _write_secret(
        database_secret,
        "postgresql+asyncpg://runtime:password@vp-postgres/videoprocess",
    )
    production = {
        "DEPLOY_MODE": "production",
        "WORKER_DATABASE_URL_FILE": str(database_secret),
    }

    assert load_worker_database_url(production).startswith("postgresql+asyncpg://")

    with pytest.raises(WorkerSecretError, match="DATABASE_URL"):
        load_worker_database_url(
            {
                **production,
                "DATABASE_URL": "postgresql+asyncpg://leaked:credential@db/vp",
            }
        )

    with pytest.raises(WorkerSecretError, match="WORKER_DATABASE_URL_FILE"):
        load_worker_database_url({"DEPLOY_MODE": "production"})


def test_nonproduction_database_url_fallback_and_production_token_file(
    tmp_path: Path,
) -> None:
    token_secret = tmp_path / "admission-token"
    _write_secret(token_secret, "admission-token")

    assert (
        load_worker_database_url(
            {
                "DEPLOY_MODE": "local",
                "REDIS_URL": "redis://localhost:6379/0",
                "DATABASE_URL": "sqlite+aiosqlite:///worker.db",
            }
        )
        == "sqlite+aiosqlite:///worker.db"
    )
    assert (
        load_worker_admission_token(
            {
                "DEPLOY_MODE": "production",
                "WORKER_ADMISSION_TOKEN_FILE": str(token_secret),
            }
        )
        == "admission-token"
    )


def test_production_admission_token_rejects_environment_value(
    tmp_path: Path,
) -> None:
    token_secret = tmp_path / "admission-token"
    _write_secret(token_secret, "admission-token")

    with pytest.raises(WorkerSecretError, match="WORKER_ADMISSION_TOKEN"):
        load_worker_admission_token(
            {
                "DEPLOY_MODE": "production",
                "WORKER_ADMISSION_TOKEN_FILE": str(token_secret),
                "WORKER_ADMISSION_TOKEN": "environment-token",
            }
        )


def test_production_minio_credentials_require_independent_mode_0400_files(
    tmp_path: Path,
) -> None:
    access_secret = tmp_path / "minio-access"
    password_secret = tmp_path / "minio-password"
    _write_secret(access_secret, "minio-access")
    _write_secret(password_secret, "minio-password")
    production = {
        "DEPLOY_MODE": "production",
        "WORKER_MINIO_ACCESS_KEY_FILE": str(access_secret),
        "WORKER_MINIO_SECRET_KEY_FILE": str(password_secret),
    }

    assert load_worker_minio_credentials(production) == (
        "minio-access",
        "minio-password",
    )
    with pytest.raises(WorkerSecretError, match="MINIO_ACCESS_KEY"):
        load_worker_minio_credentials(
            {
                **production,
                "MINIO_ACCESS_KEY": "environment-access",
            }
        )
    with pytest.raises(WorkerSecretError, match="independent"):
        load_worker_minio_credentials(
            {
                **production,
                "WORKER_MINIO_SECRET_KEY_FILE": str(access_secret),
            }
        )


def test_production_redis_url_requires_mode_0400_file(
    tmp_path: Path,
) -> None:
    redis_secret = tmp_path / "redis-url"
    _write_secret(
        redis_secret,
        "redis://vp-vision-worker:redis-secret@vp-redis:6379/0",
    )
    production = {
        "DEPLOY_MODE": "production",
        "WORKER_REDIS_URL_FILE": str(redis_secret),
    }

    assert (
        load_worker_redis_url(production)
        == "redis://vp-vision-worker:redis-secret@vp-redis:6379/0"
    )
    with pytest.raises(WorkerSecretError, match="REDIS_URL"):
        load_worker_redis_url(
            {
                **production,
                "REDIS_URL": "redis://leaked:secret@vp-redis:6379/0",
            }
        )
    with pytest.raises(WorkerSecretError, match="WORKER_REDIS_URL_FILE"):
        load_worker_redis_url({"DEPLOY_MODE": "shared"})


def test_nonproduction_redis_url_allows_explicit_environment_fallback() -> None:
    assert (
        load_worker_redis_url(
            {
                "DEPLOY_MODE": "local",
                "REDIS_URL": "redis://127.0.0.1:6379/14",
            }
        )
        == "redis://127.0.0.1:6379/14"
    )


def test_file_backed_remote_redis_rejects_environment_redis_fallback(
    tmp_path: Path,
) -> None:
    redis_secret = tmp_path / "redis-url"
    _write_secret(
        redis_secret,
        "redis://vp-worker:secret@vp-redis:6379/7",
    )

    with pytest.raises(WorkerSecretError, match="REDIS_URL"):
        load_worker_redis_url(
            {
                "DEPLOY_MODE": "development",
                "WORKER_REDIS_URL_FILE": str(redis_secret),
                "REDIS_URL": "redis://127.0.0.1:6379/14",
            }
        )


def test_registration_claim_builder_uses_one_instance_in_consumer_and_exact_integers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instance_id = uuid.uuid4()
    release_commit = "0123456789abcdef0123456789abcdef01234567"
    env = {
            "DEPLOY_MODE": "shared",
            "WORKER_SERVICE_NAME": "vp-vision-worker-swarm",
            "WORKER_ADMISSION_GENERATION": "4",
            "WORKER_SLOT": "1",
            "WORKER_TYPE": "vision",
            "WORKER_HOST": "127",
            "WORKER_CAPABILITIES": "vision_gpu",
            "WORKER_RELEASE_COMMIT": release_commit,
            "WORKER_IMAGE_IDENTITY": "vp-vision-worker:deploy-0123456789ab",
            "WORKER_REDIS_STREAM": "vp:tasks:vision",
            "WORKER_REDIS_GROUP": "vision-workers",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "vp-minio:9000",
            "MINIO_BUCKET": "videoprocess",
        }
    monkeypatch.setattr(
        worker_registration,
        "EMBEDDED_BUILD_COMMIT",
        release_commit,
    )
    claims = build_worker_registration_claims(
        env,
        database_url=(
            "postgresql+asyncpg://runtime:secret@vp-postgres:5432/videoprocess"
        ),
        redis_url="redis://vp-worker:secret@vp-redis:6379/2",
        worker_instance_id=instance_id,
    )

    assert claims.worker_instance_id == instance_id
    assert str(instance_id) in claims.redis_consumer_id
    assert type(claims.endpoint_bindings["database"]["port"]) is int
    assert type(claims.endpoint_bindings["redis"]["port"]) is int
    assert type(claims.endpoint_bindings["redis"]["database"]) is int
    assert type(claims.endpoint_bindings["storage"]["port"]) is int

    for invalid_build_commit in (
        "",
        "1123456789abcdef0123456789abcdef01234567",
        "not-a-commit",
    ):
        monkeypatch.setattr(
            worker_registration,
            "EMBEDDED_BUILD_COMMIT",
            invalid_build_commit,
        )
        with pytest.raises(WorkerRegistrationError, match="claim_mismatch"):
            build_worker_registration_claims(
                env,
                database_url=(
                    "postgresql+asyncpg://runtime:secret@"
                    "vp-postgres:5432/videoprocess"
                ),
                redis_url="redis://vp-worker:secret@vp-redis:6379/2",
                worker_instance_id=instance_id,
            )


def test_remote_effective_redis_requires_embedded_commit_in_development_mode() -> None:
    env = {
        "DEPLOY_MODE": "development",
        "WORKER_SERVICE_NAME": "vp-vision-worker-swarm",
        "WORKER_ADMISSION_GENERATION": "4",
        "WORKER_SLOT": "1",
        "WORKER_TYPE": "vision",
        "WORKER_HOST": "127",
        "WORKER_CAPABILITIES": "vision_gpu",
        "WORKER_RELEASE_COMMIT": (
            "0123456789abcdef0123456789abcdef01234567"
        ),
        "WORKER_IMAGE_IDENTITY": (
            "vp-vision-worker:deploy-0123456789ab"
        ),
        "WORKER_REDIS_STREAM": "vp:tasks:vision",
        "WORKER_REDIS_GROUP": "vision-workers",
        "STORAGE_BACKEND": "minio",
        "MINIO_ENDPOINT": "vp-minio:9000",
        "MINIO_BUCKET": "videoprocess",
    }

    with pytest.raises(WorkerRegistrationError, match="claim_mismatch"):
        build_worker_registration_claims(
            env,
            database_url=(
                "postgresql+asyncpg://runtime:secret@"
                "vp-postgres:5432/videoprocess"
            ),
            redis_url=(
                "redis://vp-worker:secret@vp-redis:6379/7"
            ),
            worker_instance_id=uuid.uuid4(),
        )


def test_registration_claim_uses_artifact_commit_not_runtime_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_commit = "0123456789abcdef0123456789abcdef01234567"
    env = {
        "DEPLOY_MODE": "production",
        "WORKER_SERVICE_NAME": "vp-vision-worker-swarm",
        "WORKER_ADMISSION_GENERATION": "4",
        "WORKER_SLOT": "1",
        "WORKER_TYPE": "vision",
        "WORKER_HOST": "127",
        "WORKER_CAPABILITIES": "vision_gpu",
        "WORKER_RELEASE_COMMIT": release_commit,
        "VP_BUILD_COMMIT": "1" * 40,
        "WORKER_IMAGE_IDENTITY": (
            "vp-vision-worker:deploy-0123456789ab"
        ),
        "WORKER_REDIS_STREAM": "vp:tasks:vision",
        "WORKER_REDIS_GROUP": "vision-workers",
        "STORAGE_BACKEND": "minio",
        "MINIO_ENDPOINT": "vp-minio:9000",
        "MINIO_BUCKET": "videoprocess",
    }
    monkeypatch.setattr(
        worker_registration,
        "EMBEDDED_BUILD_COMMIT",
        release_commit,
        raising=False,
    )

    claims = build_worker_registration_claims(
        env,
        database_url=(
            "postgresql+asyncpg://runtime:secret@"
            "vp-postgres:5432/videoprocess"
        ),
        redis_url="redis://vp-worker:secret@vp-redis:6379/7",
        worker_instance_id=uuid.uuid4(),
    )

    assert claims.release_commit == release_commit


def test_runtime_environment_cannot_mask_wrong_artifact_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release_commit = "0123456789abcdef0123456789abcdef01234567"
    env = {
        "DEPLOY_MODE": "production",
        "WORKER_SERVICE_NAME": "vp-vision-worker-swarm",
        "WORKER_ADMISSION_GENERATION": "4",
        "WORKER_SLOT": "1",
        "WORKER_TYPE": "vision",
        "WORKER_HOST": "127",
        "WORKER_CAPABILITIES": "vision_gpu",
        "WORKER_RELEASE_COMMIT": release_commit,
        "VP_BUILD_COMMIT": release_commit,
        "WORKER_IMAGE_IDENTITY": (
            "vp-vision-worker:deploy-0123456789ab"
        ),
        "WORKER_REDIS_STREAM": "vp:tasks:vision",
        "WORKER_REDIS_GROUP": "vision-workers",
        "STORAGE_BACKEND": "minio",
        "MINIO_ENDPOINT": "vp-minio:9000",
        "MINIO_BUCKET": "videoprocess",
    }
    monkeypatch.setattr(
        worker_registration,
        "EMBEDDED_BUILD_COMMIT",
        "1" * 40,
        raising=False,
    )

    with pytest.raises(WorkerRegistrationError, match="claim_mismatch"):
        build_worker_registration_claims(
            env,
            database_url=(
                "postgresql+asyncpg://runtime:secret@"
                "vp-postgres:5432/videoprocess"
            ),
            redis_url="redis://vp-worker:secret@vp-redis:6379/7",
            worker_instance_id=uuid.uuid4(),
        )


@pytest.mark.asyncio
async def test_lifecycle_registers_then_initially_heartbeats_before_start_returns() -> None:
    events: list[str] = []
    registered = _lease()
    renewed = _lease()

    class Service:
        async def register(self, claims, token):
            assert claims == _claims_value
            assert token == "admission-token"
            events.append("register")
            return registered

        async def heartbeat(self, lease):
            assert lease in {registered, renewed}
            events.append("heartbeat")
            return renewed

        async def revoke(self, lease, *, reason):
            events.append(f"revoke:{reason}")

    _claims_value = _claims()
    lifecycle = PythonWorkerRegistration(
        Service(),
        _claims_value,
        "admission-token",
    )

    lease = await lifecycle.start()
    assert lease == renewed
    assert lifecycle.redis_stream == _claims_value.redis_stream
    assert lifecycle.redis_group == _claims_value.redis_group
    assert lifecycle.worker_host == _claims_value.worker_host
    assert events[:2] == ["register", "heartbeat"]

    await lifecycle.close()
    assert events[-1] == "revoke:shutdown"


@pytest.mark.asyncio
async def test_registration_loss_synchronously_cancels_guarded_consumer() -> None:
    lifecycle = PythonWorkerRegistration(
        object(),
        _claims(),
        "admission-token",
    )
    started = asyncio.Event()
    continued_after_loss: list[bool] = []

    async def consumer() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
            continued_after_loss.append(True)
        finally:
            continued_after_loss.append(False)

    task = lifecycle.create_guarded_task(consumer())
    await started.wait()
    lifecycle._mark_lost(WorkerRegistrationError("lease_fenced"))
    await asyncio.gather(task, return_exceptions=True)

    assert task.cancelled()
    assert continued_after_loss == [False]


@pytest.mark.asyncio
async def test_heartbeat_refresh_does_not_use_worker_clock_for_margin_authority() -> None:
    registered = _lease()
    short_database_lease = WorkerLease(
        **{
            **registered.__dict__,
            "lease_expires_at": (
                datetime.now(timezone.utc) + timedelta(seconds=120)
            ),
        }
    )

    class Service:
        async def register(self, claims, token):
            return registered

        async def heartbeat(self, lease):
            return short_database_lease

        async def revoke(self, lease, *, reason):
            return None

    lifecycle = PythonWorkerRegistration(
        Service(),
        _claims(),
        "admission-token",
    )
    await lifecycle.start()

    renewed = await lifecycle.heartbeat_now(minimum_margin_seconds=150)

    assert renewed == short_database_lease
    await lifecycle.close()


@pytest.mark.asyncio
async def test_initial_heartbeat_failure_revokes_registration_before_raising() -> None:
    registered = _lease()
    revoked: list[tuple[WorkerLease, str]] = []

    class Service:
        async def register(self, claims, token):
            return registered

        async def heartbeat(self, lease):
            raise WorkerRegistrationError("lease_fenced")

        async def revoke(self, lease, *, reason):
            revoked.append((lease, reason))

    lifecycle = PythonWorkerRegistration(
        Service(),
        _claims(),
        "admission-token",
    )

    with pytest.raises(WorkerRegistrationError, match="lease_fenced"):
        await lifecycle.start()

    assert revoked == [(registered, "startup_failed")]


@pytest.mark.asyncio
async def test_initial_heartbeat_cancellation_still_revokes_registration() -> None:
    registered = _lease()
    heartbeat_started = asyncio.Event()
    revoked = asyncio.Event()

    class Service:
        async def register(self, claims, token):
            return registered

        async def heartbeat(self, lease):
            heartbeat_started.set()
            await asyncio.Event().wait()

        async def revoke(self, lease, *, reason):
            assert reason == "startup_failed"
            revoked.set()

    lifecycle = PythonWorkerRegistration(
        Service(),
        _claims(),
        "admission-token",
    )
    startup = asyncio.create_task(lifecycle.start())
    await heartbeat_started.wait()
    startup.cancel()

    with pytest.raises(asyncio.CancelledError):
        await startup

    assert revoked.is_set()


@pytest.mark.asyncio
async def test_heartbeat_loss_sets_lost_and_bounded_close_does_not_hang() -> None:
    registered = _lease(heartbeat_interval_seconds=0.001)
    heartbeat_calls = 0
    revoke_started = asyncio.Event()

    class Service:
        async def register(self, claims, token):
            return registered

        async def heartbeat(self, lease):
            nonlocal heartbeat_calls
            heartbeat_calls += 1
            if heartbeat_calls == 1:
                return registered
            raise WorkerRegistrationError("lease_fenced")

        async def revoke(self, lease, *, reason):
            revoke_started.set()
            await asyncio.Event().wait()

    lifecycle = PythonWorkerRegistration(
        Service(),
        _claims(),
        "admission-token",
        close_timeout_seconds=0.01,
    )
    await lifecycle.start()

    lost = await asyncio.wait_for(lifecycle.wait_lost(), timeout=0.2)
    assert lost.code == "lease_fenced"
    await asyncio.wait_for(lifecycle.close(), timeout=0.2)
    assert revoke_started.is_set()


def test_secret_errors_do_not_contain_secret_contents(tmp_path: Path) -> None:
    secret = tmp_path / "admission-token"
    value = "never-log-this-admission-token"
    _write_secret(secret, value, mode=0o644)

    with pytest.raises(WorkerSecretError) as exc:
        load_worker_admission_token(
            {
                "DEPLOY_MODE": "production",
                "WORKER_ADMISSION_TOKEN_FILE": os.fspath(secret),
            }
        )

    assert value not in str(exc.value)
