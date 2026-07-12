from __future__ import annotations

import pytest

from app.services.worker_admission import (
    WorkerAdmissionError,
    enforce_worker_admission_from_env,
    validate_worker_admission,
)


def _publisher_env() -> dict[str, str]:
    return {
        "DEPLOY_MODE": "production",
        "REDIS_URL": "redis://10.0.0.150:6380/0",
        "WORKER_TYPE": "youtube_publisher",
        "WORKER_HOST": "150-publisher",
        "STORAGE_BACKEND": "minio",
        "MINIO_ENDPOINT": "10.0.0.150:9000",
        "MINIO_ACCESS_KEY": "x",
        "MINIO_SECRET_KEY": "y",
        "MINIO_BUCKET": "videoprocess",
        "YOUTUBE_MANAGER_URL": "http://10.0.0.150:18999",
        "YOUTUBE_PUBLISH_ENABLED": "true",
        "PUBLIC_PUBLISH_ENABLED": "false",
    }


def test_production_youtube_publisher_is_admitted_with_explicit_contract() -> None:
    assert validate_worker_admission(_publisher_env()).allowed


def test_production_youtube_publisher_rejects_local_manager_url() -> None:
    env = _publisher_env()
    env["YOUTUBE_MANAGER_URL"] = "http://localhost:8899"

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert "production youtube_publisher workers require a non-local YOUTUBE_MANAGER_URL" in decision.reasons


def test_production_youtube_publisher_rejects_root_qualified_local_manager_url() -> None:
    env = _publisher_env()
    env["YOUTUBE_MANAGER_URL"] = "http://localhost.:8899"

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert "production youtube_publisher workers require a non-local YOUTUBE_MANAGER_URL" in decision.reasons


@pytest.mark.parametrize(
    "manager_url",
    ("http://[0:0:0:0:0:0:0:1]:8899", "http://[::]:8899"),
    ids=("expanded-loopback", "unspecified"),
)
def test_production_youtube_publisher_rejects_ipv6_loopback_or_unspecified_manager(
    manager_url: str,
) -> None:
    env = _publisher_env()
    env["YOUTUBE_MANAGER_URL"] = manager_url

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert "production youtube_publisher workers require a non-local YOUTUBE_MANAGER_URL" in decision.reasons


@pytest.mark.parametrize(
    "missing_key",
    ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET"),
)
def test_production_youtube_publisher_requires_each_minio_setting(missing_key: str) -> None:
    env = _publisher_env()
    env.pop(missing_key)

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert f"production youtube_publisher workers require {missing_key}" in decision.reasons


def test_production_youtube_publisher_rejects_public_publish() -> None:
    env = _publisher_env()
    env["PUBLIC_PUBLISH_ENABLED"] = "true"

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert "production youtube_publisher workers require PUBLIC_PUBLISH_ENABLED=false" in decision.reasons


def test_production_youtube_publisher_rejects_disabled_publishing() -> None:
    env = _publisher_env()
    env["YOUTUBE_PUBLISH_ENABLED"] = "false"

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert "production youtube_publisher workers require YOUTUBE_PUBLISH_ENABLED=true" in decision.reasons


@pytest.mark.parametrize("worker_type", ("youtube_publisher", "ffmpeg"))
def test_production_workers_reject_youtube_credentials_directory(worker_type: str) -> None:
    env = _publisher_env()
    env["WORKER_TYPE"] = worker_type
    env["YOUTUBE_CREDENTIALS_DIR"] = "/app/youtube_credentials"

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert "production workers must not set YOUTUBE_CREDENTIALS_DIR" in decision.reasons


def test_local_worker_with_local_storage_is_allowed() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://localhost:6379/0",
            "WORKER_TYPE": "ffmpeg",
            "STORAGE_BACKEND": "local",
        }
    )

    assert decision.allowed is True
    assert decision.reasons == ()


def test_remote_ffmpeg_worker_requires_minio_storage() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "local",
        }
    )

    assert decision.allowed is False
    assert "production ffmpeg workers require STORAGE_BACKEND=minio" in decision.reasons


def test_remote_ffmpeg_worker_rejects_localhost_minio_endpoint() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "localhost:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is False
    assert "production MinIO endpoint must not point at localhost:9000" in decision.reasons


def test_remote_ffmpeg_worker_rejects_root_qualified_localhost_minio_endpoint() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "localhost.:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is False
    assert "production MinIO endpoint must not point at localhost.:9000" in decision.reasons


@pytest.mark.parametrize(
    "endpoint",
    ("[0:0:0:0:0:0:0:1]:9000", "[::]:9000"),
    ids=("expanded-loopback", "unspecified"),
)
def test_remote_ffmpeg_worker_rejects_ipv6_loopback_or_unspecified_minio_endpoint(
    endpoint: str,
) -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": endpoint,
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is False
    assert f"production MinIO endpoint must not point at {endpoint}" in decision.reasons


def test_remote_ffmpeg_worker_with_minio_and_explicit_host_is_allowed() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "10.0.0.150:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is True
    assert decision.reasons == ()


@pytest.mark.parametrize(
    "missing_key",
    ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET"),
)
def test_remote_ffmpeg_worker_requires_each_minio_setting(missing_key: str) -> None:
    env = {
        "DEPLOY_MODE": "local",
        "REDIS_URL": "redis://10.0.0.150:6380/0",
        "WORKER_TYPE": "ffmpeg",
        "WORKER_HOST": "150-gpu",
        "STORAGE_BACKEND": "minio",
        "MINIO_ENDPOINT": "10.0.0.150:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin",
        "MINIO_BUCKET": "videoprocess",
    }
    env.pop(missing_key)

    decision = validate_worker_admission(env)

    assert decision.allowed is False
    assert f"production ffmpeg workers require {missing_key}" in decision.reasons


@pytest.mark.parametrize(
    "endpoint",
    ("localhost:9000", "127.0.0.1:9000", "0.0.0.0:9000", "[::1]:9000"),
)
def test_remote_ffmpeg_worker_rejects_all_local_minio_hosts(endpoint: str) -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": endpoint,
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is False
    assert f"production MinIO endpoint must not point at {endpoint}" in decision.reasons


@pytest.mark.parametrize("deploy_mode", ("shared", "production"))
def test_production_mode_requires_explicit_worker_host_even_with_local_redis(
    deploy_mode: str,
) -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": deploy_mode,
            "REDIS_URL": "redis://localhost:6379/0",
            "WORKER_TYPE": "ffmpeg",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "10.0.0.150:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is False
    assert "production workers require explicit WORKER_HOST" in decision.reasons


def test_enforce_raises_with_all_denial_reasons() -> None:
    with pytest.raises(WorkerAdmissionError) as exc:
        enforce_worker_admission_from_env(
            {
                "DEPLOY_MODE": "shared",
                "REDIS_URL": "redis://10.0.0.150:6380/0",
                "WORKER_TYPE": "ffmpeg",
                "STORAGE_BACKEND": "local",
            }
        )

    message = str(exc.value)
    assert "production workers require explicit WORKER_HOST" in message
    assert "production ffmpeg workers require STORAGE_BACKEND=minio" in message
