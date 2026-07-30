from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlparse

from app.services.worker_admission import is_production_worker_env


MAX_WORKER_SECRET_BYTES = 4096


class WorkerSecretError(RuntimeError):
    """A sanitized worker secret configuration failure."""


def read_mode_0400_secret(
    path: str | os.PathLike[str],
    *,
    label: str,
    maximum_bytes: int = MAX_WORKER_SECRET_BYTES,
) -> str:
    if maximum_bytes <= 0:
        raise ValueError("maximum_bytes must be positive")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        path_metadata = os.lstat(Path(path))
        if not stat.S_ISREG(path_metadata.st_mode):
            raise WorkerSecretError(f"{label} must be a regular file")
        if stat.S_IMODE(path_metadata.st_mode) != 0o400:
            raise WorkerSecretError(f"{label} must use mode 0400")
        descriptor = os.open(Path(path), flags)
    except WorkerSecretError:
        raise
    except OSError as exc:
        raise WorkerSecretError(f"{label} could not be opened") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_dev != path_metadata.st_dev
            or metadata.st_ino != path_metadata.st_ino
            or stat.S_IMODE(metadata.st_mode) != 0o400
        ):
            raise WorkerSecretError(f"{label} changed while being opened")
        if metadata.st_size > maximum_bytes:
            raise WorkerSecretError(f"{label} is too large")
        expected_size = metadata.st_size
        chunks: list[bytes] = []
        length = 0
        while length < expected_size:
            chunk = os.read(descriptor, expected_size - length)
            if not chunk:
                raise WorkerSecretError(f"{label} changed while being read")
            chunks.append(chunk)
            length += len(chunk)
        if os.read(descriptor, 1):
            raise WorkerSecretError(f"{label} changed while being read")

        final_metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(final_metadata.st_mode)
            or final_metadata.st_dev != metadata.st_dev
            or final_metadata.st_ino != metadata.st_ino
            or stat.S_IMODE(final_metadata.st_mode)
            != stat.S_IMODE(metadata.st_mode)
            or final_metadata.st_size != metadata.st_size
            or final_metadata.st_mtime_ns != metadata.st_mtime_ns
            or final_metadata.st_ctime_ns != metadata.st_ctime_ns
        ):
            raise WorkerSecretError(f"{label} changed while being read")
        value = b"".join(chunks)
    finally:
        os.close(descriptor)
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkerSecretError(f"{label} is not valid UTF-8") from exc
    if decoded.endswith("\n"):
        decoded = decoded[:-1]
    if not decoded or "\x00" in decoded:
        raise WorkerSecretError(f"{label} is empty or invalid")
    return decoded


def load_worker_database_url(env: Mapping[str, str]) -> str:
    if is_production_worker_env(env):
        if str(env.get("DATABASE_URL", "")).strip():
            raise WorkerSecretError(
                "production workers must not receive DATABASE_URL through the environment"
            )
        path = str(env.get("WORKER_DATABASE_URL_FILE", "")).strip()
        if not path:
            raise WorkerSecretError(
                "production workers require WORKER_DATABASE_URL_FILE"
            )
        return read_mode_0400_secret(path, label="worker database URL")

    path = str(env.get("WORKER_DATABASE_URL_FILE", "")).strip()
    if path:
        return read_mode_0400_secret(path, label="worker database URL")
    database_url = str(env.get("DATABASE_URL", "")).strip()
    if not database_url:
        raise WorkerSecretError("worker database URL is not configured")
    return database_url


def load_worker_admission_token(env: Mapping[str, str]) -> str:
    production = is_production_worker_env(env)
    environment_token = str(env.get("WORKER_ADMISSION_TOKEN", "")).strip()
    if production and environment_token:
        raise WorkerSecretError(
            "production workers must not receive WORKER_ADMISSION_TOKEN "
            "through the environment"
        )
    path = str(env.get("WORKER_ADMISSION_TOKEN_FILE", "")).strip()
    if not path:
        if production:
            raise WorkerSecretError(
                "production workers require WORKER_ADMISSION_TOKEN_FILE"
            )
        if not environment_token:
            raise WorkerSecretError("worker admission token is not configured")
        return environment_token
    return read_mode_0400_secret(path, label="worker admission token")


def load_worker_minio_credentials(
    env: Mapping[str, str],
) -> tuple[str, str]:
    production = is_production_worker_env(env)
    environment_access = str(env.get("MINIO_ACCESS_KEY", "")).strip()
    environment_secret = str(env.get("MINIO_SECRET_KEY", "")).strip()
    if production and (environment_access or environment_secret):
        raise WorkerSecretError(
            "production workers must not receive MINIO_ACCESS_KEY or "
            "MINIO_SECRET_KEY through the environment"
        )

    access_path = str(
        env.get("WORKER_MINIO_ACCESS_KEY_FILE", "")
    ).strip()
    secret_path = str(
        env.get("WORKER_MINIO_SECRET_KEY_FILE", "")
    ).strip()
    if access_path or secret_path:
        if not access_path or not secret_path or access_path == secret_path:
            raise WorkerSecretError(
                "worker MinIO credentials require independent secret files"
            )
        return (
            read_mode_0400_secret(
                access_path,
                label="worker MinIO access key",
            ),
            read_mode_0400_secret(
                secret_path,
                label="worker MinIO secret key",
            ),
        )
    if production:
        raise WorkerSecretError(
            "production workers require WORKER_MINIO_ACCESS_KEY_FILE and "
            "WORKER_MINIO_SECRET_KEY_FILE"
        )
    if not environment_access or not environment_secret:
        raise WorkerSecretError("worker MinIO credentials are not configured")
    return environment_access, environment_secret


def load_worker_redis_url(env: Mapping[str, str]) -> str:
    production = is_production_worker_env(env)
    environment_url = str(env.get("REDIS_URL", "")).strip()
    path = str(env.get("WORKER_REDIS_URL_FILE", "")).strip()
    if production:
        if environment_url:
            raise WorkerSecretError(
                "production workers must not receive REDIS_URL through the environment"
            )
        if not path:
            raise WorkerSecretError(
                "production workers require WORKER_REDIS_URL_FILE"
            )
        redis_url = read_mode_0400_secret(
            path,
            label="worker Redis URL",
        )
    elif path:
        redis_url = read_mode_0400_secret(
            path,
            label="worker Redis URL",
        )
    elif environment_url:
        redis_url = environment_url
    else:
        raise WorkerSecretError("worker Redis URL is not configured")

    try:
        parsed = urlparse(redis_url)
        port = parsed.port
    except ValueError as exc:
        raise WorkerSecretError(
            "worker Redis configuration is invalid"
        ) from exc
    if (
        parsed.scheme not in {"redis", "rediss"}
        or not parsed.hostname
        or port is None
        or not 1 <= port <= 65535
        or production
        and (not parsed.username or parsed.username == "default")
    ):
        raise WorkerSecretError("worker Redis configuration is invalid")
    return redis_url
