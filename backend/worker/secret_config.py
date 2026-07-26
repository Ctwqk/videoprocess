from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path

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
        chunks: list[bytes] = []
        length = 0
        while length <= maximum_bytes:
            chunk = os.read(
                descriptor,
                maximum_bytes + 1 - length,
            )
            if not chunk:
                break
            chunks.append(chunk)
            length += len(chunk)
        value = b"".join(chunks)
        if len(value) > maximum_bytes:
            raise WorkerSecretError(f"{label} is too large")
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
    path = str(env.get("WORKER_ADMISSION_TOKEN_FILE", "")).strip()
    if not path:
        if is_production_worker_env(env):
            raise WorkerSecretError(
                "production workers require WORKER_ADMISSION_TOKEN_FILE"
            )
        token = str(env.get("WORKER_ADMISSION_TOKEN", "")).strip()
        if not token:
            raise WorkerSecretError("worker admission token is not configured")
        return token
    return read_mode_0400_secret(path, label="worker admission token")
