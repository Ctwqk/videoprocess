from __future__ import annotations

import hashlib

from app.config import settings
from app.storage.base import StorageBackend
from app.storage.local import LocalStorageBackend


_backends: dict[str, tuple[tuple[object, ...], StorageBackend]] = {}


def get_storage(
    backend_name: str | None = None, *, create_bucket: bool = True
) -> StorageBackend:
    selected = (backend_name or settings.storage_backend or "local").strip().lower()
    identity = _backend_identity(selected, create_bucket=create_bucket)
    cached = _backends.get(selected)
    if cached is not None and cached[0] == identity:
        return cached[1]

    backend: StorageBackend
    if selected == "minio":
        from app.storage.minio_backend import MinioStorageBackend

        backend = MinioStorageBackend(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket=settings.minio_bucket,
            secure=settings.minio_secure,
            create_bucket=create_bucket,
            connect_timeout_seconds=(
                settings.minio_connect_timeout_seconds
            ),
            read_timeout_seconds=settings.minio_read_timeout_seconds,
            max_retries=settings.minio_max_retries,
            operation_timeout_seconds=(
                settings.minio_operation_timeout_seconds
            ),
        )
    else:
        backend = LocalStorageBackend(root=settings.storage_local_root)

    _backends[selected] = (identity, backend)
    return backend


def _backend_identity(
    selected: str,
    *,
    create_bucket: bool,
) -> tuple[object, ...]:
    if selected != "minio":
        return ("local", settings.storage_local_root)
    return (
        "minio",
        settings.minio_endpoint,
        hashlib.sha256(settings.minio_access_key.encode()).digest(),
        hashlib.sha256(settings.minio_secret_key.encode()).digest(),
        settings.minio_bucket,
        settings.minio_secure,
        create_bucket,
        settings.minio_connect_timeout_seconds,
        settings.minio_read_timeout_seconds,
        settings.minio_max_retries,
        settings.minio_operation_timeout_seconds,
    )
