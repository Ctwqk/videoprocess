from __future__ import annotations
from app.config import settings
from app.storage.base import StorageBackend
from app.storage.local import LocalStorageBackend


_backends: dict[str, StorageBackend] = {}


def get_storage(backend_name: str | None = None) -> StorageBackend:
    selected = (backend_name or settings.storage_backend or "local").strip().lower()
    backend = _backends.get(selected)
    if backend is not None:
        return backend

    if selected == "minio":
        from app.storage.minio_backend import MinioStorageBackend

        backend = MinioStorageBackend(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket=settings.minio_bucket,
            secure=settings.minio_secure,
        )
    else:
        backend = LocalStorageBackend(root=settings.storage_local_root)

    _backends[selected] = backend
    return backend
