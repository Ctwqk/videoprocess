from __future__ import annotations

from app.config import settings
from app.storage import manager
from app.storage import minio_backend


def test_minio_cache_does_not_reuse_credentials_across_generations(
    monkeypatch,
) -> None:
    constructions: list[tuple[str, str]] = []

    class MinioBackend:
        def __init__(self, **kwargs) -> None:
            constructions.append(
                (kwargs["access_key"], kwargs["secret_key"])
            )

    monkeypatch.setattr(
        minio_backend,
        "MinioStorageBackend",
        MinioBackend,
    )
    monkeypatch.setattr(settings, "minio_endpoint", "vp-minio:9000")
    monkeypatch.setattr(settings, "minio_bucket", "videoprocess")
    monkeypatch.setattr(settings, "minio_access_key", "generation-one")
    monkeypatch.setattr(settings, "minio_secret_key", "secret-one")
    manager._backends.clear()

    first = manager.get_storage("minio", create_bucket=False)
    settings.minio_access_key = "generation-two"
    settings.minio_secret_key = "secret-two"
    second = manager.get_storage("minio", create_bucket=False)

    assert first is not second
    assert constructions == [
        ("generation-one", "secret-one"),
        ("generation-two", "secret-two"),
    ]
