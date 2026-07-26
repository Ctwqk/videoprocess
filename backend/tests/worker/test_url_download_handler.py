from __future__ import annotations

from pathlib import Path

import pytest

from worker.handlers.url_download import UrlDownloadHandler


@pytest.mark.asyncio
async def test_remote_cache_miss_defers_the_only_object_write_to_worker_fence(
    monkeypatch,
    tmp_path,
) -> None:
    handler = UrlDownloadHandler()
    output_path = tmp_path / "download.mp4"

    async def cache_miss(*args, **kwargs):
        return False

    async def download(*args, **kwargs):
        Path(output_path).write_bytes(b"video")

    async def unexpected_cache_save(*args, **kwargs):
        raise AssertionError("remote handler must not write outside lease fence")

    monkeypatch.setattr(
        "worker.handlers.url_download.settings.storage_backend",
        "minio",
    )
    monkeypatch.setattr(handler, "_restore_from_cache", cache_miss)
    monkeypatch.setattr(handler, "_download_via_ytdlp", download)
    monkeypatch.setattr(handler, "_save_to_cache", unexpected_cache_save)

    result = await handler.execute(
        {"url": "https://example.test/video", "format": "best"},
        {},
        str(output_path),
    )

    assert output_path.read_bytes() == b"video"
    assert result["cache_hit"] is False
    assert result.get("_skip_upload") is False
    assert "_storage_path" not in result
