from __future__ import annotations

from pathlib import Path

import pytest

from worker.handlers.url_download import UrlDownloadHandler


@pytest.mark.asyncio
async def test_handler_never_reads_or_writes_remote_cache(
    monkeypatch,
    tmp_path,
) -> None:
    handler = UrlDownloadHandler()
    output_path = tmp_path / "download.mp4"

    async def download(*args, **kwargs):
        Path(output_path).write_bytes(b"video")

    async def unexpected_cache_io(*args, **kwargs):
        raise AssertionError("handler cache I/O is disabled")

    monkeypatch.setattr(
        "worker.handlers.url_download.settings.storage_backend",
        "minio",
    )
    monkeypatch.setattr(
        handler,
        "_restore_from_cache",
        unexpected_cache_io,
        raising=False,
    )
    monkeypatch.setattr(handler, "_download_via_ytdlp", download)
    monkeypatch.setattr(
        handler,
        "_save_to_cache",
        unexpected_cache_io,
        raising=False,
    )

    result = await handler.execute(
        {"url": "https://example.test/video", "format": "best"},
        {},
        str(output_path),
    )

    assert output_path.read_bytes() == b"video"
    assert "cache_hit" not in result
    assert "_skip_upload" not in result
    assert "_storage_path" not in result
