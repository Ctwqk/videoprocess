from __future__ import annotations
from pathlib import Path
from typing import BinaryIO

import aiofiles

from app.storage.base import StorageBackend


class LocalStorageBackend(StorageBackend):
    def __init__(self, root: str) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _full_path(self, path: str) -> Path:
        return self.root / path

    async def save(self, path: str, data: BinaryIO) -> int:
        full_path = self._full_path(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        size = 0
        async with aiofiles.open(full_path, "wb") as f:
            while chunk := data.read(8192):
                await f.write(chunk)
                size += len(chunk)
        return size

    async def read(self, path: str) -> bytes:
        full_path = self._full_path(path)
        async with aiofiles.open(full_path, "rb") as f:
            return await f.read()

    async def read_bounded(self, path: str, max_bytes: int) -> bytes:
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("storage_read_limit_invalid")
        async with aiofiles.open(self._full_path(path), "rb") as handle:
            content = await handle.read(max_bytes + 1)
        if len(content) > max_bytes:
            raise ValueError("storage_read_limit_exceeded")
        return content

    async def delete(self, path: str) -> None:
        full_path = self._full_path(path)
        if full_path.is_file():
            full_path.unlink()

    async def exists(self, path: str) -> bool:
        return self._full_path(path).is_file()

    def get_local_path(self, path: str) -> str | None:
        return str(self._full_path(path))
