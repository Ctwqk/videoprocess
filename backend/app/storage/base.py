from __future__ import annotations
from abc import ABC, abstractmethod
from typing import BinaryIO


class StorageBackend(ABC):
    @abstractmethod
    async def save(self, path: str, data: BinaryIO) -> int:
        """Save data to path. Returns file size in bytes."""
        ...

    @abstractmethod
    async def read(self, path: str) -> bytes:
        """Read file content."""
        ...

    async def read_bounded(self, path: str, max_bytes: int) -> bytes:
        """Read at most max_bytes, failing without allocating an unbounded blob."""
        raise NotImplementedError("bounded_storage_read_unsupported")

    @abstractmethod
    async def delete(self, path: str) -> None:
        """Delete a file."""
        ...

    @abstractmethod
    async def exists(self, path: str) -> bool:
        """Check if file exists."""
        ...

    @abstractmethod
    def get_local_path(self, path: str) -> str | None:
        """Return local filesystem path if available (for ffmpeg), else None."""
        ...
