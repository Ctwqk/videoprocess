from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
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
