from __future__ import annotations
import asyncio
import io
import os
from typing import BinaryIO

from app.storage.base import StorageBackend


class MinioStorageBackend(StorageBackend):
    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 bucket: str, secure: bool = False) -> None:
        from minio import Minio
        self.client = Minio(endpoint, access_key=access_key,
                           secret_key=secret_key, secure=secure)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    async def save(self, path: str, data: BinaryIO) -> int:
        local_path = getattr(data, "name", None)
        if isinstance(local_path, str) and local_path and os.path.exists(local_path):
            size = os.path.getsize(local_path)
            await asyncio.to_thread(self.client.fput_object, self.bucket, path, local_path)
            return size

        content = data.read()
        size = len(content)
        await asyncio.to_thread(
            self.client.put_object,
            self.bucket,
            path,
            io.BytesIO(content),
            size,
        )
        return size

    async def read(self, path: str) -> bytes:
        response = await asyncio.to_thread(self.client.get_object, self.bucket, path)
        try:
            return await asyncio.to_thread(response.read)
        finally:
            response.close()
            response.release_conn()

    async def delete(self, path: str) -> None:
        await asyncio.to_thread(self.client.remove_object, self.bucket, path)

    async def exists(self, path: str) -> bool:
        try:
            await asyncio.to_thread(self.client.stat_object, self.bucket, path)
            return True
        except Exception:
            return False

    def get_local_path(self, path: str) -> str | None:
        return None  # MinIO has no local path
