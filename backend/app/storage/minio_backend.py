from __future__ import annotations
import asyncio
import io
import os
from typing import BinaryIO

from minio.error import S3Error

from app.storage.base import StorageBackend


class MinioStorageBackend(StorageBackend):
    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 bucket: str, secure: bool = False,
                 create_bucket: bool = True) -> None:
        from minio import Minio
        self.client = Minio(endpoint, access_key=access_key,
                           secret_key=secret_key, secure=secure)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            if not create_bucket:
                raise RuntimeError("MinIO bucket is missing")
            self.client.make_bucket(bucket)

    async def save(self, path: str, data: BinaryIO) -> int:
        local_path = getattr(data, "name", None)
        if isinstance(local_path, str) and local_path and os.path.exists(local_path):
            size = os.path.getsize(local_path)
            await _settle_thread_operation_before_cancellation(
                self.client.fput_object,
                self.bucket,
                path,
                local_path,
            )
            return size

        content = data.read()
        size = len(content)
        await _settle_thread_operation_before_cancellation(
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
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                return False
            raise

    def get_local_path(self, path: str) -> str | None:
        return None  # MinIO has no local path


async def _settle_thread_operation_before_cancellation(
    function,
    *args,
):
    operation = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                continue
        operation.result()
        raise
