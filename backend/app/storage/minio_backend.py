from __future__ import annotations
import asyncio
import io
import math
import os
import threading
from contextlib import suppress
from functools import partial
from typing import Any, BinaryIO

import urllib3
from minio.error import S3Error

from app.storage.base import StorageBackend


class MinioStorageBackend(StorageBackend):
    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 bucket: str, secure: bool = False,
                 create_bucket: bool = True,
                 connect_timeout_seconds: float = 5.0,
                 read_timeout_seconds: float = 120.0,
                 max_retries: int = 2,
                 operation_timeout_seconds: float = 180.0) -> None:
        from minio import Minio
        if (
            not math.isfinite(connect_timeout_seconds)
            or connect_timeout_seconds <= 0
            or not math.isfinite(read_timeout_seconds)
            or read_timeout_seconds <= 0
            or type(max_retries) is not int
            or max_retries < 0
            or not math.isfinite(operation_timeout_seconds)
            or operation_timeout_seconds <= 0
        ):
            raise ValueError("MinIO timeout and retry settings are invalid")
        http_client = urllib3.PoolManager(
            timeout=urllib3.Timeout(
                connect=connect_timeout_seconds,
                read=read_timeout_seconds,
            ),
            retries=urllib3.Retry(
                total=max_retries,
                connect=max_retries,
                read=max_retries,
                redirect=0,
                status=0,
                backoff_factor=0.2,
            ),
        )
        self.client = Minio(endpoint, access_key=access_key,
                           secret_key=secret_key, secure=secure,
                           http_client=http_client)
        self.bucket = bucket
        self.operation_timeout_seconds = operation_timeout_seconds
        if not self.client.bucket_exists(bucket):
            if not create_bucket:
                raise RuntimeError("MinIO bucket is missing")
            self.client.make_bucket(bucket)

    async def save(self, path: str, data: BinaryIO) -> int:
        local_path = getattr(data, "name", None)
        if isinstance(local_path, str) and local_path and os.path.exists(local_path):
            size = os.path.getsize(local_path)
            await _run_bounded_daemon_operation(
                self.client.fput_object,
                self.bucket,
                path,
                local_path,
                timeout_seconds=self.operation_timeout_seconds,
            )
            return size

        content = data.read()
        size = len(content)
        await _run_bounded_daemon_operation(
            self.client.put_object,
            self.bucket,
            path,
            io.BytesIO(content),
            size,
            timeout_seconds=self.operation_timeout_seconds,
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


async def _run_bounded_daemon_operation(
    function,
    *args,
    timeout_seconds: float,
):
    loop = asyncio.get_running_loop()
    operation = loop.create_future()

    def invoke() -> None:
        try:
            result = function(*args)
        except BaseException as exc:
            completion = partial(
                _finish_daemon_operation,
                operation,
                error=exc,
            )
        else:
            completion = partial(
                _finish_daemon_operation,
                operation,
                result=result,
            )
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(completion)

    threading.Thread(
        target=invoke,
        name="vp-minio-save",
        daemon=True,
    ).start()
    deadline = loop.time() + timeout_seconds
    try:
        return await asyncio.wait_for(
            asyncio.shield(operation),
            timeout=timeout_seconds,
        )
    except TimeoutError:
        operation.add_done_callback(_consume_background_result)
        raise TimeoutError("MinIO SDK operation exceeded its deadline")
    except asyncio.CancelledError:
        current = asyncio.current_task()
        settled = False
        while not operation.done():
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(
                    asyncio.shield(operation),
                    timeout=remaining,
                )
            except asyncio.CancelledError:
                if current is not None:
                    current.uncancel()
                continue
            except TimeoutError:
                break
        if operation.done():
            operation.result()
            settled = True
        if not settled:
            operation.add_done_callback(_consume_background_result)
        raise


def _consume_background_result(operation: asyncio.Future[Any]) -> None:
    with suppress(asyncio.CancelledError, Exception):
        operation.exception()


def _finish_daemon_operation(
    operation: asyncio.Future[Any],
    *,
    result: Any = None,
    error: BaseException | None = None,
) -> None:
    if operation.done():
        return
    if error is not None:
        operation.set_exception(error)
    else:
        operation.set_result(result)
