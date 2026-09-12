from __future__ import annotations

import asyncio
import threading
import time

import pytest
from minio.error import S3Error

from app.storage.minio_backend import MinioStorageBackend


def _backend_with_client(client: object) -> MinioStorageBackend:
    backend = object.__new__(MinioStorageBackend)
    backend.client = client
    backend.bucket = "videoprocess"
    backend.operation_timeout_seconds = 5.0
    return backend


def _s3_error(code: str) -> S3Error:
    return S3Error(
        response=object(),
        code=code,
        message="storage failure",
        resource="health/deploy-readiness/probe",
        request_id="request-id",
        host_id="host-id",
    )


@pytest.mark.asyncio
async def test_exists_returns_false_for_definitive_missing_object() -> None:
    class MissingObjectClient:
        def stat_object(self, bucket: str, path: str) -> None:
            raise _s3_error("NoSuchKey")

    backend = _backend_with_client(MissingObjectClient())

    assert await backend.exists("health/deploy-readiness/probe") is False


@pytest.mark.asyncio
async def test_exists_propagates_transport_errors() -> None:
    class FailingClient:
        def stat_object(self, bucket: str, path: str) -> None:
            raise OSError("network failure")

    backend = _backend_with_client(FailingClient())

    with pytest.raises(OSError, match="network failure"):
        await backend.exists("health/deploy-readiness/probe")


@pytest.mark.asyncio
async def test_exists_propagates_non_missing_s3_errors() -> None:
    class UnauthorizedClient:
        def stat_object(self, bucket: str, path: str) -> None:
            raise _s3_error("AccessDenied")

    backend = _backend_with_client(UnauthorizedClient())

    with pytest.raises(S3Error):
        await backend.exists("health/deploy-readiness/probe")


def test_missing_bucket_does_not_create_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    import minio

    class FakeClient:
        def __init__(self) -> None:
            self.make_bucket_calls: list[str] = []

        def bucket_exists(self, bucket: str) -> bool:
            return False

        def make_bucket(self, bucket: str) -> None:
            self.make_bucket_calls.append(bucket)

    client = FakeClient()
    monkeypatch.setattr(minio, "Minio", lambda *args, **kwargs: client)

    with pytest.raises(RuntimeError):
        MinioStorageBackend(
            endpoint="minio:9000",
            access_key="access",
            secret_key="secret",
            bucket="videoprocess",
            create_bucket=False,
        )

    assert client.make_bucket_calls == []


def test_bucket_check_uncertainty_does_not_create_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import minio

    class FakeClient:
        def __init__(self) -> None:
            self.make_bucket_calls: list[str] = []

        def bucket_exists(self, bucket: str) -> bool:
            raise OSError("bucket check uncertain")

        def make_bucket(self, bucket: str) -> None:
            self.make_bucket_calls.append(bucket)

    client = FakeClient()
    monkeypatch.setattr(minio, "Minio", lambda *args, **kwargs: client)

    with pytest.raises(OSError, match="bucket check uncertain"):
        MinioStorageBackend(
            endpoint="minio:9000",
            access_key="access",
            secret_key="secret",
            bucket="videoprocess",
            create_bucket=False,
        )

    assert client.make_bucket_calls == []


def test_existing_constructor_still_creates_missing_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import minio

    class FakeClient:
        def __init__(self) -> None:
            self.make_bucket_calls: list[str] = []

        def bucket_exists(self, bucket: str) -> bool:
            return False

        def make_bucket(self, bucket: str) -> None:
            self.make_bucket_calls.append(bucket)

    client = FakeClient()
    monkeypatch.setattr(minio, "Minio", lambda *args, **kwargs: client)

    MinioStorageBackend(
        endpoint="minio:9000",
        access_key="access",
        secret_key="secret",
        bucket="videoprocess",
    )

    assert client.make_bucket_calls == ["videoprocess"]


def test_constructor_configures_bounded_http_deadlines_and_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import minio

    captured: dict[str, object] = {}

    class FakeClient:
        def bucket_exists(self, bucket: str) -> bool:
            return True

    def create_client(*args, **kwargs):
        captured.update(kwargs)
        return FakeClient()

    monkeypatch.setattr(minio, "Minio", create_client)

    backend = MinioStorageBackend(
        endpoint="minio:9000",
        access_key="access",
        secret_key="secret",
        bucket="videoprocess",
        connect_timeout_seconds=2.5,
        read_timeout_seconds=7.5,
        max_retries=2,
        operation_timeout_seconds=12.0,
    )

    http_client = captured["http_client"]
    timeout = http_client.connection_pool_kw["timeout"]
    retries = http_client.connection_pool_kw["retries"]
    assert timeout.connect_timeout == 2.5
    assert timeout.read_timeout == 7.5
    assert retries.total == 2
    assert backend.operation_timeout_seconds == 12.0


def test_hung_save_does_not_block_asyncio_run_shutdown(tmp_path) -> None:
    upload_started = threading.Event()
    release_upload = threading.Event()

    class HungClient:
        def fput_object(self, bucket, path, local_path):
            upload_started.set()
            release_upload.wait()

    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    backend = _backend_with_client(HungClient())
    backend.operation_timeout_seconds = 0.05

    async def run_save() -> None:
        with source.open("rb") as input_file:
            with pytest.raises(
                TimeoutError,
                match="exceeded its deadline",
            ):
                await backend.save(
                    "staging/artifacts/job/claim/output.mp4",
                    input_file,
                )

    started_at = time.monotonic()
    try:
        asyncio.run(run_save())
        elapsed = time.monotonic() - started_at
        assert upload_started.is_set()
        assert elapsed < 0.5
    finally:
        release_upload.set()


@pytest.mark.asyncio
async def test_cancelled_save_waits_for_background_upload_to_settle(
    tmp_path,
) -> None:
    upload_started = threading.Event()
    allow_upload_to_finish = threading.Event()
    uploaded: list[str] = []
    stored_objects: set[str] = set()
    operations: list[str] = []

    class SlowClient:
        def fput_object(self, bucket, path, local_path):
            upload_started.set()
            assert allow_upload_to_finish.wait(timeout=5)
            uploaded.append(path)
            stored_objects.add(path)
            operations.append("upload")

        def remove_object(self, bucket, path):
            stored_objects.discard(path)
            operations.append("cleanup")

    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    backend = _backend_with_client(SlowClient())

    async def save_then_cleanup(input_file):
        try:
            return await backend.save(
                "staging/claim-output.mp4",
                input_file,
            )
        finally:
            await backend.delete("staging/claim-output.mp4")

    with source.open("rb") as input_file:
        save = asyncio.create_task(
            save_then_cleanup(input_file)
        )
        assert await asyncio.to_thread(upload_started.wait, 2)
        save.cancel()
        await asyncio.sleep(0)
        assert not save.done()
        allow_upload_to_finish.set()
        with pytest.raises(asyncio.CancelledError):
            await save

    assert uploaded == ["staging/claim-output.mp4"]
    assert operations == ["upload", "cleanup"]
    assert stored_objects == set()


@pytest.mark.asyncio
async def test_repeated_cancel_converges_at_bound_and_late_write_is_staging_only(
    tmp_path,
) -> None:
    upload_started = threading.Event()
    release_upload = threading.Event()
    upload_settled = threading.Event()
    stored_objects: set[str] = set()

    class HungThenSettledClient:
        def fput_object(self, bucket, path, local_path):
            upload_started.set()
            release_upload.wait(timeout=5)
            stored_objects.add(path)
            upload_settled.set()

    source = tmp_path / "source.mp4"
    source.write_bytes(b"video")
    backend = _backend_with_client(HungThenSettledClient())
    backend.operation_timeout_seconds = 0.05
    staging_path = "staging/artifacts/job/claim/output.mp4"

    try:
        with source.open("rb") as input_file:
            save = asyncio.create_task(backend.save(staging_path, input_file))
            assert await asyncio.to_thread(upload_started.wait, 1)
            save.cancel()
            save.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(save, timeout=0.5)
        assert stored_objects == set()
    finally:
        release_upload.set()
        assert await asyncio.to_thread(upload_settled.wait, 1)

    assert stored_objects == {staging_path}
    assert all(path.startswith("staging/artifacts/") for path in stored_objects)
