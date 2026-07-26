from __future__ import annotations

from pathlib import Path
from typing import BinaryIO

import pytest
import httpx

from app.services import worker_storage_readiness
from app.services.worker_storage_readiness import (
    ReadinessFailure,
    artifact_api_health_url,
    probe_worker_storage,
)
from app.storage.base import StorageBackend


class FakeStorageBackend(StorageBackend):
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.save_paths: list[str] = []
        self.read_paths: list[str] = []
        self.delete_paths: list[str] = []
        self.exists_paths: list[str] = []
        self.save_error: Exception | None = None
        self.read_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.exists_error: Exception | None = None
        self.read_content: bytes | None = None
        self.keep_after_delete = False

    async def save(self, path: str, data: BinaryIO) -> int:
        self.save_paths.append(path)
        if self.save_error is not None:
            raise self.save_error
        content = data.read()
        self.objects[path] = content
        return len(content)

    async def read(self, path: str) -> bytes:
        self.read_paths.append(path)
        if self.read_error is not None:
            raise self.read_error
        if self.read_content is not None:
            return self.read_content
        return self.objects[path]

    async def delete(self, path: str) -> None:
        self.delete_paths.append(path)
        if self.delete_error is not None:
            raise self.delete_error
        if self.keep_after_delete:
            return
        self.objects.pop(path, None)

    async def exists(self, path: str) -> bool:
        self.exists_paths.append(path)
        if self.exists_error is not None:
            raise self.exists_error
        return path in self.objects

    def get_local_path(self, path: str) -> str | None:
        return None


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class FakeAsyncHttpClient:
    def __init__(
        self,
        *,
        status_code: int = 200,
        error: Exception | None = None,
        **kwargs: object,
    ) -> None:
        self.status_code = status_code
        self.error = error
        self.kwargs = kwargs
        self.urls: list[str] = []

    async def __aenter__(self) -> FakeAsyncHttpClient:
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        return None

    async def get(self, url: str) -> FakeResponse:
        self.urls.append(url)
        if self.error is not None:
            raise self.error
        return FakeResponse(self.status_code)


def _client_factory(
    *, status_code: int = 200, error: Exception | None = None
) -> tuple[list[FakeAsyncHttpClient], object]:
    clients: list[FakeAsyncHttpClient] = []

    def factory(**kwargs: object) -> FakeAsyncHttpClient:
        client = FakeAsyncHttpClient(
            status_code=status_code, error=error, **kwargs
        )
        clients.append(client)
        return client

    return clients, factory


def _api_env(tmp_path: Path, base_url: str) -> dict[str, str]:
    return {
        "STORAGE_BACKEND": "minio",
        "STORAGE_LOCAL_ROOT": str(tmp_path),
        "VP_ARTIFACT_DOWNLOAD_BASE_URL": base_url,
    }


async def test_probe_round_trips_scratch_and_minio(tmp_path: Path) -> None:
    fake_storage = FakeStorageBackend()

    result = await probe_worker_storage(
        {
            "STORAGE_BACKEND": "minio",
            "STORAGE_LOCAL_ROOT": str(tmp_path),
        },
        require_artifact_api=False,
        storage=fake_storage,
    )

    assert result == {
        "status": "ready",
        "components": {
            "scratch": "ready",
            "minio": "ready",
            "artifact_api": "not_required",
            "staging_janitor": "not_required",
        },
    }
    assert fake_storage.objects == {}
    assert list((tmp_path / "deploy-readiness").iterdir()) == []


async def test_scratch_mismatch_raises_stable_code_and_cleans_up(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(worker_storage_readiness, "_read_scratch_probe", lambda path: b"wrong")

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=FakeStorageBackend(),
        )

    assert failure.value.code == "scratch_mismatch"
    assert list((tmp_path / "deploy-readiness").iterdir()) == []


@pytest.mark.parametrize(
    ("attribute", "code"),
    [
        ("save_error", "minio_unavailable"),
        ("read_error", "minio_unavailable"),
        ("delete_error", "cleanup_failed"),
        ("exists_error", "cleanup_failed"),
    ],
)
async def test_minio_operation_failure_has_stable_code_and_attempts_cleanup(
    tmp_path: Path, attribute: str, code: str
) -> None:
    fake_storage = FakeStorageBackend()
    setattr(fake_storage, attribute, OSError("backend failed"))

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=fake_storage,
        )

    assert failure.value.code == code
    assert len(fake_storage.delete_paths) == 1


async def test_minio_mismatch_has_stable_code_and_attempts_cleanup(tmp_path: Path) -> None:
    fake_storage = FakeStorageBackend()
    fake_storage.read_content = b"wrong"

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=fake_storage,
        )

    assert failure.value.code == "minio_mismatch"
    assert len(fake_storage.delete_paths) == 1


async def test_minio_object_remaining_after_delete_is_cleanup_failure(tmp_path: Path) -> None:
    fake_storage = FakeStorageBackend()
    fake_storage.keep_after_delete = True

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=fake_storage,
        )

    assert failure.value.code == "cleanup_failed"
    assert len(fake_storage.delete_paths) == 1
    assert len(fake_storage.exists_paths) == 1


async def test_local_storage_backend_is_configuration_invalid(tmp_path: Path) -> None:
    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "local",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=FakeStorageBackend(),
        )

    assert failure.value.code == "configuration_invalid"


async def test_empty_storage_root_is_configuration_invalid() -> None:
    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": "",
            },
            require_artifact_api=False,
            storage=FakeStorageBackend(),
        )

    assert failure.value.code == "configuration_invalid"


async def test_readiness_requests_minio_without_bucket_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_storage = FakeStorageBackend()
    calls: list[tuple[str, bool]] = []

    def get_storage_without_creation(name: str, *, create_bucket: bool) -> FakeStorageBackend:
        calls.append((name, create_bucket))
        return fake_storage

    monkeypatch.setattr(
        "app.storage.manager.get_storage", get_storage_without_creation
    )

    result = await probe_worker_storage(
        {
            "STORAGE_BACKEND": "minio",
            "STORAGE_LOCAL_ROOT": str(tmp_path),
        },
        require_artifact_api=False,
    )

    assert result["status"] == "ready"
    assert calls == [("minio", False)]


@pytest.mark.parametrize(
    "bucket_error", [RuntimeError("bucket missing"), OSError("bucket check uncertain")]
)
async def test_minio_bucket_setup_failure_is_unavailable_without_creation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bucket_error: Exception,
) -> None:
    calls: list[tuple[str, bool]] = []

    def get_storage_without_creation(name: str, *, create_bucket: bool) -> FakeStorageBackend:
        calls.append((name, create_bucket))
        raise bucket_error

    monkeypatch.setattr(
        "app.storage.manager.get_storage", get_storage_without_creation
    )

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
        )

    assert failure.value.code == "minio_unavailable"
    assert calls == [("minio", False)]


def test_artifact_api_health_url_removes_api_prefix() -> None:
    assert (
        artifact_api_health_url("http://vp-api-swarm:8080/api/v1")
        == "http://vp-api-swarm:8080/health"
    )


async def test_artifact_api_200_is_ready_with_five_second_no_redirect_client(
    tmp_path: Path,
) -> None:
    fake_storage = FakeStorageBackend()
    clients, factory = _client_factory()

    result = await probe_worker_storage(
        _api_env(tmp_path, "http://vp-api-swarm:8080/api/v1"),
        require_artifact_api=True,
        storage=fake_storage,
        http_client_factory=factory,
    )

    assert result["components"] == {
        "scratch": "ready",
        "minio": "ready",
        "artifact_api": "ready",
        "staging_janitor": "not_required",
    }
    assert len(clients) == 1
    assert clients[0].urls == ["http://vp-api-swarm:8080/health"]
    assert clients[0].kwargs["follow_redirects"] is False
    assert clients[0].kwargs["trust_env"] is False
    timeout = clients[0].kwargs["timeout"]
    assert isinstance(timeout, httpx.Timeout)
    assert timeout.connect == 5.0
    assert timeout.read == 5.0
    assert timeout.write == 5.0
    assert timeout.pool == 5.0


async def test_artifact_api_503_is_api_unavailable(tmp_path: Path) -> None:
    clients, factory = _client_factory(status_code=503)

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            _api_env(tmp_path, "http://vp-api-swarm:8080/api/v1"),
            require_artifact_api=True,
            storage=FakeStorageBackend(),
            http_client_factory=factory,
        )

    assert failure.value.code == "api_unavailable"
    assert len(clients) == 1


async def test_artifact_api_timeout_is_api_unavailable(tmp_path: Path) -> None:
    clients, factory = _client_factory(error=httpx.ReadTimeout("timed out"))

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            _api_env(tmp_path, "http://vp-api-swarm:8080/api/v1"),
            require_artifact_api=True,
            storage=FakeStorageBackend(),
            http_client_factory=factory,
        )

    assert failure.value.code == "api_unavailable"
    assert len(clients) == 1


@pytest.mark.parametrize(
    "base_url",
    [
        "not-a-url",
        "ftp://vp-api:8080/api/v1",
        "https://vp-api-swarm:8080/api/v1",
        "http://api.example:8080/api/v1",
        "http://10.0.0.126:8080/api/v1",
        "http://CASPERs-Mac-mini:8080/api/v1",
        "http://colima-swarmbridged:8080/api/v1",
        "http://user:pass@vp-api-swarm:8080/api/v1",
        "http://vp-api-swarm:9000/api/v1",
        "http://vp-api-swarm:8080/api/v1?token=secret",
        "http://vp-api-swarm:8080/api/v1#fragment",
        "http://vp-api-swarm:8080/not-api-v1",
    ],
)
async def test_invalid_artifact_api_base_url_is_api_unavailable(
    tmp_path: Path, base_url: str
) -> None:
    clients, factory = _client_factory()

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            _api_env(tmp_path, base_url),
            require_artifact_api=True,
            storage=FakeStorageBackend(),
            http_client_factory=factory,
        )

    assert failure.value.code == "api_unavailable"
    assert clients == []


async def test_scratch_failure_does_not_expose_exception_chain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def read_failed(path: Path) -> bytes:
        raise OSError("scratch secret")

    monkeypatch.setattr(worker_storage_readiness, "_read_scratch_probe", read_failed)

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=FakeStorageBackend(),
        )

    assert failure.value.code == "scratch_unavailable"
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


async def test_minio_failure_does_not_expose_exception_chain(tmp_path: Path) -> None:
    fake_storage = FakeStorageBackend()
    fake_storage.save_error = OSError("minio secret")

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=fake_storage,
        )

    assert failure.value.code == "minio_unavailable"
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


async def test_cleanup_failure_does_not_expose_exception_chain(tmp_path: Path) -> None:
    fake_storage = FakeStorageBackend()
    fake_storage.exists_error = OSError("cleanup secret")

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
            },
            require_artifact_api=False,
            storage=fake_storage,
        )

    assert failure.value.code == "cleanup_failed"
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


async def test_api_failure_does_not_expose_exception_chain(tmp_path: Path) -> None:
    clients, factory = _client_factory(error=httpx.ReadTimeout("api secret"))

    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            _api_env(tmp_path, "http://vp-api-swarm:8080/api/v1"),
            require_artifact_api=True,
            storage=FakeStorageBackend(),
            http_client_factory=factory,
        )

    assert failure.value.code == "api_unavailable"
    assert clients
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


async def test_artifact_api_is_not_constructed_when_not_required(tmp_path: Path) -> None:
    clients, factory = _client_factory()

    result = await probe_worker_storage(
        _api_env(tmp_path, "not-a-url"),
        require_artifact_api=False,
        storage=FakeStorageBackend(),
        http_client_factory=factory,
    )

    assert result["components"]["artifact_api"] == "not_required"
    assert clients == []


async def test_required_staging_janitor_must_have_fresh_success_status(
    tmp_path: Path,
) -> None:
    with pytest.raises(ReadinessFailure) as failure:
        await probe_worker_storage(
            {
                "STORAGE_BACKEND": "minio",
                "STORAGE_LOCAL_ROOT": str(tmp_path),
                "VP_REQUIRE_STAGING_JANITOR": "true",
                "VP_STAGING_JANITOR_STATUS_FILE": str(
                    tmp_path / "missing.json"
                ),
            },
            require_artifact_api=False,
            storage=FakeStorageBackend(),
        )

    assert failure.value.code == "staging_janitor_unavailable"


async def test_required_staging_janitor_is_reported_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    status_file = tmp_path / "janitor.json"
    status_file.write_text("{}")
    monkeypatch.setattr(
        worker_storage_readiness,
        "staging_janitor_ready",
        lambda path, *, max_age_seconds: (
            path == status_file and max_age_seconds == 900
        ),
        raising=False,
    )

    result = await probe_worker_storage(
        {
            "STORAGE_BACKEND": "minio",
            "STORAGE_LOCAL_ROOT": str(tmp_path),
            "VP_REQUIRE_STAGING_JANITOR": "true",
            "VP_STAGING_JANITOR_STATUS_FILE": str(status_file),
        },
        require_artifact_api=False,
        storage=FakeStorageBackend(),
    )

    assert result["components"]["staging_janitor"] == "ready"
