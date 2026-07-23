from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.worker_admission import WorkerAdmissionError
from worker import main as worker_main


def test_worker_database_is_not_configured_at_import() -> None:
    assert worker_main.engine_db is None
    assert worker_main.worker_session is None


@pytest.mark.asyncio
async def test_process_task_downloads_missing_local_artifact_through_api(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    expected_content = b"cross-node-owned-video"
    missing_local_path = tmp_path / "gpu-scratch" / "assets" / "input.mp4"
    handled_inputs: list[bytes] = []
    handled_paths: list[str] = []
    requested: list[tuple[str, str]] = []
    succeeded: list[tuple[str, str, str]] = []
    failed: list[tuple[str, str, str]] = []

    class CopyHandler:
        async def execute(self, config, input_paths, output_path):
            input_path = input_paths["input"]
            handled_paths.append(input_path)
            handled_inputs.append(Path(input_path).read_bytes())
            Path(output_path).write_bytes(expected_content)
            return {}

        def cancel(self) -> None:
            return None

    input_artifact = SimpleNamespace(
        id=input_artifact_id,
        job_id=job_id,
        media_info={"content_sha256": hashlib.sha256(expected_content).hexdigest()},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=len(expected_content),
    )

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def get(self, model, item_id):
            if model is worker_main.Artifact and item_id == input_artifact_id:
                return input_artifact
            return None

        def add(self, item) -> None:
            return None

        async def flush(self) -> None:
            return None

        async def commit(self) -> None:
            return None

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            assert path == "assets/input.mp4"
            return str(missing_local_path)

        async def read(self, path: str) -> bytes:
            raise AssertionError(f"local artifact must use the authoritative API: {path}")

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            yield expected_content[:7]
            yield expected_content[7:]

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            requested.append((method, url))
            return DownloadResponse()

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs) -> bool:
        return True

    async def report_success(job: str, node: str, artifact: str) -> None:
        succeeded.append((job, node, artifact))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": CopyHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(
        worker_main,
        "ARTIFACT_DOWNLOAD_BASE_URL",
        "http://vp-api-swarm:8080/api/v1",
        raising=False,
    )
    monkeypatch.setattr(worker_main.settings, "storage_backend", "local")
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": json.dumps({"prompt": "owned canary"}),
            "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
        }
    )

    assert requested == [
        (
            "GET",
            f"http://vp-api-swarm:8080/api/v1/artifacts/{input_artifact_id}/download",
        )
    ]
    assert handled_inputs == [expected_content]
    assert len(succeeded) == 1
    assert failed == []
    assert handled_paths and not Path(handled_paths[0]).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("file_size", (5, None))
async def test_artifact_api_download_stops_at_size_limit_and_cleans_temp_file(
    monkeypatch,
    tmp_path: Path,
    file_size: int | None,
) -> None:
    artifact_id = uuid.uuid4()
    temp_path = tmp_path / "bounded-download.mp4"
    yielded_chunks: list[int] = []
    client_kwargs: dict = {}
    artifact = SimpleNamespace(
        id=artifact_id,
        filename="input.mp4",
        file_size=file_size,
        media_info={},
    )

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            yielded_chunks.append(6)
            yield b"123456"
            yielded_chunks.append(1)
            yield b"7"

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            client_kwargs.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            return DownloadResponse()

    def make_temp_file(*, suffix: str, prefix: str):
        fd = os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        return fd, str(temp_path)

    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(worker_main.tempfile, "mkstemp", make_temp_file)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_MAX_BYTES", 5, raising=False)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS", 30.0, raising=False)

    with pytest.raises(RuntimeError, match="exceeds"):
        await worker_main._download_artifact_via_api(artifact)

    assert yielded_chunks == [6]
    assert not temp_path.exists()
    assert client_kwargs["follow_redirects"] is False


@pytest.mark.asyncio
async def test_artifact_api_download_has_total_deadline_and_cleans_temp_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    temp_path = tmp_path / "timed-out-download.mp4"
    artifact = SimpleNamespace(
        id=uuid.uuid4(),
        filename="input.mp4",
        file_size=10,
        media_info={},
    )

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            await asyncio.sleep(60)
            yield b"never"

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            return DownloadResponse()

    def make_temp_file(*, suffix: str, prefix: str):
        fd = os.open(temp_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        return fd, str(temp_path)

    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(worker_main.tempfile, "mkstemp", make_temp_file)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_MAX_BYTES", 100, raising=False)
    monkeypatch.setattr(worker_main, "ARTIFACT_DOWNLOAD_TOTAL_TIMEOUT_SECONDS", 0.01, raising=False)

    with pytest.raises(RuntimeError, match="timed out"):
        await asyncio.wait_for(worker_main._download_artifact_via_api(artifact), timeout=0.2)

    assert not temp_path.exists()


@pytest.mark.asyncio
async def test_process_task_cancels_cross_node_download_after_closing_database_session(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    missing_local_path = tmp_path / "gpu-scratch" / "assets" / "input.mp4"
    download_started = asyncio.Event()
    session_active = False
    session_state_during_stream: list[bool] = []
    handler_calls: list[str] = []
    succeeded: list[tuple[str, str, str]] = []
    failed: list[tuple[str, str, str]] = []

    class NeverRunHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            raise AssertionError("cancelled node must not execute its handler")

        def cancel(self) -> None:
            handler_calls.append("cancel")

    input_artifact = SimpleNamespace(
        id=input_artifact_id,
        media_info={},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=100,
    )

    class FakeSession:
        async def __aenter__(self):
            nonlocal session_active
            session_active = True
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            nonlocal session_active
            session_active = False
            return False

        async def get(self, model, item_id):
            if model is worker_main.Artifact and item_id == input_artifact_id:
                return input_artifact
            return None

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            return str(missing_local_path)

    class DownloadResponse:
        status_code = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def aiter_bytes(self):
            download_started.set()
            await asyncio.Event().wait()
            yield b"never"

    class FakeAsyncClient:
        def __init__(self, **kwargs):
            return None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def stream(self, method: str, url: str):
            session_state_during_stream.append(session_active)
            return DownloadResponse()

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs) -> bool:
        return True

    async def load_cancel_state(_node_execution_id: str):
        await download_started.wait()
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.CANCELLED,
            job_status=worker_main.JobStatus.CANCELLED,
            is_cancelled=True,
            cancel_reason="test cancellation",
        )

    async def report_success(job: str, node: str, artifact: str) -> None:
        succeeded.append((job, node, artifact))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": NeverRunHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "_load_cancel_state", load_cancel_state)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.httpx, "AsyncClient", FakeAsyncClient)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await asyncio.wait_for(
        worker_main.process_task(
            {
                "job_id": str(job_id),
                "node_execution_id": str(node_execution_id),
                "node_id": "smart_trim_1",
                "node_type": "smart_trim",
                "config": json.dumps({"prompt": "owned canary"}),
                "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
            }
        ),
        timeout=0.5,
    )

    assert session_state_during_stream == [False]
    assert handler_calls == ["cancel"]
    assert succeeded == []
    assert failed == []


@pytest.mark.asyncio
async def test_completed_artifact_download_is_removed_when_cancel_wins_race(
    monkeypatch,
    tmp_path: Path,
) -> None:
    temp_path = tmp_path / "completed-before-cancel.mp4"
    artifact = worker_main.InputArtifactSnapshot(
        id=uuid.uuid4(),
        media_info={},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=5,
    )
    cancel_event = asyncio.Event()
    cancel_event.set()

    async def complete_download(_artifact):
        temp_path.write_bytes(b"video")
        return str(temp_path)

    monkeypatch.setattr(worker_main, "_download_artifact_via_api", complete_download)

    with pytest.raises(worker_main.CancelledError):
        await worker_main._download_artifact_with_cancel(artifact, cancel_event)

    assert not temp_path.exists()


@pytest.mark.asyncio
async def test_worker_admission_runs_before_database_and_redis(monkeypatch) -> None:
    events: list[str] = []

    class StopStartup(RuntimeError):
        pass

    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        lambda: events.append("admission"),
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "configure_worker_database",
        lambda: events.append("database"),
        raising=False,
    )

    def stop_at_redis() -> None:
        events.append("redis")
        raise StopStartup

    monkeypatch.setattr(worker_main, "_redis", stop_at_redis)

    with pytest.raises(StopStartup):
        await worker_main.main()

    assert events == ["admission", "database", "redis"]


@pytest.mark.asyncio
async def test_denied_worker_stops_before_database_or_redis(monkeypatch) -> None:
    touched: list[str] = []

    def deny_worker() -> None:
        raise WorkerAdmissionError("unsafe worker configuration")

    monkeypatch.setattr(
        worker_main,
        "enforce_worker_admission_from_env",
        deny_worker,
        raising=False,
    )
    monkeypatch.setattr(
        worker_main,
        "configure_worker_database",
        lambda: touched.append("database"),
        raising=False,
    )
    monkeypatch.setattr(worker_main, "_redis", lambda: touched.append("redis"))

    with pytest.raises(SystemExit) as exc:
        await worker_main.main()

    assert exc.value.code == 2
    assert touched == []


@pytest.mark.asyncio
async def test_process_task_injects_youtube_context_without_changing_other_handler_constructors(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    input_path = tmp_path / "input.mp4"
    input_path.write_bytes(b"input")
    created: list[tuple[str, object | None]] = []
    executed_configs: list[dict] = []

    class YouTubeHandler:
        def __init__(self, *, session_factory):
            created.append(("youtube", session_factory))

        async def execute(self, config, input_paths, output_path):
            executed_configs.append(dict(config))
            Path(output_path).write_bytes(Path(input_paths["input"]).read_bytes())
            return {}

        def cancel(self) -> None:
            return None

    class OtherHandler:
        def __init__(self):
            created.append(("other", None))

        async def execute(self, config, input_paths, output_path):
            Path(output_path).write_bytes(Path(input_paths["input"]).read_bytes())
            return {}

        def cancel(self) -> None:
            return None

    input_artifact = SimpleNamespace(
        job_id=job_id,
        media_info={},
        storage_backend="local",
        storage_path=str(input_path),
        filename="input.mp4",
    )
    node_execution = SimpleNamespace(
        job_id=job_id,
        node_id="youtube_upload_1",
        node_type="youtube_upload",
        node_config={"title": "Canary"},
        input_artifact_ids=[input_artifact_id],
        status=None,
        started_at=None,
        worker_id=None,
    )

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def get(self, model, item_id):
            if model is worker_main.NodeExecution:
                return node_execution
            if model is worker_main.Artifact:
                return input_artifact
            return None

        def add(self, item) -> None:
            return None

        async def flush(self) -> None:
            return None

        async def commit(self) -> None:
            return None

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            return path

    def process_session_factory():
        return FakeSession()

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(None, None, None, False, None)

    async def report_success(*args) -> None:
        return None

    async def claim_node(*args, **kwargs) -> bool:
        return True

    monkeypatch.setattr(
        worker_main,
        "HANDLER_MAP",
        {"youtube_upload": object, "source": OtherHandler},
    )
    monkeypatch.setattr(worker_main, "YouTubeUploadHandler", YouTubeHandler)
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: process_session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main.settings, "storage_backend", "local")
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    data = {
        "job_id": str(job_id),
        "node_execution_id": str(node_execution_id),
        "node_id": "youtube_upload_1",
        "node_type": "youtube_upload",
        "config": json.dumps({"title": "Canary"}),
        "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
    }
    await worker_main.process_task(data)

    await worker_main.process_task({**data, "node_id": "source_1", "node_type": "source"})

    assert created == [("youtube", process_session_factory), ("other", None)]
    assert executed_configs == [
        {
            "title": "Canary",
            "_job_id": str(job_id),
            "_node_execution_id": str(node_execution_id),
            "_input_artifact_ids": {"input": str(input_artifact_id)},
            "_input_artifact_meta": {"input": {}},
        }
    ]
