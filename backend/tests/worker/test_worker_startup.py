from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.worker_admission import WorkerAdmissionError
from worker import main as worker_main


def execution_claim(
    job_id: uuid.UUID,
    node_execution_id: uuid.UUID,
) -> worker_main.NodeExecutionClaim:
    return worker_main.NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="test-worker@localhost:1",
        started_at=datetime(2026, 7, 22, 12, 0, 0),
    )


def test_worker_database_is_not_configured_at_import() -> None:
    assert worker_main.engine_db is None
    assert worker_main.worker_session is None


def test_node_execution_started_at_orm_type_is_timezone_aware() -> None:
    assert worker_main.NodeExecution.__table__.c.started_at.type.timezone is True


@pytest.mark.asyncio
async def test_worker_events_include_canonical_execution_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    output_artifact_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    events: list[tuple[str, dict]] = []
    close_calls = 0

    class FakeRedis:
        async def xadd(self, stream: str, payload: dict) -> None:
            events.append((stream, dict(payload)))

        async def aclose(self) -> None:
            nonlocal close_calls
            close_calls += 1

    monkeypatch.setattr(worker_main, "_redis", lambda: FakeRedis())

    await worker_main._report_success(
        str(job_id),
        str(node_execution_id),
        str(output_artifact_id),
        claim,
    )
    await worker_main._report_failure(
        str(job_id),
        str(node_execution_id),
        "render failed",
        claim,
    )

    expected_claim = {
        "worker_id": claim.worker_id,
        "started_at": "2026-07-22T12:00:00+00:00",
    }
    assert events == [
        (
            worker_main.EVENT_STREAM,
            {
                "event": "node_completed",
                "job_id": str(job_id),
                "node_execution_id": str(node_execution_id),
                "output_artifact_id": str(output_artifact_id),
                **expected_claim,
            },
        ),
        (
            worker_main.EVENT_STREAM,
            {
                "event": "node_failed",
                "job_id": str(job_id),
                "node_execution_id": str(node_execution_id),
                "error": "render failed",
                **expected_claim,
            },
        ),
    ]
    assert close_calls == 2


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
    output_paths: list[str] = []
    requested: list[tuple[str, str]] = []
    authority_locks: list[tuple[uuid.UUID, uuid.UUID]] = []
    succeeded: list[tuple[str, str, str, object | None]] = []
    failed: list[tuple[str, str, str]] = []
    claim = execution_claim(job_id, node_execution_id)

    class CopyHandler:
        async def execute(self, config, input_paths, output_path):
            input_path = input_paths["input"]
            handled_paths.append(input_path)
            handled_inputs.append(Path(input_path).read_bytes())
            output_paths.append(output_path)
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

        def begin(self):
            return FakeTransaction()

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

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

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

    async def claim_node(*args, **kwargs):
        return claim

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        authority_locks.append((locked_job_id, node_execution_id))
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id=claim.worker_id,
                started_at=claim.started_at,
            ),
        )

    async def report_success(job: str, node: str, artifact: str, *args) -> None:
        succeeded.append((job, node, artifact, args[0] if args else None))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": CopyHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
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
    assert Path(output_paths[0]).name.startswith(f"{node_execution_id}-")
    assert authority_locks == [(job_id, node_execution_id)] * 3
    assert len(succeeded) == 1
    assert succeeded[0][3] == claim
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

    async def claim_node(*args, **kwargs):
        return execution_claim(job_id, node_execution_id)

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
async def test_process_task_stops_before_handler_when_claim_changes_during_download(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    input_artifact_id = uuid.uuid4()
    claimed_at = datetime(2026, 7, 22, 12, 0, 0)
    replacement_started_at = claimed_at + timedelta(minutes=11)
    missing_local_path = tmp_path / "gpu-scratch" / "assets" / "input.mp4"
    downloaded_path = tmp_path / "downloaded-input.mp4"
    handler_calls: list[str] = []
    authority_locks: list[tuple[uuid.UUID, uuid.UUID]] = []
    succeeded: list[tuple[str, str, str]] = []
    failed: list[tuple[str, str, str]] = []

    claim = SimpleNamespace(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="gpu-worker@150:old",
        started_at=claimed_at,
    )
    input_artifact = SimpleNamespace(
        id=input_artifact_id,
        media_info={},
        storage_backend="local",
        storage_path="assets/input.mp4",
        filename="input.mp4",
        file_size=5,
    )

    class NeverRunHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            raise AssertionError("a stale execution claim must not reach the handler")

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

        async def get(self, model, item_id):
            if model is worker_main.Artifact and item_id == input_artifact_id:
                return input_artifact
            return None

    class LocalStorage:
        def get_local_path(self, path: str) -> str:
            return str(missing_local_path)

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def download_artifact(_artifact, _cancel_event):
        downloaded_path.write_bytes(b"video")
        return str(downloaded_path)

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        authority_locks.append((locked_job_id, node_execution_id))
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id="gpu-worker@150:replacement",
                started_at=replacement_started_at,
            ),
        )

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def report_success(job: str, node: str, artifact: str) -> None:
        succeeded.append((job, node, artifact))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": NeverRunHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(worker_main, "_download_artifact_with_cancel", download_artifact)
    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": json.dumps({"input": str(input_artifact_id)}),
        }
    )

    assert authority_locks == [(job_id, node_execution_id)]
    assert handler_calls == ["cancel"]
    assert succeeded == []
    assert failed == []


@pytest.mark.asyncio
async def test_claim_recheck_accepts_naive_and_utc_aware_same_instant(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claimed_at = datetime(2026, 7, 22, 12, 0, 0)
    claim = worker_main.NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="gpu-worker@150:1",
        started_at=claimed_at,
    )

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

    def session_factory():
        return FakeSession()

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        assert locked_job_id == job_id
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id=claim.worker_id,
                started_at=claimed_at.replace(tzinfo=timezone.utc),
            ),
        )

    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)

    await worker_main._require_current_node_execution_claim(
        claim,
        session_factory=session_factory,
    )


@pytest.mark.asyncio
async def test_failure_claim_database_error_propagates_without_event(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    events: list[tuple] = []

    async def fail_claim_check(_claim) -> None:
        raise RuntimeError("database unavailable")

    async def report_failure(*args) -> None:
        events.append(args)

    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        fail_claim_check,
    )
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await worker_main._report_failure_for_current_claim(
            claim,
            str(job_id),
            str(node_execution_id),
            "handler failed",
        )

    assert events == []


@pytest.mark.asyncio
async def test_artifact_persistence_rejects_replaced_execution_claim(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    added: list[object] = []

    class FakeTransaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def begin(self):
            return FakeTransaction()

        def add(self, item) -> None:
            added.append(item)

        async def flush(self) -> None:
            raise AssertionError("a stale execution must not flush an artifact")

    def session_factory():
        return FakeSession()

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=locked_job_id, status=worker_main.JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=worker_main.NodeStatus.RUNNING,
                worker_id="gpu-worker@150:replacement",
                started_at=claim.started_at + timedelta(minutes=1),
            ),
        )

    monkeypatch.setattr(worker_main, "lock_job_execution_authority", lock_authority)

    with pytest.raises(
        worker_main.JobExecutionAuthorityBlocked,
        match="claim changed",
    ):
        await worker_main._persist_artifact_for_current_claim(
            claim,
            filename="output.mp4",
            mime_type="video/mp4",
            file_size=42,
            storage_backend="local",
            storage_path="artifacts/output.mp4",
            media_info={},
            session_factory=session_factory,
        )

    assert added == []


@pytest.mark.asyncio
async def test_stale_artifact_persistence_cleans_generation_outputs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    saved: list[tuple[str, bytes]] = []
    deleted: list[str] = []
    output_paths: list[str] = []
    handler_calls: list[str] = []

    class SuccessfulHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            output_paths.append(output_path)
            Path(output_path).write_bytes(b"generation output")
            return {}

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class RemoteStorage:
        async def save(self, path: str, data) -> int:
            content = data.read()
            saved.append((path, content))
            return len(content)

        async def delete(self, path: str) -> None:
            deleted.append(path)

    async def claim_node(*args, **kwargs):
        return claim

    async def require_current_claim(_claim) -> None:
        return None

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def reject_artifact(_claim, **kwargs) -> str:
        raise worker_main.JobExecutionAuthorityBlocked(
            "node execution claim changed"
        )

    async def suppress_stale_failure(*args) -> bool:
        return False

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": SuccessfulHandler})
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(
        worker_main,
        "_persist_artifact_for_current_claim",
        reject_artifact,
    )
    monkeypatch.setattr(
        worker_main,
        "_report_failure_for_current_claim",
        suppress_stale_failure,
    )
    monkeypatch.setattr(worker_main, "get_storage", lambda _backend: RemoteStorage())
    monkeypatch.setattr(worker_main.settings, "storage_backend", "minio")
    monkeypatch.setattr(
        worker_main.settings,
        "storage_local_root",
        str(tmp_path / "storage"),
    )

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    expected_storage_path = (
        f"artifacts/{job_id}/{Path(output_paths[0]).name}"
    )
    assert saved == [(expected_storage_path, b"generation output")]
    assert deleted == [expected_storage_path]
    assert not Path(output_paths[0]).exists()
    assert handler_calls == ["execute", "cancel"]


@pytest.mark.asyncio
async def test_handler_failure_after_claim_loss_does_not_emit_node_failed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    claim_checks: list[str] = []
    handler_calls: list[str] = []
    failed: list[tuple[str, str, str]] = []

    class FailingHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            raise RuntimeError("handler failed after replacement worker took over")

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def require_current_claim(_claim) -> None:
        claim_checks.append("checked")
        if len(claim_checks) > 1:
            raise worker_main.JobExecutionAuthorityBlocked("node execution claim changed")

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": FailingHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    assert claim_checks == ["checked", "checked"]
    assert handler_calls == ["execute", "cancel"]
    assert failed == []


@pytest.mark.asyncio
async def test_handler_success_after_claim_loss_does_not_emit_node_completed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    claim_checks: list[str] = []
    handler_calls: list[str] = []
    succeeded: list[tuple[str, str, str]] = []
    failed: list[tuple[str, str, str]] = []

    class SuccessfulHandler:
        async def execute(self, config, input_paths, output_path):
            handler_calls.append("execute")
            Path(output_path).write_bytes(b"stale output")
            return {}

        def cancel(self) -> None:
            handler_calls.append("cancel")

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        def add(self, item) -> None:
            return None

        async def flush(self) -> None:
            return None

        async def commit(self) -> None:
            return None

    def session_factory():
        return FakeSession()

    async def claim_node(*args, **kwargs):
        return claim

    async def require_current_claim(_claim) -> None:
        claim_checks.append("checked")
        if len(claim_checks) > 1:
            raise worker_main.JobExecutionAuthorityBlocked("node execution claim changed")

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(
            job_id=job_id,
            node_status=worker_main.NodeStatus.RUNNING,
            job_status=worker_main.JobStatus.RUNNING,
            is_cancelled=False,
            cancel_reason=None,
        )

    async def report_success(job: str, node: str, artifact: str) -> None:
        succeeded.append((job, node, artifact))

    async def report_failure(job: str, node: str, error: str) -> None:
        failed.append((job, node, error))

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": SuccessfulHandler})
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
    monkeypatch.setattr(worker_main, "_report_success", report_success)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)
    monkeypatch.setattr(worker_main.settings, "storage_local_root", str(tmp_path / "storage"))

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    assert claim_checks == ["checked", "checked"]
    assert handler_calls == ["execute", "cancel"]
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
async def test_unhandled_task_error_leaves_message_pending_without_generationless_failure(
    monkeypatch,
) -> None:
    acknowledgements: list[tuple[str, str, str]] = []
    failure_events: list[tuple] = []

    class FakeRedis:
        async def xack(self, stream: str, group: str, message_id: str) -> None:
            acknowledgements.append((stream, group, message_id))

    async def fail_before_claim(_data):
        raise RuntimeError("worker failed before a durable claim was returned")

    async def heartbeat(_redis, _message_id):
        await asyncio.Event().wait()

    async def report_failure(*args) -> None:
        failure_events.append(args)

    monkeypatch.setattr(worker_main, "process_task", fail_before_claim)
    monkeypatch.setattr(worker_main, "_heartbeat_message", heartbeat)
    monkeypatch.setattr(worker_main, "_report_failure", report_failure)

    await worker_main._process_message(
        FakeRedis(),
        "1-0",
        {
            "job_id": str(uuid.uuid4()),
            "node_execution_id": str(uuid.uuid4()),
        },
    )

    assert failure_events == []
    assert acknowledgements == []


@pytest.mark.asyncio
async def test_handler_constructor_failure_reports_for_exact_claim(monkeypatch) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = execution_claim(job_id, node_execution_id)
    failures: list[tuple[object, str, str, str]] = []

    class BrokenHandler:
        def __init__(self) -> None:
            raise RuntimeError("handler constructor failed")

    async def claim_node(*args, **kwargs):
        return claim

    async def report_failure(
        handled_claim,
        handled_job_id: str,
        handled_node_id: str,
        error: str,
    ) -> bool:
        failures.append(
            (handled_claim, handled_job_id, handled_node_id, error)
        )
        return True

    monkeypatch.setattr(worker_main, "HANDLER_MAP", {"smart_trim": BrokenHandler})
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_report_failure_for_current_claim",
        report_failure,
    )

    await worker_main.process_task(
        {
            "job_id": str(job_id),
            "node_execution_id": str(node_execution_id),
            "node_id": "smart_trim_1",
            "node_type": "smart_trim",
            "config": "{}",
            "input_artifacts": "{}",
        }
    )

    assert failures == [
        (
            claim,
            str(job_id),
            str(node_execution_id),
            "handler constructor failed",
        )
    ]


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

    async def claim_node(*args, **kwargs):
        return execution_claim(job_id, node_execution_id)

    async def require_current_claim(_claim) -> None:
        return None

    async def persist_artifact(_claim, **kwargs) -> str:
        return str(uuid.uuid4())

    monkeypatch.setattr(
        worker_main,
        "HANDLER_MAP",
        {"youtube_upload": object, "source": OtherHandler},
    )
    monkeypatch.setattr(worker_main, "YouTubeUploadHandler", YouTubeHandler)
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: process_session_factory)
    monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
    monkeypatch.setattr(
        worker_main,
        "_require_current_node_execution_claim",
        require_current_claim,
    )
    monkeypatch.setattr(
        worker_main,
        "_persist_artifact_for_current_claim",
        persist_artifact,
    )
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
            "_execution_claim": {
                "worker_id": "test-worker@localhost:1",
                "started_at": "2026-07-22T12:00:00+00:00",
            },
            "_input_artifact_meta": {"input": {}},
        }
    ]
