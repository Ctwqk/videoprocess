from __future__ import annotations

import json
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
        media_info={},
        storage_backend="local",
        storage_path=str(input_path),
        filename="input.mp4",
    )
    node_execution = SimpleNamespace(status=None, started_at=None, worker_id=None)

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

    process_session_factory = lambda: FakeSession()

    async def not_cancelled(_node_execution_id: str):
        return worker_main.CancelState(None, None, None, False, None)

    async def report_success(*args) -> None:
        return None

    monkeypatch.setattr(
        worker_main,
        "HANDLER_MAP",
        {"youtube_upload": object, "source": OtherHandler},
    )
    monkeypatch.setattr(worker_main, "YouTubeUploadHandler", YouTubeHandler)
    monkeypatch.setattr(worker_main, "get_worker_session", lambda: process_session_factory)
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
