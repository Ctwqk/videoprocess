from __future__ import annotations

import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from worker import main as worker_main


class WorkerHarness:
    def __init__(self, *, tmp_path: Path, node_execution, input_artifact) -> None:
        self._tmp_path = tmp_path
        self.node_execution = node_execution
        self.input_artifact = input_artifact
        self.created: list[object] = []
        self.executed_configs: list[dict] = []
        self.failures: list[str] = []

    def install(self, monkeypatch) -> None:
        harness = self

        class YouTubeHandler:
            def __init__(self, *, session_factory) -> None:
                harness.created.append(session_factory)

            async def execute(self, config, input_paths, output_path):
                harness.executed_configs.append(dict(config))
                Path(output_path).write_bytes(Path(input_paths["input"]).read_bytes())
                return {}

            def cancel(self) -> None:
                return None

        class FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, traceback):
                return False

            async def get(self, model, item_id):
                if model is worker_main.NodeExecution:
                    return harness.node_execution
                if model is worker_main.Artifact:
                    return harness.input_artifact
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

        async def report_failure(_job_id: str, _node_execution_id: str, error: str) -> None:
            harness.failures.append(error)

        async def claim_node(*args, **kwargs) -> bool:
            return True

        monkeypatch.setattr(worker_main, "HANDLER_MAP", {"youtube_upload": object})
        monkeypatch.setattr(worker_main, "YouTubeUploadHandler", YouTubeHandler)
        monkeypatch.setattr(worker_main, "get_worker_session", lambda: process_session_factory)
        monkeypatch.setattr(worker_main, "_claim_node_execution", claim_node)
        monkeypatch.setattr(worker_main, "_load_cancel_state", not_cancelled)
        monkeypatch.setattr(worker_main, "_report_success", report_success)
        monkeypatch.setattr(worker_main, "_report_failure", report_failure)
        monkeypatch.setattr(worker_main, "get_storage", lambda _backend: LocalStorage())
        monkeypatch.setattr(worker_main.settings, "storage_backend", "local")
        monkeypatch.setattr(worker_main.settings, "storage_local_root", str(self._tmp_path / "storage"))


def worker_data(*, job_id: uuid.UUID, node_execution_id: uuid.UUID, artifact_id: uuid.UUID) -> dict:
    return {
        "job_id": str(job_id),
        "node_execution_id": str(node_execution_id),
        "node_id": "youtube_upload_1",
        "node_type": "youtube_upload",
        "config": json.dumps({"title": "untrusted queue title", "privacy": "public"}),
        "input_artifacts": json.dumps({"input": str(artifact_id)}),
    }


def authoritative_rows(tmp_path: Path):
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    artifact_id = uuid.uuid4()
    input_path = tmp_path / "input.mp4"
    input_path.write_bytes(b"authoritative input")
    node_execution = SimpleNamespace(
        job_id=job_id,
        node_id="youtube_upload_1",
        node_type="youtube_upload",
        node_config={"title": "authoritative title", "privacy": "unlisted"},
        input_artifact_ids=[artifact_id],
        status=None,
        started_at=None,
        worker_id=None,
    )
    input_artifact = SimpleNamespace(
        id=artifact_id,
        job_id=job_id,
        media_info={},
        storage_backend="local",
        storage_path=str(input_path),
        filename="input.mp4",
    )
    return job_id, node_execution_id, artifact_id, node_execution, input_artifact


@pytest.mark.asyncio
async def test_youtube_task_uses_authoritative_node_config_and_validated_input(monkeypatch, tmp_path: Path):
    job_id, node_execution_id, artifact_id, node_execution, input_artifact = authoritative_rows(tmp_path)
    harness = WorkerHarness(
        tmp_path=tmp_path,
        node_execution=node_execution,
        input_artifact=input_artifact,
    )
    harness.install(monkeypatch)

    await worker_main.process_task(
        worker_data(
            job_id=job_id,
            node_execution_id=node_execution_id,
            artifact_id=artifact_id,
        )
    )

    assert len(harness.created) == 1
    assert harness.failures == []
    assert harness.executed_configs == [
        {
            "title": "authoritative title",
            "privacy": "unlisted",
            "_job_id": str(job_id),
            "_node_execution_id": str(node_execution_id),
            "_input_artifact_ids": {"input": str(artifact_id)},
            "_input_artifact_meta": {"input": {}},
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_case",
    [
        "cross_job_artifact",
        "wrong_node_id",
        "wrong_node_type",
        "mismatched_expected_input_ids",
        "unexpected_input_port",
    ],
)
async def test_invalid_youtube_queue_binding_never_constructs_handler(
    monkeypatch,
    tmp_path: Path,
    invalid_case: str,
):
    job_id, node_execution_id, artifact_id, node_execution, input_artifact = authoritative_rows(tmp_path)
    data = worker_data(job_id=job_id, node_execution_id=node_execution_id, artifact_id=artifact_id)
    if invalid_case == "cross_job_artifact":
        input_artifact.job_id = uuid.uuid4()
    elif invalid_case == "wrong_node_id":
        node_execution.node_id = "source_1"
    elif invalid_case == "wrong_node_type":
        node_execution.node_type = "source"
    elif invalid_case == "mismatched_expected_input_ids":
        node_execution.input_artifact_ids = [uuid.uuid4()]
    elif invalid_case == "unexpected_input_port":
        data["input_artifacts"] = json.dumps({"input": str(artifact_id), "extra": str(uuid.uuid4())})
    else:
        raise AssertionError(f"unexpected test case: {invalid_case}")

    harness = WorkerHarness(
        tmp_path=tmp_path,
        node_execution=node_execution,
        input_artifact=input_artifact,
    )
    harness.install(monkeypatch)

    await worker_main.process_task(data)

    assert harness.created == []
    assert harness.executed_configs == []
    assert len(harness.failures) == 1
