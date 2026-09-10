from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from worker import main as worker_main
from worker.handlers import youtube_upload as upload_module
from tests.worker.test_youtube_ack_drill_arming import (
    arming_case as _arming_case, protected_dir as _protected_dir, write_manifest,
)


arming_case = _arming_case
protected_dir = _protected_dir


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
            def __init__(
                self,
                *,
                session_factory,
                lease_refresher=None,
            ) -> None:
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

        async def report_failure(
            _job_id: str,
            _node_execution_id: str,
            error: str,
            _claim,
        ) -> None:
            harness.failures.append(error)

        async def claim_node(*args, **kwargs):
            return worker_main.NodeExecutionClaim(
                job_id=harness.node_execution.job_id,
                node_execution_id=harness.node_execution.id,
                worker_id="test-worker@localhost:1",
                started_at=datetime(2026, 7, 22, 12, 0, 0),
            )

        async def require_current_claim(_claim) -> None:
            return None

        async def persist_artifact(_claim, **kwargs) -> str:
            return str(uuid.uuid4())

        monkeypatch.setattr(worker_main, "HANDLER_MAP", {"youtube_upload": object})
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
        id=node_execution_id,
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
                "_execution_claim": {
                    "worker_id": "test-worker@localhost:1",
                    "started_at": "2026-07-22T12:00:00+00:00",
                },
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


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["exact", "spoofed-delivery", "missing-delivery", "off-queue-spoof", "cancel-wait", "arrival"])
async def test_real_main_construction_binds_runtime_arming(monkeypatch, tmp_path, arming_case, case):
    context = arming_case["context"]
    _, _, _, node, artifact = authoritative_rows(tmp_path)
    node.id, node.job_id, node.input_artifact_ids = context.node_execution_id, context.job_id, [context.input_artifact_id]
    node.node_config = {"title": context.title, "privacy": "unlisted"}
    artifact.id, artifact.job_id = context.input_artifact_id, context.job_id
    Path(artifact.storage_path).write_bytes(b"owned media")
    harness = WorkerHarness(tmp_path=tmp_path, node_execution=node, input_artifact=artifact)
    harness.install(monkeypatch)
    monkeypatch.setattr(worker_main, "YouTubeUploadHandler", upload_module.YouTubeUploadHandler)
    monkeypatch.setattr(upload_module.settings, "youtube_manager_url", "http://youtube-manager")
    async def real_claim(*args, **kwargs):
        return context.execution_claim
    monkeypatch.setattr(worker_main, "_claim_node_execution", real_claim)
    reached = []
    class StopAtReservation:
        async def claim(self, actual):
            reached.append(actual)
            raise RuntimeError("test stopped at reservation boundary")
    monkeypatch.setattr(upload_module, "YouTubeUploadOperationStore", lambda factory: StopAtReservation())
    data = worker_data(job_id=context.job_id, node_execution_id=context.node_execution_id, artifact_id=context.input_artifact_id)
    data["config"] = json.dumps({
        "VP_YOUTUBE_ACK_DRILL_ENABLED": "true", "VP_YOUTUBE_ACK_DRILL_MANIFEST": str(arming_case["path"]),
        "ack_drill_arming": arming_case["manifest"], "_delivery": vars(arming_case["delivery"]),
    }, default=str)
    delivered = arming_case["delivery"]
    if case == "spoofed-delivery":
        delivered.message_id = "999-0"
    elif case == "missing-delivery":
        delivered = None
    elif case == "off-queue-spoof":
        monkeypatch.delenv("VP_YOUTUBE_ACK_DRILL_ENABLED")
    if case not in {"cancel-wait", "arrival", "off-queue-spoof"}:
        write_manifest(arming_case)
    token = worker_main._current_task_delivery.set(delivered)
    cancel_watch_entered = asyncio.Event()
    release_watcher = asyncio.Event()
    checks = 0
    async def watch(_node_id):
        nonlocal checks
        checks += 1
        if checks == 1:
            return worker_main.CancelState(None, None, None, False, None)
        cancel_watch_entered.set()
        await release_watcher.wait()
        return worker_main.CancelState(None, None, None, True, "test cancellation")
    if case in {"cancel-wait", "arrival"}:
        monkeypatch.setattr(worker_main, "_load_cancel_state", watch)
    try:
        task = asyncio.create_task(worker_main.process_task(data, worker_lease=arming_case["lease"]))
        if case in {"cancel-wait", "arrival"}:
            await asyncio.wait_for(cancel_watch_entered.wait(), 2)
            assert reached == []
            assert not task.done()
            if case == "arrival":
                write_manifest(arming_case)
            else:
                release_watcher.set()
        await asyncio.wait_for(task, 3)
    finally:
        worker_main._current_task_delivery.reset(token)
    if case in {"exact", "arrival", "off-queue-spoof"}:
        assert reached == [context]
        assert harness.failures == ["test stopped at reservation boundary"]
    else:
        assert reached == []
        if case != "cancel-wait":
            assert len(harness.failures) == 1


def test_module_entrypoint_accepts_its_real_delivery_without_importing_main_again(arming_case):
    # Isolated interpreter plus Python's actual -m execution primitive. Startup
    # is intercepted before the coroutine runs, so no services are contacted.
    bootstrap = r'''
import asyncio
import dataclasses
import json
import runpy
import sys
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, sys.argv[1])

def probe(startup):
    startup.close()
    entry = sys.modules["__main__"]
    from worker import registration
    from worker.youtube_ack_drill_arming import AckDrillArming
    registration.EMBEDDED_BUILD_COMMIT = "a" * 40
    now = datetime.now(timezone.utc)
    claim = entry.NodeExecutionClaim(
        job_id=uuid.uuid4(), node_execution_id=uuid.uuid4(),
        worker_id="youtube_publisher-worker@host:1", started_at=now,
        worker_registration_id=uuid.uuid4(), worker_lease_epoch=7,
    )
    lease = entry.WorkerLease(
        registration_id=claim.worker_registration_id, grant_id=uuid.uuid4(),
        service_name="vp-youtube-publisher", worker_instance_id=uuid.uuid4(),
        worker_slot=1, redis_consumer_id=claim.worker_id, lease_epoch=7,
        lease_secret="test-only", lease_expires_at=now + timedelta(minutes=5),
    )
    delivery = entry.WorkerTaskDelivery(
        redis_stream="vp:tasks:youtube_publisher", consumer_group="youtube_publisher-workers",
        message_id="1234567890-0", payload_sha256="d" * 64,
        dispatch_key=uuid.uuid4(), attestation_id=uuid.uuid4(),
    )
    options = dict(worker_type=entry.WORKER_TYPE, worker_lease=lease, execution_claim=claim)
    assert "worker.main" not in sys.modules
    pending = AckDrillArming.from_environment(**options, delivery=delivery)
    assert pending.message_id == "1234567890-0"
    assert "worker.main" not in sys.modules, "arming imported the entry point again"
    class DeliverySubclass(entry.WorkerTaskDelivery):
        pass
    Lookalike = dataclasses.make_dataclass("WorkerTaskDelivery", [
        (field.name, field.type) for field in dataclasses.fields(delivery)
    ])
    for spoof in (vars(delivery), DeliverySubclass(**vars(delivery)), Lookalike(**vars(delivery))):
        try:
            AckDrillArming.from_environment(**options, delivery=spoof)
        except ValueError:
            pass
        else:
            raise AssertionError("noncanonical delivery was accepted")
    print(json.dumps({"module": entry.__name__, "delivery_module": type(delivery).__module__}))

asyncio.run = probe
runpy._run_module_as_main("worker.main", alter_argv=True)
'''
    result = subprocess.run(
        [sys.executable, "-I", "-c", bootstrap, str(Path(__file__).resolve().parents[2])],
        capture_output=True, text=True, timeout=10,
        env={key: value for key, value in os.environ.items() if key not in {
            "CHANNEL_OPS_POSTGRES_TEST_URL", "CHANNEL_OPS_REDIS_TEST_URL",
        }},
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"module": "__main__", "delivery_module": "worker.task_delivery"}
