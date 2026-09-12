from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import threading
import uuid
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import httpx

from app.services.job_execution_authority import NodeExecutionClaim
from app.services.worker_registration import WorkerLease
from app.services.youtube_upload_operations import UploadOperationContext
from worker import main as worker_main
from worker.handlers.base import CancelledError
from worker.handlers.youtube_upload import YouTubeUploadHandler
from tests.worker.test_youtube_ack_drill import reserved
from tests.worker.test_youtube_upload_handler import FakeOperationStore, MANAGER_TASK_ID, auth_payload


def arming_api():
    assert importlib.util.find_spec("worker.youtube_ack_drill_arming") is not None, (
        "trusted runtime arming is not implemented"
    )
    return importlib.import_module("worker.youtube_ack_drill_arming")


def digest(value):
    return hashlib.sha256(json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("ascii")).hexdigest()


@pytest.fixture
def protected_dir():
    # System temporary directories can have writable ancestors, forbidden for control files.
    with tempfile.TemporaryDirectory(prefix="vp-arming-test-", dir=Path.home()) as raw:
        yield Path(raw)


@pytest.fixture
def arming_case(monkeypatch, protected_dir):
    now = datetime.now(timezone.utc)
    job, node, artifact, registration = (uuid.uuid4() for _ in range(4))
    claim = NodeExecutionClaim(
        job_id=job, node_execution_id=node, worker_id="youtube_publisher-worker@host:1",
        started_at=now, worker_registration_id=registration, worker_lease_epoch=7,
    )
    context = UploadOperationContext(
        job_id=job, node_execution_id=node, input_artifact_id=artifact,
        content_sha256=hashlib.sha256(b"owned media").hexdigest(),
        title="Owned canary", privacy="unlisted", execution_claim=claim,
    )
    lease = WorkerLease(
        registration_id=registration, grant_id=uuid.uuid4(), service_name="vp-youtube-publisher",
        worker_instance_id=uuid.uuid4(), worker_slot=1, redis_consumer_id=claim.worker_id,
        lease_epoch=7, lease_secret="not-journaled", lease_expires_at=now + timedelta(minutes=5),
    )
    delivery = worker_main.WorkerTaskDelivery(
        redis_stream="vp:tasks:youtube_publisher", consumer_group="youtube_publisher-workers",
        message_id="1234567890-0", payload_sha256="d" * 64,
        dispatch_key=uuid.uuid4(), attestation_id=uuid.uuid4(),
    )
    state = protected_dir / "state"
    state.mkdir(mode=0o700)
    manifest_path = protected_dir / "operator" / "manifest.json"
    manifest_path.parent.mkdir(mode=0o700)
    metadata = {
        "release_commit": "a" * 40, "channel_id": str(uuid.uuid4()),
        "account_id": str(uuid.uuid4()), "platform_channel_id": "UC" + "x" * 22,
        "service_name": lease.service_name, "redis_stream": delivery.redis_stream,
        "consumer_group": delivery.consumer_group, "message_id": delivery.message_id,
        "payload_sha256": delivery.payload_sha256, "dispatch_key": str(delivery.dispatch_key),
        "attestation_id": str(delivery.attestation_id), "receiver_container_id": "b" * 64,
        "receiver_image_id": "sha256:" + "c" * 64, "audit_window_id": str(uuid.uuid4()),
        "audit_cursor_sha256": "e" * 64,
    }
    source = {
        "version": 1, "production_task_id": str(uuid.uuid4()), "job_id": str(job),
        "channel_id": metadata["channel_id"], "account_id": metadata["account_id"],
        "input_artifact_id": str(artifact), "content_sha256": context.content_sha256,
        "graph_sha256": "f" * 64, "sources_complete": True,
        "sources": [{"asset_id": str(uuid.uuid4()), "content_sha256": "1" * 64,
                     "license": "owned", "provenance": "generated"}],
    }
    manifest = {
        "version": 1, "drill_id": str(uuid.uuid4()), "issued_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=4)).isoformat(), "state_dir": str(state),
        "production_task_id": source["production_task_id"], "manager_origin": "http://youtube-manager",
        "context": {
            "job_id": str(job), "node_execution_id": str(node), "input_artifact_id": str(artifact),
            "content_sha256": context.content_sha256, "title": context.title, "privacy": "unlisted",
            "execution_claim": {
                "worker_id": claim.worker_id, "started_at": now.isoformat(),
                "worker_registration_id": str(registration), "worker_lease_epoch": 7,
            },
        },
        "arming_identity": metadata, "source_evidence": source,
        "owned_attestation_sha256": digest(source),
        "account_attestation": {
            "version": 1, "method": "channels.list(mine=True)",
            "verified_at": now.isoformat(), "expires_at": (now + timedelta(minutes=4)).isoformat(),
            "manager_origin": "http://youtube-manager", "exclusive_ingress": True,
            "credential_fingerprint_sha256": "2" * 64,
            **{key: metadata[key] for key in (
                "account_id", "platform_channel_id", "receiver_container_id", "receiver_image_id",
                "audit_window_id", "audit_cursor_sha256",
            )},
        },
    }
    for key, value in {
        "VP_YOUTUBE_ACK_DRILL_ENABLED": "true", "VP_YOUTUBE_ACK_DRILL_MANIFEST": str(manifest_path),
        "WORKER_TYPE": "youtube_publisher", "WORKER_CONCURRENCY": "1",
        "WORKER_SERVICE_NAME": lease.service_name, "WORKER_RELEASE_COMMIT": "a" * 40,
        "WORKER_CAPABILITIES": "youtube_publisher", "WORKER_REDIS_STREAM": delivery.redis_stream,
        "WORKER_REDIS_GROUP": delivery.consumer_group,
        "YOUTUBE_PUBLISH_ENABLED": "true", "PUBLIC_PUBLISH_ENABLED": "false",
    }.items():
        monkeypatch.setenv(key, value)
    from worker import registration as registration_module
    monkeypatch.setattr(registration_module, "EMBEDDED_BUILD_COMMIT", "a" * 40)
    monkeypatch.setattr(worker_main, "WORKER_TYPE", "youtube_publisher")
    return dict(context=context, lease=lease, delivery=delivery, manifest=manifest,
                path=manifest_path, state=state)


def write_manifest(case, raw=None):
    path = case["path"]
    staging = path.with_name("staging.json")
    staging.write_bytes(json.dumps(case["manifest"]).encode() if raw is None else raw)
    staging.chmod(0o400)
    staging.replace(path)


def loader(case):
    return arming_api().AckDrillArming.from_environment(
        worker_type=worker_main.WORKER_TYPE, worker_lease=case["lease"],
        execution_claim=case["context"].execution_claim, delivery=case["delivery"],
    )


async def arm(case, pending=None, cancelled=None):
    return await (pending or loader(case)).arm(
        case["context"], manager_origin="http://youtube-manager",
        cancelled=cancelled or asyncio.Event(),
    )


def test_default_off_has_no_file_or_identity_io(monkeypatch):
    api = arming_api()
    monkeypatch.delenv("VP_YOUTUBE_ACK_DRILL_ENABLED", raising=False)
    monkeypatch.setenv("VP_YOUTUBE_ACK_DRILL_MANIFEST", "/must/not/read")
    def forbidden(*args, **kwargs):
        pytest.fail("off arming must not perform file I/O")
    monkeypatch.setattr(os, "open", forbidden)
    monkeypatch.setattr(Path, "read_text", forbidden)
    assert api.AckDrillArming.from_environment(
        worker_type=None, worker_lease=None, execution_claim=None, delivery=None,
    ) is None


@pytest.mark.parametrize("key,value", [
    ("VP_YOUTUBE_ACK_DRILL_ENABLED", "TRUE"), ("VP_YOUTUBE_ACK_DRILL_ENABLED", " true "),
    ("VP_YOUTUBE_ACK_DRILL_MANIFEST", "relative.json"), ("VP_YOUTUBE_ACK_DRILL_MANIFEST", ""),
    ("WORKER_CONCURRENCY", "2"), ("WORKER_TYPE", "ffmpeg"),
    ("WORKER_CAPABILITIES", "youtube_upload"),
    ("WORKER_CAPABILITIES", "youtube_publisher,ffmpeg"), ("WORKER_RELEASE_COMMIT", "b" * 40),
    ("WORKER_SERVICE_NAME", "other"), ("WORKER_REDIS_STREAM", "other"),
    ("YOUTUBE_PUBLISH_ENABLED", "false"), ("PUBLIC_PUBLISH_ENABLED", "true"),
])
def test_invalid_enabled_config_fails_closed(arming_case, monkeypatch, key, value):
    monkeypatch.setenv(key, value)
    with pytest.raises(ValueError):
        loader(arming_case)


def test_deployed_publisher_capability_can_arm(arming_case):
    pending = loader(arming_case)
    assert pending.service_name == "vp-youtube-publisher"
    assert pending.message_id == "1234567890-0"


@pytest.mark.asyncio
async def test_exact_manifest_arms_and_copies_immutable_metadata(arming_case):
    write_manifest(arming_case)
    helper = await arm(arming_case)
    target = helper._target
    with pytest.raises(FrozenInstanceError):
        target.arming_identity.account_id = str(uuid.uuid4())
    expected = dict(arming_case["manifest"]["arming_identity"])
    arming_case["manifest"]["arming_identity"]["account_id"] = str(uuid.uuid4())
    helper.prepare(target.context, reserved(target), manager_origin=target.manager_origin, timeout_seconds=5)
    record = json.loads((arming_case["state"] / "0001.json").read_text())
    assert {key: record[key] for key in expected} == expected
    assert "lease_secret" not in record


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [
    b"{", b"{} trailing", b'{"version":1,"version":1}', b"[]", b'{"x":NaN}',
    b" " * 65537, b'{"x": {"key":1,"key":2}}', b"[" * 10000,
])
async def test_ambiguous_or_unbounded_json_rejects(arming_case, raw):
    write_manifest(arming_case, raw)
    with pytest.raises(ValueError):
        await arm(arming_case)
    assert not list(arming_case["state"].iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["symlink", "ancestor-symlink", "hardlink", "mode", "owner", "ancestor-mode", "directory"])
async def test_unsafe_control_file_rejects(arming_case, monkeypatch, change):
    write_manifest(arming_case)
    path = arming_case["path"]
    if change in {"symlink", "hardlink"}:
        other = path.with_name("other.json")
        path.rename(other)
        path.symlink_to(other) if change == "symlink" else os.link(other, path)
    elif change == "ancestor-symlink":
        other = path.parent.with_name("other-parent")
        path.parent.rename(other)
        path.parent.symlink_to(other, target_is_directory=True)
    elif change == "mode":
        path.chmod(0o600)
    elif change == "owner":
        monkeypatch.setattr(os, "geteuid", lambda: 987654)
    elif change == "ancestor-mode":
        path.parent.chmod(0o777)
    else:
        path.unlink()
        path.mkdir()
    with pytest.raises((ValueError, OSError)):
        await arm(arming_case)


@pytest.mark.asyncio
@pytest.mark.parametrize("section,field,value", [
    ("context", "content_sha256", "3" * 64), ("context", "job_id", str(uuid.UUID(int=1))),
    ("context", "node_execution_id", str(uuid.UUID(int=2))),
    ("context", "input_artifact_id", str(uuid.UUID(int=3))), ("context", "title", "Other title"),
    ("context", "privacy", "private"), ("execution_claim", "worker_id", "other-worker"),
    ("execution_claim", "started_at", "2020-01-01T00:00:00+00:00"),
    ("execution_claim", "worker_registration_id", str(uuid.UUID(int=4))),
    ("execution_claim", "worker_lease_epoch", 8),
    ("arming_identity", "release_commit", "4" * 40), ("arming_identity", "service_name", "other-service"),
    ("arming_identity", "redis_stream", "other-stream"), ("arming_identity", "consumer_group", "other-group"),
    ("arming_identity", "message_id", "999-1"), ("arming_identity", "payload_sha256", "4" * 64),
    ("arming_identity", "dispatch_key", str(uuid.UUID(int=5))),
    ("arming_identity", "attestation_id", str(uuid.UUID(int=6))),
    ("arming_identity", "account_id", str(uuid.UUID(int=7))),
    ("arming_identity", "platform_channel_id", "UC" + "z" * 22),
    ("arming_identity", "channel_id", str(uuid.UUID(int=8))),
    ("arming_identity", "receiver_container_id", "5" * 64),
    ("arming_identity", "receiver_image_id", "sha256:" + "5" * 64),
    ("arming_identity", "audit_window_id", "other-window"),
    ("arming_identity", "audit_cursor_sha256", "5" * 64),
    ("root", "manager_origin", "http://other-manager"),
    ("root", "production_task_id", str(uuid.UUID(int=9))),
    ("root", "expires_at", "2020-01-01T00:00:00+00:00"),
    ("account_attestation", "method", "auth/status"),
    ("account_attestation", "exclusive_ingress", False),
    ("account_attestation", "verified_at", "2020-01-01T00:00:00+00:00"),
    ("account_attestation", "credential_fingerprint_sha256", ""),
    ("root", "owned_attestation_sha256", "0" * 64),
])
async def test_manifest_must_match_actual_context_and_all_attested_pins(arming_case, section, field, value):
    manifest = arming_case["manifest"]
    target = manifest if section == "root" else (
        manifest["context"]["execution_claim"] if section == "execution_claim" else manifest[section]
    )
    target[field] = value
    write_manifest(arming_case)
    with pytest.raises(ValueError):
        await arm(arming_case)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["empty", "missing-id", "missing-hash", "external", "mixed", "unknown", "incomplete", "wrong-output", "duplicate"])
async def test_owned_evidence_rejects_corruption_even_with_recomputed_digest(arming_case, change):
    evidence = arming_case["manifest"]["source_evidence"]
    source = evidence["sources"][0]
    if change == "empty":
        evidence["sources"] = []
    elif change == "missing-id":
        del source["asset_id"]
    elif change == "missing-hash":
        del source["content_sha256"]
    elif change == "external":
        source["provenance"] = "external"
    elif change == "mixed":
        evidence["sources"].append({**source, "asset_id": str(uuid.uuid4()), "license": "licensed"})
    elif change == "unknown":
        source["license"] = "unknown"
    elif change == "incomplete":
        evidence["sources_complete"] = False
    elif change == "wrong-output":
        evidence["content_sha256"] = "6" * 64
    else:
        evidence["sources"].append(dict(source))
    arming_case["manifest"]["owned_attestation_sha256"] = digest(evidence)
    write_manifest(arming_case)
    with pytest.raises(ValueError):
        await arm(arming_case)


@pytest.mark.asyncio
async def test_absent_manifest_yields_and_arrival_can_arm(arming_case):
    pending = loader(arming_case)
    task = asyncio.create_task(arm(arming_case, pending))
    await asyncio.sleep(0)
    assert not task.done()
    assert not list(arming_case["state"].iterdir())
    write_manifest(arming_case)
    assert await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_cancellation_interrupts_missing_manifest(arming_case):
    cancelled = asyncio.Event()
    task = asyncio.create_task(arm(arming_case, cancelled=cancelled))
    await asyncio.sleep(0)
    cancelled.set()
    with pytest.raises(CancelledError):
        await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
async def test_missing_manifest_has_fixed_45_second_deadline(arming_case, monkeypatch):
    api = arming_api()
    clock = iter([0.0, 46.0])
    monkeypatch.setattr(api, "monotonic", lambda: next(clock))
    with pytest.raises(ValueError, match="timed out"):
        await arm(arming_case)


@pytest.mark.asyncio
async def test_manifest_replacement_during_read_rejects(arming_case, monkeypatch):
    write_manifest(arming_case)
    api = arming_api()
    original = os.read
    replaced = False
    def replacing_read(fd, size):
        nonlocal replaced
        data = original(fd, size)
        if not replaced:
            replaced = True
            arming_case["path"].unlink()
            write_manifest(arming_case)
        return data
    monkeypatch.setattr(api.os, "read", replacing_read)
    with pytest.raises(ValueError):
        await arm(arming_case)


@pytest.mark.asyncio
async def test_handler_does_not_reserve_while_waiting_or_after_cancel(arming_case, protected_dir):
    class NoReservation:
        async def claim(self, context):
            pytest.fail("reservation happened before arming")
    pending = loader(arming_case)
    handler = YouTubeUploadHandler(NoReservation(), base_url="http://youtube-manager", ack_drill_arming=pending)
    media = protected_dir / "input.mp4"
    media.write_bytes(b"owned media")
    config = dict(arming_case["manifest"]["context"])
    config.update(_job_id=config["job_id"], _node_execution_id=config["node_execution_id"],
                  _input_artifact_ids={"input": config["input_artifact_id"]},
                  _execution_claim=config["execution_claim"])
    task = asyncio.create_task(handler.execute(config, {"input": str(media)}, str(protected_dir / "output.mp4")))
    await asyncio.sleep(0)
    assert not task.done()
    handler.cancel()
    with pytest.raises(CancelledError):
        await asyncio.wait_for(task, 1)


def test_arming_module_import_does_not_initialize_handler_registry():
    result = subprocess.run(
        [sys.executable, "-c", "import worker.youtube_ack_drill_arming"], capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_every_event_and_consumed_token_have_flat_identity_and_only_one_post(arming_case, protected_dir):
    write_manifest(arming_case)
    context = arming_case["context"]
    class ArmedStore(FakeOperationStore):
        async def claim(self, actual):
            result = await super().claim(actual)
            self.operation = reserved((await arm(arming_case))._target).operation
            self.operation.id = result.operation.id
            return type(result)(result.action, self.operation)

        async def load_submitted(self, actual, *, operation_id, manager_task_id):
            assert actual == context
            assert operation_id == self.operation.id
            assert manager_task_id == MANAGER_TASK_ID
            assert self.operation.receipt_json == {}
            from app.services.youtube_upload_operations import UploadOperationClaim
            return UploadOperationClaim("resume", self.operation)

        async def mark_succeeded(self, *args, **kwargs):
            result = await super().mark_succeeded(*args, **kwargs)
            result.platform_video_id = result.receipt_json["video_id"]
            result.completed_at = datetime.now(timezone.utc)
            return result
    store = ArmedStore(["submit"])
    requests = []
    def respond(request):
        requests.append((request.method, request.url.path))
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.url.path == "/api/upload":
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            return httpx.Response(200, json={"status": "completed", "result": {
                "video_id": "video-123", "url": "https://www.youtube.com/watch?v=video-123",
            }})
        if request.url.path == "/api/videos/video-123/status":
            return httpx.Response(200, json={"video_id": "video-123", "privacy": "unlisted", "upload_status": "processed"})
        pytest.fail(f"unexpected HTTP identity lookup: {request.url.path}")
    async def refresh(**kwargs):
        pass
    media = protected_dir / "input.mp4"
    media.write_bytes(b"owned media")
    output = protected_dir / "output.mp4"
    config = arming_case["manifest"]["context"]
    config = {**config, "_job_id": config["job_id"], "_node_execution_id": config["node_execution_id"],
              "_input_artifact_ids": {"input": config["input_artifact_id"]}, "_execution_claim": config["execution_claim"]}
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        handler = YouTubeUploadHandler(
            store, client=client, base_url="http://youtube-manager", poll_interval_seconds=0,
            lease_refresher=refresh, ack_drill_arming=loader(arming_case),
        )
        result = await handler.execute(config, {"input": str(media)}, str(output))
    assert result["youtube"]["video_id"] == "video-123"
    assert output.read_bytes() == b"owned media"
    assert requests == [
        ("GET", "/api/auth/status"), ("POST", "/api/upload"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"), ("GET", "/api/videos/video-123/status"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"), ("GET", "/api/videos/video-123/status"),
    ]
    records = [json.loads(path.read_text()) for path in sorted(arming_case["state"].glob("*.json"))]
    assert [record["event"] for record in records[:-1]] == [
        "start", "upload_post_attempt", "submitted_committed", "completed_get_1", "processed_unlisted_get_1",
        "fresh_submitted_empty_receipt", "token_consumed", "pre_receipt_abort", "fresh_submitted_resume",
        "completed_get_2", "processed_unlisted_get_2", "mark_succeeded_commit",
    ]
    assert len(store.claim_contexts) == len(store.attempting) == len(store.submitted) == len(store.succeeded) == 1
    for record in records:
        for key, value in arming_case["manifest"]["arming_identity"].items():
            assert record[key] == value
        assert "arming_identity" not in record


@pytest.mark.parametrize("change", ["no-lease", "no-claim", "unregistered", "worker", "epoch", "expired", "no-attestation", "dictionary-delivery", "no-embedded"])
def test_runtime_admission_requires_real_registered_identity(arming_case, monkeypatch, change):
    if change == "no-lease":
        arming_case["lease"] = None
    elif change == "no-claim":
        arming_case["context"] = replace(arming_case["context"], execution_claim=None)
    elif change in {"unregistered", "worker", "epoch"}:
        changes = {"unregistered": {"registration_id": uuid.uuid4()}, "worker": {"redis_consumer_id": "other"}, "epoch": {"lease_epoch": 8}}
        arming_case["lease"] = replace(arming_case["lease"], **changes[change])
    elif change == "expired":
        arming_case["lease"] = replace(arming_case["lease"], lease_expires_at=datetime(2020, 1, 1, tzinfo=timezone.utc))
    elif change == "no-attestation":
        arming_case["delivery"].attestation_id = None
    elif change == "dictionary-delivery":
        arming_case["delivery"] = vars(arming_case["delivery"])
    else:
        from worker import registration
        monkeypatch.setattr(registration, "EMBEDDED_BUILD_COMMIT", "")
    with pytest.raises(ValueError):
        loader(arming_case)


@pytest.mark.asyncio
async def test_delivery_snapshot_cannot_change_after_construction(arming_case):
    pending = loader(arming_case)
    arming_case["delivery"].message_id = "99-0"
    write_manifest(arming_case)
    helper = await arm(arming_case, pending)
    assert helper._target.arming_identity.message_id == "1234567890-0"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["mode", "missing", "same-control-directory", "prior-token", "prior-partial"])
async def test_state_rejects_unsafe_or_prior_state_before_reservation(arming_case, change):
    state = arming_case["state"]
    if change == "mode":
        state.chmod(0o770)
    elif change == "missing":
        state.rmdir()
    elif change == "same-control-directory":
        arming_case["manifest"]["state_dir"] = str(arming_case["path"].parent)
    else:
        (state / ("consumed.json" if change == "prior-token" else "0001.json")).write_text("{")
    write_manifest(arming_case)
    with pytest.raises((OSError, ValueError, RuntimeError)):
        await arm(arming_case)


@pytest.mark.asyncio
@pytest.mark.parametrize("section,field,value", [
    ("root", "version", True), ("root", "version", 2), ("root", "unknown", "x"),
    ("arming_identity", "message_id", "*"), ("arming_identity", "audit_window_id", "*"),
    ("arming_identity", "service_name", "secret\ncontrol"),
    ("account_attestation", "account_id", "*"), ("source_evidence", "version", True),
])
async def test_manifest_types_unknown_fields_and_wildcards_fail_closed(arming_case, section, field, value):
    target = arming_case["manifest"] if section == "root" else arming_case["manifest"][section]
    target[field] = value
    write_manifest(arming_case)
    with pytest.raises(ValueError):
        await arm(arming_case)


@pytest.mark.asyncio
async def test_cancellation_does_not_wait_for_a_stalled_control_read(arming_case, monkeypatch):
    api = arming_api()
    started, release = threading.Event(), threading.Event()
    def stalled_read(path):
        started.set()
        release.wait(3)
        return None
    monkeypatch.setattr(api, "_read_manifest", stalled_read)
    cancelled = asyncio.Event()
    task = asyncio.create_task(arm(arming_case, cancelled=cancelled))
    try:
        await asyncio.to_thread(started.wait, 1)
        assert started.is_set()
        cancelled.set()
        with pytest.raises(CancelledError):
            await asyncio.wait_for(task, 0.5)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_control_directory_cannot_be_replaced_between_wait_polls(arming_case, monkeypatch):
    api = arming_api()
    original = api._read_manifest
    first_read = threading.Event()
    def observe_read(path):
        result = original(path)
        first_read.set()
        return result
    monkeypatch.setattr(api, "_read_manifest", observe_read)
    task = asyncio.create_task(arm(arming_case))
    await asyncio.to_thread(first_read.wait, 1)
    assert first_read.is_set()
    parent = arming_case["path"].parent
    parent.rename(parent.with_name("old-operator"))
    parent.mkdir(mode=0o700)
    write_manifest(arming_case)
    with pytest.raises(ValueError, match="replaced"):
        await asyncio.wait_for(task, 2)


def reservation_probe(case, protected_dir):
    reached = []
    class StopAtReservation:
        async def claim(self, actual):
            reached.append(actual)
            raise RuntimeError("unexpected reservation")
    handler = YouTubeUploadHandler(
        StopAtReservation(), base_url="http://youtube-manager", ack_drill_arming=loader(case),
    )
    media = protected_dir / "input.mp4"
    media.write_bytes(b"owned media")
    context = case["manifest"]["context"]
    config = {
        **context, "_job_id": context["job_id"], "_node_execution_id": context["node_execution_id"],
        "_input_artifact_ids": {"input": context["input_artifact_id"]},
        "_execution_claim": context["execution_claim"],
    }
    return handler, handler.execute(config, {"input": str(media)}, str(protected_dir / "output.mp4")), reached


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["cancel", "timeout"])
async def test_stalled_state_validation_keeps_heartbeat_live_and_never_reserves(
    arming_case, protected_dir, monkeypatch, stop,
):
    write_manifest(arming_case)
    api = arming_api()
    handler, execution, reached = reservation_probe(arming_case, protected_dir)
    if stop == "timeout":
        ticks = iter([0.0])
        # Keep the actual 45-second deadline; consume 44.8 seconds before the read.
        monkeypatch.setattr(api, "monotonic", lambda: next(ticks, 44.8))
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    state_inode = arming_case["state"].stat().st_ino
    original = os.listdir
    def stalled_state(path):
        if not isinstance(path, int) or os.fstat(path).st_ino != state_inode:
            return original(path)
        started.set()
        try:
            release.wait(0.75)
            return original(path)
        finally:
            finished.set()
    monkeypatch.setattr(os, "listdir", stalled_state)
    beats = []
    async def heartbeat():
        while True:
            beats.append(1)
            await asyncio.sleep(0.005)
    heartbeat_task = asyncio.create_task(heartbeat())
    task = asyncio.create_task(execution)
    try:
        assert await asyncio.to_thread(started.wait, 1)
        before = len(beats)
        await asyncio.sleep(0.02)
        assert not finished.is_set(), "state read blocked the event loop until completion"
        assert len(beats) > before
        if stop == "cancel":
            handler.cancel()
        expected = CancelledError if stop == "cancel" else ValueError
        with pytest.raises(expected, match="cancelled|timed out"):
            await asyncio.wait_for(task, 0.5)
        assert reached == []
        assert not finished.is_set(), "arming waited for the blocked state read"
    finally:
        release.set()
        heartbeat_task.cancel()
        await asyncio.gather(task, heartbeat_task, return_exceptions=True)
        await asyncio.to_thread(finished.wait, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("expires_during", ["state-validation", "arming-return"])
async def test_expiry_at_end_of_arming_never_reaches_store_claim(
    arming_case, protected_dir, monkeypatch, expires_during,
):
    from worker import youtube_ack_drill as helper_module

    expires = datetime.now(timezone.utc) + timedelta(seconds=10)
    arming_case["manifest"]["expires_at"] = expires.isoformat()
    write_manifest(arming_case)
    clock = [datetime.now(timezone.utc).timestamp()]
    monkeypatch.setattr(helper_module.time, "time", lambda: clock[0])
    if expires_during == "state-validation":
        original = os.listdir
        state_inode = arming_case["state"].stat().st_ino
        def expire_in_state_check(path):
            result = original(path)
            if isinstance(path, int) and os.fstat(path).st_ino == state_inode:
                clock[0] = expires.timestamp() + 1
            return result
        monkeypatch.setattr(os, "listdir", expire_in_state_check)
    else:
        original_arm = arming_api().AckDrillArming.arm
        async def expire_after_actual_arming(self, *args, **kwargs):
            helper = await original_arm(self, *args, **kwargs)
            clock[0] = expires.timestamp() + 1
            return helper
        monkeypatch.setattr(arming_api().AckDrillArming, "arm", expire_after_actual_arming)
    _, execution, reached = reservation_probe(arming_case, protected_dir)
    with pytest.raises(ValueError, match="expired"):
        await execution
    assert reached == []


@pytest.mark.asyncio
async def test_missing_manifest_poll_does_not_extend_the_45_second_budget(arming_case, monkeypatch):
    api = arming_api()
    clock = [0.0]
    original_read = api._read_manifest
    def almost_expired_read(path):
        result = original_read(path)
        clock[0] = 44.98
        return result
    async def advance_poll(awaitable, *, timeout):
        clock[0] += timeout
        awaitable.close()
        raise TimeoutError
    monkeypatch.setattr(api, "monotonic", lambda: clock[0])
    monkeypatch.setattr(api, "_read_manifest", almost_expired_read)
    monkeypatch.setattr(api.asyncio, "wait_for", advance_poll)
    with pytest.raises(ValueError, match="timed out"):
        await arm(arming_case)
    assert clock[0] <= 45.0
