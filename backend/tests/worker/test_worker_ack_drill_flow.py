"""Full registered worker proof with scratch PostgreSQL and transport-only doubles."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select, text

from app.models.artifact import Artifact
from app.models.job import Job, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    WorkerEventEmission, WorkerTaskDeliveryAttestation, WorkerTaskDispatch,
)
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.orchestrator.dag import validate_pipeline
from app.schemas.pipeline import PipelineDefinition
from app.services.registered_worker_event_receipt import _IDEMPOTENT_XADD_SCRIPT
from app.services.youtube_upload_operations import YouTubeUploadOperationStore
from tests.worker.ack_drill_postgres import (
    ack_drill_database as _ack_drill_database,
    ack_drill_runtime as _ack_drill_runtime,
)
from tests.worker.test_youtube_ack_drill import journal_records
from tests.worker.test_youtube_ack_drill_arming import (
    arming_case as _arming_case, protected_dir as _protected_dir, digest, write_manifest,
)
from tests.worker.test_youtube_upload_handler import (
    MANAGER_TASK_ID, auth_payload, media_paths as _media_paths,
)
from worker import main as worker_main
from worker import registration as registration_module
from worker.handlers import youtube_upload as upload_module
from worker.youtube_ack_drill import InjectedPreReceiptAbort, OwnedUnlistedAckDrill
from worker.youtube_ack_drill_arming import AckDrillArming


ack_drill_database = _ack_drill_database
ack_drill_runtime = _ack_drill_runtime
media_paths = _media_paths
arming_case = _arming_case
protected_dir = _protected_dir
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.parametrize("ack_drill_runtime", ["whole_worker"], indirect=True),
]


class RedisTransport:
    """Only Redis I/O is doubled; SQL dispatch/event/ACK authority stays real."""

    def __init__(self):
        self.dispatches = []
        self.events = []
        self.acks = []
        self.heartbeats = []
        self.markers = {}
        self.sequence_base = uuid.uuid4().int & ((1 << 63) - 1)
        self.observe = None
        self.heartbeat_seen = asyncio.Event()

    async def eval(self, script, key_count, stream, marker, *fields):
        assert key_count == 2 and len(fields) % 2 == 0
        payload = dict(zip(fields[::2], fields[1::2], strict=True))
        is_dispatch = marker.startswith("vp:worker-task-dispatch:")
        assert script == (_IDEMPOTENT_XADD_SCRIPT if is_dispatch else worker_main._IDEMPOTENT_EVENT_XADD_SCRIPT)
        if self.observe is not None:
            await self.observe("event", payload)
        if marker not in self.markers:
            message_id = f"1710000000000-{self.sequence_base + len(self.markers) + 1}"
            self.markers[marker] = message_id
            (self.dispatches if is_dispatch else self.events).append((stream, message_id, payload))
        return self.markers[marker]

    async def xack(self, stream, group, message_id):
        if self.observe is not None:
            await self.observe("ack", (stream, group, message_id))
        self.acks.append((stream, group, message_id))
        return 1

    async def xclaim(self, stream, group, consumer, *, min_idle_time, message_ids):
        self.heartbeats.append((stream, group, consumer, min_idle_time, message_ids))
        self.heartbeat_seen.set()
        return []

    async def aclose(self):
        pass


@pytest.fixture
def worker_redis():
    return RedisTransport()


async def test_whole_worker_fixture_leaves_claim_to_normal_message_processing(ack_drill_runtime):
    runtime = ack_drill_runtime
    async with runtime.owner_sessions() as db:
        node = await db.get(NodeExecution, runtime.node_id)
        assert node.status == NodeStatus.QUEUED
        assert node.worker_id is node.started_at is node.worker_registration_id is None
        assert node.worker_lease_epoch is None
        job = await db.get(Job, node.job_id)
        validation = validate_pipeline(PipelineDefinition.model_validate(job.pipeline_snapshot))
        assert validation.valid, validation.errors
        artifact = await db.get(Artifact, node.input_artifact_ids[0])
        source = await db.get(NodeExecution, artifact.node_execution_id)
        assert source.id != node.id and source.job_id == node.job_id
        assert source.status == NodeStatus.SUCCEEDED
        assert source.output_artifact_id == artifact.id
        dispatch = (await db.scalars(select(WorkerTaskDispatch).where(
            WorkerTaskDispatch.node_execution_id == node.id,
        ))).one()
        assert dispatch.delivery_state == "delivered"
        assert dispatch.resolution_state == "unresolved"
        assert dispatch.redis_message_id == runtime.message_id
        assert dispatch.payload_json == runtime.payload
        assert json.loads(runtime.payload["config"])["privacy"] == "public"
        assert node.node_config == {"title": "Owned canary", "privacy": "unlisted"}
        assert not (await db.scalars(select(WorkerTaskDeliveryAttestation).where(
            WorkerTaskDeliveryAttestation.node_execution_id == node.id,
        ))).all()


async def test_worker_recovers_only_the_second_get_before_normal_output_event_and_ack(flow):
    # Catches premature receipt/output/ACK, first-result reuse, and recursive resubmission.
    async with flow.running():
        await flow.reach(flow.abort_reached)
        boundary = await flow.pre_receipt()
        assert len(flow.aborts) == 1 and flow.authenticated == []
        assert len(flow.loads) == 1 and flow.writes == []
        assert journal_records(flow.case["state"])[-1]["event"] == "pre_receipt_abort"
        assert (flow.case["state"] / "consumed.json").is_file()
        await asyncio.wait_for(flow.redis.heartbeat_seen.wait(), 2)
        await flow.runtime.refresh(minimum_margin_seconds=150)
        flow.release_abort.set()

        await flow.reach(flow.resume_reached)
        resumed = await flow.pre_receipt()
        assert flow.authenticated == flow.aborts
        assert len(flow.loads) == 2 and flow.loads[0] is not flow.loads[1]
        assert flow.loads[0].action == flow.loads[1].action == "resume"
        assert flow.loads[0].operation.id == flow.loads[1].operation.id == boundary.operation.id
        assert flow.identity(boundary) == flow.identity(resumed)
        assert flow.requests == flow.expected_requests[:4]
        flow.release_resume.set()

        await flow.reach(flow.commit_reached)
        committed = await flow.snapshot()
        assert committed.operation.status == "succeeded"
        assert committed.operation.receipt_json["title"] == "SECOND distinct receipt"
        assert committed.operation.platform_video_id == "video-123"
        assert committed.operation.completed_at is not None
        assert flow.identity(committed) == flow.identity(boundary)
        flow.assert_no_output(committed)
        assert not list(flow.output_dir.glob("*.mp4"))
        flow.release_commit.set()
        await asyncio.wait_for(flow.task, 10)

    final = await flow.snapshot()
    assert len(flow.claims) == len(flow.writes) == len(flow.aborts) == len(flow.authenticated) == 1
    assert flow.requests == flow.expected_requests
    assert flow.identity(final) == flow.identity(boundary)
    assert final.node.retry_count == final.job.retry_count == 0
    assert final.node.error_message is final.job.error_message is None
    assert final.job.parent_job_id is None
    assert len(flow.redis.dispatches) == 1
    assert flow.transitions == ["claim", "mark_attempting", "mark_submitted", "mark_succeeded"]
    assert flow.timeline == ["injected", "authenticated", "resume_loaded", "receipt_committed", "event", "ack"]
    assert len(final.outputs) == len(final.emissions) == len(flow.redis.events) == len(flow.redis.acks) == 1
    output, emission = final.outputs[0], final.emissions[0]
    receipt = final.operation.receipt_json
    assert output.media_info["youtube"] == receipt
    assert receipt["title"] == "SECOND distinct receipt"
    assert output.job_id == final.job.id and output.node_execution_id == final.node.id
    assert Path(output.storage_path).read_bytes() == Path(flow.input_path).read_bytes()
    assert len(list(flow.output_dir.glob("*.mp4"))) == 1
    assert emission.event_type == "node_completed" and emission.emission_state == "emitted"
    assert emission.payload_json == flow.redis.events[0][2]
    assert emission.payload_json["output_artifact_id"] == str(output.id)
    assert emission.payload_json["task_dispatch_key"] == str(final.dispatch.dispatch_key)
    assert emission.payload_json["task_message_id"] == final.dispatch.redis_message_id
    assert emission.payload_json["task_payload_sha256"] == final.dispatch.payload_sha256
    assert emission.payload_sha256 == digest(emission.payload_json)
    assert emission.worker_started_at == final.node.started_at
    assert emission.worker_registration_id == flow.runtime.lease.registration_id
    assert emission.worker_lease_epoch == flow.runtime.lease.lease_epoch
    assert emission.source_task_attestation_id == final.attestation.id
    assert emission.message_id == flow.redis.events[0][1]
    assert final.attestation.ack_state == "acknowledged"
    assert final.attestation.ack_event_emission_id == emission.id
    assert final.attestation.acknowledged_at is not None
    assert final.operation.completed_at <= emission.prepared_at <= emission.emitted_at <= final.attestation.acknowledged_at
    assert flow.redis.acks == [(flow.stream, flow.group, flow.runtime.message_id)]
    assert all(beat == (flow.stream, flow.group, flow.runtime.lease.redis_consumer_id, 0,
                        [flow.runtime.message_id]) for beat in flow.redis.heartbeats)
    records = journal_records(flow.case["state"])
    assert [record["event"] for record in records] == [
        "start", "upload_post_attempt", "submitted_committed", "completed_get_1",
        "processed_unlisted_get_1", "fresh_submitted_empty_receipt", "token_consumed",
        "pre_receipt_abort", "fresh_submitted_resume", "completed_get_2",
        "processed_unlisted_get_2", "mark_succeeded_commit",
    ]
    assert [record["sequence"] for record in records] == list(range(1, 13))
    assert records[-1]["receipt_sha256"] == digest(receipt)
    for record in records:
        assert record["operation_id"] == str(final.operation.id)
        assert record["job_id"] == str(final.job.id)
        assert record["node_execution_id"] == str(final.node.id)
        assert record["content_sha256"] == final.operation.content_sha256
        assert record["message_id"] == flow.runtime.message_id
        assert record["dispatch_key"] == str(flow.runtime.dispatch_key)
        assert record["attestation_id"] == str(final.attestation.id)
    assert worker_main._current_task_delivery.get() is None


@pytest.mark.parametrize("loss", ["job_cancelled", "lease_fenced"])
async def test_actual_authority_loss_after_injected_abort_never_succeeds_or_reuploads(flow, loss):
    # Catches recovery that trusts the token instead of checking fresh SQL authority.
    async with flow.running():
        await flow.reach(flow.abort_reached)
        boundary = await flow.pre_receipt()
        if loss == "job_cancelled":
            await flow.runtime.owner.execute(
                "UPDATE public.jobs SET status='CANCELLED' WHERE id=$1", flow.runtime.job_id,
            )
        else:
            await flow.runtime.owner.execute(
                "UPDATE public.worker_registrations SET lease_expires_at=clock_timestamp() WHERE id=$1",
                flow.runtime.lease.registration_id,
            )
        flow.release_abort.set()
        await asyncio.wait_for(flow.task, 10)
    final = await flow.pre_receipt(active=False)
    assert flow.identity(final) == flow.identity(boundary)
    assert len(flow.claims) == len(flow.aborts) == len(flow.authenticated) == 1
    assert flow.authenticated == flow.aborts and flow.writes == []
    assert not flow.resume_reached.is_set() and not flow.commit_reached.is_set()
    assert flow.requests == flow.expected_requests[:4]
    assert flow.transitions == ["claim", "mark_attempting", "mark_submitted"]
    assert [record["event"] for record in journal_records(flow.case["state"])][-1] == "pre_receipt_abort"
    assert (flow.case["state"] / "consumed.json").is_file()
    assert worker_main._current_task_delivery.get() is None


class FlowProbe:
    stream = "vp:tasks:youtube_publisher"
    group = "youtube_publisher-workers"
    expected_requests = [
        ("GET", "/api/auth/status"), ("POST", "/api/upload"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"), ("GET", "/api/videos/video-123/status"),
        ("GET", f"/api/status/{MANAGER_TASK_ID}"), ("GET", "/api/videos/video-123/status"),
    ]

    def __init__(self, runtime, redis, case, media_paths, output_root):
        self.runtime, self.redis, self.case = runtime, redis, case
        self.input_path = media_paths[0]["input"]
        self.output_dir = output_root / "artifacts" / str(runtime.job_id)
        self.output_root = output_root
        self.claims, self.loads, self.writes, self.aborts, self.authenticated = [], [], [], [], []
        self.requests, self.timeline, self.transitions = [], [], []
        self.abort_reached, self.release_abort = asyncio.Event(), asyncio.Event()
        self.resume_reached, self.release_resume = asyncio.Event(), asyncio.Event()
        self.commit_reached, self.release_commit = asyncio.Event(), asyncio.Event()

    async def snapshot(self):
        # Independent sessions observe commits, never the worker's uncommitted ORM objects.
        async with self.runtime.owner_sessions() as db:
            async def rows(model):
                return list(await db.scalars(select(model).where(model.node_execution_id == self.runtime.node_id)))
            operations = await rows(YouTubeUploadOperation)
            attestations = await rows(WorkerTaskDeliveryAttestation)
            dispatches = await rows(WorkerTaskDispatch)
            assert len(operations) == len(attestations) == len(dispatches) == 1
            return SimpleNamespace(
                operation=operations[0], attestation=attestations[0], dispatch=dispatches[0],
                outputs=await rows(Artifact), emissions=await rows(WorkerEventEmission),
                node=await db.get(NodeExecution, self.runtime.node_id),
                job=await db.get(Job, self.runtime.job_id),
            )

    def identity(self, state):
        op, node, delivery, dispatch = state.operation, state.node, state.attestation, state.dispatch
        return (
            op.id, op.job_id, op.node_execution_id, op.production_task_id, op.manager_task_id,
            op.input_artifact_id, op.content_sha256, op.title, op.privacy, op.request_attempted_at,
            node.started_at, node.worker_id, node.worker_registration_id, node.worker_lease_epoch,
            delivery.id, delivery.worker_started_at, delivery.message_id, delivery.dispatch_key,
            dispatch.id, dispatch.redis_message_id, dispatch.dispatch_key, dispatch.payload_sha256,
        )

    def assert_no_output(self, state):
        assert state.outputs == state.emissions == self.redis.events == self.redis.acks == []
        assert state.attestation.ack_state == "pending"
        assert state.attestation.acknowledged_at is state.attestation.ack_event_emission_id is None

    async def pre_receipt(self, *, active=True):
        state = await self.snapshot()
        assert state.operation.status == "submitted" and state.operation.receipt_json == {}
        assert state.operation.platform_video_id is state.operation.completed_at is None
        assert state.operation.request_attempted_at is not None
        assert state.operation.manager_task_id == MANAGER_TASK_ID
        assert state.operation.content_sha256 == hashlib.sha256(Path(self.input_path).read_bytes()).hexdigest()
        assert state.operation.title == "Owned canary" and state.operation.privacy == "unlisted"
        assert state.operation.job_id == state.node.job_id == self.runtime.job_id
        assert state.operation.node_execution_id == state.node.id == self.runtime.node_id
        assert state.operation.production_task_id == self.runtime.task_id
        assert state.operation.input_artifact_id == self.runtime.artifact_id
        assert state.node.worker_id == self.runtime.lease.redis_consumer_id
        assert state.node.worker_registration_id == self.runtime.lease.registration_id
        assert state.node.worker_lease_epoch == self.runtime.lease.lease_epoch
        assert state.attestation.worker_started_at == state.node.started_at
        assert state.attestation.worker_id == state.node.worker_id
        assert state.attestation.worker_registration_id == state.node.worker_registration_id
        assert state.attestation.worker_lease_epoch == state.node.worker_lease_epoch
        assert state.attestation.message_id == state.dispatch.redis_message_id == self.runtime.message_id
        assert state.attestation.dispatch_key == state.dispatch.dispatch_key == self.runtime.dispatch_key
        assert state.attestation.payload_sha256 == state.dispatch.payload_sha256 == self.runtime.payload_sha256
        assert state.attestation.redis_stream == state.dispatch.redis_stream == self.stream
        assert state.attestation.consumer_group == state.dispatch.consumer_group == self.group
        assert state.job.pipeline_snapshot == self.runtime.snapshot
        assert state.node.status == NodeStatus.RUNNING and state.node.started_at is not None
        assert state.node.retry_count == state.job.retry_count == 0
        assert state.node.output_artifact_id is state.node.error_message is state.job.error_message is None
        if active:
            assert state.job.status == "RUNNING"
        assert state.dispatch.delivery_state == "delivered" and state.dispatch.resolution_state == "unresolved"
        assert state.dispatch.payload_json == self.runtime.payload
        assert len(self.redis.dispatches) == 1
        self.assert_no_output(state)
        assert not list(self.output_dir.glob("*.mp4"))
        return state

    async def reach(self, event):
        waiting = asyncio.create_task(event.wait())
        try:
            done, _ = await asyncio.wait((self.task, waiting), timeout=10, return_when=asyncio.FIRST_COMPLETED)
            if self.task in done:
                await self.task
            assert waiting in done and not self.task.done(), "worker did not reach the guarded boundary"
        finally:
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)

    @asynccontextmanager
    async def running(self):
        self.task = asyncio.create_task(worker_main._process_message(
            self.redis, self.runtime.message_id, self.runtime.payload,
            worker_lease=self.runtime.lease, lease_refresher=self.runtime.refresh,
        ))
        try:
            yield
        finally:
            if not self.task.done():
                self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)

    async def transport_observation(self, phase, payload):
        state = await self.snapshot()
        assert self.timeline[-1] == ("receipt_committed" if phase == "event" else "event")
        assert state.operation.status == "succeeded"
        assert state.operation.receipt_json["title"] == "SECOND distinct receipt"
        assert len(state.outputs) == len(state.emissions) == 1
        output, emission = state.outputs[0], state.emissions[0]
        assert output.media_info["youtube"] == state.operation.receipt_json
        assert emission.source_task_attestation_id == state.attestation.id
        if phase == "event":
            assert emission.emission_state == "prepared" and emission.message_id is None
            assert payload == emission.payload_json and payload["event"] == "node_completed"
            assert payload["output_artifact_id"] == str(output.id)
            assert state.attestation.ack_state == "pending"
        else:
            assert emission.emission_state == "emitted" and emission.emitted_at is not None
            assert payload == (self.stream, self.group, self.runtime.message_id)
            assert len(self.redis.events) == 1
        self.timeline.append(phase)

    def write_actual_manifest(self, context, arming):
        manifest = self.case["manifest"]
        manifest["production_task_id"] = str(self.runtime.task_id)
        manifest["context"] = {
            "job_id": str(context.job_id), "node_execution_id": str(context.node_execution_id),
            "input_artifact_id": str(context.input_artifact_id), "content_sha256": context.content_sha256,
            "title": context.title, "privacy": context.privacy,
            "execution_claim": {
                "worker_id": context.execution_claim.worker_id,
                "started_at": context.execution_claim.started_at.isoformat(),
                "worker_registration_id": str(context.execution_claim.worker_registration_id),
                "worker_lease_epoch": context.execution_claim.worker_lease_epoch,
            },
        }
        metadata = manifest["arming_identity"]
        metadata.update(
            release_commit=self.runtime.release, channel_id=str(self.runtime.channel_id),
            account_id=str(self.runtime.account_id), service_name=self.runtime.lease.service_name,
            redis_stream=arming.redis_stream, consumer_group=arming.consumer_group,
            message_id=arming.message_id, payload_sha256=arming.payload_sha256,
            dispatch_key=arming.dispatch_key, attestation_id=arming.attestation_id,
        )
        source = manifest["source_evidence"]
        source.update(
            production_task_id=str(self.runtime.task_id), job_id=str(context.job_id),
            channel_id=metadata["channel_id"], account_id=metadata["account_id"],
            input_artifact_id=str(context.input_artifact_id), content_sha256=context.content_sha256,
            graph_sha256=digest(self.runtime.snapshot), sources=[{
                "asset_id": str(self.runtime.asset_id), "content_sha256": context.content_sha256,
                "license": "owned", "provenance": "generated",
            }],
        )
        manifest["owned_attestation_sha256"] = digest(source)
        manifest["account_attestation"]["account_id"] = metadata["account_id"]
        write_manifest(self.case)

    async def http_route(self, request):
        self.requests.append((request.method, request.url.path))
        assert request.url.host == "youtube-manager"
        assert self.requests == self.expected_requests[:len(self.requests)]
        if request.url.path == "/api/auth/status":
            return httpx.Response(200, json=auth_payload())
        if request.method == "POST":
            body = await request.aread()
            assert Path(self.input_path).read_bytes() in body
            assert b"unlisted" in body and b"Owned canary" in body
            assert journal_records(self.case["state"])[-1]["event"] == "upload_post_attempt"
            return httpx.Response(200, json={"task_id": MANAGER_TASK_ID})
        if request.url.path == f"/api/status/{MANAGER_TASK_ID}":
            second = len(self.requests) == 5
            if second:
                assert self.timeline[-1] == "resume_loaded"
            await self.pre_receipt()
            return httpx.Response(200, json={"status": "completed", "result": {
                "video_id": "video-123", "url": "https://www.youtube.com/watch?v=video-123",
                "title": "SECOND distinct receipt" if second else "FIRST discarded receipt",
            }})
        assert request.headers["cache-control"] == "no-cache, no-store"
        return httpx.Response(200, json={
            "video_id": "video-123", "privacy": "unlisted", "upload_status": "processed",
        })

    def install(self, monkeypatch):
        for key, value in {
            "WORKER_SERVICE_NAME": self.runtime.lease.service_name,
            "WORKER_RELEASE_COMMIT": self.runtime.release,
            "WORKER_REDIS_STREAM": self.stream, "WORKER_REDIS_GROUP": self.group,
        }.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setattr(registration_module, "EMBEDDED_BUILD_COMMIT", self.runtime.release)
        monkeypatch.setattr(worker_main, "TASK_STREAM", self.stream)
        monkeypatch.setattr(worker_main, "CONSUMER_GROUP", self.group)
        monkeypatch.setattr(worker_main, "WORKER_ID", self.runtime.lease.redis_consumer_id)
        monkeypatch.setattr(worker_main, "HEARTBEAT_INTERVAL", 0.01)
        monkeypatch.setattr(worker_main, "worker_session", self.runtime.sessions)
        monkeypatch.setattr(worker_main, "_redis", lambda: self.redis)
        monkeypatch.setattr(worker_main.settings, "storage_backend", "local")
        monkeypatch.setattr(worker_main.settings, "storage_local_root", str(self.output_root))
        monkeypatch.setattr(upload_module.settings, "youtube_manager_url", "http://youtube-manager")
        real_client = httpx.AsyncClient
        monkeypatch.setattr(upload_module.httpx, "AsyncClient", lambda **kwargs: real_client(
            **kwargs, transport=httpx.MockTransport(self.http_route), trust_env=False,
        ))
        self.redis.observe = self.transport_observation
        real_arm = AckDrillArming.arm

        async def arm(arming, context, **kwargs):
            assert type(arming) is AckDrillArming
            assert worker_main._current_task_delivery.get().attestation_id == uuid.UUID(arming.attestation_id)
            self.write_actual_manifest(context, arming)
            helper = await real_arm(arming, context, **kwargs)
            assert type(helper) is OwnedUnlistedAckDrill
            return helper

        monkeypatch.setattr(AckDrillArming, "arm", arm)
        real_claim = YouTubeUploadOperationStore.claim

        async def claim(store, context):
            assert type(store) is YouTubeUploadOperationStore
            async with store._session_factory() as db:
                assert await db.scalar(text("SELECT session_user")) == self.runtime.role
            self.claims.append(context)
            self.transitions.append("claim")
            return await real_claim(store, context)

        monkeypatch.setattr(YouTubeUploadOperationStore, "claim", claim)
        real_before = OwnedUnlistedAckDrill.before_receipt

        async def before(helper, *args, **kwargs):
            try:
                return await real_before(helper, *args, **kwargs)
            except InjectedPreReceiptAbort as abort:
                await self.pre_receipt()
                self.aborts.append(abort)
                self.timeline.append("injected")
                self.abort_reached.set()
                await self.release_abort.wait()
                raise

        monkeypatch.setattr(OwnedUnlistedAckDrill, "before_receipt", before)
        real_authenticate = OwnedUnlistedAckDrill.authenticate_abort

        def authenticate(helper, abort, *args):
            result = real_authenticate(helper, abort, *args)
            self.authenticated.append(abort)
            self.timeline.append("authenticated")
            return result

        monkeypatch.setattr(OwnedUnlistedAckDrill, "authenticate_abort", authenticate)
        real_load = YouTubeUploadOperationStore.load_submitted

        async def load(store, context, **kwargs):
            result = await real_load(store, context, **kwargs)
            assert store._active_submission_fence.get() is None
            assert context == self.claims[0]
            self.loads.append(result)
            await self.pre_receipt()
            if len(self.loads) == 2:
                assert self.authenticated == self.aborts and len(self.aborts) == 1
                self.timeline.append("resume_loaded")
                self.resume_reached.set()
                await self.release_resume.wait()
            return result

        monkeypatch.setattr(YouTubeUploadOperationStore, "load_submitted", load)
        real_succeed = YouTubeUploadOperationStore.mark_succeeded

        async def succeed(store, operation_id, video_id, receipt, *, context):
            await self.pre_receipt()
            assert len(self.loads) == 2 and context == self.claims[0]
            assert receipt["title"] == "SECOND distinct receipt" and video_id == "video-123"
            self.writes.append((operation_id, video_id, dict(receipt)))
            result = await real_succeed(store, operation_id, video_id, receipt, context=context)
            self.transitions.append("mark_succeeded")
            self.timeline.append("receipt_committed")
            committed = await self.snapshot()
            assert committed.operation.receipt_json == result.receipt_json
            assert committed.operation.status == "succeeded"
            self.assert_no_output(committed)
            self.commit_reached.set()
            await self.release_commit.wait()
            return result

        monkeypatch.setattr(YouTubeUploadOperationStore, "mark_succeeded", succeed)
        for name in ("mark_attempting", "mark_submitted", "mark_failed", "mark_uncertain"):
            original = getattr(YouTubeUploadOperationStore, name)

            async def transition(store, *args, _name=name, _original=original, **kwargs):
                self.transitions.append(_name)
                return await _original(store, *args, **kwargs)

            monkeypatch.setattr(YouTubeUploadOperationStore, name, transition)


@pytest.fixture
def flow(ack_drill_runtime, worker_redis, arming_case, media_paths, tmp_path, monkeypatch):
    probe = FlowProbe(ack_drill_runtime, worker_redis, arming_case, media_paths, tmp_path / "storage")
    probe.install(monkeypatch)
    return probe
