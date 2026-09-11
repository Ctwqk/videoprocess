from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerEventEmission,
    WorkerTaskDeliveryAttestation,
    WorkerTaskDispatch,
)
from app.models.schedule import RuntimeSchedule
from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.orchestrator.engine import JobEngine
from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    claim_registered_worker_node,
)
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventReceiptService,
    canonical_redis_payload_sha256,
    parse_registered_worker_event,
)
from tests.orchestrator.test_engine_registered_receipts import (
    _seed,
    registered_engine_factory as _registered_engine_factory,
)

registered_engine_factory = _registered_engine_factory


@pytest.fixture
async def retry_case(registered_engine_factory):
    factory = registered_engine_factory
    async with factory().bind.begin() as connection:
        for model in (
            ChannelProfile,
            ProductionTask,
            RuntimeSchedule,
            WorkerAdmissionGrant,
            WorkerRegistration,
            WorkerEventEmission,
        ):
            await connection.run_sync(model.__table__.create)
    node_id, _, _, completed = await _seed(factory)
    now = datetime.now(timezone.utc)
    payload = {
        **completed.payload,
        "event": "node_failed",
        "error": "bounded failure",
        "task_stream": "vp:tasks:ffmpeg_go",
        "task_group": "ffmpeg_go-workers",
    }
    payload.pop("output_artifact_id")
    task_payload = {
        "job_id": str(completed.job_id),
        "node_execution_id": str(node_id),
        "dispatch_key": payload["task_dispatch_key"],
    }
    payload["task_payload_sha256"] = canonical_redis_payload_sha256(task_payload)
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id=completed.message_id,
        payload=payload,
    )
    async with factory() as db:
        grant = WorkerAdmissionGrant(
            service_name="retry-test",
            generation=1,
            worker_type="ffmpeg",
            worker_host="test",
            capabilities_json=["ffmpeg"],
            release_commit="a" * 40,
            image_identity="test-image",
            database_principal="test-worker",
            redis_stream=event.source_task_stream,
            redis_group=event.source_task_group,
            endpoint_bindings_json={},
            token_sha256="a" * 64,
            state="active",
            issued_at=now,
            issued_by="test",
            activated_at=now,
        )
        db.add(grant)
        await db.flush()
        registration = WorkerRegistration(
            id=event.claim.worker_registration_id,
            grant_id=grant.id,
            service_name="retry-test",
            worker_type="ffmpeg",
            worker_host="test",
            capabilities_json=["ffmpeg"],
            worker_instance_id=uuid.uuid4(),
            worker_slot=1,
            redis_consumer_id=event.claim.worker_id,
            image_identity="test-image",
            database_principal="test-worker",
            database_fingerprint="a" * 64,
            redis_fingerprint="b" * 64,
            storage_fingerprint="c" * 64,
            lease_epoch=event.claim.worker_lease_epoch,
            lease_secret_sha256="d" * 64,
            status="active",
            registered_at=now,
            heartbeat_at=now,
            lease_expires_at=now + timedelta(hours=1),
        )
        channel = ChannelProfile(name="Retry test")
        db.add_all([registration, channel])
        await db.flush()
        job = await db.get(Job, event.job_id)
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=uuid.uuid4(),
            prompt="test",
            job_id=job.id,
            pipeline_id=job.pipeline_id,
            state="producing",
        )
        schedule = RuntimeSchedule(
            service_name="videoprocess", state="OPEN", guarded_job_id=job.id
        )
        original = WorkerTaskDispatch(
            dispatch_key=event.source_task_dispatch_key,
            job_id=job.id,
            node_execution_id=node_id,
            redis_stream=event.source_task_stream,
            consumer_group=event.source_task_group,
            payload_sha256=event.source_task_payload_sha256,
            payload_json=task_payload,
            delivery_state="delivered",
            delivery_attempted_at=now,
            delivered_at=now,
            redis_message_id=event.source_task_message_id,
        )
        attestation = WorkerTaskDeliveryAttestation(
            redis_stream=original.redis_stream,
            consumer_group=original.consumer_group,
            message_id=original.redis_message_id,
            payload_sha256=original.payload_sha256,
            dispatch_key=original.dispatch_key,
            job_id=job.id,
            node_execution_id=node_id,
            worker_registration_id=registration.id,
            worker_lease_epoch=registration.lease_epoch,
            worker_id=event.claim.worker_id,
            worker_started_at=event.claim.started_at,
        )
        db.add_all([task, schedule, original, attestation])
        await db.flush()
        emission = WorkerEventEmission(
            source_task_attestation_id=attestation.id,
            redis_stream=event.redis_stream,
            consumer_group=event.consumer_group,
            message_id=event.message_id,
            payload_sha256=event.payload_sha256,
            payload_json=event.payload,
            event_type=event.event_type,
            job_id=job.id,
            node_execution_id=node_id,
            worker_registration_id=registration.id,
            worker_lease_epoch=registration.lease_epoch,
            worker_id=event.claim.worker_id,
            worker_started_at=event.claim.started_at,
            emission_state="emitted",
            emitted_at=now,
        )
        db.add(emission)
        await db.commit()

    # SQLite has no server-side observer functions; all immutable evidence is real.
    async def observe(db, observed):
        assert observed == event
        return attestation.id

    service = RegisteredWorkerEventReceiptService(factory, delivery_observer=observe)
    return SimpleNamespace(
        factory=factory,
        event=event,
        service=service,
        node_id=node_id,
        original=original,
        attestation=attestation,
        emission=emission,
        registration=registration,
        grant=grant,
        task=task,
        channel=channel,
    )


async def _claim(db, case, dispatch):
    return await claim_registered_worker_node(
        db,
        job_id=case.event.job_id,
        node_execution_id=case.node_id,
        registration_id=case.registration.id,
        lease_epoch=case.registration.lease_epoch,
        worker_id=case.event.claim.worker_id,
        redis_stream=dispatch.redis_stream,
        consumer_group=dispatch.consumer_group,
        message_id=dispatch.redis_message_id,
        payload_sha256=dispatch.payload_sha256,
        dispatch_key=dispatch.dispatch_key,
    )


@pytest.mark.asyncio
async def test_registered_failure_releases_only_ownership_and_retry_claims_same_message(
    retry_case,
):
    case = retry_case
    receipt_id = await case.service.accept_and_apply(
        case.event, JobEngine().apply_registered_worker_event
    )
    async with case.factory() as db:
        node = await db.get(NodeExecution, case.node_id)
        assert (
            node.worker_id,
            node.worker_registration_id,
            node.worker_lease_epoch,
            node.started_at,
        ) == (None,) * 4
        assert node.status == NodeStatus.QUEUED and node.retry_count == 1
        assert (
            node.queued_at is not None
            and node.progress == 10
            and node.completed_at is None
        )
        retry = (
            await db.scalars(
                select(WorkerTaskDispatch).where(
                    WorkerTaskDispatch.origin_receipt_id == receipt_id
                )
            )
        ).one()
        assert retry.delivery_state == "pending" and retry.delivery_attempted_at is None
        original = await db.get(WorkerTaskDispatch, case.original.id)
        assert original.payload_json == case.original.payload_json
        assert (
            await db.get(WorkerTaskDeliveryAttestation, case.attestation.id) is not None
        )
        now = datetime.now(timezone.utc)
        retry.delivery_state = "delivered"
        retry.delivery_attempted_at = retry.delivered_at = now
        retry.redis_message_id = "1710000002000-0"
        await db.flush()
        claim, attestation_id = await _claim(db, case, retry)
        assert (
            claim.node_execution_id == case.node_id
            and attestation_id != case.attestation.id
        )
        assert node.status == NodeStatus.RUNNING and node.retry_count == 1
        await db.commit()
    assert (
        await case.service.accept_and_apply(
            case.event, JobEngine().apply_registered_worker_event
        )
        == receipt_id
    )
    async with case.factory() as db:
        node = await db.get(NodeExecution, case.node_id)
        assert node.status == NodeStatus.RUNNING and node.started_at is not None
        assert len((await db.scalars(select(WorkerTaskDispatch))).all()) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields",
    [
        ("worker_id",),
        ("worker_registration_id", "worker_lease_epoch"),
        ("started_at",),
        ("worker_id", "worker_registration_id", "worker_lease_epoch", "started_at"),
    ],
)
async def test_sqlite_claim_rejects_each_retained_ownership_binding(retry_case, fields):
    case = retry_case
    async with case.factory() as db:
        node = await db.get(NodeExecution, case.node_id)
        values = {
            name: getattr(node, name)
            for name in (
                "worker_id",
                "worker_registration_id",
                "worker_lease_epoch",
                "started_at",
            )
        }
        node.status = NodeStatus.QUEUED
        for name in values:
            setattr(node, name, values[name] if name in fields else None)
        dispatch = await db.get(WorkerTaskDispatch, case.original.id)
        dispatch.redis_message_id = "1710000003000-0"
        await db.flush()
        with pytest.raises(JobExecutionAuthorityBlocked):
            await _claim(db, case, dispatch)


@pytest.mark.asyncio
async def test_failure_release_and_staging_roll_back_with_receipt(retry_case):
    case = retry_case

    async def fail_after_release(db, receipt, event):
        await JobEngine().apply_registered_worker_event(db, receipt, event)
        node = await db.get(NodeExecution, case.node_id)
        assert node.worker_id is None
        raise RuntimeError("rollback after release")

    with pytest.raises(RuntimeError, match="rollback after release"):
        await case.service.accept_and_apply(case.event, fail_after_release)
    async with case.factory() as db:
        node = await db.get(NodeExecution, case.node_id)
        assert node.status == NodeStatus.RUNNING and node.retry_count == 0
        assert node.worker_id == case.event.claim.worker_id
        assert (await db.scalars(select(RegisteredWorkerEventReceipt))).all() == []
        assert (await db.scalars(select(RegisteredWorkerEventDelivery))).all() == []
        assert len((await db.scalars(select(WorkerTaskDispatch))).all()) == 1
    await case.service.accept_and_apply(
        case.event, JobEngine().apply_registered_worker_event
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "alteration",
    [
        "receipt_applied",
        "receipt_type",
        "receipt_payload",
        "payload_started_at",
        "attestation_hash",
        "emission_hash",
        "emission_payload",
        "emission_message",
        "emission_prepared",
        "delivery_hash",
        "delivery_quarantined",
        "old_dispatch_hash",
        "registration_epoch",
        "registration_expired",
        "grant_stream",
        "channel_halted",
        "channel_disabled",
        "schedule_closed",
        "schedule_other_job",
        "task_cancelled",
        "job_cancelled",
        "later_claim",
        "retry_twice",
        "retry_attempted",
        "retry_origin",
        "retry_stream",
        "retry_payload",
        "retry_error",
        "retry_attestation",
        "competing_dispatch",
    ],
)
async def test_release_rechecks_exact_evidence_after_staging(retry_case, alteration):
    case = retry_case

    class AlteredStage(JobEngine):
        async def _stage_receipt_dispatch(self, db, receipt, job, node, **kwargs):
            await super()._stage_receipt_dispatch(db, receipt, job, node, **kwargs)
            retry = (
                await db.scalars(
                    select(WorkerTaskDispatch).where(
                        WorkerTaskDispatch.origin_receipt_id == receipt.id
                    )
                )
            ).one()
            original = await db.get(WorkerTaskDispatch, case.original.id)
            attestation = await db.get(
                WorkerTaskDeliveryAttestation, case.attestation.id
            )
            emission = await db.get(WorkerEventEmission, case.emission.id)
            delivery = (await db.scalars(select(RegisteredWorkerEventDelivery))).one()
            registration = await db.get(WorkerRegistration, case.registration.id)
            grant = await db.get(WorkerAdmissionGrant, case.grant.id)
            channel = await db.get(ChannelProfile, case.channel.id)
            schedule = await db.get(RuntimeSchedule, "videoprocess")
            task = await db.get(ProductionTask, case.task.id)
            now = datetime.now(timezone.utc)
            if alteration == "receipt_applied":
                receipt.application_state, receipt.applied_at = "applied", now
            elif alteration == "receipt_type":
                receipt.event_type = "node_completed"
            elif alteration == "receipt_payload":
                receipt.payload_json = {**receipt.payload_json, "error": "other"}
            elif alteration == "payload_started_at":
                receipt.payload_json = {
                    **receipt.payload_json,
                    "started_at": now.isoformat(),
                }
                emission.payload_json = receipt.payload_json
            elif alteration == "attestation_hash":
                attestation.payload_sha256 = "e" * 64
            elif alteration == "emission_hash":
                emission.payload_sha256 = "e" * 64
            elif alteration == "emission_payload":
                emission.payload_json = {**emission.payload_json, "error": "other"}
            elif alteration == "emission_message":
                emission.message_id = "1710000004000-0"
            elif alteration == "emission_prepared":
                emission.emission_state, emission.message_id, emission.emitted_at = (
                    "prepared",
                    None,
                    None,
                )
            elif alteration == "delivery_hash":
                delivery.payload_sha256 = "e" * 64
            elif alteration == "delivery_quarantined":
                delivery.resolution_state, delivery.reason_code = (
                    "quarantined",
                    "mismatch",
                )
            elif alteration == "old_dispatch_hash":
                original.payload_sha256 = "e" * 64
            elif alteration == "registration_epoch":
                registration.lease_epoch += 1
            elif alteration == "registration_expired":
                registration.registered_at = now - timedelta(hours=3)
                registration.heartbeat_at = now - timedelta(hours=2)
                registration.lease_expires_at = now - timedelta(hours=1)
            elif alteration == "grant_stream":
                grant.redis_stream = "vp:tasks:other"
            elif alteration == "channel_halted":
                channel.halted_at = now
            elif alteration == "channel_disabled":
                channel.enabled = False
            elif alteration == "schedule_closed":
                schedule.state = "CLOSED"
            elif alteration == "schedule_other_job":
                schedule.guarded_job_id = uuid.uuid4()
            elif alteration == "task_cancelled":
                task.state = "cancelled"
            elif alteration == "job_cancelled":
                job.status = "CANCELLED"
            elif alteration == "later_claim":
                node.worker_id = "later-worker"
            elif alteration == "retry_twice":
                node.retry_count = 2
            elif alteration == "retry_attempted":
                retry.delivery_state, retry.delivery_attempted_at = "attempting", now
            elif alteration == "retry_origin":
                retry.origin_receipt_id = uuid.uuid4()
            elif alteration == "retry_stream":
                retry.redis_stream = "vp:tasks:other"
            elif alteration == "retry_payload":
                retry.payload_json = {
                    **retry.payload_json,
                    "node_execution_id": str(uuid.uuid4()),
                }
                retry.payload_sha256 = canonical_redis_payload_sha256(
                    retry.payload_json
                )
            elif alteration == "retry_error":
                retry.delivery_error = "previously attempted"
            elif alteration == "retry_attestation":
                db.add(
                    WorkerTaskDeliveryAttestation(
                        redis_stream=retry.redis_stream,
                        consumer_group=retry.consumer_group,
                        message_id="1710000005000-0",
                        payload_sha256=retry.payload_sha256,
                        dispatch_key=retry.dispatch_key,
                        job_id=job.id,
                        node_execution_id=node.id,
                        worker_registration_id=registration.id,
                        worker_lease_epoch=registration.lease_epoch,
                        worker_id="later-worker",
                        worker_started_at=now,
                    )
                )
            elif alteration == "competing_dispatch":
                db.add(
                    WorkerTaskDispatch(
                        origin_receipt_id=uuid.uuid4(),
                        dispatch_key=uuid.uuid4(),
                        job_id=job.id,
                        node_execution_id=node.id,
                        redis_stream=retry.redis_stream,
                        consumer_group=retry.consumer_group,
                        payload_sha256="a" * 64,
                        payload_json={},
                    )
                )
            await db.flush()

    with pytest.raises(JobExecutionAuthorityBlocked):
        await case.service.accept_and_apply(
            case.event, AlteredStage().apply_registered_worker_event
        )
    async with case.factory() as db:
        node = await db.get(NodeExecution, case.node_id)
        assert (
            node.status == NodeStatus.RUNNING
            and node.worker_id == case.event.claim.worker_id
        )
        assert (await db.scalars(select(RegisteredWorkerEventReceipt))).all() == []
        assert len((await db.scalars(select(WorkerTaskDispatch))).all()) == 1
