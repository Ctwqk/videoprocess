from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerTaskDeliveryAttestation,
    WorkerTaskDispatch,
)
from app.services.job_service import delete_job


@pytest.fixture
async def deletion_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'job-delete.sqlite3'}",
        json_serializer=lambda value: json.dumps(value, default=str),
    )
    async with engine.begin() as connection:
        for table in (
            Job.__table__,
            NodeExecution.__table__,
            WorkerTaskDispatch.__table__,
            WorkerTaskDeliveryAttestation.__table__,
            RegisteredWorkerEventReceipt.__table__,
            RegisteredWorkerEventDelivery.__table__,
        ):
            await connection.run_sync(table.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


async def _seed_authority(
    factory,
    *,
    source_acknowledged: bool,
    event_acknowledged: bool,
    dispatch_delivered: bool = True,
) -> tuple[uuid.UUID, tuple[uuid.UUID, ...]]:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    registration_id = uuid.uuid4()
    attestation_id = uuid.uuid4()
    receipt_id = uuid.uuid4()
    delivery_id = uuid.uuid4()
    dispatch_id = uuid.uuid4()
    dispatch_key = uuid.uuid4()
    now = datetime.now(timezone.utc)
    async with factory() as db:
        db.add(
            Job(
                id=job_id,
                pipeline_id=uuid.uuid4(),
                pipeline_snapshot={"nodes": [], "edges": []},
                status=JobStatus.SUCCEEDED,
            )
        )
        db.add(
            NodeExecution(
                id=node_execution_id,
                job_id=job_id,
                node_id="vision-1",
                node_type="vision",
                node_label="Vision",
                node_config={},
                status=NodeStatus.SUCCEEDED,
            )
        )
        db.add(
            WorkerTaskDispatch(
                id=dispatch_id,
                origin_receipt_id=None,
                dispatch_key=dispatch_key,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload_sha256="1" * 64,
                payload_json={"dispatch_key": str(dispatch_key)},
                delivery_state=(
                    "delivered" if dispatch_delivered else "pending"
                ),
                redis_message_id=(
                    "1710000000000-4" if dispatch_delivered else None
                ),
                delivery_attempted_at=now if dispatch_delivered else None,
                delivered_at=now if dispatch_delivered else None,
                resolution_state=(
                    "acknowledged"
                    if source_acknowledged
                    else "unresolved"
                ),
                acknowledged_at=now if source_acknowledged else None,
            )
        )
        db.add(
            WorkerTaskDeliveryAttestation(
                id=attestation_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                message_id="1710000000000-4",
                payload_sha256="1" * 64,
                dispatch_key=dispatch_key,
                job_id=job_id,
                node_execution_id=node_execution_id,
                worker_registration_id=registration_id,
                worker_lease_epoch=9,
                worker_id="vision-worker@127:1:instance",
                worker_started_at=now,
                ack_state=(
                    "acknowledged" if source_acknowledged else "pending"
                ),
                acknowledged_at=now if source_acknowledged else None,
            )
        )
        db.add(
            RegisteredWorkerEventReceipt(
                id=receipt_id,
                source_task_attestation_id=attestation_id,
                redis_stream="vp:events",
                consumer_group="orchestrator",
                message_id="1710000001000-0",
                payload_sha256="2" * 64,
                payload_json={},
                event_type="node_completed",
                job_id=job_id,
                node_execution_id=node_execution_id,
                worker_registration_id=registration_id,
                worker_lease_epoch=9,
                worker_id="vision-worker@127:1:instance",
                worker_started_at=now,
                source_task_stream="vp:tasks:vision",
                source_task_group="vision-workers",
                source_task_message_id="1710000000000-4",
                application_state="applied",
                ack_state=(
                    "acknowledged" if event_acknowledged else "pending"
                ),
                source_task_ack_state=(
                    "acknowledged" if source_acknowledged else "pending"
                ),
                applied_at=now,
                acknowledged_at=now if event_acknowledged else None,
                source_task_acknowledged_at=(
                    now if source_acknowledged else None
                ),
            )
        )
        db.add(
            RegisteredWorkerEventDelivery(
                id=delivery_id,
                source_task_attestation_id=attestation_id,
                receipt_id=receipt_id,
                redis_stream="vp:events",
                consumer_group="orchestrator",
                message_id="1710000001000-0",
                payload_sha256="2" * 64,
                resolution_state="accepted",
                reason_code=None,
                ack_state=(
                    "acknowledged" if event_acknowledged else "pending"
                ),
                acknowledged_at=now if event_acknowledged else None,
            )
        )
        await db.commit()
    return job_id, (
        dispatch_id,
        attestation_id,
        receipt_id,
        delivery_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_acknowledged", "event_acknowledged"),
    [(False, False), (True, False), (False, True)],
)
async def test_delete_holds_until_source_and_event_acknowledgements(
    deletion_factory,
    source_acknowledged,
    event_acknowledged,
) -> None:
    job_id, _ = await _seed_authority(
        deletion_factory,
        source_acknowledged=source_acknowledged,
        event_acknowledged=event_acknowledged,
    )

    async with deletion_factory() as db:
        with pytest.raises(ValueError, match="authority.*unresolved"):
            await delete_job(db, job_id)
        assert await db.get(Job, job_id) is not None


@pytest.mark.asyncio
async def test_delete_cleans_resolved_worker_authority_before_job(
    deletion_factory,
) -> None:
    job_id, authority_ids = await _seed_authority(
        deletion_factory,
        source_acknowledged=True,
        event_acknowledged=True,
    )

    async with deletion_factory() as db:
        assert await delete_job(db, job_id) is True
        assert await db.get(Job, job_id) is None
        model_ids = zip(
            (
                WorkerTaskDispatch,
                WorkerTaskDeliveryAttestation,
                RegisteredWorkerEventReceipt,
                RegisteredWorkerEventDelivery,
            ),
            authority_ids,
            strict=True,
        )
        for model, authority_id in model_ids:
            assert await db.get(model, authority_id) is None


@pytest.mark.asyncio
async def test_delete_cancelled_claim_with_acknowledged_task_needs_no_receipt(
    deletion_factory,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    attestation_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    async with deletion_factory() as db:
        db.add(
            Job(
                id=job_id,
                pipeline_id=uuid.uuid4(),
                pipeline_snapshot={"nodes": [], "edges": []},
                status=JobStatus.CANCELLED,
            )
        )
        db.add(
            NodeExecution(
                id=node_execution_id,
                job_id=job_id,
                node_id="vision-1",
                node_type="vision",
                node_label="Vision",
                node_config={},
                status=NodeStatus.CANCELLED,
            )
        )
        dispatch_key = uuid.uuid4()
        db.add(
            WorkerTaskDispatch(
                dispatch_key=dispatch_key,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload_sha256="3" * 64,
                payload_json={"dispatch_key": str(dispatch_key)},
                delivery_state="delivered",
                delivery_attempted_at=now,
                redis_message_id="1710000002000-0",
                delivered_at=now,
                resolution_state="acknowledged",
                acknowledged_at=now,
            )
        )
        db.add(
            WorkerTaskDeliveryAttestation(
                id=attestation_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                message_id="1710000002000-0",
                payload_sha256="3" * 64,
                dispatch_key=dispatch_key,
                job_id=job_id,
                node_execution_id=node_execution_id,
                worker_registration_id=uuid.uuid4(),
                worker_lease_epoch=10,
                worker_id="vision-worker@127:2:instance",
                worker_started_at=now,
                ack_state="acknowledged",
                acknowledged_at=now,
            )
        )
        await db.commit()

    async with deletion_factory() as db:
        assert await delete_job(db, job_id) is True
        assert await db.get(Job, job_id) is None
        assert (
            await db.get(
                WorkerTaskDeliveryAttestation,
                attestation_id,
            )
            is None
        )


@pytest.mark.asyncio
async def test_delete_rejects_delivered_dispatch_without_exact_ack(
    deletion_factory,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    dispatch_key = uuid.uuid4()
    now = datetime.now(timezone.utc)
    async with deletion_factory() as db:
        db.add(
            Job(
                id=job_id,
                pipeline_id=uuid.uuid4(),
                pipeline_snapshot={"nodes": [], "edges": []},
                status=JobStatus.CANCELLED,
            )
        )
        db.add(
            NodeExecution(
                id=node_execution_id,
                job_id=job_id,
                node_id="vision-1",
                node_type="vision",
                node_label="Vision",
                node_config={},
                status=NodeStatus.CANCELLED,
            )
        )
        db.add(
            WorkerTaskDispatch(
                dispatch_key=dispatch_key,
                job_id=job_id,
                node_execution_id=node_execution_id,
                redis_stream="vp:tasks:vision",
                consumer_group="vision-workers",
                payload_sha256="4" * 64,
                payload_json={"dispatch_key": str(dispatch_key)},
                delivery_state="delivered",
                delivery_attempted_at=now,
                redis_message_id="1710000003000-0",
                delivered_at=now,
                resolution_state="unresolved",
            )
        )
        await db.commit()

    async with deletion_factory() as db:
        with pytest.raises(ValueError, match="authority.*unresolved"):
            await delete_job(db, job_id)
        assert await db.get(Job, job_id) is not None
