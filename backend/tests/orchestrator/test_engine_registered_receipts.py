from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from app.models.artifact import (
    Artifact,
    ArtifactKind,
    IntermediateArtifactCache,
)
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventReceipt,
    WorkerEventDispatch,
)
from app.orchestrator.engine import JobEngine
from app.services.job_execution_authority import NodeExecutionClaim
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventError,
    RegisteredWorkerEventReceiptService,
    parse_registered_worker_event,
)


@pytest.fixture
async def registered_engine_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'registered-engine.sqlite3'}",
        json_serializer=lambda value: json.dumps(value, default=str),
    )
    async with engine.begin() as connection:
        for table in (
            Job.__table__,
            NodeExecution.__table__,
            Artifact.__table__,
            IntermediateArtifactCache.__table__,
            RegisteredWorkerEventReceipt.__table__,
            WorkerEventDispatch.__table__,
        ):
            await connection.run_sync(table.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


def _pipeline() -> dict:
    return {
        "nodes": [
            {
                "id": "source",
                "type": "source",
                "position": {"x": 0, "y": 0},
                "data": {
                    "label": "Source",
                    "config": {"asset_id": str(uuid.uuid4())},
                },
            },
            {
                "id": "trim",
                "type": "trim",
                "position": {"x": 100, "y": 0},
                "data": {"label": "Trim", "config": {"duration": 5}},
            },
            {
                "id": "encode",
                "type": "transcode",
                "position": {"x": 200, "y": 0},
                "data": {
                    "label": "Encode",
                    "config": {"format": "mp4"},
                },
            },
        ],
        "edges": [
            {
                "id": "source-trim",
                "source": "source",
                "target": "trim",
                "sourceHandle": "output",
                "targetHandle": "input",
            },
            {
                "id": "trim-encode",
                "source": "trim",
                "target": "encode",
                "sourceHandle": "output",
                "targetHandle": "input",
            },
        ],
    }


async def _seed(factory):
    job_id = uuid.uuid4()
    registration_id = uuid.uuid4()
    started_at = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)
    async with factory() as db:
        job = Job(
            id=job_id,
            pipeline_id=uuid.uuid4(),
            pipeline_snapshot=_pipeline(),
            status=JobStatus.RUNNING,
            execution_plan={
                "dependencies": {
                    "source": [],
                    "trim": ["source"],
                    "encode": ["trim"],
                }
            },
        )
        source = NodeExecution(
            id=uuid.uuid4(),
            job_id=job_id,
            node_id="source",
            node_type="source",
            node_label="Source",
            node_config={},
            status=NodeStatus.SUCCEEDED,
            progress=100,
        )
        trim = NodeExecution(
            id=uuid.uuid4(),
            job_id=job_id,
            node_id="trim",
            node_type="trim",
            node_label="Trim",
            node_config={"duration": 5},
            status=NodeStatus.RUNNING,
            progress=10,
            worker_id="vision-worker@127:42:instance",
            started_at=started_at,
            worker_registration_id=registration_id,
            worker_lease_epoch=9,
        )
        encode = NodeExecution(
            id=uuid.uuid4(),
            job_id=job_id,
            node_id="encode",
            node_type="transcode",
            node_label="Encode",
            node_config={"format": "mp4"},
            status=NodeStatus.PENDING,
        )
        db.add_all([job, source, trim, encode])
        await db.flush()
        source_artifact = Artifact(
            id=uuid.uuid4(),
            job_id=job_id,
            node_execution_id=source.id,
            kind=ArtifactKind.INTERMEDIATE,
            filename="source.mp4",
            storage_backend="local",
            storage_path="/tmp/source.mp4",
        )
        output_artifact = Artifact(
            id=uuid.uuid4(),
            job_id=job_id,
            node_execution_id=trim.id,
            kind=ArtifactKind.INTERMEDIATE,
            filename="trim.mp4",
            storage_backend="local",
            storage_path="/tmp/trim.mp4",
        )
        db.add_all([source_artifact, output_artifact])
        await db.flush()
        source.output_artifact_id = source_artifact.id
        trim.input_artifact_ids = [source_artifact.id]
        await db.commit()
    payload = {
        "event": "node_completed",
        "job_id": str(job_id),
        "node_execution_id": str(trim.id),
        "output_artifact_id": str(output_artifact.id),
        "worker_id": trim.worker_id,
        "started_at": started_at.isoformat(),
        "worker_registration_id": str(registration_id),
        "worker_lease_epoch": "9",
        "task_stream": "vp:tasks:vision",
        "task_group": "vision-workers",
        "task_message_id": "1710000000000-4",
    }
    event = parse_registered_worker_event(
        redis_stream="vp:events",
        consumer_group="orchestrator",
        message_id="1710000001000-0",
        payload=payload,
    )
    return trim.id, encode.id, output_artifact.id, event


def _authority(job, node):
    return SimpleNamespace(
        channel=None,
        task=None,
        schedule=SimpleNamespace(state="OPEN", guarded_job_id=job.id),
        job=job,
        node=node,
    )


async def _lock_seeded_authority(db, event):
    job = (
        await db.execute(
            select(Job)
            .where(Job.id == event.job_id)
            .options(selectinload(Job.node_executions))
        )
    ).scalar_one()
    node = next(
        item
        for item in job.node_executions
        if item.id == event.node_execution_id
    )
    return _authority(job, node)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "method_args"),
    [
        ("on_node_completed", (uuid.uuid4(),)),
        ("on_node_failed", ("failed",)),
    ],
)
async def test_legacy_event_entrypoints_reject_registered_worker_claims(
    method_name,
    method_args,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claim = NodeExecutionClaim(
        job_id=job_id,
        node_execution_id=node_execution_id,
        worker_id="vision-worker@127:42:instance",
        started_at=datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc),
        worker_registration_id=uuid.uuid4(),
        worker_lease_epoch=9,
    )

    with pytest.raises(RegisteredWorkerEventError, match="receipt"):
        await getattr(JobEngine(), method_name)(
            job_id,
            node_execution_id,
            *method_args,
            claim=claim,
        )


@pytest.mark.asyncio
async def test_receipt_application_atomically_writes_completion_cache_and_outbox(
    registered_engine_factory,
) -> None:
    trim_id, encode_id, output_artifact_id, event = await _seed(
        registered_engine_factory
    )

    async def lock_authority(db, locked_job_id, **kwargs):
        assert locked_job_id == event.job_id
        return await _lock_seeded_authority(db, event)

    async def observe(db, claim):
        assert db.in_transaction()

    job_engine = JobEngine()
    service = RegisteredWorkerEventReceiptService(
        registered_engine_factory,
        authority_locker=lock_authority,
        lease_observer=observe,
    )
    receipt_id = await service.accept_and_apply(
        event,
        job_engine.apply_registered_worker_event,
    )

    async with registered_engine_factory() as db:
        trim = await db.get(NodeExecution, trim_id)
        encode = await db.get(NodeExecution, encode_id)
        receipt = await db.get(RegisteredWorkerEventReceipt, receipt_id)
        dispatches = list(
            (await db.execute(select(WorkerEventDispatch))).scalars()
        )
        cache_entries = list(
            (await db.execute(select(IntermediateArtifactCache))).scalars()
        )
    assert trim is not None and trim.status == NodeStatus.SUCCEEDED
    assert trim.output_artifact_id == output_artifact_id
    assert encode is not None and encode.status == NodeStatus.QUEUED
    assert receipt is not None and receipt.application_state == "applied"
    assert len(cache_entries) == 1
    assert len(dispatches) == 1
    assert dispatches[0].receipt_id == receipt_id
    assert dispatches[0].node_execution_id == encode_id
    assert dispatches[0].delivery_state == "pending"


@pytest.mark.asyncio
async def test_receipt_application_rollback_leaves_no_completion_derived_write(
    registered_engine_factory,
) -> None:
    trim_id, encode_id, _artifact_id, event = await _seed(
        registered_engine_factory
    )

    async def lock_authority(db, locked_job_id, **kwargs):
        return await _lock_seeded_authority(db, event)

    async def observe(db, claim):
        return None

    engine = JobEngine()

    async def apply_then_fail(db, receipt, accepted_event):
        await engine.apply_registered_worker_event(
            db,
            receipt,
            accepted_event,
        )
        raise RuntimeError("failure before receipt commit")

    service = RegisteredWorkerEventReceiptService(
        registered_engine_factory,
        authority_locker=lock_authority,
        lease_observer=observe,
    )
    with pytest.raises(RuntimeError, match="before receipt commit"):
        await service.accept_and_apply(event, apply_then_fail)

    async with registered_engine_factory() as db:
        trim = await db.get(NodeExecution, trim_id)
        encode = await db.get(NodeExecution, encode_id)
        receipts = list(
            (
                await db.execute(
                    select(RegisteredWorkerEventReceipt)
                )
            ).scalars()
        )
        dispatches = list(
            (await db.execute(select(WorkerEventDispatch))).scalars()
        )
        cache_entries = list(
            (await db.execute(select(IntermediateArtifactCache))).scalars()
        )
    assert trim is not None and trim.status == NodeStatus.RUNNING
    assert encode is not None and encode.status == NodeStatus.PENDING
    assert receipts == []
    assert dispatches == []
    assert cache_entries == []
