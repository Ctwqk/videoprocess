from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import CheckConstraint, event, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.artifact import Artifact
from app.models.channel_agent import ChannelProfile, ProductionTask, PublicationRecord
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.models.schedule import RuntimeSchedule
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.services import youtube_upload_operations as upload_operations
from app.services.job_execution_authority import (
    JobExecutionAuthorityBlocked,
    NodeExecutionClaim,
)
from app.services.youtube_upload_operations import (
    UploadOperationConflictError,
    UploadOperationContext,
    YouTubeUploadOperationStore,
)


MANAGER_TASK_ID = "a0b1c2d3-e4f5-4678-9abc-def012345678"
SECOND_MANAGER_TASK_ID = "12345678-90ab-4cde-8f01-23456789abcd"
INVALID_MANAGER_TASK_ID_CASES = (
    pytest.param(None, id="null"),
    pytest.param("", id="empty"),
    pytest.param("   ", id="spaces"),
    pytest.param("\t", id="tab"),
    pytest.param("\n", id="newline"),
    pytest.param("\t\n", id="control-whitespace"),
    pytest.param("manager-task-1", id="legacy-placeholder"),
    pytest.param("a0b1c2d3e4f546789abcdef012345678", id="compact"),
    pytest.param("{a0b1c2d3-e4f5-4678-9abc-def012345678}", id="braced"),
    pytest.param("A0B1C2D3-E4F5-4678-9ABC-DEF012345678", id="uppercase"),
    pytest.param("g0b1c2d3-e4f5-4678-9abc-def012345678", id="non-hex"),
    pytest.param("a0b1c2d3-e4f5-4678-9abc-def01234567-", id="extra-hyphen"),
    pytest.param(f"{MANAGER_TASK_ID}\n", id="valid-plus-newline"),
)


@pytest.fixture
async def operation_session_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'upload-operations.sqlite3'}",
        connect_args={"timeout": 10},
    )
    async with engine.begin() as conn:
        for table in (
            Job.__table__,
            NodeExecution.__table__,
            Artifact.__table__,
            ChannelProfile.__table__,
            ProductionTask.__table__,
            RuntimeSchedule.__table__,
            YouTubeUploadOperation.__table__,
            OwnedSeedInventory.__table__,
        ):
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    yield session_factory
    await engine.dispose()


async def _context_for(
    db: AsyncSession,
    *,
    production_task: ProductionTask | None = None,
    registered: bool = False,
) -> UploadOperationContext:
    claimed_at = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)
    worker_id = "test-worker@localhost:1"
    job = Job(
        pipeline_id=uuid.uuid4(),
        pipeline_snapshot={},
        status=JobStatus.RUNNING,
    )
    db.add(job)
    await db.flush()

    node = NodeExecution(
        job_id=job.id,
        node_id=f"youtube_upload_{uuid.uuid4().hex}",
        node_type="youtube_upload",
        status=NodeStatus.RUNNING,
        worker_id=worker_id,
        started_at=claimed_at,
        worker_registration_id=uuid.uuid4() if registered else None,
        worker_lease_epoch=11 if registered else None,
    )
    db.add(node)
    await db.flush()

    artifact = Artifact(
        job_id=job.id,
        node_execution_id=node.id,
        filename="canary.mp4",
        storage_path="artifacts/canary.mp4",
    )
    db.add(artifact)
    await db.flush()

    if production_task is None:
        channel = ChannelProfile(name=f"canary-{uuid.uuid4()}")
        db.add(channel)
        await db.flush()
        production_task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=uuid.uuid4(),
            prompt="Upload the owned canary video",
            job_id=job.id,
            state="producing",
        )
        db.add(production_task)
    else:
        production_task.job_id = job.id
    await db.commit()

    return UploadOperationContext(
        job_id=job.id,
        node_execution_id=node.id,
        execution_claim=NodeExecutionClaim(
            job_id=job.id,
            node_execution_id=node.id,
            worker_id=worker_id,
            started_at=claimed_at,
            worker_registration_id=node.worker_registration_id,
            worker_lease_epoch=node.worker_lease_epoch,
        ),
        input_artifact_id=artifact.id,
        content_sha256="a" * 64,
        title="Owned canary",
        privacy="unlisted",
    )


@pytest.fixture
async def submitted_operation(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    reserved = await store.claim(context)
    await store.mark_attempting(reserved.operation.id, context=context)
    submitted = await store.mark_submitted(
        reserved.operation.id, MANAGER_TASK_ID, context=context,
    )
    return store, context, submitted


@asynccontextmanager
async def _observe_read_only(session_factory):
    tables = (
        Job.__table__, NodeExecution.__table__, Artifact.__table__,
        ChannelProfile.__table__, ProductionTask.__table__,
        RuntimeSchedule.__table__, YouTubeUploadOperation.__table__,
    )

    async def snapshot():
        async with session_factory() as db:
            return [list((await db.execute(select(table))).mappings()) for table in tables]

    before = await snapshot()
    statements = []

    def record_sql(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    engine = session_factory.kw["bind"].sync_engine
    event.listen(engine, "before_cursor_execute", record_sql)
    try:
        yield
    finally:
        event.remove(engine, "before_cursor_execute", record_sql)
    assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)
    assert await snapshot() == before


@pytest.mark.asyncio
async def test_load_submitted_reads_fresh_detached_operation_without_writes(
    operation_session_factory, submitted_operation,
):
    store, context, submitted = submitted_operation
    reads = []
    for _ in range(2):
        async with _observe_read_only(operation_session_factory):
            loaded = await store.load_submitted(
                context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
            )
        assert loaded.action == "resume"
        assert loaded.operation.id == submitted.id
        assert loaded.operation.status == "submitted"
        assert loaded.operation.manager_task_id == MANAGER_TASK_ID
        assert loaded.operation.request_attempted_at is not None
        assert loaded.operation.receipt_json == {}
        assert loaded.operation.platform_video_id is None
        assert loaded.operation.completed_at is None
        assert inspect(loaded.operation).detached
        reads.append(loaded.operation)
    assert reads[0] is not reads[1]

    await store.mark_succeeded(
        submitted.id, "abcdefghijk", {"video_id": "abcdefghijk"}, context=context,
    )
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(ValueError):
            await store.load_submitted(
                context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.asyncio
async def test_load_submitted_rechecks_operation_after_authority_observation(
    monkeypatch, operation_session_factory, submitted_operation,
):
    store, context, submitted = submitted_operation
    production_task_id = store._production_task_id

    async def complete_during_read(db, job_id):
        task_id = await production_task_id(db, job_id)
        await store.mark_succeeded(
            submitted.id, "abcdefghijk", {"video_id": "abcdefghijk"}, context=context,
        )
        return task_id

    # A real transition commits through an independent session during the read.
    monkeypatch.setattr(store, "_production_task_id", complete_during_read)
    with pytest.raises(ValueError):
        await store.load_submitted(
            context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
        )
    async with operation_session_factory() as db:
        stored = await db.get(YouTubeUploadOperation, submitted.id)
        assert stored.status == "succeeded"
        assert stored.receipt_json["video_id"] == "abcdefghijk"


@pytest.mark.parametrize("change", ["unlinked", "reassigned", "replaced"])
@pytest.mark.asyncio
async def test_load_submitted_rechecks_task_link_after_registered_authority(
    monkeypatch, operation_session_factory, change,
):
    async def require_lease(db, claim):
        pass

    monkeypatch.setattr(upload_operations, "require_worker_registration_lease", require_lease)
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db, registered=True)
    reserved = await store.claim(context)
    await store.mark_attempting(reserved.operation.id, context=context)
    submitted = await store.mark_submitted(reserved.operation.id, MANAGER_TASK_ID, context=context)

    async with AsyncExitStack() as observations:
        async def change_link_during_authority(db, claim):
            async with operation_session_factory() as writer:
                task = await writer.get(ProductionTask, submitted.production_task_id)
                task.job_id = None
                if change == "reassigned":
                    other_job = Job(
                        pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.RUNNING,
                    )
                    writer.add(other_job)
                    await writer.flush()
                    task.job_id = other_job.id
                elif change == "replaced":
                    writer.add(ProductionTask(
                        channel_profile_id=task.channel_profile_id,
                        target_account_id=task.target_account_id,
                        prompt="Replacement task", job_id=context.job_id, state="producing",
                    ))
                await writer.commit()
            # Exclude the deliberate independent writer, then observe the reader.
            await observations.enter_async_context(_observe_read_only(operation_session_factory))

        # Exercise registered-branch ordering with real SQLite rows, not PostgreSQL authority.
        monkeypatch.setattr(upload_operations, "_session_is_postgresql", lambda db: True)
        monkeypatch.setattr(
            upload_operations, "require_registered_worker_node_claim", change_link_during_authority,
        )
        with pytest.raises(JobExecutionAuthorityBlocked, match="production task changed"):
            await store.load_submitted(
                context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.parametrize("with_existing_operation", [False, True])
@pytest.mark.asyncio
async def test_load_submitted_missing_operation_never_reserves(
    operation_session_factory, with_existing_operation,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    if with_existing_operation:
        await store.claim(context)
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(LookupError):
            await store.load_submitted(
                context, operation_id=uuid.uuid4(), manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("job_id", uuid.UUID(int=101)),
        ("node_execution_id", uuid.UUID(int=102)),
        ("input_artifact_id", uuid.UUID(int=103)),
        ("content_sha256", "b" * 64),
        ("title", "Different title"),
        ("privacy", "private"),
    ],
)
@pytest.mark.asyncio
async def test_load_submitted_rejects_context_mismatch(
    operation_session_factory, submitted_operation, field, value,
):
    store, context, submitted = submitted_operation
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(JobExecutionAuthorityBlocked):
            await store.load_submitted(
                replace(context, **{field: value}),
                operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.parametrize("manager_task_id", [SECOND_MANAGER_TASK_ID, *INVALID_MANAGER_TASK_ID_CASES])
@pytest.mark.asyncio
async def test_load_submitted_requires_exact_canonical_manager_id(
    operation_session_factory, submitted_operation, manager_task_id,
):
    store, context, submitted = submitted_operation
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(ValueError):
            await store.load_submitted(
                context, operation_id=submitted.id, manager_task_id=manager_task_id,
            )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "reserved"),
        ("status", "uncertain"),
        ("status", "failed"),
        ("status", "succeeded"),
        ("request_attempted_at", None),
        ("receipt_json", {"video_id": "abcdefghijk"}),
        ("receipt_json", None),
        ("receipt_json", []),
        ("platform_video_id", "abcdefghijk"),
        ("platform_video_id", ""),
        ("completed_at", datetime(2026, 9, 9, tzinfo=timezone.utc)),
        ("manager_task_id", SECOND_MANAGER_TASK_ID),
        ("manager_task_id", MANAGER_TASK_ID.upper()),
        ("manager_task_id", None),
    ],
)
@pytest.mark.asyncio
async def test_load_submitted_rejects_nonresumable_durable_state(
    operation_session_factory, submitted_operation, field, value,
):
    store, context, submitted = submitted_operation
    async with operation_session_factory() as db:
        # Deliberately corrupt only negative fixtures, including legacy Manager IDs.
        await db.execute(text("PRAGMA ignore_check_constraints = ON"))
        operation = await db.get(YouTubeUploadOperation, submitted.id)
        setattr(operation, field, value)
        await db.commit()
        await db.execute(text("PRAGMA ignore_check_constraints = OFF"))
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(ValueError):
            await store.load_submitted(
                context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.parametrize("attempted", [False, True])
@pytest.mark.asyncio
async def test_load_submitted_rejects_unsubmitted_reservations(
    operation_session_factory, attempted,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    reserved = await store.claim(context)
    if attempted:
        await store.mark_attempting(reserved.operation.id, context=context)
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(ValueError):
            await store.load_submitted(
                context, operation_id=reserved.operation.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.parametrize("change", ["unlinked", "wrong-task", "missing-link", "multiple-tasks"])
@pytest.mark.asyncio
async def test_load_submitted_requires_same_linked_production_task(
    operation_session_factory, submitted_operation, change,
):
    store, context, submitted = submitted_operation
    async with operation_session_factory() as db:
        task = await db.get(ProductionTask, submitted.production_task_id)
        operation = await db.get(YouTubeUploadOperation, submitted.id)
        if change == "unlinked":
            task.job_id = None
        elif change == "wrong-task":
            operation.production_task_id = uuid.uuid4()
        elif change == "missing-link":
            operation.production_task_id = None
        else:
            db.add(ProductionTask(
                channel_profile_id=task.channel_profile_id,
                target_account_id=task.target_account_id,
                prompt="Conflicting task", job_id=context.job_id, state="producing",
            ))
        await db.commit()
    async with _observe_read_only(operation_session_factory):
        with pytest.raises((JobExecutionAuthorityBlocked, UploadOperationConflictError)):
            await store.load_submitted(
                context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.parametrize(
    ("target", "field", "value"),
    [
        ("job", "status", JobStatus.CANCELLED),
        ("job", "status", JobStatus.SUCCEEDED),
        ("node", "status", NodeStatus.FAILED),
        ("node", "job_id", uuid.UUID("bbbbbbbb-0000-4000-8000-000000000104")),
        ("node", "worker_id", "replacement-worker"),
        ("node", "started_at", datetime(2026, 9, 9, tzinfo=timezone.utc)),
        ("node", "worker_registration_id", uuid.UUID("bbbbbbbb-0000-4000-8000-000000000105")),
        ("node", "worker_lease_epoch", 12),
        ("task", "state", "failed"),
        ("channel", "enabled", False),
        ("channel", "halted_at", datetime(2026, 9, 9, tzinfo=timezone.utc)),
        ("schedule", "state", "CLOSED"),
        ("schedule", "guarded_job_id", uuid.UUID(int=106)),
        ("schedule", None, None),
    ],
)
@pytest.mark.asyncio
async def test_load_submitted_rechecks_durable_execution_authority(
    operation_session_factory, submitted_operation, target, field, value,
):
    store, context, submitted = submitted_operation
    async with operation_session_factory() as db:
        task = await db.get(ProductionTask, submitted.production_task_id)
        model, identity = {
            "job": (Job, context.job_id),
            "node": (NodeExecution, context.node_execution_id),
            "task": (ProductionTask, task.id),
            "channel": (ChannelProfile, task.channel_profile_id),
            "schedule": (RuntimeSchedule, "videoprocess"),
        }[target]
        row = await db.get(model, identity)
        if field is None:
            await db.delete(row)
        else:
            # Negative fixtures may intentionally violate the paired lease binding.
            await db.execute(text("PRAGMA ignore_check_constraints = ON"))
            setattr(row, field, value)
        await db.commit()
        await db.execute(text("PRAGMA ignore_check_constraints = OFF"))
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(JobExecutionAuthorityBlocked):
            await store.load_submitted(
                context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.asyncio
async def test_load_submitted_does_not_trust_active_submission_fence(
    operation_session_factory, submitted_operation,
):
    store, context, submitted = submitted_operation
    async with store.submission_fence(context):
        # Release SQLite's write lock while retaining the active fence context.
        await store._active_submission_fence.get().db.commit()
        async with operation_session_factory() as db:
            node = await db.get(NodeExecution, context.node_execution_id)
            node.worker_id = "replacement-worker"
            await db.commit()
        async with _observe_read_only(operation_session_factory):
            with pytest.raises(JobExecutionAuthorityBlocked):
                await store.load_submitted(
                    context, operation_id=submitted.id, manager_task_id=MANAGER_TASK_ID,
                )


@pytest.mark.asyncio
async def test_load_submitted_propagates_registration_lease_loss(
    monkeypatch, operation_session_factory,
):
    async def require_lease(db, claim):
        pass

    monkeypatch.setattr(upload_operations, "require_worker_registration_lease", require_lease)
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db, registered=True)
    reserved = await store.claim(context)
    await store.mark_attempting(reserved.operation.id, context=context)
    await store.mark_submitted(reserved.operation.id, MANAGER_TASK_ID, context=context)

    async def lost_lease(db, claim):
        raise JobExecutionAuthorityBlocked("worker registration lease is no longer authoritative")

    # SQLite cannot execute PostgreSQL's lease function; only its rejection is simulated.
    monkeypatch.setattr(upload_operations, "require_worker_registration_lease", lost_lease)
    async with _observe_read_only(operation_session_factory):
        with pytest.raises(JobExecutionAuthorityBlocked, match="registration lease"):
            await store.load_submitted(
                context, operation_id=reserved.operation.id, manager_task_id=MANAGER_TASK_ID,
            )


@pytest.mark.asyncio
async def test_submission_fence_rejects_reassigned_execution_claim(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    claimed_at = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)
    context = SimpleNamespace(
        job_id=job_id,
        node_execution_id=node_execution_id,
        content_sha256="a" * 64,
        execution_claim=SimpleNamespace(
            job_id=job_id,
            node_execution_id=node_execution_id,
            worker_id="gpu-worker@150:old",
            started_at=claimed_at,
        ),
    )
    entered: list[str] = []

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

        async def begin(self):
            return FakeTransaction()

        def in_transaction(self):
            return False

    def session_factory():
        return FakeSession()

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        assert locked_job_id == job_id
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(state="OPEN", guarded_job_id=job_id),
            task=None,
            job=SimpleNamespace(id=job_id, status=JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=NodeStatus.RUNNING,
                worker_id="gpu-worker@150:replacement",
                started_at=claimed_at + timedelta(minutes=11),
            ),
        )

    monkeypatch.setattr(
        upload_operations,
        "lock_job_execution_authority",
        lock_authority,
    )
    store = YouTubeUploadOperationStore(session_factory)

    async def task_id(_db, _job_id):
        return None

    async def producer(_db, _task_id, **_kwargs):
        return None

    from app.services import owned_producer_fence
    monkeypatch.setattr(store, "_production_task_id", task_id)
    monkeypatch.setattr(owned_producer_fence, "lock_producer", producer)

    with pytest.raises(JobExecutionAuthorityBlocked, match="claim"):
        async with store.submission_fence(context):
            entered.append("posted")

    assert entered == []


async def test_local_final_fence_rechecks_after_committed_attempt(operation_session_factory, monkeypatch):
    from app.services import owned_producer_fence
    from app.services.owned_seed_inventory import OwnedInventoryError
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)
    original = owned_producer_fence.lock_producer
    calls = []

    async def guarded(db, task_id, **kwargs):
        calls.append(task_id)
        if len(calls) == 3:
            raise OwnedInventoryError("owned_inventory_producer_inactive")
        return await original(db, task_id, **kwargs)

    monkeypatch.setattr(owned_producer_fence, "lock_producer", guarded)
    with pytest.raises(OwnedInventoryError, match="producer_inactive"):
        async with store.submission_fence(context):
            await store.mark_attempting(claim.operation.id, context=context)
    assert len(calls) == 3
    async with operation_session_factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation.status == "reserved" and operation.request_attempted_at is not None
        assert operation.manager_task_id is None


@pytest.mark.asyncio
async def test_registered_submission_fence_uses_database_150_second_margin(
    monkeypatch,
) -> None:
    job_id = uuid.uuid4()
    node_execution_id = uuid.uuid4()
    registration_id = uuid.uuid4()
    claimed_at = datetime(2026, 7, 22, 12, 0, tzinfo=timezone.utc)
    context = SimpleNamespace(
        job_id=job_id,
        node_execution_id=node_execution_id,
        execution_claim=NodeExecutionClaim(
            job_id=job_id,
            node_execution_id=node_execution_id,
            worker_id="gpu-worker@150:registered",
            started_at=claimed_at,
            worker_registration_id=registration_id,
            worker_lease_epoch=19,
        ),
    )
    margin_checks: list[tuple[NodeExecutionClaim, int]] = []
    claim_checks: list[NodeExecutionClaim] = []

    class FakeSession:
        def __init__(self):
            self.active = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, traceback):
            return False

        async def begin(self):
            self.active = True

        def in_transaction(self):
            return self.active

        async def rollback(self):
            self.active = False

    async def lock_authority(_db, locked_job_id, *, node_execution_id):
        return SimpleNamespace(
            channel=None,
            schedule=SimpleNamespace(
                state="OPEN",
                guarded_job_id=job_id,
            ),
            task=None,
            job=SimpleNamespace(id=job_id, status=JobStatus.RUNNING),
            node=SimpleNamespace(
                id=node_execution_id,
                status=NodeStatus.RUNNING,
                worker_id=context.execution_claim.worker_id,
                started_at=claimed_at,
                worker_registration_id=registration_id,
                worker_lease_epoch=19,
            ),
        )

    async def require_margin(
        _db,
        claim,
        *,
        minimum_margin_seconds,
    ):
        margin_checks.append((claim, minimum_margin_seconds))

    async def require_claim(_db, claim):
        claim_checks.append(claim)

    async def reject_plain_lease(*args, **kwargs):
        raise AssertionError(
            "submission entry must use the database margin function"
        )

    monkeypatch.setattr(
        upload_operations,
        "lock_job_execution_authority",
        lock_authority,
    )
    monkeypatch.setattr(
        upload_operations,
        "require_worker_registration_margin",
        require_margin,
        raising=False,
    )
    monkeypatch.setattr(
        upload_operations,
        "require_registered_worker_node_claim",
        require_claim,
        raising=False,
    )
    monkeypatch.setattr(
        upload_operations,
        "require_worker_registration_lease",
        reject_plain_lease,
    )
    store = YouTubeUploadOperationStore(lambda: FakeSession())

    async with store.submission_fence(context):
        assert claim_checks == [context.execution_claim]
        assert margin_checks == [(context.execution_claim, 150)]


@pytest.mark.asyncio
async def test_registered_operation_transitions_require_context_and_lease_fence(
    monkeypatch,
    operation_session_factory,
) -> None:
    lease_checks: list[NodeExecutionClaim] = []

    async def require_lease(_db, claim):
        lease_checks.append(claim)

    monkeypatch.setattr(
        upload_operations,
        "require_worker_registration_lease",
        require_lease,
        raising=False,
    )
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db, registered=True)

    claimed = await store.claim(context)
    with pytest.raises(JobExecutionAuthorityBlocked, match="context"):
        await store.mark_attempting(claimed.operation.id)

    attempting = await store.mark_attempting(
        claimed.operation.id,
        context=context,
    )
    submitted = await store.mark_submitted(
        attempting.id,
        MANAGER_TASK_ID,
        context=context,
    )
    succeeded = await store.mark_succeeded(
        submitted.id,
        "abcdefghijk",
        {"video_id": "abcdefghijk"},
        context=context,
    )
    async with operation_session_factory() as db:
        failed_context = await _context_for(db, registered=True)
    failed_claim = await store.claim(failed_context)
    failed = await store.mark_failed(
        failed_claim.operation.id,
        "manager rejected upload",
        context=failed_context,
    )
    async with operation_session_factory() as db:
        uncertain_context = await _context_for(db, registered=True)
    uncertain_claim = await store.claim(uncertain_context)
    await store.mark_attempting(
        uncertain_claim.operation.id,
        context=uncertain_context,
    )
    uncertain = await store.mark_uncertain(
        uncertain_claim.operation.id,
        "submission outcome is unknown",
        context=uncertain_context,
    )

    assert succeeded.status == "succeeded"
    assert failed.status == "failed"
    assert uncertain.status == "uncertain"
    checked_ids = {
        check.worker_registration_id
        for check in lease_checks
    }
    assert checked_ids == {
        context.execution_claim.worker_registration_id,
        failed_context.execution_claim.worker_registration_id,
        uncertain_context.execution_claim.worker_registration_id,
    }


@pytest.mark.asyncio
async def test_claim_reserves_once_then_resumes_and_replays(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)

    claim = await store.claim(context)
    assert claim.action == "submit"
    assert claim.operation.status == "reserved"

    again = await store.claim(context)
    assert again.action == "submit"
    assert again.operation.id == claim.operation.id

    attempting = await store.mark_attempting(claim.operation.id)
    assert attempting.request_attempted_at is not None
    assert (await store.claim(context)).action == "block"

    submitted = await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)
    assert submitted.manager_task_id == MANAGER_TASK_ID
    assert (await store.claim(context)).action == "resume"

    receipt = {
        "video_id": "abcdefghijk",
        "url": "https://youtu.be/abcdefghijk",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["canary"],
        "quota_estimate": 1600,
        "access_token": "must-not-persist",
    }
    succeeded = await store.mark_succeeded(claim.operation.id, "abcdefghijk", receipt)
    assert succeeded.receipt_json == {
        "video_id": "abcdefghijk",
        "url": "https://youtu.be/abcdefghijk",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["canary"],
        "quota_estimate": 1600,
    }
    assert (await store.claim(context)).action == "replay"


@pytest.mark.asyncio
async def test_replacement_claim_can_take_over_unattempted_reservation(
    operation_session_factory,
) -> None:
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)

    reserved = await store.claim(context)
    replacement_started_at = context.execution_claim.started_at + timedelta(minutes=1)
    replacement_claim = replace(
        context.execution_claim,
        worker_id="test-worker@replacement:2",
        started_at=replacement_started_at,
    )
    replacement_context = replace(context, execution_claim=replacement_claim)
    async with operation_session_factory() as db:
        node = await db.get(NodeExecution, context.node_execution_id)
        assert node is not None
        node.worker_id = replacement_claim.worker_id
        node.started_at = replacement_started_at
        await db.commit()

    with pytest.raises(JobExecutionAuthorityBlocked, match="claim changed"):
        await store.claim(context)

    replacement = await store.claim(replacement_context)
    assert replacement.action == "submit"
    assert replacement.operation.id == reserved.operation.id


@pytest.mark.asyncio
async def test_replacement_claim_blocks_after_submission_was_attempted(
    operation_session_factory,
) -> None:
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)

    reserved = await store.claim(context)
    await store.mark_attempting(reserved.operation.id)
    replacement_started_at = context.execution_claim.started_at + timedelta(minutes=1)
    replacement_claim = replace(
        context.execution_claim,
        worker_id="test-worker@replacement:2",
        started_at=replacement_started_at,
    )
    replacement_context = replace(context, execution_claim=replacement_claim)
    async with operation_session_factory() as db:
        node = await db.get(NodeExecution, context.node_execution_id)
        assert node is not None
        node.worker_id = replacement_claim.worker_id
        node.started_at = replacement_started_at
        await db.commit()

    replacement = await store.claim(replacement_context)

    assert replacement.action == "block"
    assert replacement.operation.id == reserved.operation.id


@pytest.mark.asyncio
async def test_existing_reserved_uncertain_and_failed_operations_never_submit(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        reserved_context = await _context_for(db)

    reserved = await store.claim(reserved_context)
    await store.mark_attempting(reserved.operation.id)
    assert (await store.claim(reserved_context)).action == "block"

    await store.mark_uncertain(reserved.operation.id, "upload response was ambiguous")
    assert (await store.claim(reserved_context)).action == "block"

    async with operation_session_factory() as db:
        failed_context = await _context_for(db)
    failed = await store.claim(failed_context)
    await store.mark_failed(failed.operation.id, "manager rejected upload")
    assert (await store.claim(failed_context)).action == "block"


@pytest.mark.asyncio
async def test_claim_rejects_second_node_for_the_same_production_task(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        production_task = await db.get(ProductionTask, claim.operation.production_task_id)
        assert production_task is not None
        second_context = await _context_for(db, production_task=production_task)

    with pytest.raises(
        UploadOperationConflictError,
        match="production task already has a YouTube upload operation",
    ):
        await store.claim(second_context)


@pytest.mark.asyncio
async def test_mark_succeeded_rejects_duplicate_platform_video_id(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        first_context = await _context_for(db)
    async with operation_session_factory() as db:
        second_context = await _context_for(db)
    first = await store.claim(first_context)
    second = await store.claim(second_context)

    await store.mark_submitted(first.operation.id, MANAGER_TASK_ID)
    await store.mark_succeeded(first.operation.id, "abcdefghijk", {"video_id": "abcdefghijk"})
    await store.mark_submitted(second.operation.id, SECOND_MANAGER_TASK_ID)

    with pytest.raises(
        UploadOperationConflictError,
        match="platform video id already belongs to a YouTube upload operation",
    ):
        await store.mark_succeeded(second.operation.id, "abcdefghijk", {"video_id": "abcdefghijk"})


@pytest.mark.asyncio
async def test_competing_successes_cannot_replace_the_winning_receipt(
    operation_session_factory,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)
    await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)

    start_barrier = asyncio.Barrier(2)

    async def transition(platform_video_id: str, title: str):
        await start_barrier.wait()
        return await store.mark_succeeded(
            claim.operation.id,
            platform_video_id,
            {"video_id": platform_video_id, "title": title},
        )

    results = await asyncio.gather(
        transition("video-win-a", "receipt-a"),
        transition("video-win-b", "receipt-b"),
        return_exceptions=True,
    )

    successes = [result for result in results if isinstance(result, YouTubeUploadOperation)]
    conflicts = [result for result in results if isinstance(result, UploadOperationConflictError)]
    assert len(successes) == 1
    assert len(conflicts) == 1
    async with operation_session_factory() as db:
        stored = await db.get(YouTubeUploadOperation, claim.operation.id)
    assert stored is not None
    assert stored.platform_video_id == successes[0].platform_video_id
    assert stored.receipt_json == successes[0].receipt_json


@pytest.mark.asyncio
async def test_terminal_transition_does_not_replace_an_existing_failure(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    failed = await store.mark_failed(claim.operation.id, "first conclusive failure")
    repeated = await store.mark_failed(claim.operation.id, "later failure must not replace evidence")
    assert repeated.status == "failed"
    assert repeated.error_message == failed.error_message == "first conclusive failure"
    with pytest.raises(ValueError, match="cannot mark failed operation uncertain"):
        await store.mark_uncertain(claim.operation.id, "ambiguous after failure")


def test_operation_model_requires_manager_task_for_submitted_and_succeeded_states():
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in YouTubeUploadOperation.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    manager_check = checks["ck_youtube_upload_operations_manager_task"]
    assert manager_check.startswith("(manager_task_id IS NULL OR (")
    assert (
        ")) AND (status NOT IN ('submitted', 'succeeded') OR manager_task_id IS NOT NULL)"
        in manager_check
    )
    assert "length(manager_task_id) = 36" in manager_check
    assert "manager_task_id = lower(manager_task_id)" in manager_check
    assert "length(replace(manager_task_id, '-', '')) = 32" in manager_check
    for position in (9, 14, 19, 24):
        assert f"substr(manager_task_id, {position}, 1) = '-'" in manager_check
    for character in "0123456789abcdef":
        assert f", '{character}', '')" in manager_check


@pytest.mark.parametrize(
    "manager_task_id",
    INVALID_MANAGER_TASK_ID_CASES,
)
@pytest.mark.asyncio
async def test_database_rejects_submitted_operation_without_canonical_manager_uuid(
    operation_session_factory,
    manager_task_id,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation is not None
        operation.status = "submitted"
        operation.manager_task_id = manager_task_id
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()


@pytest.mark.parametrize(
    "manager_task_id",
    [
        pytest.param("\t", id="tab"),
        pytest.param("\n", id="newline"),
        pytest.param("manager-task-1", id="malformed"),
    ],
)
@pytest.mark.asyncio
async def test_database_rejects_noncanonical_manager_uuid_before_submission(
    operation_session_factory,
    manager_task_id,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation is not None
        operation.manager_task_id = manager_task_id
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()


@pytest.mark.asyncio
async def test_database_accepts_submitted_operation_with_canonical_manager_uuid(
    operation_session_factory,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation is not None
        operation.status = "submitted"
        operation.manager_task_id = MANAGER_TASK_ID
        await db.commit()

    async with operation_session_factory() as db:
        stored = await db.get(YouTubeUploadOperation, claim.operation.id)
    assert stored is not None
    assert stored.manager_task_id == MANAGER_TASK_ID


@pytest.mark.parametrize("manager_task_id", INVALID_MANAGER_TASK_ID_CASES)
@pytest.mark.asyncio
async def test_mark_submitted_rejects_noncanonical_manager_task(
    operation_session_factory,
    manager_task_id,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    with pytest.raises(ValueError, match="manager task id"):
        await store.mark_submitted(claim.operation.id, manager_task_id)


@pytest.mark.parametrize("status", ["submitted", "succeeded"])
@pytest.mark.parametrize("manager_task_id", INVALID_MANAGER_TASK_ID_CASES)
def test_noncanonical_manager_durable_state_fails_closed(status, manager_task_id):
    operation = YouTubeUploadOperation(status=status, manager_task_id=manager_task_id)
    assert YouTubeUploadOperationStore._action_for(operation) == "block"


@pytest.mark.parametrize(
    ("quota_estimate", "expected"),
    [
        pytest.param({"units": 1600}, None, id="nested-object"),
        pytest.param([1600], None, id="nested-array"),
        pytest.param(float("nan"), None, id="nan"),
        pytest.param(float("inf"), None, id="infinity"),
        pytest.param(True, None, id="boolean"),
        pytest.param("1600", 1600.0, id="numeric-string"),
        pytest.param(1600, 1600, id="integer"),
        pytest.param(1600.5, 1600.5, id="float"),
    ],
)
@pytest.mark.asyncio
async def test_receipt_quota_estimate_is_a_finite_numeric_scalar_or_none(
    operation_session_factory,
    quota_estimate,
    expected,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)
    await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)

    succeeded = await store.mark_succeeded(
        claim.operation.id,
        "abcdefghijk",
        {"video_id": "abcdefghijk", "quota_estimate": quota_estimate},
    )
    assert succeeded.receipt_json["quota_estimate"] == expected


@pytest.mark.asyncio
async def test_receipt_never_stringifies_nested_or_non_string_values(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)
    await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)

    secrets = {
        "RECEIPT_VIDEO_TOKEN",
        "RECEIPT_URL_TOKEN",
        "RECEIPT_VIDEO_URL_TOKEN",
        "RECEIPT_TITLE_TOKEN",
        "RECEIPT_PRIVACY_TOKEN",
        "RECEIPT_TAG_DICT_TOKEN",
        "RECEIPT_TAG_LIST_TOKEN",
        "RECEIPT_QUOTA_TOKEN",
        "RECEIPT_IGNORED_TOKEN",
    }
    succeeded = await store.mark_succeeded(
        claim.operation.id,
        "safe-video-id",
        {
            "video_id": {"access_token": "RECEIPT_VIDEO_TOKEN"},
            "url": {"access_token": "RECEIPT_URL_TOKEN"},
            "video_url": [{"refresh_token": "RECEIPT_VIDEO_URL_TOKEN"}],
            "title": {"token": "RECEIPT_TITLE_TOKEN"},
            "privacy": {"token": "RECEIPT_PRIVACY_TOKEN"},
            "tags": [
                "safe-tag",
                {"access_token": "RECEIPT_TAG_DICT_TOKEN"},
                ["RECEIPT_TAG_LIST_TOKEN"],
                True,
                1600,
            ],
            "quota_estimate": {"access_token": "RECEIPT_QUOTA_TOKEN"},
            "ignored": {"access_token": "RECEIPT_IGNORED_TOKEN"},
        },
    )

    assert succeeded.receipt_json == {
        "video_id": "safe-video-id",
        "url": "",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["safe-tag"],
        "quota_estimate": None,
    }
    serialized = json.dumps(succeeded.receipt_json, sort_keys=True)
    assert all(secret not in serialized for secret in secrets)


def test_publication_record_orm_has_migration_unique_indexes():
    indexes = {index.name: index for index in PublicationRecord.__table__.indexes}

    production_task = indexes["ux_publication_records_production_task"]
    assert production_task.unique is True
    assert [column.name for column in production_task.columns] == ["production_task_id"]

    platform_content = indexes["ux_publication_records_platform_content"]
    assert platform_content.unique is True
    assert [column.name for column in platform_content.columns] == ["platform", "platform_content_id"]


def test_alembic_upgrade_head_renders_offline_postgresql_sql():
    backend_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["DATABASE_URL"] = "postgresql+asyncpg://offline:offline@127.0.0.1:5432/offline"
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=backend_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "DO $$" in completed.stdout
    assert "RAISE EXCEPTION 'cannot add ux_publication_records_production_task" in completed.stdout
    assert "RAISE EXCEPTION 'cannot add ux_publication_records_platform_content" in completed.stdout

    widening = "ALTER TABLE alembic_version ALTER COLUMN version_num TYPE VARCHAR(128)"
    revision_update = (
        "UPDATE alembic_version SET version_num='020_channelops_decision_audit_failure_category'"
    )
    assert widening in completed.stdout
    assert completed.stdout.index(widening) < completed.stdout.index(revision_update)
    assert "CONSTRAINT ck_youtube_upload_operations_manager_task CHECK" in completed.stdout
    assert "(manager_task_id IS NULL OR (" in completed.stdout
    assert (
        ")) AND (status NOT IN ('submitted', 'succeeded') OR manager_task_id IS NOT NULL)"
        in completed.stdout
    )
    assert "length(manager_task_id) = 36" in completed.stdout
    assert "manager_task_id = lower(manager_task_id)" in completed.stdout
    assert "length(replace(manager_task_id, '-', '')) = 32" in completed.stdout
    for position in (9, 14, 19, 24):
        assert f"substr(manager_task_id, {position}, 1) = '-'" in completed.stdout
    for character in "0123456789abcdef":
        assert f", '{character}', '')" in completed.stdout

    assert "ADD COLUMN human_review_evidence_json JSON" in completed.stdout
    assert "ALTER COLUMN human_review_evidence_json DROP DEFAULT" not in completed.stdout
    assert "UPDATE channel_ops_queue_items AS q" in completed.stdout
    assert "authoritative_channel_id" in completed.stdout
    assert "queue_authority_unresolved" in completed.stdout
    assert "ADD COLUMN approved_revision_hash VARCHAR(64)" in completed.stdout
    assert "ADD COLUMN execute_idempotency_key VARCHAR(512)" in completed.stdout
    assert "uq_autoflow_runs_execute_idempotency_key" in completed.stdout
