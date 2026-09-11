"""Explicit scratch-PG qualification; no Redis server or upload transport is used."""

from __future__ import annotations

import secrets
import uuid
from datetime import timezone
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.models.job import NodeStatus
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerTaskDispatch,
)
from app.orchestrator.engine import JobEngine
from app.services.job_execution_authority import (
    claim_registered_worker_node,
    lock_job_execution_authority,
    mark_worker_event_emitted,
    prepare_worker_event_emission,
)
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventReceiptService,
    canonical_redis_payload_sha256,
    parse_registered_worker_event,
    stage_worker_task_dispatch,
)
from app.services.registered_worker_retry import release_registered_retry_claim
from app.services.worker_control_role_cli import (
    ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS,
    ORCHESTRATOR_ENTITY_COLUMNS,
    ORCHESTRATOR_INSERT_COLUMNS,
    ORCHESTRATOR_UPDATE_COLUMNS,
    ROLE_FUNCTIONS,
)
from app.services.worker_role_cli_common import (
    create_login_role,
    ensure_stable_role,
    grant_columns,
    grant_functions,
    reset_public_privileges,
)
from tests.worker.ack_drill_postgres import (
    ack_drill_database as _ack_drill_database,
    ack_drill_runtime as _ack_drill_runtime,
    asyncpg_dsn,
)
from tests.worker.test_youtube_upload_handler import media_paths  # noqa: F401

ack_drill_database = _ack_drill_database
ack_drill_runtime = _ack_drill_runtime


@pytest.fixture
async def retry_pg(ack_drill_database, ack_drill_runtime):
    database, worker = ack_drill_database, ack_drill_runtime
    role, stable = (
        f"vp_retry_orch_{uuid.uuid4().hex}",
        "vp_orchestrator_control_runtime",
    )
    password = secrets.token_hex(24)
    url = database.owner_url.set(username=role, password=password)
    created = False
    engine = connection = None
    try:
        async with worker.owner.transaction():
            assert not await worker.owner.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", stable
            )
            await ensure_stable_role(
                worker.owner,
                stable,
                setting_prefix="retry_test",
                authorized_members=(role,),
            )
            await reset_public_privileges(worker.owner, stable)
            await grant_functions(worker.owner, stable, ROLE_FUNCTIONS["orchestrator"])
            for privilege, mapping in (
                ("SELECT", ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS),
                ("SELECT", ORCHESTRATOR_ENTITY_COLUMNS),
                ("INSERT", ORCHESTRATOR_INSERT_COLUMNS),
                ("UPDATE", ORCHESTRATOR_UPDATE_COLUMNS),
            ):
                for table, columns in mapping.items():
                    await grant_columns(worker.owner, stable, privilege, table, columns)
            await create_login_role(
                worker.owner,
                role,
                password,
                setting_prefix="retry_test",
                stable_role=stable,
            )
            await worker.owner.execute(
                f'GRANT CONNECT ON DATABASE "{url.database}" TO "{role}"'
            )
        created = True
        connection = await asyncpg.connect(asyncpg_dsn(url), timeout=10)
        engine = create_async_engine(url, poolclass=NullPool)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        claim = worker.context.execution_claim
        payload = {
            "event": "node_failed",
            "error": "bounded test failure",
            "job_id": str(worker.job_id),
            "node_execution_id": str(worker.node_id),
            "worker_id": claim.worker_id,
            "started_at": claim.started_at.isoformat(),
            "worker_registration_id": str(claim.worker_registration_id),
            "worker_lease_epoch": str(claim.worker_lease_epoch),
            "task_stream": "vp:tasks:youtube_publisher",
            "task_group": "youtube_publisher-workers",
            "task_message_id": worker.message_id,
            "task_payload_sha256": worker.payload_sha256,
            "task_dispatch_key": str(worker.dispatch_key),
        }
        event = parse_registered_worker_event(
            redis_stream="vp:events",
            consumer_group="orchestrator",
            message_id=f"1711000000000-{secrets.randbits(63)}",
            payload=payload,
        )
        async with worker.sessions() as db:
            emission_id = await prepare_worker_event_emission(
                db,
                claim,
                attestation_id=worker.attestation_id,
                redis_stream=event.redis_stream,
                consumer_group=event.consumer_group,
                payload_sha256=event.payload_sha256,
                payload=payload,
                event_type="node_failed",
            )
            await mark_worker_event_emitted(
                db, claim, emission_id=emission_id, message_id=event.message_id
            )
            await db.commit()
        yield SimpleNamespace(
            worker=worker,
            connection=connection,
            sessions=sessions,
            event=event,
            service=RegisteredWorkerEventReceiptService(sessions),
            role=role,
        )
    finally:
        if connection is not None:
            await connection.close()
        if engine is not None:
            await engine.dispose()
        if created:
            await worker.owner.execute(f'DROP OWNED BY "{role}", "{stable}"')
            await worker.owner.execute(f'DROP ROLE "{role}"')
            await worker.owner.execute(f'DROP ROLE "{stable}"')


async def _node(db, node_id):
    return dict(
        (
            await db.execute(
                text("SELECT * FROM public.node_executions WHERE id=:id"),
                {"id": node_id},
            )
        )
        .mappings()
        .one()
    )


@pytest.mark.asyncio
async def test_restricted_retry_release_only_four_fields_then_same_message_claim(
    retry_pg,
):
    case, stages = retry_pg, []

    class CapturedStage(JobEngine):
        async def _stage_receipt_dispatch(self, db, receipt, job, node, **kwargs):
            await super()._stage_receipt_dispatch(db, receipt, job, node, **kwargs)
            stages.append(await _node(db, node.id))

    async def apply(db, receipt, event):
        await CapturedStage().apply_registered_worker_event(db, receipt, event)
        after = await _node(db, case.worker.node_id)
        expected = {
            **stages[0],
            **dict.fromkeys(
                (
                    "worker_id",
                    "worker_registration_id",
                    "worker_lease_epoch",
                    "started_at",
                )
            ),
        }
        assert after == expected
        with pytest.raises(DBAPIError, match="retry_claim_mismatch"):
            async with db.begin_nested():
                await release_registered_retry_claim(db, receipt.id)

    for column in (
        "worker_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "started_at",
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await case.connection.execute(
                f"UPDATE public.node_executions SET {column}=NULL WHERE id=$1",
                case.worker.node_id,
            )
        assert not await case.worker.owner.fetchval(
            "SELECT has_column_privilege($1,'public.node_executions',$2,'UPDATE')",
            case.role,
            column,
        )
    assert not await case.worker.owner.fetchval(
        "SELECT has_function_privilege($1,'public.vp_release_registered_retry_claim(uuid)','EXECUTE')",
        case.worker.role,
    )
    receipt_id = await case.service.accept_and_apply(case.event, apply)
    with pytest.raises(asyncpg.RaiseError, match="retry_receipt_mismatch"):
        await case.connection.fetchval(
            "SELECT public.vp_release_registered_retry_claim($1)", receipt_id
        )

    class Redis:
        def __init__(self):
            self.evals = 0
            self.message = f"1712000000000-{secrets.randbits(63)}"

        async def eval(self, *args):
            self.evals += 1
            return self.message

        async def xack(self, *args):
            return 1

    redis = Redis()
    await case.service.acknowledge_applied(redis, case.event)
    await case.service.deliver_pending_dispatches(redis)
    async with case.sessions() as db:
        retry = (
            await db.scalars(
                select(WorkerTaskDispatch).where(
                    WorkerTaskDispatch.origin_receipt_id == receipt_id
                )
            )
        ).one()
    assert redis.evals == 1 and retry.redis_message_id == redis.message
    await case.worker.refresh(minimum_margin_seconds=1)
    async with case.worker.sessions() as db:
        claim, attestation_id = await claim_registered_worker_node(
            db,
            job_id=case.worker.job_id,
            node_execution_id=case.worker.node_id,
            registration_id=case.worker.lease.registration_id,
            lease_epoch=case.worker.lease.lease_epoch,
            worker_id=case.event.claim.worker_id,
            redis_stream=retry.redis_stream,
            consumer_group=retry.consumer_group,
            message_id=redis.message,
            payload_sha256=retry.payload_sha256,
            dispatch_key=retry.dispatch_key,
        )
        await db.commit()
    assert (
        attestation_id != case.worker.attestation_id
        and claim.started_at != case.event.claim.started_at
    )
    with pytest.raises(asyncpg.RaiseError, match="retry_claim_mismatch"):
        await case.connection.fetchval(
            "SELECT public.vp_release_registered_retry_claim($1)", receipt_id
        )
    assert await case.service.accept_and_apply(case.event, apply) == receipt_id
    assert len(stages) == 1 and redis.evals == 1


@pytest.mark.asyncio
async def test_restricted_retry_release_rolls_back_with_failed_receipt_transaction(
    retry_pg,
):
    case = retry_pg

    async def fail(db, receipt, event):
        await JobEngine().apply_registered_worker_event(db, receipt, event)
        assert (await _node(db, case.worker.node_id))["worker_id"] is None
        raise RuntimeError("rollback released retry")

    with pytest.raises(RuntimeError, match="rollback released retry"):
        await case.service.accept_and_apply(case.event, fail)
    row = await case.worker.owner.fetchrow(
        "SELECT * FROM public.node_executions WHERE id=$1", case.worker.node_id
    )
    assert row["status"] == "RUNNING" and row["retry_count"] == 0
    assert (
        row["worker_id"] == case.event.claim.worker_id
        and row["started_at"] == case.event.claim.started_at
    )
    assert (
        await case.worker.owner.fetchval(
            "SELECT count(*) FROM public.registered_worker_event_receipts WHERE node_execution_id=$1",
            case.worker.node_id,
        )
        == 0
    )
    assert (
        await case.worker.owner.fetchval(
            "SELECT count(*) FROM public.worker_task_dispatches WHERE node_execution_id=$1",
            case.worker.node_id,
        )
        == 1
    )
    assert await case.service.accept_and_apply(
        case.event, JobEngine().apply_registered_worker_event
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "alteration",
    [
        "receipt_message",
        "receipt_payload",
        "receipt_applied",
        "receipt_type",
        "retry_attempted",
        "missing_retry",
        "retry_payload",
        "missing_delivery",
        "later_claim",
        "closed_schedule",
        "other_guard",
    ],
)
async def test_sql_primitive_independently_rejects_wrong_provenance(
    retry_pg, alteration
):
    case = retry_pg
    if alteration == "later_claim":
        await case.worker.owner.execute(
            "UPDATE public.node_executions SET worker_id='later-worker' WHERE id=$1",
            case.worker.node_id,
        )
    if alteration == "closed_schedule":
        await case.worker.owner.execute(
            "UPDATE public.runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'"
        )
    if alteration == "other_guard":
        other_job = await case.worker.owner.fetchval(
            "INSERT INTO public.jobs(pipeline_id,pipeline_snapshot,status) "
            "SELECT pipeline_id,pipeline_snapshot,'RUNNING' FROM public.jobs WHERE id=$1 RETURNING id",
            case.worker.job_id,
        )
        await case.worker.owner.execute(
            "UPDATE public.runtime_schedules SET guarded_job_id=$1 WHERE service_name='videoprocess'",
            other_job,
        )
    try:
        async with case.sessions() as db, db.begin():
            db_now = await db.scalar(text("SELECT CURRENT_TIMESTAMP"))
            authority = await lock_job_execution_authority(
                db, case.worker.job_id, node_execution_id=case.worker.node_id
            )
            facts = case.event.receipt_facts(
                source_task_attestation_id=case.worker.attestation_id
            )
            if alteration == "receipt_message":
                facts["message_id"] = "1713000000000-0"
            if alteration == "receipt_payload":
                facts["payload_json"] = {**facts["payload_json"], "error": "other"}
                facts["payload_sha256"] = canonical_redis_payload_sha256(
                    facts["payload_json"]
                )
            if alteration == "receipt_type":
                facts["event_type"] = "node_completed"
            receipt = RegisteredWorkerEventReceipt(**facts)
            if alteration == "receipt_applied":
                receipt.application_state, receipt.applied_at = (
                    "applied",
                    db_now,
                )
            db.add(receipt)
            await db.flush()
            if alteration != "missing_delivery":
                db.add(
                    RegisteredWorkerEventDelivery(
                        receipt_id=receipt.id,
                        source_task_attestation_id=case.worker.attestation_id,
                        redis_stream=receipt.redis_stream,
                        consumer_group=receipt.consumer_group,
                        message_id=receipt.message_id,
                        payload_sha256=receipt.payload_sha256,
                        resolution_state="accepted",
                    )
                )
            node = authority.node
            node.status, node.retry_count, node.queued_at = (
                NodeStatus.QUEUED,
                1,
                db_now.astimezone(timezone.utc).replace(tzinfo=None),
            )
            if alteration != "missing_retry":
                retry = await stage_worker_task_dispatch(
                    db,
                    origin_receipt_id=receipt.id,
                    job_id=case.worker.job_id,
                    node_execution_id=case.worker.node_id,
                    redis_stream=case.event.source_task_stream,
                    consumer_group=case.event.source_task_group,
                    payload={
                        "job_id": str(case.worker.job_id),
                        "node_execution_id": str(
                            uuid.uuid4()
                            if alteration == "retry_payload"
                            else case.worker.node_id
                        ),
                    },
                )
                if alteration == "retry_attempted":
                    retry.delivery_state, retry.delivery_attempted_at = (
                        "attempting",
                        db_now,
                    )
            await db.flush()
            # Catch only the primitive's server error, never an earlier fixture/ACL failure.
            with pytest.raises(DBAPIError, match="retry_|schedule_authority_changed"):
                async with db.begin_nested():
                    await release_registered_retry_claim(db, receipt.id)
            await db.rollback()
    finally:
        if alteration in {"closed_schedule", "other_guard"}:
            await case.worker.owner.execute(
                "UPDATE public.runtime_schedules SET state='OPEN',guarded_job_id=NULL WHERE service_name='videoprocess'"
            )
