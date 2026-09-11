"""Explicit scratch-PG qualification; no Redis server or upload transport is used."""

from __future__ import annotations

import secrets
import uuid
from datetime import timedelta, timezone
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.artifact import Artifact, ArtifactKind, IntermediateArtifactCache
from app.models.registered_worker_event_receipt import (
    RegisteredWorkerEventDelivery,
    RegisteredWorkerEventReceipt,
    WorkerTaskDispatch,
)
from app.orchestrator.engine import JobEngine
from app.orchestrator.registered_db import RegisteredDatabase, RegisteredDatabaseError
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
    role_names_for_generation,
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

ack_drill_runtime = _ack_drill_runtime


@pytest.fixture
def ack_drill_database():
    # The global dispatcher must see only this case's durable work.
    yield from _ack_drill_database.__wrapped__()


@pytest.fixture
async def retry_pg(ack_drill_database, ack_drill_runtime, tmp_path, monkeypatch, request):
    database, worker = ack_drill_database, ack_drill_runtime
    generation = f"c-{uuid.uuid4().hex[:20]}"
    role = role_names_for_generation(generation).versioned["orchestrator"]
    stable = "vp_orchestrator_control_runtime"
    password = secrets.token_hex(24)
    url = database.owner_url.set(username=role, password=password)
    created = False
    runtime, connection = RegisteredDatabase(), None
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
        path = tmp_path / "orchestrator-url"
        path.write_text(url.render_as_string(hide_password=False))
        path.chmod(0o400)
        monkeypatch.setenv("WORKER_ORCHESTRATOR_DATABASE_URL_FILE", str(path))
        monkeypatch.setenv("WORKER_ORCHESTRATOR_CONTROL_GENERATION", generation)
        await runtime.start(database.owner_url.render_as_string(hide_password=False))
        sessions = runtime.session
        claim = worker.context.execution_claim
        event_type = "node_completed" if getattr(request, "param", "") == "completion" else "node_failed"
        payload = {
            "event": event_type,
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
        if event_type == "node_completed":
            payload.pop("error")
            payload["output_artifact_id"] = str(worker.artifact_id)
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
                event_type=event_type,
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
            runtime=runtime,
            target=database.owner_url.render_as_string(hide_password=False),
        )
    finally:
        if connection is not None:
            await connection.close()
        await runtime.close()
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
@pytest.mark.parametrize("fault", ["superuser", "ownership_update", "authority_select", "missing_update", "missing_rpc"])
async def test_actual_factory_rejects_unsafe_or_incomplete_pg_grants(retry_pg, fault):
    case = retry_pg
    await case.runtime.close()
    statements = {
        "superuser": f'ALTER ROLE "{case.role}" SUPERUSER',
        "ownership_update": 'GRANT UPDATE(started_at) ON public.node_executions TO vp_orchestrator_control_runtime',
        "authority_select": 'GRANT SELECT(id) ON public.worker_registrations TO vp_orchestrator_control_runtime',
        "missing_update": 'REVOKE UPDATE(status) ON public.node_executions FROM vp_orchestrator_control_runtime',
        "missing_rpc": 'REVOKE EXECUTE ON FUNCTION public.vp_release_registered_retry_claim(uuid) FROM vp_orchestrator_control_runtime',
    }
    await case.worker.owner.execute(statements[fault])
    with pytest.raises(RegisteredDatabaseError):
        await case.runtime.start(case.target)
    assert not case.runtime.ready


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_pg", ["completion"], indirect=True)
@pytest.mark.parametrize("mode", ["dispatch", "hit", "invalid", "rollback", "owned"])
async def test_actual_factory_registered_completion_cache_and_finalization(retry_pg, mode):
    from app.orchestrator.artifact_cache import IntermediateArtifactCacheService
    from app.services.registered_worker_event_receipt import RegisteredWorkerEventError

    case = retry_pg
    cache = IntermediateArtifactCacheService()
    async with case.worker.owner_sessions() as db:
        job = await db.get(Job, case.worker.job_id)
        node = await db.get(NodeExecution, case.worker.node_id)
        node.node_type, node.node_config, node.input_artifact_ids = "url_download", {"url": f"https://fixture.invalid/video/{job.id}"}, []
        downstream = NodeExecution(
            job_id=job.id, node_id="encode", node_type="transcode", node_config={"format": "mp4"},
            status=NodeStatus.PENDING, worker_id="unrelated-claim" if mode == "owned" else None,
        )
        db.add(downstream)
        job.pipeline_snapshot = {
            "nodes": [
                {"id": "publish", "type": "url_download", "position": {"x": 0, "y": 0}, "data": {"config": node.node_config}},
                {"id": "encode", "type": "transcode", "position": {"x": 100, "y": 0}, "data": {"config": downstream.node_config}},
            ],
            "edges": [{"id": "e", "source": "publish", "target": "encode", "sourceHandle": "output", "targetHandle": "input"}],
        }
        job.execution_plan = {"dependencies": {"publish": [], "encode": ["publish"]}}
        await db.flush()
        entry_id = None
        if mode in {"hit", "invalid", "rollback"}:
            source = await db.get(Artifact, case.worker.artifact_id)
            entry = IntermediateArtifactCache(
                cache_key=cache.cache_key("transcode", downstream.node_config, {"input": source}),
                node_type="transcode", node_config_hash="fixture", input_signature_hash="fixture",
                output_artifact_id=None, storage_backend="local", storage_path="fixture/cached.mp4",
                filename=None if mode == "invalid" else "cached.mp4", media_info={},
            )
            db.add(entry)
            await db.flush()
            entry_id = entry.id
        downstream_id = downstream.id
        await db.commit()

    async def apply(db, receipt, event):
        assert await db.scalar(text("SELECT session_user")) == case.role
        await JobEngine().apply_registered_worker_event(db, receipt, event)
        if mode == "rollback":
            raise RuntimeError("rollback complete closure")

    if mode in {"rollback", "owned"}:
        error = RuntimeError if mode == "rollback" else RegisteredWorkerEventError
        with pytest.raises(error):
            await case.service.accept_and_apply(case.event, apply)
    else:
        assert await case.service.accept_and_apply(case.event, apply)
    async with case.sessions() as db:
        node = await db.get(NodeExecution, case.worker.node_id)
        downstream = await db.get(NodeExecution, downstream_id)
        job = await db.get(Job, case.worker.job_id)
        dispatches = (await db.scalars(select(WorkerTaskDispatch).where(
            WorkerTaskDispatch.job_id == case.worker.job_id,
            WorkerTaskDispatch.origin_receipt_id.is_not(None),
        ))).all()
        receipts = (await db.scalars(select(RegisteredWorkerEventReceipt).where(
            RegisteredWorkerEventReceipt.job_id == case.worker.job_id,
        ))).all()
        if mode in {"rollback", "owned"}:
            assert node.status == NodeStatus.RUNNING and downstream.status == NodeStatus.PENDING
            assert not dispatches and not receipts
        else:
            assert node.status == NodeStatus.SUCCEEDED
            assert len(receipts) == 1 and receipts[0].application_state == "applied"
            stored = (await db.scalars(select(IntermediateArtifactCache).where(
                IntermediateArtifactCache.cache_key == cache.cache_key("url_download", node.node_config, {}),
            ))).one()
            assert stored.output_artifact_id == case.worker.artifact_id
            if mode == "hit":
                assert job.status == JobStatus.SUCCEEDED and downstream.status == NodeStatus.SUCCEEDED
                assert downstream.worker_id is downstream.worker_registration_id is downstream.worker_lease_epoch is downstream.started_at is None
                assert (await db.get(Artifact, downstream.output_artifact_id)).kind == ArtifactKind.FINAL
                assert not dispatches
            else:
                assert downstream.status == NodeStatus.QUEUED and len(dispatches) == 1
        if entry_id:
            entry = await db.get(IntermediateArtifactCache, entry_id)
            assert entry is not None and entry.hit_count == (1 if mode == "hit" else 0)
            assert entry.last_used_at.utcoffset() == timedelta(0)


@pytest.mark.asyncio
async def test_actual_factory_terminal_registered_failure_updates_only_permitted_fields(retry_pg):
    case = retry_pg
    await case.worker.owner.execute("UPDATE public.node_executions SET retry_count=1 WHERE id=$1", case.worker.node_id)
    receipt_id = await case.service.accept_and_apply(case.event, JobEngine().apply_registered_worker_event)
    async with case.sessions() as db:
        row = await _node(db, case.worker.node_id)
        assert row["status"] == "FAILED" and row["retry_count"] == 1
        assert row["worker_id"] == case.event.claim.worker_id
        assert (await db.get(Job, case.worker.job_id)).status == JobStatus.FAILED
        assert (await db.get(RegisteredWorkerEventReceipt, receipt_id)).application_state == "applied"


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
