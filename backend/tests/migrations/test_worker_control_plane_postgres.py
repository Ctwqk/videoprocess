from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.job import JobStatus, NodeStatus
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventReceiptService,
    parse_registered_worker_event,
)


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND_ROOT = Path(__file__).resolve().parents[2]
RELEASE_COMMIT = "0123456789abcdef0123456789abcdef01234567"
IMAGE_IDENTITY = "vp-python-worker:deploy-0123456789ab"
ENDPOINT_BINDINGS = {
    "database": {
        "database": "videoprocess",
        "driver": "postgresql",
        "host": "vp-postgres-swarm",
        "port": 5432,
    },
    "redis": {
        "database": 3,
        "host": "vp-redis-swarm",
        "port": 6379,
        "scheme": "redis",
    },
    "storage": {"backend": "not_applicable"},
}


def _database_url(database: str) -> str:
    return f"{POSTGRES_URL.rsplit('/', 1)[0]}/{database}"


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _run_alembic(database_url: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live migration tests",
)
async def test_control_plane_observer_is_separate_and_fences_takeover_and_revoke() -> None:
    database = f"vp_worker_observer_{uuid.uuid4().hex}"
    worker_role = f"vp_worker_{uuid.uuid4().hex[:16]}"
    operator_role = f"vp_operator_{uuid.uuid4().hex[:16]}"
    orchestrator_role = f"vp_orchestrator_{uuid.uuid4().hex[:16]}"
    worker_password = uuid.uuid4().hex
    operator_password = uuid.uuid4().hex
    orchestrator_password = uuid.uuid4().hex
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
        await admin.execute(
            f'CREATE ROLE "{worker_role}" LOGIN PASSWORD '
            f"'{worker_password}'"
        )
        await admin.execute(
            f'CREATE ROLE "{operator_role}" LOGIN PASSWORD '
            f"'{operator_password}'"
        )
        await admin.execute(
            f'CREATE ROLE "{orchestrator_role}" LOGIN PASSWORD '
            f"'{orchestrator_password}'"
        )
    finally:
        await admin.close()

    target_url = _database_url(database)
    try:
        migrated = _run_alembic(target_url)
        assert migrated.returncode == 0, migrated.stdout + migrated.stderr

        admin = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            assert await admin.fetchval("SHOW server_version_num") >= "160000"
            fingerprints = await admin.fetchrow(
                """
                SELECT *
                FROM public.vp_worker_endpoint_fingerprints($1::jsonb)
                """,
                json.dumps(ENDPOINT_BINDINGS),
            )
            token_hash = _sha256("observer-admission-token")
            await admin.execute(
                """
                INSERT INTO public.worker_admission_grants (
                    service_name, generation, worker_type, worker_host,
                    capabilities_json, release_commit, image_identity,
                    database_principal, redis_stream, redis_group,
                    endpoint_bindings_json, token_sha256, state, issued_at,
                    issued_by, activated_at
                ) VALUES (
                    'observer-service', 1, 'vision', 'worker-127',
                    '["vision"]'::jsonb, $1, $2, $3,
                    'vp:tasks:vision', 'vision-workers', $4::jsonb,
                    $5, 'active', clock_timestamp(), 'test',
                    clock_timestamp()
                )
                """,
                RELEASE_COMMIT,
                IMAGE_IDENTITY,
                worker_role,
                json.dumps(ENDPOINT_BINDINGS),
                token_hash,
            )
            worker_functions = (
                "vp_worker_register(text,bigint,text,text,uuid,integer,text,"
                "jsonb,text,text,text,text,jsonb,text,text,text,text,text)",
                "vp_require_worker_lease(uuid,bigint)",
                "vp_require_worker_lease_margin(uuid,bigint,integer)",
            )
            for signature in worker_functions:
                await admin.execute(
                    f"GRANT EXECUTE ON FUNCTION public.{signature} "
                    f'TO "{worker_role}"'
                )
            await admin.execute(
                "GRANT EXECUTE ON FUNCTION "
                "public.vp_worker_registration_revoke(text,uuid,text) "
                f'TO "{operator_role}"'
            )
        finally:
            await admin.close()

        suffix = target_url.split("@", 1)[1]
        worker = await asyncpg.connect(
            f"postgresql://{worker_role}:{worker_password}@{suffix}"
        )
        worker_peer = await asyncpg.connect(
            f"postgresql://{worker_role}:{worker_password}@{suffix}"
        )
        operator = await asyncpg.connect(
            f"postgresql://{operator_role}:{operator_password}@{suffix}"
        )
        orchestrator = await asyncpg.connect(
            "postgresql://"
            f"{orchestrator_role}:{orchestrator_password}@{suffix}"
        )
        try:
            register_sql = """
                SELECT *
                FROM public.vp_worker_register(
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10,
                    $11, $12, $13::jsonb, $14, $15, $16, $17, $18
                )
            """
            first_args = (
                "observer-service",
                1,
                "vision",
                "worker-127",
                uuid.uuid4(),
                1,
                "vision-worker:first",
                '["vision"]',
                RELEASE_COMMIT,
                IMAGE_IDENTITY,
                "vp:tasks:vision",
                "vision-workers",
                json.dumps(ENDPOINT_BINDINGS),
                fingerprints["database_fingerprint"],
                fingerprints["redis_fingerprint"],
                fingerprints["storage_fingerprint"],
                token_hash,
                _sha256("observer-lease-one"),
            )
            first = await worker.fetchrow(register_sql, *first_args)
            registration_id = first["registration_id"]
            lease_epoch = first["lease_epoch"]

            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await orchestrator.execute(
                    "SELECT public.vp_observe_worker_lease($1, $2)",
                    registration_id,
                    lease_epoch,
                )

            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_require_worker_lease(uuid,bigint) "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_observe_worker_lease(uuid,bigint) "
                    f'TO "{orchestrator_role}"'
                )
            finally:
                await admin.close()

            with pytest.raises(
                asyncpg.RaiseError,
                match="database_principal_mismatch",
            ):
                await orchestrator.execute(
                    "SELECT public.vp_require_worker_lease($1, $2)",
                    registration_id,
                    lease_epoch,
                )
            await orchestrator.execute(
                "SELECT public.vp_observe_worker_lease($1, $2)",
                registration_id,
                lease_epoch,
            )

            observer_tx = orchestrator.transaction()
            await observer_tx.start()
            await orchestrator.execute(
                "SELECT public.vp_observe_worker_lease($1, $2)",
                registration_id,
                lease_epoch,
            )
            second_args = list(first_args)
            second_args[4] = uuid.uuid4()
            second_args[5] = 2
            second_args[6] = "vision-worker:second"
            second_args[17] = _sha256("observer-lease-two")
            takeover = asyncio.create_task(
                worker.fetchrow(register_sql, *second_args)
            )
            done, _ = await asyncio.wait({takeover}, timeout=0.25)
            assert takeover not in done
            await observer_tx.commit()
            second = await asyncio.wait_for(takeover, timeout=3)
            assert second["lease_epoch"] == lease_epoch + 1

            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                await admin.execute(
                    """
                    UPDATE public.worker_registrations
                    SET lease_expires_at =
                        clock_timestamp() + interval '120 seconds'
                    WHERE id = $1
                    """,
                    second["registration_id"],
                )
            finally:
                await admin.close()
            with pytest.raises(
                asyncpg.RaiseError,
                match="lease_margin_insufficient",
            ):
                await worker.execute(
                    "SELECT public.vp_require_worker_lease_margin($1, $2, 150)",
                    second["registration_id"],
                    second["lease_epoch"],
                )

            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                await admin.execute(
                    """
                    UPDATE public.worker_registrations
                    SET lease_expires_at =
                        clock_timestamp() + interval '180 seconds'
                    WHERE id = $1
                    """,
                    second["registration_id"],
                )
            finally:
                await admin.close()
            await worker.execute(
                "SELECT public.vp_require_worker_lease_margin($1, $2, 150)",
                second["registration_id"],
                second["lease_epoch"],
            )

            worker_started_at = datetime(
                2026,
                7,
                26,
                12,
                0,
                tzinfo=timezone.utc,
            )
            task_message_id = "1710000000000-4"
            task_payload_hash = _sha256("canonical-worker-task")
            dispatch_key = uuid.uuid4()
            claim_dispatch_key = uuid.uuid4()
            claim_message_id = "1710000000000-5"
            claim_payload_hash = _sha256("atomic-claim-task")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await worker.execute(
                    """
                    SELECT public.vp_require_worker_task_ack_receipt(
                        $1, $2, $3, $4, $5, $6, $7, $8, $9
                    )
                    """,
                    second["registration_id"],
                    second["lease_epoch"],
                    "vision-worker:second",
                    worker_started_at,
                    "vp:tasks:vision",
                    "vision-workers",
                    task_message_id,
                    task_payload_hash,
                    dispatch_key,
                )

            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                seeded = await admin.fetchrow(
                    """
                    WITH pipeline AS (
                        INSERT INTO public.pipelines (name, definition)
                        VALUES ('receipt-test', '{}'::json)
                        RETURNING id
                    ),
                    job AS (
                        INSERT INTO public.jobs (
                            pipeline_id, pipeline_snapshot, status
                        )
                        SELECT id, '{}'::json, 'RUNNING'::job_status
                        FROM pipeline
                        RETURNING id
                    )
                    INSERT INTO public.node_executions (
                        job_id, node_id, node_type, status, worker_id,
                        started_at, worker_registration_id,
                        worker_lease_epoch
                    )
                    SELECT
                        id, 'vision-1', 'vision', 'RUNNING'::node_status,
                        $1, $2, $3, $4
                    FROM job
                    RETURNING id, job_id
                    """,
                    "vision-worker:second",
                    worker_started_at,
                    second["registration_id"],
                    second["lease_epoch"],
                )
                assert seeded is not None
                claim_node = await admin.fetchrow(
                    """
                    INSERT INTO public.node_executions (
                        job_id, node_id, node_type, status
                    ) VALUES (
                        $1, 'vision-atomic-claim', 'vision',
                        'QUEUED'::node_status
                    )
                    RETURNING id
                    """,
                    seeded["job_id"],
                )
                assert claim_node is not None
                await admin.execute(
                    """
                    INSERT INTO public.worker_task_dispatches (
                        dispatch_key, job_id, node_execution_id,
                        redis_stream, consumer_group, payload_sha256,
                        payload_json, delivery_state, redis_message_id,
                        delivery_attempted_at, delivered_at
                    ) VALUES (
                        $1, $2, $3, 'vp:tasks:vision', 'vision-workers',
                        $4, $5::jsonb, 'delivered', $6,
                        clock_timestamp(), clock_timestamp()
                    )
                    """,
                    claim_dispatch_key,
                    seeded["job_id"],
                    claim_node["id"],
                    claim_payload_hash,
                    json.dumps(
                        {
                            "dispatch_key": str(claim_dispatch_key),
                            "job_id": str(seeded["job_id"]),
                            "node_execution_id": str(claim_node["id"]),
                        }
                    ),
                    claim_message_id,
                )
                source_task_payload = {
                    "job_id": str(seeded["job_id"]),
                    "node_execution_id": str(seeded["id"]),
                    "node_id": "vision-1",
                    "node_type": "vision",
                    "config": "{}",
                    "input_artifacts": "{}",
                    "dispatch_key": str(dispatch_key),
                }
                task_payload_hash = _sha256(
                    json.dumps(
                        source_task_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
                await admin.execute(
                    """
                    INSERT INTO public.worker_task_dispatches (
                        dispatch_key, job_id, node_execution_id,
                        redis_stream, consumer_group, payload_sha256,
                        payload_json, delivery_state, redis_message_id,
                        delivery_attempted_at, delivered_at
                    ) VALUES (
                        $1, $2, $3, 'vp:tasks:vision', 'vision-workers',
                        $4, $5::jsonb, 'delivered', $6,
                        clock_timestamp(), clock_timestamp()
                    )
                    """,
                    dispatch_key,
                    seeded["job_id"],
                    seeded["id"],
                    task_payload_hash,
                    json.dumps(source_task_payload),
                    task_message_id,
                )
                receipt_payload = {
                    "event": "node_completed",
                    "job_id": str(seeded["job_id"]),
                    "node_execution_id": str(seeded["id"]),
                    "output_artifact_id": str(uuid.uuid4()),
                    "worker_id": "vision-worker:second",
                    "started_at": worker_started_at.isoformat(),
                    "worker_registration_id": str(
                        second["registration_id"]
                    ),
                    "worker_lease_epoch": str(second["lease_epoch"]),
                    "task_stream": "vp:tasks:vision",
                    "task_group": "vision-workers",
                    "task_message_id": task_message_id,
                    "task_payload_sha256": task_payload_hash,
                    "task_dispatch_key": str(dispatch_key),
                }
                await admin.execute(
                    "GRANT SELECT ON public.node_executions "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT UPDATE (status, progress, completed_at) "
                    "ON public.node_executions "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT SELECT ON public.worker_task_dispatches, "
                    "public.worker_task_delivery_attestations "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT UPDATE (ack_state, acknowledged_at) ON "
                    "public.worker_task_delivery_attestations "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT SELECT, INSERT ON "
                    "public.registered_worker_event_receipts, "
                    "public.registered_worker_event_deliveries "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT UPDATE ("
                    "application_state, applied_at, ack_state, "
                    "acknowledged_at, source_task_ack_state, "
                    "source_task_acknowledged_at) "
                    "ON public.registered_worker_event_receipts "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT UPDATE (ack_state, acknowledged_at) ON "
                    "public.registered_worker_event_deliveries "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_require_worker_task_ack_receipt("
                    "uuid,bigint,text,timestamp with time zone,"
                    "text,text,text,text,uuid) "
                    f'TO "{worker_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_attest_worker_task_delivery("
                    "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
                    "text,text,text,text,uuid) "
                    f'TO "{worker_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_authorize_worker_task_ack("
                    "uuid,uuid,bigint,text,timestamp with time zone) "
                    f'TO "{worker_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_acknowledge_worker_task_delivery("
                    "uuid,uuid,bigint,text,timestamp with time zone,"
                    "text,text,text,text,uuid) "
                    f'TO "{worker_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_claim_worker_node("
                    "uuid,bigint,text,uuid,uuid,text,text,text,text,uuid) "
                    f'TO "{worker_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_observe_worker_task_delivery("
                    "uuid,bigint,text,timestamp with time zone,uuid,uuid,"
                    "text,text,text,text,uuid) "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_acknowledge_proven_worker_task_dispatch(uuid) "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_resolve_worker_event_authority_for_job_deletion("
                    "uuid) "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_recover_registered_worker_node(uuid,uuid) "
                    f'TO "{orchestrator_role}"'
                )
                for signature in (
                    "vp_authorize_cancelled_worker_task_ack(uuid)",
                    "vp_require_cancelled_worker_task_ack("
                    "uuid,text,text,text,text,uuid)",
                    "vp_acknowledge_cancelled_worker_task("
                    "uuid,text,text,text,text,uuid)",
                ):
                    await admin.execute(
                        f"GRANT EXECUTE ON FUNCTION public.{signature} "
                        f'TO "{orchestrator_role}"'
                    )
                await admin.execute(
                    "GRANT SELECT ON public.jobs "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT UPDATE (status) ON public.jobs "
                    f'TO "{orchestrator_role}"'
                )
            finally:
                await admin.close()

            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await worker.execute(
                    """
                    UPDATE public.node_executions
                    SET status = 'RUNNING'::node_status
                    WHERE id = $1
                    """,
                    claim_node["id"],
                )
            claim_sql = """
                SELECT *
                FROM public.vp_claim_worker_node(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                )
            """
            claim_args = (
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:atomic",
                seeded["job_id"],
                claim_node["id"],
                "vp:tasks:vision",
                "vision-workers",
                claim_message_id,
                claim_payload_hash,
                claim_dispatch_key,
            )
            wrong_claim_args = list(claim_args)
            wrong_claim_args[7] = "1710000000000-999"
            with pytest.raises(asyncpg.RaiseError, match="task_dispatch_mismatch"):
                await worker.fetchrow(claim_sql, *wrong_claim_args)
            assert await orchestrator.fetchval(
                "SELECT status::text FROM public.node_executions WHERE id = $1",
                claim_node["id"],
            ) == "QUEUED"
            claimed = await worker.fetchrow(claim_sql, *claim_args)
            assert claimed is not None
            assert isinstance(claimed["worker_started_at"], datetime)
            assert isinstance(claimed["attestation_id"], uuid.UUID)
            assert await orchestrator.fetchval(
                "SELECT status::text FROM public.node_executions WHERE id = $1",
                claim_node["id"],
            ) == "RUNNING"
            await worker.execute(
                """
                SELECT public.vp_authorize_worker_task_ack(
                    $1, $2, $3, $4, $5
                )
                """,
                claimed["attestation_id"],
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:atomic",
                claimed["worker_started_at"],
            )
            await worker.execute(
                """
                SELECT public.vp_acknowledge_worker_task_delivery(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                )
                """,
                claimed["attestation_id"],
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:atomic",
                claimed["worker_started_at"],
                "vp:tasks:vision",
                "vision-workers",
                claim_message_id,
                claim_payload_hash,
                claim_dispatch_key,
            )
            assert await orchestrator.fetchval(
                """
                SELECT public.vp_recover_registered_worker_node($1, $2)
                """,
                seeded["job_id"],
                claim_node["id"],
            ) == "live"

            cancel_pending_key = uuid.uuid4()
            cancel_delivered_key = uuid.uuid4()
            cancel_message_id = "1710000000000-6"
            cancel_payload_hash = _sha256("cancelled-delivered-task")
            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                cancelled_job = await admin.fetchrow(
                    """
                    WITH pipeline AS (
                        INSERT INTO public.pipelines (name, definition)
                        VALUES ('cancel-reconcile-test', '{}'::json)
                        RETURNING id
                    )
                    INSERT INTO public.jobs (
                        pipeline_id, pipeline_snapshot, status
                    )
                    SELECT id, '{}'::json, 'CANCELLED'::job_status
                    FROM pipeline
                    RETURNING id
                    """
                )
                assert cancelled_job is not None
                cancel_pending_node = await admin.fetchval(
                    """
                    INSERT INTO public.node_executions (
                        job_id, node_id, node_type, status
                    ) VALUES (
                        $1, 'cancel-pending', 'vision',
                        'CANCELLED'::node_status
                    )
                    RETURNING id
                    """,
                    cancelled_job["id"],
                )
                cancel_delivered_node = await admin.fetchval(
                    """
                    INSERT INTO public.node_executions (
                        job_id, node_id, node_type, status
                    ) VALUES (
                        $1, 'cancel-delivered', 'vision',
                        'CANCELLED'::node_status
                    )
                    RETURNING id
                    """,
                    cancelled_job["id"],
                )
                await admin.execute(
                    """
                    INSERT INTO public.worker_task_dispatches (
                        dispatch_key, job_id, node_execution_id,
                        redis_stream, consumer_group, payload_sha256,
                        payload_json, delivery_state
                    ) VALUES (
                        $1, $2, $3, 'vp:tasks:vision', 'vision-workers',
                        $4, $5::jsonb, 'pending'
                    )
                    """,
                    cancel_pending_key,
                    cancelled_job["id"],
                    cancel_pending_node,
                    _sha256("cancelled-pending-task"),
                    json.dumps({"dispatch_key": str(cancel_pending_key)}),
                )
                await admin.execute(
                    """
                    INSERT INTO public.worker_task_dispatches (
                        dispatch_key, job_id, node_execution_id,
                        redis_stream, consumer_group, payload_sha256,
                        payload_json, delivery_state, redis_message_id,
                        delivery_attempted_at, delivered_at
                    ) VALUES (
                        $1, $2, $3, 'vp:tasks:vision', 'vision-workers',
                        $4, $5::jsonb, 'delivered', $6,
                        clock_timestamp(), clock_timestamp()
                    )
                    """,
                    cancel_delivered_key,
                    cancelled_job["id"],
                    cancel_delivered_node,
                    cancel_payload_hash,
                    json.dumps({"dispatch_key": str(cancel_delivered_key)}),
                    cancel_message_id,
                )
            finally:
                await admin.close()

            cancel_engine = create_async_engine(
                "postgresql+asyncpg://"
                f"{orchestrator_role}:{orchestrator_password}@{suffix}"
            )
            cancel_service = RegisteredWorkerEventReceiptService(
                async_sessionmaker(cancel_engine, expire_on_commit=False)
            )

            class CancelRedis:
                def __init__(self) -> None:
                    self.calls: list[tuple[str, str, str]] = []

                async def get(self, key):
                    return None

                async def xack(self, stream, group, message_id):
                    self.calls.append((stream, group, message_id))
                    return 1

            cancel_redis = CancelRedis()
            try:
                await cancel_service.reconcile_cancelled_dispatches(
                    cancel_redis
                )
            finally:
                await cancel_engine.dispose()
            assert cancel_redis.calls == [
                (
                    "vp:tasks:vision",
                    "vision-workers",
                    cancel_message_id,
                )
            ]
            cancel_states = await orchestrator.fetch(
                """
                SELECT dispatch_key, delivery_state, resolution_state
                FROM public.worker_task_dispatches
                WHERE job_id = $1
                ORDER BY dispatch_key
                """,
                cancelled_job["id"],
            )
            state_by_key = {
                row["dispatch_key"]: (
                    row["delivery_state"],
                    row["resolution_state"],
                )
                for row in cancel_states
            }
            assert state_by_key == {
                cancel_pending_key: ("cancelled", "cancelled"),
                cancel_delivered_key: ("delivered", "acknowledged"),
            }

            concurrent_cancel_key = uuid.uuid4()
            concurrent_cancel_message_id = "1710000000000-7"
            concurrent_cancel_hash = _sha256("concurrent-cancel-task")
            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                concurrent_cancel_job = await admin.fetchrow(
                    """
                    WITH pipeline AS (
                        INSERT INTO public.pipelines (name, definition)
                        VALUES ('concurrent-cancel-test', '{}'::json)
                        RETURNING id
                    ),
                    job AS (
                        INSERT INTO public.jobs (
                            pipeline_id, pipeline_snapshot, status
                        )
                        SELECT id, '{}'::json, 'CANCELLED'::job_status
                        FROM pipeline
                        RETURNING id
                    )
                    INSERT INTO public.node_executions (
                        job_id, node_id, node_type, status
                    )
                    SELECT
                        id, 'cancel-concurrent', 'vision',
                        'CANCELLED'::node_status
                    FROM job
                    RETURNING id, job_id
                    """
                )
                assert concurrent_cancel_job is not None
                await admin.execute(
                    """
                    INSERT INTO public.worker_task_dispatches (
                        dispatch_key, job_id, node_execution_id,
                        redis_stream, consumer_group, payload_sha256,
                        payload_json, delivery_state, redis_message_id,
                        delivery_attempted_at, delivered_at
                    ) VALUES (
                        $1, $2, $3, 'vp:tasks:vision', 'vision-workers',
                        $4, $5::jsonb, 'delivered', $6,
                        clock_timestamp(), clock_timestamp()
                    )
                    """,
                    concurrent_cancel_key,
                    concurrent_cancel_job["job_id"],
                    concurrent_cancel_job["id"],
                    concurrent_cancel_hash,
                    json.dumps(
                        {"dispatch_key": str(concurrent_cancel_key)}
                    ),
                    concurrent_cancel_message_id,
                )
                await admin.execute(
                    """
                    INSERT INTO public.worker_task_delivery_attestations (
                        redis_stream, consumer_group, message_id,
                        payload_sha256, dispatch_key, job_id,
                        node_execution_id, worker_registration_id,
                        worker_lease_epoch, worker_id, worker_started_at
                    ) VALUES (
                        'vp:tasks:vision', 'vision-workers', $1, $2, $3,
                        $4, $5, $6, $7, 'vision-worker:cancel',
                        clock_timestamp()
                    )
                    """,
                    concurrent_cancel_message_id,
                    concurrent_cancel_hash,
                    concurrent_cancel_key,
                    concurrent_cancel_job["job_id"],
                    concurrent_cancel_job["id"],
                    second["registration_id"],
                    second["lease_epoch"],
                )
            finally:
                await admin.close()

            fence_holder = await asyncpg.connect(_asyncpg_url(target_url))
            cleanup_peer = await asyncpg.connect(
                "postgresql://"
                f"{orchestrator_role}:{orchestrator_password}@{suffix}"
            )
            fence_tx = fence_holder.transaction()
            await fence_tx.start()
            await fence_holder.execute(
                """
                SELECT pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended(
                        'vp-worker-registration:' || $1::text,
                        0
                    )
                )
                """,
                str(second["registration_id"]),
            )
            lock_cancel_engine = create_async_engine(
                "postgresql+asyncpg://"
                f"{orchestrator_role}:{orchestrator_password}@{suffix}"
            )
            lock_cancel_service = RegisteredWorkerEventReceiptService(
                async_sessionmaker(
                    lock_cancel_engine,
                    expire_on_commit=False,
                )
            )
            lock_cancel_redis = CancelRedis()
            try:
                cleanup_task = asyncio.create_task(
                    cleanup_peer.fetchval(
                        "SELECT public."
                        "vp_resolve_worker_event_authority_for_job_deletion($1)",
                        concurrent_cancel_job["job_id"],
                    )
                )
                for _ in range(40):
                    waiting = await fence_holder.fetchval(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM pg_catalog.pg_stat_activity
                            WHERE datname = current_database()
                              AND wait_event = 'advisory'
                              AND query LIKE
                                  '%vp_resolve_worker_event_authority%'
                        )
                        """
                    )
                    if waiting:
                        break
                    await asyncio.sleep(0.025)
                assert waiting is True
                cancel_task = asyncio.create_task(
                    lock_cancel_service.reconcile_cancelled_dispatches(
                        lock_cancel_redis
                    )
                )
                done, _ = await asyncio.wait(
                    {cleanup_task, cancel_task},
                    timeout=0.25,
                )
                assert not done
                await fence_tx.commit()
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="worker_event_authority_unresolved",
                ):
                    await asyncio.wait_for(cleanup_task, timeout=3)
                await asyncio.wait_for(cancel_task, timeout=3)
            finally:
                if not fence_holder.is_closed():
                    with suppress(asyncpg.InterfaceError):
                        await fence_tx.rollback()
                await lock_cancel_engine.dispose()
                await cleanup_peer.close()
                await fence_holder.close()
            assert lock_cancel_redis.calls == [
                (
                    "vp:tasks:vision",
                    "vision-workers",
                    concurrent_cancel_message_id,
                )
            ]
            assert await orchestrator.fetchval(
                """
                SELECT resolution_state
                FROM public.worker_task_dispatches
                WHERE dispatch_key = $1
                """,
                concurrent_cancel_key,
            ) == "acknowledged"

            attest_sql = """
                SELECT public.vp_attest_worker_task_delivery(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                )
            """
            attestation_args = (
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
                seeded["job_id"],
                seeded["id"],
                "vp:tasks:vision",
                "vision-workers",
                task_message_id,
                task_payload_hash,
                dispatch_key,
            )
            attestation_ids = await asyncio.gather(
                worker.fetchval(attest_sql, *attestation_args),
                worker_peer.fetchval(attest_sql, *attestation_args),
            )
            assert len(set(attestation_ids)) == 1
            attestation_id = attestation_ids[0]
            assert isinstance(attestation_id, uuid.UUID)
            with pytest.raises(
                asyncpg.RaiseError,
                match="task_ack_authority_missing",
            ):
                await orchestrator.execute(
                    """
                    UPDATE public.worker_task_delivery_attestations
                    SET ack_state = 'authorized'
                    WHERE id = $1
                    """,
                    attestation_id,
                )
            await worker.execute(
                """
                SELECT public.vp_authorize_worker_task_ack(
                    $1, $2, $3, $4, $5
                )
                """,
                attestation_id,
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
            )
            await worker.execute(
                """
                SELECT public.vp_authorize_worker_task_ack(
                    $1, $2, $3, $4, $5
                )
                """,
                attestation_id,
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
            )
            assert await orchestrator.fetchval(
                """
                SELECT ack_state
                FROM public.worker_task_delivery_attestations
                WHERE id = $1
                """,
                attestation_id,
            ) == "authorized"

            for argument_index, wrong_value in (
                (5, uuid.uuid4()),
                (6, "vp:tasks:other"),
                (7, "other-workers"),
                (8, "1710000000000-99"),
                (9, "f" * 64),
                (10, uuid.uuid4()),
            ):
                mismatched_args = [
                    second["registration_id"],
                    second["lease_epoch"],
                    "vision-worker:second",
                    worker_started_at,
                    seeded["job_id"],
                    seeded["id"],
                    "vp:tasks:vision",
                    "vision-workers",
                    task_message_id,
                    task_payload_hash,
                    dispatch_key,
                ]
                mismatched_args[argument_index] = wrong_value
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="mismatch",
                ):
                    await worker.fetchval(
                        """
                        SELECT public.vp_attest_worker_task_delivery(
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                            $11
                        )
                        """,
                        *mismatched_args,
                    )

            observed_attestation_id = await orchestrator.fetchval(
                """
                SELECT public.vp_observe_worker_task_delivery(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                )
                """,
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
                seeded["job_id"],
                seeded["id"],
                "vp:tasks:vision",
                "vision-workers",
                task_message_id,
                task_payload_hash,
                dispatch_key,
            )
            assert observed_attestation_id == attestation_id

            orchestrator_database_url = (
                "postgresql+asyncpg://"
                f"{orchestrator_role}:{orchestrator_password}@{suffix}"
            )
            orchestrator_engine = create_async_engine(
                orchestrator_database_url
            )
            orchestrator_sessions = async_sessionmaker(
                orchestrator_engine,
                expire_on_commit=False,
            )
            apply_count = 0

            async def lock_authority(
                db,
                job_id,
                *,
                node_execution_id,
                lock_all_nodes,
            ):
                assert job_id == seeded["job_id"]
                assert node_execution_id == seeded["id"]
                assert lock_all_nodes is True
                locked_node = (
                    await db.execute(
                        text(
                            """
                            SELECT
                                id, job_id, status, worker_id, started_at,
                                worker_registration_id, worker_lease_epoch
                            FROM public.node_executions
                            WHERE id = :node_execution_id
                            FOR UPDATE
                            """
                        ),
                        {"node_execution_id": node_execution_id},
                    )
                ).mappings().one()
                assert locked_node["status"] in {"RUNNING", "SUCCEEDED"}
                assert (
                    locked_node["worker_registration_id"]
                    == second["registration_id"]
                )
                return SimpleNamespace(
                    channel=None,
                    task=None,
                    schedule=SimpleNamespace(
                        state="OPEN",
                        guarded_job_id=job_id,
                    ),
                    job=SimpleNamespace(
                        id=job_id,
                        status=JobStatus.RUNNING,
                    ),
                    node=SimpleNamespace(
                        id=node_execution_id,
                        status=NodeStatus(locked_node["status"]),
                        worker_id=locked_node["worker_id"],
                        started_at=locked_node["started_at"],
                        worker_registration_id=locked_node[
                            "worker_registration_id"
                        ],
                        worker_lease_epoch=locked_node[
                            "worker_lease_epoch"
                        ],
                    ),
                )

            async def apply_event(db, receipt, event):
                nonlocal apply_count
                apply_count += 1
                await db.execute(
                    text(
                        """
                        UPDATE public.node_executions
                        SET status = 'SUCCEEDED'::node_status,
                            progress = 100,
                            completed_at = clock_timestamp()
                        WHERE id = :node_execution_id
                        """
                    ),
                    {"node_execution_id": event.node_execution_id},
                )

            receipt_service = RegisteredWorkerEventReceiptService(
                orchestrator_sessions,
                authority_locker=lock_authority,
            )
            first_event = parse_registered_worker_event(
                redis_stream="vp:events",
                consumer_group="orchestrator",
                message_id="1710000001000-0",
                payload=receipt_payload,
            )
            duplicate_event = parse_registered_worker_event(
                redis_stream="vp:events",
                consumer_group="orchestrator",
                message_id="1710000001001-0",
                payload=receipt_payload,
            )
            mismatched_event = parse_registered_worker_event(
                redis_stream="vp:events",
                consumer_group="orchestrator",
                message_id="1710000001002-0",
                payload={
                    **receipt_payload,
                    "output_artifact_id": str(uuid.uuid4()),
                },
            )
            try:
                receipt_ids = await asyncio.gather(
                    receipt_service.accept_and_apply(
                        first_event,
                        apply_event,
                    ),
                    receipt_service.accept_and_apply(
                        duplicate_event,
                        apply_event,
                    ),
                )
                assert (
                    await receipt_service.accept_and_apply(
                        mismatched_event,
                        apply_event,
                    )
                    is None
                )

                class Redis:
                    def __init__(self) -> None:
                        self.calls: list[tuple[str, str, str]] = []

                    async def xack(self, stream, group, message_id):
                        self.calls.append((stream, group, message_id))
                        return 1

                redis = Redis()
                await receipt_service.acknowledge_applied(
                    redis,
                    mismatched_event,
                )
                assert redis.calls == [
                    (
                        "vp:events",
                        "orchestrator",
                        "1710000001002-0",
                    )
                ]
            finally:
                await orchestrator_engine.dispose()
            assert len(set(receipt_ids)) == 1
            receipt_id = receipt_ids[0]
            assert isinstance(receipt_id, uuid.UUID)
            assert apply_count == 1
            assert await orchestrator.fetchval(
                """
                SELECT count(*)
                FROM public.registered_worker_event_deliveries
                WHERE receipt_id = $1
                """,
                receipt_id,
            ) == 3
            quarantined = await orchestrator.fetchrow(
                """
                SELECT resolution_state, reason_code, ack_state
                FROM public.registered_worker_event_deliveries
                WHERE message_id = '1710000001002-0'
                """
            )
            assert tuple(quarantined) == (
                "quarantined",
                "event_payload_mismatch",
                "acknowledged",
            )
            with pytest.raises(
                asyncpg.RaiseError,
                match="worker_event_authority_unresolved",
            ):
                await orchestrator.execute(
                    "SELECT public."
                    "vp_resolve_worker_event_authority_for_job_deletion($1)",
                    seeded["job_id"],
                )

            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                await admin.execute(
                    """
                    UPDATE public.jobs
                    SET status = 'SUCCEEDED'::job_status,
                        completed_at = clock_timestamp()
                    WHERE id = $1
                    """,
                    seeded["job_id"],
                )
            finally:
                await admin.close()
            alias_locked = asyncio.Event()
            release_alias = asyncio.Event()

            async def blocking_alias_authority(
                db,
                job_id,
                *,
                node_execution_id,
                lock_all_nodes,
            ):
                await db.execute(
                    text(
                        """
                        SELECT id
                        FROM public.jobs
                        WHERE id = :job_id
                        FOR UPDATE
                        """
                    ),
                    {"job_id": job_id},
                )
                alias_locked.set()
                await release_alias.wait()
                return await lock_authority(
                    db,
                    job_id,
                    node_execution_id=node_execution_id,
                    lock_all_nodes=lock_all_nodes,
                )

            alias_event = parse_registered_worker_event(
                redis_stream="vp:events",
                consumer_group="orchestrator",
                message_id="1710000001003-0",
                payload=receipt_payload,
            )
            alias_engine = create_async_engine(orchestrator_database_url)
            alias_service = RegisteredWorkerEventReceiptService(
                async_sessionmaker(alias_engine, expire_on_commit=False),
                authority_locker=blocking_alias_authority,
            )
            cleanup_peer = await asyncpg.connect(
                "postgresql://"
                f"{orchestrator_role}:{orchestrator_password}@{suffix}"
            )
            try:
                alias_task = asyncio.create_task(
                    alias_service.accept_and_apply(alias_event, apply_event)
                )
                await asyncio.wait_for(alias_locked.wait(), timeout=3)
                cleanup_task = asyncio.create_task(
                    cleanup_peer.fetchval(
                        "SELECT public."
                        "vp_resolve_worker_event_authority_for_job_deletion($1)",
                        seeded["job_id"],
                    )
                )
                done, _ = await asyncio.wait({cleanup_task}, timeout=0.25)
                assert cleanup_task not in done
                release_alias.set()
                assert await asyncio.wait_for(alias_task, timeout=3) == receipt_id
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="worker_event_authority_unresolved",
                ):
                    await asyncio.wait_for(cleanup_task, timeout=3)
            finally:
                release_alias.set()
                await alias_engine.dispose()
                await cleanup_peer.close()

            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                tamper = admin.transaction()
                await tamper.start()
                try:
                    with pytest.raises(
                        asyncpg.RaiseError,
                        match="receipt_facts_immutable",
                    ):
                        await admin.execute(
                            """
                            UPDATE public.registered_worker_event_receipts
                            SET worker_id = 'tampered-worker'
                            WHERE source_task_stream = 'vp:tasks:vision'
                              AND source_task_group = 'vision-workers'
                              AND source_task_message_id = $1
                            """,
                            task_message_id,
                        )
                finally:
                    await tamper.rollback()
            finally:
                await admin.close()

            observer_tx = orchestrator.transaction()
            await observer_tx.start()
            await orchestrator.execute(
                """
                SELECT public.vp_observe_worker_task_delivery(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
                )
                """,
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
                seeded["job_id"],
                seeded["id"],
                "vp:tasks:vision",
                "vision-workers",
                task_message_id,
                task_payload_hash,
                dispatch_key,
            )
            revoke = asyncio.create_task(
                operator.fetchval(
                    """
                    SELECT public.vp_worker_registration_revoke($1, $2, $3)
                    """,
                    "observer-service",
                    second["registration_id"],
                    "observer-test",
                )
            )
            done, _ = await asyncio.wait({revoke}, timeout=0.25)
            assert revoke not in done
            await observer_tx.commit()
            assert await asyncio.wait_for(revoke, timeout=3) is True
            assert await orchestrator.fetchval(
                """
                SELECT public.vp_recover_registered_worker_node($1, $2)
                """,
                seeded["job_id"],
                seeded["id"],
            ) == "held_unresolved"

            ack_engine = create_async_engine(orchestrator_database_url)
            ack_service = RegisteredWorkerEventReceiptService(
                async_sessionmaker(
                    ack_engine,
                    expire_on_commit=False,
                )
            )

            class AckRedis:
                def __init__(self) -> None:
                    self.calls: list[tuple[str, str, str]] = []

                async def xack(self, stream, group, message_id):
                    self.calls.append((stream, group, message_id))
                    return 1

            ack_redis = AckRedis()
            try:
                await ack_service.acknowledge_applied(
                    ack_redis,
                    alias_event,
                )
                await ack_service.acknowledge_applied(
                    ack_redis,
                    first_event,
                )
                await ack_service.acknowledge_applied(
                    ack_redis,
                    duplicate_event,
                )
            finally:
                await ack_engine.dispose()
            assert ack_redis.calls == [
                (
                    "vp:tasks:vision",
                    "vision-workers",
                    task_message_id,
                ),
                ("vp:events", "orchestrator", "1710000001003-0"),
                ("vp:events", "orchestrator", "1710000001000-0"),
                ("vp:events", "orchestrator", "1710000001001-0"),
            ]

            await worker.execute(
                """
                SELECT public.vp_require_worker_task_ack_receipt(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9
                )
                """,
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
                "vp:tasks:vision",
                "vision-workers",
                task_message_id,
                task_payload_hash,
                dispatch_key,
            )
            await worker.execute(
                """
                SELECT public.vp_acknowledge_worker_task_delivery(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                )
                """,
                attestation_id,
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
                "vp:tasks:vision",
                "vision-workers",
                task_message_id,
                task_payload_hash,
                dispatch_key,
            )
            assert await orchestrator.fetchval(
                """
                SELECT ack_state
                FROM public.worker_task_delivery_attestations
                WHERE id = $1
                """,
                attestation_id,
            ) == "acknowledged"
            assert await orchestrator.fetchval(
                """
                SELECT resolution_state
                FROM public.worker_task_dispatches
                WHERE dispatch_key = $1
                """,
                dispatch_key,
            ) == "acknowledged"
            with pytest.raises(
                asyncpg.RaiseError,
                match="event_receipt_missing",
            ):
                await worker.execute(
                    """
                    SELECT public.vp_require_worker_task_ack_receipt(
                        $1, $2, $3, $4, $5, $6, $7, $8, $9
                    )
                    """,
                    second["registration_id"],
                    second["lease_epoch"],
                    "vision-worker:second",
                    worker_started_at,
                    "vp:tasks:vision",
                    "vision-workers",
                    "wrong-message",
                    task_payload_hash,
                    dispatch_key,
                )
            assert await orchestrator.fetchval(
                """
                SELECT public.vp_recover_registered_worker_node($1, $2)
                """,
                seeded["job_id"],
                seeded["id"],
            ) == "recovered"
            assert await orchestrator.fetchval(
                """
                SELECT public.vp_recover_registered_worker_node($1, $2)
                """,
                seeded["job_id"],
                seeded["id"],
            ) == "not_registered"
            admin = await asyncpg.connect(_asyncpg_url(target_url))
            try:
                await admin.execute(
                    """
                    UPDATE public.jobs
                    SET status = 'SUCCEEDED'::job_status,
                        completed_at = clock_timestamp()
                    WHERE id = $1
                    """,
                    seeded["job_id"],
                )
            finally:
                await admin.close()
            await orchestrator.execute(
                "SELECT public."
                "vp_resolve_worker_event_authority_for_job_deletion($1)",
                seeded["job_id"],
            )
            assert await orchestrator.fetchval(
                """
                SELECT count(*)
                FROM public.worker_task_delivery_attestations
                WHERE job_id = $1
                """,
                seeded["job_id"],
            ) == 0
            assert await orchestrator.fetchval(
                """
                SELECT count(*)
                FROM public.worker_task_dispatches
                WHERE job_id = $1
                """,
                seeded["job_id"],
            ) == 0
        finally:
            await worker.close()
            await worker_peer.close()
            await operator.close()
            await orchestrator.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            await admin.execute(f'DROP ROLE IF EXISTS "{worker_role}"')
            await admin.execute(f'DROP ROLE IF EXISTS "{operator_role}"')
            await admin.execute(
                f'DROP ROLE IF EXISTS "{orchestrator_role}"'
            )
        finally:
            await admin.close()
