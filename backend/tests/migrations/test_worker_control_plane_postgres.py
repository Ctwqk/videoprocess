from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pytest


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
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await worker.execute(
                    """
                    SELECT public.vp_require_worker_task_ack_receipt(
                        $1, $2, $3, $4, $5, $6, $7
                    )
                    """,
                    second["registration_id"],
                    second["lease_epoch"],
                    "vision-worker:second",
                    worker_started_at,
                    "vp:tasks:vision",
                    "vision-workers",
                    task_message_id,
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
                }
                payload_hash = _sha256(
                    json.dumps(
                        receipt_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                )
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
                    "GRANT SELECT, INSERT ON "
                    "public.registered_worker_event_receipts "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT UPDATE (application_state, applied_at) "
                    "ON public.registered_worker_event_receipts "
                    f'TO "{orchestrator_role}"'
                )
                await admin.execute(
                    "GRANT EXECUTE ON FUNCTION "
                    "public.vp_require_worker_task_ack_receipt("
                    "uuid,bigint,text,timestamp with time zone,"
                    "text,text,text) "
                    f'TO "{worker_role}"'
                )
            finally:
                await admin.close()

            receipt_tx = orchestrator.transaction()
            await receipt_tx.start()
            try:
                await orchestrator.execute(
                    "SELECT public.vp_observe_worker_lease($1, $2)",
                    second["registration_id"],
                    second["lease_epoch"],
                )
                locked_node = await orchestrator.fetchrow(
                    """
                    SELECT
                        id, job_id, status, worker_id, started_at,
                        worker_registration_id, worker_lease_epoch
                    FROM public.node_executions
                    WHERE id = $1
                    FOR UPDATE
                    """,
                    seeded["id"],
                )
                assert locked_node is not None
                assert locked_node["status"] == "RUNNING"
                assert (
                    locked_node["worker_registration_id"]
                    == second["registration_id"]
                )
                receipt_id = await orchestrator.fetchval(
                    """
                    INSERT INTO public.registered_worker_event_receipts (
                        redis_stream, consumer_group, message_id,
                        payload_sha256, payload_json, event_type, job_id,
                        node_execution_id, worker_registration_id,
                        worker_lease_epoch, worker_id, worker_started_at,
                        source_task_stream, source_task_group,
                        source_task_message_id, application_state,
                        ack_state, source_task_ack_state
                    ) VALUES (
                        'vp:events', 'orchestrator', '1710000001000-0',
                        $1, $2::jsonb, 'node_completed', $3, $4, $5, $6,
                        $7, $8, 'vp:tasks:vision', 'vision-workers', $9,
                        'accepted', 'pending', 'pending'
                    )
                    RETURNING id
                    """,
                    payload_hash,
                    json.dumps(receipt_payload),
                    seeded["job_id"],
                    seeded["id"],
                    second["registration_id"],
                    second["lease_epoch"],
                    "vision-worker:second",
                    worker_started_at,
                    task_message_id,
                )
                await orchestrator.execute(
                    """
                    UPDATE public.node_executions
                    SET status = 'SUCCEEDED'::node_status,
                        progress = 100,
                        completed_at = clock_timestamp()
                    WHERE id = $1
                    """,
                    seeded["id"],
                )
                await orchestrator.execute(
                    """
                    UPDATE public.registered_worker_event_receipts
                    SET application_state = 'applied',
                        applied_at = clock_timestamp()
                    WHERE id = $1
                    """,
                    receipt_id,
                )
            except BaseException:
                await receipt_tx.rollback()
                raise
            else:
                await receipt_tx.commit()

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
                "SELECT public.vp_observe_worker_lease($1, $2)",
                second["registration_id"],
                second["lease_epoch"],
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

            await worker.execute(
                """
                SELECT public.vp_require_worker_task_ack_receipt(
                    $1, $2, $3, $4, $5, $6, $7
                )
                """,
                second["registration_id"],
                second["lease_epoch"],
                "vision-worker:second",
                worker_started_at,
                "vp:tasks:vision",
                "vision-workers",
                task_message_id,
            )
            with pytest.raises(
                asyncpg.RaiseError,
                match="event_receipt_missing",
            ):
                await worker.execute(
                    """
                    SELECT public.vp_require_worker_task_ack_receipt(
                        $1, $2, $3, $4, $5, $6, $7
                    )
                    """,
                    second["registration_id"],
                    second["lease_epoch"],
                    "vision-worker:second",
                    worker_started_at,
                    "vp:tasks:vision",
                    "vision-workers",
                    "wrong-message",
                )
        finally:
            await worker.close()
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
