from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import os
import stat
import subprocess
import sys
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import asyncpg
import pytest


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")
BACKEND_ROOT = Path(__file__).resolve().parents[2]
STABLE_ROLES = {
    "readiness": "vp_marker_readiness_runtime",
    "janitor": "vp_marker_janitor_runtime",
    "repair": "vp_marker_repair_runtime",
}
FUNCTIONS = {
    "readiness": {
        "vp_list_worker_redis_marker_expectations(text,integer)",
        "vp_begin_worker_redis_continuity_check(uuid,integer)",
        "vp_finish_worker_redis_continuity_check("
        "uuid,text,text,text,bigint,bigint)",
    },
    "janitor": {
        "vp_claim_worker_redis_marker_cleanup(uuid,integer,integer)",
        "vp_finish_worker_redis_marker_cleanup(uuid,uuid,text,text)",
    },
    "repair": {
        "vp_load_worker_redis_marker_repair(text,uuid)",
        "vp_promote_observed_worker_event_emission(uuid,text,text)",
    },
    "worker": {"vp_require_worker_redis_continuity(integer)"},
}
ALL_FUNCTIONS = set().union(*FUNCTIONS.values())
MARKER_TABLES = (
    "worker_redis_marker_cleanup_authorizations",
    "worker_redis_continuity_status",
    "worker_redis_marker_repair_audits",
)


def _database_url(database: str) -> str:
    return f"{POSTGRES_URL.rsplit('/', 1)[0]}/{database}"


def _asyncpg_url(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _versioned_roles(generation: str) -> dict[str, str]:
    suffix = hashlib.sha256(generation.encode("utf-8")).hexdigest()[:16]
    return {
        purpose: f"vp_marker_{purpose}_{suffix}"
        for purpose in STABLE_ROLES
    }


def _run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=BACKEND_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        text=True,
        capture_output=True,
        check=False,
    )


def _run_role_cli(
    command: str,
    generation: str,
    state_dir: Path,
    owner_url_file: Path,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "app.services.worker_marker_control_role_cli",
            command,
            "--generation",
            generation,
            "--state-dir",
            str(state_dir),
        ],
        cwd=BACKEND_ROOT,
        env={
            **os.environ,
            "WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE": str(
                owner_url_file
            ),
        },
        text=True,
        capture_output=True,
        check=False,
    )


async def _seed_authority(
    connection: asyncpg.Connection,
    *,
    resolved: bool,
    prepared: bool = False,
) -> dict[str, object]:
    identity = uuid.uuid4().hex
    service_name = f"marker-seed-{identity}"
    registration_id = uuid.uuid4()
    grant_id = uuid.uuid4()
    pipeline_id = uuid.uuid4()
    job_id = uuid.uuid4()
    node_id = uuid.uuid4()
    dispatch_id = uuid.uuid4()
    dispatch_key = uuid.uuid4()
    attestation_id = uuid.uuid4()
    emission_id = uuid.uuid4()
    receipt_id = uuid.uuid4()
    task_message_id = f"1710000000000-{int(identity[:4], 16)}"
    event_message_id = f"1710000000001-{int(identity[4:8], 16)}"
    task_hash = _sha256(f"task-{identity}")
    event_hash = _sha256(f"event-{identity}")
    worker_started_at = datetime(
        2026,
        7,
        28,
        12,
        0,
        tzinfo=timezone.utc,
    )
    source_acknowledged = resolved or prepared
    emission_state = (
        "prepared" if prepared else ("resolved" if resolved else "emitted")
    )

    await connection.execute(
        """
        INSERT INTO public.worker_admission_grants (
            id, service_name, generation, worker_type, worker_host,
            capabilities_json, release_commit, image_identity,
            database_principal, redis_stream, redis_group,
            endpoint_bindings_json, token_sha256, state, issued_at,
            issued_by, activated_at
        ) VALUES (
            $1, $2, 1, 'vision', 'worker-150', '["vision"]'::jsonb,
            '0123456789abcdef0123456789abcdef01234567',
            'vp-python-worker:deploy-0123456789ab', $3,
            'vp:tasks:vision', 'vision-workers', '{}'::jsonb, $4,
            'active', clock_timestamp(), 'marker-test', clock_timestamp()
        )
        """,
        grant_id,
        service_name,
        f"seed_{identity[:16]}",
        _sha256(f"token-{identity}"),
    )
    await connection.execute(
        """
        INSERT INTO public.worker_registrations (
            id, grant_id, service_name, worker_type, worker_host,
            capabilities_json, worker_instance_id, worker_slot,
            redis_consumer_id, image_identity, database_principal,
            database_fingerprint, redis_fingerprint, storage_fingerprint,
            lease_epoch, lease_secret_sha256, status, registered_at,
            heartbeat_at, lease_expires_at
        ) VALUES (
            $1, $2, $3, 'vision', 'worker-150', '["vision"]'::jsonb,
            $4, 1, $5, 'vp-python-worker:deploy-0123456789ab', $6,
            $7, $7, $7, 1, $8, 'active',
            clock_timestamp() - interval '1 second',
            clock_timestamp(), clock_timestamp() + interval '180 seconds'
        )
        """,
        registration_id,
        grant_id,
        service_name,
        uuid.uuid4(),
        f"consumer-{identity[:16]}",
        f"seed_{identity[:16]}",
        _sha256(f"fingerprint-{identity}"),
        _sha256(f"lease-{identity}"),
    )
    await connection.execute(
        """
        INSERT INTO public.pipelines (id, name, definition)
        VALUES ($1, $2, '{}'::json)
        """,
        pipeline_id,
        f"marker-pipeline-{identity}",
    )
    await connection.execute(
        """
        INSERT INTO public.jobs (
            id, pipeline_id, pipeline_snapshot, status
        ) VALUES ($1, $2, '{}'::json, 'RUNNING'::job_status)
        """,
        job_id,
        pipeline_id,
    )
    await connection.execute(
        """
        INSERT INTO public.node_executions (
            id, job_id, node_id, node_type, status, worker_id,
            started_at, worker_registration_id, worker_lease_epoch
        ) VALUES (
            $1, $2, 'vision-1', 'vision', 'RUNNING'::node_status,
            $3, $4::timestamptz, $5, 1
        )
        """,
        node_id,
        job_id,
        f"worker-{identity[:16]}",
        worker_started_at,
        registration_id,
    )
    await connection.execute(
        """
        INSERT INTO public.worker_task_dispatches (
            id, dispatch_key, job_id, node_execution_id, redis_stream,
            consumer_group, payload_sha256, payload_json, delivery_state,
            delivery_attempted_at, redis_message_id, resolution_state,
            acknowledged_at, delivered_at
        ) VALUES (
            $1, $2, $3, $4, 'vp:tasks:vision', 'vision-workers',
            $5, $6::jsonb, 'delivered', clock_timestamp(), $7, $8::text,
            CASE WHEN $8::text = 'acknowledged'
                THEN clock_timestamp() END,
            clock_timestamp()
        )
        """,
        dispatch_id,
        dispatch_key,
        job_id,
        node_id,
        task_hash,
        json.dumps({"dispatch_key": str(dispatch_key)}),
        task_message_id,
        "acknowledged" if source_acknowledged else "unresolved",
    )
    await connection.execute(
        """
        INSERT INTO public.worker_task_delivery_attestations (
            id, redis_stream, consumer_group, message_id, payload_sha256,
            dispatch_key, job_id, node_execution_id,
            worker_registration_id, worker_lease_epoch, worker_id,
            worker_started_at, ack_state, acknowledged_at,
            ack_event_emission_id
        ) VALUES (
            $1, 'vp:tasks:vision', 'vision-workers', $2, $3, $4, $5, $6,
            $7, 1, $8, $9::timestamptz, $10::text,
            CASE WHEN $10::text = 'acknowledged'
                THEN clock_timestamp() END,
            CASE WHEN $10::text = 'acknowledged' THEN $11::uuid END
        )
        """,
        attestation_id,
        task_message_id,
        task_hash,
        dispatch_key,
        job_id,
        node_id,
        registration_id,
        f"worker-{identity[:16]}",
        worker_started_at,
        "acknowledged" if source_acknowledged else "pending",
        emission_id,
    )
    await connection.execute(
        """
        INSERT INTO public.worker_event_emissions (
            id, source_task_attestation_id, redis_stream, consumer_group,
            message_id, payload_sha256, payload_json, event_type, job_id,
            node_execution_id, worker_registration_id, worker_lease_epoch,
            worker_id, worker_started_at, emission_state, prepared_at,
            emitted_at, resolved_at
        ) VALUES (
            $1, $2, 'vp:events', 'orchestrator-events',
            CASE WHEN $3 = 'prepared' THEN NULL ELSE $4 END,
            $5, $6::jsonb, 'node_completed', $7, $8, $9, 1, $10,
            $11::timestamptz, $3, clock_timestamp(),
            CASE WHEN $3 = 'prepared' THEN NULL ELSE clock_timestamp() END,
            CASE WHEN $3 = 'resolved' THEN clock_timestamp() END
        )
        """,
        emission_id,
        attestation_id,
        emission_state,
        event_message_id,
        event_hash,
        json.dumps({"event": "node_completed", "job_id": str(job_id)}),
        job_id,
        node_id,
        registration_id,
        f"worker-{identity[:16]}",
        worker_started_at,
    )
    if resolved:
        await connection.execute(
            """
            INSERT INTO public.registered_worker_event_receipts (
                id, source_task_attestation_id, redis_stream,
                consumer_group, message_id, payload_sha256, payload_json,
                event_type, job_id, node_execution_id,
                worker_registration_id, worker_lease_epoch, worker_id,
                worker_started_at, source_task_stream, source_task_group,
                source_task_message_id, application_state, ack_state,
                source_task_ack_state, applied_at, acknowledged_at,
                source_task_acknowledged_at
            ) VALUES (
                $1, $2, 'vp:events', 'orchestrator-events', $3, $4,
                $5::jsonb, 'node_completed', $6, $7, $8, 1, $9,
                $10::timestamptz, 'vp:tasks:vision', 'vision-workers', $11,
                'applied', 'acknowledged', 'acknowledged',
                clock_timestamp(), clock_timestamp(), clock_timestamp()
            )
            """,
            receipt_id,
            attestation_id,
            event_message_id,
            event_hash,
            json.dumps({"event": "node_completed", "job_id": str(job_id)}),
            job_id,
            node_id,
            registration_id,
            f"worker-{identity[:16]}",
            worker_started_at,
            task_message_id,
        )
        await connection.execute(
            """
            INSERT INTO public.registered_worker_event_deliveries (
                source_task_attestation_id, receipt_id, redis_stream,
                consumer_group, message_id, payload_sha256,
                resolution_state, ack_state, acknowledged_at
            ) VALUES (
                $1, $2, 'vp:events', 'orchestrator-events', $3, $4,
                'accepted', 'acknowledged', clock_timestamp()
            )
            """,
            attestation_id,
            receipt_id,
            event_message_id,
            event_hash,
        )
    if not prepared:
        await connection.execute(
            """
            UPDATE public.node_executions
            SET status = 'SUCCEEDED'::node_status,
                progress = 100,
                completed_at = clock_timestamp()
            WHERE id = $1
            """,
            node_id,
        )
        await connection.execute(
            """
            UPDATE public.jobs
            SET status = 'SUCCEEDED'::job_status,
                completed_at = clock_timestamp()
            WHERE id = $1
            """,
            job_id,
        )
    return {
        "service_name": service_name,
        "registration_id": registration_id,
        "job_id": job_id,
        "dispatch_id": dispatch_id,
        "dispatch_key": dispatch_key,
        "dispatch_stream": "vp:tasks:vision",
        "dispatch_message_id": task_message_id,
        "dispatch_hash": task_hash,
        "emission_id": emission_id,
        "emission_stream": "vp:events",
        "emission_message_id": event_message_id,
        "emission_hash": event_hash,
    }


async def _assert_direct_table_denial(
    connection: asyncpg.Connection,
) -> None:
    for table_name in MARKER_TABLES:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.fetch(f"SELECT * FROM public.{table_name}")


async def _assert_exact_function_access(
    connection: asyncpg.Connection,
    expected: set[str],
) -> None:
    for signature in ALL_FUNCTIONS:
        assert await connection.fetchval(
            """
            SELECT pg_catalog.has_function_privilege(
                session_user, $1, 'EXECUTE'
            )
            """,
            f"public.{signature}",
        ) is (signature in expected)


@pytest.mark.asyncio
@pytest.mark.skipif(
    not POSTGRES_URL,
    reason="set CHANNEL_OPS_POSTGRES_TEST_URL for live migration tests",
)
async def test_postgres_16_worker_redis_marker_lifecycle_is_fail_closed(
    tmp_path: Path,
) -> None:
    database = f"vp_worker_marker_{uuid.uuid4().hex}"
    worker_role = f"vp_marker_worker_{uuid.uuid4().hex[:16]}"
    worker_password = uuid.uuid4().hex
    generation = f"marker-test-{uuid.uuid4().hex}"
    next_generation = f"marker-test-{uuid.uuid4().hex}"
    generation_roles = _versioned_roles(generation)
    next_generation_roles = _versioned_roles(next_generation)
    admin_url = _database_url("postgres")
    admin = await asyncpg.connect(_asyncpg_url(admin_url))
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
        await admin.execute(
            f'CREATE ROLE "{worker_role}" LOGIN PASSWORD '
            f"'{worker_password}'"
        )
    finally:
        await admin.close()

    target_url = _database_url(database)
    owner_url_file = tmp_path / "owner-database-url"
    owner_url_file.write_text(f"{target_url}\n", encoding="utf-8")
    owner_url_file.chmod(0o400)
    state_dir = tmp_path / "marker-control"
    migrated = _run_alembic(target_url, "upgrade", "head")
    assert migrated.returncode == 0, migrated.stdout + migrated.stderr

    async def exercise_marker_lifecycle_under_real_roles(
        database_url: str,
    ) -> None:
        module = importlib.import_module(
            "app.services.worker_marker_control_role_cli"
        )
        assert module.role_names_for_generation(
            generation
        ).versioned == generation_roles

        provisioned = _run_role_cli(
            "provision",
            generation,
            state_dir,
            owner_url_file,
        )
        assert provisioned.returncode == 0, (
            provisioned.stdout + provisioned.stderr
        )
        provision_payload = json.loads(provisioned.stdout)
        assert provision_payload == {
            "reason_code": "marker_control_roles_provisioned",
            "roles": generation_roles,
            "status": "ok",
        }
        assert provisioned.stderr == ""
        assert "postgresql" not in provisioned.stdout
        assert "postgres" not in provisioned.stdout

        next_provisioned = _run_role_cli(
            "provision",
            next_generation,
            state_dir,
            owner_url_file,
        )
        assert next_provisioned.returncode == 0, (
            next_provisioned.stdout + next_provisioned.stderr
        )

        generation_dir = state_dir / generation
        assert stat.S_IMODE(generation_dir.stat().st_mode) == 0o700
        credential_paths = module.credential_paths(state_dir, generation)
        for path in credential_paths.values():
            assert path.is_file()
            assert stat.S_IMODE(path.stat().st_mode) == 0o400
        credential_urls = {
            purpose: path.read_text(encoding="utf-8").strip()
            for purpose, path in credential_paths.items()
        }
        assert len(set(credential_urls.values())) == 3
        for purpose, url in credential_urls.items():
            parsed = urlsplit(url.replace("postgresql+asyncpg://", "postgresql://", 1))
            assert parsed.username == generation_roles[purpose]
            assert parsed.password
            assert parsed.path == f"/{database}"

        admin_connection = await asyncpg.connect(_asyncpg_url(database_url))
        try:
            assert await admin_connection.fetchval(
                "SHOW server_version_num"
            ) >= "160000"
            await admin_connection.execute(
                "GRANT EXECUTE ON FUNCTION "
                "public.vp_require_worker_redis_continuity(integer) "
                f'TO "{worker_role}"'
            )
            stable_rows = await admin_connection.fetch(
                """
                SELECT rolname, rolcanlogin, rolsuper, rolcreaterole,
                       rolcreatedb, rolreplication, rolbypassrls
                FROM pg_catalog.pg_roles
                WHERE rolname = ANY($1::text[])
                ORDER BY rolname
                """,
                list(STABLE_ROLES.values()),
            )
            assert len(stable_rows) == 3
            assert all(
                not row["rolcanlogin"]
                and not row["rolsuper"]
                and not row["rolcreaterole"]
                and not row["rolcreatedb"]
                and not row["rolreplication"]
                and not row["rolbypassrls"]
                for row in stable_rows
            )
            memberships = await admin_connection.fetch(
                """
                SELECT member.rolname AS member_name,
                       granted.rolname AS granted_name
                FROM pg_catalog.pg_auth_members AS membership
                JOIN pg_catalog.pg_roles AS member
                  ON member.oid = membership.member
                JOIN pg_catalog.pg_roles AS granted
                  ON granted.oid = membership.roleid
                WHERE member.rolname = ANY($1::text[])
                ORDER BY member.rolname, granted.rolname
                """,
                list(generation_roles.values()),
            )
            assert {
                (row["member_name"], row["granted_name"])
                for row in memberships
            } == {
                (generation_roles[purpose], STABLE_ROLES[purpose])
                for purpose in STABLE_ROLES
            }
        finally:
            await admin_connection.close()

        role_connections = {
            purpose: await asyncpg.connect(_asyncpg_url(url))
            for purpose, url in credential_urls.items()
        }
        worker_url = (
            "postgresql://"
            f"{worker_role}:{worker_password}@{database_url.split('@', 1)[1]}"
        )
        worker = await asyncpg.connect(worker_url)
        try:
            for connection in (*role_connections.values(), worker):
                await _assert_direct_table_denial(connection)
            for purpose, connection in role_connections.items():
                await _assert_exact_function_access(
                    connection,
                    FUNCTIONS[purpose],
                )
            await _assert_exact_function_access(worker, FUNCTIONS["worker"])

            admin_connection = await asyncpg.connect(
                _asyncpg_url(database_url)
            )
            try:
                await admin_connection.execute(
                    "GRANT SELECT ON public."
                    "worker_redis_continuity_status "
                    f'TO "{STABLE_ROLES["readiness"]}"'
                )
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="database_principal_privileged",
                ):
                    await role_connections["readiness"].execute(
                        "SELECT public."
                        "vp_begin_worker_redis_continuity_check($1, 300)",
                        uuid.uuid4(),
                    )
            finally:
                await admin_connection.execute(
                    "REVOKE SELECT ON public."
                    "worker_redis_continuity_status "
                    f'FROM "{STABLE_ROLES["readiness"]}"'
                )
                await admin_connection.close()

            with pytest.raises(
                asyncpg.RaiseError,
                match="worker_redis_continuity_missing",
            ):
                await worker.execute(
                    "SELECT public.vp_require_worker_redis_continuity(90)"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await worker.execute(
                    "SELECT * FROM public."
                    "vp_list_worker_redis_marker_expectations('', 10)"
                )

            readiness = role_connections["readiness"]
            first_run = uuid.uuid4()
            second_run = uuid.uuid4()
            assert await readiness.fetchval(
                "SELECT public."
                "vp_begin_worker_redis_continuity_check($1, 300)",
                first_run,
            ) == "begun"
            assert await readiness.fetchval(
                "SELECT public."
                "vp_begin_worker_redis_continuity_check($1, 300)",
                second_run,
            ) == "overlap"
            admin_connection = await asyncpg.connect(
                _asyncpg_url(database_url)
            )
            try:
                await admin_connection.execute(
                    """
                    UPDATE public.worker_redis_continuity_status
                    SET started_at =
                        clock_timestamp() - interval '301 seconds'
                    WHERE singleton
                    """
                )
            finally:
                await admin_connection.close()
            assert await readiness.fetchval(
                "SELECT public."
                "vp_begin_worker_redis_continuity_check($1, 300)",
                second_run,
            ) == "begun"
            with pytest.raises(
                asyncpg.RaiseError,
                match="worker_redis_continuity_result_invalid",
            ):
                await readiness.execute(
                    """
                    SELECT public.vp_finish_worker_redis_continuity_check(
                        $1, 'unknown', 'ready', 'redis-run', 2, 2
                    )
                    """,
                    second_run,
                )
            assert await readiness.fetchval(
                """
                SELECT public.vp_finish_worker_redis_continuity_check(
                    $1, 'ready', 'ready', 'redis-run', 2, 2
                )
                """,
                second_run,
            )
            await worker.execute(
                "SELECT public.vp_require_worker_redis_continuity(90)"
            )
            admin_connection = await asyncpg.connect(
                _asyncpg_url(database_url)
            )
            try:
                await admin_connection.execute(
                    """
                    UPDATE public.worker_redis_continuity_status
                    SET finished_at =
                        clock_timestamp() - interval '91 seconds'
                    WHERE singleton
                    """
                )
            finally:
                await admin_connection.close()
            with pytest.raises(
                asyncpg.RaiseError,
                match="worker_redis_continuity_stale",
            ):
                await worker.execute(
                    "SELECT public.vp_require_worker_redis_continuity(90)"
                )
            error_run = uuid.uuid4()
            assert await readiness.fetchval(
                "SELECT public."
                "vp_begin_worker_redis_continuity_check($1, 300)",
                error_run,
            ) == "begun"
            assert not await readiness.fetchval(
                """
                SELECT public.vp_finish_worker_redis_continuity_check(
                    $1, 'error', 'redis_unavailable', NULL, 2, 0
                )
                """,
                error_run,
            )
            with pytest.raises(
                asyncpg.RaiseError,
                match="worker_redis_continuity_error",
            ):
                await worker.execute(
                    "SELECT public.vp_require_worker_redis_continuity(90)"
                )

            admin_connection = await asyncpg.connect(
                _asyncpg_url(database_url)
            )
            try:
                resolved = await _seed_authority(
                    admin_connection,
                    resolved=True,
                )
                await admin_connection.execute(
                    "SELECT public."
                    "vp_resolve_worker_event_authority_for_job_deletion($1)",
                    resolved["job_id"],
                )
                tombstones = await admin_connection.fetch(
                    """
                    SELECT marker_kind, source_id, marker_key, redis_stream,
                           expected_message_id, payload_sha256,
                           authorization_state
                    FROM public.worker_redis_marker_cleanup_authorizations
                    WHERE source_id = ANY($1::uuid[])
                    ORDER BY marker_kind
                    """,
                    [resolved["emission_id"], resolved["dispatch_id"]],
                )
                assert [dict(row) for row in tombstones] == [
                    {
                        "marker_kind": "event_emission",
                        "source_id": resolved["emission_id"],
                        "marker_key": (
                            "vp:worker-event-emission:"
                            f"{resolved['emission_id']}"
                        ),
                        "redis_stream": resolved["emission_stream"],
                        "expected_message_id": resolved[
                            "emission_message_id"
                        ],
                        "payload_sha256": resolved["emission_hash"],
                        "authorization_state": "pending",
                    },
                    {
                        "marker_kind": "task_dispatch",
                        "source_id": resolved["dispatch_id"],
                        "marker_key": (
                            "vp:worker-task-dispatch:"
                            f"{resolved['dispatch_key']}"
                        ),
                        "redis_stream": resolved["dispatch_stream"],
                        "expected_message_id": resolved[
                            "dispatch_message_id"
                        ],
                        "payload_sha256": resolved["dispatch_hash"],
                        "authorization_state": "pending",
                    },
                ]
                assert await admin_connection.fetchval(
                    """
                    SELECT count(*)
                    FROM public.worker_event_emissions
                    WHERE job_id = $1
                    """,
                    resolved["job_id"],
                ) == 0
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="marker_cleanup_proof_immutable",
                ):
                    await admin_connection.execute(
                        """
                        UPDATE public.worker_redis_marker_cleanup_authorizations
                        SET marker_key = marker_key || '-changed'
                        WHERE source_id = $1
                        """,
                        resolved["emission_id"],
                    )

                unresolved = await _seed_authority(
                    admin_connection,
                    resolved=False,
                )
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="worker_event_authority_unresolved",
                ):
                    await admin_connection.execute(
                        "SELECT public."
                        "vp_resolve_worker_event_authority_for_job_deletion($1)",
                        unresolved["job_id"],
                    )
                assert await admin_connection.fetchval(
                    """
                    SELECT count(*)
                    FROM public.worker_redis_marker_cleanup_authorizations
                    WHERE source_id = ANY($1::uuid[])
                    """,
                    [unresolved["emission_id"], unresolved["dispatch_id"]],
                ) == 0
                assert await admin_connection.fetchval(
                    """
                    SELECT count(*) FROM public.worker_event_emissions
                    WHERE job_id = $1
                    """,
                    unresolved["job_id"],
                ) == 1

                raced = await _seed_authority(
                    admin_connection,
                    resolved=True,
                )
                await admin_connection.executemany(
                    """
                    INSERT INTO public.
                        worker_redis_marker_cleanup_authorizations (
                            marker_kind, source_id, marker_key, redis_stream,
                            expected_message_id, payload_sha256,
                            authorization_state, authorized_at
                        )
                    VALUES ($1, $2, $3, $4, $5, $6, 'pending',
                            clock_timestamp())
                    """,
                    [
                        (
                            "event_emission",
                            raced["emission_id"],
                            "vp:worker-event-emission:"
                            f"{raced['emission_id']}",
                            raced["emission_stream"],
                            raced["emission_message_id"],
                            raced["emission_hash"],
                        ),
                        (
                            "task_dispatch",
                            raced["dispatch_id"],
                            "vp:worker-task-dispatch:"
                            f"{raced['dispatch_key']}",
                            raced["dispatch_stream"],
                            raced["dispatch_message_id"],
                            raced["dispatch_hash"],
                        ),
                    ],
                )
            finally:
                await admin_connection.close()

            janitor = role_connections["janitor"]
            janitor_transaction = janitor.transaction()
            await janitor_transaction.start()
            race_run = uuid.uuid4()
            claims = await janitor.fetch(
                """
                SELECT *
                FROM public.vp_claim_worker_redis_marker_cleanup($1, 100, 300)
                """,
                race_run,
            )
            assert len(claims) >= 2
            cleanup_connection = await asyncpg.connect(
                _asyncpg_url(database_url)
            )
            cleanup_task = asyncio.create_task(
                cleanup_connection.execute(
                    "SELECT public."
                    "vp_resolve_worker_event_authority_for_job_deletion($1)",
                    raced["job_id"],
                )
            )
            try:
                done, _ = await asyncio.wait({cleanup_task}, timeout=0.1)
                assert cleanup_task not in done
                for claim in claims:
                    if claim["source_id"] in {
                        raced["emission_id"],
                        raced["dispatch_id"],
                    }:
                        await janitor.execute(
                            """
                            SELECT public.vp_finish_worker_redis_marker_cleanup(
                                $1, $2, 'absent', 'marker_absent'
                            )
                            """,
                            claim["id"],
                            race_run,
                        )
                await janitor_transaction.commit()
                await asyncio.wait_for(cleanup_task, timeout=3)
            finally:
                if not cleanup_task.done():
                    cleanup_task.cancel()
                    await asyncio.gather(
                        cleanup_task,
                        return_exceptions=True,
                    )
                if janitor.is_in_transaction():
                    await janitor_transaction.rollback()
                await cleanup_connection.close()

            admin_connection = await asyncpg.connect(
                _asyncpg_url(database_url)
            )
            try:
                assert await admin_connection.fetchval(
                    """
                    SELECT count(*) FROM public.worker_event_emissions
                    WHERE job_id = $1
                    """,
                    raced["job_id"],
                ) == 0
                assert await admin_connection.fetchval(
                    """
                    SELECT bool_and(authorization_state = 'absent')
                    FROM public.worker_redis_marker_cleanup_authorizations
                    WHERE source_id = ANY($1::uuid[])
                    """,
                    [raced["emission_id"], raced["dispatch_id"]],
                )

                prepared = await _seed_authority(
                    admin_connection,
                    resolved=False,
                    prepared=True,
                )
            finally:
                await admin_connection.close()

            repair = role_connections["repair"]
            repair_evidence = await repair.fetchrow(
                """
                SELECT *
                FROM public.vp_load_worker_redis_marker_repair(
                    'promote_prepared', $1
                )
                """,
                prepared["emission_id"],
            )
            assert repair_evidence is not None
            assert repair_evidence["source_id"] == prepared["emission_id"]
            assert repair_evidence["payload_sha256"] == prepared[
                "emission_hash"
            ]
            assert repair_evidence["expected_message_id"] is None
            with pytest.raises(
                asyncpg.RaiseError,
                match="marker_repair_proof_mismatch",
            ):
                await repair.execute(
                    """
                    SELECT public.vp_promote_observed_worker_event_emission(
                        $1, 'not-a-message-id', $2
                    )
                    """,
                    prepared["emission_id"],
                    prepared["emission_hash"],
                )
            with pytest.raises(
                asyncpg.RaiseError,
                match="marker_repair_proof_mismatch",
            ):
                await repair.execute(
                    """
                    SELECT public.vp_promote_observed_worker_event_emission(
                        $1, '1710000000999-0', $2
                    )
                """,
                prepared["emission_id"],
                _sha256("wrong"),
            )
            assert await repair.fetchval(
                """
                SELECT public.vp_promote_observed_worker_event_emission(
                    $1, '1710000000999-0', $2
                )
                """,
                prepared["emission_id"],
                prepared["emission_hash"],
            )
            restored_evidence = await repair.fetchrow(
                """
                SELECT *
                FROM public.vp_load_worker_redis_marker_repair(
                    'restore_marker_applied', $1
                )
                """,
                unresolved["emission_id"],
            )
            assert restored_evidence is not None
            assert restored_evidence["source_id"] == unresolved[
                "emission_id"
            ]
            audit_sources = {
                row["source_id"]
                for row in await repair.fetch(
                    """
                    SELECT source_id
                    FROM public.vp_load_worker_redis_marker_repair(
                        'audit', NULL
                    )
                    """
                )
            }
            assert {
                unresolved["emission_id"],
                unresolved["dispatch_id"],
                prepared["emission_id"],
            } <= audit_sources

            admin_connection = await asyncpg.connect(
                _asyncpg_url(database_url)
            )
            try:
                promoted = await admin_connection.fetchrow(
                    """
                    SELECT emission_state, message_id, payload_sha256
                    FROM public.worker_event_emissions
                    WHERE id = $1
                    """,
                    prepared["emission_id"],
                )
                assert dict(promoted) == {
                    "emission_state": "emitted",
                    "message_id": "1710000000999-0",
                    "payload_sha256": prepared["emission_hash"],
                }
                audits = await admin_connection.fetch(
                    """
                    SELECT source_id, action, result_code, principal
                    FROM public.worker_redis_marker_repair_audits
                    WHERE source_id = ANY($1::uuid[])
                    ORDER BY action
                    """,
                    [
                        prepared["emission_id"],
                        unresolved["emission_id"],
                    ],
                )
                assert [dict(row) for row in audits] == [
                    {
                        "source_id": prepared["emission_id"],
                        "action": "promote_prepared",
                        "result_code": "promoted",
                        "principal": generation_roles["repair"],
                    },
                    {
                        "source_id": unresolved["emission_id"],
                        "action": "restore_marker",
                        "result_code": "restored",
                        "principal": generation_roles["repair"],
                    },
                ]
                audit_columns = {
                    row["column_name"]
                    for row in await admin_connection.fetch(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'public'
                          AND table_name =
                              'worker_redis_marker_repair_audits'
                        """
                    )
                }
                assert audit_columns == {
                    "id",
                    "source_id",
                    "action",
                    "result_code",
                    "principal",
                    "created_at",
                }
                with pytest.raises(
                    asyncpg.RaiseError,
                    match="marker_repair_audit_append_only",
                ):
                    await admin_connection.execute(
                        """
                        UPDATE public.worker_redis_marker_repair_audits
                        SET result_code = 'changed'
                        WHERE source_id = $1
                        """,
                        prepared["emission_id"],
                    )
            finally:
                await admin_connection.close()
        finally:
            await worker.close()
            for connection in role_connections.values():
                await connection.close()

        revoked = _run_role_cli(
            "revoke",
            generation,
            state_dir,
            owner_url_file,
        )
        assert revoked.returncode == 0, revoked.stdout + revoked.stderr
        assert json.loads(revoked.stdout) == {
            "reason_code": "marker_control_roles_revoked",
            "roles": generation_roles,
            "status": "ok",
        }
        admin_connection = await asyncpg.connect(_asyncpg_url(database_url))
        try:
            assert not await admin_connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_roles
                    WHERE rolname = ANY($1::text[])
                )
                """,
                list(generation_roles.values()),
            )
            assert await admin_connection.fetchval(
                """
                SELECT count(*) = 3
                FROM pg_catalog.pg_roles
                WHERE rolname = ANY($1::text[])
                """,
                list(next_generation_roles.values()),
            )
        finally:
            await admin_connection.close()
        assert not (state_dir / generation).exists()
        assert (state_dir / next_generation).is_dir()
        for path in module.credential_paths(
            state_dir,
            next_generation,
        ).values():
            assert path.is_file()

        next_revoked = _run_role_cli(
            "revoke",
            next_generation,
            state_dir,
            owner_url_file,
        )
        assert next_revoked.returncode == 0, (
            next_revoked.stdout + next_revoked.stderr
        )

    try:
        await asyncio.wait_for(
            exercise_marker_lifecycle_under_real_roles(target_url),
            timeout=15,
        )
        downgraded = _run_alembic(target_url, "downgrade", "033")
        assert downgraded.returncode == 0, (
            downgraded.stdout + downgraded.stderr
        )
        connection = await asyncpg.connect(_asyncpg_url(target_url))
        try:
            for table_name in MARKER_TABLES:
                assert not await connection.fetchval(
                    "SELECT pg_catalog.to_regclass($1) IS NOT NULL",
                    f"public.{table_name}",
                )
        finally:
            await connection.close()
    finally:
        admin = await asyncpg.connect(_asyncpg_url(admin_url))
        try:
            await admin.execute(
                f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'
            )
            for role_name in (
                *generation_roles.values(),
                *next_generation_roles.values(),
                worker_role,
                *STABLE_ROLES.values(),
            ):
                with suppress(asyncpg.PostgresError):
                    await admin.execute(f'DROP ROLE IF EXISTS "{role_name}"')
        finally:
            await admin.close()
