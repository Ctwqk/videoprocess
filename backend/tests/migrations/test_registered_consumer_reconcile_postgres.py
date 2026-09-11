"""Parent-only, pre-migrated disposable PG16 qualification. Never provisions DBs.

Opt in with REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_URL and
REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_CONFIRM equal to the exact database name.
Only a loopback, explicit nondefault port and vp_registered_reconcile_test_* DB
are accepted. The fixture creates bounded test roles/rows, not runtime grants to
tables; every call under test authenticates a new real login (never SET ROLE).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import timedelta
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from app.models.worker_registration import WorkerAdmissionGrant, WorkerRegistration
from app.services import registered_consumer_reconcile_runtime as runtime
from app.services.worker_control_role_cli import role_names_for_generation
from app.services.worker_marker_control_role_cli import (
    role_names_for_generation as marker_roles,
)
from app.services.worker_role_cli_common import (
    asyncpg_url,
    create_login_role,
    quote_identifier,
    role_database_url,
)
from app.services.worker_runtime_role_cli import (
    role_names_for_generation as worker_roles,
)
from tests.services.test_registered_consumer_reconcile import (
    NOW,
    decode,
    document,
    facts,
)
from tests.services.test_registered_consumer_reconcile_runtime import invocation


HEAD = "040_owned_history_seal"
SIGNATURE = "public.vp_registered_consumer_reconcile_guard(text,uuid[],uuid[])"
CALL = "SELECT * FROM public.vp_registered_consumer_reconcile_guard($1::text,$2::uuid[],$3::uuid[])"


def checked_url(raw: str, confirmation: str | None) -> str:
    try:
        url = make_url(raw)
        valid = (
            url.drivername in {"postgresql", "postgresql+asyncpg"}
            and url.host in {"127.0.0.1", "::1"}
            and url.port is not None
            and 1024 <= url.port <= 65535
            and url.port != 5432
            and bool(url.username)
            and bool(url.password)
            and not url.query
            and re.fullmatch(
                r"vp_registered_reconcile_test_[a-z0-9_]+", url.database or ""
            )
            and confirmation == url.database
        )
    except Exception:
        valid = False
    if not valid:
        raise ValueError("explicit disposable PostgreSQL URL/confirmation required")
    return asyncpg_url(raw)


def seed_rows(now, generation):
    payload = document()
    base_generation = int(hashlib.sha256(generation.encode()).hexdigest()[:7], 16) + 1
    for worker in payload["workers"]:
        for kind in ("current", "predecessor"):
            pin = worker[kind]
            pin["generation"] = base_generation + (kind == "current")
            pin["registered_at"] = (
                now - timedelta(hours=2 if kind == "predecessor" else 1)
            ).isoformat()
            pin["database_principal"] = worker_roles(
                pin["service_name"], pin["generation"]
            ).versioned
    registrations, grants = facts(payload)
    shift = now - NOW
    for row in registrations:
        row.heartbeat_at += shift
        row.lease_expires_at += shift
        if row.revoked_at is not None:
            row.revoked_at += shift
        row.lease_secret_sha256 = hashlib.sha256(f"lease:{row.id}".encode()).hexdigest()
    for row in grants:
        row.activated_at += shift
        if row.revoked_at is not None:
            row.revoked_at += shift
        row.token_sha256 = hashlib.sha256(f"grant:{row.id}".encode()).hexdigest()
        row.issued_at = now - timedelta(hours=3)
        row.issued_by = "vp-deploy-controller"
        row.created_at = row.updated_at = now - timedelta(hours=3)
    return payload, registrations, grants


def row_values(row):
    return {column.name: getattr(row, column.name) for column in row.__table__.columns}


def json_value(value):
    return str(value) if isinstance(value, UUID) else value.isoformat()


async def insert_row(connection, row):
    table = row.__table__.name
    assert table in {"worker_registrations", "worker_admission_grants"}
    await connection.execute(
        f"INSERT INTO public.{table} SELECT * FROM pg_catalog.jsonb_populate_record(NULL::public.{table},$1::jsonb)",
        json.dumps(row_values(row), default=json_value),
    )


@dataclass
class Case:
    owner: object
    operator: object
    runtime_worker: object
    watcher: object
    request: runtime.Invocation
    registrations: list[WorkerRegistration]
    grants: list[WorkerAdmissionGrant]
    created_roles: list[str]

    def arguments(self):
        return (
            self.request.control_generation,
            [worker.current.registration_id for worker in self.request.pins.workers],
            [
                worker.predecessor.registration_id
                for worker in self.request.pins.workers
            ],
        )

    async def snapshot(self):
        return await self.owner.fetchval("""
            SELECT jsonb_build_object(
                'registrations',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM public.worker_registrations r),
                'grants',(SELECT jsonb_agg(to_jsonb(g) ORDER BY id) FROM public.worker_admission_grants g),
                'schedule',(SELECT to_jsonb(s) FROM public.runtime_schedules s WHERE service_name='videoprocess')
            )::text
        """)


@pytest.fixture
def postgres_case():
    raw = os.environ.get("REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_URL")
    if not raw:
        pytest.skip("parent-only disposable PostgreSQL not configured")
    url = checked_url(
        raw, os.environ.get("REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_CONFIRM")
    )

    @asynccontextmanager
    async def opened():
        owner = await asyncpg.connect(url, timeout=2, command_timeout=2)
        connections, roles, stable_created, execute_added = [], [], [], False
        seeded = schedule_created = False
        registration_ids = grant_ids = []
        try:
            assert (
                160000 <= int(await owner.fetchval("SHOW server_version_num")) < 170000
            )
            assert (
                await owner.fetchval("SELECT version_num FROM public.alembic_version")
                == HEAD
            )
            assert (
                await owner.fetchval("SELECT count(*) FROM public.worker_registrations")
                == 0
            )
            assert (
                await owner.fetchval(
                    "SELECT count(*) FROM public.worker_admission_grants"
                )
                == 0
            )
            assert await owner.fetchval("SELECT count(*) FROM public.jobs") == 0
            schedule = await owner.fetchrow(
                "SELECT * FROM public.runtime_schedules WHERE service_name='videoprocess'"
            )
            if schedule is None:
                await owner.execute(
                    "INSERT INTO public.runtime_schedules(service_name,state,updated_by) VALUES('videoprocess','CLOSED','registered-reconcile-test')"
                )
                schedule_created = True
            else:
                assert (
                    schedule["state"] == "CLOSED" and schedule["guarded_job_id"] is None
                )
            generation = "rcr-test-" + uuid4().hex[:16]
            now = await owner.fetchval("SELECT pg_catalog.clock_timestamp()")
            payload, registrations, grants = seed_rows(now, generation)
            request = replace(
                invocation(), pins=decode(payload), control_generation=generation
            )
            registration_ids = [row.id for row in registrations]
            grant_ids = [row.id for row in grants]
            async with owner.transaction():
                for row in grants:
                    await insert_row(owner, row)
                for row in sorted(
                    registrations, key=lambda row: row.superseded_by is not None
                ):
                    await insert_row(owner, row)
            seeded = True
            operator_role = role_names_for_generation(generation).versioned["operator"]
            worker_role = worker_roles(
                payload["workers"][0]["current"]["service_name"],
                payload["workers"][0]["current"]["generation"],
            ).versioned
            watcher_role = marker_roles(generation).versioned["readiness"]
            stable_roles = (
                "vp_worker_operator_runtime",
                "vp_worker_runtime",
                "vp_marker_readiness_runtime",
            )
            for role, stable in zip(
                (operator_role, worker_role, watcher_role), stable_roles, strict=True
            ):
                if (
                    await owner.fetchval("SELECT pg_catalog.to_regrole($1)", stable)
                    is None
                ):
                    await owner.execute(
                        f"CREATE ROLE {quote_identifier(stable)} NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
                    )
                    stable_created.append(stable)
                password = uuid4().hex + uuid4().hex
                async with owner.transaction():
                    await create_login_role(
                        owner,
                        role,
                        password,
                        setting_prefix="rcr_test",
                        stable_role=stable,
                    )
                roles.append(role)
                connections.append(
                    await asyncpg.connect(
                        role_database_url(url, role, password),
                        timeout=2,
                        command_timeout=2,
                    )
                )
            if not await owner.fetchval(
                "SELECT pg_catalog.has_function_privilege($1,$2,'EXECUTE')",
                stable_roles[0],
                SIGNATURE,
            ):
                await owner.execute(
                    f"GRANT EXECUTE ON FUNCTION {SIGNATURE} TO vp_worker_operator_runtime"
                )
                execute_added = True
            case = Case(owner, *connections, request, registrations, grants, roles)
            yield case
        finally:
            for connection in connections:
                await connection.close(timeout=2)
            if seeded:
                await owner.execute(
                    "DELETE FROM public.worker_registrations WHERE id=ANY($1::uuid[]) AND superseded_by IS NOT NULL",
                    registration_ids,
                )
                await owner.execute(
                    "DELETE FROM public.worker_registrations WHERE id=ANY($1::uuid[])",
                    registration_ids,
                )
                await owner.execute(
                    "DELETE FROM public.worker_admission_grants WHERE id=ANY($1::uuid[])",
                    grant_ids,
                )
            if schedule_created:
                await owner.execute(
                    "DELETE FROM public.runtime_schedules WHERE service_name='videoprocess' AND updated_by='registered-reconcile-test'"
                )
            for role in reversed(roles):
                await owner.execute(f"DROP ROLE {quote_identifier(role)}")
            if execute_added:
                await owner.execute(
                    f"REVOKE EXECUTE ON FUNCTION {SIGNATURE} FROM vp_worker_operator_runtime"
                )
            for role in reversed(stable_created):
                await owner.execute(f"DROP ROLE {quote_identifier(role)}")
            await owner.close(timeout=2)

    return opened


@pytest.mark.asyncio
async def test_actual_operator_guard_native_facts_and_no_direct_table_authority(
    postgres_case,
):
    async with postgres_case() as case:
        before = await case.snapshot()
        assert (
            await case.operator.fetchval("SELECT session_user")
            == role_names_for_generation(case.request.control_generation).versioned[
                "operator"
            ]
        )
        for table in (
            "worker_registrations",
            "worker_admission_grants",
            "runtime_schedules",
        ):
            column = "service_name" if table == "runtime_schedules" else "id"
            for suffix in ("", " FOR SHARE"):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await case.operator.fetch(
                        f"SELECT {column} FROM public.{table}" + suffix
                    )
        function = await case.owner.fetchrow(
            "SELECT prosecdef,proconfig FROM pg_catalog.pg_proc WHERE oid=$1::regprocedure",
            SIGNATURE,
        )
        assert function["prosecdef"] is True and function["proconfig"] == [
            "search_path=pg_catalog"
        ]
        assert not await case.owner.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_proc p, LATERAL pg_catalog.aclexplode(p.proacl) acl WHERE p.oid=$1::regprocedure AND acl.grantee=0)",
            SIGNATURE,
        )
        async with case.operator.transaction():
            rows = await case.operator.fetch(CALL, *case.arguments())
            assert len(rows) == 8 and isinstance(rows[0]["registration_id"], UUID)
            state = runtime.decode_guard(rows, case.request.pins)
            assert len(state.registrations) == len(state.grants) == 8
            assert "token_sha256" not in str(rows) and "lease_secret_sha256" not in str(
                rows
            )
        for table in (
            "worker_registrations",
            "worker_admission_grants",
            "runtime_schedules",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await case.operator.execute(f"DELETE FROM public.{table} WHERE false")
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                column = "state" if table != "worker_registrations" else "status"
                await case.operator.execute(
                    f"UPDATE public.{table} SET {column}={column} WHERE false"
                )
        assert await case.snapshot() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("caller", ["owner", "runtime_worker", "watcher"])
async def test_actual_nonoperator_logins_are_refused(postgres_case, caller):
    async with postgres_case() as case:
        before = await case.snapshot()
        with pytest.raises(asyncpg.PostgresError):
            await getattr(case, caller).fetch(CALL, *case.arguments())
        assert await case.snapshot() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ["schedule", "registration", "grant"])
async def test_guard_locks_contend_and_rollback_releases(postgres_case, target):
    async with postgres_case() as case:
        transaction = case.operator.transaction()
        await transaction.start()
        before = await case.snapshot()
        try:
            await runtime.read_guard(case.operator, case.request)
            statements = {
                "schedule": (
                    "UPDATE public.runtime_schedules SET state=state WHERE service_name='videoprocess'",
                    (),
                ),
                "registration": (
                    "UPDATE public.worker_registrations SET lease_expires_at=lease_expires_at WHERE id=$1",
                    (case.registrations[0].id,),
                ),
                "grant": (
                    "UPDATE public.worker_admission_grants SET state=state WHERE id=$1",
                    (case.grants[0].id,),
                ),
            }
            sql, args = statements[target]
            async with case.owner.transaction():
                await case.owner.execute("SET LOCAL lock_timeout='100ms'")
                with pytest.raises(asyncpg.LockNotAvailableError):
                    async with case.owner.transaction():
                        await case.owner.execute(sql, *args)
        finally:
            await transaction.rollback()
        async with case.owner.transaction():
            await case.owner.execute("SET LOCAL lock_timeout='100ms'")
            assert await case.owner.execute(sql, *args) == "UPDATE 1"
        assert await case.snapshot() == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "generation",
        "missing",
        "duplicate",
        "schedule",
        "current_lease",
        "grant_binding",
        "supersession",
        "active_job",
    ],
)
async def test_changed_authority_or_work_refuses_without_guard_mutation(
    postgres_case, change
):
    async with postgres_case() as case:
        args = list(case.arguments())
        job_id, pipeline_id = uuid4(), uuid4()
        try:
            if change == "generation":
                args[0] += "-changed"
            elif change == "missing":
                args[1][0] = uuid4()
            elif change == "duplicate":
                args[2][0] = args[1][0]
            elif change == "schedule":
                await case.owner.execute(
                    "UPDATE public.runtime_schedules SET state='OPEN' WHERE service_name='videoprocess'"
                )
            elif change == "current_lease":
                await case.owner.execute(
                    "UPDATE public.worker_registrations SET lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id=$1",
                    args[1][0],
                )
            elif change == "grant_binding":
                await case.owner.execute(
                    "UPDATE public.worker_admission_grants SET worker_host='changed-host' WHERE id=$1",
                    case.grants[0].id,
                )
            elif change == "supersession":
                await case.owner.execute(
                    "UPDATE public.worker_registrations SET superseded_by=$2 WHERE id=$1",
                    args[2][0],
                    args[1][1],
                )
            else:
                # A real schema-valid unowned RUNNING job is active work too.
                async with case.owner.transaction():
                    await case.owner.execute(
                        "INSERT INTO public.pipelines(id,name,description,definition,is_template,template_tags,created_by,version) VALUES($1,'rcr-test','','{}'::jsonb,false,'{}','rcr-test',1)",
                        pipeline_id,
                    )
                    await case.owner.execute(
                        "INSERT INTO public.jobs(id,pipeline_id,status,pipeline_snapshot,submitted_by,retry_count,orchestrator_owner) VALUES($1,$2,'RUNNING','{}'::jsonb,'rcr-test',0,'python')",
                        job_id,
                        pipeline_id,
                    )
            before = await case.snapshot()
            with pytest.raises((asyncpg.PostgresError, runtime.ReconcileRuntimeError)):
                async with case.operator.transaction():
                    runtime.decode_guard(
                        await case.operator.fetch(CALL, *args), case.request.pins
                    )
            assert await case.snapshot() == before
        finally:
            if change in {
                "schedule",
                "current_lease",
                "grant_binding",
                "supersession",
                "active_job",
            }:
                # Restore only this test's exact state, never a service operation.
                if change == "schedule":
                    await case.owner.execute(
                        "UPDATE public.runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'"
                    )
                elif change == "active_job":
                    await case.owner.execute(
                        "DELETE FROM public.jobs WHERE id=$1", job_id
                    )
                    await case.owner.execute(
                        "DELETE FROM public.pipelines WHERE id=$1", pipeline_id
                    )
                else:
                    row = (
                        case.grants[0]
                        if change == "grant_binding"
                        else next(
                            row
                            for row in case.registrations
                            if row.id
                            == (args[2][0] if change == "supersession" else args[1][0])
                        )
                    )
                    column = {
                        "current_lease": "lease_expires_at",
                        "grant_binding": "worker_host",
                        "supersession": "superseded_by",
                    }[change]
                    await case.owner.execute(
                        f"UPDATE public.{row.__table__.name} SET {column}=$2 WHERE id=$1",
                        row.id,
                        getattr(row, column),
                    )


@pytest.mark.asyncio
async def test_assumable_role_admin_authority_is_refused(postgres_case):
    async with postgres_case() as case:
        parent = "rcr_unsafe_" + uuid4().hex[:20]
        operator = case.created_roles[0]
        await case.owner.execute(
            f"CREATE ROLE {quote_identifier(parent)} NOLOGIN CREATEROLE"
        )
        try:
            await case.owner.execute(
                f"GRANT {quote_identifier(parent)} TO {quote_identifier(operator)} WITH SET TRUE"
            )
            with pytest.raises(asyncpg.PostgresError):
                await case.operator.fetch(CALL, *case.arguments())
        finally:
            await case.owner.execute(
                f"REVOKE {quote_identifier(parent)} FROM {quote_identifier(operator)}"
            )
            await case.owner.execute(f"DROP ROLE {quote_identifier(parent)}")
