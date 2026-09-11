"""Opt-in real PG tests. Reuse the exact consumer fixture's disposable contract.

No database provisioning, Redis, worker or Manager calls. Synthetic terminal
rows are inserted by the fixture owner; the proof is called by the real operator.
"""

from __future__ import annotations

import copy
import asyncio
import json
import os
import re
import runpy
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest

from app.models.base import Base
from app.services.registered_worker_event_receipt import canonical_redis_payload_sha256
from tests.migrations.test_registered_consumer_reconcile_postgres import (
    CALL,
    SIGNATURE,
    checked_url,
    postgres_case as _postgres_case,
)
from app.services.worker_role_cli_common import (
    create_login_role,
    quote_identifier,
    role_database_url,
)
from tests.services.test_owned_seed_inventory_history import (
    NOW,
    receipt_authorized_ack_rows,
)


HELPER = "public.vp_registered_consumer_uploads_quiescent()"
postgres_case = _postgres_case
TABLES = (
    "channel_profiles",
    "publishing_accounts",
    "assets",
    "manual_seeds",
    "pipelines",
    "jobs",
    "production_tasks",
    "node_executions",
    "artifacts",
    "youtube_upload_operations",
    "worker_task_delivery_attestations",
    "worker_event_emissions",
    "registered_worker_event_receipts",
    "registered_worker_event_deliveries",
    "worker_task_dispatches",
    "channel_ops_queue_items",
    "publication_records",
    "publication_promotion_operations",
    "legacy_worker_event_resolutions",
    "worker_redis_marker_cleanup_authorizations",
    "worker_redis_marker_repair_audits",
)
UUID_RE = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")


def terminal_rows(case, now):
    rows, _ = receipt_authorized_ack_rows()
    rows = {table: rows.get(table, []) for table in TABLES}
    replacements = {value: str(uuid4()) for value in UUID_RE.findall(json.dumps(rows))}
    shift = now - NOW + timedelta(days=4, minutes=-30)

    def remap(value):
        if isinstance(value, dict):
            return {key: remap(item) for key, item in value.items()}
        if isinstance(value, list):
            return [remap(item) for item in value]
        if isinstance(value, str):
            if value.startswith("2026-"):
                return (datetime.fromisoformat(value) + shift).isoformat()
            return UUID_RE.sub(lambda match: replacements[match[0]], value)
        return value

    rows = remap(rows)
    pipeline_id = str(uuid4())
    rows["pipelines"] = [
        {
            "id": pipeline_id,
            "name": "terminal-proof-test",
            "definition": rows["jobs"][0]["pipeline_snapshot"],
        }
    ]
    rows["jobs"][0]["pipeline_id"] = pipeline_id
    rows["channel_profiles"][0]["name"] = "terminal-proof-test"
    rows["publishing_accounts"][0]["account_label"] = "terminal-proof-test"
    rows["production_tasks"][0]["prompt"] = "terminal-proof-test"
    rows["manual_seeds"][0]["prompt"] = "terminal-proof-test"
    for att in rows["worker_task_delivery_attestations"]:
        stream = att["redis_stream"]
        grant = next(
            g for g in case.grants if g.redis_stream == stream and g.state == "active"
        )
        registration = next(r for r in case.registrations if r.grant_id == grant.id)
        claim = {
            "worker_registration_id": str(registration.id),
            "worker_lease_epoch": registration.lease_epoch,
            "worker_id": registration.redis_consumer_id,
        }
        att.update(claim)
        node = next(
            n for n in rows["node_executions"] if n["id"] == att["node_execution_id"]
        )
        node.update(claim)
        if node["status"] == "CANCELLED":
            node["worker_id"] = None
        for table in ("worker_event_emissions", "registered_worker_event_receipts"):
            row = next(
                r for r in rows[table] if r["source_task_attestation_id"] == att["id"]
            )
            row.update(claim)
            row["payload_json"].update(
                {key: str(value) for key, value in claim.items()}
            )
    for dispatch in rows["worker_task_dispatches"]:
        dispatch["payload_sha256"] = canonical_redis_payload_sha256(
            dispatch["payload_json"]
        )
        att = next(
            (
                a
                for a in rows["worker_task_delivery_attestations"]
                if a["dispatch_key"] == dispatch["dispatch_key"]
            ),
            None,
        )
        if att:
            att["payload_sha256"] = dispatch["payload_sha256"]
            for table in ("worker_event_emissions", "registered_worker_event_receipts"):
                row = next(
                    r
                    for r in rows[table]
                    if r["source_task_attestation_id"] == att["id"]
                )
                row["payload_json"]["task_payload_sha256"] = dispatch["payload_sha256"]
                row["payload_sha256"] = canonical_redis_payload_sha256(
                    row["payload_json"]
                )
            receipt = next(
                r
                for r in rows["registered_worker_event_receipts"]
                if r["source_task_attestation_id"] == att["id"]
            )
            for delivery in rows["registered_worker_event_deliveries"]:
                if delivery["source_task_attestation_id"] == att["id"]:
                    delivery["payload_sha256"] = receipt["payload_sha256"]
        if dispatch["origin_receipt_id"] is not None:
            origin = next(
                r
                for r in rows["registered_worker_event_receipts"]
                if r["id"] == dispatch["origin_receipt_id"]
            )
            dispatch["created_at"] = origin["accepted_at"]
    return rows


def with_defaults(table, row):
    result = copy.deepcopy(row)
    if table == "artifacts" and result.get("kind") == "intermediate":
        result["kind"] = "INTERMEDIATE"
    for column in Base.metadata.tables[table].columns:
        if result.get(column.name) is None and not column.nullable:
            if column.default is not None:
                default = column.default
                result[column.name] = (
                    default.arg(None) if default.is_callable else default.arg
                )
            elif column.server_default is not None:
                result.pop(column.name, None)
    return result


async def insert_rows(connection, rows):
    async with connection.transaction():
        for table in TABLES:
            for raw in rows[table]:
                row = with_defaults(table, raw)
                assert set(row) <= set(Base.metadata.tables[table].columns.keys())
                columns = ",".join(f'"{name}"' for name in row)
                await connection.execute(
                    f"INSERT INTO public.{table} ({columns}) SELECT {columns} FROM "
                    f"pg_catalog.jsonb_populate_record(NULL::public.{table}, $1::jsonb)",
                    json.dumps(
                        row,
                        default=lambda x: (
                            str(x) if isinstance(x, UUID) else x.isoformat()
                        ),
                    ),
                )


async def delete_rows(connection, rows):
    # Only these random synthetic IDs. No TRUNCATE, trigger disabling or runtime repair.
    async with connection.transaction():
        for table in reversed(TABLES):
            if rows[table]:
                await connection.execute(
                    f"DELETE FROM public.{table} WHERE id=ANY($1::uuid[])",
                    [UUID(row["id"]) for row in rows[table]],
                )


async def snapshot_rows(connection, rows):
    return {
        table: await connection.fetchval(
            f"SELECT jsonb_agg(to_jsonb(r) ORDER BY id)::text FROM public.{table} r WHERE id=ANY($1::uuid[])",
            [UUID(row["id"]) for row in records],
        )
        for table, records in rows.items()
    }


@pytest.mark.asyncio
async def test_terminal_reservation_passes_without_history_or_row_mutation(
    postgres_case,
):
    async with postgres_case() as case:
        now = await case.owner.fetchval("SELECT clock_timestamp()")
        rows = terminal_rows(case, now)
        await insert_rows(case.owner, rows)
        try:
            before = await snapshot_rows(case.owner, rows)
            assert len(await case.operator.fetch(CALL, *case.arguments())) == 8
            assert await snapshot_rows(case.owner, rows) == before
            assert (
                await case.owner.fetchval("SELECT count(*) FROM owned_seed_inventories")
                == 0
            )
            for connection in (case.operator, case.runtime_worker, case.watcher):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.fetchval(f"SELECT {HELPER}")
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.fetch("SELECT * FROM youtube_upload_operations")
        finally:
            await delete_rows(case.owner, rows)


FAULTS = (
    "attempt",
    "manager",
    "video",
    "receipt",
    "completed",
    "error",
    "privacy",
    "task_orphan",
    "task_active",
    "job_active",
    "node_active",
    "unhalted",
    "unpaused",
    "retained_epoch",
    "retained_started",
    "payload_hash",
    "payload_identity",
    "ack_emission",
    "premature_ack",
    "retry_origin",
    "retry_ack_order",
    "missing_receipt",
    "missing_delivery",
    "extra_dispatch",
    "missing_node",
    "artifact_orphan",
    "account_orphan",
    "input_orphan",
    "node_wrong_job",
    "ambiguous_task",
    "runnable_queue",
    "receipt_pending",
    "publication",
    "event_payload",
    "retry_before_origin",
)


def corrupt(rows, fault, now):
    op = rows["youtube_upload_operations"][0]
    upload = next(
        n for n in rows["node_executions"] if n["id"] == op["node_execution_id"]
    )
    att = rows["worker_task_delivery_attestations"][0]
    if fault in {"attempt", "completed"}:
        op["request_attempted_at" if fault == "attempt" else "completed_at"] = (
            now.isoformat()
        )
    elif fault in {"manager", "video", "error", "privacy"}:
        key, value = {
            "manager": ("manager_task_id", str(uuid4())),
            "video": ("platform_video_id", "abcdefghijk"),
            "error": ("error_message", "uncertain"),
            "privacy": ("privacy", "public"),
        }[fault]
        op[key] = value
    elif fault == "receipt":
        op["receipt_json"] = {"unknown": True}
    elif fault == "task_orphan":
        op["production_task_id"] = None
    elif fault in {"task_active", "job_active", "node_active"}:
        row, key, value = {
            "task_active": (rows["production_tasks"][0], "state", "producing"),
            "job_active": (rows["jobs"][0], "status", "RUNNING"),
            "node_active": (upload, "status", "RUNNING"),
        }[fault]
        row[key] = value
    elif fault in {"unhalted", "unpaused"}:
        rows["channel_profiles"][0][
            "halted_at" if fault == "unhalted" else "intake_paused_at"
        ] = None
    elif fault == "retained_epoch":
        upload["worker_lease_epoch"] += 1
    elif fault == "retained_started":
        upload["started_at"] = (now - timedelta(minutes=1)).isoformat()
    elif fault == "payload_hash":
        rows["worker_task_dispatches"][0]["payload_sha256"] = "f" * 64
    elif fault == "payload_identity":
        rows["worker_task_dispatches"][0]["payload_json"]["node_id"] = "other"
    elif fault == "ack_emission":
        att["ack_event_emission_id"] = str(uuid4())
    elif fault == "premature_ack":
        when = (
            datetime.fromisoformat(
                rows["registered_worker_event_receipts"][0]["applied_at"]
            )
            - timedelta(seconds=1)
        ).isoformat()
        att["acknowledged_at"] = rows["worker_task_dispatches"][0][
            "acknowledged_at"
        ] = when
        rows["registered_worker_event_receipts"][0]["source_task_acknowledged_at"] = (
            when
        )
    elif fault in {"retry_origin", "retry_ack_order"}:
        retry = next(
            d for d in rows["worker_task_dispatches"] if d["origin_receipt_id"]
        )
        if fault == "retry_origin":
            retry["origin_receipt_id"] = rows["registered_worker_event_receipts"][0][
                "id"
            ]
        else:
            retry["acknowledged_at"] = (
                datetime.fromisoformat(rows["jobs"][0]["completed_at"])
                - timedelta(seconds=1)
            ).isoformat()
    elif fault == "missing_receipt":
        receipt = rows["registered_worker_event_receipts"].pop(0)
        rows["registered_worker_event_deliveries"] = [
            d
            for d in rows["registered_worker_event_deliveries"]
            if d["receipt_id"] != receipt["id"]
        ]
    elif fault == "missing_delivery":
        rows["registered_worker_event_deliveries"].pop(0)
    elif fault == "extra_dispatch":
        extra = copy.deepcopy(rows["worker_task_dispatches"][0])
        extra.update(
            id=str(uuid4()), dispatch_key=str(uuid4()), redis_message_id="99999-0"
        )
        extra["payload_json"]["dispatch_key"] = extra["dispatch_key"]
        extra["payload_sha256"] = canonical_redis_payload_sha256(extra["payload_json"])
        rows["worker_task_dispatches"].append(extra)
    elif fault == "missing_node":
        rows["jobs"][0]["pipeline_snapshot"]["nodes"].pop(0)
    elif fault == "artifact_orphan":
        rows["artifacts"][0]["node_execution_id"] = upload["id"]
    elif fault == "account_orphan":
        rows["production_tasks"][0]["target_account_id"] = str(uuid4())
    elif fault == "input_orphan":
        upload["input_artifact_ids"] = [str(uuid4())]
    elif fault == "node_wrong_job":
        extra = copy.deepcopy(rows["jobs"][0])
        extra["id"] = str(uuid4())
        rows["jobs"].append(extra)
        upload["job_id"] = extra["id"]
    elif fault == "ambiguous_task":
        extra = copy.deepcopy(rows["production_tasks"][0])
        extra["id"] = str(uuid4())
        rows["production_tasks"].append(extra)
    elif fault == "runnable_queue":
        rows["channel_ops_queue_items"].append(
            {
                "id": str(uuid4()),
                "kind": "execute",
                "idempotency_key": str(uuid4()),
                "channel_profile_id": rows["channel_profiles"][0]["id"],
                "payload_json": {"production_task_id": op["production_task_id"]},
                "status": "queued",
            }
        )
    elif fault == "receipt_pending":
        rows["registered_worker_event_receipts"][0].update(
            ack_state="pending", acknowledged_at=None
        )
    elif fault == "publication":
        rows["publication_records"].append(
            {
                "id": str(uuid4()),
                "production_task_id": op["production_task_id"],
                "account_id": rows["publishing_accounts"][0]["id"],
                "platform": "youtube",
                "platform_content_id": "abcdefghijk",
                "title": "terminal-proof-test",
                "desired_privacy": "unlisted",
                "current_privacy": "unlisted",
                "compliance_disposition": "owned_generated",
            }
        )
    elif fault == "event_payload":
        rows["registered_worker_event_receipts"][0]["payload_json"]["worker_id"] = (
            "foreign"
        )
    elif fault == "retry_before_origin":
        retry = next(
            d for d in rows["worker_task_dispatches"] if d["origin_receipt_id"]
        )
        origin = next(
            r
            for r in rows["registered_worker_event_receipts"]
            if r["id"] == retry["origin_receipt_id"]
        )
        retry["created_at"] = (
            datetime.fromisoformat(origin["accepted_at"]) - timedelta(seconds=1)
        ).isoformat()
    else:
        raise AssertionError(fault)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", FAULTS)
async def test_unsupported_or_drifting_terminal_proof_refuses(postgres_case, fault):
    async with postgres_case() as case:
        now = await case.owner.fetchval("SELECT clock_timestamp()")
        rows = terminal_rows(case, now)
        corrupt(rows, fault, now)
        await insert_rows(case.owner, rows)
        try:
            with pytest.raises(
                asyncpg.RaiseError, match="registered_reconcile_work_active"
            ):
                await case.operator.fetch(CALL, *case.arguments())
        finally:
            await delete_rows(case.owner, rows)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"config": '{"title": "Chinese: \\u4e2d"}', "plain": "value"},
        {"unicode": "\u4e2d\U0001f680\u007f", "control": '\t\n\r\b\f\\"'},
        {"\U0001f680": "last", "\uffff": "earlier", "a": "first"},
    ],
)
async def test_private_payload_hash_matches_existing_python_contract(
    postgres_case, payload
):
    async with postgres_case() as case:
        assert await case.owner.fetchval(
            "SELECT public.vp_registered_consumer_payload_sha256($1::jsonb)",
            json.dumps(payload),
        ) == canonical_redis_payload_sha256(payload)


@pytest.mark.asyncio
async def test_guard_definition_roundtrip_preserves_oid_acl_and_old_refusal(
    postgres_case,
):
    unit = runpy.run_path(
        str(
            Path(__file__).parents[2]
            / "alembic/versions/041_registered_consumer_terminal.py"
        )
    )
    async with postgres_case() as case:
        rows = terminal_rows(
            case, await case.owner.fetchval("SELECT clock_timestamp()")
        )
        await insert_rows(case.owner, rows)
        info = await case.owner.fetchrow(
            "SELECT oid,prosrc,pg_get_functiondef(oid) AS definition,proacl::text AS acl FROM pg_proc WHERE oid=$1::regprocedure",
            SIGNATURE,
        )
        try:
            await case.owner.execute(unit["_replacement"](downgrade=True))
            with pytest.raises(
                asyncpg.RaiseError, match="registered_reconcile_work_active"
            ):
                await case.operator.fetch(CALL, *case.arguments())
            await case.owner.execute(unit["_replacement"]())
            assert len(await case.operator.fetch(CALL, *case.arguments())) == 8
            actual = await case.owner.fetchrow(
                "SELECT oid,proacl::text AS acl FROM pg_proc WHERE oid=$1::regprocedure",
                SIGNATURE,
            )
            assert (actual["oid"], actual["acl"]) == (info["oid"], info["acl"])
        finally:
            await case.owner.execute(info["definition"])
            await delete_rows(case.owner, rows)


@pytest.mark.asyncio
async def test_evidence_row_sentinel_and_overall_bytes_refuse_without_truncation(
    postgres_case,
):
    async with postgres_case() as case:
        rows = terminal_rows(
            case, await case.owner.fetchval("SELECT clock_timestamp()")
        )
        await insert_rows(case.owner, rows)
        extra = [uuid4() for _ in range(4096)]
        try:
            await case.owner.execute(
                """INSERT INTO public.assets(id,filename,original_name,storage_backend,storage_path,uploaded_by)
                SELECT id,'bound','bound','local','test-only','test-only' FROM unnest($1::uuid[]) id""",
                extra[:-1],
            )
            assert len(await case.operator.fetch(CALL, *case.arguments())) == 8
            await case.owner.execute(
                "INSERT INTO public.assets(id,filename,original_name,storage_backend,storage_path,uploaded_by) VALUES($1,'bound','bound','local','test-only','test-only')",
                extra[-1],
            )
            with pytest.raises(
                asyncpg.RaiseError, match="registered_reconcile_work_active"
            ):
                await case.operator.fetch(CALL, *case.arguments())
            await case.owner.execute(
                "DELETE FROM public.assets WHERE id=ANY($1::uuid[])", extra
            )
            await case.owner.execute(
                "UPDATE public.assets SET media_info=jsonb_build_object('oversized',repeat('x',16777216)) WHERE id=$1",
                UUID(rows["assets"][0]["id"]),
            )
            with pytest.raises(
                asyncpg.RaiseError, match="registered_reconcile_work_active"
            ):
                await case.operator.fetch(CALL, *case.arguments())
        finally:
            await case.owner.execute(
                "DELETE FROM public.assets WHERE id=ANY($1::uuid[])", extra
            )
            await delete_rows(case.owner, rows)


@asynccontextmanager
async def publisher_connection(case):
    registration = next(
        r
        for r in case.registrations
        if r.worker_type == "youtube_publisher" and r.status == "active"
    )
    role, password = registration.database_principal, uuid4().hex + uuid4().hex
    signatures = (
        "public.vp_claim_worker_node(uuid,bigint,text,uuid,uuid,text,text,text,text,uuid)",
        "public.vp_transition_worker_youtube_upload(uuid,bigint,text,timestamptz,uuid,text,text,text,text,jsonb,text)",
    )
    url = checked_url(
        os.environ["REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_URL"],
        os.environ.get("REGISTERED_RECONCILE_DISPOSABLE_POSTGRES_CONFIRM"),
    )
    async with case.owner.transaction():
        await create_login_role(
            case.owner,
            role,
            password,
            setting_prefix="terminal_test",
            stable_role="vp_worker_runtime",
        )
    connection = None
    try:
        for signature in signatures:
            await case.owner.execute(
                f"GRANT EXECUTE ON FUNCTION {signature} TO {quote_identifier(role)}"
            )
        connection = await asyncpg.connect(
            role_database_url(url, role, password), timeout=2, command_timeout=2
        )
        yield connection, registration
    finally:
        if connection is not None:
            await connection.close(timeout=2)
        for signature in signatures:
            await case.owner.execute(
                f"REVOKE EXECUTE ON FUNCTION {signature} FROM {quote_identifier(role)}"
            )
        await case.owner.execute(f"DROP ROLE {quote_identifier(role)}")


async def observed_blocker(owner, blocked_pid, blocker_pid):
    async with asyncio.timeout(1):
        while blocker_pid not in await owner.fetchval(
            "SELECT pg_blocking_pids($1)", blocked_pid
        ):
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_schedule_reopen_waits_for_guard_and_new_attempt_is_seen_fresh(
    postgres_case,
):
    async with postgres_case() as case:
        rows = terminal_rows(
            case, await case.owner.fetchval("SELECT clock_timestamp()")
        )
        await insert_rows(case.owner, rows)
        guard = case.operator.transaction()
        await guard.start()
        reopening = None
        try:
            assert len(await case.operator.fetch(CALL, *case.arguments())) == 8
            operator_pid = await case.operator.fetchval("SELECT pg_backend_pid()")
            owner_pid = await case.owner.fetchval("SELECT pg_backend_pid()")
            # The watcher has no table rights; use the existing worker connection
            # only to observe pg_blocking_pids while the fixture owner attempts OPEN.
            reopening = asyncio.create_task(
                case.owner.execute(
                    "UPDATE runtime_schedules SET state='OPEN' WHERE service_name='videoprocess'"
                )
            )
            await observed_blocker(case.watcher, owner_pid, operator_pid)
            assert not reopening.done()
            await guard.rollback()
            await reopening
            with pytest.raises(
                asyncpg.RaiseError, match="registered_reconcile_schedule_unsafe"
            ):
                await case.operator.fetch(CALL, *case.arguments())
            await case.owner.execute(
                "UPDATE runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'"
            )
            await case.owner.execute(
                "UPDATE youtube_upload_operations SET request_attempted_at=clock_timestamp() WHERE id=$1",
                UUID(rows["youtube_upload_operations"][0]["id"]),
            )
            with pytest.raises(
                asyncpg.RaiseError, match="registered_reconcile_work_active"
            ):
                await case.operator.fetch(CALL, *case.arguments())
        finally:
            if reopening is not None and not reopening.done():
                reopening.cancel()
                await asyncio.gather(reopening, return_exceptions=True)
            if case.operator.is_in_transaction():
                await guard.rollback()
            await case.owner.execute(
                "UPDATE runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'"
            )
            await delete_rows(case.owner, rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["claim", "attempting", "fence"])
async def test_native_publisher_authority_holds_schedule_before_reconcile(
    postgres_case, phase
):
    async with (
        postgres_case() as case,
        publisher_connection(case) as (worker, registration),
    ):
        now = await case.owner.fetchval("SELECT clock_timestamp()")
        rows = terminal_rows(case, now)
        # An independent non-ChannelOps RUNNING job exercises the existing writer,
        # not a resurrection of the terminal reservation being qualified.
        for table in TABLES:
            if table not in {
                "jobs",
                "pipelines",
                "node_executions",
                "artifacts",
                "youtube_upload_operations",
                "worker_task_dispatches",
            }:
                rows[table] = []
        job, operation = rows["jobs"][0], rows["youtube_upload_operations"][0]
        upload = next(
            n
            for n in rows["node_executions"]
            if n["id"] == operation["node_execution_id"]
        )
        source = next(
            n
            for n in rows["node_executions"]
            if n["id"] == rows["artifacts"][1]["node_execution_id"]
        )
        source.update(
            worker_registration_id=None, worker_lease_epoch=None, worker_id=None
        )
        rows["node_executions"] = [source, upload]
        rows["artifacts"] = [rows["artifacts"][1]]
        source["input_artifact_ids"] = []
        job.update(status="RUNNING", completed_at=None, error_message=None)
        operation["production_task_id"] = None
        upload.update(
            status="QUEUED" if phase == "claim" else "RUNNING",
            completed_at=None,
            error_message=None,
            worker_registration_id=None if phase == "claim" else str(registration.id),
            worker_lease_epoch=None if phase == "claim" else registration.lease_epoch,
            worker_id=None if phase == "claim" else registration.redis_consumer_id,
            started_at=None if phase == "claim" else now.isoformat(),
        )
        dispatch = next(
            d
            for d in rows["worker_task_dispatches"]
            if d["node_execution_id"] == upload["id"] and d["origin_receipt_id"] is None
        )
        dispatch.update(resolution_state="unresolved", acknowledged_at=None)
        rows["worker_task_dispatches"] = [dispatch]
        if phase == "fence":
            operation["request_attempted_at"] = now.isoformat()
        await insert_rows(case.owner, rows)
        pending = None
        transaction = worker.transaction()
        try:
            await case.owner.execute(
                "UPDATE runtime_schedules SET state='OPEN' WHERE service_name='videoprocess'"
            )
            await transaction.start()
            if phase == "claim":
                await worker.fetch(
                    "SELECT * FROM public.vp_claim_worker_node($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    registration.id,
                    registration.lease_epoch,
                    registration.redis_consumer_id,
                    UUID(job["id"]),
                    UUID(upload["id"]),
                    dispatch["redis_stream"],
                    dispatch["consumer_group"],
                    dispatch["redis_message_id"],
                    dispatch["payload_sha256"],
                    UUID(dispatch["dispatch_key"]),
                )
            else:
                await worker.fetchval(
                    "SELECT public.vp_transition_worker_youtube_upload($1,$2,$3,$4,$5,'reserved',$6,NULL,NULL,NULL,NULL)",
                    registration.id,
                    registration.lease_epoch,
                    registration.redis_consumer_id,
                    now,
                    UUID(operation["id"]),
                    phase,
                )
            blocked_pid = await case.operator.fetchval("SELECT pg_backend_pid()")
            blocker_pid = await worker.fetchval("SELECT pg_backend_pid()")
            pending = asyncio.create_task(case.operator.fetch(CALL, *case.arguments()))
            await observed_blocker(case.owner, blocked_pid, blocker_pid)
            assert not pending.done()
            await transaction.rollback()
            with pytest.raises(
                asyncpg.RaiseError, match="registered_reconcile_schedule_unsafe"
            ):
                await pending
        finally:
            if pending is not None and not pending.done():
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
            if worker.is_in_transaction():
                await transaction.rollback()
            await case.owner.execute(
                "UPDATE runtime_schedules SET state='CLOSED' WHERE service_name='videoprocess'"
            )
            await delete_rows(case.owner, rows)
