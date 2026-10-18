"""Opt-in PG16 checks of retained legacy history without platform or Redis I/O.

Each module creates and drops only its own random child database. The supplied
confirmed loopback URL is an administrative scratch anchor, never a data target.
"""
from __future__ import annotations

import asyncio
import copy
import enum
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from app.services import owned_seed_inventory_history as history
from app.models.base import Base
from tests.migrations.owned_history_postgres import dsn


FIXTURE = Path(__file__).parents[1] / "fixtures/owned_seed_inventory_retention_gap.json"
HEAD = "046_owned_retained_terminal"
PREVIOUS = "045_registered_consumer_history"
CALL = "SELECT public.vp_owned_producer_history($1::json,$2::uuid,$3::text,$4::timestamptz)"


async def migrate(url, target, direction="upgrade"):
    child = await asyncio.create_subprocess_exec(sys.executable, "-m", "alembic", direction, target,
        cwd=Path(__file__).parents[2], env={**os.environ, "DATABASE_URL": url.render_as_string(hide_password=False)},
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        output, error = await asyncio.wait_for(child.communicate(), 120)
        assert child.returncode == 0, "\n".join(line for line in error.decode().splitlines() if "Error" in line or "EXCEPTION" in line)[:2500]
    finally:
        if child.returncode is None:
            child.kill()
            await child.wait()


def disposable_target(environment):
    raw = environment.get("OWNED_RETAINED_POSTGRES_TEST_URL", "")
    if not raw:
        ci_url = environment.get("CHANNEL_OPS_POSTGRES_TEST_URL", "")
        if not ci_url:
            return None
        if (environment.get("GITHUB_ACTIONS") != "true"
                or ci_url != "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres"):
            raise ValueError("explicit disposable retained-history database required")
        return make_url(ci_url), None
    url = make_url(raw)
    system = environment.get("OWNED_RETAINED_POSTGRES_SYSTEM_ID", "")
    if not (url.drivername == "postgresql+asyncpg" and url.host in {"127.0.0.1", "::1"}
            and url.port is not None and 1024 <= url.port <= 65535 and url.port != 5432
            and url.username and url.password and not url.query
            and re.fullmatch(r"vp_(?:retention|owned_retained)_test_[a-z0-9_]+", url.database or "")
            and environment.get("OWNED_RETAINED_POSTGRES_TEST_CONFIRM") == url.database
            and re.fullmatch(r"[0-9]{10,20}", system)):
        raise ValueError("explicit disposable database/cluster confirmation required")
    return url, system


@pytest.mark.parametrize("actions,ci_url,accepted", [
    ("true", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres", True),
    ("", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres", False),
    ("true", "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres", False),
    ("true", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/production", False),
    ("true", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres?host=elsewhere", False),
])
def test_disposable_target_only_accepts_repository_ci_service(actions, ci_url, accepted):
    values = {"GITHUB_ACTIONS": actions, "CHANNEL_OPS_POSTGRES_TEST_URL": ci_url}
    if accepted:
        url, system = disposable_target(values)
        assert url.database == "postgres" and system is None
    else:
        with pytest.raises(ValueError, match="^explicit disposable"):
            disposable_target(values)


def test_disposable_target_never_uses_ambient_database_url():
    assert disposable_target({"DATABASE_URL": "postgresql://owner@production/db"}) is None


def test_explicit_target_precedes_ci_and_requires_cluster_confirmation():
    environment = {"GITHUB_ACTIONS": "true", "CHANNEL_OPS_POSTGRES_TEST_URL":
        "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres",
        "OWNED_RETAINED_POSTGRES_TEST_URL":
        "postgresql+asyncpg://fixture:fixture@127.0.0.1:25446/vp_retention_test_example",
        "OWNED_RETAINED_POSTGRES_TEST_CONFIRM": "vp_retention_test_example",
        "OWNED_RETAINED_POSTGRES_SYSTEM_ID": "7697814515776643111"}
    url, system = disposable_target(environment)
    assert url.port == 25446 and system == "7697814515776643111"
    for key in ("OWNED_RETAINED_POSTGRES_TEST_CONFIRM", "OWNED_RETAINED_POSTGRES_SYSTEM_ID"):
        with pytest.raises(ValueError, match="^explicit disposable"):
            disposable_target({**environment, key: ""})


@pytest.fixture(scope="module")
def retained_database():
    selected = disposable_target(os.environ)
    if selected is None:
        pytest.skip("explicit retained-history disposable PostgreSQL qualification")
    url, system = selected
    target = url.set(database="vp_owned_retained_test_" + uuid4().hex)
    created = False

    async def setup():
        nonlocal created
        admin = await asyncpg.connect(dsn(url), timeout=5, command_timeout=10)
        try:
            assert 160000 <= int(await admin.fetchval("SHOW server_version_num")) < 170000
            assert await admin.fetchval("SELECT current_database()") == url.database
            assert system is None or str(await admin.fetchval("SELECT system_identifier FROM pg_control_system()")) == system
            await admin.execute(f'CREATE DATABASE "{target.database}"')
            created = True
        finally:
            await admin.close()
        await migrate(target, "head")

    async def cleanup():
        if created:
            admin = await asyncpg.connect(dsn(url), timeout=5, command_timeout=10)
            try:
                await admin.execute(f'DROP DATABASE "{target.database}"')
            finally:
                await admin.close()

    try:
        asyncio.run(setup())
        yield target
    finally:
        asyncio.run(cleanup())


@pytest.fixture
async def retained_pg(retained_database):
    connection = await asyncpg.connect(dsn(retained_database), timeout=5, command_timeout=15)
    transaction = connection.transaction()
    await transaction.start()
    try:
        yield SimpleNamespace(owner=connection, url=retained_database)
    finally:
        await transaction.rollback()
        await connection.close()


def document():
    return json.loads(FIXTURE.read_text())


async def assess_sql(db, fixture):
    result = await db.fetchval(CALL, json.dumps(fixture["rows"]), UUID(int=999),
                               fixture["platform_channel_id"], datetime.fromisoformat(fixture["now"]))
    return json.loads(result)


@pytest.mark.asyncio
async def test_pg_approved_retention_gap_accepts_truthful_terminal_facts(retained_pg):
    fixture = document()
    before = copy.deepcopy(fixture)
    result = await assess_sql(retained_pg.owner, fixture)
    assert fixture == before
    assert list(result["bindings"]) == ["00000000-0000-0000-0000-000000000003"]
    assert result["retired"] is None


@pytest.mark.asyncio
async def test_pg_queue_gap_without_certificate_stays_blocked(retained_pg):
    fixture = document()
    inventory = fixture["rows"]["owned_seed_inventories"][0]
    legacy = inventory["manifest_json"]["legacy_history"]
    legacy.pop("retained_terminal_uploads")
    legacy["version"] = 1
    inventory["manifest_sha256"] = history.history_sha256(inventory["manifest_json"])
    with pytest.raises(asyncpg.RaiseError, match="owned_inventory_"):
        await assess_sql(retained_pg.owner, fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["empty", "duplicate", "string_version", "foreign_manifest"])
async def test_pg_certificate_envelope_stays_bound_to_its_own_manifest(retained_pg, mutation):
    fixture = document()
    inventory = fixture["rows"]["owned_seed_inventories"][0]
    legacy = inventory["manifest_json"]["legacy_history"]
    if mutation == "empty":
        legacy["retained_terminal_uploads"] = []
        # Remove old operations too, so only the malformed envelope can fail.
        fixture["rows"]["production_tasks"] = []
        fixture["rows"]["youtube_upload_operations"] = []
        legacy["bindings"] = []
    elif mutation == "duplicate":
        legacy["retained_terminal_uploads"] *= 2
    elif mutation == "string_version":
        legacy["version"] = "2"
    else:
        sibling = copy.deepcopy(inventory)
        sibling["id"] = str(UUID(int=998))
        sibling["manifest_json"]["inventory_id"] = sibling["id"]
        sibling["manifest_json"]["legacy_history"].pop("retained_terminal_uploads")
        sibling["manifest_json"]["legacy_history"]["version"] = 1
        sibling["manifest_sha256"] = history.history_sha256(sibling["manifest_json"])
        fixture["rows"]["owned_seed_inventories"].append(sibling)
        legacy["bindings"] = []
    inventory["manifest_sha256"] = history.history_sha256(inventory["manifest_json"])
    with pytest.raises(asyncpg.RaiseError, match="owned_history_retained_terminal_invalid"):
        await assess_sql(retained_pg.owner, fixture)


CASES = json.loads(FIXTURE.with_name("owned_seed_inventory_retention_gap_cases.json").read_text())


def reseal(fixture):
    rows = fixture["rows"]
    inventory = rows["owned_seed_inventories"][0]
    cert = inventory["manifest_json"]["legacy_history"]["retained_terminal_uploads"][0]
    facts = copy.deepcopy(history._task_history(rows, rows["production_tasks"][0]))
    facts["promotion_operations"] = copy.deepcopy(rows["publication_promotion_operations"])
    for value in facts.values():
        if isinstance(value, list):
            value.sort(key=lambda row: row["id"])
    cert["retained_facts"] = facts
    cert["retained_facts_sha256"] = history.history_sha256(facts)
    qualification = inventory["manifest_json"]["legacy_history"]["bindings"][0]["qualification"]
    qualification["sanitized_facts"][0]["operation_sha256"] = history.history_sha256(rows["youtube_upload_operations"][0])
    qualification["facts_sha256"] = history.history_sha256(qualification["sanitized_facts"])
    inventory["manifest_sha256"] = history.history_sha256(inventory["manifest_json"])


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
async def test_pg_invalid_outcomes_remain_blocked_with_matching_certificate_hash(retained_pg, case):
    fixture = document()
    for change in case["changes"]:
        target = fixture
        for key in change["path"][:-1]:
            target = target[key]
        target[change["path"][-1]] = change["value"]
    reseal(fixture)
    with pytest.raises(asyncpg.RaiseError, match="owned_(?:history|inventory)_"):
        await assess_sql(retained_pg.owner, fixture)


async def insert_model(db, table, supplied):
    """Insert complete synthetic model rows; constraints and triggers stay on."""
    values = copy.deepcopy(supplied)
    for column in Base.metadata.tables[table].columns:
        if column.name in values or column.server_default is not None:
            continue
        if column.default is not None:
            value = column.default.arg(None) if column.default.is_callable else column.default.arg
            values[column.name] = value
        elif column.nullable:
            values[column.name] = None
    def encode(value):
        if isinstance(value, enum.Enum):
            return value.name
        if isinstance(value, (UUID, datetime)):
            return str(value)
        raise TypeError(type(value))
    columns = ",".join(values)
    await db.execute(f"INSERT INTO public.{table} ({columns}) SELECT {columns} FROM jsonb_populate_record(NULL::public.{table},$1::jsonb)", json.dumps(values, default=encode))


async def persist_document(db, *, occupied=False):
    """Populate genuine constraints, then certify the persisted complete rows."""
    fixture = document()
    delta = (await db.fetchval("SELECT date_trunc('second',clock_timestamp())")) - datetime.fromisoformat(fixture["now"])
    def shift(value):
        if isinstance(value, dict):
            return {key: shift(item) for key,item in value.items()}
        if isinstance(value, list):
            return [shift(item) for item in value]
        if isinstance(value, str):
            return re.sub(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:\+00:00|Z)",
                lambda match: (datetime.fromisoformat(match[0])+delta).isoformat().replace("+00:00", "Z")
                if match[0].endswith("Z") else (datetime.fromisoformat(match[0])+delta).isoformat(), value)
        return value
    fixture = shift(fixture)
    rows = fixture["rows"]
    manifest = rows["owned_seed_inventories"][0]["manifest_json"]
    input_artifact = rows["artifacts"][0]
    rows["node_executions"].append({**rows["node_executions"][0],
        "id": input_artifact["node_execution_id"], "node_id": "render", "node_type": "transcode",
        "input_artifact_ids": [], "output_artifact_id": input_artifact["id"]})
    pipeline = str(uuid4())
    await insert_model(db, "pipelines", {"id": pipeline, "name": "retained synthetic", "definition": {}})
    overrides = {
        "channel_profiles": {"name": "legacy stopped"},
        "publishing_accounts": {"account_label": "legacy synthetic"},
        "jobs": {"pipeline_id": pipeline, "pipeline_snapshot": {}},
        "artifacts": {"filename": "synthetic.mp4", "storage_path": "synthetic/only.mp4"},
        "production_tasks": {"prompt": "synthetic owned test"},
        "publication_records": {"compliance_disposition": "owned_generated"},
    }
    for table in ("channel_profiles", "publishing_accounts", "jobs", "node_executions", "artifacts", "production_tasks", "youtube_upload_operations", "publication_records", "publication_metric_schedules", "feedback_snapshots", "channel_ops_queue_items", "publication_promotion_operations"):
        for row in rows[table]:
            if table == "node_executions":
                row.setdefault("node_id", "upload")
            await insert_model(db, table, {**row, **overrides.get(table, {})})
    await insert_model(db, "channel_profiles", {"id": manifest["channel_profile_id"], "name": "new owned"})
    await insert_model(db, "publishing_accounts", {"id": manifest["target_account_id"], "channel_profile_id": manifest["channel_profile_id"], "account_label": "new owned", "platform_account_id": fixture["platform_channel_id"], "default_privacy": "unlisted"})
    await insert_model(db, "topic_lanes", {"id": manifest["topic_lane_id"], "channel_profile_id": manifest["channel_profile_id"], "name": "owned"})
    await insert_model(db, "lane_format_matrix", {"id": manifest["lane_format_id"], "topic_lane_id": manifest["topic_lane_id"], "format_key": "owned", "default_publish_visibility": "unlisted"})
    # Requalification takes fresh rows after PostgreSQL fills native columns.
    fresh = json.loads(await db.fetchval("SELECT public.vp_owned_producer_rows()"))
    inventory = rows["owned_seed_inventories"][0]
    if occupied:
        inventory.update(state="approved", succession_released_at=None)
    fresh["owned_seed_inventories"] = [inventory]
    fixture["rows"] = fresh
    binding = manifest["legacy_history"]["bindings"][0]
    account = next(a for a in fresh["publishing_accounts"] if a["id"] == binding["legacy_account_id"])
    binding["account_descriptor_sha256"] = history.history_sha256({key: account[key] for key in ("id", "channel_profile_id", "platform", "platform_account_id", "credential_ref", "platform_specific_config_json")})
    reseal(fixture)
    await insert_model(db, "owned_seed_inventories", {**inventory, "client_request_id": str(uuid4()), "topic_lane_id": manifest["topic_lane_id"], "lane_format_id": manifest["lane_format_id"], "request_sha256": "f" * 64, "starts_at": manifest["starts_at"], "expires_at": manifest["expires_at"], "created_by": "fixture"})
    return fixture


@pytest.mark.asyncio
async def test_pg_seal_preserves_retained_facts_and_allows_unrelated_cleanup(retained_pg):
    db = retained_pg.owner
    fixture = await persist_document(db)
    await assess_sql(db, fixture)
    rows = fixture["rows"]
    for table, field, value in [("publication_metric_schedules", "last_error_code", "changed"), ("feedback_snapshots", "views", 100), ("publication_promotion_operations", "status", "uncertain"), ("channel_ops_queue_items", "status", "queued")]:
        with pytest.raises(asyncpg.RaiseError, match="owned_history_sealed"):
            async with db.transaction():
                await db.execute(f"UPDATE public.{table} SET {field}=$1 WHERE id=$2", value, UUID(rows[table][0]["id"]))
        with pytest.raises(asyncpg.RaiseError, match="owned_history_sealed"):
            async with db.transaction():
                await db.execute(f"DELETE FROM public.{table} WHERE id=$1", UUID(rows[table][0]["id"]))
    unrelated = {"id": str(uuid4()), "kind": "discover", "idempotency_key": "unrelated:" + uuid4().hex, "channel_profile_id": rows["channel_profiles"][0]["id"], "status": "succeeded", "payload_json": {}, "run_after": fixture["now"]}
    # An unrelated terminal row on the legacy channel must not poison cleanup.
    await insert_model(db, "channel_ops_queue_items", unrelated)
    await db.execute("DELETE FROM channel_ops_queue_items WHERE id=$1", UUID(unrelated["id"]))
    with pytest.raises(asyncpg.RaiseError, match="owned_history_sealed"):
        async with db.transaction():
            await insert_model(db, "channel_ops_queue_items", {**unrelated, "status": "queued"})
    manifest = rows["owned_seed_inventories"][0]["manifest_json"]
    task_id, publication_id, metric_id = str(uuid4()), str(uuid4()), str(uuid4())
    await insert_model(db, "production_tasks", {"id": task_id, "channel_profile_id": manifest["channel_profile_id"], "target_account_id": manifest["target_account_id"], "prompt": "new owned task", "state": "producing"})
    await insert_model(db, "publication_records", {**rows["publication_records"][0], "id": publication_id, "production_task_id": task_id, "account_id": manifest["target_account_id"], "platform_content_id": "lmnopqrstuv"})
    await insert_model(db, "publication_metric_schedules", {**rows["publication_metric_schedules"][0], "id": metric_id, "publication_id": publication_id, "status": "pending", "attempt_count": 0, "completed_at": None, "last_attempt_at": None})
    await db.execute("UPDATE publication_metric_schedules SET status='succeeded',attempt_count=1,completed_at=due_at,last_attempt_at=due_at WHERE id=$1", UUID(metric_id))
    await insert_model(db, "feedback_snapshots", {**rows["feedback_snapshots"][0], "id": str(uuid4()), "publication_id": publication_id})


@pytest.mark.asyncio
@pytest.mark.parametrize("occupied", [False, True])
async def test_pg_native_restricted_producer_reaches_retained_history_without_private_grants(retained_database, tmp_path, occupied):
    import hashlib
    from contextlib import aclosing
    from app.services.worker_runtime_role_cli import _set_runtime_privileges
    from app.services.worker_control_role_cli import ROLE_FUNCTIONS
    from app.services.worker_role_cli_common import reset_public_privileges, grant_functions
    from tests.worker.ack_drill_postgres import ack_drill_runtime

    anchor = await asyncpg.connect(dsn(retained_database), command_timeout=15)
    target = retained_database.set(database="vp_owned_retained_test_" + uuid4().hex)
    roles = ["vp_retained_worker_" + uuid4().hex, "vp_retained_operator_" + uuid4().hex]
    password = uuid4().hex
    owner = None
    stable_created = False
    try:
        await anchor.execute(f'CREATE DATABASE "{target.database}"')
        await migrate(target, "head")
        owner = await asyncpg.connect(dsn(target), command_timeout=15)
        if not await owner.fetchval("SELECT to_regrole('vp_worker_runtime')"):
            await owner.execute("CREATE ROLE vp_worker_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS")
            stable_created = True
        for role in roles:
            await owner.execute(f'CREATE ROLE "{role}" LOGIN PASSWORD \'{password}\' NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS')
        await _set_runtime_privileges(owner, roles[0])
        await owner.execute(f'GRANT vp_worker_runtime TO "{roles[0]}" WITH SET FALSE')
        await reset_public_privileges(owner, roles[1])
        await grant_functions(owner, roles[1], ROLE_FUNCTIONS["operator"])
        urls = [target.set(username=role, password=password) for role in roles]
        await owner.execute("INSERT INTO runtime_schedules(service_name,state,updated_by) VALUES('videoprocess','OPEN','synthetic') ON CONFLICT(service_name) DO UPDATE SET state='OPEN'")
        path = tmp_path / "synthetic.bin"
        path.write_bytes(b"synthetic bytes, never uploaded")
        database = SimpleNamespace(owner_url=target, runtime_url=urls[0], operator_url=urls[1], role=roles[0], service="retained-" + uuid4().hex, token_hash=hashlib.sha256(uuid4().bytes).hexdigest())
        generator = ack_drill_runtime.__wrapped__(database, [{"input": str(path)}], SimpleNamespace(param=None))
        async with aclosing(generator):
            worker = await anext(generator)
            fixture = await persist_document(owner, occupied=occupied)
            await insert_model(owner, "publishing_accounts", {"id": str(worker.account_id), "channel_profile_id": str(worker.channel_id), "account_label": "new owned producer", "platform_account_id": fixture["platform_channel_id"], "default_privacy": "unlisted"})
            for signature in ("vp_owned_retained_terminal_facts(json,json)", "vp_owned_producer_retained_terminal(json,json,json,timestamp with time zone)", "vp_owned_retained_terminal_seal_guard()"):
                assert not await owner.fetchval("SELECT has_function_privilege($1,$2,'EXECUTE')", roles[0], signature)
                assert not await owner.fetchval("SELECT has_function_privilege($1,$2,'EXECUTE')", roles[1], signature)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await worker.runtime.fetchval("SELECT public.vp_owned_retained_terminal_facts('{}'::json,'{}'::json)")
            claim = worker.context.execution_claim
            async def reserve():
                return await worker.runtime.fetchval("SELECT public.vp_reserve_worker_youtube_upload($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    claim.worker_registration_id, claim.worker_lease_epoch, claim.worker_id, claim.started_at,
                    worker.job_id, worker.node_id, worker.artifact_id, worker.context.content_sha256, "Owned canary", "unlisted")
            if occupied:
                with pytest.raises(asyncpg.RaiseError, match="^owned_inventory_producer_binding$"):
                    await reserve()
            else:
                operation = await reserve()
                assert isinstance(operation, UUID)
                assert await owner.fetchval("SELECT status='reserved' AND request_attempted_at IS NULL FROM youtube_upload_operations WHERE id=$1", operation)
                transition = "SELECT public.vp_transition_worker_youtube_upload($1,$2,$3,$4,$5,'reserved',$6,NULL,NULL,NULL,NULL)"
                args = (claim.worker_registration_id, claim.worker_lease_epoch, claim.worker_id, claim.started_at, operation)
                await worker.runtime.fetchval(transition, *args, "attempting")
                fence_connection = await asyncpg.connect(dsn(urls[0]), command_timeout=15)
                try:
                    async with fence_connection.transaction():
                        await fence_connection.fetchval(transition, *args, "fence")
                        # A separate authenticated heartbeat works while the native producer fence is held.
                        await asyncio.wait_for(worker.refresh(minimum_margin_seconds=150), timeout=2)
                        await fence_connection.execute("SELECT public.vp_require_worker_lease_margin($1,$2,150)", claim.worker_registration_id, claim.worker_lease_epoch)
                finally:
                    await fence_connection.close()
            # An active inventory still requires its own new item; released history grants none.
            assert await owner.fetchval("SELECT count(*) FROM youtube_upload_operations WHERE node_execution_id=$1", worker.node_id) == (0 if occupied else 1)
            await owner.execute("UPDATE production_tasks SET prompt='new owned update remains allowed' WHERE id=$1", worker.task_id)
    finally:
        if owner is not None:
            await owner.close()
        await anchor.execute(f'DROP DATABASE "{target.database}"')
        for role in reversed(roles):
            await anchor.execute(f'DROP ROLE IF EXISTS "{role}"')
        if stable_created:
            await anchor.execute("DROP ROLE vp_worker_runtime")
        await anchor.close()


@pytest.mark.asyncio
async def test_pg_existing_snapshot_bound_accepts_8192_and_refuses_8193(retained_pg):
    db = retained_pg.owner
    await insert_model(db, "channel_profiles", {"id": str(uuid4()), "name": "bounded synthetic"})
    row = await db.fetchval("SELECT row_to_json(c) FROM channel_profiles c LIMIT 1")
    await db.execute("DELETE FROM channel_profiles")
    await db.execute("INSERT INTO channel_profiles SELECT (jsonb_populate_record(NULL::channel_profiles,$1::jsonb || jsonb_build_object('id',gen_random_uuid()))).* FROM generate_series(1,8192)", row)
    assert await db.fetchval("SELECT json_array_length(public.vp_owned_producer_rows()->'channel_profiles')", timeout=60) == 8192
    await insert_model(db, "channel_profiles", {"id": str(uuid4()), "name": "one over bound"})
    with pytest.raises(asyncpg.RaiseError, match="owned_history_incomplete"):
        await db.fetchval("SELECT public.vp_owned_producer_rows()")


@pytest.mark.asyncio
async def test_pg_additive_042_to_046_catalog_and_downgrade_preservation(retained_database):
    from tests.migrations.owned_history_postgres import catalogue
    anchor = await asyncpg.connect(dsn(retained_database), command_timeout=15)
    target = retained_database.set(database="vp_owned_retained_test_" + uuid4().hex)
    db = None
    try:
        await anchor.execute(f'CREATE DATABASE "{target.database}"')
        await migrate(target, "042_owned_producer_fence")
        await migrate(target, PREVIOUS)
        db = await asyncpg.connect(dsn(target), command_timeout=15)
        before = await catalogue(db)
        old_oids = dict(await db.fetch("SELECT proname,oid FROM pg_proc WHERE pronamespace='public'::regnamespace"))
        await migrate(target, HEAD)
        after = await catalogue(db)
        for name, row in before.items():
            current = after[name]
            assert await db.fetchval("SELECT oid FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname=$1", name) == old_oids[name]
            assert ({**current, "source": row["source"]} if name == "vp_owned_producer_history" else current) == row
        new = {"vp_owned_retained_terminal_facts", "vp_owned_producer_retained_terminal", "vp_owned_retained_terminal_seal_guard"}
        assert set(after) - set(before) == new
        for name in new:
            assert after[name]["config"] == ["search_path=pg_catalog"]
            assert after[name]["definer"] is (name == "vp_owned_retained_terminal_seal_guard")
            assert not await db.fetchval("SELECT EXISTS(SELECT 1 FROM pg_proc p CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) acl WHERE p.proname=$1 AND p.pronamespace='public'::regnamespace AND acl.grantee=0 AND acl.privilege_type='EXECUTE')", name)
        assert await db.fetchval("SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'owned_retained_seal_%'") == 16
        await migrate(target, PREVIOUS, "downgrade")
        assert await catalogue(db) == before
        await migrate(target, HEAD)
        await persist_document(db)
        with pytest.raises(AssertionError, match="owned_inventory_history_requires_preservation"):
            await migrate(target, PREVIOUS, "downgrade")
        assert await db.fetchval("SELECT version_num FROM alembic_version") == HEAD
        assert await db.fetchval("SELECT count(*) FROM owned_seed_inventories WHERE approved_at IS NOT NULL") == 1
    finally:
        if db is not None:
            await db.close()
        await anchor.execute(f'DROP DATABASE "{target.database}"')
        await anchor.close()
