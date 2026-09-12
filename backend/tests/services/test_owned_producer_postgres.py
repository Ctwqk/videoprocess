"""Parent-only D SQL checkpoint: synthetic owned graph, real restricted claims/RPCs."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from app.models.artifact import Artifact
from app.models.asset import Asset
from app.models.autoflow import AutoFlowPlan, AutoFlowRun
from app.models.channel_agent import ProductionTask
from app.models.job import Job, NodeExecution, NodeStatus
from app.models.owned_seed_inventory import OwnedSeedInventoryItem
from app.services import owned_seed_inventory as inv
from app.services import owned_seed_inventory_history as history
from app.services.worker_control_role_cli import ROLE_FUNCTIONS
from app.services.worker_role_cli_common import create_login_role, ensure_stable_role, grant_functions, reset_public_privileges
from app.services.worker_runtime_role_cli import _set_runtime_privileges
from app.services.youtube_upload_operations import YouTubeUploadOperationStore
from tests.api.test_owned_seed_inventory import inventory_env as api_inventory_env
from tests.channel_agent.test_owned_inventory import configure_owned_env, tick
from tests.migrations.owned_history_postgres import dsn, migrate, wait_blocked
from tests.services.test_owned_producer_fence import decision, owned_graph
from tests.worker.ack_drill_postgres import ack_drill_runtime
from tests.worker.test_youtube_upload_handler import media_paths as media_paths  # noqa: F401


def checked_url(raw, confirmation, system):
    url = make_url(raw)
    if not (url.drivername == "postgresql+asyncpg" and url.host in {"127.0.0.1", "::1"}
            and url.port is not None and 1024 <= url.port <= 65535 and url.port != 5432
            and url.username and url.password and not url.query and confirmation == url.database
            and re.fullmatch(r"vp_owned_inventory_test_d_[a-z0-9_]+", url.database or "")
            and re.fullmatch(r"[0-9]{10,20}", system or "")):
        raise ValueError("explicit D disposable database, confirmation and system identity required")
    return url


@pytest.fixture
def d_database():
    raw = os.environ.get("OWNED_D_POSTGRES_TEST_URL", "")
    if not raw:
        pytest.skip("parent-only explicit D PostgreSQL qualification")
    system = os.environ.get("OWNED_D_POSTGRES_SYSTEM_ID", "")
    anchor = checked_url(raw, os.environ.get("OWNED_D_POSTGRES_TEST_CONFIRM"), system)
    name = "vp_owned_inventory_test_d_" + uuid.uuid4().hex
    target = anchor.set(database=name)
    role, operator_role = "vp_d_runtime_" + uuid.uuid4().hex, "vp_d_operator_" + uuid.uuid4().hex
    runtime = target.set(username=role, password=secrets.token_hex(24))
    operator = target.set(username=operator_role, password=secrets.token_hex(24))
    created = []

    async def setup():
        admin = await asyncpg.connect(dsn(anchor), timeout=5, command_timeout=10)
        try:
            assert 160000 <= int(await admin.fetchval("SHOW server_version_num")) < 170000
            assert str(await admin.fetchval("SELECT system_identifier FROM pg_control_system()")) == system
            assert not await admin.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='vp_worker_runtime')")
            await admin.execute(f'CREATE DATABASE "{name}"')
            created.append("database")
        finally:
            await admin.close()
        await migrate(target, "042_owned_producer_fence")
        owner = await asyncpg.connect(dsn(target), timeout=5)
        try:
            await owner.execute("INSERT INTO runtime_schedules(service_name,state,updated_by) VALUES('videoprocess','CLOSED','d-fixture') ON CONFLICT(service_name) DO UPDATE SET state='CLOSED',guarded_job_id=NULL")
            async with owner.transaction():
                await ensure_stable_role(owner, "vp_worker_runtime", setting_prefix="d_fixture", authorized_members=(role,))
                await _set_runtime_privileges(owner, "vp_worker_runtime")
                await create_login_role(owner, role, runtime.password, setting_prefix="d_fixture", stable_role="vp_worker_runtime")
                await create_login_role(owner, operator_role, operator.password, setting_prefix="d_fixture")
                await reset_public_privileges(owner, operator_role)
                await grant_functions(owner, operator_role, ROLE_FUNCTIONS["operator"])
            created.extend(["vp_worker_runtime", role, operator_role])
            await owner.execute(f'GRANT CONNECT ON DATABASE "{name}" TO "{role}", "{operator_role}"')
        finally:
            await owner.close()

    async def cleanup():
        admin = await asyncpg.connect(dsn(anchor), timeout=5, command_timeout=10)
        try:
            if "database" in created:
                # Leaks must fail, not be concealed by DROP ... FORCE.
                await admin.execute(f'DROP DATABASE "{name}"')
            for role_name in reversed([n for n in created if n != "database"]):
                await admin.execute(f'DROP ROLE "{role_name}"')
        finally:
            await admin.close()

    try:
        asyncio.run(setup())
        yield SimpleNamespace(owner_url=target, runtime_url=runtime, operator_url=operator, role=role,
            service="d-publisher-" + uuid.uuid4().hex, token_hash=hashlib.sha256(secrets.token_bytes(32)).hexdigest())
    finally:
        asyncio.run(cleanup())


class RealPolicy:
    async def decide(self, _request):
        return decision()


@pytest.fixture
async def d_pg(d_database, monkeypatch, media_paths, request):
    monkeypatch.setenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM", d_database.owner_url.database)
    api = api_inventory_env.__wrapped__(monkeypatch, SimpleNamespace(param=d_database.owner_url.render_as_string(hide_password=False),
        expected_migration_head="042_owned_producer_fence"))
    worker = None
    try:
        env = await anext(api)
        await configure_owned_env(env, monkeypatch, sqlite=False)
        await tick(env, RealPolicy())
        worker = ack_drill_runtime.__wrapped__(d_database, media_paths, request)
        native = await anext(worker)
        async with env.factory() as db:
            item = (await db.scalars(select(OwnedSeedInventoryItem).where(OwnedSeedInventoryItem.production_task_id.is_not(None)))).one()
            task = await db.get(ProductionTask, item.production_task_id)
            task_id = task.id
            old = await db.get(ProductionTask, native.task_id)
            old.job_id, old.state = None, "held"
            graph = owned_graph(str(item.asset_id))
            upload = next(n for n in graph["nodes"] if n["type"] == "youtube_upload")
            upload["data"]["config"]["title"] = native.context.title
            job = await db.get(Job, native.job_id)
            job.pipeline_snapshot = graph
            now = datetime.now(timezone.utc)
            node = await db.get(NodeExecution, native.node_id)
            node.node_id, node.node_config = upload["id"], upload["data"]["config"]
            nodes = {upload["id"]: node}
            for spec in graph["nodes"]:
                if spec["type"] == "youtube_upload":
                    continue
                config = dict(spec["data"]["config"])
                if spec["data"].get("asset_id"):
                    config["asset_id"] = spec["data"]["asset_id"]
                n = NodeExecution(job_id=job.id, node_id=spec["id"], node_type=spec["type"], node_config=config,
                    status=NodeStatus.SUCCEEDED, started_at=(now - timedelta(seconds=2)).replace(tzinfo=None), completed_at=now.replace(tzinfo=None))
                db.add(n)
                nodes[spec["id"]] = n
            await db.flush()
            input_artifact = await db.get(Artifact, native.artifact_id)
            upstream = next(e["source"] for e in graph["edges"] if e["target"] == upload["id"])
            input_artifact.node_execution_id = nodes[upstream].id
            nodes[upstream].output_artifact_id = input_artifact.id
            asset = await db.get(Asset, item.asset_id)
            source = nodes[next(n["id"] for n in graph["nodes"] if n["type"] == "source")]
            source_art = Artifact(job_id=job.id, node_execution_id=source.id, filename=asset.filename,
                mime_type=asset.mime_type, file_size=asset.file_size, storage_backend=asset.storage_backend,
                storage_path=asset.storage_path, media_info={"asset_id": str(asset.id), "source_asset_id": str(asset.id)})
            db.add(source_art)
            await db.flush()
            source.output_artifact_id = source_art.id
            for name, current in nodes.items():
                if current.output_artifact_id is None and current is not node:
                    output = Artifact(job_id=job.id, node_execution_id=current.id, filename=f"{name}.mp4", storage_path=f"artifacts/{name}.mp4")
                    db.add(output)
                    await db.flush()
                    current.output_artifact_id = output.id
                current.input_artifact_ids = [nodes[e["source"]].output_artifact_id for e in graph["edges"] if e["target"] == name]
            plan = AutoFlowPlan(prompt=task.prompt, request_json={}, intent_json={}, template_id="animal_compilation_short",
                pipeline_definition=graph, candidates_json=[], metadata_json={}, rights_json={"review_approved": True},
                validation_json={"valid": True}, status="approved", execution_revision=1, approved_revision=1,
                approved_revision_hash="a" * 64, review_approved_at=now)
            db.add(plan)
            await db.flush()
            run = AutoFlowRun(plan_id=plan.id, pipeline_id=job.pipeline_id, job_id=job.id, status="running")
            db.add(run)
            await db.flush()
            task.job_id, task.pipeline_id, task.autoflow_plan_id, task.autoflow_run_id = job.id, job.pipeline_id, plan.id, run.id
            task.state = "producing"
            task.rationale_json = {**task.rationale_json, "autoflow_plan_payload": {"plan_id": str(plan.id),
                "expected_approved_revision_hash": plan.approved_revision_hash, "expected_approved_revision": 1}}
            task.agent_approval_evidence_json = {**task.agent_approval_evidence_json, "plan_pds": {
                "request": {"actor_id": str(task.target_account_id), "action_type": "plan_approval", "platform": "youtube",
                    "content": {"title": task.title_seed, "description": task.prompt},
                    "context": {"production_task_id": str(task.id), "autoflow_plan_id": str(plan.id)}},
                "response": asdict(decision())}}
            await db.commit()
        env.native, env.task_id, env.item_id, env.asset_id = native, task_id, item.id, item.asset_id
        env.owner_url = d_database.owner_url
        env.store = YouTubeUploadOperationStore(native.sessions)
        yield env
    finally:
        if worker is not None:
            await worker.aclose()
        await api.aclose()


async def transition(env, operation_id, action):
    claim = env.native.context.execution_claim
    return await env.native.runtime.fetchval("SELECT public.vp_transition_worker_youtube_upload($1,$2,$3,$4,$5,'reserved',$6,NULL,NULL,NULL,NULL)",
        claim.worker_registration_id, claim.worker_lease_epoch, claim.worker_id, claim.started_at, operation_id, action)


async def reserve(env):
    context = env.native.context
    claim = context.execution_claim
    return await env.native.runtime.fetchval("SELECT public.vp_reserve_worker_youtube_upload($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
        claim.worker_registration_id, claim.worker_lease_epoch, claim.worker_id, claim.started_at,
        context.job_id, context.node_execution_id, context.input_artifact_id, context.content_sha256, context.title, context.privacy)


async def test_pg_d_catalog_no_new_worker_authority(d_database):
    owner = await asyncpg.connect(dsn(d_database.owner_url), timeout=5)
    try:
        assert await owner.fetchval("SELECT version_num FROM alembic_version") == "042_owned_producer_fence"
        rows = await owner.fetch("SELECT proname, has_function_privilege($1,p.oid,'EXECUTE') allowed FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND proname LIKE 'vp_owned_producer_%'", d_database.role)
        assert len(rows) == 13 and all(not r["allowed"] for r in rows)
        assert not await owner.fetchval("SELECT has_table_privilege($1,'owned_seed_inventories','SELECT')", d_database.role)
        assert not await owner.fetchval("SELECT has_table_privilege($1,'youtube_upload_operations','INSERT,UPDATE,DELETE')", d_database.role)
    finally:
        await owner.close()


@pytest.mark.parametrize("value", [{"text": "汉字\u007f😀", "float": 1.0}, {"e": 1e-7, "large": 1e100},
                                  {"n": -0.0, "tiny": 5e-324, "int": 10000000000000001}])
async def test_pg_d_python_hash_vectors(d_database, value):
    raw = json.dumps(value, ensure_ascii=True)
    owner = await asyncpg.connect(dsn(d_database.owner_url), timeout=5)
    try:
        result = await owner.fetchval("SELECT public.vp_owned_producer_hash($1::json)", raw)
        assert result == inv.sha256(value)
    finally:
        await owner.close()


async def test_pg_d_restricted_reserve_attempt_final_and_expired_settlement(d_pg):
    env = d_pg
    claim = await env.store.claim(env.native.context)
    async with env.store.submission_fence(env.native.context):
        await env.store.mark_attempting(claim.operation.id, context=env.native.context)
        await env.store.mark_submitted(claim.operation.id, str(uuid.uuid4()), context=env.native.context)
    await env.native.owner.execute("UPDATE owned_seed_inventories SET state='revoked',revoked_at=clock_timestamp() WHERE id=$1", env.inventory_id)
    receipt = {"video_id": "abcdefghijk", "title": env.native.context.title, "privacy": "unlisted"}
    await env.store.mark_succeeded(claim.operation.id, "abcdefghijk", receipt, context=env.native.context)
    row = await env.native.owner.fetchrow("SELECT status,request_attempted_at,completed_at FROM youtube_upload_operations WHERE id=$1", claim.operation.id)
    assert row["status"] == "succeeded" and row["request_attempted_at"] <= row["completed_at"]
    resumed = await env.store.claim(env.native.context)
    assert resumed.action == "replay" and resumed.operation.id == claim.operation.id


async def test_pg_d_complete_sql_snapshot_matches_a1_hash(d_pg):
    env = d_pg
    async with env.factory() as db:
        expected = await history.load_owned_history_evidence(db, platform_channel_id=env.scope["platform_channel_id"])
        actual = await db.scalar(text("SELECT public.vp_owned_producer_rows()"))
        assert set(actual) == set(history.HISTORY_MODELS)
        differences = []
        for name, expected_rows in expected.rows.as_dict().items():
            expected_by_id = {row.get("id", row.get("service_name")): row for row in expected_rows}
            actual_by_id = {row.get("id", row.get("service_name")): row for row in actual[name]}
            assert actual_by_id.keys() == expected_by_id.keys(), name
            for key, before in expected_by_id.items():
                after = actual_by_id[key]
                for field in sorted(before.keys() | after.keys()):
                    if field not in before or field not in after or history.history_sha256(before[field]) != history.history_sha256(after[field]):
                        values = []
                        for value in (before.get(field), after.get(field)):
                            safe = value is None or type(value) in {bool, int, float} or (
                                isinstance(value, str) and re.fullmatch(r"[0-9T:+.Z-]{10,40}", value))
                            values.append({"type": type(value).__name__, "value": value if safe else "omitted"})
                        differences.append({"table": name, "id": key, "field": field, "a1": values[0], "sql042": values[1]})
        assert not differences, differences


@pytest.mark.parametrize("boundary", ["reserve", "attempt", "fence"])
@pytest.mark.parametrize("fault", ["revoked", "expired", "privacy", "seed", "asset", "pds", "unclassified"])
async def test_pg_d_restricted_new_effect_refuses_drift(d_pg, boundary, fault):
    env, operation_id = d_pg, None
    if boundary != "reserve":
        operation_id = (await env.store.claim(env.native.context)).operation.id
    if boundary == "fence":
        await transition(env, operation_id, "attempting")
    if fault == "revoked":
        await env.native.owner.execute("UPDATE owned_seed_inventories SET state='revoked',revoked_at=clock_timestamp() WHERE id=$1", env.inventory_id)
    elif fault == "expired":
        await env.native.owner.execute("UPDATE owned_seed_inventories SET state='expired' WHERE id=$1", env.inventory_id)
    elif fault == "privacy":
        await env.native.owner.execute("UPDATE publishing_accounts SET default_privacy='private' WHERE id=(SELECT target_account_id FROM production_tasks WHERE id=$1)", env.task_id)
    elif fault == "seed":
        await env.native.owner.execute("UPDATE manual_seeds SET prompt=prompt||' changed' WHERE id=(SELECT manual_seed_id FROM production_tasks WHERE id=$1)", env.task_id)
    elif fault == "asset":
        await env.native.owner.execute("UPDATE assets SET file_size=file_size+1 WHERE id=$1", env.asset_id)
    elif fault == "pds":
        await env.native.owner.execute("UPDATE production_tasks SET agent_approval_evidence_json=jsonb_set(agent_approval_evidence_json::jsonb,'{plan_pds,response,rules_version}','\"\"') WHERE id=$1", env.task_id)
    else:
        # A fresh legal legacy reservation is not filtered away by missing account/task identity.
        node_id = await env.native.owner.fetchval("SELECT id FROM node_executions WHERE job_id=$1 AND node_type='export'", env.native.job_id)
        await env.native.owner.execute("INSERT INTO youtube_upload_operations(job_id,node_execution_id,input_artifact_id,content_sha256,title,privacy,status) VALUES($1,$2,$3,$4,'unknown','unlisted','reserved')",
            env.native.job_id, node_id, env.native.artifact_id, "b" * 64)
    with pytest.raises(asyncpg.RaiseError, match="owned_(inventory|history)_"):
        if boundary == "reserve":
            await reserve(env)
        else:
            await transition(env, operation_id, "attempting" if boundary == "attempt" else "fence")
    rows = await env.native.owner.fetch("SELECT request_attempted_at,manager_task_id FROM youtube_upload_operations WHERE production_task_id=$1", env.task_id)
    assert len(rows) == (0 if boundary == "reserve" else 1)
    assert all(r["manager_task_id"] is None and (r["request_attempted_at"] is not None) == (boundary == "fence") for r in rows)


async def test_pg_d_final_fence_serializes_revocation(d_pg):
    env = d_pg
    claim = await env.store.claim(env.native.context)
    observer = await asyncpg.connect(dsn(env.owner_url), timeout=5)
    blocked = None
    try:
        async with env.store.submission_fence(env.native.context):
            await env.store.mark_attempting(claim.operation.id, context=env.native.context)
            holder = await env.store._active_submission_fence.get().db.scalar(text("SELECT pg_backend_pid()"))
            contender = await observer.fetchval("SELECT pg_backend_pid()")
            blocked = asyncio.create_task(observer.execute("UPDATE owned_seed_inventories SET state='revoked',revoked_at=clock_timestamp() WHERE id=$1", env.inventory_id))
            await wait_blocked(env.native.owner, contender, holder)
            assert not blocked.done()
            await env.store.mark_submitted(claim.operation.id, str(uuid.uuid4()), context=env.native.context)
        await asyncio.wait_for(blocked, 5)
    finally:
        if blocked is not None and not blocked.done():
            blocked.cancel()
            await asyncio.gather(blocked, return_exceptions=True)
        await observer.close()
