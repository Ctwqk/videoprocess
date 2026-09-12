from dataclasses import replace
from datetime import timezone
import uuid

import pytest
from sqlalchemy import select

from app.models.artifact import Artifact
from app.models.asset import Asset
from app.models.autoflow import AutoFlowPlan, AutoFlowRun
from app.models.channel_agent import ProductionTask
from app.models.job import Job, NodeExecution, NodeStatus
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.services import owned_seed_inventory as inv
from app.services.youtube_upload_operations import YouTubeUploadOperationStore
from tests.services.test_owned_producer_autoflow import bound_plan as bound_plan, owned_env as owned_env, inventory_env as inventory_env
from tests.services.test_youtube_upload_operations import _context_for


@pytest.fixture
async def upload_env(bound_plan, monkeypatch):
    from tests.channel_agent import test_owned_inventory as admission_tests
    env = bound_plan
    monkeypatch.setattr(admission_tests, "SQLITE_TABLES", admission_tests.SQLITE_TABLES | {"artifacts"})
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            await conn.run_sync(Artifact.__table__.create)
        approved = await env.service.approve(env.plan_id, db)
        task = await db.get(ProductionTask, env.task_id)
        context = await _context_for(db, production_task=task)
        plan = await db.get(AutoFlowPlan, uuid.UUID(env.plan_id))
        graph = plan.pipeline_definition
        upload = next(n for n in graph["nodes"] if n["type"] == "youtube_upload")
        context = replace(context, title=upload["data"]["config"]["title"])
        job = await db.get(Job, context.job_id)
        job.pipeline_snapshot = graph
        node = await db.get(NodeExecution, context.node_execution_id)
        node.node_id, node.node_config = upload["id"], upload["data"]["config"]
        nodes = {upload["id"]: node}
        asset = await db.get(Asset, uuid.UUID(plan.request_json["input_asset_id"]))
        for spec in graph["nodes"]:
            if spec["type"] == "youtube_upload":
                continue
            config = {**spec["data"]["config"]}
            if spec["data"].get("asset_id"):
                config["asset_id"] = spec["data"]["asset_id"]
            current = NodeExecution(job_id=job.id, node_id=spec["id"], node_type=spec["type"], node_config=config,
                status=NodeStatus.PENDING if spec["type"] == "export" else NodeStatus.SUCCEEDED)
            db.add(current)
            await db.flush()
            nodes[spec["id"]] = current
            if spec["type"] == "export":
                continue
            output = Artifact(job_id=job.id, node_execution_id=current.id, filename=asset.filename,
                storage_path=asset.storage_path, storage_backend=asset.storage_backend,
                mime_type=asset.mime_type, file_size=asset.file_size,
                media_info={"asset_id": str(asset.id), "source_asset_id": str(asset.id)} if spec["type"] == "source" else {})
            db.add(output)
            await db.flush()
            current.output_artifact_id = output.id
        for name, current in nodes.items():
            current.input_artifact_ids = [str(nodes[e["source"]].output_artifact_id) for e in graph["edges"] if e["target"] == name]
        context = replace(context, input_artifact_id=uuid.UUID(node.input_artifact_ids[0]))
        run = AutoFlowRun(plan_id=plan.id, pipeline_id=job.pipeline_id, job_id=job.id, status="running")
        db.add(run)
        await db.flush()
        task.state, task.pipeline_id, task.autoflow_run_id = "producing", job.pipeline_id, run.id
        task.rationale_json = {**task.rationale_json, "autoflow_plan_payload": {"plan_id": str(plan.id),
            "expected_approved_revision_hash": approved.approved_revision_hash,
            "expected_approved_revision": approved.approved_revision}}
        await db.commit()
    env.context, env.store = context, YouTubeUploadOperationStore(env.factory)
    return env


@pytest.mark.parametrize("boundary", ["reserve", "attempt", "fence"])
@pytest.mark.parametrize("fault", ["trim_input", "source_descriptor", "node_config", "plan_revision", "run_job"])
async def test_local_owned_upload_revalidates_full_plan_and_lineage(upload_env, boundary, fault):
    env = upload_env
    operation_id = None
    if boundary != "reserve":
        operation_id = (await env.store.claim(env.context)).operation.id
    async with env.factory() as db:
        nodes = (await db.scalars(select(NodeExecution).where(NodeExecution.job_id == env.context.job_id))).all()
        if fault == "trim_input":
            next(n for n in nodes if n.node_type == "trim").input_artifact_ids = [str(env.context.input_artifact_id)]
        elif fault == "source_descriptor":
            source = next(n for n in nodes if n.node_type == "source")
            (await db.get(Artifact, source.output_artifact_id)).storage_path = "other.mp4"
        elif fault == "node_config":
            node = next(n for n in nodes if n.node_type == "transcode")
            node.node_config = {**node.node_config, "width": 999}
        elif fault == "plan_revision":
            (await db.get(AutoFlowPlan, uuid.UUID(env.plan_id))).execution_revision += 1
        else:
            run = (await db.scalars(select(AutoFlowRun))).one()
            run.job_id = uuid.uuid4()
        await db.commit()
    with pytest.raises(inv.OwnedInventoryError):
        if boundary == "reserve":
            await env.store.claim(env.context)
        elif boundary == "attempt":
            await env.store.mark_attempting(operation_id, context=env.context)
        else:
            async with env.store.submission_fence(env.context):
                pytest.fail("changed lineage reached final fence")
    async with env.factory() as db:
        rows = (await db.scalars(select(YouTubeUploadOperation))).all()
        assert len(rows) == int(boundary != "reserve")
        assert all(r.request_attempted_at is None and r.manager_task_id is None for r in rows)


async def test_local_owned_upload_preserves_settlement_after_elapsed_expiry(upload_env, monkeypatch):
    from app.models.owned_seed_inventory import OwnedSeedInventory
    env = upload_env
    claim = await env.store.claim(env.context)
    async with env.store.submission_fence(env.context):
        await env.store.mark_attempting(claim.operation.id, context=env.context)
        await env.store.mark_submitted(claim.operation.id, str(uuid.uuid4()), context=env.context)
    async with env.factory() as db:
        expiry = (await db.get(OwnedSeedInventory, env.inventory_id)).expires_at.replace(tzinfo=timezone.utc)
    async def expired_clock(db):
        return expiry
    monkeypatch.setattr(inv, "_now", expired_clock)
    await env.store.mark_succeeded(claim.operation.id, "abcdefghijk", {"video_id": "abcdefghijk", "privacy": "unlisted"}, context=env.context)
    assert (await env.store.claim(env.context)).action == "replay"


@pytest.mark.parametrize("status", ["failed", "uncertain"])
async def test_local_terminal_uses_the_held_submission_connection(upload_env, monkeypatch, status):
    env = upload_env
    claim = await env.store.claim(env.context)
    async with env.store.submission_fence(env.context):
        await env.store.mark_attempting(claim.operation.id, context=env.context)
        def no_second_connection():
            pytest.fail("terminal transition must not acquire a second connection under its own fence")
        monkeypatch.setattr(env.store, "_session_factory", no_second_connection)
        result = await getattr(env.store, "mark_" + status)(claim.operation.id, "synthetic response failure", context=env.context)
        assert result.status == status and result.request_attempted_at is not None


@pytest.mark.parametrize("boundary", ["reserve", "attempt", "fence"])
async def test_existing_local_reservation_cannot_change_render_hash(upload_env, boundary):
    from app.services.job_execution_authority import JobExecutionAuthorityBlocked
    env = upload_env
    claim = await env.store.claim(env.context)
    changed = replace(env.context, content_sha256="b" * 64)
    with pytest.raises(JobExecutionAuthorityBlocked, match="context changed"):
        if boundary == "reserve":
            await env.store.claim(changed)
        elif boundary == "attempt":
            await env.store.mark_attempting(claim.operation.id, context=changed)
        else:
            async with env.store.submission_fence(changed):
                pytest.fail("mismatched reservation reached final fence")
    async with env.factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation.content_sha256 == env.context.content_sha256 and operation.request_attempted_at is None
