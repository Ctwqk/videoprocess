from dataclasses import asdict
import uuid

import pytest
from sqlalchemy import select

from app.autoflow.metadata_generator import MetadataGenerator
from app.autoflow.service import AutoFlowService
from app.models.autoflow import AutoFlowPlan as PlanRow, AutoFlowRun, AutoFlowUsedClip
from app.models.channel_agent import ProductionTask
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.schemas.autoflow import AutoFlowPlan, AutoFlowIntent, AutoFlowRequest, AutoFlowExecuteRequest
from app.schemas.pipeline import PipelineDefinition
from app.services import owned_seed_inventory as inv
from tests.channel_agent.test_owned_inventory import inventory_env as inventory_env, owned_env as owned_env, tick
from tests.services.test_owned_producer_fence import decision, owned_graph
from app.channel_agent.service import ChannelAgentService


class Policy:
    async def decide(self, request):
        return decision()


@pytest.fixture
async def bound_plan(owned_env):
    env = owned_env
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            for model in (PlanRow, AutoFlowRun, AutoFlowUsedClip):
                await conn.run_sync(model.__table__.create)
    await tick(env, Policy())
    service = AutoFlowService()
    async with env.factory() as db:
        task = (await db.scalars(select(ProductionTask))).one()
        asset_id = task.agent_approval_evidence_json["owned_inventory"]["input_asset_id"]
        request = AutoFlowRequest(prompt=task.prompt, input_asset_id=asset_id, source_policy="owned_only",
            source_strategy="input_video", planning_mode="template", target_platforms=["youtube"], source_platforms=[],
            publish_mode="unlisted_upload", constraints={"channelops": {"production_task_id": str(task.id)}})
        intent = AutoFlowIntent(intent_type="animal_compilation", subject=task.prompt, publish_mode="unlisted_upload")
        plan = AutoFlowPlan(plan_id=str(uuid.uuid4()), request=request, intent=intent,
            template_id="animal_compilation_short", pipeline_definition=PipelineDefinition.model_validate(owned_graph(asset_id)),
            candidates=[], metadata=MetadataGenerator().generate(intent, []), validation={"valid": True},
            rights={"status": "allowed", "review_approved": False}, needs_review=True, status="review_required")
        plan = await service._save_plan(db, plan)
        task.autoflow_plan_id = uuid.UUID(plan.plan_id)
        task.agent_approval_evidence_json = {**task.agent_approval_evidence_json, "plan_pds": {
            "request": {"actor_id": str(task.target_account_id), "platform": "youtube", "action_type": "plan_approval",
                "content": {"title": task.title_seed, "description": task.prompt},
                "context": {"production_task_id": str(task.id), "autoflow_plan_id": plan.plan_id}},
            "response": asdict(decision())}}
        await db.commit()
        env.task_id, env.plan_id, env.service = task.id, plan.plan_id, service
    return env


@pytest.mark.parametrize("fault", ["missing_pds", "wrong_plan", "wrong_task", "wrong_actor", "fallback", "revoked", "public"])
async def test_autoflow_approval_requires_durable_owned_plan_authority(bound_plan, fault):
    env = bound_plan
    async with env.factory() as db:
        task = await db.get(ProductionTask, env.task_id)
        evidence = inv.canonical(task.agent_approval_evidence_json)
        import json
        changed = json.loads(evidence)
        if fault == "missing_pds":
            changed.pop("plan_pds")
        elif fault in {"wrong_plan", "wrong_task"}:
            changed["plan_pds"]["request"]["context"]["autoflow_plan_id" if fault == "wrong_plan" else "production_task_id"] = str(uuid.uuid4())
        elif fault == "wrong_actor":
            changed["plan_pds"]["request"]["actor_id"] = str(uuid.uuid4())
        elif fault == "fallback":
            changed["plan_pds"]["response"]["metadata"] = {"warning": "pds_unavailable"}
        elif fault == "revoked":
            (await db.get(OwnedSeedInventory, env.inventory_id)).state = "revoked"
        task.agent_approval_evidence_json = changed
        await db.commit()
        with pytest.raises(inv.OwnedInventoryError):
            if fault == "public":
                await env.service.approve_public(env.plan_id, db, review_notes=evidence)
            else:
                await env.service.approve(env.plan_id, db, review_notes=evidence)
        await db.rollback()
        row = await db.get(PlanRow, uuid.UUID(env.plan_id))
        assert row.review_approved_at is None and row.approved_revision_hash is None


async def test_autoflow_approval_accepts_durable_envelope_not_review_notes(bound_plan):
    env = bound_plan
    async with env.factory() as db:
        first = await env.service.approve(env.plan_id, db, review_notes="audit only")
        assert first.approved_revision_hash and first.review_notes == "audit only"
        task = await db.get(ProductionTask, env.task_id)
        assert task.state == "selected"
        again = await env.service.approve(env.plan_id, db, review_notes="audit only")
        assert again.approved_revision_hash == first.approved_revision_hash


async def test_owned_plan_uses_exact_asset_and_retries_durable_plan_without_search(owned_env, monkeypatch):
    env = owned_env
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            await conn.run_sync(PlanRow.__table__.create)
    await tick(env, Policy())
    service = AutoFlowService()
    async def no_search(*args, **kwargs):
        pytest.fail("owned input must not invoke material selection")
    monkeypatch.setattr(service.material_selector, "find_candidates_with_warnings", no_search)
    async with env.factory() as db:
        task = (await db.scalars(select(ProductionTask))).one()
        request = AutoFlowRequest.model_validate(ChannelAgentService()._autoflow_request(task))
        first = await service.plan(request, db)
        await db.refresh(task)
        assert str(task.autoflow_plan_id) == first.plan_id and task.state == "selected"
        assert len(first.candidates) == 1 and first.candidates[0].asset_id == request.input_asset_id
        second = await service.plan(request, db)
        assert second.plan_id == first.plan_id
        assert len((await db.scalars(select(PlanRow))).all()) == 1


async def test_owned_plan_refuses_substitution_before_planning(owned_env):
    env = owned_env
    await tick(env, Policy())
    async with env.factory() as db:
        task = (await db.scalars(select(ProductionTask))).one()
        request = ChannelAgentService()._autoflow_request(task)
        request["input_asset_id"] = str(uuid.uuid4())
        with pytest.raises(inv.OwnedInventoryError, match="owned_inventory_plan_request"):
            await AutoFlowService().plan(AutoFlowRequest.model_validate(request), db)


async def test_owned_execute_cannot_use_unbound_manual_api(bound_plan):
    env = bound_plan
    async with env.factory() as db:
        plan = await env.service.approve(env.plan_id, db)
        with pytest.raises(inv.OwnedInventoryError, match="owned_inventory_execute_binding"):
            await env.service.execute(AutoFlowExecuteRequest(plan_id=env.plan_id, execute=True,
                expected_approved_revision_hash=plan.approved_revision_hash, expected_approved_revision=plan.approved_revision), db)
        await db.rollback()
        assert not list((await db.scalars(select(AutoFlowRun))).all())


async def test_bound_owned_execute_locks_queue_before_revocation_check_or_job_creation(bound_plan, monkeypatch):
    from datetime import datetime, timezone
    from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile
    env = bound_plan
    async with env.factory() as db:
        plan = await env.service.approve(env.plan_id, db)
        task = await db.get(ProductionTask, env.task_id)
        queue = (await db.scalars(select(ChannelOpsQueueItem))).one()
        queue.kind, queue.status, queue.locked_by, queue.locked_at = "execute_task", "running", "owner", datetime.now(timezone.utc)
        queue.payload_json = {"production_task_id": str(task.id), "autoflow_plan_id": env.plan_id,
            "expected_approved_revision_hash": plan.approved_revision_hash, "expected_approved_revision": plan.approved_revision}
        queue_id, at = queue.id, queue.locked_at
        (await db.get(OwnedSeedInventory, env.inventory_id)).state = "revoked"
        await db.commit()
        calls = []
        execute, row = db.execute, inv._row
        async def observe(statement, *args, **kwargs):
            if "FROM channel_ops_queue_items" in str(statement) and statement._for_update_arg is not None:
                calls.append("queue")
            return await execute(statement, *args, **kwargs)
        async def channel(db, model, *args, **kwargs):
            if model is ChannelProfile:
                assert calls, "bound execution must acquire its queue before channel/schedule"
            return await row(db, model, *args, **kwargs)
        monkeypatch.setattr(db, "execute", observe)
        monkeypatch.setattr(inv, "_row", channel)
        with pytest.raises(inv.OwnedInventoryError, match="owned_inventory_producer_inactive"):
            await env.service.execute(AutoFlowExecuteRequest(plan_id=env.plan_id, execute=True,
                expected_approved_revision_hash=plan.approved_revision_hash, expected_approved_revision=plan.approved_revision,
                production_task_id=str(env.task_id), channelops_queue_item_id=str(queue_id),
                channelops_queue_locked_by="owner", channelops_queue_locked_at=at), db)


async def test_owned_graph_endpoint_refuses_before_graph_planner(owned_env, monkeypatch):
    env = owned_env
    await tick(env, Policy())
    service = AutoFlowService()
    async def no_graph(*args, **kwargs):
        pytest.fail("owned producer cannot enter arbitrary graph planning")
    monkeypatch.setattr(service.graph_planner, "plan", no_graph)
    async with env.factory() as db:
        task = (await db.scalars(select(ProductionTask))).one()
        request = AutoFlowRequest.model_validate(ChannelAgentService()._autoflow_request(task))
        with pytest.raises(inv.OwnedInventoryError, match="owned_inventory_plan_request"):
            await service.plan_graph(request, db)


async def test_owned_plan_patch_cannot_replace_privacy(bound_plan):
    from app.schemas.autoflow import AutoFlowPlanPatch
    env = bound_plan
    async with env.factory() as db:
        with pytest.raises(inv.OwnedInventoryError):
            await env.service.patch_plan(env.plan_id, AutoFlowPlanPatch(publish_mode="public_after_review", rebuild_definition=False), db)
        await db.rollback()
        assert (await env.service.get_plan(env.plan_id, db)).request.publish_mode == "unlisted_upload"


async def test_reject_serializes_before_plan_row_and_remains_available_after_expiry(bound_plan, monkeypatch):
    env = bound_plan
    calls = []
    original = inv.lock_history_channel_mutation
    async def channel_lock(db, channel_id):
        calls.append("channel-schedule")
        return await original(db, channel_id)
    monkeypatch.setattr(inv, "lock_history_channel_mutation", channel_lock)
    original_plan = env.service._get_plan_for_update
    async def plan_lock(*args, **kwargs):
        assert calls == ["channel-schedule"]
        return await original_plan(*args, **kwargs)
    monkeypatch.setattr(env.service, "_get_plan_for_update", plan_lock)
    async with env.factory() as db:
        (await db.get(OwnedSeedInventory, env.inventory_id)).state = "expired"
        await db.commit()
        result = await env.service.reject(env.plan_id, db)
        assert result.status == "rejected"


@pytest.mark.parametrize("fault", [None, "absent", "unapproved", "revision", "plan_pds"])
async def test_first_promotion_requires_actual_approved_bound_plan(bound_plan, fault):
    from app.services import owned_producer_workflow as workflow
    from app.services.owned_producer_fence import lock_producer
    env = bound_plan
    async with env.factory() as db:
        plan = await env.service.approve(env.plan_id, db)
        task = await db.get(ProductionTask, env.task_id)
        task.rationale_json = {**task.rationale_json, "autoflow_plan_payload": {
            "plan_id": plan.plan_id, "expected_approved_revision": plan.approved_revision,
            "expected_approved_revision_hash": plan.approved_revision_hash}}
        if fault == "absent":
            task.autoflow_plan_id = None
        elif fault == "unapproved":
            (await db.get(PlanRow, uuid.UUID(env.plan_id))).review_approved_at = None
        elif fault == "revision":
            (await db.get(PlanRow, uuid.UUID(env.plan_id))).execution_revision += 1
        elif fault == "plan_pds":
            task.agent_approval_evidence_json = {k: v for k, v in task.agent_approval_evidence_json.items() if k != "plan_pds"}
        await db.commit()
        authority = await lock_producer(db, task.id)
        if fault is None:
            approved = await workflow.require_approved_plan(db, task, authority.identity)
            assert str(approved.id) == plan.plan_id
        else:
            with pytest.raises(inv.OwnedInventoryError):
                await workflow.require_approved_plan(db, task, authority.identity)
