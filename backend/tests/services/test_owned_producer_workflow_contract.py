from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import select

from app.channel_agent.clients import LocalAutoFlowClient
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, ProductionTask
from app.models.owned_seed_inventory import OwnedSeedInventory
from tests.channel_agent.test_owned_inventory import inventory_env as inventory_env, owned_env as owned_env, tick, Policy


async def test_settlement_phase_uses_original_item_after_expiry_and_pointer_change(owned_env):
    from app.channel_agent.owned_producer import QueueLease, settlement_phase
    env = owned_env
    await tick(env, Policy())
    async with env.factory() as db:
        task_id = (await db.scalars(select(ProductionTask.id))).one()
        queue = (await db.scalars(select(ChannelOpsQueueItem))).one()
        queue.status, queue.locked_by, queue.locked_at = "running", "owner", datetime.now(timezone.utc)
        (await db.get(OwnedSeedInventory, env.inventory_id)).state = "expired"
        (await db.get(ChannelProfile, env.channel_id)).owned_seed_inventory_id = None
        await db.commit()
        lease = QueueLease.capture(queue)
        task = await settlement_phase(db, lease, task_id)
        assert task.id == task_id and db.in_transaction() and not db.dirty
        assert (await db.get(OwnedSeedInventory, env.inventory_id)).state == "expired"


async def test_local_autoflow_forwards_exact_existing_bound_execute_fields(monkeypatch):
    task = SimpleNamespace(id=uuid.uuid4(), autoflow_plan_id=uuid.uuid4(), agent_approval_evidence_json={"owned_inventory": {"item_id": "owned"}})
    fields = {"production_task_id": str(task.id), "channelops_queue_item_id": str(uuid.uuid4()),
        "channelops_queue_locked_by": "owner", "channelops_queue_locked_at": datetime.now(timezone.utc),
        "expected_approved_revision": 1, "expected_approved_revision_hash": "a" * 64,
        "idempotency_key": "channelops-execute:exact-bound-fixture"}
    calls = []
    async def execute(request, db):
        calls.append(request)
        return SimpleNamespace(run_id=str(uuid.uuid4()), pipeline_id=None, job_id=None, status="running", error_message=None)
    @asynccontextmanager
    async def factory():
        yield object()
    monkeypatch.setattr("app.autoflow.service.autoflow_service.execute", execute)
    result = await LocalAutoFlowClient(session_factory=factory).execute_task(task, {"_channelops_execute": fields})
    assert result.status != "failed"
    assert calls[0].model_dump(include=set(fields)) == fields


async def test_owned_local_autoflow_refuses_missing_bound_execution_without_call(monkeypatch):
    task = SimpleNamespace(id=uuid.uuid4(), autoflow_plan_id=uuid.uuid4(), agent_approval_evidence_json={"owned_inventory": {"item_id": "owned"}})
    @asynccontextmanager
    async def factory():
        pytest.fail("missing owned execution binding must not open a session")
        yield
    result = await LocalAutoFlowClient(session_factory=factory).execute_task(task, {})
    assert result.status == "failed" and "owned_inventory_execute_binding" in result.error_message


async def test_hold_locks_canonical_before_inventory(owned_env, monkeypatch):
    from app.channel_agent.owned_producer import QueueLease, hold
    from app.channel_agent.service import ChannelAgentService
    from app.services import owned_seed_inventory as inv
    env = owned_env
    await tick(env, Policy())
    calls = []
    platform_lock, row_lock = inv.lock_platform_scope, inv._row
    async def platform(db, scope):
        calls.append(scope)
        return await platform_lock(db, scope)
    async def row(db, model, row_id, **kwargs):
        if model is OwnedSeedInventory and kwargs.get("lock"):
            assert calls == [env.scope["platform_channel_id"]]
        return await row_lock(db, model, row_id, **kwargs)
    monkeypatch.setattr(inv, "lock_platform_scope", platform)
    monkeypatch.setattr(inv, "_row", row)
    async with env.factory() as db:
        task_id = (await db.scalars(select(ProductionTask.id))).one()
        queue = (await db.scalars(select(ChannelOpsQueueItem))).one()
        queue.status, queue.locked_by, queue.locked_at = "running", "owner", datetime.now(timezone.utc)
        await db.commit()
        task = await hold(db, ChannelAgentService(), QueueLease.capture(queue), task_id, "synthetic_denial")
        assert task.state == "held"


@pytest.mark.parametrize("field", ["job_id", "pipeline_id", "autoflow_run_id"])
async def test_plan_reentry_allows_plan_binding_but_not_new_execution_binding(owned_env, field):
    from app.models.autoflow import AutoFlowPlan
    from app.channel_agent.service import ChannelAgentService
    from tests.services.test_owned_producer_fence import decision
    env = owned_env
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            await conn.run_sync(AutoFlowPlan.__table__.create)
    class RealPolicy:
        async def decide(self, request):
            return decision()
    await tick(env, RealPolicy())
    class ChangedBinding(LocalAutoFlowClient):
        async def plan_task(self, task, request):
            result = await super().plan_task(task, request)
            async with env.factory() as writer:
                row = await writer.get(ProductionTask, uuid.UUID(str(task.id)))
                setattr(row, field, uuid.uuid4())
                await writer.commit()
            return result
    async with env.factory() as db:
        queue = (await db.scalars(select(ChannelOpsQueueItem))).one()
        queue.status, queue.locked_by, queue.locked_at = "running", "owner", datetime.now(timezone.utc)
        await db.commit()
        service = ChannelAgentService(pds_client=RealPolicy(), autoflow_client=ChangedBinding(session_factory=env.factory))
        task = await service.handle_plan_task(db, queue)
        assert task.state == "held" and task.blocked_by_guard == "owned_inventory_plan_stage"
        assert not list((await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "execute_task"))).all())
