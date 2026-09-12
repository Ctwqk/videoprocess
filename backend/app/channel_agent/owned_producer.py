"""Owned downstream prepare/reentry phases on the normal ChannelOps queue."""
from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import select

from app.models.autoflow import AutoFlowPlan
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, ProductionTask
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.pds_client import PDSDecisionRequest
from app.services import owned_seed_inventory as inv
from app.services import owned_seed_inventory_history as history
from app.services.owned_producer_fence import lock_producer, require_real_pds, require_owned_pipeline
from app.services.owned_producer_workflow import require_plan_stage, require_policy, require_request

NOT_OWNED = object()


class QueueAuthorityLost(ValueError):
    pass


@dataclass(frozen=True)
class QueueLease:
    id: uuid.UUID
    kind: str
    channel_id: uuid.UUID
    owner: str
    at: datetime
    payload: dict

    @classmethod
    def capture(cls, item):
        if item.status != "running" or not item.locked_by or item.locked_at is None:
            raise QueueAuthorityLost("owned_inventory_queue_authority")
        return cls(item.id, item.kind, item.channel_profile_id, item.locked_by, inv.utc(item.locked_at), dict(item.payload_json))

    def matches(self, item):
        return (item is not None and item.status == "running" and item.kind == self.kind
            and item.channel_profile_id == self.channel_id and item.locked_by == self.owner
            and item.locked_at is not None and inv.utc(item.locked_at) == self.at and item.payload_json == self.payload)

    async def lock(self, db):
        item = (await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.id == self.id)
            .with_for_update().execution_options(populate_existing=True))).one_or_none()
        if not self.matches(item):
            raise QueueAuthorityLost("owned_inventory_queue_authority")
        return item


async def phase(db, lease, task_id):
    authority = await lock_producer(db, task_id, queue_lease=lease)
    inv.require(authority is not None, "owned_inventory_producer_changed")
    task = (await db.scalars(select(ProductionTask).where(ProductionTask.id == task_id)
        .execution_options(populate_existing=True))).one()
    inv.require(task.channel_profile_id == lease.channel_id, "owned_inventory_queue_binding")
    return authority, task


async def complete_queue(db, lease):
    queue = await lease.lock(db)
    queue.status, queue.locked_by, queue.locked_at, queue.last_error = "succeeded", None, None, None
    await db.flush()


async def settlement_phase(db, lease, task_id):
    """Observation/settlement only; never use this scope to authorize a POST."""
    await lease.lock(db)
    task = (await db.scalars(select(ProductionTask).where(ProductionTask.id == task_id)
        .execution_options(populate_existing=True))).one_or_none()
    inv.require(task is not None and task.channel_profile_id == lease.channel_id, "owned_inventory_settlement_binding")
    await inv._row(db, ChannelProfile, task.channel_profile_id, lock=True)
    await inv.lock_history_schedule(db)
    item = (await db.scalars(select(OwnedSeedInventoryItem).where(OwnedSeedInventoryItem.production_task_id == task_id))).one_or_none()
    inv.require(item is not None, "owned_inventory_settlement_binding")
    await inv.lock_platform_scope(db, item.platform_channel_id)
    row = await inv._row(db, OwnedSeedInventory, item.inventory_id, lock=True)
    item = await inv._row(db, OwnedSeedInventoryItem, item.id, lock=True)
    inv.require(row.channel_profile_id == task.channel_profile_id and row.target_account_id == task.target_account_id
        and item.production_task_id == task.id and item.manual_seed_id == task.manual_seed_id
        and item.platform_channel_id == row.platform_channel_id, "owned_inventory_settlement_binding")
    return task


async def hold(db, service, lease, task_id, reason, envelope=None, *, evidence_key="plan_pds"):
    await db.rollback()
    await lease.lock(db)
    task = await db.get(ProductionTask, task_id)
    inv.require(task is not None and task.channel_profile_id == lease.channel_id, "owned_inventory_queue_binding")
    channel = await inv._row(db, ChannelProfile, task.channel_profile_id, lock=True)
    await inv.lock_history_schedule(db)
    item = (await db.scalars(select(OwnedSeedInventoryItem).where(OwnedSeedInventoryItem.production_task_id == task_id))).one_or_none()
    now = await inv._now(db)
    if item is not None:
        await inv.lock_platform_scope(db, item.platform_channel_id)
        row = await inv._row(db, OwnedSeedInventory, item.inventory_id, lock=True)
        if row.state in {"approved", "exhausted"}:
            row.state, row.hold_reason = "held", reason
    channel.intake_paused_at, channel.intake_pause_reason = now, reason
    before = task.state
    task.state, task.blocked_by_guard, task.failure_reason, task.state_updated_at = "held", reason, reason, now
    task.transition_history_json = [*task.transition_history_json,
        {"from": before, "to": "held", "actor": lease.kind, "at": now.isoformat(), "reason": reason}]
    if envelope is not None:
        inv.require(evidence_key in {"plan_pds", "promotion_pds"}, "owned_inventory_pds_context")
        task.agent_approval_evidence_json = {**task.agent_approval_evidence_json, evidence_key: envelope}
    await service._emit_actor_action_event(db, actor_id=str(task.target_account_id), action_type="owned_producer_held",
        platform="youtube", metadata={"production_task_id": str(task.id), "stage": lease.kind, "guard": reason})
    await complete_queue(db, lease)
    await db.commit()
    await db.refresh(task)
    return task


async def maybe_plan(service, db, item):
    if await db.scalar(select(OwnedSeedInventory.id).where(OwnedSeedInventory.approved_at.is_not(None)).limit(1)) is None:
        return NOT_OWNED
    lease = QueueLease.capture(item)
    task_id = uuid.UUID(lease.payload["production_task_id"])
    db.info["owned_tick_lease"] = lease
    envelope = None
    try:
        authority, task = await phase(db, lease, task_id)
        if authority.identity.inventory_id is None:
            db.info.pop("owned_tick_lease", None)
            return NOT_OWNED
        prepared = authority.digest
        require_plan_stage(task)
        require_policy(task)
        request = service._autoflow_request(task)
        require_request(request, task, authority.identity)
        detached = SimpleNamespace(**next(r for r in authority.snapshot.rows.as_dict()["production_tasks"] if r["id"] == str(task_id)))
        plan_id = str(task.autoflow_plan_id) if task.autoflow_plan_id else None
        await db.rollback()
        if plan_id is None:
            observation = await service.autoflow_client.plan_task(detached, request)
            plan_id = observation.plan_id
        authority, task = await phase(db, lease, task_id)
        inv.require(authority.digest == prepared, "owned_inventory_producer_changed")
        require_plan_stage(task)
        plan = await db.get(AutoFlowPlan, uuid.UUID(plan_id))
        inv.require(plan is not None and task.autoflow_plan_id in {None, plan.id}, "owned_inventory_plan_binding")
        require_request(plan.request_json, task, authority.identity)
        require_owned_pipeline(plan.pipeline_definition, request["input_asset_id"])
        task.autoflow_plan_id = plan.id
        await db.commit()
        authority, task = await phase(db, lease, task_id)
        inv.require(authority.digest == prepared, "owned_inventory_producer_changed")
        require_plan_stage(task)
        policy_request = PDSDecisionRequest(actor_id=str(task.target_account_id), action_type="plan_approval", platform="youtube",
            content={"title": task.title_seed, "description": task.prompt},
            context={"production_task_id": str(task_id), "autoflow_plan_id": plan_id, "channel_id": str(task.channel_profile_id)})
        existing = task.agent_approval_evidence_json.get("plan_pds")
        if existing is not None:
            require_policy(task, plan_id=plan_id)
            envelope = existing
            await db.rollback()
        else:
            await db.rollback()
            try:
                decision = asdict(await service.pds_client.decide(policy_request))
            except Exception:
                raise inv.OwnedInventoryError("owned_inventory_pds_error") from None
            envelope = {"request": asdict(policy_request), "response": decision}
            authority, task = await phase(db, lease, task_id)
            inv.require(authority.digest == prepared and str(task.autoflow_plan_id) == plan_id, "owned_inventory_producer_changed")
            require_plan_stage(task)
            require_real_pds(decision)
            task.agent_approval_evidence_json = {**task.agent_approval_evidence_json, "plan_pds": envelope}
            await db.commit()
        # The unchanged approval API reads the committed task envelope itself.
        await service.autoflow_client.approve_plan(plan_id, approved_by="channel_agent", evidence=envelope["response"])
        authority, task = await phase(db, lease, task_id)
        inv.require(authority.digest == prepared and str(task.autoflow_plan_id) == plan_id, "owned_inventory_producer_changed")
        require_plan_stage(task)
        require_policy(task, plan_id=plan_id)
        plan = (await db.scalars(select(AutoFlowPlan).where(AutoFlowPlan.id == uuid.UUID(plan_id))
            .execution_options(populate_existing=True))).one()
        inv.require(plan.approved_revision == plan.execution_revision and plan.approved_revision_hash
            and (plan.review_approved_at is not None or plan.agent_approved_by), "owned_inventory_plan_approval")
        payload = {"production_task_id": str(task_id), "plan_id": plan_id, "autoflow_plan_id": plan_id,
            "expected_approved_revision": plan.approved_revision, "expected_approved_revision_hash": plan.approved_revision_hash}
        task.rationale_json = {**task.rationale_json, "autoflow_plan_payload": payload}
        task.state, task.state_updated_at = "planning", await inv._now(db)
        await service.queue.enqueue(db, kind="execute_task", idempotency_key=f"execute_task:{task_id}", payload=payload,
            priority=60, channel_profile_id=task.channel_profile_id, parent_queue_item_id=lease.id, commit=False)
        await complete_queue(db, lease)
        await db.commit()
        await db.refresh(task)
        return task
    except (inv.OwnedInventoryError, history.OwnedHistoryError) as error:
        return await hold(db, service, lease, task_id, str(error), envelope)
    except BaseException:
        await db.rollback()
        raise
