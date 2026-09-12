"""Task-bound AutoFlow authority; caller owns the existing fenced transaction."""
from __future__ import annotations

import uuid
from types import SimpleNamespace

from sqlalchemy import select

from app.models.autoflow import AutoFlowPlan
from app.models.channel_agent import ProductionTask
from app.schemas.autoflow import AutoFlowRequest
from app.services import owned_seed_inventory as inv
from app.services.owned_producer_fence import lock_producer, require_owned_pipeline, require_real_pds


async def plan_owned(service, request, db):
    """Plan the pinned source with existing builders, then bind once after reentry."""
    from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowPlan as Plan
    binding = request.constraints.get("channelops", {})
    task_id = binding.get("production_task_id")
    authority = await lock_producer(db, task_id)
    if authority is None or authority.identity.inventory_id is None:
        await db.rollback()
        return None
    task_id = uuid.UUID(authority.identity.task_id)
    rows = authority.snapshot.rows.as_dict()
    task = SimpleNamespace(**next(t for t in rows["production_tasks"] if t["id"] == str(task_id)))
    require_request(request, task, authority.identity)
    require_policy(task)
    if task.autoflow_plan_id:
        plan = await service.get_plan(task.autoflow_plan_id, db)
        inv.require(plan is not None and plan.request == request, "owned_inventory_plan_binding")
        require_owned_pipeline(plan.pipeline_definition, request.input_asset_id)
        await db.commit()
        return plan
    require_plan_stage(task)
    asset = next(a for a in rows["assets"] if a["id"] == request.input_asset_id)
    prepared = authority.digest
    await db.rollback()
    intent = service.intent_parser.parse(request).model_copy(update={"source_policy": "owned_only", "publish_mode": "unlisted_upload"})
    template = service.template_library.select_template(intent)
    candidate = AutoFlowClipCandidate(id="owned:" + request.input_asset_id, title=task.title_seed,
        source_type="asset", asset_id=request.input_asset_id, rights_status="allowed",
        start_sec=0, end_sec=min(float(asset["media_info"].get("duration") or request.duration_sec), float(request.duration_sec)),
        metadata={**asset["media_info"], "license": "owned", "provenance": "generated"})
    metadata = service.metadata_generator.generate(intent, [candidate])
    graph = service.pipeline_builder.build(template, intent, [candidate], metadata)
    require_owned_pipeline(graph, request.input_asset_id)
    plan = Plan(plan_id=str(uuid.uuid4()), request=request, intent=intent, template_id=template.id,
        pipeline_definition=graph, candidates=[candidate], metadata=metadata,
        rights=service.rights_policy.evaluate(request, [candidate]).model_dump(),
        validation={"valid": True, "errors": [], "warnings": [], "repairs": []}, needs_review=True, status="review_required")
    current = await lock_producer(db, task_id)
    inv.require(current is not None and current.digest == prepared, "owned_inventory_producer_changed")
    row = (await db.scalars(select(ProductionTask).where(ProductionTask.id == task_id)
        .execution_options(populate_existing=True))).one()
    require_request(request, row, current.identity)
    require_policy(row)
    require_plan_stage(row)
    if row.autoflow_plan_id:
        existing = await service.get_plan(str(row.autoflow_plan_id), db)
        inv.require(existing is not None and existing.request == request, "owned_inventory_plan_binding")
        await db.commit()
        return existing
    plan = await service._save_plan(db, plan, commit=False)
    row.autoflow_plan_id = uuid.UUID(plan.plan_id)
    row.rationale_json = {**row.rationale_json, "autoflow_plan_payload": {"plan_id": plan.plan_id}}
    await db.commit()
    return plan


def require_plan_stage(task):
    inv.require(task.state == "selected" and task.job_id is None and task.pipeline_id is None
        and task.autoflow_run_id is None, "owned_inventory_plan_stage")


def require_request(request, task, identity):
    request = request if isinstance(request, AutoFlowRequest) else AutoFlowRequest.model_validate(request)
    evidence = task.agent_approval_evidence_json["owned_inventory"]
    binding = request.constraints.get("channelops", {})
    inv.require(request.prompt == task.prompt and request.input_asset_id == evidence["input_asset_id"]
        and request.source_strategy == "input_video" and request.planning_mode == "template"
        and request.source_policy == "owned_only" and request.publish_mode == "unlisted_upload"
        and request.target_platforms == ["youtube"] and not request.source_platforms
        and not request.material_library_ids and not request.allow_video_generation
        and binding.get("production_task_id") == identity.task_id,
        "owned_inventory_plan_request")
    return request


def require_policy(task, *, plan_id=None):
    evidence = task.agent_approval_evidence_json
    owned = evidence.get("owned_inventory", {})
    candidate = evidence.get("candidate_pds_request", {})
    require_real_pds(evidence.get("candidate_pds"))
    inv.require(candidate.get("actor_id") == str(task.target_account_id)
        and candidate.get("action_type") == "candidate_accept" and candidate.get("platform") == "youtube"
        and candidate.get("content") == {"title": task.title_seed, "description": task.prompt}
        and candidate.get("context", {}).get("candidate_id") == f"owned_inventory:{owned.get('inventory_id')}:{owned.get('item_id')}"
        and candidate.get("context", {}).get("owned_inventory") == owned, "owned_inventory_pds_binding")
    if plan_id is None:
        return
    envelope = evidence.get("plan_pds", {})
    require_real_pds(envelope.get("response"))
    request = envelope.get("request", {})
    context = request.get("context", {})
    inv.require(request.get("actor_id") == str(task.target_account_id)
        and request.get("action_type") == "plan_approval" and request.get("platform") == "youtube"
        and request.get("content") == {"title": task.title_seed, "description": task.prompt}
        and context.get("production_task_id") == str(task.id)
        and context.get("autoflow_plan_id") == str(plan_id)
        and ("channel_id" not in context or context["channel_id"] == str(task.channel_profile_id)),
        "owned_inventory_pds_binding")


async def lock_plan(db, plan_id, *, public=False, queue_lease=None):
    """Resolve only the durable task link, never review_notes or caller evidence."""
    plan_uuid = uuid.UUID(str(plan_id))
    ids = list((await db.scalars(select(ProductionTask.id).where(ProductionTask.autoflow_plan_id == plan_uuid))).all())
    inv.require(len(ids) <= 1, "owned_inventory_plan_binding")
    authority = await lock_producer(db, ids[0] if ids else None, queue_lease=queue_lease)
    if authority is None or authority.identity.inventory_id is None:
        return authority
    inv.require(not public, "owned_inventory_unlisted_only")
    task = (await db.scalars(select(ProductionTask).where(ProductionTask.id == uuid.UUID(authority.identity.task_id))
        .execution_options(populate_existing=True))).one()
    inv.require(task.autoflow_plan_id == plan_uuid, "owned_inventory_plan_binding")
    row = (await db.scalars(select(AutoFlowPlan).where(AutoFlowPlan.id == plan_uuid)
        .execution_options(populate_existing=True))).one_or_none()
    inv.require(row is not None, "owned_inventory_plan_missing")
    require_request(row.request_json, task, authority.identity)
    require_owned_pipeline(row.pipeline_definition, task.agent_approval_evidence_json["owned_inventory"]["input_asset_id"])
    require_policy(task, plan_id=plan_uuid)
    return authority


async def require_approved_plan(db, task, identity):
    """New-effect check inside an existing producer phase; no locks or commit."""
    inv.require(task.autoflow_plan_id is not None, "owned_inventory_plan_missing")
    plan = (await db.scalars(select(AutoFlowPlan).where(AutoFlowPlan.id == task.autoflow_plan_id)
        .execution_options(populate_existing=True))).one_or_none()
    inv.require(plan is not None, "owned_inventory_plan_missing")
    payload = task.rationale_json.get("autoflow_plan_payload", {})
    inv.require(plan.approved_revision is not None and plan.approved_revision == plan.execution_revision
        and plan.approved_revision == payload.get("expected_approved_revision")
        and plan.approved_revision_hash is not None and plan.approved_revision_hash == payload.get("expected_approved_revision_hash")
        and (plan.review_approved_at is not None or plan.agent_approved_by is not None), "owned_inventory_plan_changed")
    require_request(plan.request_json, task, identity)
    require_policy(task, plan_id=plan.id)
    require_owned_pipeline(plan.pipeline_definition, task.agent_approval_evidence_json["owned_inventory"]["input_asset_id"])
    return plan


async def lock_plan_mutation(db, plan_id):
    """Rejection is still allowed after expiry, but cannot race a final POST."""
    query = select(ProductionTask).where(ProductionTask.autoflow_plan_id == uuid.UUID(str(plan_id)))
    rows = list((await db.scalars(query)).all())
    inv.require(len(rows) <= 1, "owned_inventory_plan_binding")
    if not rows:
        await inv.lock_history_schedule(db)
        inv.require(not (await db.scalars(query)).all(), "owned_inventory_plan_binding")
        return
    task_id, channel_id = rows[0].id, rows[0].channel_profile_id
    await inv.lock_history_channel_mutation(db, channel_id)
    current = list((await db.scalars(query.execution_options(populate_existing=True))).all())
    inv.require(len(current) == 1 and current[0].id == task_id and current[0].channel_profile_id == channel_id,
        "owned_inventory_plan_binding")
