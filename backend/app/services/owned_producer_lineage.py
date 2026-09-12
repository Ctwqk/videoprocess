"""Exact durable artifact lineage for an owned upload, not its export sibling."""
from __future__ import annotations

from types import SimpleNamespace
import uuid

from sqlalchemy import select

from app.services import owned_seed_inventory as inv


def require_upload_lineage(graph, nodes, artifacts, *, job_id, upload_id, input_id):
    reason = "owned_inventory_artifact_lineage"
    members = [n for n in nodes if n["job_id"] == job_id]
    by_name = {n["node_id"]: n for n in members}
    specs = {n["id"]: n for n in graph["nodes"]}
    inv.require(len(by_name) == len(members) and by_name.keys() == specs.keys(), reason)
    uploads = [n for n in members if n["id"] == upload_id and n["node_type"] == "youtube_upload"]
    inv.require(len(uploads) == 1, reason)
    upload = uploads[0]
    inv.require(upload["status"] == "RUNNING" and upload["input_artifact_ids"] == [input_id], reason)
    by_id = {a["id"]: a for a in artifacts}
    current, seen = upload, set()
    while True:
        name = current["node_id"]
        inv.require(name not in seen, reason)
        seen.add(name)
        inv.require(current["error_message"] is None and current["retry_count"] == 0
            and (current is upload or current["status"] == "SUCCEEDED"), reason)
        parents = [e["source"] for e in graph["edges"] if e["target"] == name]
        if current["node_type"] == "source":
            inv.require(not parents and current["input_artifact_ids"] == [], reason)
            return
        inv.require(len(parents) == 1 and parents[0] in by_name, reason)
        previous = by_name[parents[0]]
        output_id = previous["output_artifact_id"]
        artifact = by_id.get(output_id)
        inv.require(output_id is not None and artifact is not None
            and artifact["job_id"] == job_id and artifact["node_execution_id"] == previous["id"]
            and current["input_artifact_ids"] == [output_id], reason)
        current = previous


async def require_local_upload(db, authority, context):
    """Use the already-fenced A1 snapshot, plus fresh durable plan/run bindings."""
    from app.models.autoflow import AutoFlowPlan, AutoFlowRun
    from app.services.owned_producer_fence import _one, require_owned_pipeline
    from app.services.owned_producer_workflow import require_policy, require_request

    if authority is None or authority.identity.inventory_id is None:
        return
    rows = authority.snapshot.rows.as_dict()
    task = _one(rows["production_tasks"], lambda r: r["id"] == authority.identity.task_id)
    plan_id, run_id = task["autoflow_plan_id"], task["autoflow_run_id"]
    inv.require(plan_id is not None and run_id is not None, "owned_inventory_plan_missing")
    plan = (await db.scalars(select(AutoFlowPlan).where(AutoFlowPlan.id == uuid.UUID(plan_id))
        .execution_options(populate_existing=True))).one_or_none()
    run = (await db.scalars(select(AutoFlowRun).where(AutoFlowRun.id == uuid.UUID(run_id))
        .execution_options(populate_existing=True))).one_or_none()
    inv.require(plan is not None and run is not None, "owned_inventory_plan_missing")
    payload = task["rationale_json"].get("autoflow_plan_payload", {})
    inv.require(run.job_id == context.job_id and run.plan_id == plan.id
        and str(run.pipeline_id) == task["pipeline_id"] and task["job_id"] == str(context.job_id)
        and plan.approved_revision is not None and plan.approved_revision == plan.execution_revision
        and plan.approved_revision == payload.get("expected_approved_revision")
        and plan.approved_revision_hash is not None and plan.approved_revision_hash == payload.get("expected_approved_revision_hash")
        and (plan.review_approved_at is not None or plan.agent_approved_by is not None), "owned_inventory_plan_changed")
    task_row = SimpleNamespace(**task)
    require_request(plan.request_json, task_row, authority.identity)
    require_policy(task_row, plan_id=plan_id)
    asset_id = task["agent_approval_evidence_json"]["owned_inventory"]["input_asset_id"]
    graph = require_owned_pipeline(plan.pipeline_definition, asset_id).model_dump(mode="json")
    job_id, upload_id, input_id = str(context.job_id), str(context.node_execution_id), str(context.input_artifact_id)
    job = _one(rows["jobs"], lambda r: r["id"] == job_id)
    inv.require(job["status"] == "RUNNING" and job["error_message"] is None
        and job["pipeline_snapshot"] == graph, "owned_inventory_pipeline_binding")
    require_upload_lineage(graph, rows["node_executions"], rows["artifacts"],
        job_id=job_id, upload_id=upload_id, input_id=input_id)
    specs = {n["id"]: n for n in graph["nodes"]}
    asset = _one(rows["assets"], lambda r: r["id"] == asset_id)
    for node in (n for n in rows["node_executions"] if n["job_id"] == job_id):
        spec = specs[node["node_id"]]
        config = {**spec["data"]["config"]}
        if spec["data"].get("asset_id"):
            config["asset_id"] = spec["data"]["asset_id"]
        inv.require(node["node_type"] == spec["type"] and node["node_config"] == config
            and node["error_message"] is None and node["retry_count"] == 0
            and node["status"] in {"PENDING", "QUEUED", "RUNNING", "SUCCEEDED"}, "owned_inventory_pipeline_binding")
        if node["id"] == upload_id:
            inv.require(config.get("title") == context.title and config.get("privacy") == context.privacy,
                "owned_inventory_pipeline_binding")
        if node["node_type"] == "source":
            artifact = _one(rows["artifacts"], lambda a: a["id"] == node["output_artifact_id"])
            inv.require(artifact["media_info"].get("asset_id") == asset_id
                and artifact["media_info"].get("source_asset_id") == asset_id
                and all(artifact[k] == asset[k] for k in ("filename", "mime_type", "file_size", "storage_backend", "storage_path")),
                "owned_inventory_source_changed")
