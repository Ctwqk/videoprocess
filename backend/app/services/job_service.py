from __future__ import annotations
import uuid
from typing import Any
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.pipeline import Pipeline
from app.schemas.pipeline import PipelineDefinition
from app.orchestrator.dag import validate_pipeline
from app.orchestrator.planner import compile_runtime_definition


def _set_nested_value(target: dict[str, Any], path: str, value: Any) -> None:
    if "." not in path:
        target[path] = value
        return

    current = target
    parts = path.split(".")
    for part in parts[:-1]:
        existing = current.get(part)
        if not isinstance(existing, dict):
            existing = {}
            current[part] = existing
        current = existing
    current[parts[-1]] = value


def _merge_override_dict(target: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and "." not in key:
            existing = target.get(key)
            nested = dict(existing) if isinstance(existing, dict) else {}
            target[key] = _merge_override_dict(nested, value)
            continue
        _set_nested_value(target, key, value)
    return target


def _normalize_node_overrides(input_overrides: dict[str, Any]) -> dict[str, dict[str, Any]]:
    node_overrides: dict[str, dict[str, Any]] = {}

    for key, value in input_overrides.items():
        if key == "asset_id":
            continue

        if "." in key and not isinstance(value, dict):
            node_id, param_name = key.split(".", 1)
            bucket = node_overrides.setdefault(node_id, {})
            _set_nested_value(bucket, param_name, value)
            continue

        if isinstance(value, dict):
            bucket = node_overrides.setdefault(key, {})
            _merge_override_dict(bucket, value)
            continue

        bucket = node_overrides.setdefault(key, {})
        bucket["asset_id"] = value

    return node_overrides


def _apply_input_overrides(
    definition: PipelineDefinition,
    input_overrides: dict[str, Any] | None = None,
) -> PipelineDefinition:
    if not input_overrides:
        return definition

    data = definition.model_dump()
    node_overrides = _normalize_node_overrides(input_overrides)
    top_level_asset_applied = False

    for node in data["nodes"]:
        config = dict(node["data"].get("config") or {})

        node_override = node_overrides.get(node["id"])
        if node_override:
            _merge_override_dict(config, dict(node_override))

        if (
            node["type"] == "source"
            and "asset_id" in input_overrides
            and not top_level_asset_applied
        ):
            config["asset_id"] = input_overrides["asset_id"]
            top_level_asset_applied = True

        node["data"]["config"] = config
        if "asset_id" in config:
            node["data"]["asset_id"] = config["asset_id"]

    return PipelineDefinition.model_validate(data)


async def _create_job_from_definition(
    db: AsyncSession,
    pipeline_id: uuid.UUID,
    definition: PipelineDefinition,
) -> Job:
    validation = validate_pipeline(definition)
    if not validation.valid:
        error_msgs = "; ".join(e.message for e in validation.errors)
        raise ValueError(f"Pipeline validation failed: {error_msgs}")

    job = Job(
        pipeline_id=pipeline_id,
        pipeline_snapshot=definition.model_dump(),
        status=JobStatus.PENDING,
    )
    db.add(job)
    await db.flush()

    for node in definition.nodes:
        config = dict(node.data.config or {})
        node_exec = NodeExecution(
            job_id=job.id,
            node_id=node.id,
            node_type=node.type,
            node_label=node.data.label or node.type,
            node_config=config,
            status=NodeStatus.PENDING,
        )
        if node.type == "source":
            asset_id = config.get("asset_id") or node.data.asset_id
            if asset_id:
                node_exec.node_config = {**node_exec.node_config, "asset_id": asset_id}
        db.add(node_exec)

    await db.commit()
    await db.refresh(job, attribute_names=["node_executions"])
    return job


async def create_job(
    db: AsyncSession,
    pipeline_id: uuid.UUID,
    input_overrides: dict[str, Any] | None = None,
) -> Job:
    """Create a new job from a pipeline. Does NOT start execution - that's the orchestrator's job.

    input_overrides: optional dict to override node configs before the job snapshot is created.
        Supported forms:
        - {"asset_id": "..."} for legacy first-source override
        - {"src": {"asset_id": "..."}, "trim": {"start_time": "00:00:01"}}
        - {"src.asset_id": "...", "trim.start_time": "00:00:01"}
    """
    pipeline = await db.get(Pipeline, pipeline_id)
    if not pipeline:
        raise ValueError(f"Pipeline {pipeline_id} not found")

    definition = PipelineDefinition.model_validate(pipeline.definition)
    effective_definition = _apply_input_overrides(definition, input_overrides)
    validation = validate_pipeline(effective_definition)
    if not validation.valid:
        error_msgs = "; ".join(e.message for e in validation.errors)
        raise ValueError(f"Pipeline validation failed: {error_msgs}")

    runtime_definition = compile_runtime_definition(effective_definition)
    return await _create_job_from_definition(db, pipeline_id, runtime_definition)


async def create_job_from_snapshot(
    db: AsyncSession,
    pipeline_id: uuid.UUID,
    pipeline_snapshot: dict,
) -> Job:
    definition = PipelineDefinition.model_validate(pipeline_snapshot)
    return await _create_job_from_definition(db, pipeline_id, definition)


async def get_job(db: AsyncSession, job_id: uuid.UUID) -> Job | None:
    """Get a job with all node executions eagerly loaded."""
    stmt = (
        select(Job)
        .where(Job.id == job_id)
        .options(selectinload(Job.node_executions))
    )
    result = await db.execute(stmt)
    return result.scalar_one_or_none()


async def list_jobs(
    db: AsyncSession,
    skip: int = 0,
    limit: int = 50,
    pipeline_id: uuid.UUID | None = None,
    status: str | None = None,
) -> tuple[list[Job], int]:
    base = select(Job)
    count_q = select(func.count()).select_from(Job)

    if pipeline_id:
        base = base.where(Job.pipeline_id == pipeline_id)
        count_q = count_q.where(Job.pipeline_id == pipeline_id)
    if status:
        base = base.where(Job.status == status)
        count_q = count_q.where(Job.status == status)

    total = (await db.execute(count_q)).scalar() or 0
    stmt = base.order_by(Job.submitted_at.desc()).offset(skip).limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all()), total


async def cancel_job(db: AsyncSession, job_id: uuid.UUID) -> Job | None:
    """Cancel a job and all its pending/queued/running node executions."""
    job = await get_job(db, job_id)
    if not job:
        return None
    if job.status in (JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED):
        return job  # already terminal

    job.status = JobStatus.CANCELLED
    for ne in job.node_executions:
        if ne.status in (NodeStatus.PENDING, NodeStatus.QUEUED, NodeStatus.RUNNING):
            ne.status = NodeStatus.CANCELLED

    await db.commit()
    await db.refresh(job, attribute_names=["node_executions"])
    return job


async def delete_job(db: AsyncSession, job_id: uuid.UUID) -> bool:
    job = await db.get(Job, job_id)
    if not job:
        return False

    if job.status in (JobStatus.PENDING, JobStatus.PLANNING, JobStatus.RUNNING):
        raise ValueError("Only terminal jobs can be deleted")

    await db.delete(job)
    await db.commit()
    return True
