from __future__ import annotations
import uuid
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.job import Job
from app.models.pipeline import Pipeline
from app.schemas.pipeline import PipelineCreate, PipelineUpdate, PipelineDefinition, ValidationResult
from app.orchestrator.dag import validate_pipeline


async def create_pipeline(db: AsyncSession, data: PipelineCreate) -> Pipeline:
    pipeline = Pipeline(
        name=data.name,
        description=data.description,
        definition=data.definition.model_dump(),
        is_template=data.is_template,
        template_tags=data.template_tags,
    )
    db.add(pipeline)
    await db.commit()
    await db.refresh(pipeline)
    return pipeline


async def get_pipeline(db: AsyncSession, pipeline_id: uuid.UUID) -> Pipeline | None:
    return await db.get(Pipeline, pipeline_id)


async def list_pipelines(
    db: AsyncSession,
    skip: int = 0,
    limit: int = 50,
    is_template: bool | None = None,
) -> tuple[list[Pipeline], int]:
    base_query = select(Pipeline)
    count_query = select(func.count()).select_from(Pipeline)

    if is_template is not None:
        base_query = base_query.where(Pipeline.is_template == is_template)
        count_query = count_query.where(Pipeline.is_template == is_template)

    total = (await db.execute(count_query)).scalar() or 0
    stmt = base_query.order_by(Pipeline.updated_at.desc()).offset(skip).limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all()), total


async def update_pipeline(
    db: AsyncSession, pipeline_id: uuid.UUID, data: PipelineUpdate,
) -> Pipeline | None:
    pipeline = await db.get(Pipeline, pipeline_id)
    if not pipeline:
        return None

    update_data = data.model_dump(exclude_unset=True)
    if "definition" in update_data and update_data["definition"] is not None:
        update_data["definition"] = data.definition.model_dump()

    for key, value in update_data.items():
        setattr(pipeline, key, value)

    pipeline.version += 1
    await db.commit()
    await db.refresh(pipeline)
    return pipeline


async def delete_pipeline(db: AsyncSession, pipeline_id: uuid.UUID) -> bool:
    pipeline = await db.get(Pipeline, pipeline_id)
    if not pipeline:
        return False

    referencing_job = (
        await db.execute(
            select(Job.id).where(Job.pipeline_id == pipeline_id).limit(1)
        )
    ).scalar_one_or_none()

    if referencing_job is not None:
        if not pipeline.is_template:
            raise ValueError("Pipeline is referenced by existing jobs and cannot be deleted")

        pipeline.is_template = False
        pipeline.template_tags = []
        pipeline.version += 1
        await db.commit()
        return True

    try:
        await db.delete(pipeline)
        await db.commit()
    except IntegrityError:
        await db.rollback()
        raise ValueError("Pipeline could not be deleted because it was referenced while the delete was in progress")
    return True


async def duplicate_pipeline(db: AsyncSession, pipeline_id: uuid.UUID) -> Pipeline | None:
    original = await db.get(Pipeline, pipeline_id)
    if not original:
        return None

    copy = Pipeline(
        name=f"{original.name} (copy)",
        description=original.description,
        definition=original.definition,
        is_template=False,
        template_tags=list(original.template_tags) if original.template_tags else [],
    )
    db.add(copy)
    await db.commit()
    await db.refresh(copy)
    return copy


def validate_definition(definition: PipelineDefinition) -> ValidationResult:
    return validate_pipeline(definition)
