from __future__ import annotations
import uuid
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.api.job_helpers import create_jobs_or_400
from app.db import get_db
from app.schemas.job import TemplateExecuteRequest, TemplateBatchExecuteRequest, JobDetailResponse
from app.schemas.pipeline import (
    PipelineCreate, PipelineUpdate, PipelineResponse, PipelineListResponse,
    PipelineDefinition, ValidationResult,
)
from app.services.pipeline_service import (
    create_pipeline, get_pipeline, list_pipelines, update_pipeline,
    delete_pipeline, duplicate_pipeline, validate_definition,
)
from app.services.job_service import create_job
from app.services.job_runtime import start_or_defer_jobs, to_job_detail_response

router = APIRouter(prefix="/api/v1", tags=["pipelines"])


def _to_response(p) -> PipelineResponse:
    return PipelineResponse(
        id=str(p.id),
        name=p.name,
        description=p.description,
        definition=p.definition,
        is_template=p.is_template,
        template_tags=p.template_tags or [],
        created_at=p.created_at,
        updated_at=p.updated_at,
        version=p.version,
    )


@router.post("/pipelines", response_model=PipelineResponse, status_code=201)
async def create(data: PipelineCreate, db: AsyncSession = Depends(get_db)):
    pipeline = await create_pipeline(db, data)
    return _to_response(pipeline)


@router.get("/pipelines", response_model=PipelineListResponse)
async def list_all(
    skip: int = 0,
    limit: int = Query(default=50, le=100),
    is_template: bool | None = None,
    db: AsyncSession = Depends(get_db),
):
    items, total = await list_pipelines(db, skip, limit, is_template)
    return PipelineListResponse(
        items=[_to_response(p) for p in items],
        total=total,
    )


@router.get("/templates", response_model=PipelineListResponse)
async def list_templates(
    skip: int = 0,
    limit: int = Query(default=50, le=100),
    db: AsyncSession = Depends(get_db),
):
    items, total = await list_pipelines(db, skip, limit, is_template=True)
    return PipelineListResponse(
        items=[_to_response(p) for p in items],
        total=total,
    )


@router.get("/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def get_one(pipeline_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    pipeline = await get_pipeline(db, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return _to_response(pipeline)


@router.put("/pipelines/{pipeline_id}", response_model=PipelineResponse)
async def update(
    pipeline_id: uuid.UUID,
    data: PipelineUpdate,
    db: AsyncSession = Depends(get_db),
):
    pipeline = await update_pipeline(db, pipeline_id, data)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return _to_response(pipeline)


@router.delete("/pipelines/{pipeline_id}")
async def delete(pipeline_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    try:
        deleted = await delete_pipeline(db, pipeline_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return {"status": "deleted"}


@router.post("/pipelines/{pipeline_id}/duplicate", response_model=PipelineResponse, status_code=201)
async def duplicate(pipeline_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    pipeline = await duplicate_pipeline(db, pipeline_id)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return _to_response(pipeline)


@router.post("/pipelines/validate", response_model=ValidationResult)
async def validate(definition: PipelineDefinition):
    result = validate_definition(definition)
    return result


@router.post("/templates/{pipeline_id}/execute", response_model=JobDetailResponse, status_code=201)
async def execute_template(
    pipeline_id: uuid.UUID,
    data: TemplateExecuteRequest,
    db: AsyncSession = Depends(get_db),
):
    pipeline = await get_pipeline(db, pipeline_id)
    if not pipeline or not pipeline.is_template:
        raise HTTPException(status_code=404, detail="Template not found")

    try:
        job = await create_job(db, pipeline_id, input_overrides=data.inputs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    await start_or_defer_jobs(db, [job])
    return await to_job_detail_response(db, job)


@router.post("/templates/{pipeline_id}/execute/batch", response_model=list[JobDetailResponse], status_code=201)
async def execute_template_batch(
    pipeline_id: uuid.UUID,
    data: TemplateBatchExecuteRequest,
    db: AsyncSession = Depends(get_db),
):
    pipeline = await get_pipeline(db, pipeline_id)
    if not pipeline or not pipeline.is_template:
        raise HTTPException(status_code=404, detail="Template not found")

    jobs = await create_jobs_or_400(db, pipeline_id, data.items)

    await start_or_defer_jobs(db, jobs)
    return [await to_job_detail_response(db, job) for job in jobs]
