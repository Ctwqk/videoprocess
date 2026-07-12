from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.capability_manifest import get_capability_manifest
from app.autoflow.content_strategy import ContentStrategyService
from app.autoflow.metrics_service import MetricsService
from app.autoflow.service import OwnedInputAssetError, autoflow_service
from app.autoflow.template_library import TemplateLibrary
from app.autoflow.trend_service import TrendService
from app.db import get_db
from app.schemas.autoflow import (
    AutoFlowApprovalRequest,
    AutoFlowExecuteRequest,
    AutoFlowPlan,
    AutoFlowPlanPatch,
    AutoFlowRejectRequest,
    AutoFlowRequest,
    AutoFlowRun,
    AutoFlowStoryboardRequest,
    AutoFlowStoryboardResponse,
    WorkflowTemplate,
)


router = APIRouter(prefix="/api/v1/autoflow", tags=["autoflow"])
metrics_service = MetricsService()
trend_service = TrendService()
content_strategy_service = ContentStrategyService()


@router.post("/plan", response_model=AutoFlowPlan)
async def create_plan(data: AutoFlowRequest, db: AsyncSession | None = Depends(get_db)):
    try:
        return await autoflow_service.plan(data, db)
    except OwnedInputAssetError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/plan/graph", response_model=AutoFlowPlan)
async def create_graph_plan(data: AutoFlowRequest, db: AsyncSession | None = Depends(get_db)):
    try:
        return await autoflow_service.plan_graph(data, db)
    except OwnedInputAssetError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/storyboard", response_model=AutoFlowStoryboardResponse)
async def create_storyboard(data: AutoFlowStoryboardRequest):
    return await autoflow_service.storyboard(data)


@router.get("/plans", response_model=list[AutoFlowPlan])
async def list_plans(db: AsyncSession | None = Depends(get_db)):
    return await autoflow_service.list_plans(db)


@router.get("/plans/{plan_id}", response_model=AutoFlowPlan)
async def get_plan(plan_id: str, db: AsyncSession | None = Depends(get_db)):
    plan = await autoflow_service.get_plan(plan_id, db)
    if not plan:
        raise HTTPException(status_code=404, detail="AutoFlow plan not found")
    return plan


@router.patch("/plans/{plan_id}", response_model=AutoFlowPlan)
async def patch_plan(
    plan_id: str,
    data: AutoFlowPlanPatch,
    db: AsyncSession | None = Depends(get_db),
):
    try:
        plan = await autoflow_service.patch_plan(plan_id, data, db)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not plan:
        raise HTTPException(status_code=404, detail="AutoFlow plan not found")
    return plan


@router.post("/plans/{plan_id}/approve", response_model=AutoFlowPlan)
async def approve_plan(
    plan_id: str,
    data: AutoFlowApprovalRequest | None = None,
    db: AsyncSession | None = Depends(get_db),
):
    try:
        plan = await autoflow_service.approve(plan_id, db, review_notes=data.review_notes if data else None)
    except PermissionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not plan:
        raise HTTPException(status_code=404, detail="AutoFlow plan not found")
    return plan


@router.post("/plans/{plan_id}/approve-public", response_model=AutoFlowPlan)
async def approve_plan_public(
    plan_id: str,
    data: AutoFlowApprovalRequest | None = None,
    db: AsyncSession | None = Depends(get_db),
):
    try:
        plan = await autoflow_service.approve_public(plan_id, db, review_notes=data.review_notes if data else None)
    except PermissionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not plan:
        raise HTTPException(status_code=404, detail="AutoFlow plan not found")
    return plan


@router.post("/plans/{plan_id}/reject", response_model=AutoFlowPlan)
async def reject_plan(
    plan_id: str,
    data: AutoFlowRejectRequest | None = None,
    db: AsyncSession | None = Depends(get_db),
):
    plan = await autoflow_service.reject(plan_id, db, rejected_reason=data.rejected_reason if data else None)
    if not plan:
        raise HTTPException(status_code=404, detail="AutoFlow plan not found")
    return plan


@router.post("/execute", response_model=AutoFlowRun)
async def execute_plan(data: AutoFlowExecuteRequest, db: AsyncSession | None = Depends(get_db)):
    try:
        return await autoflow_service.execute(data, db)
    except OwnedInputAssetError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs", response_model=list[AutoFlowRun])
async def list_runs(db: AsyncSession | None = Depends(get_db)):
    return await autoflow_service.list_runs(db)


@router.get("/runs/{run_id}", response_model=AutoFlowRun)
async def get_run(run_id: str, db: AsyncSession | None = Depends(get_db)):
    run = await autoflow_service.get_run(run_id, db)
    if not run:
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    return run


@router.post("/runs/{run_id}/collect-metrics")
async def collect_metrics(run_id: str, payload: dict[str, Any], db: AsyncSession | None = Depends(get_db)):
    run = await autoflow_service.get_run(run_id, db)
    if not run:
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    if db is not None:
        try:
            return await metrics_service.save_manual_metrics_db(run_id, payload, db)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    enriched = dict(payload)
    if run.plan_id:
        plan = await autoflow_service.get_plan(run.plan_id, db)
        if plan:
            enriched.setdefault("template_id", plan.template_id)
            enriched.setdefault("intent_type", plan.intent.intent_type)
    return metrics_service.save_manual_metrics(run_id, enriched)


@router.get("/runs/{run_id}/collect-metrics")
async def collect_metrics_placeholder(run_id: str, db: AsyncSession | None = Depends(get_db)):
    if not await autoflow_service.get_run(run_id, db):
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    return {"status": "not_implemented", "run_id": run_id}


@router.get("/runs/{run_id}/metrics")
async def list_run_metrics(run_id: str, db: AsyncSession | None = Depends(get_db)):
    if not await autoflow_service.get_run(run_id, db):
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    if db is not None:
        return await metrics_service.list_for_run_db(run_id, db)
    return metrics_service.list_for_run(run_id)


@router.get("/metrics/templates")
async def template_metrics_summary(db: AsyncSession | None = Depends(get_db)):
    if db is not None:
        return await metrics_service.aggregate_by_template_db(db)
    return metrics_service.aggregate_by_template()


@router.post("/trend-signals")
async def create_trend_signal(payload: dict[str, Any], db: AsyncSession | None = Depends(get_db)):
    if db is not None:
        return await trend_service.add_signal_db(payload, db)
    return trend_service.add_signal(payload)


@router.get("/trend-suggestions")
async def trend_suggestions(
    source_policy: str = "owned_only",
    material_library_ids: str | None = None,
    limit: int = 10,
    db: AsyncSession | None = Depends(get_db),
):
    libraries = _split_param(material_library_ids)
    if db is not None:
        return await trend_service.suggest_db(
            db,
            material_library_ids=libraries,
            source_policy=source_policy,
            template_performance=await metrics_service.aggregate_by_template_db(db),
            limit=limit,
        )
    return trend_service.suggest(
        material_library_ids=libraries,
        source_policy=source_policy,
        template_performance=metrics_service.aggregate_by_template(),
        limit=limit,
    )


@router.post("/ideas")
async def create_ideas(payload: dict[str, Any], db: AsyncSession | None = Depends(get_db)):
    source_policy = str(payload.get("source_policy") or "owned_only")
    libraries = [str(item) for item in payload.get("material_library_ids") or []]
    if db is not None:
        template_performance = await metrics_service.aggregate_by_template_db(db)
        suggestions = await trend_service.suggest_db(
            db,
            material_library_ids=libraries,
            source_policy=source_policy,
            template_performance=template_performance,
            limit=int(payload.get("count") or 10),
        )
    else:
        template_performance = metrics_service.aggregate_by_template()
        suggestions = trend_service.suggest(
            material_library_ids=libraries,
            source_policy=source_policy,
            template_performance=template_performance,
            limit=int(payload.get("count") or 10),
        )
    return content_strategy_service.generate_ideas(
        payload,
        trend_suggestions=suggestions,
        template_performance=template_performance,
    )


@router.get("/templates", response_model=list[WorkflowTemplate])
async def list_templates():
    return TemplateLibrary().list_templates()


@router.get("/capabilities")
async def capabilities():
    return get_capability_manifest()


def _split_param(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]
