from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.capability_manifest import get_capability_manifest
from app.autoflow.content_strategy import ContentStrategyService
from app.autoflow.metrics_service import MetricsService
from app.autoflow.service import autoflow_service
from app.autoflow.template_library import TemplateLibrary
from app.autoflow.trend_service import TrendService
from app.db import get_db
from app.schemas.autoflow import AutoFlowExecuteRequest, AutoFlowPlan, AutoFlowRequest, AutoFlowRun, WorkflowTemplate


router = APIRouter(prefix="/api/v1/autoflow", tags=["autoflow"])
metrics_service = MetricsService()
trend_service = TrendService()
content_strategy_service = ContentStrategyService()


@router.post("/plan", response_model=AutoFlowPlan)
async def create_plan(data: AutoFlowRequest, db: AsyncSession | None = Depends(get_db)):
    return await autoflow_service.plan(data, db)


@router.get("/plans", response_model=list[AutoFlowPlan])
async def list_plans():
    return await autoflow_service.list_plans()


@router.get("/plans/{plan_id}", response_model=AutoFlowPlan)
async def get_plan(plan_id: str):
    plan = await autoflow_service.get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="AutoFlow plan not found")
    return plan


@router.post("/plans/{plan_id}/approve", response_model=AutoFlowPlan)
async def approve_plan(plan_id: str):
    plan = await autoflow_service.approve(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="AutoFlow plan not found")
    return plan


@router.post("/execute", response_model=AutoFlowRun)
async def execute_plan(data: AutoFlowExecuteRequest, db: AsyncSession | None = Depends(get_db)):
    try:
        return await autoflow_service.execute(data, db)
    except PermissionError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/runs", response_model=list[AutoFlowRun])
async def list_runs():
    return await autoflow_service.list_runs()


@router.get("/runs/{run_id}", response_model=AutoFlowRun)
async def get_run(run_id: str):
    run = await autoflow_service.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    return run


@router.post("/runs/{run_id}/collect-metrics")
async def collect_metrics(run_id: str, payload: dict[str, Any]):
    run = await autoflow_service.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    enriched = dict(payload)
    if run.plan_id:
        plan = await autoflow_service.get_plan(run.plan_id)
        if plan:
            enriched.setdefault("template_id", plan.template_id)
            enriched.setdefault("intent_type", plan.intent.intent_type)
    return metrics_service.save_manual_metrics(run_id, enriched)


@router.get("/runs/{run_id}/collect-metrics")
async def collect_metrics_placeholder(run_id: str):
    if not await autoflow_service.get_run(run_id):
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    return {"status": "not_implemented", "run_id": run_id}


@router.get("/runs/{run_id}/metrics")
async def list_run_metrics(run_id: str):
    if not await autoflow_service.get_run(run_id):
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    return metrics_service.list_for_run(run_id)


@router.get("/metrics/templates")
async def template_metrics_summary():
    return metrics_service.aggregate_by_template()


@router.post("/trend-signals")
async def create_trend_signal(payload: dict[str, Any]):
    return trend_service.add_signal(payload)


@router.get("/trend-suggestions")
async def trend_suggestions(
    source_policy: str = "owned_only",
    material_library_ids: str | None = None,
    limit: int = 10,
):
    libraries = _split_param(material_library_ids)
    return trend_service.suggest(
        material_library_ids=libraries,
        source_policy=source_policy,
        template_performance=metrics_service.aggregate_by_template(),
        limit=limit,
    )


@router.post("/ideas")
async def create_ideas(payload: dict[str, Any]):
    source_policy = str(payload.get("source_policy") or "owned_only")
    libraries = [str(item) for item in payload.get("material_library_ids") or []]
    suggestions = trend_service.suggest(
        material_library_ids=libraries,
        source_policy=source_policy,
        template_performance=metrics_service.aggregate_by_template(),
        limit=int(payload.get("count") or 10),
    )
    return content_strategy_service.generate_ideas(
        payload,
        trend_suggestions=suggestions,
        template_performance=metrics_service.aggregate_by_template(),
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
