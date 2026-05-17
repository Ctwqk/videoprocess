from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.capability_manifest import get_capability_manifest
from app.autoflow.service import autoflow_service
from app.autoflow.template_library import TemplateLibrary
from app.db import get_db
from app.schemas.autoflow import AutoFlowExecuteRequest, AutoFlowPlan, AutoFlowRequest, AutoFlowRun, WorkflowTemplate


router = APIRouter(prefix="/api/v1/autoflow", tags=["autoflow"])


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


@router.get("/runs/{run_id}/collect-metrics")
async def collect_metrics_placeholder(run_id: str):
    if not await autoflow_service.get_run(run_id):
        raise HTTPException(status_code=404, detail="AutoFlow run not found")
    return {"status": "not_implemented", "run_id": run_id}


@router.get("/templates", response_model=list[WorkflowTemplate])
async def list_templates():
    return TemplateLibrary().list_templates()


@router.get("/capabilities")
async def capabilities():
    return get_capability_manifest()
