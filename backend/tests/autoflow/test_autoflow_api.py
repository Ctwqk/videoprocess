from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.autoflow import router
from app.db import get_db


@pytest.mark.asyncio
async def test_autoflow_plan_approve_execute_api_without_live_database():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        caps = await client.get("/api/v1/autoflow/capabilities")
        assert caps.status_code == 200
        assert any(node["type_name"] == "source" for node in caps.json()["nodes"])

        templates = await client.get("/api/v1/autoflow/templates")
        assert templates.status_code == 200
        assert {item["id"] for item in templates.json()} >= {"animal_compilation_short", "material_library_remix"}

        response = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要公开发布。",
                "target_platforms": ["youtube_shorts"],
            },
        )
        assert response.status_code == 200
        plan = response.json()
        assert plan["intent"]["intent_type"] == "animal_compilation"
        assert plan["validation"]["valid"] is True

        approved = await client.post(f"/api/v1/autoflow/plans/{plan['plan_id']}/approve")
        assert approved.status_code == 200
        assert approved.json()["needs_review"] is False

        executed = await client.post("/api/v1/autoflow/execute", json={"plan_id": plan["plan_id"]})
        assert executed.status_code == 200
        run = executed.json()
        assert run["plan_id"] == plan["plan_id"]
        assert run["status"] == "pending"
