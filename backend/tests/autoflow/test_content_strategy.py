from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api import autoflow as autoflow_api_module
from app.api.autoflow import router
from app.autoflow.content_strategy import ContentStrategyService
from app.db import get_db


def test_content_strategy_generates_ranked_ideas_from_trends_and_template_performance():
    service = ContentStrategyService()

    ideas = service.generate_ideas(
        {
            "target_platforms": ["youtube_shorts"],
            "material_library_ids": ["pets-owned"],
            "source_policy": "owned_only",
            "count": 2,
        },
        trend_suggestions=[
            {
                "keyword": "cat fails",
                "opportunity_score": 0.77,
                "recommended_template": "animal_compilation_short",
                "estimated_material_count": 18,
                "rights_risk": 0.1,
            }
        ],
        template_performance=[
            {
                "template_id": "animal_compilation_short",
                "avg_virality_score": 0.8,
                "metric_count": 3,
            }
        ],
    )

    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["idea_id"].startswith("idea-")
    assert "cat fails" in idea["prompt"]
    assert idea["template_id"] == "animal_compilation_short"
    assert idea["opportunity_score"] == pytest.approx(0.81)
    assert idea["estimated_material_count"] == 18
    assert idea["risk"] == "low"
    assert idea["target_platforms"] == ["youtube_shorts"]


def test_content_strategy_falls_back_to_template_recommendations_without_trends():
    service = ContentStrategyService()

    ideas = service.generate_ideas(
        {
            "target_platforms": ["youtube_shorts"],
            "material_library_ids": ["owned-library"],
            "source_policy": "owned_only",
            "count": 3,
        },
        trend_suggestions=[],
        template_performance=[
            {
                "template_id": "animal_compilation_short",
                "avg_virality_score": 0.7,
                "metric_count": 2,
            }
        ],
    )

    assert ideas[0]["template_id"] == "animal_compilation_short"
    assert ideas[0]["risk"] == "low"
    assert ideas[0]["estimated_material_count"] > 0


@pytest.mark.asyncio
async def test_ideas_api_combines_trend_suggestions_and_metrics_summary():
    autoflow_api_module.metrics_service._metrics.clear()
    autoflow_api_module.trend_service._signals.clear()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        await client.post(
            "/api/v1/autoflow/trend-signals",
            json={
                "source": "manual",
                "keyword": "cat fails",
                "score": 0.9,
                "trend_growth": 0.8,
                "cross_platform_mentions": 0.7,
                "material_availability": 0.9,
                "competition": 0.2,
                "rights_risk": 0.1,
            },
        )

        ideas = await client.post(
            "/api/v1/autoflow/ideas",
            json={
                "target_platforms": ["youtube_shorts"],
                "material_library_ids": ["pets-owned"],
                "source_policy": "owned_only",
                "count": 5,
            },
        )

        assert ideas.status_code == 200
        payload = ideas.json()
        assert payload[0]["template_id"] == "animal_compilation_short"
        assert payload[0]["risk"] == "low"
        assert "cat fails" in payload[0]["prompt"]
