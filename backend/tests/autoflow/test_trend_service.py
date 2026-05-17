from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import autoflow as autoflow_api_module
from app.api.autoflow import router
from app.autoflow.trend_service import TrendService
from app.db import get_db
from app.models.autoflow import AutoFlowPlan as AutoFlowPlanModel
from app.models.autoflow import AutoFlowRun as AutoFlowRunModel
from app.models.autoflow import ContentMetric, TrendSignal


@pytest.fixture
async def autoflow_db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in (
            AutoFlowPlanModel.__table__,
            AutoFlowRunModel.__table__,
            ContentMetric.__table__,
            TrendSignal.__table__,
        ):
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


def _reset_autoflow_singletons() -> None:
    autoflow_api_module.autoflow_service._plans.clear()
    autoflow_api_module.autoflow_service._runs.clear()
    autoflow_api_module.metrics_service._metrics.clear()
    autoflow_api_module.trend_service._signals.clear()


def test_trend_service_accepts_manual_signals_and_scores_opportunities():
    service = TrendService()
    signal = service.add_signal(
        {
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

    suggestions = service.suggest(
        material_library_ids=["pets-owned"],
        source_policy="owned_only",
        template_performance=[
            {
                "template_id": "animal_compilation_short",
                "avg_virality_score": 0.8,
                "intent_type": "animal_compilation",
            }
        ],
    )

    assert signal["keyword"] == "cat fails"
    assert suggestions[0]["keyword"] == "cat fails"
    assert suggestions[0]["recommended_template"] == "animal_compilation_short"
    assert suggestions[0]["opportunity_score"] == pytest.approx(0.77)
    assert "reason" in suggestions[0]


def test_trend_service_penalizes_rights_risk_and_sorts_descending():
    service = TrendService()
    service.add_signal(
        {
            "source": "manual",
            "keyword": "licensed product teardown",
            "score": 0.95,
            "trend_growth": 0.95,
            "cross_platform_mentions": 0.8,
            "material_availability": 0.8,
            "competition": 0.1,
            "rights_risk": 0.9,
        },
    )
    service.add_signal(
        {
            "source": "manual",
            "keyword": "owned studio tips",
            "score": 0.75,
            "trend_growth": 0.7,
            "cross_platform_mentions": 0.6,
            "material_availability": 1.0,
            "competition": 0.2,
            "rights_risk": 0.0,
        },
    )

    suggestions = service.suggest(source_policy="owned_only")

    assert [item["keyword"] for item in suggestions] == ["owned studio tips", "licensed product teardown"]
    assert suggestions[0]["opportunity_score"] > suggestions[1]["opportunity_score"]


@pytest.mark.asyncio
async def test_trend_api_creates_manual_signals_and_returns_suggestions():
    _reset_autoflow_singletons()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
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
        assert created.status_code == 200
        assert created.json()["signal_id"].startswith("trend-")

        suggestions = await client.get(
            "/api/v1/autoflow/trend-suggestions",
            params={"source_policy": "owned_only", "material_library_ids": "pets-owned"},
        )
        assert suggestions.status_code == 200
        payload = suggestions.json()
        assert payload[0]["keyword"] == "cat fails"
        assert payload[0]["recommended_template"] == "animal_compilation_short"


@pytest.mark.asyncio
async def test_trend_service_persists_signals_and_suggests_from_db(autoflow_db_session):
    service = TrendService()

    signal = await service.add_signal_db(
        {
            "source": "manual",
            "keyword": "cat fails",
            "score": 0.9,
            "trend_growth": 0.8,
            "cross_platform_mentions": 0.7,
            "material_availability": 0.9,
            "competition": 0.2,
            "rights_risk": 0.1,
        },
        autoflow_db_session,
    )

    assert signal["signal_id"].startswith("trend-")
    stored_count = await autoflow_db_session.scalar(select(func.count()).select_from(TrendSignal))
    assert stored_count == 1

    suggestions = await service.suggest_db(
        autoflow_db_session,
        material_library_ids=["pets-owned"],
        source_policy="owned_only",
        template_performance=[
            {
                "template_id": "animal_compilation_short",
                "avg_virality_score": 0.8,
                "intent_type": "animal_compilation",
            }
        ],
    )

    assert suggestions[0]["keyword"] == "cat fails"
    assert suggestions[0]["recommended_template"] == "animal_compilation_short"
    assert suggestions[0]["opportunity_score"] == pytest.approx(0.77)
