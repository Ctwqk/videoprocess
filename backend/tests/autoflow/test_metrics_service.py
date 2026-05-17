from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.autoflow import router
from app.autoflow.metrics_service import MetricsService
from app.db import get_db


def test_metrics_service_saves_manual_metrics_and_derives_rates():
    service = MetricsService()

    metric = service.save_manual_metrics(
        "run-1",
        {
            "template_id": "animal_compilation_short",
            "intent_type": "animal_compilation",
            "platform": "youtube_shorts",
            "platform_content_id": "yt-1",
            "views": 1000,
            "likes": 100,
            "comments": 20,
            "shares": 30,
            "watch_time_sec": 18_000,
            "avg_view_duration_sec": 18,
            "video_duration_sec": 30,
            "retention": [1.0, 0.8, 0.6],
        },
    )

    assert metric["run_id"] == "run-1"
    assert metric["like_rate"] == pytest.approx(0.1)
    assert metric["comment_rate"] == pytest.approx(0.02)
    assert metric["share_rate"] == pytest.approx(0.03)
    assert metric["avg_retention"] == pytest.approx(0.6)
    assert 0.0 < metric["virality_score"] <= 1.0
    assert service.list_for_run("run-1") == [metric]


def test_metrics_service_aggregates_by_template_id():
    service = MetricsService()
    service.save_manual_metrics(
        "run-1",
        {
            "template_id": "animal_compilation_short",
            "intent_type": "animal_compilation",
            "platform": "youtube_shorts",
            "views": 1000,
            "likes": 100,
            "comments": 20,
            "shares": 20,
            "avg_view_duration_sec": 15,
            "video_duration_sec": 30,
        },
    )
    service.save_manual_metrics(
        "run-2",
        {
            "template_id": "animal_compilation_short",
            "intent_type": "animal_compilation",
            "platform": "youtube_shorts",
            "views": 3000,
            "likes": 450,
            "comments": 30,
            "shares": 120,
            "avg_view_duration_sec": 24,
            "video_duration_sec": 30,
        },
    )
    service.save_manual_metrics(
        "run-3",
        {
            "template_id": "hot_topic_explainer_short",
            "intent_type": "hot_topic_explainer",
            "platform": "youtube_shorts",
            "views": 500,
            "likes": 25,
            "comments": 5,
            "shares": 5,
            "avg_view_duration_sec": 20,
            "video_duration_sec": 40,
        },
    )

    summary = service.aggregate_by_template()

    animal = next(item for item in summary if item["template_id"] == "animal_compilation_short")
    assert animal["metric_count"] == 2
    assert animal["total_views"] == 4000
    assert animal["avg_views"] == pytest.approx(2000)
    assert animal["avg_like_rate"] == pytest.approx(0.125)
    assert animal["avg_virality_score"] > 0


@pytest.mark.asyncio
async def test_metrics_api_collects_lists_and_summarizes_run_metrics():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        plan_response = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a 30 second cat compilation.",
                "target_platforms": ["youtube_shorts"],
            },
        )
        assert plan_response.status_code == 200
        plan = plan_response.json()

        run_response = await client.post("/api/v1/autoflow/execute", json={"plan_id": plan["plan_id"]})
        assert run_response.status_code == 200
        run = run_response.json()

        metric_response = await client.post(
            f"/api/v1/autoflow/runs/{run['run_id']}/collect-metrics",
            json={
                "platform": "youtube_shorts",
                "platform_content_id": "yt-api-1",
                "views": 2000,
                "likes": 220,
                "comments": 30,
                "shares": 80,
                "avg_view_duration_sec": 21,
                "video_duration_sec": 30,
            },
        )
        assert metric_response.status_code == 200
        metric = metric_response.json()
        assert metric["run_id"] == run["run_id"]
        assert metric["template_id"] == plan["template_id"]
        assert metric["like_rate"] == pytest.approx(0.11)

        list_response = await client.get(f"/api/v1/autoflow/runs/{run['run_id']}/metrics")
        assert list_response.status_code == 200
        assert [item["metric_id"] for item in list_response.json()] == [metric["metric_id"]]

        summary_response = await client.get("/api/v1/autoflow/metrics/templates")
        assert summary_response.status_code == 200
        assert any(item["template_id"] == plan["template_id"] for item in summary_response.json())
