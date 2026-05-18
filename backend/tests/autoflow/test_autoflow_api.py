from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.autoflow import router
from app.api import autoflow as autoflow_api_module
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


def _app_with_db(db_session):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db_session
    return app


def _plan_row(
    *,
    plan_id: uuid.UUID | None = None,
    prompt: str = "Review a database-backed AutoFlow plan",
    publish_mode: str = "preview_only",
    source_policy: str = "owned_only",
    rights_status: str = "allowed",
    status: str = "drafted",
):
    plan_uuid = plan_id or uuid.uuid4()
    request_json = {
        "prompt": prompt,
        "target_platforms": ["youtube_shorts"],
        "duration_sec": None,
        "aspect_ratio": "auto",
        "source_policy": source_policy,
        "publish_mode": publish_mode,
        "material_library_ids": [],
        "user_constraints": {},
    }
    intent_json = {
        "intent_type": "generic_video",
        "subject": "database-backed plan",
        "style": "auto",
        "duration_sec": 30,
        "aspect_ratio": "9:16",
        "target_platforms": ["youtube_shorts"],
        "source_policy": source_policy,
        "publish_mode": publish_mode,
        "keywords": [],
        "negative_keywords": [],
        "needs_voiceover": False,
        "needs_subtitles": True,
        "needs_bgm": True,
        "user_confirmation_questions": [],
    }
    kwargs = {
        "id": plan_uuid,
        "prompt": prompt,
        "intent_json": intent_json,
        "template_id": "material_library_remix",
        "pipeline_definition": {"nodes": [], "edges": [], "viewport": {"x": 0, "y": 0, "zoom": 1}},
        "candidates_json": [],
        "metadata_json": {},
        "rights_json": {
            "status": rights_status,
            "reasons": ["test rights decision"],
            "allowed_publish_modes": ["preview_only", "private_upload", "unlisted_upload"],
            "execute_allowed": rights_status != "blocked",
            "publish_allowed": rights_status == "allowed",
        },
        "validation_json": {"valid": True, "errors": [], "warnings": [], "repairs": []},
        "status": status,
    }
    if "request_json" in AutoFlowPlanModel.__table__.c:
        kwargs["request_json"] = request_json
    return AutoFlowPlanModel(**kwargs)


@pytest.mark.asyncio
async def test_autoflow_plan_approve_execute_api_without_live_database():
    _reset_autoflow_singletons()
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


@pytest.mark.asyncio
async def test_autoflow_graph_plan_api_builds_dog_cat_vertical_timeline_without_live_database():
    _reset_autoflow_singletons()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/autoflow/plan/graph",
            json={
                "prompt": "生成一个视频，上半部分是小狗，下半部分是小猫视频，上半部分先播放，下半部分后播放",
                "planning_mode": "ai_graph",
                "publish_mode": "private_upload",
                "target_platforms": ["youtube"],
            },
        )

    assert response.status_code == 200
    plan = response.json()
    node_types = [node["type"] for node in plan["pipeline_definition"]["nodes"]]
    upload = next(node for node in plan["pipeline_definition"]["nodes"] if node["type"] == "youtube_upload")
    assert plan["template_id"] == "ai_graph"
    assert plan["validation"]["valid"] is True
    assert "concat_vertical_timeline" in node_types
    assert upload["data"]["config"]["privacy"] == "private"
    assert plan["validation"]["graph_planning"]["attempts"][0]["source"] == "rule.dog_cat_vertical_timeline"
    assert plan["needs_review"] is True


@pytest.mark.asyncio
async def test_autoflow_plan_with_ai_graph_mode_falls_back_to_template_when_unavailable():
    _reset_autoflow_singletons()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: None

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要公开发布。",
                "planning_mode": "ai_graph",
                "target_platforms": ["youtube_shorts"],
            },
        )

    assert response.status_code == 200
    plan = response.json()
    assert plan["template_id"] == "animal_compilation_short"
    assert plan["validation"]["valid"] is True
    assert any("AI graph planner unavailable" in warning for warning in plan["warnings"])


@pytest.mark.asyncio
async def test_db_backed_plan_get_and_list_read_persisted_plan(autoflow_db_session):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a 30 second cat compilation backed by the database.",
                "target_platforms": ["youtube_shorts"],
            },
        )
        assert response.status_code == 200
        plan = response.json()

        autoflow_api_module.autoflow_service._plans.clear()

        fetched = await client.get(f"/api/v1/autoflow/plans/{plan['plan_id']}")
        assert fetched.status_code == 200
        assert fetched.json()["plan_id"] == plan["plan_id"]
        assert fetched.json()["request"]["prompt"] == plan["request"]["prompt"]

        listed = await client.get("/api/v1/autoflow/plans")
        assert listed.status_code == 200
        assert [item["plan_id"] for item in listed.json()] == [plan["plan_id"]]


@pytest.mark.asyncio
async def test_blocked_db_plan_cannot_be_approved(autoflow_db_session):
    _reset_autoflow_singletons()
    plan_id = uuid.uuid4()
    autoflow_db_session.add(
        _plan_row(plan_id=plan_id, rights_status="blocked", status="blocked", source_policy="owned_only")
    )
    await autoflow_db_session.commit()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/v1/autoflow/plans/{plan_id}/approve")

    assert response.status_code == 400
    assert "blocked" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_public_after_review_requires_public_approval_after_ordinary_approval(
    autoflow_db_session, monkeypatch
):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async def fake_create_pipeline(db, data):
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_create_job(db, pipeline_id):
        return SimpleNamespace(id=uuid.uuid4(), status=SimpleNamespace(value="PENDING"))

    async def fake_start_or_defer_jobs(db, jobs):
        return None

    monkeypatch.setattr("app.autoflow.service.create_pipeline", fake_create_pipeline)
    monkeypatch.setattr("app.autoflow.service.create_job", fake_create_job)
    monkeypatch.setattr("app.autoflow.service.start_or_defer_jobs", fake_start_or_defer_jobs)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a 30 second cat compilation for public publication after review.",
                "target_platforms": ["youtube_shorts"],
                "publish_mode": "public_after_review",
            },
        )
        assert response.status_code == 200
        plan = response.json()

        approved = await client.post(f"/api/v1/autoflow/plans/{plan['plan_id']}/approve")
        assert approved.status_code == 200
        assert approved.json()["review_approved_at"] is not None
        assert approved.json()["public_approved_at"] is None

        blocked_execute = await client.post("/api/v1/autoflow/execute", json={"plan_id": plan["plan_id"]})
        assert blocked_execute.status_code == 400
        assert "public" in blocked_execute.json()["detail"].lower()

        public_approved = await client.post(f"/api/v1/autoflow/plans/{plan['plan_id']}/approve-public")
        assert public_approved.status_code == 200
        assert public_approved.json()["public_approved_at"] is not None

        executed = await client.post("/api/v1/autoflow/execute", json={"plan_id": plan["plan_id"]})
        assert executed.status_code == 200
        run = executed.json()
        assert run["plan_id"] == plan["plan_id"]
        assert run["pipeline_id"] is not None
        assert run["job_id"] is not None

        autoflow_api_module.autoflow_service._runs.clear()

        fetched_run = await client.get(f"/api/v1/autoflow/runs/{run['run_id']}")
        assert fetched_run.status_code == 200
        assert fetched_run.json()["run_id"] == run["run_id"]


@pytest.mark.asyncio
async def test_public_after_review_does_not_trust_execute_payload_public_approval(
    autoflow_db_session, monkeypatch
):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async def fake_create_pipeline(db, data):
        pytest.fail("execution should be blocked before pipeline creation")

    monkeypatch.setattr("app.autoflow.service.create_pipeline", fake_create_pipeline)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a public cat compilation after review.",
                "target_platforms": ["youtube_shorts"],
                "publish_mode": "public_after_review",
            },
        )
        assert created.status_code == 200
        plan = created.json()

        approved = await client.post(f"/api/v1/autoflow/plans/{plan['plan_id']}/approve")
        assert approved.status_code == 200

        bypass = await client.post(
            "/api/v1/autoflow/execute",
            json={"plan_id": plan["plan_id"], "public_approved": True, "review_approved": True},
        )

    assert bypass.status_code == 400
    assert "public approval" in bypass.json()["detail"].lower()


@pytest.mark.asyncio
async def test_external_url_public_after_review_requires_persisted_public_approval(
    autoflow_db_session, monkeypatch
):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async def fake_create_pipeline(db, data):
        pytest.fail("external public execution should be blocked before pipeline creation")

    monkeypatch.setattr("app.autoflow.service.create_pipeline", fake_create_pipeline)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a public hot-topic explainer from external research.",
                "source_policy": "research_only",
                "publish_mode": "public_after_review",
                "target_platforms": ["youtube_shorts"],
            },
        )
        assert created.status_code == 200
        plan = created.json()
        assert plan["rights"]["status"] == "review_required"

        approved = await client.post(f"/api/v1/autoflow/plans/{plan['plan_id']}/approve")
        assert approved.status_code == 200
        assert approved.json()["review_approved_at"] is not None
        assert approved.json()["public_approved_at"] is None

        blocked = await client.post("/api/v1/autoflow/execute", json={"plan_id": plan["plan_id"]})

    assert blocked.status_code == 400
    assert "public approval" in blocked.json()["detail"].lower()


@pytest.mark.asyncio
async def test_db_execute_rejects_client_supplied_plan_graph(autoflow_db_session):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={"prompt": "Create a cat compilation for persisted execution."},
        )
        assert created.status_code == 200
        client_plan = created.json()
        client_plan["pipeline_definition"] = {"nodes": [], "edges": []}

        response = await client.post("/api/v1/autoflow/execute", json={"plan": client_plan})

    assert response.status_code == 400
    assert "plan_id" in response.json()["detail"]


@pytest.mark.asyncio
async def test_patch_plan_replaces_candidates_metadata_rebuilds_and_persists(autoflow_db_session):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a 30 second cat compilation with editable candidates.",
                "target_platforms": ["youtube_shorts"],
            },
        )
        assert created.status_code == 200
        plan = created.json()

        patched = await client.patch(
            f"/api/v1/autoflow/plans/{plan['plan_id']}",
            json={
                "selected_candidate_ids": ["replacement-owned"],
                "locked_candidate_ids": ["replacement-owned"],
                "replacement_candidates": [
                    {
                        "id": "replacement-owned",
                        "title": "Replacement owned clip",
                        "source_type": "asset",
                        "asset_id": "asset-replacement-owned",
                        "start_sec": 2,
                        "end_sec": 8,
                        "score": 0.99,
                        "rights_status": "allowed",
                        "metadata": {"license": "owned"},
                    }
                ],
                "metadata": {
                    "selected_title": "Reviewed replacement edit",
                    "description": "A reviewed replacement description.",
                    "tags": ["cat", "reviewed"],
                    "hashtags": ["#cat"],
                    "title_candidates": ["Reviewed replacement edit"],
                    "thumbnail_text_candidates": ["Reviewed"],
                    "platform_payloads": {},
                },
                "publish_mode": "private_upload",
                "rebuild_definition": True,
                "validate": True,
                "evaluate_rights": True,
            },
        )
        assert patched.status_code == 200
        payload = patched.json()
        assert [candidate["id"] for candidate in payload["candidates"]] == ["replacement-owned"]
        assert payload["candidates"][0]["metadata"]["locked"] is True
        assert payload["metadata"]["selected_title"] == "Reviewed replacement edit"
        assert payload["request"]["publish_mode"] == "private_upload"
        assert payload["validation"]["valid"] is True
        assert payload["rights"]["status"] == "allowed"
        assert any(node["id"] == "youtube_upload_1" for node in payload["pipeline_definition"]["nodes"])

        autoflow_api_module.autoflow_service._plans.clear()

        fetched = await client.get(f"/api/v1/autoflow/plans/{plan['plan_id']}")
        assert fetched.status_code == 200
        assert fetched.json()["metadata"]["selected_title"] == "Reviewed replacement edit"


@pytest.mark.asyncio
async def test_metadata_patch_rebuild_reapplies_validation_repairs(autoflow_db_session):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a 20 second funny dog compilation for YouTube Shorts.",
                "target_platforms": ["youtube_shorts"],
                "publish_mode": "private_upload",
            },
        )
        assert created.status_code == 200
        plan = created.json()
        assert plan["validation"]["valid"] is True
        assert plan["validation"]["repairs"] == ["invalid_param:montage_1.style"]

        patched = await client.patch(
            f"/api/v1/autoflow/plans/{plan['plan_id']}",
            json={
                "metadata": {
                    "selected_title": "Reviewed dog montage",
                    "description": "Metadata-only review edit.",
                },
                "rebuild_definition": True,
                "validate": True,
                "evaluate_rights": True,
            },
        )

    assert patched.status_code == 200
    payload = patched.json()
    assert payload["metadata"]["selected_title"] == "Reviewed dog montage"
    assert payload["validation"]["valid"] is True
    assert payload["validation"]["errors"] == []
    assert payload["validation"]["repairs"] == ["invalid_param:montage_1.style"]
    montage = next(node for node in payload["pipeline_definition"]["nodes"] if node["id"] == "montage_1")
    assert montage["data"]["config"]["style"] == "fast_cuts"


@pytest.mark.asyncio
async def test_metadata_patch_resets_previous_public_approval(autoflow_db_session):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a public cat compilation with editable metadata.",
                "target_platforms": ["youtube_shorts"],
                "publish_mode": "public_after_review",
            },
        )
        assert created.status_code == 200
        plan = created.json()

        approved = await client.post(f"/api/v1/autoflow/plans/{plan['plan_id']}/approve")
        assert approved.status_code == 200
        public_approved = await client.post(f"/api/v1/autoflow/plans/{plan['plan_id']}/approve-public")
        assert public_approved.status_code == 200
        assert public_approved.json()["public_approved_at"] is not None

        patched = await client.patch(
            f"/api/v1/autoflow/plans/{plan['plan_id']}",
            json={"metadata": {"selected_title": "Changed after approval"}, "evaluate_rights": False},
        )

    assert patched.status_code == 200
    payload = patched.json()
    assert payload["metadata"]["selected_title"] == "Changed after approval"
    assert payload["review_approved_at"] is None
    assert payload["public_approved_at"] is None
    assert payload["rights"].get("review_approved") is not True
    assert payload["rights"].get("public_approved") is not True
    assert payload["rights"].get("publish_allowed") is not True
    assert payload["status"] == "review_required"


@pytest.mark.asyncio
async def test_patch_plan_can_clear_last_candidate_lock(autoflow_db_session):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={"prompt": "Create a 30 second cat compilation with a lockable candidate."},
        )
        assert created.status_code == 200
        plan = created.json()
        first_candidate_id = plan["candidates"][0]["id"]

        locked = await client.patch(
            f"/api/v1/autoflow/plans/{plan['plan_id']}",
            json={
                "selected_candidate_ids": [first_candidate_id],
                "locked_candidate_ids": [first_candidate_id],
            },
        )
        assert locked.status_code == 200
        assert locked.json()["candidates"][0]["metadata"]["locked"] is True

        unlocked = await client.patch(
            f"/api/v1/autoflow/plans/{plan['plan_id']}",
            json={
                "selected_candidate_ids": [first_candidate_id],
                "locked_candidate_ids": [],
            },
        )

        assert unlocked.status_code == 200
        assert unlocked.json()["candidates"][0]["metadata"]["locked"] is False


@pytest.mark.asyncio
async def test_storyboard_endpoint_and_input_video_plan_persist_storyboard(autoflow_db_session):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        storyboard_response = await client.post(
            "/api/v1/autoflow/storyboard",
            json={
                "prompt": "我要一个 15 秒小猫视频，竖屏，可爱快节奏",
                "input_asset_id": "asset-cat",
                "target_duration": 15,
                "aspect_ratio": "9:16",
                "source_strategy": "input_video",
                "min_shots": 3,
                "max_shots": 3,
            },
        )
        assert storyboard_response.status_code == 200
        storyboard_payload = storyboard_response.json()
        assert len(storyboard_payload["storyboard"]["shots"]) == 3
        assert storyboard_payload["storyboard"]["shots"][0]["generation"]["prompt"]

        plan_response = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "我要一个 15 秒小猫视频，竖屏，可爱快节奏",
                "input_asset_id": "asset-cat",
                "target_platforms": ["youtube_shorts"],
                "duration_sec": 15,
                "aspect_ratio": "9:16",
                "source_strategy": "input_video",
                "min_shots": 3,
                "max_shots": 3,
            },
        )
        assert plan_response.status_code == 200
        plan = plan_response.json()
        assert plan["storyboard"]["subject"] == "小猫"
        assert plan["validation"]["valid"] is True
        assert [node["type"] for node in plan["pipeline_definition"]["nodes"]].count("smart_trim") == 3

        autoflow_api_module.autoflow_service._plans.clear()

        fetched = await client.get(f"/api/v1/autoflow/plans/{plan['plan_id']}")
        assert fetched.status_code == 200
        assert fetched.json()["storyboard"]["subject"] == "小猫"


@pytest.mark.asyncio
async def test_storyboard_material_plan_materializes_matches_and_keeps_missing_shot(
    autoflow_db_session, monkeypatch
):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)
    calls = []

    async def fake_materialize_material_search(db, payload):
        calls.append(payload.query)
        if len(calls) <= 2:
            return SimpleNamespace(id=uuid.uuid4()), [
                {
                    "id": f"result-{len(calls)}",
                    "title": f"Matched shot {len(calls)}",
                    "asset_id": f"asset-matched-{len(calls)}",
                    "source_asset_id": f"asset-source-{len(calls)}",
                    "start_sec": float(len(calls)),
                    "end_sec": float(len(calls) + 3),
                    "confidence": 0.88,
                }
            ]
        return SimpleNamespace(id=uuid.uuid4()), []

    monkeypatch.setattr(
        "app.services.material_service.materialize_material_search",
        fake_materialize_material_search,
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "我要一个 12 秒小猫视频，竖屏，可爱快节奏",
                "duration_sec": 12,
                "aspect_ratio": "9:16",
                "source_strategy": "material_library",
                "material_library_ids": [str(uuid.uuid4())],
                "min_shots": 3,
                "max_shots": 3,
            },
        )

    assert response.status_code == 200
    plan = response.json()
    statuses = [shot["match_status"] for shot in plan["storyboard"]["shots"]]
    assert statuses == ["matched", "matched", "missing"]
    assert [node["type"] for node in plan["pipeline_definition"]["nodes"]].count("source") == 2
    assert [node["type"] for node in plan["pipeline_definition"]["nodes"]].count("smart_trim") == 0
    assert plan["validation"]["valid"] is True


@pytest.mark.asyncio
async def test_execute_persists_failed_run_when_job_creation_fails(autoflow_db_session, monkeypatch):
    _reset_autoflow_singletons()
    app = _app_with_db(autoflow_db_session)

    async def fake_create_pipeline(db, data):
        return SimpleNamespace(id=uuid.uuid4())

    async def fake_create_job(db, pipeline_id):
        raise RuntimeError("job planner unavailable")

    monkeypatch.setattr("app.autoflow.service.create_pipeline", fake_create_pipeline)
    monkeypatch.setattr("app.autoflow.service.create_job", fake_create_job)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        created = await client.post(
            "/api/v1/autoflow/plan",
            json={
                "prompt": "Create a 30 second cat compilation that will fail job creation.",
                "target_platforms": ["youtube_shorts"],
            },
        )
        assert created.status_code == 200
        plan = created.json()

        executed = await client.post("/api/v1/autoflow/execute", json={"plan_id": plan["plan_id"]})
        assert executed.status_code == 200
        run = executed.json()
        assert run["status"] == "failed"
        assert "job planner unavailable" in run["error_message"]

        autoflow_api_module.autoflow_service._runs.clear()

        fetched_run = await client.get(f"/api/v1/autoflow/runs/{run['run_id']}")
        assert fetched_run.status_code == 200
        assert fetched_run.json()["status"] == "failed"
