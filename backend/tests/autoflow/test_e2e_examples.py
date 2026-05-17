from __future__ import annotations

from pathlib import Path

import pytest

from app.autoflow.service import AutoFlowService
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowRequest
from app.services.material_service import _candidate_window_from_cluster, _material_result_metadata


REPO_ROOT = Path(__file__).resolve().parents[3]


def _assert_valid_plan(plan, *, intent_type: str, template_id: str) -> None:
    validation = validate_pipeline(plan.pipeline_definition)

    assert plan.intent.intent_type == intent_type
    assert plan.template_id == template_id
    assert plan.validation["valid"] is True
    assert validation.valid, [error.message for error in validation.errors]
    assert plan.pipeline_definition.nodes
    assert any(node.type == "export" for node in plan.pipeline_definition.nodes)


def _node_types(plan) -> list[str]:
    return [node.type for node in plan.pipeline_definition.nodes]


@pytest.mark.asyncio
async def test_cat_compilation_generates_valid_private_preview_plan():
    service = AutoFlowService()

    plan = await service.plan(
        AutoFlowRequest(
            prompt="我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要直接公开发布。",
            target_platforms=["youtube_shorts"],
        )
    )

    _assert_valid_plan(plan, intent_type="animal_compilation", template_id="animal_compilation_short")
    assert plan.request.source_policy == "owned_only"
    assert plan.request.publish_mode == "preview_only"
    assert plan.intent.aspect_ratio == "9:16"
    assert plan.intent.duration_sec == 30
    assert plan.rights["status"] == "allowed"
    assert plan.needs_review is False
    assert {candidate.source_type for candidate in plan.candidates} <= {"asset", "material"}
    assert all(candidate.url is None for candidate in plan.candidates)
    assert all(candidate.rights_status == "allowed" for candidate in plan.candidates)
    assert "youtube_upload" not in _node_types(plan)


@pytest.mark.asyncio
async def test_hot_topic_explainer_keeps_external_research_in_review_preview():
    service = AutoFlowService()

    plan = await service.plan(
        AutoFlowRequest(
            prompt="请做一个 45 秒热点解释短视频，讨论 AI 视频生成，竖屏，带讲解和字幕，只做草稿预览。",
            duration_sec=45,
            source_policy="research_only",
            publish_mode="preview_only",
            target_platforms=["youtube_shorts"],
        )
    )

    _assert_valid_plan(plan, intent_type="hot_topic_explainer", template_id="hot_topic_explainer_short")
    assert plan.intent.needs_voiceover is True
    assert plan.intent.needs_subtitles is True
    assert plan.request.publish_mode == "preview_only"
    assert plan.rights["status"] == "review_required"
    assert plan.needs_review is True
    assert any("external URL candidates require human review" in reason for reason in plan.rights["reasons"])
    external_candidates = [candidate for candidate in plan.candidates if candidate.url]
    assert external_candidates
    assert all(candidate.source_type == "youtube" for candidate in external_candidates)
    assert all(candidate.rights_status == "review_required" for candidate in external_candidates)
    assert "url_download" in _node_types(plan)
    assert "youtube_upload" not in _node_types(plan)


@pytest.mark.asyncio
async def test_material_library_remix_uses_owned_material_defaults_and_validates():
    service = AutoFlowService()

    plan = await service.plan(
        AutoFlowRequest(
            prompt="用素材库里的旅行素材做一个 20 秒海边日落治愈混剪，竖屏，先导出预览。",
            material_library_ids=["travel-library"],
        )
    )

    _assert_valid_plan(plan, intent_type="material_library_remix", template_id="material_library_remix")
    assert plan.request.source_policy == "owned_only"
    assert plan.request.publish_mode == "preview_only"
    assert plan.intent.keywords == ["海边", "日落", "旅行", "治愈"]
    assert plan.rights["status"] == "allowed"
    assert plan.rights["allowed_publish_modes"] == ["preview_only", "private_upload", "unlisted_upload"]
    assert plan.needs_review is False
    assert all(candidate.asset_id for candidate in plan.candidates)
    assert all(candidate.rights_status == "allowed" for candidate in plan.candidates)
    assert {"source", "trim", "concat_timeline", "transcode", "export"}.issubset(set(_node_types(plan)))


def test_material_search_result_metadata_keeps_source_visual_metadata():
    window = _candidate_window_from_cluster(
        [
            {
                "library_id": "library-1",
                "source_asset_id": "asset-1",
                "clip_id": "asset-1:1",
                "start_sec": 0.0,
                "end_sec": 5.0,
                "subtitle_text": "sunset over the water",
                "neighbor_clip_ids": [],
                "coarse_score": 0.76,
                "metadata": {
                    "aspect_ratio": "9:16",
                    "width": 1080,
                    "height": 1920,
                    "visual": {
                        "motion_score": 0.82,
                        "scene_change_score": 0.34,
                        "watermark_score": 0.08,
                        "suggested_crop": {"x": 0.0, "y": 0.0, "w": 1.0, "h": 1.0},
                    },
                },
            }
        ]
    )

    metadata = _material_result_metadata(window)

    assert metadata["aspect_ratio"] == "9:16"
    assert metadata["width"] == 1080
    assert metadata["height"] == 1920
    assert metadata["visual"]["motion_score"] == 0.82
    assert metadata["visual"]["scene_change_score"] == 0.34
    assert metadata["visual"]["watermark_score"] == 0.08
    assert window.visual_metadata == metadata


def test_autoflow_docs_and_demo_scripts_document_production_review_boundaries():
    docs = "\n".join(
        [
            (REPO_ROOT / "docs/autoflow/architecture.md").read_text(),
            (REPO_ROOT / "docs/autoflow/codex-task-guide.md").read_text(),
        ]
    )
    demo_scripts = "\n".join(
        [
            (REPO_ROOT / "scripts/autoflow_demo_cat_compilation.py").read_text(),
            (REPO_ROOT / "scripts/autoflow_demo_hot_topic.py").read_text(),
            (REPO_ROOT / "scripts/autoflow_demo_material_remix.py").read_text(),
        ]
    )

    assert "plan patch" in docs.lower()
    assert "public approval" in docs.lower()
    assert "db-backed metrics" in docs.lower()
    assert "review gate" in demo_scripts.lower()
    assert "db-backed metrics" in demo_scripts.lower()
