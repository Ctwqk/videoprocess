from __future__ import annotations

import pytest

from app.autoflow.material_selector import MaterialSelector
from app.autoflow.search_service import SearchService
from app.autoflow.service import AutoFlowService
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowRequest


class FakePlatformClient:
    async def search(self, platform: str, query: str, max_results: int = 8):
        return [
            AutoFlowClipCandidate(
                id=f"{platform}-{index}",
                title=f"{query} {platform} reference {index}",
                source_type=platform,
                url=f"https://media.example.test/{platform}/{index}.mp4",
                start_sec=0,
                end_sec=5,
                rights_status="review_required",
            )
            for index in range(1, min(max_results, 4) + 1)
        ]


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
    service = AutoFlowService(material_selector=MaterialSelector(SearchService(platform_client=FakePlatformClient())))

    plan = await service.plan(
        AutoFlowRequest(
            prompt="请做一个 45 秒热点解释短视频，讨论 AI 视频生成，竖屏，带讲解和字幕，只做草稿预览。",
            duration_sec=45,
            source_policy="research_only",
            publish_mode="preview_only",
            target_platforms=["youtube_shorts"],
            source_platforms=["youtube"],
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
