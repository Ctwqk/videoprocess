from __future__ import annotations

import pytest

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.material_selector import MaterialSelector
from app.autoflow.search_service import SearchService
from app.autoflow.service import AutoFlowService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest


def intent(policy: str = "owned_only") -> AutoFlowIntent:
    return AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        duration_sec=30,
        aspect_ratio="9:16",
        source_policy=policy,
        keywords=["小猫", "cat", "kitten"],
    )


class UnsafeSearchService:
    async def search_material(self, intent: AutoFlowIntent, request: AutoFlowRequest, db=None, max_results: int = 8, **kwargs):
        return [
            AutoFlowClipCandidate(
                id="owned",
                title=f"{intent.subject} owned",
                source_type="asset",
                asset_id="asset-owned",
                rights_status="allowed",
            ),
            AutoFlowClipCandidate(
                id="bad-external-from-material",
                title="external should not leak",
                source_type="youtube",
                url="https://example.test/bad.mp4",
                rights_status="review_required",
            ),
        ]

    async def search_external(self, intent: AutoFlowIntent, request: AutoFlowRequest, max_results: int = 8):
        return [
            AutoFlowClipCandidate(
                id="external",
                title=f"{intent.subject} external",
                source_type="youtube",
                url="https://example.test/external.mp4",
                rights_status="unknown",
            )
        ]


class EmptySelector:
    async def find_candidates(self, intent: AutoFlowIntent, request: AutoFlowRequest, db=None):
        return []


@pytest.mark.asyncio
async def test_owned_only_selector_returns_no_external_urls():
    request = AutoFlowRequest(prompt="我要一个小猫视频集锦", source_policy="owned_only")

    candidates = await MaterialSelector(search_service=UnsafeSearchService()).find_candidates(intent(), request)

    assert candidates
    assert all(candidate.url is None for candidate in candidates)
    assert {candidate.source_type for candidate in candidates} <= {"asset", "material"}


@pytest.mark.asyncio
async def test_research_and_remix_external_candidates_require_review():
    selector = MaterialSelector(search_service=SearchService())

    for policy in ("research_only", "remix_with_review"):
        request = AutoFlowRequest(prompt="搜索小猫外部素材", source_policy=policy)
        candidates = await selector.find_candidates(intent(policy), request)
        external = [candidate for candidate in candidates if candidate.url]

        assert external
        assert all(candidate.rights_status == "review_required" for candidate in external)


@pytest.mark.asyncio
async def test_licensed_only_selector_keeps_only_licensed_material_candidates():
    request = AutoFlowRequest(prompt="使用授权小猫素材", source_policy="licensed_only")

    candidates = await MaterialSelector(search_service=SearchService()).find_candidates(intent("licensed_only"), request)

    assert candidates
    assert all(candidate.url is None for candidate in candidates)
    assert {candidate.source_type for candidate in candidates} <= {"asset", "material"}
    assert all(candidate.metadata.get("license") for candidate in candidates)
    assert all(candidate.rights_status == "allowed" for candidate in candidates)


@pytest.mark.asyncio
async def test_search_service_returns_safe_local_material_and_external_stubs():
    request = AutoFlowRequest(prompt="我要小猫素材", source_policy="research_only")
    service = SearchService()

    material = await service.search_material(intent("research_only"), request)
    external = await service.search_external(intent("research_only"), request)

    assert material
    assert external
    assert all(candidate.url is None for candidate in material)
    assert all("example.test" in (candidate.url or "") for candidate in external)


@pytest.mark.asyncio
async def test_autoflow_service_falls_back_and_ranks_when_selector_is_empty():
    service = AutoFlowService(material_selector=EmptySelector(), clip_ranker=ClipRanker())
    plan = await service.plan(AutoFlowRequest(prompt="我要一个 30 秒小猫视频集锦"))

    assert plan.candidates
    assert all(candidate.score_breakdown for candidate in plan.candidates)
    assert any("fewer than 5" in warning for warning in plan.warnings)
    assert plan.validation["valid"] is True
