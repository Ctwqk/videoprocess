from __future__ import annotations

import pytest

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.material_selector import CandidateSelectionResult, MaterialSelector
from app.autoflow.search_service import ExternalSearchResult, SearchService
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


class MultiPlatformSearchService(SearchService):
    async def search_material(self, intent: AutoFlowIntent, request: AutoFlowRequest, db=None, max_results: int = 8, **kwargs):
        return []

    async def search_external_platforms(self, intent: AutoFlowIntent, request: AutoFlowRequest, max_results: int = 8):
        return (await self.search_external_platforms_with_warnings(intent, request, max_results=max_results)).candidates

    async def search_external_platforms_with_warnings(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        max_results: int = 8,
    ):
        return ExternalSearchResult(
            candidates=[
                AutoFlowClipCandidate(
                    id=f"{platform}-1",
                    title=f"{platform} candidate",
                    source_type=platform,
                    url=f"https://example.test/{platform}/1.mp4",
                    rights_status="unknown",
                )
                for platform in request.source_platforms
            ],
            warnings=[],
        )


class FakePlatformClient:
    async def search(self, platform: str, query: str, max_results: int = 8):
        return [
            AutoFlowClipCandidate(
                id=f"{platform}-{index}",
                title=f"{query} {platform} candidate {index}",
                source_type=platform,
                url=f"https://media.example.test/{platform}/{index}.mp4",
                start_sec=0,
                end_sec=5,
                rights_status="review_required",
            )
            for index in range(1, min(max_results, 2) + 1)
        ]


class RecordingPlatformClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []

    async def search(self, platform: str, query: str, max_results: int = 8):
        self.calls.append((platform, query, max_results))
        return [
            AutoFlowClipCandidate(
                id=f"{platform}-{index}",
                title=f"{query} {platform} candidate {index}",
                source_type=platform,
                url=f"https://media.example.test/{platform}/{index}.mp4",
                rights_status="review_required",
            )
            for index in range(1, max_results + 1)
        ]


class EmptySelector:
    async def find_candidates(self, intent: AutoFlowIntent, request: AutoFlowRequest, db=None):
        return []


class WarningResultSelector:
    last_warnings = ["stale warning"]

    async def find_candidates_with_warnings(self, intent: AutoFlowIntent, request: AutoFlowRequest, db=None):
        return CandidateSelectionResult(
            candidates=[
                AutoFlowClipCandidate(
                    id="owned",
                    title=f"{intent.subject} owned",
                    source_type="asset",
                    asset_id="asset-owned",
                    rights_status="allowed",
                )
            ],
            warnings=["fresh warning"],
        )


@pytest.mark.asyncio
async def test_owned_only_selector_returns_no_external_urls():
    request = AutoFlowRequest(prompt="我要一个小猫视频集锦", source_policy="owned_only")

    candidates = await MaterialSelector(search_service=UnsafeSearchService()).find_candidates(intent(), request)

    assert candidates
    assert all(candidate.url is None for candidate in candidates)
    assert {candidate.source_type for candidate in candidates} <= {"asset", "material"}


@pytest.mark.asyncio
async def test_research_and_remix_external_candidates_require_review():
    selector = MaterialSelector(search_service=SearchService(platform_client=FakePlatformClient()))

    for policy in ("research_only", "remix_with_review"):
        request = AutoFlowRequest(prompt="搜索小猫外部素材", source_policy=policy)
        candidates = await selector.find_candidates(intent(policy), request)
        external = [candidate for candidate in candidates if candidate.url]

        assert external
        assert all(candidate.rights_status == "review_required" for candidate in external)


@pytest.mark.asyncio
async def test_research_selector_searches_requested_source_platforms():
    request = AutoFlowRequest(
        prompt="搜索外部素材",
        source_policy="research_only",
        source_platforms=["bilibili", "x", "xiaohongshu"],
    )

    candidates = await MaterialSelector(search_service=MultiPlatformSearchService()).find_candidates(
        intent("research_only"),
        request,
    )

    assert {candidate.source_type for candidate in candidates} == {"bilibili", "x", "xiaohongshu"}
    assert all(candidate.rights_status == "review_required" for candidate in candidates)


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
async def test_search_service_returns_safe_local_material_and_platform_candidates():
    request = AutoFlowRequest(prompt="我要小猫素材", source_policy="research_only")
    service = SearchService(platform_client=FakePlatformClient())

    material = await service.search_material(intent("research_only"), request)
    external = await service.search_external(intent("research_only"), request)

    assert material
    assert external
    assert all(candidate.url is None for candidate in material)
    assert {candidate.source_type for candidate in external} == {"youtube", "bilibili", "x", "xiaohongshu"}
    assert all("media.example.test" in (candidate.url or "") for candidate in external)


@pytest.mark.asyncio
async def test_external_search_caps_total_results_across_platforms():
    request = AutoFlowRequest(prompt="搜索外部素材", source_policy="research_only")
    client = RecordingPlatformClient()
    service = SearchService(platform_client=client)

    external = await service.search_external(intent("research_only"), request, max_results=5)

    assert len(external) == 5
    assert [call[0] for call in client.calls] == ["youtube", "bilibili", "x", "xiaohongshu"]
    assert {call[2] for call in client.calls} == {2}


@pytest.mark.asyncio
async def test_empty_source_platforms_skip_external_search():
    request = AutoFlowRequest(prompt="不要外部搜索", source_policy="research_only", source_platforms=[])
    client = RecordingPlatformClient()
    service = SearchService(platform_client=client)

    result = await service.search_external_platforms_with_warnings(intent("research_only"), request, max_results=5)

    assert result.candidates == []
    assert client.calls == []
    assert result.warnings == ["No supported source platforms selected; external search skipped."]


@pytest.mark.asyncio
async def test_autoflow_service_uses_selection_warnings_from_return_value():
    service = AutoFlowService(material_selector=WarningResultSelector(), clip_ranker=ClipRanker())

    plan = await service.plan(AutoFlowRequest(prompt="我要一个 30 秒小猫视频集锦"))

    assert "fresh warning" in plan.warnings
    assert "stale warning" not in plan.warnings


@pytest.mark.asyncio
async def test_autoflow_service_falls_back_and_ranks_when_selector_is_empty():
    service = AutoFlowService(material_selector=EmptySelector(), clip_ranker=ClipRanker())
    plan = await service.plan(AutoFlowRequest(prompt="我要一个 30 秒小猫视频集锦"))

    assert plan.candidates
    assert all(candidate.score_breakdown for candidate in plan.candidates)
    assert any("fewer than 5" in warning for warning in plan.warnings)
    assert plan.validation["valid"] is True
