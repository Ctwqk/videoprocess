from __future__ import annotations

import pytest

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.material_selector import MaterialSelector
from app.autoflow.search_service import SearchService
from app.autoflow.service import AutoFlowService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest


VALID_LIBRARY_ID = "00000000-0000-0000-0000-000000000101"


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


class LicensedSearchService:
    async def search_material(self, intent: AutoFlowIntent, request: AutoFlowRequest, db=None, max_results: int = 8, **kwargs):
        return [
            AutoFlowClipCandidate(
                id="licensed",
                title=f"{intent.subject} licensed",
                source_type="material",
                asset_id="asset-licensed",
                rights_status="allowed",
                metadata={"license": "standard-library"},
            ),
            AutoFlowClipCandidate(
                id="unlicensed",
                title=f"{intent.subject} unlicensed",
                source_type="material",
                asset_id="asset-unlicensed",
                rights_status="allowed",
            ),
        ]

    async def search_external(self, intent: AutoFlowIntent, request: AutoFlowRequest, max_results: int = 8):
        return []


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
    selector = MaterialSelector(search_service=UnsafeSearchService())

    for policy in ("research_only", "remix_with_review"):
        request = AutoFlowRequest(prompt="搜索小猫外部素材", source_policy=policy)
        candidates = await selector.find_candidates(intent(policy), request)
        external = [candidate for candidate in candidates if candidate.url]

        assert external
        assert all(candidate.rights_status == "review_required" for candidate in external)


@pytest.mark.asyncio
async def test_licensed_only_selector_keeps_only_licensed_material_candidates():
    request = AutoFlowRequest(prompt="使用授权小猫素材", source_policy="licensed_only")

    candidates = await MaterialSelector(search_service=LicensedSearchService()).find_candidates(intent("licensed_only"), request)

    assert candidates
    assert all(candidate.url is None for candidate in candidates)
    assert {candidate.source_type for candidate in candidates} <= {"asset", "material"}
    assert all(candidate.metadata.get("license") for candidate in candidates)
    assert all(candidate.rights_status == "allowed" for candidate in candidates)


@pytest.mark.asyncio
async def test_search_service_returns_empty_without_db_or_material_libraries():
    request = AutoFlowRequest(prompt="我要小猫素材", source_policy="research_only")
    service = SearchService()

    material = await service.search_material(intent("research_only"), request)
    material_without_libraries = await service.search_material(
        intent("research_only"),
        AutoFlowRequest(prompt="我要小猫素材", source_policy="research_only", material_library_ids=[]),
        db=object(),
    )

    assert material == []
    assert material_without_libraries == []


@pytest.mark.asyncio
async def test_search_material_ignores_non_uuid_library_ids(monkeypatch):
    async def fail_materialize(db, payload):
        pytest.fail("invalid material library ids should not reach material search")

    async def fail_preview(db, payload):
        pytest.fail("invalid material library ids should not reach preview search")

    monkeypatch.setattr("app.autoflow.search_service.materialize_material_search", fail_materialize, raising=False)
    monkeypatch.setattr("app.autoflow.search_service.preview_material_search", fail_preview, raising=False)

    request = AutoFlowRequest(
        prompt="用素材库里的旅行素材做一个 20 秒混剪",
        source_policy="owned_only",
        material_library_ids=["travel-library"],
    )

    candidates = await SearchService().search_material(intent("owned_only"), request, db=object())

    assert candidates == []


@pytest.mark.asyncio
async def test_search_material_uses_material_service_materialized_results(monkeypatch):
    calls = {}

    async def fake_materialize(db, payload):
        calls["materialize"] = payload
        return object(), [
            {
                "id": "result-1",
                "title": "Kitten jump",
                "asset_id": "asset-cut-1",
                "source_asset_id": "source-asset-1",
                "library_id": "library-1",
                "start_sec": 1.25,
                "end_sec": 4.75,
                "subtitle_text": "tiny jump",
                "coarse_score": 0.42,
                "lighthouse_score": 0.81,
                "confidence": 0.9,
                "metadata": {"visual": {"motion": "fast"}},
            }
        ]

    async def fake_preview(db, payload):
        pytest.fail("preview should not be used when materialization succeeds")

    monkeypatch.setattr("app.services.material_service.materialize_material_search", fake_materialize)
    monkeypatch.setattr("app.services.material_service.preview_material_search", fake_preview)
    monkeypatch.setattr("app.autoflow.search_service.materialize_material_search", fake_materialize, raising=False)
    monkeypatch.setattr("app.autoflow.search_service.preview_material_search", fake_preview, raising=False)

    request = AutoFlowRequest(
        prompt="我要小猫素材",
        source_policy="owned_only",
        material_library_ids=[VALID_LIBRARY_ID],
    )

    candidates = await SearchService().search_material(intent("owned_only"), request, db=object(), max_results=3)

    assert calls["materialize"].source_library_ids == [VALID_LIBRARY_ID]
    assert calls["materialize"].top_k == 3
    assert candidates == [
        AutoFlowClipCandidate(
            id="result-1",
            title="Kitten jump",
            source_type="material",
            asset_id="asset-cut-1",
            start_sec=1.25,
            end_sec=4.75,
            rights_status="allowed",
            metadata={
                "library_id": "library-1",
                "source_asset_id": "source-asset-1",
                "asset_id": "asset-cut-1",
                "coarse": 0.42,
                "lighthouse": 0.81,
                "confidence": 0.9,
                "subtitle": "tiny jump",
                "visual": {"motion": "fast"},
            },
        )
    ]


@pytest.mark.asyncio
async def test_search_material_falls_back_to_preview_asset_when_materialization_unavailable(monkeypatch):
    async def fake_materialize(db, payload):
        raise RuntimeError("materialization unavailable")

    async def fake_preview(db, payload):
        return object(), [
            {
                "rank": 1,
                "library_id": "library-1",
                "source_asset_id": "source-asset-1",
                "start_sec": 2.0,
                "end_sec": 5.0,
                "subtitle_text": "preview subtitle",
                "coarse_score": 0.5,
                "lighthouse_score": 0.7,
                "confidence": 0.8,
                "metadata": {"visual": {"objects": ["cat"]}},
            }
        ]

    monkeypatch.setattr("app.services.material_service.materialize_material_search", fake_materialize)
    monkeypatch.setattr("app.services.material_service.preview_material_search", fake_preview)
    monkeypatch.setattr("app.autoflow.search_service.materialize_material_search", fake_materialize, raising=False)
    monkeypatch.setattr("app.autoflow.search_service.preview_material_search", fake_preview, raising=False)

    request = AutoFlowRequest(
        prompt="我要小猫素材",
        source_policy="owned_only",
        material_library_ids=[VALID_LIBRARY_ID],
    )

    candidates = await SearchService().search_material(intent("owned_only"), request, db=object(), max_results=2)

    assert len(candidates) == 1
    assert candidates[0].source_type == "material"
    assert candidates[0].asset_id == "source-asset-1"
    assert candidates[0].metadata["subtitle"] == "preview subtitle"
    assert candidates[0].metadata["visual"] == {"objects": ["cat"]}


@pytest.mark.asyncio
async def test_external_provider_stubs_return_empty_when_unconfigured():
    request = AutoFlowRequest(prompt="搜索小猫外部素材", source_policy="research_only")
    service = SearchService()

    external_groups = [
        await service.search_external(intent("research_only"), request),
        await service.search_youtube("小猫", max_results=3),
        await service.search_x("小猫", max_results=3),
        await service.search_xiaohongshu("小猫", max_results=3),
        await service.search_bilibili("小猫", max_results=3),
    ]

    assert external_groups == [[], [], [], [], []]


@pytest.mark.asyncio
async def test_autoflow_service_falls_back_and_ranks_when_selector_is_empty():
    service = AutoFlowService(material_selector=EmptySelector(), clip_ranker=ClipRanker())
    plan = await service.plan(AutoFlowRequest(prompt="我要一个 30 秒小猫视频集锦"))

    assert plan.candidates
    assert all(candidate.score_breakdown for candidate in plan.candidates)
    assert any("fewer than 5" in warning for warning in plan.warnings)
    assert plan.validation["valid"] is True
