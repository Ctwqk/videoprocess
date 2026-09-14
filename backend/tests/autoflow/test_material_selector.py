from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.autoflow.clip_ranker import ClipRanker
from app.autoflow.material_selector import CandidateSelectionResult, MaterialSelector
from app.autoflow.search_service import ExternalSearchResult, SearchService
from app.autoflow.service import AutoFlowService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest
from app.schemas.material import MaterialSearchRequest
from app.services import material_service


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
    async def search_material(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db=None,
        max_results: int = 8,
        **kwargs,
    ):
        return [
            AutoFlowClipCandidate(
                id="owned",
                title=f"{intent.subject} owned",
                source_type="asset",
                asset_id="asset-owned",
                rights_status="allowed",
                metadata={"license": "owned"},
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
    async def search_material(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db=None,
        max_results: int = 8,
        **kwargs,
    ):
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
            AutoFlowClipCandidate(
                id="garbage-license",
                title=f"{intent.subject} untrusted license",
                source_type="material",
                asset_id="asset-garbage",
                rights_status="allowed",
                metadata={"license": "whatever-the-model-said"},
            ),
        ]

    async def search_external(self, intent: AutoFlowIntent, request: AutoFlowRequest, max_results: int = 8):
        return []


class MultiPlatformSearchService(SearchService):
    async def search_material(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db=None,
        max_results: int = 8,
        **kwargs,
    ):
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
async def test_owned_only_selector_rejects_unknown_and_blocked_local_assets():
    class RightsSearchService:
        async def search_material(self, intent, request, db=None, max_results=8, **kwargs):
            base = {
                "title": "local",
                "source_type": "asset",
                "asset_id": "asset-1",
                "metadata": {"license": "owned"},
            }
            return [
                AutoFlowClipCandidate(id="owned", rights_status="allowed", **base),
                AutoFlowClipCandidate(id="unknown", rights_status="unknown", **base),
                AutoFlowClipCandidate(id="blocked", rights_status="blocked", **base),
                AutoFlowClipCandidate(
                    id="missing-license",
                    title="local",
                    source_type="asset",
                    asset_id="asset-2",
                    rights_status="allowed",
                ),
            ]

    candidates = await MaterialSelector(search_service=RightsSearchService()).find_candidates(
        intent("owned_only"),
        AutoFlowRequest(prompt="只用自有素材", source_policy="owned_only"),
    )

    assert [candidate.id for candidate in candidates] == ["owned"]


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

    candidates = await MaterialSelector(search_service=LicensedSearchService()).find_candidates(
        intent("licensed_only"),
        request,
    )

    assert candidates
    assert all(candidate.url is None for candidate in candidates)
    assert {candidate.source_type for candidate in candidates} <= {"asset", "material"}
    assert all(candidate.metadata.get("license") for candidate in candidates)
    assert all(candidate.rights_status == "allowed" for candidate in candidates)
    assert [candidate.id for candidate in candidates] == ["licensed"]


@pytest.mark.asyncio
async def test_public_domain_selector_accepts_known_cc_aliases_only():
    class PublicDomainSearchService:
        async def search_material(self, intent, request, db=None, max_results=8, **kwargs):
            return [
                AutoFlowClipCandidate(
                    id="cc-by",
                    title="CC BY",
                    source_type="material",
                    asset_id="asset-cc",
                    rights_status="allowed",
                    metadata={"license": "CC-BY"},
                ),
                AutoFlowClipCandidate(
                    id="garbage",
                    title="Garbage",
                    source_type="material",
                    asset_id="asset-garbage",
                    rights_status="allowed",
                    metadata={"license": "free-ish"},
                ),
            ]

    candidates = await MaterialSelector(search_service=PublicDomainSearchService()).find_candidates(
        intent("public_domain_or_cc"),
        AutoFlowRequest(prompt="使用 CC 素材", source_policy="public_domain_or_cc"),
    )

    assert [candidate.id for candidate in candidates] == ["cc-by"]


@pytest.mark.asyncio
async def test_search_service_returns_empty_without_db_or_material_libraries():
    request = AutoFlowRequest(prompt="我要小猫素材", source_policy="research_only")
    service = SearchService(platform_client=FakePlatformClient())

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
                "metadata": {
                    "license": "owned",
                    "provenance": "original",
                    "rights_evidence": {"attestation": "user-upload"},
                    "visual": {"motion": "fast"},
                },
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
            material_id="asset-cut-1",
            asset_id="asset-cut-1",
            start_sec=1.25,
            end_sec=4.75,
            rights_status="allowed",
            metadata={
                "library_id": "library-1",
                "material_id": "asset-cut-1",
                "source_asset_id": "source-asset-1",
                "asset_id": "asset-cut-1",
                "coarse": 0.42,
                "lighthouse": 0.81,
                "confidence": 0.9,
                "subtitle": "tiny jump",
                "license": "owned",
                "provenance": "original",
                "rights_evidence": {"attestation": "user-upload"},
                "visual": {"motion": "fast"},
            },
        )
    ]


def test_material_result_conversion_does_not_promote_missing_or_blocked_rights():
    from app.autoflow.search_service import _candidate_from_material_result

    unknown = _candidate_from_material_result(
        {"id": "unknown", "asset_id": "asset-unknown", "title": "Unknown"},
        1,
    )
    blocked = _candidate_from_material_result(
        {
            "id": "blocked",
            "asset_id": "asset-blocked",
            "title": "Blocked",
            "rights_status": "allowed",
            "metadata": {"license": "owned", "rights_status": "blocked"},
        },
        2,
    )

    assert unknown.rights_status == "unknown"
    assert blocked.rights_status == "blocked"


def test_material_result_metadata_keeps_rights_facts():
    window = material_service._candidate_window_from_cluster(
        [
            {
                "library_id": "library-1",
                "source_asset_id": "asset-source",
                "clip_id": "clip-1",
                "start_sec": 0.0,
                "end_sec": 4.0,
                "subtitle_text": "cat",
                "neighbor_clip_ids": [],
                "coarse_score": 0.8,
                "metadata": {
                    "license": "owned",
                    "provenance": "original",
                    "rights_evidence": {"attestation": "user-upload"},
                    "visual": {"objects": ["cat"]},
                },
            }
        ]
    )

    metadata = material_service._material_result_metadata(window)

    assert metadata["license"] == "owned"
    assert metadata["provenance"] == "original"
    assert metadata["rights_evidence"] == {"attestation": "user-upload"}


@pytest.mark.asyncio
async def test_cut_material_asset_inherits_source_rights_facts(monkeypatch, tmp_path):
    source_path = tmp_path / "source.mp4"
    source_path.write_bytes(b"source")
    source_asset = SimpleNamespace(
        storage_backend="local",
        storage_path="assets/source.mp4",
        original_name="source.mp4",
        media_info={
            "duration": 10.0,
            "license": "owned",
            "provenance": "original",
            "evidence_ref": "capture",
        },
    )
    final_asset = SimpleNamespace(id="asset-cut", media_info={"duration": 3.0})

    class FakeStorage:
        def get_local_path(self, storage_path):
            return str(source_path)

    class FakeProcess:
        returncode = 0

        async def communicate(self):
            return b"", b""

    class FakeDB:
        def __init__(self):
            self.flushes = 0

        async def flush(self):
            self.flushes += 1

    async def fake_subprocess(*args, **kwargs):
        return FakeProcess()

    async def fake_create_asset(db, local_path, **kwargs):
        return final_asset

    monkeypatch.setattr(material_service, "get_storage", lambda backend: FakeStorage())
    monkeypatch.setattr(material_service.asyncio, "create_subprocess_exec", fake_subprocess)
    monkeypatch.setattr(material_service, "create_asset_from_local_file", fake_create_asset)
    db = FakeDB()

    result = await material_service._cut_asset_clip(source_asset, 1.0, 4.0, "clip", db)

    assert result.media_info == {
        "duration": 3.0,
        "license": "owned",
        "provenance": "original",
        "evidence_ref": "capture",
    }
    assert db.flushes == 1


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
async def test_preview_material_search_falls_back_to_db_when_qdrant_has_no_hits(monkeypatch):
    fallback_calls = []

    class FakeDB:
        def add(self, row):
            self.row = row

        async def flush(self):
            pass

        async def commit(self):
            pass

        async def refresh(self, row):
            pass

    async def fake_embed_texts(texts):
        return [[0.1, 0.2]]

    async def fake_qdrant_search(vector_name, query_embedding, source_library_ids, limit):
        return []

    async def fake_fallback_db_search(db, query, source_library_ids, limit):
        fallback_calls.append((query, source_library_ids, limit))
        return [
            {
                "id": "clip-point-1",
                "score": 0.9,
                "payload": {
                    "library_id": VALID_LIBRARY_ID,
                    "source_asset_id": "00000000-0000-0000-0000-000000000201",
                    "clip_id": "clip-1",
                    "start_sec": 0.0,
                    "end_sec": 4.0,
                    "subtitle_text": "cat jump",
                    "neighbor_clip_ids": [],
                    "metadata": {"visual": {"objects": ["cat"]}},
                },
            }
        ]

    async def fake_lighthouse_rerank(query, windows, top_m):
        return windows[:top_m]

    async def fake_univtg_refine(query, window, min_duration, max_duration):
        return window.start_sec, window.end_sec, 0.8

    monkeypatch.setattr(material_service, "_embed_texts", fake_embed_texts)
    monkeypatch.setattr(material_service, "_qdrant_search", fake_qdrant_search)
    monkeypatch.setattr(material_service, "_fallback_db_search", fake_fallback_db_search)
    monkeypatch.setattr(material_service, "_lighthouse_rerank", fake_lighthouse_rerank)
    monkeypatch.setattr(material_service, "_univtg_refine", fake_univtg_refine)

    _query, results = await material_service.preview_material_search(
        FakeDB(),
        MaterialSearchRequest(
            query="cat jump",
            source_library_ids=[VALID_LIBRARY_ID],
            result_library_ids=[VALID_LIBRARY_ID],
            top_k=2,
        ),
    )

    assert fallback_calls == [("cat jump", [VALID_LIBRARY_ID], 2)]
    assert results[0]["source_asset_id"] == "00000000-0000-0000-0000-000000000201"
    assert results[0]["metadata"]["visual"] == {"objects": ["cat"]}


@pytest.mark.asyncio
async def test_external_search_uses_adapter_and_returns_warnings_when_unconfigured():
    class FailingPlatformClient:
        async def search(self, platform: str, query: str, max_results: int = 8):
            from app.autoflow.platform_media_client import PlatformMediaClientError

            raise PlatformMediaClientError(platform, "platform_unavailable", "not configured")

    request = AutoFlowRequest(prompt="搜索小猫外部素材", source_policy="research_only")
    result = await SearchService(platform_client=FailingPlatformClient()).search_external_platforms_with_warnings(
        intent("research_only"),
        request,
    )

    assert result.candidates == []
    assert len(result.warnings) == 4
    assert all("platform_unavailable" in warning for warning in result.warnings)


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
async def test_external_search_prefers_user_prompt_as_query():
    request = AutoFlowRequest(
        prompt="funny puppy short clip 10 seconds",
        source_policy="remix_with_review",
        source_platforms=["youtube"],
    )
    client = RecordingPlatformClient()
    service = SearchService(platform_client=client)

    await service.search_external(intent("remix_with_review"), request, max_results=3)

    assert client.calls == [("youtube", "funny puppy short clip 10 seconds", 3)]


@pytest.mark.asyncio
async def test_external_search_honors_max_candidates_constraint():
    request = AutoFlowRequest(
        prompt="funny puppy short clip 10 seconds",
        source_policy="remix_with_review",
        source_platforms=["youtube"],
        constraints={"max_candidates": 4},
    )
    client = RecordingPlatformClient()
    selector = MaterialSelector(search_service=SearchService(platform_client=client))

    candidates = await selector.find_candidates(intent("remix_with_review"), request)

    assert len(candidates) == 4
    assert client.calls == [("youtube", "funny puppy short clip 10 seconds", 4)]


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
async def test_autoflow_service_blocks_when_selector_is_empty():
    service = AutoFlowService(material_selector=EmptySelector(), clip_ranker=ClipRanker())
    plan = await service.plan(AutoFlowRequest(prompt="我要一个 30 秒小猫视频集锦"))

    assert plan.candidates == []
    assert plan.status == "blocked"
    assert plan.rights["status"] == "blocked"
    assert "no_material" in plan.rights["reasons"]
