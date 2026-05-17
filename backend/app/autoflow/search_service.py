from __future__ import annotations

from dataclasses import dataclass
from math import ceil

from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.platform_media_client import PlatformMediaClient, PlatformMediaClientError, SUPPORTED_SOURCE_PLATFORMS
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest


@dataclass(frozen=True)
class ExternalSearchResult:
    candidates: list[AutoFlowClipCandidate]
    warnings: list[str]


class SearchService:
    def __init__(self, platform_client: PlatformMediaClient | None = None) -> None:
        self.platform_client = platform_client or PlatformMediaClient()

    async def search_material(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db: AsyncSession | None = None,
        max_results: int = 8,
    ) -> list[AutoFlowClipCandidate]:
        subject = intent.subject or "video"
        licensed = request.source_policy == "licensed_only"
        library_scope = request.material_library_ids or ["default"]
        count = min(max_results, 6)
        return [
            AutoFlowClipCandidate(
                id=f"material-{index}",
                title=f"{subject} material clip {index}",
                source_type="material",
                asset_id=f"autoflow-material-{index}",
                start_sec=0,
                end_sec=4 + (index % 3),
                rights_status="allowed",
                metadata={
                    "library_ids": library_scope,
                    "license": "standard-library" if licensed else "owned",
                    "duration": 4 + (index % 3),
                    "aspect_ratio": intent.aspect_ratio,
                    "quality_score": 0.72 + min(index, 3) * 0.04,
                    "visual": {
                        "motion_score": 0.55 + min(index, 4) * 0.08,
                        "watermark_score": 0.0,
                    },
                },
            )
            for index in range(1, count + 1)
        ]

    async def search_external(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        max_results: int = 8,
    ) -> list[AutoFlowClipCandidate]:
        result = await self.search_external_platforms_with_warnings(intent, request, max_results=max_results)
        return result.candidates

    async def search_external_platforms(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        max_results: int = 8,
    ) -> list[AutoFlowClipCandidate]:
        result = await self.search_external_platforms_with_warnings(intent, request, max_results=max_results)
        return result.candidates

    async def search_external_platforms_with_warnings(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        max_results: int = 8,
    ) -> ExternalSearchResult:
        query = _query_for_intent(intent)
        platforms = _source_platforms(request)
        total_limit = max(0, min(int(max_results), 50))
        results: list[AutoFlowClipCandidate] = []
        warnings: list[str] = []
        if total_limit == 0:
            return ExternalSearchResult(candidates=[], warnings=[])
        if not platforms:
            return ExternalSearchResult(
                candidates=[],
                warnings=["No supported source platforms selected; external search skipped."],
            )

        per_platform_limit = max(1, min(8, ceil(total_limit / len(platforms))))
        for platform in platforms:
            try:
                results.extend(await self.platform_client.search(platform, query, max_results=per_platform_limit))
            except PlatformMediaClientError as exc:
                warnings.append(f"{platform} search skipped: {exc.category}: {exc.detail}")
        candidates = [_annotate_external(candidate, intent) for candidate in results[:total_limit]]
        return ExternalSearchResult(candidates=candidates, warnings=warnings)

    async def search_youtube(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self.platform_client.search("youtube", query, max_results=max_results)

    async def search_x(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self.platform_client.search("x", query, max_results=max_results)

    async def search_xiaohongshu(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self.platform_client.search("xiaohongshu", query, max_results=max_results)

    async def search_bilibili(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self.platform_client.search("bilibili", query, max_results=max_results)


def _query_for_intent(intent: AutoFlowIntent) -> str:
    keywords = [keyword for keyword in intent.keywords if keyword]
    return " ".join(keywords[:3]) or intent.subject or "video"


def _source_platforms(request: AutoFlowRequest) -> list[str]:
    requested = request.source_platforms
    result: list[str] = []
    for platform in requested:
        value = platform.strip().lower()
        if value == "youtube_shorts":
            value = "youtube"
        if value in SUPPORTED_SOURCE_PLATFORMS and value not in result:
            result.append(value)
    return result


def _annotate_external(candidate: AutoFlowClipCandidate, intent: AutoFlowIntent) -> AutoFlowClipCandidate:
    metadata = {
        **candidate.metadata,
        "aspect_ratio": intent.aspect_ratio,
        "quality_score": candidate.metadata.get("quality_score", 0.62),
        "visual": candidate.metadata.get("visual", {"motion_score": 0.6, "watermark_score": 0.12}),
    }
    return candidate.model_copy(update={"rights_status": "review_required", "metadata": metadata})
