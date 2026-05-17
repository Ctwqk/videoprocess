from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest


class SearchService:
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
        subject = intent.subject or "video"
        count = min(max_results, 4)
        return [
            AutoFlowClipCandidate(
                id=f"external-{index}",
                title=f"{subject} external reference {index}",
                source_type="youtube",
                url=f"https://example.test/autoflow/{subject}-{index}.mp4",
                start_sec=0,
                end_sec=5 + (index % 2),
                rights_status="review_required",
                metadata={
                    "duration": 5 + (index % 2),
                    "aspect_ratio": intent.aspect_ratio,
                    "quality_score": 0.62,
                    "visual": {"motion_score": 0.6, "watermark_score": 0.12},
                },
            )
            for index in range(1, count + 1)
        ]

    async def search_youtube(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        intent = AutoFlowIntent(intent_type="generic_video", subject=query, keywords=[query])
        request = AutoFlowRequest(prompt=query, source_policy="research_only")
        return await self.search_external(intent, request, max_results=max_results)

    async def search_x(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self._platform_stubs("x", query, max_results)

    async def search_xiaohongshu(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self._platform_stubs("xiaohongshu", query, max_results)

    async def search_bilibili(self, query: str, max_results: int = 8) -> list[AutoFlowClipCandidate]:
        return await self._platform_stubs("bilibili", query, max_results)

    async def _platform_stubs(
        self,
        platform: str,
        query: str,
        max_results: int,
    ) -> list[AutoFlowClipCandidate]:
        return [
            AutoFlowClipCandidate(
                id=f"{platform}-{index}",
                title=f"{query} {platform} reference {index}",
                source_type=platform,
                url=f"https://example.test/{platform}/{index}.mp4",
                start_sec=0,
                end_sec=5,
                rights_status="review_required",
            )
            for index in range(1, min(max_results, 4) + 1)
        ]
