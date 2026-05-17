from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.search_service import SearchService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest


class MaterialSelector:
    def __init__(self, search_service: SearchService | None = None) -> None:
        self.search_service = search_service or SearchService()

    async def find_candidates(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db: AsyncSession | None = None,
    ) -> list[AutoFlowClipCandidate]:
        material = await self.search_service.search_material(intent, request, db=db)

        if request.source_policy == "owned_only":
            return [candidate for candidate in material if _is_owned_material(candidate)]

        if request.source_policy == "licensed_only":
            return [candidate for candidate in material if _is_licensed_material(candidate)]

        if request.source_policy in {"research_only", "remix_with_review"}:
            external = await self.search_service.search_external(intent, request)
            return [
                *[candidate for candidate in material if candidate.url is None],
                *[_force_review_required(candidate) for candidate in external],
            ]

        if request.source_policy == "public_domain_or_cc":
            return [
                candidate
                for candidate in material
                if candidate.url is None and candidate.metadata.get("license") in {"public_domain", "creative_commons"}
            ]

        return [candidate for candidate in material if candidate.url is None]


def _is_owned_material(candidate: AutoFlowClipCandidate) -> bool:
    return (
        candidate.url is None
        and candidate.source_type in {"asset", "material"}
        and candidate.metadata.get("license") in {None, "owned"}
    )


def _is_licensed_material(candidate: AutoFlowClipCandidate) -> bool:
    return (
        candidate.url is None
        and candidate.source_type in {"asset", "material"}
        and bool(candidate.metadata.get("license"))
        and candidate.rights_status == "allowed"
    )


def _force_review_required(candidate: AutoFlowClipCandidate) -> AutoFlowClipCandidate:
    if not candidate.url:
        return candidate
    return candidate.model_copy(update={"rights_status": "review_required"})
