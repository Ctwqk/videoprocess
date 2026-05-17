from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.autoflow.search_service import SearchService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowRequest


@dataclass(frozen=True)
class CandidateSelectionResult:
    candidates: list[AutoFlowClipCandidate]
    warnings: list[str]


class MaterialSelector:
    def __init__(self, search_service: SearchService | None = None) -> None:
        self.search_service = search_service or SearchService()
        self.last_warnings: list[str] = []

    async def find_candidates(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db: AsyncSession | None = None,
    ) -> list[AutoFlowClipCandidate]:
        result = await self.find_candidates_with_warnings(intent, request, db=db)
        self.last_warnings = list(result.warnings)
        return result.candidates

    async def find_candidates_with_warnings(
        self,
        intent: AutoFlowIntent,
        request: AutoFlowRequest,
        db: AsyncSession | None = None,
    ) -> CandidateSelectionResult:
        material = await self.search_service.search_material(intent, request, db=db)

        if request.source_policy == "owned_only":
            return CandidateSelectionResult([candidate for candidate in material if _is_owned_material(candidate)], [])

        if request.source_policy == "licensed_only":
            return CandidateSelectionResult([candidate for candidate in material if _is_licensed_material(candidate)], [])

        if request.source_policy in {"research_only", "remix_with_review"}:
            search_with_warnings = getattr(self.search_service, "search_external_platforms_with_warnings", None)
            if callable(search_with_warnings):
                external_result = await search_with_warnings(intent, request)
                external = external_result.candidates
                warnings = external_result.warnings
            elif hasattr(self.search_service, "search_external_platforms"):
                external = await self.search_service.search_external_platforms(intent, request)
                warnings = list(getattr(self.search_service, "last_warnings", []))
            else:
                external = await self.search_service.search_external(intent, request)
                warnings = list(getattr(self.search_service, "last_warnings", []))
            return CandidateSelectionResult(
                [
                    *[candidate for candidate in material if candidate.url is None],
                    *[_force_review_required(candidate) for candidate in external],
                ],
                warnings,
            )

        if request.source_policy == "public_domain_or_cc":
            return CandidateSelectionResult(
                [
                    candidate
                    for candidate in material
                    if candidate.url is None and candidate.metadata.get("license") in {"public_domain", "creative_commons"}
                ],
                [],
            )

        return CandidateSelectionResult([candidate for candidate in material if candidate.url is None], [])


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
