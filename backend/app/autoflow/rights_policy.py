from __future__ import annotations

from typing import Any, Literal, Mapping

from pydantic import BaseModel, Field

from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowRequest


class RightsDecision(BaseModel):
    status: Literal["allowed", "review_required", "blocked"]
    reasons: list[str] = Field(default_factory=list)
    allowed_publish_modes: list[str] = Field(default_factory=list)
    execute_allowed: bool = True
    publish_allowed: bool = False


RIGHTS_METADATA_KEYS = (
    "license",
    "provenance",
    "evidence",
    "rights_evidence",
    "license_evidence",
    "provenance_evidence",
    "evidence_ref",
    "evidence_refs",
    "license_scope",
    "license_source",
    "rights_source",
    "rights_status",
)
OWNED_LICENSES = {"owned"}
LICENSED_LICENSES = {
    "licensed",
    "standard_library",
    "commercial",
    "royalty_free",
    "public_domain",
    "creative_commons",
    "cc0",
    "cc_by",
    "cc_by_sa",
}
PUBLIC_DOMAIN_OR_CC_LICENSES = {
    "public_domain",
    "creative_commons",
    "cc0",
    "cc_by",
    "cc_by_sa",
}


def rights_metadata_from_mapping(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        return {}
    return {key: metadata[key] for key in RIGHTS_METADATA_KEYS if key in metadata}


def merge_rights_metadata(*sources: Mapping[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    statuses: list[Any] = []
    for source in sources:
        facts = rights_metadata_from_mapping(source)
        status = facts.pop("rights_status", None)
        if status is not None:
            statuses.append(status)
        merged.update(facts)
    if statuses:
        merged["rights_status"] = _most_restrictive_status(*statuses)
    return merged


def candidate_rights_facts(candidate: AutoFlowClipCandidate) -> dict[str, Any]:
    return merge_rights_metadata(candidate.metadata, {"rights_status": candidate.rights_status})


def rights_status_from_metadata(metadata: Mapping[str, Any] | None) -> str:
    facts = rights_metadata_from_mapping(metadata)
    explicit_status = _normalized_token(facts.get("rights_status"))
    if explicit_status in {"blocked", "review_required", "unknown"}:
        return explicit_status
    if _normalized_token(facts.get("license")) in OWNED_LICENSES | LICENSED_LICENSES:
        return "allowed"
    return "unknown"


def candidate_is_owned(candidate: AutoFlowClipCandidate) -> bool:
    return _is_local_candidate(candidate) and _candidate_license(candidate) in OWNED_LICENSES


def candidate_is_licensed(candidate: AutoFlowClipCandidate) -> bool:
    return _is_local_candidate(candidate) and _candidate_license(candidate) in LICENSED_LICENSES


def candidate_is_public_domain_or_cc(candidate: AutoFlowClipCandidate) -> bool:
    return _is_local_candidate(candidate) and _candidate_license(candidate) in PUBLIC_DOMAIN_OR_CC_LICENSES


def _is_local_candidate(candidate: AutoFlowClipCandidate) -> bool:
    return (
        rights_status_from_metadata(candidate_rights_facts(candidate)) == "allowed"
        and candidate.url is None
        and candidate.source_type in {"asset", "material"}
        and bool(candidate.asset_id)
    )


def _candidate_license(candidate: AutoFlowClipCandidate) -> str:
    return _normalized_token(candidate.metadata.get("license"))


def _normalized_token(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _most_restrictive_status(*values: Any) -> str:
    statuses = {_normalized_token(value) for value in values}
    for status in ("blocked", "review_required", "unknown", "allowed"):
        if status in statuses:
            return status
    return "unknown"


class RightsPolicy:
    external_sources = {"youtube", "x", "bilibili", "xiaohongshu", "external_url", "url"}

    def evaluate(
        self,
        request: AutoFlowRequest,
        candidates: list[AutoFlowClipCandidate],
    ) -> RightsDecision:
        if not candidates:
            return RightsDecision(
                status="blocked",
                reasons=["no_material"],
                allowed_publish_modes=["preview_only"],
                execute_allowed=False,
                publish_allowed=False,
            )

        effective_statuses = [rights_status_from_metadata(candidate_rights_facts(candidate)) for candidate in candidates]
        if "blocked" in effective_statuses:
            return RightsDecision(
                status="blocked",
                reasons=["one or more candidates are blocked by rights policy"],
                allowed_publish_modes=["preview_only"],
                execute_allowed=False,
                publish_allowed=False,
            )

        has_external = any(candidate.url or candidate.source_type in self.external_sources for candidate in candidates)

        if request.source_policy in {"owned_only", "licensed_only", "public_domain_or_cc"} and has_external:
            return RightsDecision(
                status="blocked",
                reasons=[f"{request.source_policy} policy does not allow external URL candidates"],
                allowed_publish_modes=["preview_only"],
                execute_allowed=False,
                publish_allowed=False,
            )

        if has_external:
            return RightsDecision(
                status="review_required",
                reasons=["external URL candidates require human review and private/unlisted defaults"],
                allowed_publish_modes=["preview_only", "private_upload", "unlisted_upload"],
                execute_allowed=True,
                publish_allowed=False,
            )

        mismatched = [
            candidate
            for candidate in candidates
            if _candidate_has_known_license(candidate) and not _candidate_matches_policy(candidate, request.source_policy)
        ]
        if mismatched:
            return RightsDecision(
                status="blocked",
                reasons=[f"one or more candidates do not satisfy {request.source_policy}"],
                allowed_publish_modes=["preview_only"],
                execute_allowed=False,
                publish_allowed=False,
            )

        if any(
            status != "allowed" or not _candidate_matches_policy(candidate, request.source_policy)
            for candidate, status in zip(candidates, effective_statuses)
        ):
            return RightsDecision(
                status="review_required",
                reasons=["unknown or review-required candidate rights require human review"],
                allowed_publish_modes=["preview_only", "private_upload", "unlisted_upload"],
                execute_allowed=True,
                publish_allowed=False,
            )

        if request.publish_mode == "public_after_review":
            return RightsDecision(
                status="review_required",
                reasons=["public publishing requires explicit human approval"],
                allowed_publish_modes=["preview_only", "private_upload", "unlisted_upload", "public_after_review"],
                execute_allowed=True,
                publish_allowed=False,
            )

        return RightsDecision(
            status="allowed",
            reasons=["owned or library-backed candidates are allowed for preview/private execution"],
            allowed_publish_modes=["preview_only", "private_upload", "unlisted_upload"],
            execute_allowed=True,
            publish_allowed=request.publish_mode in {"preview_only", "private_upload", "unlisted_upload"},
        )


def _candidate_has_known_license(candidate: AutoFlowClipCandidate) -> bool:
    return _candidate_license(candidate) in OWNED_LICENSES | LICENSED_LICENSES


def _candidate_matches_policy(candidate: AutoFlowClipCandidate, source_policy: str) -> bool:
    if source_policy == "owned_only":
        return candidate_is_owned(candidate)
    if source_policy == "licensed_only":
        return candidate_is_licensed(candidate)
    if source_policy == "public_domain_or_cc":
        return candidate_is_public_domain_or_cc(candidate)
    return rights_status_from_metadata(candidate_rights_facts(candidate)) == "allowed"
