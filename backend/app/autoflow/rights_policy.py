from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowRequest


class RightsDecision(BaseModel):
    status: Literal["allowed", "review_required", "blocked"]
    reasons: list[str] = Field(default_factory=list)
    allowed_publish_modes: list[str] = Field(default_factory=list)
    execute_allowed: bool = True
    publish_allowed: bool = False


class RightsPolicy:
    external_sources = {"youtube", "x", "bilibili", "xiaohongshu", "external_url", "url"}

    def evaluate(
        self,
        request: AutoFlowRequest,
        candidates: list[AutoFlowClipCandidate],
    ) -> RightsDecision:
        has_external = any(candidate.url or candidate.source_type in self.external_sources for candidate in candidates)
        has_unknown = any(candidate.rights_status == "unknown" and not candidate.asset_id for candidate in candidates)

        if request.source_policy == "owned_only" and has_external:
            return RightsDecision(
                status="blocked",
                reasons=["owned_only policy does not allow external URL candidates"],
                allowed_publish_modes=["preview_only"],
                execute_allowed=False,
                publish_allowed=False,
            )

        if has_unknown and request.publish_mode == "public_after_review":
            return RightsDecision(
                status="review_required",
                reasons=["unknown candidate rights require review before public publishing"],
                allowed_publish_modes=["preview_only", "private_upload", "unlisted_upload"],
                execute_allowed=True,
                publish_allowed=False,
            )

        if has_external:
            return RightsDecision(
                status="review_required",
                reasons=["external URL candidates require human review and private/unlisted defaults"],
                allowed_publish_modes=["preview_only", "private_upload", "unlisted_upload"],
                execute_allowed=True,
                publish_allowed=request.publish_mode in {"preview_only", "private_upload", "unlisted_upload"},
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
