"""Explicit material providers for planner tests; never installed by production."""
from app.schemas.autoflow import AutoFlowClipCandidate


class FixtureMaterialSelector:
    async def find_candidates(self, intent, request, db=None):
        external = request.source_policy in {"research_only", "remix_with_review"}
        return [AutoFlowClipCandidate(
            id=f"owned-{index}", title=f"{intent.subject} clip {index}",
            source_type="external_url" if external else "asset",
            asset_id=None if external else f"test-owned-asset-{index}",
            url=f"https://media.invalid/{index}.mp4" if external else None,
            start_sec=0, end_sec=5, rights_status="review_required" if external else "allowed",
            metadata={} if external else {"license": "owned", "provenance": "test", "evidence_ref": "test-fixture"},
        ) for index in (1, 2)]
