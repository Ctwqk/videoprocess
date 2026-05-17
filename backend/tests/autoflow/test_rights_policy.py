from __future__ import annotations

from app.autoflow.rights_policy import RightsPolicy
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowRequest


def test_owned_asset_preview_is_allowed():
    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="小猫预览"),
        [AutoFlowClipCandidate(id="c1", title="owned", source_type="asset", asset_id="asset-1")],
    )

    assert decision.status == "allowed"
    assert decision.execute_allowed is True
    assert "preview_only" in decision.allowed_publish_modes


def test_owned_only_blocks_external_url_candidates():
    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="小猫预览", source_policy="owned_only"),
        [AutoFlowClipCandidate(id="c1", title="external", source_type="youtube", url="https://example.test/a.mp4")],
    )

    assert decision.status == "blocked"
    assert decision.execute_allowed is False


def test_research_external_url_requires_review():
    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="小猫预览", source_policy="research_only"),
        [AutoFlowClipCandidate(id="c1", title="external", source_type="youtube", url="https://example.test/a.mp4")],
    )

    assert decision.status == "review_required"
    assert "private_upload" in decision.allowed_publish_modes
