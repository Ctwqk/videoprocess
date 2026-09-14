from __future__ import annotations

from app.autoflow.rights_policy import (
    RightsPolicy,
    candidate_is_licensed,
    candidate_is_owned,
    candidate_rights_facts,
)
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowRequest


def test_owned_asset_preview_is_allowed():
    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="小猫预览"),
        [
            AutoFlowClipCandidate(
                id="c1",
                title="owned",
                source_type="asset",
                asset_id="asset-1",
                rights_status="allowed",
                metadata={"license": "owned", "provenance": "original"},
            )
        ],
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


def test_local_only_source_policies_block_external_candidates():
    candidate = AutoFlowClipCandidate(
        id="external",
        title="external",
        source_type="youtube",
        url="https://example.test/a.mp4",
        rights_status="review_required",
    )

    for source_policy in ("owned_only", "licensed_only", "public_domain_or_cc"):
        decision = RightsPolicy().evaluate(
            AutoFlowRequest(prompt="local sources only", source_policy=source_policy),
            [candidate],
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


def test_unknown_local_asset_is_not_treated_as_owned_or_automatically_publishable():
    candidate = AutoFlowClipCandidate(
        id="c1",
        title="unknown local asset",
        source_type="asset",
        asset_id="asset-1",
        rights_status="unknown",
    )

    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="上传小猫", source_policy="owned_only", publish_mode="private_upload"),
        [candidate],
    )

    assert candidate_is_owned(candidate) is False
    assert decision.status == "review_required"
    assert decision.publish_allowed is False


def test_blocked_candidate_takes_precedence_over_external_review():
    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="搜索素材", source_policy="research_only"),
        [
            AutoFlowClipCandidate(
                id="blocked",
                title="blocked external",
                source_type="youtube",
                url="https://example.test/blocked.mp4",
                rights_status="blocked",
            )
        ],
    )

    assert decision.status == "blocked"
    assert decision.execute_allowed is False
    assert decision.publish_allowed is False


def test_empty_candidates_are_blocked_as_no_material():
    decision = RightsPolicy().evaluate(AutoFlowRequest(prompt="没有素材"), [])

    assert decision.status == "blocked"
    assert decision.execute_allowed is False
    assert "no_material" in decision.reasons


def test_owned_and_licensed_helpers_require_explicit_known_rights():
    owned = AutoFlowClipCandidate(
        id="owned",
        title="owned",
        source_type="material",
        asset_id="asset-owned",
        rights_status="allowed",
        metadata={"license": "owned"},
    )
    licensed = AutoFlowClipCandidate(
        id="licensed",
        title="licensed",
        source_type="material",
        asset_id="asset-licensed",
        rights_status="allowed",
        metadata={"license": "standard-library"},
    )
    garbage = licensed.model_copy(update={"metadata": {"license": "whatever-the-model-said"}})

    assert candidate_is_owned(owned) is True
    assert candidate_is_licensed(licensed) is True
    assert candidate_is_licensed(garbage) is False


def test_candidate_rights_facts_preserve_provenance_and_evidence():
    candidate = AutoFlowClipCandidate(
        id="licensed",
        title="licensed",
        source_type="material",
        asset_id="asset-licensed",
        rights_status="allowed",
        metadata={
            "license": "standard-library",
            "provenance": "partner-library",
            "rights_evidence": {"contract_id": "contract-7"},
            "evidence_ref": "capture",
            "license_scope": "worldwide",
            "license_source": "partner",
            "rights_source": "asset-media-info",
            "evidence_refs": ["contract-7", "capture"],
            "visual": {"motion_score": 0.8},
        },
    )

    assert candidate_rights_facts(candidate) == {
        "rights_status": "allowed",
        "license": "standard-library",
        "provenance": "partner-library",
        "rights_evidence": {"contract_id": "contract-7"},
        "evidence_ref": "capture",
        "license_scope": "worldwide",
        "license_source": "partner",
        "rights_source": "asset-media-info",
        "evidence_refs": ["contract-7", "capture"],
    }


def test_nested_blocked_status_overrides_allowed_candidate_status():
    candidate = AutoFlowClipCandidate(
        id="blocked",
        title="blocked",
        source_type="material",
        asset_id="asset-blocked",
        rights_status="allowed",
        metadata={"license": "owned", "rights_status": "blocked"},
    )

    assert candidate_rights_facts(candidate)["rights_status"] == "blocked"
    assert candidate_is_owned(candidate) is False
    assert RightsPolicy().evaluate(AutoFlowRequest(prompt="blocked"), [candidate]).status == "blocked"


def test_owned_policy_does_not_trust_allowed_status_without_owned_license():
    candidate = AutoFlowClipCandidate(
        id="unproven",
        title="unproven",
        source_type="asset",
        asset_id="asset-unproven",
        rights_status="allowed",
    )

    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="private upload", source_policy="owned_only", publish_mode="private_upload"),
        [candidate],
    )

    assert decision.status == "review_required"
    assert decision.publish_allowed is False


def test_owned_policy_blocks_known_non_owned_license_even_when_status_is_allowed():
    candidate = AutoFlowClipCandidate(
        id="licensed",
        title="licensed",
        source_type="asset",
        asset_id="asset-licensed",
        rights_status="allowed",
        metadata={"license": "standard-library"},
    )

    decision = RightsPolicy().evaluate(
        AutoFlowRequest(prompt="owned only", source_policy="owned_only"),
        [candidate],
    )

    assert decision.status == "blocked"
