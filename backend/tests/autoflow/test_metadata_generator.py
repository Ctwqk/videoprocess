from __future__ import annotations

from app.autoflow.metadata_generator import MetadataGenerator
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


class FakeMetadataClient:
    def generate(self, payload: dict) -> dict:
        return {
            "titles": ["小猫追玩具的高光合集", "猫咪玩具挑战"],
            "thumbnail_texts": ["小猫追玩具"],
            "tags": ["小猫", "追玩具", "可爱"],
            "rationale": "grounded in object labels",
        }


def test_metadata_generator_uses_clip_facts_for_title_candidates():
    intent = AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        style="cute_fast_montage",
        duration_sec=30,
        aspect_ratio="9:16",
        target_platforms=["douyin"],
        keywords=["可爱"],
    )
    candidates = [
        AutoFlowClipCandidate(
            id="c1",
            title="小猫追玩具",
            source_type="asset",
            asset_id="asset-1",
            metadata={"visual": {"object_labels": ["玩具"], "dominant_action": "追玩具"}},
        )
    ]

    metadata = MetadataGenerator(llm_client=FakeMetadataClient()).generate(intent, candidates)

    assert metadata.selected_title == "小猫追玩具的高光合集"
    assert "小猫追玩具" in metadata.thumbnail_text_candidates
    assert {"小猫", "追玩具", "可爱"}.issubset(set(metadata.tags))
    assert "#小猫" in metadata.hashtags
    assert metadata.platform_payloads["douyin"]["title"] == metadata.selected_title
    assert "metadata_llm" not in " ".join(metadata.platform_payloads["douyin"].get("warnings", []))


def test_metadata_generator_fallback_avoids_unverifiable_last_seconds_claim():
    intent = AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        target_platforms=["douyin"],
        keywords=["可爱"],
    )
    candidates = [AutoFlowClipCandidate(id="c1", title="小猫晒太阳", source_type="asset", asset_id="asset-1")]

    metadata = MetadataGenerator().generate(intent, candidates)

    combined = " ".join([*metadata.title_candidates, *metadata.thumbnail_text_candidates])
    assert "最后 2 秒" not in combined
    assert metadata.selected_title
    assert metadata.platform_payloads["douyin"]["privacy"] == "draft"


def test_metadata_generator_rejects_ungrounded_thumbnail_text():
    class UngroundedClient:
        def generate(self, payload: dict) -> dict:
            return {"titles": ["小猫晒太阳"], "thumbnail_texts": ["最后反转"], "tags": ["小猫"]}

    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", target_platforms=["douyin"])
    candidates = [AutoFlowClipCandidate(id="c1", title="小猫晒太阳", source_type="asset", asset_id="asset-1")]

    metadata = MetadataGenerator(llm_client=UngroundedClient()).generate(intent, candidates)

    assert "最后反转" not in metadata.thumbnail_text_candidates
    assert "metadata_llm_ungrounded_claims_removed" in metadata.platform_payloads["douyin"]["warnings"]
