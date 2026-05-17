from __future__ import annotations

from app.autoflow.metadata_generator import MetadataGenerator
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


def test_cat_compilation_metadata_has_platform_payloads_and_five_titles():
    intent = AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        style="cute_fast_montage",
        duration_sec=30,
        aspect_ratio="9:16",
        target_platforms=["youtube_shorts", "x", "xiaohongshu", "bilibili"],
        keywords=["小猫", "可爱", "cat"],
    )
    candidates = [
        AutoFlowClipCandidate(id="c1", title="小猫翻车", source_type="asset", asset_id="asset-1"),
        AutoFlowClipCandidate(id="c2", title="猫咪奔跑", source_type="asset", asset_id="asset-2"),
    ]

    metadata = MetadataGenerator().generate(intent, candidates)

    assert len(metadata.title_candidates) >= 5
    assert metadata.selected_title == metadata.title_candidates[0]
    assert "小猫" in metadata.description
    assert {"小猫", "cat"}.issubset(set(metadata.tags))
    assert "#小猫" in metadata.hashtags
    assert metadata.thumbnail_text_candidates
    assert set(metadata.platform_payloads) == {"youtube_shorts", "x", "xiaohongshu", "bilibili"}
    assert metadata.platform_payloads["youtube_shorts"]["title"] == metadata.selected_title
    assert metadata.platform_payloads["youtube_shorts"]["privacy"] == "private"
