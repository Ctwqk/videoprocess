from __future__ import annotations

from app.autoflow.clip_ranker import ClipRanker
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


def intent() -> AutoFlowIntent:
    return AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        duration_sec=30,
        aspect_ratio="9:16",
        keywords=["小猫", "cat", "kitten"],
    )


def candidate(
    candidate_id: str,
    *,
    title: str,
    source_type: str = "asset",
    asset_id: str | None = None,
    url: str | None = None,
    start_sec: float = 0,
    end_sec: float = 5,
    rights_status: str = "allowed",
    metadata: dict | None = None,
) -> AutoFlowClipCandidate:
    return AutoFlowClipCandidate(
        id=candidate_id,
        title=title,
        source_type=source_type,
        asset_id=asset_id,
        url=url,
        start_sec=start_sec,
        end_sec=end_sec,
        rights_status=rights_status,
        metadata=metadata or {},
    )


def test_ranker_scores_with_explainable_breakdown_and_visual_metadata():
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate(
                "low",
                title="generic outdoor clip",
                asset_id="asset-low",
                metadata={"duration": 11, "aspect_ratio": "16:9", "visual": {"motion_score": 0.1}},
            ),
            candidate(
                "strong",
                title="小猫 kitten jumps in first seconds",
                asset_id="asset-strong",
                metadata={
                    "duration": 4.5,
                    "aspect_ratio": "9:16",
                    "visual": {"motion_score": 0.92, "watermark_score": 0.0},
                    "quality_score": 0.9,
                    "first_seconds_hook_score": 0.8,
                },
            ),
        ],
    )

    assert [item.id for item in ranked] == ["strong", "low"]
    assert ranked[0].score > ranked[1].score
    assert ranked[0].score > 0
    assert ranked[0].score <= 1
    assert set(ranked[0].score_breakdown) >= {
        "topic_relevance",
        "duration_fit",
        "visual_motion_score",
        "first_seconds_hook_score",
        "aspect_ratio_fit",
        "quality_score",
        "source_reputation",
        "novelty_score",
        "copyright_risk",
        "duplicate_penalty",
        "watermark_penalty",
    }
    assert ranked[0].score_breakdown["visual_motion_score"] == 0.92


def test_ranker_dedupes_urls_assets_titles_and_overlapping_source_windows():
    same_url = "https://example.test/cat-clip.mp4"
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate("url-a", title="小猫 url clip", source_type="youtube", url=same_url, rights_status="review_required"),
            candidate("url-b", title="duplicate url", source_type="youtube", url=same_url, rights_status="review_required"),
            candidate("asset-a", title="小猫 asset clip", asset_id="asset-1"),
            candidate("asset-b", title="duplicate asset", asset_id="asset-1"),
            candidate("title-a", title="Cat rooftop jump", asset_id="asset-2", end_sec=5.1),
            candidate("title-b", title="cat rooftop jump", asset_id="asset-3", end_sec=5.4),
            candidate(
                "window-a",
                title="小猫 source window one",
                asset_id="asset-4",
                start_sec=0,
                end_sec=5,
                metadata={"source_video_id": "source-video-1"},
            ),
            candidate(
                "window-b",
                title="小猫 source window overlap",
                asset_id="asset-5",
                start_sec=2,
                end_sec=6,
                metadata={"source_video_id": "source-video-1"},
            ),
        ],
    )

    assert len([item for item in ranked if item.url == same_url]) == 1
    assert len([item for item in ranked if item.asset_id == "asset-1"]) == 1
    assert len([item for item in ranked if item.title.lower() == "cat rooftop jump"]) == 1
    assert len([item for item in ranked if item.metadata.get("source_video_id") == "source-video-1"]) == 1
