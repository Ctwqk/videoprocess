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


def test_ranker_historical_performance_fit_boosts_matching_template():
    ranker = ClipRanker(
        historical_performance={
            "templates": {
                "animal_compilation_short": {"score": 0.95},
                "generic_short": {"score": 0.1},
            },
            "intent_types": {"animal_compilation": {"score": 0.8}},
        }
    )

    shared_metadata = {
        "duration": 5,
        "aspect_ratio": "9:16",
        "visual": {"motion_score": 0.75, "watermark_score": 0.0},
        "quality_score": 0.8,
        "first_seconds_hook_score": 0.7,
    }
    ranked = ranker.rank(
        intent(),
        [
            candidate(
                "generic",
                title="小猫 balanced edit b",
                asset_id="asset-generic",
                metadata={**shared_metadata, "template_id": "generic_short"},
            ),
            candidate(
                "proven",
                title="小猫 balanced edit a",
                asset_id="asset-proven",
                metadata={**shared_metadata, "template_id": "animal_compilation_short"},
            ),
        ],
    )

    assert [item.id for item in ranked] == ["proven", "generic"]
    assert ranked[0].score_breakdown["historical_performance_fit"] == 0.95
    assert ranked[1].score_breakdown["historical_performance_fit"] == 0.1
    assert ranked[0].score > ranked[1].score


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


def test_ranker_uses_semantic_relevance_scores_when_available():
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate("weak", title="generic office clip", asset_id="asset-weak", metadata={"duration": 5}),
            candidate("semantic", title="playful animal clip", asset_id="asset-semantic", metadata={"duration": 5}),
        ],
        semantic_relevance_scores={"asset-weak": 0.05, "asset-semantic": 0.98},
    )

    assert [item.id for item in ranked] == ["semantic", "weak"]
    assert ranked[0].score_breakdown["semantic_relevance"] == 0.98


def test_ranker_penalizes_recently_used_asset_ids():
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate("fresh", title="小猫 fresh", asset_id="asset-fresh", metadata={"duration": 5}),
            candidate("recent", title="小猫 recent", asset_id="asset-recent", metadata={"duration": 5}),
        ],
        recent_used_asset_ids={"asset-recent"},
    )

    assert [item.id for item in ranked] == ["fresh", "recent"]
    assert ranked[1].score_breakdown["recent_used_penalty"] == 1.0


def test_ranker_uses_visual_face_scene_and_brightness_signals():
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate("plain", title="小猫 plain", asset_id="asset-plain", metadata={"duration": 5}),
            candidate(
                "visual",
                title="小猫 visual",
                asset_id="asset-visual",
                metadata={
                    "duration": 5,
                    "visual": {
                        "motion_score": 0.8,
                        "face_present": True,
                        "scene_change_score": 0.9,
                        "brightness_score": 0.85,
                    },
                },
            ),
        ],
    )

    assert [item.id for item in ranked] == ["visual", "plain"]
    assert ranked[0].score_breakdown["face_present"] == 1.0
    assert ranked[0].score_breakdown["scene_change_diversity"] == 0.9
    assert ranked[0].score_breakdown["brightness_fit"] == 0.85
