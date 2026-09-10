from __future__ import annotations

import pytest

from app.autoflow.pipeline_builder import PipelineBuilder
from app.autoflow.storyboard_generator import StoryboardGenerator
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowStoryboardRequest


def test_unlisted_storyboard_preserves_freeform_topic_without_relaxing_publication_gates():
    storyboard = StoryboardGenerator().generate(AutoFlowStoryboardRequest(
        prompt="绿色背景上的白色中文文字",
        input_asset_id="owned-green-title",
        target_duration=8,
        aspect_ratio="16:9",
        min_shots=3,
        max_shots=3,
    )).storyboard
    definition = PipelineBuilder().build_storyboard_input_video(
        storyboard, input_asset_id="owned-green-title", publish_mode="unlisted_upload",
    )

    assert validate_pipeline(definition).valid
    trims = [node for node in definition.nodes if node.type == "smart_trim"]
    assert len(trims) == 3
    assert all("绿色背景上的白色中文文字" in node.data.config["prompt"] for node in trims)
    assert all(node.data.config["no_match_policy"] == "fail" for node in trims)
    uploads = [node for node in definition.nodes if node.type == "youtube_upload"]
    assert len(uploads) == 1
    assert uploads[0].data.config["privacy"] == "unlisted"


@pytest.mark.parametrize("shot_count", [1, 3])
@pytest.mark.parametrize(
    ("publish_mode", "no_match_policy", "privacy"),
    [
        ("preview_only", "placeholder", None),
        ("private_upload", "fail", "private"),
        ("unlisted_upload", "fail", "unlisted"),
        ("public_after_review", "fail", "private"),
    ],
)
def test_storyboard_input_video_no_match_policy_respects_upload_mode(
    shot_count, publish_mode, no_match_policy, privacy,
):
    storyboard = StoryboardGenerator().generate(
        AutoFlowStoryboardRequest(
            prompt="Create a 15 second product video",
            input_asset_id="asset-product",
            target_duration=15,
            source_strategy="input_video",
            min_shots=3,
            max_shots=3,
        )
    ).storyboard
    storyboard.shots = storyboard.shots[:shot_count]

    definition = PipelineBuilder().build_storyboard_input_video(
        storyboard, input_asset_id="asset-product", publish_mode=publish_mode,
    )

    validation = validate_pipeline(definition)
    assert validation.valid, [error.message for error in validation.errors]
    trims = [node for node in definition.nodes if node.type == "smart_trim"]
    assert len(trims) == shot_count
    assert all(node.data.config["no_match_policy"] == no_match_policy for node in trims)
    uploads = [node for node in definition.nodes if node.type == "youtube_upload"]
    assert len(uploads) == (0 if privacy is None else 1)
    if uploads:
        assert uploads[0].data.config["privacy"] == privacy
        assert any(
            edge.source == "transcode_1" and edge.target == uploads[0].id
            for edge in definition.edges
        )


def test_storyboard_input_video_pipeline_uses_smart_trim_per_shot():
    storyboard = StoryboardGenerator().generate(
        AutoFlowStoryboardRequest(
            prompt="我要一个 15 秒小猫视频，竖屏，可爱快节奏",
            input_asset_id="asset-cat",
            target_duration=15,
            aspect_ratio="9:16",
            source_strategy="input_video",
            min_shots=3,
            max_shots=3,
        )
    ).storyboard

    definition = PipelineBuilder().build_storyboard_input_video(storyboard, input_asset_id="asset-cat")
    validation = validate_pipeline(definition)

    assert validation.valid, [error.message for error in validation.errors]
    assert [node.type for node in definition.nodes].count("source") == 1
    assert [node.type for node in definition.nodes].count("smart_trim") == 3
    assert any(node.type == "concat_timeline" for node in definition.nodes)
    smart_trim = next(node for node in definition.nodes if node.type == "smart_trim")
    assert smart_trim.data.config["prompt"] == storyboard.shots[0].search_query
    assert smart_trim.data.config["target_duration"] == storyboard.shots[0].target_duration


def test_storyboard_material_pipeline_uses_matched_assets_and_skips_missing_shots():
    storyboard = StoryboardGenerator().generate(
        AutoFlowStoryboardRequest(
            prompt="Create a 10 second generic product video",
            target_duration=10,
            source_strategy="material_library",
            min_shots=3,
            max_shots=3,
        )
    ).storyboard
    storyboard.shots[0].matched_asset_id = "asset-1"
    storyboard.shots[0].matched_start_sec = 1
    storyboard.shots[0].matched_end_sec = 4
    storyboard.shots[0].match_status = "matched"
    storyboard.shots[1].matched_asset_id = "asset-2"
    storyboard.shots[1].match_status = "matched"
    storyboard.shots[2].match_status = "missing"

    definition = PipelineBuilder().build_storyboard_material_library(storyboard)

    assert validate_pipeline(definition).valid
    assert [node.type for node in definition.nodes].count("source") == 2
    assert all(node.type != "smart_trim" for node in definition.nodes)
    assert any(node.type == "concat_timeline" for node in definition.nodes)


def test_storyboard_material_pipeline_adds_private_upload_node_when_requested():
    storyboard = StoryboardGenerator().generate(
        AutoFlowStoryboardRequest(
            prompt="Create a 10 second generic product video",
            target_duration=10,
            source_strategy="material_library",
            min_shots=3,
            max_shots=3,
            target_platforms=["youtube"],
        )
    ).storyboard
    storyboard.shots[0].matched_asset_id = "asset-1"
    storyboard.shots[0].match_status = "matched"
    storyboard.shots[1].matched_asset_id = "asset-2"
    storyboard.shots[1].match_status = "matched"

    definition = PipelineBuilder().build_storyboard_material_library(
        storyboard,
        publish_mode="private_upload",
    )

    assert validate_pipeline(definition).valid
    upload = next(node for node in definition.nodes if node.id == "youtube_upload_1")
    assert upload.type == "youtube_upload"
    assert upload.data.config["privacy"] == "private"
    assert any(edge.source == "transcode_1" and edge.target == "youtube_upload_1" for edge in definition.edges)


def test_storyboard_input_video_pipeline_does_not_truncate_more_than_twelve_shots():
    storyboard = StoryboardGenerator().generate(
        AutoFlowStoryboardRequest(
            prompt="Create a 52 second cat video with many quick beats",
            input_asset_id="asset-cat",
            target_duration=52,
            aspect_ratio="9:16",
            source_strategy="input_video",
            min_shots=13,
            max_shots=13,
        )
    ).storyboard
    base_shot = storyboard.shots[0]
    storyboard.shots = [
        base_shot.model_copy(
            update={
                "id": f"shot_{index:02d}",
                "search_query": f"cat beat {index}",
            },
            deep=True,
        )
        for index in range(1, 14)
    ]

    definition = PipelineBuilder().build_storyboard_input_video(storyboard, input_asset_id="asset-cat")

    assert validate_pipeline(definition).valid
    concat = next(node for node in definition.nodes if node.type == "concat_timeline")
    assert concat.data.config["input_count"] == 13
    assert [edge.targetHandle for edge in definition.edges if edge.target == concat.id] == [
        f"video_{index}" for index in range(1, 14)
    ]
