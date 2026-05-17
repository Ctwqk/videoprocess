from __future__ import annotations

from app.autoflow.pipeline_builder import PipelineBuilder
from app.autoflow.storyboard_generator import StoryboardGenerator
from app.orchestrator.dag import validate_pipeline
from app.schemas.autoflow import AutoFlowStoryboardRequest


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
    assert any(node.type == "concat_many" for node in definition.nodes)
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
    assert any(node.type == "concat_many" for node in definition.nodes)
