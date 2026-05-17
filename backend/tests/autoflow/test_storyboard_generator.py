from __future__ import annotations

from app.autoflow.storyboard_generator import StoryboardGenerator
from app.schemas.autoflow import AutoFlowStoryboardRequest, StoryboardPlan


def test_rule_based_storyboard_generates_long_cat_shots_without_video_generation():
    request = AutoFlowStoryboardRequest(
        prompt="我要一个 30 秒小猫视频，竖屏，可爱快节奏。素材来自我上传的视频。如果没有合适片段，先标记缺失，不要生成。",
        target_duration=30,
        aspect_ratio="9:16",
        source_strategy="input_video",
        allow_video_generation=False,
        min_shots=3,
        max_shots=5,
    )

    response = StoryboardGenerator().generate(request)
    storyboard = response.storyboard

    assert isinstance(storyboard, StoryboardPlan)
    assert storyboard.subject == "小猫"
    assert storyboard.aspect_ratio == "9:16"
    assert storyboard.source_strategy == "input_video"
    assert 3 <= len(storyboard.shots) <= 5
    assert sum(shot.target_duration for shot in storyboard.shots) == 30
    assert all(len(shot.description) >= 30 for shot in storyboard.shots)
    assert all(shot.search_query for shot in storyboard.shots)
    assert all(shot.generation.prompt for shot in storyboard.shots)
    assert all(shot.generation.enabled is False for shot in storyboard.shots)


def test_rule_based_storyboard_marks_generation_enabled_when_allowed():
    request = AutoFlowStoryboardRequest(
        prompt="Create a 12 second dog video",
        target_duration=12,
        source_strategy="generate_missing",
        allow_video_generation=True,
        min_shots=3,
        max_shots=3,
    )

    storyboard = StoryboardGenerator().generate(request).storyboard

    assert storyboard.subject in {"小狗", "dog"}
    assert storyboard.allow_video_generation is True
    assert all(shot.generation.enabled is True for shot in storyboard.shots)
    assert all(shot.match_status == "pending" for shot in storyboard.shots)
