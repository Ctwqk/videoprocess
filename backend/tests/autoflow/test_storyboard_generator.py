from __future__ import annotations

import pytest

from app.autoflow.storyboard_generator import StoryboardGenerator
from app.schemas.autoflow import AutoFlowStoryboardRequest, StoryboardPlan


@pytest.mark.parametrize(
    ("prompt", "subject"),
    [
        ("绿色背景上的白色中文文字", "绿色背景上的白色中文文字"),
        ("深色网格背景，黄色边框内的红色矩形", "深色网格背景，黄色边框内的红色矩形"),
        ("  Green\n background\tand white text  ", "Green background and white text"),
        ("An educational video about green backgrounds", "An educational video about green backgrounds"),
        ("A video about dogma", "A video about dogma"),
        ("Film production workflow", "Film production workflow"),
    ],
)
def test_unknown_topic_survives_into_storyboard_search_queries(prompt, subject):
    storyboard = StoryboardGenerator().generate(AutoFlowStoryboardRequest(prompt=prompt)).storyboard

    assert storyboard.subject == subject
    assert len(storyboard.shots) == 3
    assert all(subject in shot.search_query for shot in storyboard.shots)
    assert all(subject in shot.generation.prompt for shot in storyboard.shots)


@pytest.mark.parametrize(
    ("prompt", "subject"),
    [
        ("A CAT video", "小猫"),
        ("Two kittens playing", "小猫"),
        ("A dog's day", "dog"),
        ("Two puppies playing", "小狗"),
        ("A product-demo", "产品"),
        ("Two products", "产品"),
    ],
)
def test_builtin_topics_remain_available_as_whole_words(prompt, subject):
    storyboard = StoryboardGenerator().generate(AutoFlowStoryboardRequest(prompt=prompt)).storyboard

    assert storyboard.subject == subject
    assert all(subject in shot.search_query or "小狗" in shot.search_query for shot in storyboard.shots)


def test_long_freeform_topic_keeps_queries_within_visual_provider_limit():
    prompt = "绿色背景上的白色中文文字" * 100
    storyboard = StoryboardGenerator().generate(AutoFlowStoryboardRequest(prompt=prompt)).storyboard

    assert storyboard.subject.startswith("绿色背景上的白色中文文字")
    assert len(storyboard.title) <= 100
    assert all(0 < len(shot.search_query) <= 512 for shot in storyboard.shots)
    assert all(0 < len(shot.generation.prompt) <= 512 for shot in storyboard.shots)


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


def test_storyboard_fit_uses_short_video_hook_and_clamps():
    request = AutoFlowStoryboardRequest(
        prompt="我要一个 8 秒小猫视频，竖屏，可爱快节奏。",
        target_duration=8,
        aspect_ratio="9:16",
        target_platforms=["douyin"],
        source_strategy="input_video",
        allow_video_generation=False,
        min_shots=5,
        max_shots=5,
    )

    storyboard = StoryboardGenerator().generate(request).storyboard
    durations = [shot.target_duration for shot in storyboard.shots]

    assert sum(durations) == 8
    assert durations[0] == 1.0
    assert all(0.5 <= duration <= 2.0 for duration in durations)
    assert storyboard.extra["platform_profile"]["platform_key"] == "douyin"
