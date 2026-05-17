from __future__ import annotations

from app.autoflow.intent_parser import RuleBasedIntentParser
from app.schemas.autoflow import AutoFlowRequest


def parse(prompt: str, **overrides):
    parser = RuleBasedIntentParser()
    return parser.parse(AutoFlowRequest(prompt=prompt, **overrides))


def test_parses_chinese_cat_compilation_prompt():
    intent = parse("我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要直接公开发布。")

    assert intent.intent_type == "animal_compilation"
    assert intent.subject == "小猫"
    assert intent.duration_sec == 30
    assert intent.aspect_ratio == "9:16"
    assert intent.publish_mode == "preview_only"
    assert intent.source_policy == "owned_only"
    assert "cat" in intent.keywords
    assert intent.needs_bgm is True


def test_parses_chinese_hot_topic_explainer_prompt():
    intent = parse("做一个 45 秒的热点解释短视频，解释今天大家为什么讨论某个 AI 工具，竖屏，有字幕和旁白。")

    assert intent.intent_type == "hot_topic_explainer"
    assert intent.subject == "AI 工具"
    assert intent.duration_sec == 45
    assert intent.aspect_ratio == "9:16"
    assert intent.needs_voiceover is True
    assert intent.needs_subtitles is True
    assert "热点" in intent.keywords


def test_parses_chinese_material_library_remix_prompt():
    intent = parse("从我的旅行素材库里找海边、日落、人物背影，做一个 20 秒治愈混剪。")

    assert intent.intent_type == "material_library_remix"
    assert intent.subject == "旅行素材"
    assert intent.duration_sec == 20
    assert intent.source_policy == "owned_only"
    assert "海边" in intent.keywords
    assert "日落" in intent.keywords
    assert intent.style == "healing_remix"


def test_request_overrides_take_precedence_over_prompt_defaults():
    intent = parse(
        "做一个小狗搞笑集锦",
        duration_sec=15,
        aspect_ratio="16:9",
        source_policy="research_only",
        publish_mode="private_upload",
    )

    assert intent.duration_sec == 15
    assert intent.aspect_ratio == "16:9"
    assert intent.source_policy == "research_only"
    assert intent.publish_mode == "private_upload"


def test_fallback_generic_intent_has_confirmation_question():
    intent = parse("随便做一个视频")

    assert intent.intent_type == "generic_video"
    assert intent.subject == "视频"
    assert intent.user_confirmation_questions
