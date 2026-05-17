from __future__ import annotations

import re

from app.schemas.autoflow import AutoFlowIntent, AutoFlowRequest


class RuleBasedIntentParser:
    def parse(self, request: AutoFlowRequest) -> AutoFlowIntent:
        prompt = request.prompt
        duration = request.duration_sec or _duration_from_prompt(prompt) or 30
        aspect_ratio = request.aspect_ratio if request.aspect_ratio != "auto" else _aspect_ratio_from_prompt(prompt)
        publish_mode = request.publish_mode
        if request.publish_mode == "preview_only":
            publish_mode = _publish_mode_from_prompt(prompt)

        intent_type = "generic_video"
        subject = "视频"
        style = "auto"
        keywords: list[str] = []
        needs_voiceover = False
        needs_subtitles = True
        needs_bgm = True
        confirmation_questions: list[str] = []

        lowered = prompt.lower()
        if any(word in lowered for word in ("小猫", "猫", "cat", "kitten", "小狗", "狗", "dog", "puppy", "宠物")):
            intent_type = "animal_compilation"
            if "小猫" in prompt:
                subject = "小猫"
                keywords = ["小猫", "可爱", "搞笑", "cat", "kitten"]
            elif "小狗" in prompt:
                subject = "小狗"
                keywords = ["小狗", "搞笑", "dog", "puppy"]
            else:
                subject = "宠物"
                keywords = ["宠物", "可爱", "pet"]
            style = "cute_fast_montage" if any(word in prompt for word in ("可爱", "快节奏", "搞笑")) else "animal_montage"
        elif any(word in prompt for word in ("热点", "解释", "发生了什么", "讨论")):
            intent_type = "hot_topic_explainer"
            subject = _extract_hot_topic_subject(prompt)
            style = "explainer_short"
            keywords = ["热点", subject]
            needs_voiceover = "旁白" in prompt or "讲解" in prompt
            needs_subtitles = True
            needs_bgm = False
        elif any(word in prompt for word in ("素材库", "混剪", "旅行素材")):
            intent_type = "material_library_remix"
            subject = "旅行素材" if "旅行" in prompt else "素材库"
            style = "healing_remix" if any(word in prompt for word in ("治愈", "海边", "日落")) else "library_remix"
            keywords = [word for word in ("海边", "日落", "人物背影", "旅行", "治愈") if word in prompt]
            needs_voiceover = False
            needs_bgm = True
        else:
            confirmation_questions.append("Please choose a content type or template before AutoFlow builds a workflow.")

        source_policy = request.source_policy
        if source_policy == "owned_only" and any(word in prompt for word in ("外部", "搜索", "下载")):
            source_policy = "research_only"

        return AutoFlowIntent(
            intent_type=intent_type,
            subject=subject,
            style=style,
            duration_sec=duration,
            aspect_ratio=aspect_ratio,
            target_platforms=request.target_platforms or _target_platforms_from_prompt(prompt),
            source_policy=source_policy,
            publish_mode=publish_mode,
            keywords=_dedupe(keywords),
            needs_voiceover=needs_voiceover,
            needs_subtitles=needs_subtitles,
            needs_bgm=needs_bgm,
            user_confirmation_questions=confirmation_questions,
        )


def _duration_from_prompt(prompt: str) -> int | None:
    match = re.search(r"(\d+)\s*(秒|s|sec|second)", prompt, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _aspect_ratio_from_prompt(prompt: str) -> str:
    lowered = prompt.lower()
    if "竖屏" in prompt or "shorts" in lowered or "9:16" in prompt:
        return "9:16"
    if "横屏" in prompt or "16:9" in prompt:
        return "16:9"
    if "方形" in prompt or "1:1" in prompt:
        return "1:1"
    return "9:16"


def _publish_mode_from_prompt(prompt: str) -> str:
    if any(word in prompt for word in ("不要发布", "不公开", "预览", "草稿")):
        return "preview_only"
    if "unlisted" in prompt.lower() or "不公开视频" in prompt:
        return "unlisted_upload"
    if "private" in prompt.lower() or "私密" in prompt:
        return "private_upload"
    return "preview_only"


def _target_platforms_from_prompt(prompt: str) -> list[str]:
    platforms: list[str] = []
    lowered = prompt.lower()
    if "shorts" in lowered or "youtube" in lowered:
        platforms.append("youtube_shorts")
    if "x" in lowered or "twitter" in lowered:
        platforms.append("x")
    if "小红书" in prompt:
        platforms.append("xiaohongshu")
    return platforms


def _extract_hot_topic_subject(prompt: str) -> str:
    match = re.search(r"讨论(.+?)(，|。|,|$)", prompt)
    if match:
        subject = match.group(1).strip()
        subject = subject.removeprefix("某个").strip()
        return subject or "热点"
    return "热点"


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
