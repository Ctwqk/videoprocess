from __future__ import annotations

from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowMetadata


class MetadataGenerator:
    def generate(
        self,
        intent: AutoFlowIntent,
        candidates: list[AutoFlowClipCandidate],
    ) -> AutoFlowMetadata:
        subject = intent.subject or "短视频"
        if intent.intent_type == "animal_compilation":
            titles = [
                f"{intent.duration_sec} 秒内看完这些{subject}的离谱瞬间",
                f"这些{subject}的反应也太可爱了",
                f"{subject}：我只是路过，结果翻车了",
                f"今日份{subject}快乐源泉",
                f"这只{subject}的最后 2 秒太好笑了",
            ]
        elif intent.intent_type == "hot_topic_explainer":
            titles = [
                f"{intent.duration_sec} 秒讲清楚：{subject}",
                f"为什么大家都在讨论{subject}",
                f"今天的{subject}到底发生了什么",
                f"一口气看懂{subject}",
                f"{subject}的关键信息都在这里",
            ]
        else:
            titles = [
                f"{subject}治愈混剪",
                f"{intent.duration_sec} 秒{subject}高光片段",
                f"把{subject}剪成一支短片",
                f"{subject}素材库精选",
                f"今日份{subject}灵感",
            ]

        tags = _dedupe([subject, *intent.keywords, intent.intent_type])
        hashtags = [f"#{tag}" for tag in tags[:8] if tag]
        selected_title = titles[0]
        description = f"AutoFlow generated preview for {subject}. Candidate clips: {len(candidates)}."

        payloads: dict[str, dict] = {}
        platforms = intent.target_platforms or ["youtube_shorts"]
        for platform in platforms:
            payloads[platform] = {
                "title": selected_title,
                "description": description,
                "tags": tags,
                "hashtags": hashtags,
                "privacy": "private" if platform in {"youtube", "youtube_shorts"} else "draft",
            }

        return AutoFlowMetadata(
            title_candidates=titles,
            selected_title=selected_title,
            description=description,
            tags=tags,
            hashtags=hashtags,
            thumbnail_text_candidates=[titles[0][:24], f"{subject}高光", "最后 2 秒"],
            platform_payloads=payloads,
        )


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result
