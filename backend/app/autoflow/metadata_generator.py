from __future__ import annotations

import json
import re
from typing import Any, Protocol

import httpx

from app.autoflow.platform_profiles import PlatformProfileService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent, AutoFlowMetadata


class MetadataLLMClient(Protocol):
    def generate(self, payload: dict[str, Any]) -> dict[str, Any]:
        ...


class MetadataGenerator:
    def __init__(
        self,
        *,
        llm_client: MetadataLLMClient | None = None,
        platform_profiles: PlatformProfileService | None = None,
    ) -> None:
        self.llm_client = llm_client
        self.platform_profiles = platform_profiles or PlatformProfileService()

    def generate(
        self,
        intent: AutoFlowIntent,
        candidates: list[AutoFlowClipCandidate],
    ) -> AutoFlowMetadata:
        subject = intent.subject or "短视频"
        platforms = intent.target_platforms or ["youtube_shorts"]
        profile = self.platform_profiles.for_platforms(platforms)
        facts = [_clip_fact(candidate) for candidate in candidates[:8]]
        grounding_text = _grounding_text(subject, intent, facts)
        warnings: list[str] = []

        llm_payload = self._generate_with_llm(intent, facts, profile.to_dict(), warnings)
        fallback = _fallback_metadata(subject, intent, facts, profile.title_max_chars)
        titles = _validated_strings(
            llm_payload.get("titles") if llm_payload else [],
            grounding_text,
            max_count=10,
            max_chars=profile.title_max_chars,
        )
        thumbnail_texts, removed_thumbnail = _validated_strings_with_removed(
            llm_payload.get("thumbnail_texts") if llm_payload else [],
            grounding_text,
            max_count=5,
            max_chars=profile.thumbnail_text_max_chars,
        )
        if removed_thumbnail:
            warnings.append("metadata_llm_ungrounded_claims_removed")

        tags = _dedupe(
            [
                *(_validated_tag_strings(llm_payload.get("tags") if llm_payload else [])),
                subject,
                *intent.keywords,
                intent.intent_type,
                *fallback["tags"],
            ]
        )
        if not titles:
            titles = fallback["titles"]
        if not thumbnail_texts:
            thumbnail_texts = fallback["thumbnail_texts"]

        selected_title = titles[0] if titles else subject
        description = _description(subject, candidates, facts)
        hashtags = [f"#{tag}" for tag in tags[:8] if tag]
        payloads = {
            platform: {
                "title": selected_title,
                "description": description,
                "tags": tags,
                "hashtags": hashtags,
                "privacy": "private" if platform in {"youtube", "youtube_shorts"} else "draft",
                "warnings": list(warnings),
            }
            for platform in platforms
        }

        return AutoFlowMetadata(
            title_candidates=titles,
            selected_title=selected_title,
            description=description,
            tags=tags,
            hashtags=hashtags,
            thumbnail_text_candidates=thumbnail_texts,
            platform_payloads=payloads,
        )

    def _generate_with_llm(
        self,
        intent: AutoFlowIntent,
        facts: list[dict[str, Any]],
        platform_profile: dict[str, object],
        warnings: list[str],
    ) -> dict[str, Any]:
        if self.llm_client is None:
            return {}
        try:
            payload = self.llm_client.generate(
                {
                    "intent": intent.model_dump(mode="json"),
                    "clip_facts": facts,
                    "platform_profile": platform_profile,
                    "output_schema": {
                        "titles": "list[str]",
                        "thumbnail_texts": "list[str]",
                        "tags": "list[str]",
                        "rationale": "str",
                    },
                }
            )
        except Exception:
            warnings.append("metadata_llm_unavailable")
            return {}
        if not isinstance(payload, dict):
            warnings.append("metadata_llm_invalid_json")
            return {}
        return payload


class LLMGatewayMetadataClient:
    def __init__(
        self,
        *,
        base_url: str,
        timeout_seconds: float,
        source: str = "videoprocess",
        profile: str = "generic_chat",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.source = source
        self.profile = profile

    def generate(self, payload: dict[str, Any]) -> dict[str, Any]:
        request_payload = {
            "model": "auto",
            "temperature": 0.3,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Generate VideoProcess AutoFlow metadata as strict JSON. "
                        "Only use facts supplied in the user message. Do not invent visual events."
                    ),
                },
                {
                    "role": "user",
                    "content": _json_dumps(
                        {
                            "source": self.source,
                            "profile": self.profile,
                            "input": payload,
                            "required_shape": {
                                "titles": ["string"],
                                "thumbnail_texts": ["string"],
                                "tags": ["string"],
                                "rationale": "string",
                            },
                        }
                    ),
                },
            ],
        }
        with httpx.Client(timeout=self.timeout_seconds) as client:
            response = client.post(f"{self.base_url}/v1/chat/completions", json=request_payload)
        response.raise_for_status()
        data = response.json()
        content = str(data["choices"][0]["message"]["content"])
        return _json_object_from_text(content)


def _clip_fact(candidate: AutoFlowClipCandidate) -> dict[str, Any]:
    metadata = candidate.metadata if isinstance(candidate.metadata, dict) else {}
    visual = metadata.get("visual") if isinstance(metadata.get("visual"), dict) else {}
    object_labels = _list_value(visual.get("object_labels", metadata.get("object_labels")))
    dominant_action = str(visual.get("dominant_action") or metadata.get("dominant_action") or "")
    tags = _list_value(metadata.get("tags", metadata.get("keywords")))
    return {
        "title": candidate.title,
        "description": str(metadata.get("description") or ""),
        "tags": tags,
        "object_labels": object_labels,
        "dominant_action": dominant_action,
        "duration": _duration(candidate),
        "source_platform": str(metadata.get("source_platform") or metadata.get("platform") or candidate.source_type),
    }


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _json_object_from_text(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("LLM response did not contain a JSON object")
    payload = json.loads(cleaned[start : end + 1])
    if not isinstance(payload, dict):
        raise ValueError("LLM response was not a JSON object")
    return payload


def _fallback_metadata(
    subject: str,
    intent: AutoFlowIntent,
    facts: list[dict[str, Any]],
    title_max_chars: int,
) -> dict[str, list[str]]:
    observed_action = _first_non_empty(fact.get("dominant_action") for fact in facts)
    observed_label = _first_non_empty(label for fact in facts for label in fact.get("object_labels", []))
    observed_title = _first_non_empty(fact.get("title") for fact in facts)
    descriptor = observed_action or observed_label or observed_title or "高光片段"
    titles = _dedupe(
        [
            _truncate(f"{subject}{descriptor}合集", title_max_chars),
            _truncate(f"{int(intent.duration_sec)} 秒{subject}精选", title_max_chars),
            _truncate(f"今日份{subject}短片", title_max_chars),
            _truncate(f"{subject}素材库精选", title_max_chars),
            _truncate(f"把{subject}剪成一支短片", title_max_chars),
        ]
    )
    thumbnail_texts = _dedupe(
        [
            _truncate(f"{subject}{observed_action}", 14) if observed_action else "",
            _truncate(f"{subject}{observed_label}", 14) if observed_label else "",
            _truncate(f"{subject}高光", 14),
        ]
    )
    tags = _dedupe([subject, observed_action, observed_label, intent.intent_type])
    return {"titles": titles, "thumbnail_texts": thumbnail_texts, "tags": tags}


def _description(subject: str, candidates: list[AutoFlowClipCandidate], facts: list[dict[str, Any]]) -> str:
    strongest_fact = _first_non_empty(fact.get("dominant_action") for fact in facts)
    if strongest_fact:
        return f"AutoFlow generated preview for {subject}, grounded in selected clips showing {strongest_fact}."
    return f"AutoFlow generated preview for {subject}. Candidate clips: {len(candidates)}."


def _validated_strings(
    values: object,
    grounding_text: str,
    *,
    max_count: int,
    max_chars: int,
) -> list[str]:
    valid, _removed = _validated_strings_with_removed(
        values,
        grounding_text,
        max_count=max_count,
        max_chars=max_chars,
    )
    return valid


def _validated_strings_with_removed(
    values: object,
    grounding_text: str,
    *,
    max_count: int,
    max_chars: int,
) -> tuple[list[str], bool]:
    removed = False
    result: list[str] = []
    for value in values if isinstance(values, list) else []:
        text = _truncate(str(value).strip(), max_chars)
        if not text:
            continue
        if not _is_grounded(text, grounding_text):
            removed = True
            continue
        if text not in result:
            result.append(text)
        if len(result) >= max_count:
            break
    return result, removed


def _validated_tag_strings(values: object) -> list[str]:
    result: list[str] = []
    for value in values if isinstance(values, list) else []:
        text = re.sub(r"[#\s]+", "", str(value).strip())
        if text and text not in result:
            result.append(text[:24])
    return result[:12]


def _is_grounded(text: str, grounding_text: str) -> bool:
    normalized = text.lower()
    grounded = grounding_text.lower()
    if normalized in grounded:
        return True
    tokens = [token for token in re.findall(r"[\w\u4e00-\u9fff]+", normalized) if len(token) >= 2]
    if not tokens:
        return False
    if any(token in grounded for token in tokens):
        return True
    cjk_bigrams = {
        normalized[index : index + 2]
        for index in range(max(0, len(normalized) - 1))
        if re.fullmatch(r"[\u4e00-\u9fff]{2}", normalized[index : index + 2])
    }
    return any(token in grounded for token in cjk_bigrams)


def _grounding_text(subject: str, intent: AutoFlowIntent, facts: list[dict[str, Any]]) -> str:
    parts: list[str] = [subject, intent.intent_type, *intent.keywords]
    for fact in facts:
        parts.extend(
            [
                str(fact.get("title") or ""),
                str(fact.get("description") or ""),
                str(fact.get("dominant_action") or ""),
                str(fact.get("source_platform") or ""),
            ]
        )
        parts.extend(str(tag) for tag in fact.get("tags", []))
        parts.extend(str(label) for label in fact.get("object_labels", []))
    return " ".join(part for part in parts if part)


def _list_value(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if value:
        return [str(value)]
    return []


def _duration(candidate: AutoFlowClipCandidate) -> float:
    if candidate.start_sec is not None and candidate.end_sec is not None:
        return max(0.0, float(candidate.end_sec) - float(candidate.start_sec))
    value = candidate.metadata.get("duration") or candidate.metadata.get("duration_sec") if candidate.metadata else 0
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _first_non_empty(values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _truncate(value: str, max_chars: int) -> str:
    return value[:max_chars].strip()


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result
