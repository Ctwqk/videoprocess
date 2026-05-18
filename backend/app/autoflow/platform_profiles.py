from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


PacingCurve = Literal["front_loaded", "steady", "long_form"]


@dataclass(frozen=True)
class PlatformProfile:
    platform_key: str
    min_shot_seconds: float
    max_shot_seconds: float
    hook_seconds: float
    pacing_curve: PacingCurve
    preferred_aspect_ratios: list[str]
    title_max_chars: int
    thumbnail_text_max_chars: int
    motion_preference: float
    novelty_preference: float

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


_GENERIC = PlatformProfile(
    platform_key="generic",
    min_shot_seconds=1.5,
    max_shot_seconds=3.5,
    hook_seconds=2.0,
    pacing_curve="steady",
    preferred_aspect_ratios=["9:16", "16:9", "1:1"],
    title_max_chars=60,
    thumbnail_text_max_chars=16,
    motion_preference=0.6,
    novelty_preference=0.6,
)

_PROFILES: dict[str, PlatformProfile] = {
    "generic": _GENERIC,
    "douyin": PlatformProfile(
        platform_key="douyin",
        min_shot_seconds=0.5,
        max_shot_seconds=2.0,
        hook_seconds=1.0,
        pacing_curve="front_loaded",
        preferred_aspect_ratios=["9:16"],
        title_max_chars=36,
        thumbnail_text_max_chars=10,
        motion_preference=1.0,
        novelty_preference=0.9,
    ),
    "tiktok": PlatformProfile(
        platform_key="tiktok",
        min_shot_seconds=0.5,
        max_shot_seconds=2.0,
        hook_seconds=1.0,
        pacing_curve="front_loaded",
        preferred_aspect_ratios=["9:16"],
        title_max_chars=40,
        thumbnail_text_max_chars=10,
        motion_preference=1.0,
        novelty_preference=0.9,
    ),
    "youtube_shorts": PlatformProfile(
        platform_key="youtube_shorts",
        min_shot_seconds=0.5,
        max_shot_seconds=2.0,
        hook_seconds=1.0,
        pacing_curve="front_loaded",
        preferred_aspect_ratios=["9:16"],
        title_max_chars=40,
        thumbnail_text_max_chars=12,
        motion_preference=0.95,
        novelty_preference=0.85,
    ),
    "youtube": PlatformProfile(
        platform_key="youtube",
        min_shot_seconds=3.0,
        max_shot_seconds=5.0,
        hook_seconds=3.0,
        pacing_curve="long_form",
        preferred_aspect_ratios=["16:9", "9:16"],
        title_max_chars=70,
        thumbnail_text_max_chars=18,
        motion_preference=0.55,
        novelty_preference=0.65,
    ),
    "bilibili": PlatformProfile(
        platform_key="bilibili",
        min_shot_seconds=2.0,
        max_shot_seconds=4.0,
        hook_seconds=2.0,
        pacing_curve="steady",
        preferred_aspect_ratios=["16:9", "9:16"],
        title_max_chars=54,
        thumbnail_text_max_chars=14,
        motion_preference=0.7,
        novelty_preference=0.7,
    ),
}


_ALIASES = {
    "抖音": "douyin",
    "douyin": "douyin",
    "tik tok": "tiktok",
    "tiktok": "tiktok",
    "youtube shorts": "youtube_shorts",
    "yt_shorts": "youtube_shorts",
    "shorts": "youtube_shorts",
    "youtube_shorts": "youtube_shorts",
    "youtube": "youtube",
    "b站": "bilibili",
    "bilibili": "bilibili",
}


class PlatformProfileService:
    def for_platforms(self, platforms: list[str] | tuple[str, ...] | None) -> PlatformProfile:
        resolved = [profile for platform in platforms or [] if (profile := self._resolve(platform))]
        if not resolved:
            return _GENERIC
        if len(resolved) == 1:
            return resolved[0]
        return _merge_profiles(resolved)

    def _resolve(self, platform: str) -> PlatformProfile | None:
        key = _ALIASES.get(str(platform or "").strip().lower())
        if not key:
            return None
        return _PROFILES.get(key)


def _merge_profiles(profiles: list[PlatformProfile]) -> PlatformProfile:
    aspects: list[str] = []
    for profile in profiles:
        for aspect_ratio in profile.preferred_aspect_ratios:
            if aspect_ratio not in aspects:
                aspects.append(aspect_ratio)
    return PlatformProfile(
        platform_key="merged",
        min_shot_seconds=min(profile.min_shot_seconds for profile in profiles),
        max_shot_seconds=min(profile.max_shot_seconds for profile in profiles),
        hook_seconds=min(profile.hook_seconds for profile in profiles),
        pacing_curve=_merged_pacing_curve(profiles),
        preferred_aspect_ratios=aspects or list(_GENERIC.preferred_aspect_ratios),
        title_max_chars=min(profile.title_max_chars for profile in profiles),
        thumbnail_text_max_chars=min(profile.thumbnail_text_max_chars for profile in profiles),
        motion_preference=max(profile.motion_preference for profile in profiles),
        novelty_preference=max(profile.novelty_preference for profile in profiles),
    )


def _merged_pacing_curve(profiles: list[PlatformProfile]) -> PacingCurve:
    curves = {profile.pacing_curve for profile in profiles}
    if "front_loaded" in curves:
        return "front_loaded"
    if "long_form" in curves:
        return "long_form"
    return "steady"
