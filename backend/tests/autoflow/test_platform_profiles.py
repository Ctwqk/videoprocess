from __future__ import annotations

from app.autoflow.platform_profiles import PlatformProfileService


def test_platform_profile_merges_short_form_constraints():
    profile = PlatformProfileService().for_platforms(["youtube", "douyin", "bilibili"])

    assert profile.platform_key == "merged"
    assert profile.max_shot_seconds == 2.0
    assert profile.title_max_chars <= 40
    assert profile.motion_preference == 1.0
    assert "9:16" in profile.preferred_aspect_ratios
    assert profile.pacing_curve == "front_loaded"


def test_unknown_platform_uses_generic_profile():
    profile = PlatformProfileService().for_platforms(["unknown-platform"])

    assert profile.platform_key == "generic"
    assert profile.min_shot_seconds == 1.5
    assert profile.max_shot_seconds == 3.5
