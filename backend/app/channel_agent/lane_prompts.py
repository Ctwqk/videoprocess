from __future__ import annotations

PROMPT_TEMPLATE = """Create a {format_key} video for the \"{lane_name}\" topic.
Theme: {lane_description}
Keywords: {keywords}
Target duration: {duration_sec}s, aspect ratio {aspect_ratio}.
"""


def build_lane_prompt(
    *,
    lane_name: str,
    lane_description: str,
    keywords: list[str],
    format_key: str,
    duration_sec: int,
    aspect_ratio: str,
) -> str:
    keyword_text = ", ".join(keyword for keyword in keywords if keyword) or lane_name
    return PROMPT_TEMPLATE.format(
        lane_name=lane_name,
        lane_description=lane_description or lane_name,
        keywords=keyword_text,
        format_key=format_key,
        duration_sec=duration_sec,
        aspect_ratio=aspect_ratio,
    ).strip()
