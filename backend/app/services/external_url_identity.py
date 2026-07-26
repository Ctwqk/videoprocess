from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def normalize_external_media_url(url: str) -> str:
    parsed = urlparse(url.strip())
    host = parsed.netloc.lower()
    hostname = (parsed.hostname or "").lower()

    if _host_matches(hostname, "youtu.be"):
        video_id = parsed.path.strip("/").split("/", 1)[0]
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"

    if _host_matches(hostname, "youtube.com"):
        query = dict(parse_qsl(parsed.query, keep_blank_values=False))
        video_id = query.get("v", "").strip()
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"

    bilibili_id = (
        _extract_bilibili_bvid(parsed.path)
        if _host_matches(hostname, "bilibili.com")
        else None
    )
    if bilibili_id:
        return f"https://www.bilibili.com/video/{bilibili_id}"

    xiaohongshu_note_id = (
        _extract_xiaohongshu_note_id(parsed.path)
        if _host_matches(hostname, "xiaohongshu.com")
        else None
    )
    if xiaohongshu_note_id:
        return f"https://www.xiaohongshu.com/explore/{xiaohongshu_note_id}"

    x_post_id, x_screen_name = (
        _extract_x_post_components(parsed.path)
        if (
            _host_matches(hostname, "x.com")
            or _host_matches(hostname, "twitter.com")
        )
        else (None, None)
    )
    if x_post_id and x_screen_name:
        return f"https://x.com/{x_screen_name}/status/{x_post_id}"

    normalized_query = urlencode(
        sorted(parse_qsl(parsed.query, keep_blank_values=True))
    )
    cleaned = parsed._replace(
        scheme=(parsed.scheme or "https").lower(),
        netloc=host,
        fragment="",
        query=normalized_query,
    )
    return urlunparse(cleaned)


def _host_matches(hostname: str, root: str) -> bool:
    return hostname == root or hostname.endswith(f".{root}")


def _extract_bilibili_bvid(path: str) -> str | None:
    match = re.search(r"(?:^|/)(BV[0-9A-Za-z]+)(?:/|$)", path)
    return match.group(1) if match else None


def _extract_xiaohongshu_note_id(path: str) -> str | None:
    match = re.search(
        r"(?:^|/)([0-9a-f]{24})(?:/|$)",
        path,
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else None


def _extract_x_post_components(path: str) -> tuple[str | None, str | None]:
    match = re.search(
        r"^/([^/]+)/status/(\d+)(?:/|$)",
        path,
        flags=re.IGNORECASE,
    )
    if not match:
        return None, None
    return match.group(2), match.group(1)
