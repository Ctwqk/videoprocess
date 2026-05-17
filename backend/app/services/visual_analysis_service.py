from __future__ import annotations

import re
from pathlib import Path
from typing import Any


class VisualAnalysisService:
    def analyze(
        self,
        source_path: str | Path | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = metadata or {}
        source = Path(source_path) if source_path is not None else None
        duration = _number(metadata.get("duration"), metadata.get("duration_sec"), metadata.get("length_sec"), default=0.0)
        width = _int_number(metadata.get("width"), default=0)
        height = _int_number(metadata.get("height"), default=0)

        if source and (not width or not height):
            parsed_width, parsed_height = _parse_dimensions_from_name(source.name)
            width = width or parsed_width
            height = height or parsed_height

        aspect_ratio = str(metadata.get("aspect_ratio") or _aspect_ratio(width, height))
        visual = metadata.get("visual") if isinstance(metadata.get("visual"), dict) else {}
        file_size = source.stat().st_size if source and source.exists() else 0
        motion_score = _clamp(_number(visual.get("motion_score"), metadata.get("motion_score"), default=_file_score(file_size, 17)))
        watermark_score = _clamp(
            _number(visual.get("watermark_score"), metadata.get("watermark_score"), default=_file_score(file_size, 7) * 0.2)
        )
        quality_score = _clamp(_number(metadata.get("quality_score"), default=0.65 if file_size else 0.5))

        result = {
            "duration": duration,
            "aspect_ratio": aspect_ratio,
            "width": width,
            "height": height,
            "motion_score": motion_score,
            "watermark_score": watermark_score,
            "quality_score": quality_score,
            "file_size_bytes": file_size,
            "visual": {
                "motion_score": motion_score,
                "watermark_score": watermark_score,
                "suggested_crop": _suggested_crop(aspect_ratio),
                "object_labels": list(visual.get("object_labels") or []),
                "ocr_text": str(visual.get("ocr_text") or ""),
            },
        }
        return result


def _number(*values: object, default: float) -> float:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _int_number(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _aspect_ratio(width: int, height: int) -> str:
    if width <= 0 or height <= 0:
        return "auto"
    ratio = width / height
    if abs(ratio - 16 / 9) < 0.08:
        return "16:9"
    if abs(ratio - 9 / 16) < 0.08:
        return "9:16"
    if abs(ratio - 1.0) < 0.08:
        return "1:1"
    return f"{width}:{height}"


def _parse_dimensions_from_name(name: str) -> tuple[int, int]:
    match = re.search(r"(?P<width>\d{3,5})x(?P<height>\d{3,5})", name)
    if not match:
        return 0, 0
    return int(match.group("width")), int(match.group("height"))


def _suggested_crop(aspect_ratio: str) -> dict[str, float]:
    if aspect_ratio == "9:16":
        return {"x": 0.0, "y": 0.0, "w": 1.0, "h": 1.0}
    if aspect_ratio == "16:9":
        return {"x": 0.21875, "y": 0.0, "w": 0.5625, "h": 1.0}
    return {"x": 0.0, "y": 0.0, "w": 1.0, "h": 1.0}


def _file_score(file_size: int, salt: int) -> float:
    if file_size <= 0:
        return 0.0
    return ((file_size + salt) % 100) / 100


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))
