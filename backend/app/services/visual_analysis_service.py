from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from statistics import mean
from typing import Any


class VisualAnalysisService:
    def analyze(
        self,
        source_path: str | Path | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = metadata or {}
        source = Path(source_path) if source_path is not None else None
        duration = _number(
            metadata.get("duration"),
            metadata.get("duration_sec"),
            metadata.get("length_sec"),
            default=0.0,
        )
        width = _int_number(metadata.get("width"), default=0)
        height = _int_number(metadata.get("height"), default=0)
        methods = {"probe": "metadata" if duration or width or height else "fallback"}

        probed = _ffprobe(source)
        if probed:
            methods["probe"] = "ffprobe"
            duration = duration or _number(probed.get("duration"), default=0.0)
            width = width or _int_number(probed.get("width"), default=0)
            height = height or _int_number(probed.get("height"), default=0)

        if source and (not width or not height):
            parsed_width, parsed_height = _parse_dimensions_from_name(source.name)
            width = width or parsed_width
            height = height or parsed_height

        aspect_ratio = str(metadata.get("aspect_ratio") or _aspect_ratio(width, height))
        visual = metadata.get("visual") if isinstance(metadata.get("visual"), dict) else {}
        file_size = source.stat().st_size if source and source.exists() else 0
        frame_scores = _opencv_frame_scores(source)
        if frame_scores:
            methods["motion"] = "opencv"
            methods["scene_change"] = "opencv"
            methods["watermark"] = "opencv_corner_variance"
        else:
            methods["motion"] = (
                "metadata"
                if visual.get("motion_score") is not None or metadata.get("motion_score") is not None
                else "fallback"
            )
            methods["scene_change"] = (
                "metadata"
                if visual.get("scene_change_score") is not None or metadata.get("scene_change_score") is not None
                else "fallback"
            )
            methods["watermark"] = (
                "metadata"
                if visual.get("watermark_score") is not None or metadata.get("watermark_score") is not None
                else "fallback"
            )

        motion_score = _clamp(
            _number(
                visual.get("motion_score"),
                metadata.get("motion_score"),
                frame_scores.get("motion_score") if frame_scores else None,
                default=_file_score(file_size, 17),
            )
        )
        scene_change_score = _clamp(
            _number(
                visual.get("scene_change_score"),
                metadata.get("scene_change_score"),
                frame_scores.get("scene_change_score") if frame_scores else None,
                default=0.0,
            )
        )
        watermark_score = _clamp(
            _number(
                visual.get("watermark_score"),
                metadata.get("watermark_score"),
                frame_scores.get("watermark_score") if frame_scores else None,
                default=_file_score(file_size, 7) * 0.2,
            )
        )
        quality_score = _clamp(_number(metadata.get("quality_score"), default=0.65 if file_size else 0.5))

        result = {
            "duration": duration,
            "aspect_ratio": aspect_ratio,
            "width": width,
            "height": height,
            "motion_score": motion_score,
            "scene_change_score": scene_change_score,
            "watermark_score": watermark_score,
            "quality_score": quality_score,
            "file_size_bytes": file_size,
            "visual": {
                "motion_score": motion_score,
                "scene_change_score": scene_change_score,
                "watermark_score": watermark_score,
                "suggested_crop": _suggested_crop(aspect_ratio),
                "object_labels": list(visual.get("object_labels") or []),
                "ocr_text": str(visual.get("ocr_text") or ""),
                "analysis_methods": methods,
            },
        }
        return result


def _ffprobe(source: Path | None) -> dict[str, Any]:
    if source is None or not source.exists():
        return {}
    try:
        proc = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_streams",
                "-show_format",
                str(source),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return {}
    if proc.returncode != 0:
        return {}
    try:
        payload = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return {}

    video_stream = next(
        (
            stream
            for stream in payload.get("streams", [])
            if isinstance(stream, dict) and stream.get("codec_type") == "video"
        ),
        {},
    )
    return {
        "duration": (
            (payload.get("format") or {}).get("duration")
            if isinstance(payload.get("format"), dict)
            else None
        ),
        "width": video_stream.get("width"),
        "height": video_stream.get("height"),
    }


def _opencv_frame_scores(source: Path | None) -> dict[str, float]:
    if source is None or not source.exists():
        return {}
    try:
        import cv2  # type: ignore[import-not-found]
    except Exception:
        return {}

    capture = None
    try:
        capture = cv2.VideoCapture(str(source))
        if not capture or not capture.isOpened():
            return {}
        frame_count = int(capture.get(getattr(cv2, "CAP_PROP_FRAME_COUNT", 7)) or 0)
        if frame_count <= 0:
            sample_positions = range(0, 8)
        else:
            step = max(1, frame_count // 8)
            sample_positions = range(0, frame_count, step)

        frames: list[Any] = []
        for position in list(sample_positions)[:8]:
            capture.set(getattr(cv2, "CAP_PROP_POS_FRAMES", 1), position)
            ok, frame = capture.read()
            if not ok or frame is None:
                continue
            frames.append(_gray_frame(cv2, frame))

        if len(frames) < 2:
            return {}

        diffs: list[float] = []
        for previous, current in zip(frames, frames[1:]):
            diff = cv2.absdiff(previous, current)
            diffs.append(_frame_mean(diff) / 255.0)

        corner_variances = [_corner_variance(frame) for frame in frames]
        avg_corner_variance = mean(corner_variances) if corner_variances else 0.0
        motion_score = _clamp(mean(diffs) * 2.5 if diffs else 0.0)
        scene_change_score = _clamp(sum(1 for value in diffs if value >= 0.18) / max(1, len(diffs)))
        watermark_score = _clamp(1.0 - min(avg_corner_variance / 1800.0, 1.0))
        return {
            "motion_score": motion_score,
            "scene_change_score": scene_change_score,
            "watermark_score": watermark_score,
        }
    except Exception:
        return {}
    finally:
        if capture is not None:
            try:
                capture.release()
            except Exception:
                pass


def _gray_frame(cv2: Any, frame: Any) -> Any:
    if len(getattr(frame, "shape", ())) == 2:
        return frame
    return cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)


def _frame_mean(frame: Any) -> float:
    value = frame.mean() if hasattr(frame, "mean") else 0.0
    return float(value)


def _corner_variance(frame: Any) -> float:
    shape = getattr(frame, "shape", ())
    if len(shape) < 2:
        return 0.0
    height, width = int(shape[0]), int(shape[1])
    if height <= 0 or width <= 0:
        return 0.0
    patch_h = max(1, min(24, height // 5 or 1))
    patch_w = max(1, min(24, width // 5 or 1))
    corners = [
        frame[:patch_h, :patch_w],
        frame[:patch_h, width - patch_w : width],
        frame[height - patch_h : height, :patch_w],
        frame[height - patch_h : height, width - patch_w : width],
    ]
    variances = [float(corner.var()) for corner in corners if hasattr(corner, "var")]
    return mean(variances) if variances else 0.0


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
