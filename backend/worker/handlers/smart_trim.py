from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import httpx

from app.config import settings
from worker.handlers.base import BaseHandler
from worker.handlers.subtitle_utils import SubtitleCue


@dataclass(frozen=True)
class ScoredWindow:
    start: float
    end: float
    score: float
    visual_score: float = 0.0
    subtitle_score: float = 0.0
    keyword_score: float = 0.0

    def model_dump(self) -> dict[str, float]:
        return {
            "start": self.start,
            "end": self.end,
            "score": self.score,
            "visual_score": self.visual_score,
            "subtitle_score": self.subtitle_score,
            "keyword_score": self.keyword_score,
        }


@dataclass(frozen=True)
class SmartTrimConfig:
    prompt: str
    negative_prompt: str = ""
    mode: str = "auto"
    target_duration: float = 0.0
    min_clip_duration: float = 1.5
    max_clip_duration: float = 8.0
    max_clips: int = 8
    sample_fps: float = 1.0
    match_threshold: float = 0.35
    return_full_threshold: float = 0.65
    padding_before: float = 0.5
    padding_after: float = 0.5
    merge_gap: float = 1.0
    use_visual: bool = True
    use_asr: bool = True
    use_vlm_verify: bool = False
    language: str = "zh"
    whisper_model: str = "medium"
    output_format: str = "mp4"
    no_match_policy: str = "placeholder"

    @classmethod
    def from_node_config(cls, node_config: dict[str, Any]) -> SmartTrimConfig:
        return cls(
            prompt=str(node_config.get("prompt") or "").strip(),
            negative_prompt=str(node_config.get("negative_prompt") or "").strip(),
            mode=_select_value(node_config.get("mode"), "auto", {"auto", "best_clip", "all_matches_montage", "full_if_match", "no_full_video"}),
            target_duration=_float_param(node_config.get("target_duration"), 0.0),
            min_clip_duration=_float_param(node_config.get("min_clip_duration"), 1.5),
            max_clip_duration=_float_param(node_config.get("max_clip_duration"), 8.0),
            max_clips=max(1, int(_float_param(node_config.get("max_clips"), 8))),
            sample_fps=max(0.1, _float_param(node_config.get("sample_fps"), 1.0)),
            match_threshold=_clamp(_float_param(node_config.get("match_threshold"), 0.35)),
            return_full_threshold=_clamp(_float_param(node_config.get("return_full_threshold"), 0.65)),
            padding_before=max(0.0, _float_param(node_config.get("padding_before"), 0.5)),
            padding_after=max(0.0, _float_param(node_config.get("padding_after"), 0.5)),
            merge_gap=max(0.0, _float_param(node_config.get("merge_gap"), 1.0)),
            use_visual=BaseHandler.parse_bool_param(node_config.get("use_visual"), True),
            use_asr=BaseHandler.parse_bool_param(node_config.get("use_asr"), True),
            use_vlm_verify=BaseHandler.parse_bool_param(node_config.get("use_vlm_verify"), False),
            language=str(node_config.get("language") or "zh"),
            whisper_model=_select_value(
                node_config.get("whisper_model"),
                "medium",
                {"tiny", "base", "small", "medium", "large-v3"},
            ),
            output_format=_select_value(node_config.get("output_format"), "mp4", {"mp4", "mkv", "webm"}),
            no_match_policy=_select_value(node_config.get("no_match_policy"), "placeholder", {"placeholder", "fail"}),
        )


@dataclass(frozen=True)
class SmartTrimSelection:
    decision: str
    coverage_ratio: float
    segments: list[ScoredWindow] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    query_expansion: list[str] = field(default_factory=list)


class SmartTrimHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        config = SmartTrimConfig.from_node_config(node_config)
        if not config.prompt:
            raise ValueError("smart_trim requires a prompt")

        video_path = input_paths["input"]
        probe = await self.run_ffprobe(video_path)
        duration = _duration_from_probe(probe)
        has_audio = _has_audio_stream(probe)
        if duration <= 0:
            raise ValueError("smart_trim input video has no detectable duration")

        warnings: list[str] = []
        windows: list[ScoredWindow] = []

        if config.use_visual:
            visual_windows, visual_warnings = await self._visual_windows(video_path, duration, config)
            windows.extend(visual_windows)
            warnings.extend(visual_warnings)

        if config.use_asr:
            subtitle_windows, subtitle_warnings = await self._subtitle_windows(video_path, config)
            windows.extend(subtitle_windows)
            warnings.extend(subtitle_warnings)

        selected = select_smart_trim_segments(windows, duration=duration, config=config, warnings=warnings)

        if selected.decision == "no_match":
            if config.no_match_policy == "fail":
                raise RuntimeError("smart_trim found no matching video segment")
            await self.run_ffmpeg(self.build_no_match_placeholder_args(output_path))
        elif selected.decision == "return_full_video":
            try:
                await self.run_ffmpeg(self.build_full_video_args(video_path, output_path))
            except RuntimeError:
                await self.run_ffmpeg(self.build_full_video_reencode_args(video_path, output_path))
        elif selected.decision == "best_clip":
            await self.run_ffmpeg(self.build_single_clip_args(video_path, output_path, selected.segments[0]))
        else:
            await self.run_ffmpeg(
                self.build_cut_and_concat_args(video_path, output_path, selected.segments, has_audio=has_audio)
            )

        return {
            "smart_trim_prompt": config.prompt,
            "smart_trim_negative_prompt": config.negative_prompt,
            "decision": selected.decision,
            "coverage_ratio": selected.coverage_ratio,
            "matched_windows": [segment.model_dump() for segment in selected.segments],
            "query_expansion": selected.query_expansion,
            "video_duration": duration,
            "warnings": selected.warnings,
        }

    def build_no_match_placeholder_args(self, output_path: str) -> list[str]:
        return [
            "-f",
            "lavfi",
            "-i",
            "color=c=black:s=1280x720:d=1",
            "-f",
            "lavfi",
            "-i",
            "anullsrc=channel_layout=stereo:sample_rate=48000",
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            *self.build_video_encode_args("libx264", preset="medium", crf=28),
            "-c:a",
            "aac",
            "-shortest",
            output_path,
        ]

    def build_full_video_args(self, video_path: str, output_path: str) -> list[str]:
        return ["-i", video_path, "-map", "0:v:0", "-map", "0:a?", "-c", "copy", output_path]

    def build_full_video_reencode_args(self, video_path: str, output_path: str) -> list[str]:
        return [
            "-i",
            video_path,
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            *self.intermediate_video_encode_args("libx264"),
            "-c:a",
            "aac",
            output_path,
        ]

    def build_single_clip_args(self, video_path: str, output_path: str, segment: ScoredWindow) -> list[str]:
        return [
            "-ss",
            _format_seconds(segment.start),
            "-i",
            video_path,
            "-t",
            _format_seconds(max(0.1, segment.end - segment.start)),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            *self.intermediate_video_encode_args("libx264"),
            "-c:a",
            "aac",
            output_path,
        ]

    def build_cut_and_concat_args(
        self,
        video_path: str,
        output_path: str,
        segments: list[ScoredWindow],
        *,
        has_audio: bool = True,
    ) -> list[str]:
        args: list[str] = []
        for _segment in segments:
            args.extend(["-i", video_path])

        filters: list[str] = []
        concat_inputs: list[str] = []
        for index, segment in enumerate(segments):
            start = _format_seconds(segment.start)
            end = _format_seconds(segment.end)
            filters.append(f"[{index}:v]trim=start={start}:end={end},setpts=PTS-STARTPTS[v{index}]")
            concat_inputs.append(f"[v{index}]")
            if has_audio:
                filters.append(f"[{index}:a]atrim=start={start}:end={end},asetpts=PTS-STARTPTS[a{index}]")
                concat_inputs.append(f"[a{index}]")

        if has_audio:
            filters.append("".join(concat_inputs) + f"concat=n={len(segments)}:v=1:a=1[outv][outa]")
            maps = ["-map", "[outv]", "-map", "[outa]"]
        else:
            filters.append("".join(concat_inputs) + f"concat=n={len(segments)}:v=1:a=0[outv]")
            maps = ["-map", "[outv]"]

        args.extend(["-filter_complex", ";".join(filters), *maps, *self.intermediate_video_encode_args("libx264")])
        if has_audio:
            args.extend(["-c:a", "aac"])
        args.append(output_path)
        return args

    async def _visual_windows(
        self,
        video_path: str,
        duration: float,
        config: SmartTrimConfig,
    ) -> tuple[list[ScoredWindow], list[str]]:
        endpoint = str(getattr(settings, "vision_embedding_url", "") or "").strip()
        if not endpoint:
            return [], ["visual scoring unavailable; result may be poor for visual-only queries"]

        with TemporaryDirectory(prefix="smart_trim_frames_") as temp_dir:
            frames = await self._extract_frames(video_path, duration, config.sample_fps, Path(temp_dir))
            if not frames:
                return [], ["visual scoring produced no sampled frames"]
            try:
                scores = await self._score_frames(endpoint, frames, config)
            except Exception as exc:
                return [], [f"visual scoring unavailable: {exc}"]

        windows = []
        for timestamp, score in scores:
            half_window = (1.0 / config.sample_fps) / 2
            windows.append(
                ScoredWindow(
                    start=max(0.0, timestamp - half_window),
                    end=min(duration, timestamp + half_window),
                    score=score,
                    visual_score=score,
                )
            )
        return windows, []

    async def _extract_frames(
        self,
        video_path: str,
        duration: float,
        sample_fps: float,
        temp_dir: Path,
    ) -> list[tuple[float, Path]]:
        timestamps = []
        cursor = 0.0
        step = max(0.25, 1.0 / sample_fps)
        while cursor < duration:
            timestamps.append(cursor)
            cursor += step
        timestamps = timestamps[: max(1, min(len(timestamps), 240))]

        frames: list[tuple[float, Path]] = []
        for index, timestamp in enumerate(timestamps):
            frame_path = temp_dir / f"frame_{index:04d}.jpg"
            await self.run_ffmpeg(
                [
                    "-ss",
                    _format_seconds(timestamp),
                    "-i",
                    video_path,
                    "-frames:v",
                    "1",
                    "-q:v",
                    "3",
                    str(frame_path),
                ]
            )
            if frame_path.exists():
                frames.append((timestamp, frame_path))
        return frames

    async def _score_frames(
        self,
        endpoint: str,
        frames: list[tuple[float, Path]],
        config: SmartTrimConfig,
    ) -> list[tuple[float, float]]:
        import base64

        images_base64 = [base64.b64encode(path.read_bytes()).decode("ascii") for _timestamp, path in frames]
        texts = [config.prompt]
        if config.negative_prompt:
            texts.append(config.negative_prompt)

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{endpoint.rstrip('/')}/embed-image-text",
                json={"texts": texts, "images_base64": images_base64},
            )
            response.raise_for_status()
            payload = response.json()

        scores: list[tuple[float, float]] = []
        for (timestamp, _path), item in zip(frames, payload.get("similarities") or []):
            if not item:
                continue
            positive = float(item[0])
            negative = float(item[1]) if len(item) > 1 else 0.0
            scores.append((timestamp, _clamp(positive - max(0.0, negative) * 0.4)))
        return scores

    async def _subtitle_windows(
        self,
        video_path: str,
        config: SmartTrimConfig,
    ) -> tuple[list[ScoredWindow], list[str]]:
        try:
            cues = await asyncio.to_thread(self._transcribe, video_path, config)
        except Exception as exc:
            return [], [f"ASR scoring unavailable: {exc}"]

        windows: list[ScoredWindow] = []
        for cue in cues:
            score = _text_score(config.prompt, cue.text, config.negative_prompt)
            if score > 0:
                windows.append(
                    ScoredWindow(
                        start=cue.start_seconds,
                        end=cue.end_seconds,
                        score=score,
                        subtitle_score=score,
                        keyword_score=score,
                    )
                )
        return windows, []

    def _transcribe(self, video_path: str, config: SmartTrimConfig) -> list[SubtitleCue]:
        try:
            from faster_whisper import WhisperModel
        except ModuleNotFoundError as exc:
            raise RuntimeError("faster-whisper is not installed on this worker") from exc

        device = "cuda" if self.gpu_enabled() else "cpu"
        compute_type = "float16" if device == "cuda" else "int8"
        model = WhisperModel(config.whisper_model, device=device, compute_type=compute_type)
        segments, _info = model.transcribe(
            video_path,
            language=config.language or None,
            beam_size=5,
            vad_filter=True,
            vad_parameters={"min_silence_duration_ms": 500},
            word_timestamps=True,
            condition_on_previous_text=False,
        )
        cues: list[SubtitleCue] = []
        for index, segment in enumerate(segments, start=1):
            text = segment.text.strip()
            if text:
                cues.append(SubtitleCue(index=index, start_seconds=float(segment.start), end_seconds=float(segment.end), text=text))
        return cues


def select_smart_trim_segments(
    windows: list[ScoredWindow],
    *,
    duration: float,
    config: SmartTrimConfig,
    warnings: list[str] | None = None,
) -> SmartTrimSelection:
    warnings = list(warnings or [])
    matched = sorted((window for window in windows if window.score >= config.match_threshold), key=lambda item: item.start)
    if not matched or duration <= 0:
        return SmartTrimSelection(decision="no_match", coverage_ratio=0.0, warnings=warnings, query_expansion=[config.prompt])

    raw_merged = _merge_windows(matched, merge_gap=config.merge_gap)
    padded = [
        ScoredWindow(
            start=max(0.0, window.start - config.padding_before),
            end=min(duration, window.end + config.padding_after),
            score=window.score,
            visual_score=window.visual_score,
            subtitle_score=window.subtitle_score,
            keyword_score=window.keyword_score,
        )
        for window in matched
    ]
    merged = _merge_windows(padded, merge_gap=config.merge_gap)
    coverage_ratio = _coverage_ratio(raw_merged, duration)

    if (
        config.mode in {"auto", "full_if_match"}
        and config.target_duration <= 0
        and coverage_ratio >= config.return_full_threshold
    ):
        return SmartTrimSelection(
            decision="return_full_video",
            coverage_ratio=coverage_ratio,
            segments=[ScoredWindow(start=0, end=duration, score=max(window.score for window in merged))],
            warnings=warnings,
            query_expansion=[config.prompt],
        )

    if config.mode == "all_matches_montage":
        return SmartTrimSelection("all_matches_montage", coverage_ratio, _fit_segments(merged, config=config), warnings, [config.prompt])

    if config.mode == "no_full_video" and len(merged) > 1 and config.target_duration <= 0:
        return SmartTrimSelection("all_matches_montage", coverage_ratio, _fit_segments(merged, config=config), warnings, [config.prompt])

    if config.mode == "auto" and config.target_duration <= 0 and len(merged) > 1:
        return SmartTrimSelection("all_matches_montage", coverage_ratio, _fit_segments(merged, config=config), warnings, [config.prompt])

    best = max(merged, key=lambda item: (item.score, item.end - item.start))
    return SmartTrimSelection("best_clip", coverage_ratio, [_clip_segment(best, duration=duration, config=config)], warnings, [config.prompt])


def _merge_windows(windows: list[ScoredWindow], *, merge_gap: float) -> list[ScoredWindow]:
    if not windows:
        return []
    merged = [windows[0]]
    for window in windows[1:]:
        current = merged[-1]
        if window.start - current.end <= merge_gap:
            total = max(0.001, (current.end - current.start) + (window.end - window.start))
            score = ((current.score * (current.end - current.start)) + (window.score * (window.end - window.start))) / total
            merged[-1] = ScoredWindow(
                start=current.start,
                end=max(current.end, window.end),
                score=score,
                visual_score=max(current.visual_score, window.visual_score),
                subtitle_score=max(current.subtitle_score, window.subtitle_score),
                keyword_score=max(current.keyword_score, window.keyword_score),
            )
        else:
            merged.append(window)
    return merged


def _fit_segments(windows: list[ScoredWindow], *, config: SmartTrimConfig) -> list[ScoredWindow]:
    selected = sorted(windows, key=lambda item: item.score, reverse=True)[: config.max_clips]
    selected.sort(key=lambda item: item.start)
    if config.target_duration <= 0:
        return [_clip_segment(window, duration=math.inf, config=config) for window in selected]

    result: list[ScoredWindow] = []
    remaining = config.target_duration
    for window in selected:
        if remaining <= 0:
            break
        clipped = _clip_segment(window, duration=math.inf, config=config, max_duration=remaining)
        result.append(clipped)
        remaining -= max(0.0, clipped.end - clipped.start)
    return result


def _clip_segment(
    window: ScoredWindow,
    *,
    duration: float,
    config: SmartTrimConfig,
    max_duration: float | None = None,
) -> ScoredWindow:
    start = max(0.0, window.start)
    end = min(duration, window.end) if duration != math.inf else window.end
    limit = max_duration if max_duration is not None else (config.target_duration if config.target_duration > 0 else config.max_clip_duration)
    limit = max(config.min_clip_duration, min(limit, config.max_clip_duration))
    if end - start > limit:
        end = start + limit
    if end - start < config.min_clip_duration:
        end = min(duration, start + config.min_clip_duration) if duration != math.inf else start + config.min_clip_duration
    return ScoredWindow(start=start, end=end, score=window.score, visual_score=window.visual_score, subtitle_score=window.subtitle_score, keyword_score=window.keyword_score)


def _coverage_ratio(windows: list[ScoredWindow], duration: float) -> float:
    if duration <= 0:
        return 0.0
    covered = sum(max(0.0, window.end - window.start) for window in windows)
    return _clamp(covered / duration)


def _duration_from_probe(probe: dict[str, Any]) -> float:
    try:
        return float((probe.get("format") or {}).get("duration") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _has_audio_stream(probe: dict[str, Any]) -> bool:
    return any(stream.get("codec_type") == "audio" for stream in probe.get("streams") or [])


def _float_param(value: Any, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _select_value(value: Any, default: str, allowed: set[str]) -> str:
    candidate = str(value or default)
    return candidate if candidate in allowed else default


def _clamp(value: float) -> float:
    return max(0.0, min(1.0, value))


def _format_seconds(value: float) -> str:
    return f"{value:.3f}"


def _text_score(prompt: str, text: str, negative_prompt: str = "") -> float:
    prompt_terms = _terms(prompt)
    if not prompt_terms:
        return 0.0
    text_lower = text.lower()
    hits = sum(1 for term in prompt_terms if term in text_lower)
    if negative_prompt and any(term in text_lower for term in _terms(negative_prompt)):
        return 0.0
    return _clamp(hits / len(prompt_terms))


def _terms(value: str) -> list[str]:
    cleaned = value.lower().replace(",", " ").replace("，", " ").replace("。", " ")
    terms = [part.strip() for part in cleaned.split() if part.strip()]
    if terms:
        return terms
    return [cleaned.strip()] if cleaned.strip() else []
