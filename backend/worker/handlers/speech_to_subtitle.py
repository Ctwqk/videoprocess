from __future__ import annotations

import asyncio
import logging
import os

from worker.handlers.base import BaseHandler
from worker.handlers.subtitle_utils import SubtitleCue, write_srt

logger = logging.getLogger(__name__)


class SpeechToSubtitleHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        media_path = input_paths["media"]
        model_name = str(node_config.get("model", "small") or "small")
        language = str(node_config.get("language", "") or "").strip() or None
        beam_size = int(node_config.get("beam_size", 5) or 5)
        merge_adjacent = self.parse_bool_param(node_config.get("merge_adjacent"), True)
        merge_max_gap_seconds = float(node_config.get("merge_max_gap_seconds", 0.6) or 0.6)
        merge_min_chars = int(node_config.get("merge_min_chars", 40) or 40)
        merge_min_duration_seconds = float(node_config.get("merge_min_duration_seconds", 2.2) or 2.2)
        merge_max_duration_seconds = float(node_config.get("merge_max_duration_seconds", 8.0) or 8.0)
        device = str(
            node_config.get("device")
            or os.environ.get("VIDEO_WHISPER_DEVICE")
            or ("cuda" if self.gpu_enabled() else "cpu")
        ).strip()
        compute_type = str(
            node_config.get("compute_type")
            or os.environ.get("VIDEO_WHISPER_COMPUTE_TYPE")
            or ("float16" if device == "cuda" else "int8")
        ).strip()

        try:
            result = await asyncio.to_thread(
                self._transcribe_to_srt,
                media_path,
                output_path,
                model_name,
                language,
                beam_size,
                device,
                compute_type,
                merge_adjacent,
                merge_max_gap_seconds,
                merge_min_chars,
                merge_min_duration_seconds,
                merge_max_duration_seconds,
            )
        except RuntimeError as exc:
            if not self._should_fallback_to_cpu(device, exc):
                raise
            logger.warning(
                "speech_to_subtitle GPU execution failed (%s); retrying on CPU",
                exc,
            )
            result = await asyncio.to_thread(
                self._transcribe_to_srt,
                media_path,
                output_path,
                model_name,
                language,
                beam_size,
                "cpu",
                "int8",
                merge_adjacent,
                merge_max_gap_seconds,
                merge_min_chars,
                merge_min_duration_seconds,
                merge_max_duration_seconds,
            )
            result["asr_fallback"] = "cpu"
        return result

    @staticmethod
    def _should_fallback_to_cpu(device: str, exc: RuntimeError) -> bool:
        if device != "cuda":
            return False
        message = str(exc).lower()
        return any(fragment in message for fragment in (
            "libcublas",
            "libcudnn",
            "cuda",
            "cudnn",
            "no cuda-capable device",
            "no compatible gpu",
            "failed to create cublas handle",
        ))

    def _transcribe_to_srt(
        self,
        media_path: str,
        output_path: str,
        model_name: str,
        language: str | None,
        beam_size: int,
        device: str,
        compute_type: str,
        merge_adjacent: bool,
        merge_max_gap_seconds: float,
        merge_min_chars: int,
        merge_min_duration_seconds: float,
        merge_max_duration_seconds: float,
    ) -> dict:
        try:
            from faster_whisper import WhisperModel
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "speech_to_subtitle requires the 'faster-whisper' package to be installed on this worker"
            ) from exc

        model = WhisperModel(model_name, device=device, compute_type=compute_type)
        segments, info = model.transcribe(
            media_path,
            language=language,
            beam_size=beam_size,
        )

        cues: list[SubtitleCue] = []
        for index, segment in enumerate(segments, start=1):
            text = segment.text.strip()
            if not text:
                continue
            cues.append(
                SubtitleCue(
                    index=index,
                    start_seconds=float(segment.start),
                    end_seconds=float(segment.end),
                    text=text,
                )
            )

        if not cues:
            raise RuntimeError("Speech recognition produced no subtitle segments")

        original_segments = len(cues)
        cues = self._merge_adjacent_cues(
            cues,
            enabled=merge_adjacent,
            max_gap_seconds=merge_max_gap_seconds,
            min_chars=merge_min_chars,
            min_duration_seconds=merge_min_duration_seconds,
            max_duration_seconds=merge_max_duration_seconds,
        )

        write_srt(cues, output_path)
        return {
            "subtitle_language": getattr(info, "language", language),
            "subtitle_segments": len(cues),
            "subtitle_segments_original": original_segments,
            "subtitle_segments_merged": max(0, original_segments - len(cues)),
            "asr_model": model_name,
            "asr_device": device,
            "subtitle_merge_adjacent": merge_adjacent,
        }

    def _merge_adjacent_cues(
        self,
        cues: list[SubtitleCue],
        *,
        enabled: bool,
        max_gap_seconds: float,
        min_chars: int,
        min_duration_seconds: float,
        max_duration_seconds: float,
    ) -> list[SubtitleCue]:
        if not enabled or len(cues) < 2:
            return cues

        merged: list[SubtitleCue] = []
        current = cues[0]
        for next_cue in cues[1:]:
            if self._should_merge_pair(
                current,
                next_cue,
                max_gap_seconds=max_gap_seconds,
                min_chars=min_chars,
                min_duration_seconds=min_duration_seconds,
                max_duration_seconds=max_duration_seconds,
            ):
                current = SubtitleCue(
                    index=current.index,
                    start_seconds=current.start_seconds,
                    end_seconds=next_cue.end_seconds,
                    text=self._join_cue_text(current.text, next_cue.text),
                )
            else:
                merged.append(current)
                current = next_cue
        merged.append(current)

        return [
            SubtitleCue(
                index=index,
                start_seconds=cue.start_seconds,
                end_seconds=cue.end_seconds,
                text=cue.text,
            )
            for index, cue in enumerate(merged, start=1)
        ]

    def _should_merge_pair(
        self,
        current: SubtitleCue,
        next_cue: SubtitleCue,
        *,
        max_gap_seconds: float,
        min_chars: int,
        min_duration_seconds: float,
        max_duration_seconds: float,
    ) -> bool:
        gap_seconds = max(0.0, next_cue.start_seconds - current.end_seconds)
        if gap_seconds > max_gap_seconds:
            return False

        merged_duration = next_cue.end_seconds - current.start_seconds
        if merged_duration > max_duration_seconds:
            return False

        current_duration = current.end_seconds - current.start_seconds
        current_text = current.text.strip()
        next_text = next_cue.text.strip()
        merged_chars = len((current_text + " " + next_text).strip())

        current_is_short = len(current_text) < min_chars or current_duration < min_duration_seconds
        next_is_short = len(next_text) < min_chars

        return current_is_short or next_is_short or merged_chars <= min_chars * 2

    @staticmethod
    def _join_cue_text(left: str, right: str) -> str:
        left = left.strip()
        right = right.strip()
        if not left:
            return right
        if not right:
            return left
        if left.endswith(("\n", "-", "—", "–", "/", "(", "[")):
            return f"{left}{right}"
        return f"{left}\n{right}"
