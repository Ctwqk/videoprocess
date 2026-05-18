from __future__ import annotations

import json
import math
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from worker.handlers.base import BaseHandler


@dataclass(frozen=True)
class QualityQAConfig:
    enabled: bool = True
    gate_mode: str = "soft_repair_once"
    vmaf_min_score: float = 80.0
    loudnorm_target_i: float = -16.0
    loudnorm_target_lra: float = 11.0
    loudnorm_target_tp: float = -1.5

    @classmethod
    def from_node_config(cls, node_config: dict[str, Any]) -> "QualityQAConfig":
        return cls(
            enabled=BaseHandler.parse_bool_param(node_config.get("enable_quality_qa"), True),
            gate_mode=str(node_config.get("quality_gate_mode") or "soft_repair_once"),
            vmaf_min_score=_float_param(node_config.get("vmaf_min_score"), 80.0),
            loudnorm_target_i=_float_param(node_config.get("loudnorm_target_i"), -16.0),
            loudnorm_target_lra=_float_param(node_config.get("loudnorm_target_lra"), 11.0),
            loudnorm_target_tp=_float_param(node_config.get("loudnorm_target_tp"), -1.5),
        )


def parse_vmaf_score(log_path: str | Path) -> float | None:
    try:
        payload = json.loads(Path(log_path).read_text())
    except (OSError, json.JSONDecodeError):
        return None
    pooled_vmaf = ((payload.get("pooled_metrics") or {}).get("vmaf") or {}).get("mean")
    if pooled_vmaf is None:
        return None
    try:
        return float(pooled_vmaf)
    except (TypeError, ValueError):
        return None


def parse_loudnorm_json(stderr: str) -> dict[str, str]:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", stderr):
        try:
            payload, _end = decoder.raw_decode(stderr[match.start() :])
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        required = {"input_i", "input_tp", "input_lra", "input_thresh", "target_offset"}
        if required.issubset(payload.keys()):
            return {key: str(payload[key]) for key in required}
    return {}


def build_loudnorm_apply_filter(
    stats: dict[str, str],
    *,
    target_i: float,
    target_lra: float,
    target_tp: float,
) -> str:
    return (
        f"loudnorm=I={_format_number(target_i)}:LRA={_format_number(target_lra)}:"
        f"TP={_format_number(target_tp)}:measured_I={stats['input_i']}:"
        f"measured_LRA={stats['input_lra']}:measured_TP={stats['input_tp']}:"
        f"measured_thresh={stats['input_thresh']}:offset={stats['target_offset']}:"
        "linear=true:print_format=summary"
    )


@dataclass(frozen=True)
class QualityQAResult:
    report: dict[str, Any]
    repaired_path: str | None = None


class MediaQualityService:
    def __init__(self, handler: BaseHandler | None = None) -> None:
        self.handler = handler or _MediaQualityCommandHandler()

    async def qa_export(
        self,
        *,
        source_path: str,
        output_path: str,
        node_config: dict[str, Any],
    ) -> QualityQAResult:
        config = QualityQAConfig.from_node_config(node_config)
        report = _base_report(config)
        if not config.enabled:
            report["qa_action"] = "disabled"
            return QualityQAResult(report=report)

        loudnorm_stats: dict[str, str] | None = None
        try:
            vmaf_score = await self.measure_vmaf(source_path, output_path)
        except Exception:
            vmaf_score = None
            report["warnings"].append("vmaf_unavailable")
        if vmaf_score is None:
            report["warnings"].append("vmaf_unavailable")
        else:
            report["vmaf_score"] = round(float(vmaf_score), 3)

        try:
            loudnorm_stats = await self.measure_loudnorm(output_path, config)
        except Exception:
            loudnorm_stats = None
            report["warnings"].append("loudnorm_measure_failed")
        if loudnorm_stats and _loudnorm_stats_are_finite(loudnorm_stats):
            report["audio_lufs"] = _float_param(loudnorm_stats.get("input_i"), 0.0)
            report["audio_true_peak"] = _float_param(loudnorm_stats.get("input_tp"), 0.0)
            report["audio_lra"] = _float_param(loudnorm_stats.get("input_lra"), 0.0)
        elif loudnorm_stats:
            loudnorm_stats = None
            report["warnings"].append("loudnorm_non_finite")

        needs_repair = _needs_repair(report, config)
        if not needs_repair or config.gate_mode != "soft_repair_once":
            report["qa_action"] = "passed" if not needs_repair else "warning_only"
            return QualityQAResult(report=report)

        report["reencode_attempted"] = True
        try:
            repaired_path = await self.repair_export(source_path, output_path, config, loudnorm_stats)
        except Exception:
            report["qa_action"] = "repair_failed"
            report["warnings"].append("repair_failed")
            return QualityQAResult(report=report)

        report["qa_action"] = "reencoded_once"
        return QualityQAResult(report=report, repaired_path=repaired_path)

    async def measure_vmaf(self, reference_path: str, distorted_path: str) -> float | None:
        if not reference_path or not Path(reference_path).exists():
            return None
        if not distorted_path or not Path(distorted_path).exists():
            return None
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_log:
            log_path = temp_log.name
        try:
            args = [
                "-i",
                distorted_path,
                "-i",
                reference_path,
                "-lavfi",
                f"libvmaf=log_fmt=json:log_path={log_path}",
                "-f",
                "null",
                "-",
            ]
            await self.handler.run_ffmpeg(args)
            return parse_vmaf_score(log_path)
        finally:
            try:
                Path(log_path).unlink()
            except OSError:
                pass

    async def measure_loudnorm(self, media_path: str, config: QualityQAConfig) -> dict[str, str] | None:
        args = [
            "-i",
            media_path,
            "-af",
            (
                f"loudnorm=I={_format_number(config.loudnorm_target_i)}:"
                f"LRA={_format_number(config.loudnorm_target_lra)}:"
                f"TP={_format_number(config.loudnorm_target_tp)}:print_format=json"
            ),
            "-f",
            "null",
            "-",
        ]
        stderr = await self.handler.run_ffmpeg(args)
        return parse_loudnorm_json(stderr or "") or None

    async def repair_export(
        self,
        source_path: str,
        output_path: str,
        config: QualityQAConfig,
        loudnorm_stats: dict[str, str] | None,
    ) -> str:
        repaired = tempfile.NamedTemporaryFile(delete=False, suffix=Path(output_path).suffix or ".mp4")
        repaired.close()
        args = ["-i", output_path]
        if loudnorm_stats:
            args.extend(
                [
                    "-af",
                    build_loudnorm_apply_filter(
                        loudnorm_stats,
                        target_i=config.loudnorm_target_i,
                        target_lra=config.loudnorm_target_lra,
                        target_tp=config.loudnorm_target_tp,
                    ),
                ]
            )
        args.extend(
            [
                "-map",
                "0:v:0",
                "-map",
                "0:a?",
                *self.handler.intermediate_video_encode_args("libx264"),
                "-c:a",
                "aac",
                "-ar",
                "48000",
                "-ac",
                "2",
                repaired.name,
            ]
        )
        await self.handler.run_ffmpeg(args)
        return repaired.name


class _MediaQualityCommandHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        raise NotImplementedError("MediaQualityCommandHandler is only used for ffmpeg commands")


def _float_param(value: Any, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric if math.isfinite(numeric) else default


def _loudnorm_stats_are_finite(stats: dict[str, str]) -> bool:
    required = ("input_i", "input_tp", "input_lra", "input_thresh", "target_offset")
    for key in required:
        try:
            value = float(stats[key])
        except (KeyError, TypeError, ValueError):
            return False
        if not math.isfinite(value):
            return False
    return True


def _format_number(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


def _base_report(config: QualityQAConfig) -> dict[str, Any]:
    return {
        "enabled": config.enabled,
        "gate_mode": config.gate_mode,
        "qa_action": "not_run",
        "reencode_attempted": False,
        "vmaf_score": None,
        "audio_lufs": None,
        "audio_true_peak": None,
        "audio_lra": None,
        "thresholds": {
            "vmaf_min_score": config.vmaf_min_score,
            "loudnorm_target_i": config.loudnorm_target_i,
            "loudnorm_target_lra": config.loudnorm_target_lra,
            "loudnorm_target_tp": config.loudnorm_target_tp,
        },
        "warnings": [],
    }


def _needs_repair(report: dict[str, Any], config: QualityQAConfig) -> bool:
    vmaf_score = report.get("vmaf_score")
    if vmaf_score is not None and float(vmaf_score) < config.vmaf_min_score:
        return True
    audio_lufs = report.get("audio_lufs")
    if audio_lufs is not None and abs(float(audio_lufs) - config.loudnorm_target_i) > 1.0:
        return True
    audio_true_peak = report.get("audio_true_peak")
    if audio_true_peak is not None and float(audio_true_peak) > config.loudnorm_target_tp + 0.5:
        return True
    return False
