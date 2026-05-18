from __future__ import annotations

import json
import shutil
from pathlib import Path

from worker.handlers.media_quality import (
    MediaQualityService,
    QualityQAConfig,
    build_loudnorm_apply_filter,
    parse_loudnorm_json,
    parse_vmaf_score,
)


def test_quality_config_parses_defaults_and_node_overrides():
    default_config = QualityQAConfig.from_node_config({})
    assert default_config.enabled is True
    assert default_config.gate_mode == "soft_repair_once"
    assert default_config.vmaf_min_score == 80

    overridden = QualityQAConfig.from_node_config(
        {
            "enable_quality_qa": "false",
            "vmaf_min_score": "92",
            "loudnorm_target_i": "-18",
            "loudnorm_target_lra": "9",
            "loudnorm_target_tp": "-2",
        }
    )
    assert overridden.enabled is False
    assert overridden.vmaf_min_score == 92
    assert overridden.loudnorm_target_i == -18
    assert overridden.loudnorm_target_lra == 9
    assert overridden.loudnorm_target_tp == -2


def test_parse_vmaf_score_reads_pooled_metrics(tmp_path: Path):
    log_path = tmp_path / "vmaf.json"
    log_path.write_text(json.dumps({"pooled_metrics": {"vmaf": {"mean": 83.42}}}))
    assert parse_vmaf_score(log_path) == 83.42


def test_parse_loudnorm_json_reads_ffmpeg_stderr_block():
    stderr = """
    ignored
    {
      "input_i" : "-19.03",
      "input_tp" : "-1.12",
      "input_lra" : "8.20",
      "input_thresh" : "-29.11",
      "target_offset" : "2.10"
    }
    """
    stats = parse_loudnorm_json(stderr)
    assert stats["input_i"] == "-19.03"
    assert stats["target_offset"] == "2.10"


def test_loudnorm_apply_filter_uses_measured_values():
    stats = {
        "input_i": "-19.03",
        "input_tp": "-1.12",
        "input_lra": "8.20",
        "input_thresh": "-29.11",
        "target_offset": "2.10",
    }
    filter_text = build_loudnorm_apply_filter(stats, target_i=-16, target_lra=11, target_tp=-1.5)
    assert "measured_I=-19.03" in filter_text
    assert "measured_LRA=8.20" in filter_text
    assert "measured_TP=-1.12" in filter_text
    assert "measured_thresh=-29.11" in filter_text
    assert "offset=2.10" in filter_text


class FakeQualityService(MediaQualityService):
    def __init__(self, *, vmaf_score=90.0, audio_stats=None, repair_fails=False):
        super().__init__()
        self.vmaf_score = vmaf_score
        self.audio_stats = audio_stats
        self.repair_fails = repair_fails
        self.repair_calls = 0

    async def measure_vmaf(self, reference_path, distorted_path):
        return self.vmaf_score

    async def measure_loudnorm(self, media_path, config):
        return self.audio_stats

    async def repair_export(self, source_path, output_path, config, loudnorm_stats):
        self.repair_calls += 1
        if self.repair_fails:
            raise RuntimeError("repair failed")
        repaired = Path(output_path).with_suffix(".repaired.mp4")
        shutil.copy2(output_path, repaired)
        return str(repaired)


async def test_qa_export_warns_only_when_disabled(tmp_path: Path):
    source = tmp_path / "source.mp4"
    output = tmp_path / "output.mp4"
    source.write_bytes(b"source")
    output.write_bytes(b"output")
    result = await FakeQualityService(vmaf_score=10).qa_export(
        source_path=str(source),
        output_path=str(output),
        node_config={"enable_quality_qa": False},
    )
    assert result.repaired_path is None
    assert result.report["enabled"] is False


async def test_qa_export_repairs_once_when_vmaf_is_below_threshold(tmp_path: Path):
    source = tmp_path / "source.mp4"
    output = tmp_path / "output.mp4"
    source.write_bytes(b"source")
    output.write_bytes(b"output")
    service = FakeQualityService(vmaf_score=70)
    result = await service.qa_export(source_path=str(source), output_path=str(output), node_config={})
    assert service.repair_calls == 1
    assert result.repaired_path is not None
    assert result.report["reencode_attempted"] is True
    assert result.report["qa_action"] == "reencoded_once"


async def test_qa_export_keeps_original_when_repair_fails(tmp_path: Path):
    source = tmp_path / "source.mp4"
    output = tmp_path / "output.mp4"
    source.write_bytes(b"source")
    output.write_bytes(b"output")
    result = await FakeQualityService(vmaf_score=70, repair_fails=True).qa_export(
        source_path=str(source),
        output_path=str(output),
        node_config={},
    )
    assert result.repaired_path is None
    assert "repair_failed" in result.report["warnings"]
