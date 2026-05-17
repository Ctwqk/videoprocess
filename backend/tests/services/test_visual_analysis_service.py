from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

from app.services.visual_analysis_service import VisualAnalysisService


def test_visual_analysis_uses_ffprobe_for_existing_source_metadata(tmp_path, monkeypatch):
    source_path = tmp_path / "camera_upload.mp4"
    source_path.write_bytes(b"video bytes")

    def fake_run(cmd, **kwargs):
        assert cmd[0] == "ffprobe"
        assert str(source_path) in cmd
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "streams": [{"codec_type": "video", "width": 1080, "height": 1920}],
                    "format": {"duration": "7.5"},
                }
            ),
            stderr="",
            returncode=0,
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = VisualAnalysisService().analyze(source_path=source_path)

    assert result["duration"] == 7.5
    assert result["width"] == 1080
    assert result["height"] == 1920
    assert result["aspect_ratio"] == "9:16"
    assert result["visual"]["analysis_methods"]["probe"] == "ffprobe"
    assert result["visual"]["suggested_crop"] == {"x": 0.0, "y": 0.0, "w": 1.0, "h": 1.0}


def test_visual_analysis_fallback_returns_safe_method_metadata(monkeypatch, tmp_path):
    missing_path = tmp_path / "missing.mp4"

    def missing_ffprobe(*_args, **_kwargs):
        raise FileNotFoundError("ffprobe is not installed")

    monkeypatch.setattr(subprocess, "run", missing_ffprobe)

    result = VisualAnalysisService().analyze(source_path=missing_path)

    assert result["duration"] == 0.0
    assert result["width"] == 0
    assert result["height"] == 0
    assert result["aspect_ratio"] == "auto"
    assert result["scene_change_score"] == 0.0
    assert result["visual"]["analysis_methods"]["probe"] == "fallback"
    assert result["visual"]["analysis_methods"]["motion"] == "fallback"
    assert result["visual"]["analysis_methods"]["watermark"] == "fallback"
    assert result["visual"]["object_labels"] == []
    assert result["visual"]["ocr_text"] == ""


def test_visual_analysis_derives_normalized_fields_from_metadata():
    result = VisualAnalysisService().analyze(
        metadata={
            "duration": 12.4,
            "width": 1920,
            "height": 1080,
            "visual": {"motion_score": 0.72, "watermark_score": 0.18},
            "quality_score": 0.91,
        }
    )

    assert result["duration"] == 12.4
    assert result["aspect_ratio"] == "16:9"
    assert result["motion_score"] == 0.72
    assert result["watermark_score"] == 0.18
    assert result["quality_score"] == 0.91
    assert result["visual"]["motion_score"] == 0.72


def test_visual_analysis_uses_cheap_local_file_probe_when_metadata_is_sparse(tmp_path):
    source_path = tmp_path / "sample_1080x1920.mp4"
    source_path.write_bytes(b"not a real video but enough for a file probe")

    result = VisualAnalysisService().analyze(source_path=source_path, metadata={"duration_sec": 6})

    assert result["duration"] == 6
    assert result["aspect_ratio"] == "9:16"
    assert result["file_size_bytes"] == source_path.stat().st_size
    assert 0 <= result["motion_score"] <= 1
    assert 0 <= result["watermark_score"] <= 1
