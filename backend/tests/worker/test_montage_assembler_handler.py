from __future__ import annotations

import pytest

from worker.handlers.montage_assembler import MontageAssemblerHandler


@pytest.mark.asyncio
async def test_montage_assembler_builds_vertical_audio_tolerant_concat(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(MontageAssemblerHandler, "run_ffmpeg", fake_run_ffmpeg)

    await MontageAssemblerHandler().execute(
        {"style": "fast_cuts", "aspect_ratio": "9:16", "target_duration": 12, "beat_sync": True},
        {"video_1": "a.mp4", "video_2": "b.mp4", "video_3": "c.mp4"},
        "out.mp4",
    )

    args = captured["args"]
    filter_complex = args[args.index("-filter_complex") + 1]
    assert "scale=1080:1920" in filter_complex
    assert "concat=n=3:v=1:a=0[v]" in filter_complex
    assert "anullsrc=channel_layout=stereo:sample_rate=48000:duration=12" in args
    assert args[args.index("-t") + 1] == "12"
    assert args[-1] == "out.mp4"
