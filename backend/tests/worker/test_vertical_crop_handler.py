from __future__ import annotations

import pytest

from worker.handlers.vertical_crop import VerticalCropHandler


@pytest.mark.asyncio
async def test_vertical_crop_builds_center_crop_filter(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(VerticalCropHandler, "run_ffmpeg", fake_run_ffmpeg)

    await VerticalCropHandler().execute(
        {"mode": "center_crop", "width": 1080, "height": 1920},
        {"input": "in.mp4"},
        "out.mp4",
    )

    args = captured["args"]
    assert "-vf" in args
    assert "crop=1080:1920" in args[args.index("-vf") + 1]
    assert args[-1] == "out.mp4"
