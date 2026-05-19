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


@pytest.mark.asyncio
async def test_vertical_crop_uses_filter_complex_for_blur_background(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(VerticalCropHandler, "run_ffmpeg", fake_run_ffmpeg)

    await VerticalCropHandler().execute(
        {"mode": "blur_bg", "width": 720, "height": 1280},
        {"input": "in.mp4"},
        "out.mp4",
    )

    args = captured["args"]
    assert "-filter_complex" in args
    assert "-vf" not in args
    filter_graph = args[args.index("-filter_complex") + 1]
    assert "[bg][fg]overlay=(W-w)/2:(H-h)/2,setsar=1" in filter_graph
    assert args[-1] == "out.mp4"
