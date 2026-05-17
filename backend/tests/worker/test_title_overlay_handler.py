from __future__ import annotations

import pytest

from worker.handlers.title_overlay import TitleOverlayHandler


@pytest.mark.asyncio
async def test_title_overlay_builds_drawtext_filter(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(TitleOverlayHandler, "run_ffmpeg", fake_run_ffmpeg)

    await TitleOverlayHandler().execute(
        {"text": "Hello", "position": "top", "start_time": 1, "duration": 2, "font_size": 64},
        {"input": "in.mp4"},
        "out.mp4",
    )

    vf = captured["args"][captured["args"].index("-vf") + 1]
    assert "drawtext" in vf
    assert "Hello" in vf
    assert "between(t,1.0,3.0)" in vf
