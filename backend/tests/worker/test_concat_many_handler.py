from __future__ import annotations

import pytest

from worker.handlers.concat_many import ConcatManyHandler


@pytest.mark.asyncio
async def test_concat_many_builds_concat_filter(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {"input_count": 3, "width": 720, "height": 1280},
        {"video_1": "a.mp4", "video_2": "b.mp4", "video_3": "c.mp4"},
        "out.mp4",
    )

    args = captured["args"]
    assert args.count("-i") == 3
    assert "concat=n=3:v=1:a=0[v]" in ";".join(args)
    assert "out.mp4" == args[-1]
