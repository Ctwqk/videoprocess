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
        {"input_count": 3, "width": 720, "height": 1280, "target_duration": 9, "transition": "fade"},
        {"video_1": "a.mp4", "video_2": "b.mp4", "video_3": "c.mp4"},
        "out.mp4",
    )

    args = captured["args"]
    assert args.count("-i") == 4
    assert "anullsrc=channel_layout=stereo:sample_rate=48000" in args
    assert "concat=n=3:v=1:a=0[v]" in ";".join(args)
    assert "-t" in args
    assert args[args.index("-t") + 1] == "9"
    assert "out.mp4" == args[-1]


@pytest.mark.asyncio
async def test_concat_many_uses_numbered_inputs_without_explicit_input_count(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {"width": 720, "height": 1280},
        {f"video_{index}": f"{index}.mp4" for index in range(1, 7)},
        "out.mp4",
    )

    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "concat=n=6:v=1:a=0[v]" in filter_complex
    assert filter_complex.count("scale=720:1280") == 6


@pytest.mark.asyncio
async def test_concat_many_uses_all_dynamic_numbered_inputs_even_past_legacy_limit(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {"input_count": 2, "width": 720, "height": 1280},
        {f"video_{index}": f"{index}.mp4" for index in range(1, 15)},
        "out.mp4",
    )

    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "concat=n=14:v=1:a=0[v]" in filter_complex
    assert captured["args"].count("-i") == 15
