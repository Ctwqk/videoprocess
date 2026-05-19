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
    assert "anullsrc=channel_layout=stereo:sample_rate=48000:duration=9" in args
    assert "concat=n=3:v=1:a=0[v]" in ";".join(args)
    assert "-t" in args
    assert args[args.index("-t") + 1] == "9"
    assert "out.mp4" == args[-1]


@pytest.mark.asyncio
async def test_concat_many_uses_finite_silence_for_target_duration(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {"target_duration": 9},
        {"video_1": "a.mp4", "video_2": "b.mp4", "video_3": "c.mp4"},
        "out.mp4",
    )

    assert "anullsrc=channel_layout=stereo:sample_rate=48000:duration=9" in captured["args"]


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
async def test_concat_many_uses_aspect_ratio_for_default_dimensions(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {"aspect_ratio": "16:9"},
        {"video_1": "a.mp4", "video_2": "b.mp4"},
        "out.mp4",
    )

    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "scale=1920:1080" in filter_complex
    assert "pad=1920:1080" in filter_complex


@pytest.mark.asyncio
async def test_concat_many_explicit_dimensions_override_aspect_ratio(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {"aspect_ratio": "16:9", "width": 720, "height": 1280},
        {"video_1": "a.mp4", "video_2": "b.mp4"},
        "out.mp4",
    )

    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "scale=720:1280" in filter_complex
    assert "pad=720:1280" in filter_complex


@pytest.mark.asyncio
async def test_concat_many_auto_aspect_uses_first_input_metadata(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {
            "aspect_ratio": "auto",
            "_input_artifact_meta": {
                "video_1": {"width": 1280, "height": 720},
                "video_2": {"width": 720, "height": 1280},
            },
        },
        {"video_2": "b.mp4", "video_1": "a.mp4"},
        "out.mp4",
    )

    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "scale=1280:720" in filter_complex
    assert "pad=1280:720" in filter_complex


@pytest.mark.asyncio
async def test_concat_many_auto_aspect_uses_dominant_input_orientation(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {
            "aspect_ratio": "auto",
            "_input_artifact_meta": {
                "video_1": {"width": 1280, "height": 720},
                "video_2": {"width": 720, "height": 1280},
                "video_3": {"width": 720, "height": 1280},
            },
        },
        {"video_1": "a.mp4", "video_2": "b.mp4", "video_3": "c.mp4"},
        "out.mp4",
    )

    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "scale=720:1280" in filter_complex
    assert "pad=720:1280" in filter_complex


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
