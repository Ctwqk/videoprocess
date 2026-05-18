from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from app.node_registry.builtin import smart_trim as smart_trim_node
from app.node_registry.builtin import speech_to_subtitle as speech_to_subtitle_node
from app.node_registry.builtin import subtitle_to_speech as subtitle_to_speech_node
from worker.handlers.base import BaseHandler
from worker.handlers.bgm import BgmHandler
from worker.handlers.concat_many import ConcatManyHandler
from worker.handlers.concat_vertical_timeline import ConcatVerticalTimelineHandler
from worker.handlers.smart_trim import SmartTrimConfig, SmartTrimHandler
from worker.handlers.speech_to_subtitle import SpeechToSubtitleHandler
from worker.handlers.subtitle import SubtitleHandler
from worker.handlers.transcode import TranscodeHandler


class DummyHandler(BaseHandler):
    async def execute(self, node_config, input_paths, output_path):
        return None


def _value_after(args: list[str], option: str) -> str:
    return args[args.index(option) + 1]


def _param_default(definition, name: str):
    return next(param.default for param in definition.params if param.name == name)


def test_base_video_encode_profiles_include_quality_and_mp4_compatibility(monkeypatch):
    monkeypatch.delenv("VIDEO_USE_GPU", raising=False)
    monkeypatch.delenv("VIDEO_USE_VIDEOTOOLBOX", raising=False)
    handler = DummyHandler()

    final_args = handler.final_video_encode_args("libx264")
    intermediate_args = handler.intermediate_video_encode_args("libx264")

    assert _value_after(final_args, "-preset") == "medium"
    assert _value_after(final_args, "-crf") == "20"
    assert _value_after(intermediate_args, "-preset") == "slow"
    assert _value_after(intermediate_args, "-crf") == "18"
    for args in (final_args, intermediate_args):
        assert ["-pix_fmt", "yuv420p"] == args[args.index("-pix_fmt") : args.index("-pix_fmt") + 2]
        assert ["-movflags", "+faststart"] == args[args.index("-movflags") : args.index("-movflags") + 2]
        assert ["-color_primaries", "bt709"] == args[
            args.index("-color_primaries") : args.index("-color_primaries") + 2
        ]
        assert ["-color_trc", "bt709"] == args[args.index("-color_trc") : args.index("-color_trc") + 2]
        assert ["-colorspace", "bt709"] == args[args.index("-colorspace") : args.index("-colorspace") + 2]


def test_nvenc_cq_fallback_maps_to_better_cpu_crf():
    handler = DummyHandler()

    rewritten = handler._rewrite_hardware_args_for_cpu(
        ["-c:v", "h264_nvenc", "-rc:v", "vbr", "-cq:v", "23", "-preset", "fast"]
    )

    assert rewritten[:4] == ["-c:v", "libx264", "-crf", "21"]


@pytest.mark.asyncio
async def test_transcode_defaults_to_final_quality_and_lanczos_scale(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(TranscodeHandler, "run_ffmpeg", fake_run_ffmpeg)

    await TranscodeHandler().execute(
        {"video_codec": "libx264", "resolution": "1280x720"},
        {"input": "in.mp4"},
        "out.mp4",
    )

    args = captured["args"]
    assert _value_after(args, "-preset") == "medium"
    assert _value_after(args, "-crf") == "20"
    assert _value_after(args, "-vf") == "scale=1280:720:flags=lanczos"
    assert "-pix_fmt" in args
    assert "-movflags" in args


@pytest.mark.asyncio
async def test_concat_many_uses_intermediate_quality_and_lanczos(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatManyHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatManyHandler().execute(
        {"width": 720, "height": 1280},
        {"video_1": "a.mp4", "video_2": "b.mp4"},
        "out.mp4",
    )

    args = captured["args"]
    filter_complex = _value_after(args, "-filter_complex")
    assert "scale=720:1280:force_original_aspect_ratio=decrease:flags=lanczos" in filter_complex
    assert _value_after(args, "-preset") == "slow"
    assert _value_after(args, "-crf") == "18"


@pytest.mark.asyncio
async def test_concat_vertical_timeline_uses_48k_silent_audio(monkeypatch):
    captured = {}

    async def fake_run_ffprobe(self, _path):
        return {"format": {"duration": "2.0"}, "streams": [{"codec_type": "video"}]}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(ConcatVerticalTimelineHandler, "run_ffprobe", fake_run_ffprobe)
    monkeypatch.setattr(ConcatVerticalTimelineHandler, "run_ffmpeg", fake_run_ffmpeg)

    await ConcatVerticalTimelineHandler()._render_segment(
        active_video="active.mp4",
        static_image="still.png",
        active_position="top",
        pane_width=720,
        pane_height=640,
        background_color="black",
    )

    args = captured["args"]
    assert "anullsrc=r=48000:cl=stereo" in args


@pytest.mark.asyncio
async def test_bgm_uses_sidechain_ducking_loudnorm_and_48k_stereo(monkeypatch):
    captured = {}

    async def fake_run_ffprobe(self, _path):
        return {"format": {"duration": "8.0"}, "streams": [{"codec_type": "audio"}]}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(BgmHandler, "run_ffprobe", fake_run_ffprobe)
    monkeypatch.setattr(BgmHandler, "run_ffmpeg", fake_run_ffmpeg)

    await BgmHandler().execute(
        {"volume": 0.3, "original_volume": 1.0, "loop": True},
        {"video": "video.mp4", "audio": "bgm.wav"},
        "out.mp4",
    )

    args = captured["args"]
    filter_complex = _value_after(args, "-filter_complex")
    assert "aresample=48000:async=1" in filter_complex
    assert "sidechaincompress=threshold=0.03:ratio=8:attack=200:release=800" in filter_complex
    assert "loudnorm=I=-16:LRA=11:TP=-1.5" in filter_complex
    assert ["-c:a", "aac"] == args[args.index("-c:a") : args.index("-c:a") + 2]
    assert ["-ar", "48000"] == args[args.index("-ar") : args.index("-ar") + 2]
    assert ["-ac", "2"] == args[args.index("-ac") : args.index("-ac") + 2]


@pytest.mark.asyncio
async def test_subtitle_uses_configured_ass_colors_and_adaptive_style(monkeypatch):
    captured = {}

    async def fake_run_ffprobe(self, _path):
        return {"streams": [{"codec_type": "video", "height": 1080}]}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(SubtitleHandler, "run_ffprobe", fake_run_ffprobe)
    monkeypatch.setattr(SubtitleHandler, "run_ffmpeg", fake_run_ffmpeg)

    await SubtitleHandler().execute(
        {"font_size": 24, "font_color": "#336699", "outline_color": "yellow", "position": "top"},
        {"video": "video.mp4", "subtitle_file": "subs.srt"},
        "out.mp4",
    )

    vf = _value_after(captured["args"], "-vf")
    assert "FontName=PingFang SC" in vf
    assert "FontSize=36" in vf
    assert "PrimaryColour=&H00996633" in vf
    assert "OutlineColour=&H0000FFFF" in vf
    assert "BorderStyle=1" in vf
    assert "Outline=2" in vf
    assert "Shadow=1" in vf
    assert "MarginV=54" in vf
    assert "Alignment=8" in vf


@pytest.mark.asyncio
async def test_speech_to_subtitle_default_model_is_medium(monkeypatch, tmp_path):
    captured = {}

    def fake_transcribe(
        self,
        media_path,
        output_path,
        model_name,
        language,
        beam_size,
        device,
        compute_type,
        merge_adjacent,
        merge_max_gap_seconds,
        merge_min_chars,
        merge_min_duration_seconds,
        merge_max_duration_seconds,
    ):
        captured["model_name"] = model_name
        return {"subtitle_segments": 1}

    monkeypatch.setattr(SpeechToSubtitleHandler, "_transcribe_to_srt", fake_transcribe)

    await SpeechToSubtitleHandler().execute({}, {"media": "video.mp4"}, str(tmp_path / "out.srt"))

    assert captured["model_name"] == "medium"
    assert _param_default(speech_to_subtitle_node.DEFINITION, "model") == "medium"


def test_speech_to_subtitle_transcribe_uses_vad_word_timestamps_and_no_conditioning(monkeypatch, tmp_path):
    captured = {}

    class FakeWhisperModel:
        def __init__(self, model_name, *, device, compute_type):
            captured["model"] = (model_name, device, compute_type)

        def transcribe(self, media_path, **kwargs):
            captured["transcribe_kwargs"] = kwargs
            return [SimpleNamespace(start=0.0, end=1.0, text=" hello ")], SimpleNamespace(language="en")

    monkeypatch.setitem(sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=FakeWhisperModel))

    SpeechToSubtitleHandler()._transcribe_to_srt(
        "video.mp4",
        str(tmp_path / "out.srt"),
        "medium",
        "en",
        5,
        "cpu",
        "int8",
        False,
        0.6,
        40,
        2.2,
        8.0,
    )

    kwargs = captured["transcribe_kwargs"]
    assert kwargs["vad_filter"] is True
    assert kwargs["vad_parameters"] == {"min_silence_duration_ms": 500}
    assert kwargs["word_timestamps"] is True
    assert kwargs["condition_on_previous_text"] is False


def test_smart_trim_whisper_model_defaults_to_medium():
    config = SmartTrimConfig.from_node_config({"prompt": "小猫"})

    assert config.whisper_model == "medium"
    assert _param_default(smart_trim_node.DEFINITION, "whisper_model") == "medium"


def test_smart_trim_transcribe_uses_configured_model_and_vad_options(monkeypatch):
    captured = {}

    class FakeWhisperModel:
        def __init__(self, model_name, *, device, compute_type):
            captured["model"] = model_name

        def transcribe(self, media_path, **kwargs):
            captured["transcribe_kwargs"] = kwargs
            return [SimpleNamespace(start=0.0, end=1.0, text=" 小猫 ")], SimpleNamespace(language="zh")

    monkeypatch.setitem(sys.modules, "faster_whisper", SimpleNamespace(WhisperModel=FakeWhisperModel))
    config = SmartTrimConfig.from_node_config({"prompt": "小猫", "whisper_model": "large-v3", "language": "zh"})

    cues = SmartTrimHandler()._transcribe("video.mp4", config)

    assert captured["model"] == "large-v3"
    kwargs = captured["transcribe_kwargs"]
    assert kwargs["vad_filter"] is True
    assert kwargs["vad_parameters"] == {"min_silence_duration_ms": 500}
    assert kwargs["word_timestamps"] is True
    assert kwargs["condition_on_previous_text"] is False
    assert cues[0].text == "小猫"


def test_subtitle_to_speech_default_speedup_is_quality_safe():
    assert _param_default(subtitle_to_speech_node.DEFINITION, "alignment_max_speedup") == 1.10
    assert _param_default(subtitle_to_speech_node.DEFINITION, "alignment_rewrite_with_llm") is True
    assert _param_default(subtitle_to_speech_node.DEFINITION, "alignment_rewrite_min_speedup") == 1.10
