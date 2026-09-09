from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from app.config import settings
from worker.handlers.subtitle_utils import SubtitleCue
from worker.handlers.smart_trim import (
    ScoredWindow,
    SmartTrimConfig,
    SmartTrimHandler,
    select_smart_trim_segments,
)


@pytest.fixture
def handler(monkeypatch):
    handler = SmartTrimHandler()
    monkeypatch.setattr(settings, "vision_embedding_url", "")
    monkeypatch.setattr(handler, "run_ffprobe", AsyncMock(return_value={
        "format": {"duration": "10"},
        "streams": [{"codec_type": "video"}],
    }))
    monkeypatch.setattr(handler, "run_ffmpeg", AsyncMock(return_value=""))
    monkeypatch.setattr(handler, "_transcribe", Mock(return_value=[]))
    return handler


@pytest.mark.parametrize("policy_config", [
    {},
    {"no_match_policy": None},
    {"no_match_policy": ""},
    {"no_match_policy": "invalid"},
    {"no_match_policy": "fail"},
    {"no_match_policy": SmartTrimConfig(prompt="product").no_match_policy},
])
async def test_no_match_requires_explicit_placeholder_opt_in(handler, policy_config, tmp_path):
    output_path = tmp_path / "out.mp4"

    with pytest.raises(RuntimeError, match="no matching video segment"):
        await handler.execute(
            {"prompt": "product", "use_asr": False, **policy_config},
            {"input": "input.mp4"}, str(output_path),
        )

    handler.run_ffmpeg.assert_not_awaited()
    assert not output_path.exists()


async def test_no_match_error_explains_missing_visual_scoring_and_audio(handler):
    with pytest.raises(RuntimeError, match="no matching video segment") as error:
        await handler.execute(
            {"prompt": "product closeup", "no_match_policy": "fail"},
            {"input": "input.mp4"}, "out.mp4",
        )

    message = str(error.value)
    assert "product closeup" in message
    assert "visual scoring unavailable" in message
    assert "vision_embedding_url" in message
    assert "ASR skipped" in message
    assert "no audio stream" in message
    handler._transcribe.assert_not_called()
    handler.run_ffmpeg.assert_not_awaited()


async def test_explicit_preview_placeholder_preserves_no_match_metadata(handler):
    metadata = await handler.execute(
        {"prompt": "product", "no_match_policy": "placeholder", "use_asr": False},
        {"input": "input.mp4"}, "out.mp4",
    )

    assert metadata["decision"] == "no_match"
    assert metadata["coverage_ratio"] == 0
    assert metadata["matched_windows"] == []
    assert metadata["warnings"]
    handler.run_ffmpeg.assert_awaited_once()
    assert "color=c=black:s=1280x720:d=1" in handler.run_ffmpeg.call_args.args[0]


@pytest.mark.parametrize("has_audio", [False, True])
async def test_asr_only_loads_for_audio_streams(handler, has_audio):
    if has_audio:
        handler.run_ffprobe.return_value["streams"].append({"codec_type": "audio"})
    handler._transcribe.return_value = [
        SubtitleCue(index=1, start_seconds=2, end_seconds=4, text="product closeup"),
    ]

    metadata = await handler.execute(
        {"prompt": "product", "use_visual": False, "no_match_policy": "placeholder"},
        {"input": "input.mp4"}, "out.mp4",
    )

    if has_audio:
        handler._transcribe.assert_called_once()
        assert metadata["decision"] == "best_clip"
        assert metadata["coverage_ratio"] == pytest.approx(0.2)
        assert metadata["warnings"] == []
    else:
        handler._transcribe.assert_not_called()
        assert metadata["decision"] == "no_match"
        assert metadata["coverage_ratio"] == 0
        assert metadata["matched_windows"] == []
        assert any("ASR skipped" in warning and "no audio stream" in warning for warning in metadata["warnings"])


@pytest.mark.parametrize("no_match_policy", ["fail", "placeholder"])
@pytest.mark.parametrize(
    ("mode", "windows", "decision", "coverage", "segments"),
    [
        ("best_clip", [ScoredWindow(2, 4, 0.9, visual_score=0.9)], "best_clip", 0.2, [(2, 4)]),
        ("auto", [ScoredWindow(0, 8, 0.9, visual_score=0.9)], "return_full_video", 0.8, [(0, 10)]),
        (
            "all_matches_montage",
            [ScoredWindow(1, 3, 0.8, visual_score=0.8), ScoredWindow(6, 8, 0.9, visual_score=0.9)],
            "all_matches_montage", 0.4, [(1, 3), (6, 8)],
        ),
    ],
)
async def test_matching_keeps_real_source_selection(
    handler, monkeypatch, no_match_policy, mode, windows, decision, coverage, segments,
):
    monkeypatch.setattr(handler, "_visual_windows", AsyncMock(return_value=(windows, [])))

    metadata = await handler.execute(
        {
            "prompt": "product", "mode": mode, "no_match_policy": no_match_policy,
            "use_asr": False, "padding_before": 0, "padding_after": 0,
        },
        {"input": "input.mp4"}, "out.mp4",
    )

    assert metadata["decision"] == decision
    assert metadata["coverage_ratio"] == pytest.approx(coverage)
    assert [(item["start"], item["end"]) for item in metadata["matched_windows"]] == segments
    assert metadata["warnings"] == []
    handler.run_ffmpeg.assert_awaited_once()
    args = handler.run_ffmpeg.call_args.args[0]
    assert args[args.index("-i") + 1] == "input.mp4"
    assert "lavfi" not in args
    assert args[-1] == "out.mp4"
    if decision == "best_clip":
        assert args[args.index("-ss") + 1] == "2.000"
        assert args[args.index("-t") + 1] == "2.000"
    elif decision == "return_full_video":
        assert args[args.index("-c") + 1] == "copy"
    else:
        assert "concat=n=2:v=1:a=0[outv]" in args[args.index("-filter_complex") + 1]


async def test_below_threshold_scores_fail_without_source_fallback(handler, monkeypatch):
    monkeypatch.setattr(handler, "_visual_windows", AsyncMock(return_value=(
        [ScoredWindow(0, 10, 0.2, visual_score=0.2)], [],
    )))

    with pytest.raises(RuntimeError, match="no matching video segment"):
        await handler.execute(
            {"prompt": "product", "use_asr": False, "no_match_policy": "fail"},
            {"input": "input.mp4"}, "out.mp4",
        )

    handler.run_ffmpeg.assert_not_awaited()


def test_smart_trim_config_parses_node_params():
    config = SmartTrimConfig.from_node_config(
        {
            "prompt": "小猫玩玩具",
            "negative_prompt": "狗",
            "mode": "best_clip",
            "target_duration": "6",
            "min_clip_duration": "1.5",
            "max_clip_duration": "8",
            "max_clips": "3",
            "sample_fps": "1",
            "match_threshold": "0.42",
            "return_full_threshold": "0.7",
            "padding_before": "0.4",
            "padding_after": "0.6",
            "merge_gap": "1.2",
            "use_visual": "false",
            "use_asr": "true",
            "use_vlm_verify": "false",
            "language": "zh",
            "output_format": "mp4",
            "no_match_policy": "placeholder",
        }
    )

    assert config.prompt == "小猫玩玩具"
    assert config.negative_prompt == "狗"
    assert config.mode == "best_clip"
    assert config.target_duration == 6
    assert config.max_clips == 3
    assert config.use_visual is False
    assert config.use_asr is True
    assert config.no_match_policy == "placeholder"


def test_smart_trim_returns_full_video_when_coverage_is_high_and_unconstrained():
    config = SmartTrimConfig.from_node_config(
        {
            "prompt": "我要小猫的视频",
            "mode": "auto",
            "target_duration": 0,
            "return_full_threshold": 0.65,
            "match_threshold": 0.35,
        }
    )
    windows = [
        ScoredWindow(start=0, end=4, score=0.8),
        ScoredWindow(start=4, end=8, score=0.7),
    ]

    selected = select_smart_trim_segments(windows, duration=10, config=config)

    assert selected.decision == "return_full_video"
    assert selected.coverage_ratio == pytest.approx(0.8)
    assert [(segment.start, segment.end) for segment in selected.segments] == [(0, 10)]


def test_smart_trim_target_duration_blocks_full_video_return():
    config = SmartTrimConfig.from_node_config(
        {
            "prompt": "小猫",
            "mode": "auto",
            "target_duration": 5,
            "return_full_threshold": 0.65,
            "match_threshold": 0.35,
        }
    )
    windows = [ScoredWindow(start=0, end=9, score=0.9)]

    selected = select_smart_trim_segments(windows, duration=10, config=config)

    assert selected.decision == "best_clip"
    assert selected.segments[0].end - selected.segments[0].start <= 5.01


def test_smart_trim_no_match_builds_placeholder_metadata_and_ffmpeg_args():
    config = SmartTrimConfig.from_node_config({"prompt": "黑色小猫睡觉", "no_match_policy": "placeholder"})

    selected = select_smart_trim_segments([], duration=12, config=config)
    args = SmartTrimHandler().build_no_match_placeholder_args("out.mp4")

    assert selected.decision == "no_match"
    assert selected.segments == []
    assert "color=c=black:s=1280x720:d=1" in args
    assert args[-1] == "out.mp4"


def test_smart_trim_builds_montage_args_for_multiple_segments():
    handler = SmartTrimHandler()
    args = handler.build_cut_and_concat_args(
        "input.mp4",
        "out.mp4",
        [
            ScoredWindow(start=1.0, end=3.5, score=0.8),
            ScoredWindow(start=7.0, end=9.0, score=0.7),
        ],
    )

    filter_complex = args[args.index("-filter_complex") + 1]
    assert args.count("-i") == 2
    assert "trim=start=1.000:end=3.500,setpts=PTS-STARTPTS[v0]" in filter_complex
    assert "trim=start=7.000:end=9.000,setpts=PTS-STARTPTS[v1]" in filter_complex
    assert "concat=n=2:v=1:a=1[outv][outa]" in filter_complex
