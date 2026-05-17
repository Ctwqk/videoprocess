from __future__ import annotations

import pytest

from worker.handlers.smart_trim import (
    ScoredWindow,
    SmartTrimConfig,
    SmartTrimHandler,
    select_smart_trim_segments,
)


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
