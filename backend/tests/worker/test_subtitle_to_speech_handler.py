from __future__ import annotations

import pytest

from worker.handlers.subtitle_to_speech import GeneratedAudioBlock, SpeechBlock, SubtitleToSpeechHandler


@pytest.mark.asyncio
async def test_align_audio_blocks_returns_artifact_warning_for_speedup_overshoot(monkeypatch):
    handler = SubtitleToSpeechHandler()

    async def fake_speed_up_audio(*, audio_path, factor, temp_audio_files):
        temp_audio_files.append(f"{audio_path}.fast")
        return f"{audio_path}.fast"

    async def fake_probe_audio_duration_ms(audio_path):
        return 1818

    monkeypatch.setattr(handler, "_speed_up_audio", fake_speed_up_audio)
    monkeypatch.setattr(handler, "_probe_audio_duration_ms", fake_probe_audio_duration_ms)

    result = await handler._align_audio_blocks(
        audio_blocks=[
            GeneratedAudioBlock(
                block=SpeechBlock(
                    index=1,
                    start_seconds=0.0,
                    end_seconds=1.0,
                    text="hello",
                    cue_indexes=[1],
                ),
                audio_path="block1.wav",
                duration_ms=2000,
                provider="local",
            )
        ],
        timeline_duration=1.0,
        max_speedup=1.10,
        max_leading_delay_ms=0,
        temp_audio_files=[],
    )

    assert result.audio_inputs == [("block1.wav.fast", 0)]
    assert result.warnings == [
        {
            "type": "tts_alignment_speedup_overshoot",
            "block_index": 1,
            "cue_indexes": [1],
            "required_speedup": 2.0,
            "applied_speedup": 1.1,
            "safe_speedup": 1.1,
            "message": "Generated speech block 1 required 2.00x speed-up; applied 1.10x cap.",
        }
    ]


@pytest.mark.asyncio
async def test_mix_audio_timeline_uses_small_dropout_transition(monkeypatch):
    captured = {}

    async def fake_run_ffmpeg(self, args):
        captured["args"] = args
        return ""

    monkeypatch.setattr(SubtitleToSpeechHandler, "run_ffmpeg", fake_run_ffmpeg)

    await SubtitleToSpeechHandler()._mix_audio_timeline(
        audio_inputs=[("block1.wav", 0), ("block2.wav", 500)],
        duration=2.0,
        output_path="out.wav",
    )

    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "dropout_transition=0.05" in filter_complex
