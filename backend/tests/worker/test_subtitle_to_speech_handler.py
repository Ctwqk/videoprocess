from __future__ import annotations

from pathlib import Path

import pytest

from worker.handlers.subtitle_to_speech import (
    AlignmentResult,
    GeneratedAudioBlock,
    SpeechBlock,
    SubtitleToSpeechHandler,
)


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


@pytest.mark.asyncio
async def test_llm_rewrite_resynth_replaces_long_block_before_speedup(monkeypatch, tmp_path):
    handler = SubtitleToSpeechHandler()

    async def fake_rewrite_text_for_tts_slot(**_kwargs):
        return "short text"

    async def fake_probe_audio_duration_ms(audio_path):
        return 900 if Path(audio_path).read_text() == "short text" else 2000

    async def fake_synthesize_text(text, output_path):
        Path(output_path).write_text(text)
        return "local"

    monkeypatch.setattr(handler, "_rewrite_text_for_tts_slot", fake_rewrite_text_for_tts_slot)
    monkeypatch.setattr(handler, "_probe_audio_duration_ms", fake_probe_audio_duration_ms)

    original_path = tmp_path / "original.wav"
    original_path.write_text("long text that does not fit")
    result, warnings = await handler._maybe_rewrite_and_resynthesize_block(
        GeneratedAudioBlock(
            block=SpeechBlock(
                index=1,
                start_seconds=0.0,
                end_seconds=1.0,
                text="long text that does not fit",
                cue_indexes=[1],
            ),
            audio_path=str(original_path),
            duration_ms=2000,
            provider="local",
        ),
        slot_ms=1000,
        language="en",
        rewrite_enabled=True,
        rewrite_min_speedup=1.10,
        rewrite_model=None,
        temp_audio_files=[],
        synthesize_text=fake_synthesize_text,
    )

    assert result.block.text == "short text"
    assert result.duration_ms == 900
    assert Path(result.audio_path).read_text() == "short text"
    assert warnings[0]["type"] == "tts_llm_rewrite_resynth_applied"
    assert warnings[0]["original_duration_ms"] == 2000
    assert warnings[0]["rewritten_duration_ms"] == 900


@pytest.mark.asyncio
async def test_execute_surfaces_resynth_warnings_in_artifact_metadata(monkeypatch, tmp_path):
    handler = SubtitleToSpeechHandler()
    subtitle_path = tmp_path / "input.srt"
    output_path = tmp_path / "speech.wav"
    subtitle_path.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nLong text that needs rewrite\n",
        encoding="utf-8",
    )
    resynth_warning = {
        "type": "tts_llm_rewrite_failed",
        "block_index": 1,
        "cue_indexes": [1],
        "message": "LLM rewrite failed for speech block 1.",
    }

    async def fake_synthesize_cue_with_local_fallback(**kwargs):
        Path(kwargs["output_path"]).write_text(kwargs["text"])
        return "local", kwargs["client"], kwargs["current_base_url"], kwargs["speaker_id"]

    async def fake_probe_audio_duration_ms(_audio_path):
        return 2000

    async def fake_maybe_rewrite_and_resynthesize_block(audio_block, **_kwargs):
        return audio_block, [resynth_warning]

    async def fake_align_audio_blocks(**_kwargs):
        return AlignmentResult(
            audio_inputs=[("block.wav", 0)],
            final_duration=1.0,
            peak_shift_ms=0,
            warnings=[],
        )

    async def fake_mix_audio_timeline(**_kwargs):
        output_path.write_text("mixed")

    monkeypatch.setattr(
        handler,
        "_synthesize_cue_with_local_fallback",
        fake_synthesize_cue_with_local_fallback,
    )
    monkeypatch.setattr(handler, "_probe_audio_duration_ms", fake_probe_audio_duration_ms)
    monkeypatch.setattr(
        handler,
        "_maybe_rewrite_and_resynthesize_block",
        fake_maybe_rewrite_and_resynthesize_block,
    )
    monkeypatch.setattr(handler, "_align_audio_blocks", fake_align_audio_blocks)
    monkeypatch.setattr(handler, "_mix_audio_timeline", fake_mix_audio_timeline)

    metadata = await handler.execute(
        {},
        {"subtitle_file": str(subtitle_path)},
        str(output_path),
    )

    assert metadata["tts_resynth_warnings"] == [resynth_warning]
    assert metadata["tts_alignment_warnings"] == [resynth_warning]
    assert metadata["warnings"] == ["LLM rewrite failed for speech block 1."]
