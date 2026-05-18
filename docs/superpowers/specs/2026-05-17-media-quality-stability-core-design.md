# Media Quality And Stability Core Design

Date: 2026-05-17

## Summary

VideoProcess should ship a focused P0/P2 quality and stability pass before adding
larger AutoFlow ranking, cache, or VMAF features. This phase fixes confirmed
quality regressions in the current worker chain, tightens AutoFlow validation
repair behavior, and prevents failed terminal pipeline nodes from being
presented as deliverable final artifacts.

The selected approach is a layered core fix:

1. Centralize worker media quality defaults.
2. Improve audio, subtitle, TTS, and Whisper defaults.
3. Make unrepairable AutoFlow validation failures fail closed or use a
   deterministic fallback.
4. Treat failed pipeline leaf nodes as failed delivery.
5. Add focused regression tests around each changed contract.

## Goals

- Stop repeated `fast`/CRF 23 re-encodes from visibly degrading multi-step
  pipelines.
- Ensure MP4 outputs are broadly compatible with iOS, WeChat, Douyin, and
  streaming playback defaults.
- Improve resize sharpness, subtitle readability, speech transcription
  accuracy, and BGM dialogue intelligibility.
- Prevent invalid AutoFlow pipeline definitions from reaching execution.
- Prevent partial jobs from marking broken leaf outputs as final deliverables.
- Keep the implementation small enough to land before larger ranking and cache
  projects.

## Non-Goals

- Do not implement seven-day used-clip dedupe in this phase.
- Do not add embedding recall, CLIP ranking, LLM metadata generation, or LLM
  rerank in this phase.
- Do not add intermediate artifact cache tables.
- Do not implement two-pass loudnorm, VMAF QA gates, MediaPipe face analysis,
  diarization, or batched CLIP scoring in this phase.
- Do not change public publishing policy. External platform assets still require
  human review, and default publication privacy remains private or unlisted.
- Do not modify the current AI graph planner design except where validation or
  execution safety requires stricter handling.

## Current State

The reviewed code confirms the core issues:

- `backend/worker/handlers/base.py` defaults to `preset="fast"` and individual
  worker handlers repeatedly hardcode `fast`/CRF 23.
- MP4 output parameters such as `-pix_fmt yuv420p`, `-movflags +faststart`, and
  BT.709 color metadata are not centralized.
- Scale filters in handlers such as `transcode`, `concat_many`, and
  `concat_vertical_timeline` do not use high-quality Lanczos scaling.
- `bgm` mixes fixed-volume audio with `amix` and does not duck under original
  dialogue.
- `subtitle.py` reads `font_color` and `outline_color` but hardcodes ASS colors.
- `subtitle_to_speech` allows up to `1.35` speed-up and mixes blocks with
  `dropout_transition=0`.
- `speech_to_subtitle` and `smart_trim` do not pass VAD, word timestamp, or
  anti-hallucination options to faster-whisper. `smart_trim` hardcodes
  `model="small"`.
- `AutoFlowRepairService.repair()` returns the original broken definition for
  unrepairable validation errors.
- `JobEngine._maybe_mark_job_complete()` allows partial success to mark terminal
  artifacts even when a pipeline leaf failed.

The repository currently has unrelated AutoFlow planner work in the working
tree. This design intentionally avoids broad planner refactors and limits code
touches to media quality helpers, affected handlers, validation repair, and
orchestrator completion semantics.

## Design

### 1. Worker Encoding Baseline

`BaseHandler` becomes the source of truth for quality profiles:

- `build_video_encode_args()` defaults to final-output quality:
  `preset="medium"` and `crf=20`.
- MP4-oriented H.264/H.265 outputs include:
  `-pix_fmt yuv420p`, `-movflags +faststart`, and BT.709 color metadata.
  This should be centralized through `BaseHandler` helpers so handlers do not
  repeat the same args, but non-MP4 outputs and stream-copy paths must not
  receive muxer or encoder flags that ffmpeg rejects.
- A helper such as `intermediate_video_encode_args()` returns
  `libx264` with `preset="slow"` and `crf=18`.
- A helper such as `final_video_encode_args()` returns the final profile:
  `preset="medium"` and `crf=20`.
- `h264_nvenc` and `hevc_nvenc` CPU fallback maps NVENC CQ to libx264/libx265
  CRF with `max(18, cq - 2)` instead of copying the value directly.

Handlers that generate intermediate visual outputs should move to the
intermediate helper: `concat_many`, `concat_vertical_timeline`, `smart_trim`,
`subtitle`, `trim`, `vertical_crop`, `watermark`, and `title_overlay`.

Final transcode paths should use the final helper unless the user explicitly
sets codec, CRF, bitrate, or preset in node config. `video_codec="copy"` remains
stream copy and must not receive video encoder flags. MP4 copy outputs may still
receive container-safe `+faststart` when ffmpeg accepts it without re-encoding.

Scale filters should use high-quality scaling. Prefer local
`scale=...:flags=lanczos` where the filter is simple. For complex filters where
local scale flags are brittle, add global
`-sws_flags lanczos+accurate_rnd+full_chroma_int`.

### 2. Audio, Subtitle, TTS, And Whisper

`bgm` keeps video stream copy but replaces fixed `amix` with dialogue-aware
mixing:

- Resample original audio and BGM to 48 kHz.
- Apply BGM volume and fade filters before ducking.
- Use `sidechaincompress` so original dialogue lowers BGM gain.
- Mix original plus ducked BGM with `amix=duration=first:normalize=0`.
- Apply `loudnorm=I=-16:LRA=11:TP=-1.5`.
- Output `aac`, `-ar 48000`, and `-ac 2`.

For videos with no original audio, BGM still uses 48 kHz stereo and loudnorm.

`subtitle.py` should generate ASS style from node config:

- Add `_color_to_ass(color: str) -> str` supporting common names and `#RRGGBB`.
- Probe video height and scale font size from a 720p baseline.
- Include `FontName=PingFang SC`, `BorderStyle=1`, `Outline=2`, `Shadow=1`,
  and `MarginV=int(height * 0.05)`.
- Preserve existing position to ASS alignment behavior.
- Encode with the intermediate video quality helper.

`subtitle_to_speech` should reduce mechanical speed-up:

- Node registry default `alignment_max_speedup` changes from `1.35` to `1.10`.
- Audio block mixing changes `dropout_transition=0` to a small value such as
  `0.05`.
- This phase records or surfaces long-overlap warnings where practical, but does
  not introduce LLM text compression or resynthesis.

Whisper defaults should improve accuracy without forcing the heaviest model:

- `speech_to_subtitle` default model changes from `small` to `medium`.
- The existing options remain `tiny`, `base`, `small`, `medium`, and
  `large-v3`.
- `smart_trim` reads `whisper_model` from node config and defaults to `medium`
  instead of hardcoding `small`.
- Both faster-whisper call sites pass:
  `vad_filter=True`,
  `vad_parameters={"min_silence_duration_ms": 500}`,
  `word_timestamps=True`, and
  `condition_on_previous_text=False`.

Missing faster-whisper remains a worker error. This phase does not silently
downgrade transcription quality when the dependency is unavailable.

### 3. AutoFlow Validation Repair

`validate_pipeline()` remains the structural authority. Repair stays
deterministic and limited.

Add `AutoFlowUnrepairableError` with:

- `unrepairable_errors: list[str]`
- `applied_repairs: list[str]`

`AutoFlowRepairService.repair()` raises this error for:

- `cycle_detected`
- `port_type_mismatch`
- missing assets that cannot be filled from candidates
- invalid params that cannot be reset from registry defaults
- any unknown validation error type

`AutoFlowService.plan()` and metadata patch rebuild paths catch the error and:

- Add a plan warning explaining that the generated workflow was unrepairable.
- Rebuild with the deterministic `material_library_remix` fallback template.
- Re-run `validate_pipeline()` on the fallback definition.
- Expose successful repairs in `validation.repairs`.
- Preserve validation errors and block execution approval if the fallback is
  still invalid.

Execution approval and execution paths must require a valid pipeline definition.
This keeps invalid graphs out of orchestrator jobs even if the UI or planner
returns a malformed draft.

### 4. Orchestrator Delivery Semantics

Pipeline leaf nodes are the delivery boundary. A leaf node is any node whose id
does not appear as an edge source.

Update job completion rules:

- If all node executions succeed, the job is `SUCCEEDED` and successful leaf
  artifacts are marked `FINAL`.
- If any leaf execution is `FAILED` or `SKIPPED`, the job is `FAILED` and no
  failed or skipped leaf output is marked `FINAL`.
- If only non-leaf nodes fail while at least one leaf succeeds, the job may be
  `PARTIALLY_FAILED`, and only successful leaf outputs are marked `FINAL`.
- If no successful leaf output exists, the job is `FAILED`.

`_mark_final_artifacts()` should only promote artifacts from leaf node
executions whose status is `SUCCEEDED`.

Artifact listing should mirror the same rule. Terminal-node outputs may be
preserved for completed jobs only when their node execution succeeded; failed or
skipped leaf outputs should not appear as final deliverables.

Explicit `export` nodes have no output port in the registry. Graph leaf
calculation can still use edge structure, but artifact promotion only acts when
the node execution has an `output_artifact_id`.

### 5. Tests And Verification

Add focused tests for the changed contracts:

- Worker encode args include `slow`/CRF 18 for intermediate paths and
  `medium`/CRF 20 for final paths.
- Encoded MP4 outputs include `yuv420p`, `+faststart`, BT.709 metadata, and
  high-quality scaling.
- NVENC fallback maps CQ to CPU CRF with `max(18, cq - 2)`.
- `bgm` args include `sidechaincompress`, 48 kHz stereo output, loudnorm, and
  AAC encoding.
- `subtitle` style applies configured colors, `#RRGGBB` conversion, adaptive
  font size, outline, shadow, and margin.
- `speech_to_subtitle` and `smart_trim` pass VAD, word timestamp, and
  `condition_on_previous_text=False` to faster-whisper.
- `subtitle_to_speech` defaults to `alignment_max_speedup=1.10` and uses a
  nonzero audio dropout transition.
- `AutoFlowRepairService` raises on unrepairable errors.
- AutoFlow service/API tests cover fallback success and invalid fallback
  execution blocking.
- Orchestrator tests cover failed leaf jobs, successful partial leaf delivery,
  and full success behavior.

Required backend verification:

```bash
cd /home/taiwei/Constructure-repos/videoprocess/backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Frontend checks are only required if implementation changes frontend code,
schemas consumed by frontend types, or UI behavior:

```bash
cd /home/taiwei/Constructure-repos/videoprocess/frontend
npm install
npm run build
npm run lint || true
```

## Rollout And Rollback

No database migration is included in this phase.

The main rollout risk is increased CPU time from `slow`/CRF 18 intermediate
encodes and `medium` Whisper defaults. This is intentional for the quality-first
scope. If worker capacity becomes the bottleneck, rollback should first adjust
central quality helpers rather than reverting every handler.

Rollback boundaries:

- Encoding defaults are centralized in `BaseHandler` helpers.
- BGM behavior is isolated to `backend/worker/handlers/bgm.py`.
- Whisper/TTS default changes are node-registry and handler-local.
- AutoFlow fail-closed repair behavior is isolated to validation repair and
  service handling.
- Final artifact promotion is isolated to orchestrator completion and artifact
  listing logic.

## Implementation Order

1. Add encoding profile helpers and update worker handlers.
2. Add worker tests for encoding args, scaling, BGM, subtitle style, TTS, and
   Whisper options.
3. Add `AutoFlowUnrepairableError` and service fallback handling.
4. Add AutoFlow repair/API regression tests.
5. Update orchestrator completion and artifact listing semantics.
6. Add orchestrator final artifact regression tests.
7. Run backend verification.

This order keeps media quality fixes independent from validation and delivery
semantics, while still finishing with end-to-end safety gates.
