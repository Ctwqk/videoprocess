# P3A Quality QA Foundation Design

## Context

The P0/P1/P2 passes improved encode defaults, audio ducking, Whisper defaults, clip scoring, recent-clip novelty, artifact caching, and aspect-ratio aware assembly. The remaining P3 quality work should start with a narrow foundation that improves final delivery consistency without blocking normal pipeline completion.

This spec covers a soft quality gate at export time plus reusable audio normalization helpers. If final media is below the configured quality target, the system gets one automatic repair attempt. If the repair cannot run or still misses the threshold, the pipeline still delivers the artifact and records warnings in artifact metadata.

## Goals

- Add export-time quality QA metadata for final artifacts.
- Give low-quality exports one automatic repair attempt.
- Add reusable two-pass loudnorm support for final audio normalization paths.
- Keep failure behavior soft: QA failures should not make the job fail unless the export itself cannot produce an artifact.
- Make the implementation deterministic and unit-testable without requiring real `libvmaf` in CI.

## Non-Goals

- No multi-pass or iterative repair loop beyond one attempt.
- No UI changes in this pass.
- No MediaPipe, diarization, LLM reranking, or batched CLIP work in this spec.
- No hard-fail quality gate by default.
- No new database tables; QA output is stored in existing `Artifact.media_info`.

## User-Visible Behavior

Export produces an artifact as it does today. When quality QA is enabled, the artifact metadata includes `quality_report`.

Example shape:

```json
{
  "quality_report": {
    "enabled": true,
    "gate_mode": "soft_repair_once",
    "qa_action": "reencoded_once",
    "reencode_attempted": true,
    "vmaf_score": 78.4,
    "audio_lufs": -18.1,
    "audio_true_peak": -1.2,
    "thresholds": {
      "vmaf_min_score": 80,
      "loudnorm_target_i": -16,
      "loudnorm_target_lra": 11,
      "loudnorm_target_tp": -1.5
    },
    "warnings": [
      "vmaf_below_threshold_after_repair"
    ]
  }
}
```

If VMAF or loudnorm measurement is unavailable, export still succeeds and records warnings such as `vmaf_unavailable` or `loudnorm_measure_failed`.

## Configuration

`export` node config gets these optional fields:

- `enable_quality_qa`: boolean, default `true`
- `quality_gate_mode`: select, default `soft_repair_once`
- `vmaf_min_score`: number, default `80`
- `loudnorm_target_i`: number, default `-16`
- `loudnorm_target_lra`: number, default `11`
- `loudnorm_target_tp`: number, default `-1.5`

Only `soft_repair_once` is implemented in this pass. The config shape leaves room for future `warn_only` and `hard_fail`, but those modes are not exposed as behavior here.

## Architecture

Add a worker-local helper module:

`backend/worker/handlers/media_quality.py`

Responsibilities:

- Parse export QA config.
- Measure VMAF when a reference is available.
- Probe audio loudnorm stats with a first ffmpeg pass.
- Build the second-pass loudnorm filter from measured stats.
- Perform one repair encode into a temporary file.
- Return a structured `QualityReport` dict that can be merged into artifact metadata.

`ExportHandler` remains the integration point because export is the final user-facing artifact boundary. The handler will:

1. Copy input to export destination as today.
2. Copy input to `output_path` as today.
3. Run `MediaQualityService.qa_export(output_path, config, input_meta)` when enabled.
4. If the service returns a repaired file, replace both the export destination and `output_path` with the repaired file.
5. Return `{"quality_report": report}` so `worker/main.py` persists it to `Artifact.media_info`.

The helper should avoid direct database access. It receives file paths and metadata only.

## VMAF Measurement

VMAF needs a reference file. For export QA, the safest first step is to compare the exported file against the immediate input artifact when both are local files. This catches unnecessary export-time degradation and repair regressions. It does not claim absolute perceptual quality across unrelated sources.

Implementation detail:

- Use ffmpeg with `libvmaf` against input and exported output.
- Parse JSON output from a temporary VMAF log file.
- If ffmpeg lacks `libvmaf`, return `vmaf_unavailable`.
- If no local reference path is available, return `vmaf_reference_unavailable`.

Default threshold: `vmaf_min_score=80`.

## Audio Loudnorm

Two-pass loudnorm should be available through helper functions so multiple handlers can adopt it without duplicating parsing logic.

First pass:

```text
loudnorm=I={target_i}:LRA={target_lra}:TP={target_tp}:print_format=json
```

Second pass:

```text
loudnorm=I={target_i}:LRA={target_lra}:TP={target_tp}:measured_I=...:measured_LRA=...:measured_TP=...:measured_thresh=...:offset=...:linear=true:print_format=summary
```

P3A integrates this in export repair. Existing audio handlers can keep their current single-pass loudnorm behavior initially, but their tests should cover the reusable helper. A follow-up can switch `bgm`, `replace_audio`, and `subtitle_to_speech` to the helper once the foundation is verified.

## Repair Strategy

One repair attempt is allowed.

When VMAF is below threshold or audio loudness is outside tolerance:

- Re-encode video with `libx264`, `preset=slow`, `crf=18`, `pix_fmt=yuv420p`, `movflags=+faststart`, BT.709 color tags.
- Apply two-pass loudnorm when audio exists and measurement succeeds.
- Keep audio as AAC, `-ar 48000`, `-ac 2`.
- Write repaired output to a temporary path, then atomically replace export copies.

If repair command fails:

- Keep the original exported file.
- Record `repair_failed`.
- Do not fail the node.

If repair succeeds but QA still misses threshold:

- Keep the repaired file.
- Record `vmaf_below_threshold_after_repair` or `loudness_out_of_range_after_repair`.

## Error Handling

- Missing ffmpeg or copy failure remains a node failure because export cannot operate.
- Missing `libvmaf`, malformed VMAF JSON, or loudnorm parse errors become warnings.
- Repair encode failure becomes a warning and preserves the original export.
- Temporary files are cleaned up best-effort.
- Cancellation behavior remains owned by the existing worker cancellation watcher and ffmpeg subprocess handling.

## Testing

Unit tests should not require real media, ffmpeg, or libvmaf.

Add tests for:

- Export writes `quality_report` metadata when QA is enabled.
- Export replaces output when soft repair returns a repaired path.
- Export keeps original output and records warnings when repair fails.
- VMAF parser extracts score from ffmpeg JSON logs.
- VMAF unavailable maps to a warning instead of an exception.
- Loudnorm parser extracts measured fields.
- Loudnorm second-pass filter includes measured values.
- Export node registry exposes QA parameters.

Run existing backend tests after implementation:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

## Rollout

This is safe to ship behind export config defaults because the gate is soft. If runtime cost is a concern, callers can set `enable_quality_qa=false` per export node. The next P3 slice can add UI visibility for `quality_report` and then adopt the loudnorm helper inside audio-producing handlers.
