# Media Quality Stability Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the P0/P2 media quality and delivery-safety issues from the approved design, including 48 kHz silent placeholders and TTS overshoot warnings in artifact metadata.

**Architecture:** Centralize encode-quality helpers in `BaseHandler`, then update media handlers to use those helpers. Keep AutoFlow repair fail-closed at the service boundary and tighten orchestrator final-artifact semantics around successful leaf nodes only.

**Tech Stack:** FastAPI backend, SQLAlchemy models, async worker handlers, ffmpeg/faster-whisper command assembly, pytest.

---

## File Structure

- Modify `backend/worker/handlers/base.py`: encoding profiles, MP4 compatibility flags, scale flags helper, NVENC fallback mapping.
- Modify `backend/worker/handlers/{transcode,concat_many,concat_vertical_timeline,smart_trim,subtitle,trim,vertical_crop,watermark,title_overlay,bgm,speech_to_subtitle,subtitle_to_speech}.py`: media quality behavior.
- Modify `backend/app/node_registry/builtin/{speech_to_subtitle,subtitle_to_speech,smart_trim}.py`: defaults and exposed params.
- Modify `backend/app/autoflow/validation_repair.py`: raise `AutoFlowUnrepairableError`.
- Modify `backend/app/autoflow/service.py`: fallback or block invalid AutoFlow plans.
- Modify `backend/app/orchestrator/engine.py` and `backend/app/api/artifacts.py`: leaf-node final-artifact semantics.
- Add/modify tests in `backend/tests/worker`, `backend/tests/autoflow`, and `backend/tests/orchestrator`.

## Task 1: Encoding Helpers And Worker Command Tests

**Files:**
- Modify: `backend/worker/handlers/base.py`
- Modify: worker handlers that call `build_video_encode_args`
- Test: `backend/tests/worker/test_media_quality_args.py`

- [ ] **Step 1: Write failing tests for encoding defaults**

Create tests that assert final helpers produce `-preset medium`, `-crf 20`, `-pix_fmt yuv420p`, `-movflags +faststart`, BT.709 metadata, and intermediate helpers produce `-preset slow`, `-crf 18`.

- [ ] **Step 2: Run the focused test**

Run: `cd backend && python3 -m pytest tests/worker/test_media_quality_args.py -q`
Expected: fail because helpers do not exist or defaults are still old.

- [ ] **Step 3: Implement helpers**

Add `intermediate_video_encode_args()`, `final_video_encode_args()`, `scale_filter()`, MP4 compatibility args, and NVENC CQ to CPU CRF mapping.

- [ ] **Step 4: Update handlers**

Move intermediate visual handlers to the intermediate helper and final transcode defaults to the final helper while preserving explicit user config and copy mode.

- [ ] **Step 5: Run focused tests**

Run: `cd backend && python3 -m pytest tests/worker/test_media_quality_args.py -q`
Expected: pass.

## Task 2: Audio, Subtitle, TTS, And Whisper Quality Tests

**Files:**
- Modify: `backend/worker/handlers/bgm.py`
- Modify: `backend/worker/handlers/subtitle.py`
- Modify: `backend/worker/handlers/subtitle_to_speech.py`
- Modify: `backend/worker/handlers/speech_to_subtitle.py`
- Modify: `backend/worker/handlers/smart_trim.py`
- Modify: `backend/app/node_registry/builtin/speech_to_subtitle.py`
- Modify: `backend/app/node_registry/builtin/subtitle_to_speech.py`
- Modify: `backend/app/node_registry/builtin/smart_trim.py`
- Test: `backend/tests/worker/test_media_quality_args.py`
- Test: `backend/tests/worker/test_subtitle_to_speech_handler.py`

- [ ] **Step 1: Write failing tests**

Cover BGM ducking/loudnorm, subtitle ASS color/style, Whisper transcribe kwargs, `smart_trim` model config, `alignment_max_speedup=1.10`, `dropout_transition=0.05`, `concat_vertical_timeline` silent audio at 48 kHz, and TTS overshoot warnings returned in artifact metadata.

- [ ] **Step 2: Run focused tests**

Run: `cd backend && python3 -m pytest tests/worker/test_media_quality_args.py tests/worker/test_subtitle_to_speech_handler.py -q`
Expected: fail for missing behavior.

- [ ] **Step 3: Implement media behavior**

Update handlers and node registry defaults. For TTS alignment, return metadata with a warning when the unclamped required speed-up exceeds the safe 1.10 budget.

- [ ] **Step 4: Run focused tests**

Run: `cd backend && python3 -m pytest tests/worker/test_media_quality_args.py tests/worker/test_subtitle_to_speech_handler.py -q`
Expected: pass.

## Task 3: AutoFlow Validation Repair Fail-Closed

**Files:**
- Modify: `backend/app/autoflow/validation_repair.py`
- Modify: `backend/app/autoflow/service.py`
- Test: `backend/tests/autoflow/test_validation_repair.py`
- Test: `backend/tests/autoflow/test_autoflow_api.py`

- [ ] **Step 1: Write failing tests**

Assert `AutoFlowRepairService.repair()` raises `AutoFlowUnrepairableError` for cycles and port mismatches, and service fallback does not approve invalid plans for execution.

- [ ] **Step 2: Run focused tests**

Run: `cd backend && python3 -m pytest tests/autoflow/test_validation_repair.py tests/autoflow/test_autoflow_api.py -q`
Expected: fail because repair currently returns broken definitions.

- [ ] **Step 3: Implement error and fallback handling**

Raise unrepairable errors from repair, catch them in service plan/rebuild paths, build the deterministic fallback template, revalidate, and block execution if invalid.

- [ ] **Step 4: Run focused tests**

Run: `cd backend && python3 -m pytest tests/autoflow/test_validation_repair.py tests/autoflow/test_autoflow_api.py -q`
Expected: pass.

## Task 4: Orchestrator Leaf Delivery Semantics

**Files:**
- Modify: `backend/app/orchestrator/engine.py`
- Modify: `backend/app/api/artifacts.py`
- Test: `backend/tests/orchestrator/test_engine_delivery_semantics.py`

- [ ] **Step 1: Write failing tests**

Assert failed/skipped leaf nodes make the job `FAILED`, successful leaf outputs are the only artifacts promoted to `FINAL`, and non-leaf failure with a successful leaf can be `PARTIALLY_FAILED`.

- [ ] **Step 2: Run focused tests**

Run: `cd backend && python3 -m pytest tests/orchestrator/test_engine_delivery_semantics.py -q`
Expected: fail because current engine marks partial jobs final too broadly.

- [ ] **Step 3: Implement leaf helper and completion rules**

Compute leaf node ids from `PipelineDefinition.edges`, update job status rules, and only promote successful leaf outputs.

- [ ] **Step 4: Run focused tests**

Run: `cd backend && python3 -m pytest tests/orchestrator/test_engine_delivery_semantics.py -q`
Expected: pass.

## Task 5: Full Backend Verification

**Files:**
- Verify all modified backend files.

- [ ] **Step 1: Run pytest**

Run: `cd backend && python3 -m pytest`
Expected: pass.

- [ ] **Step 2: Run optional linters**

Run: `cd backend && python3 -m ruff check . || true`
Expected: command may pass or report missing ruff, but must not block.

Run: `cd backend && python3 -m mypy app || true`
Expected: command may pass or report missing mypy, but must not block.

- [ ] **Step 3: Review diff scope**

Run: `git status --short` and `git diff --check`.
Expected: only planned files are modified by this work, with no whitespace errors.
