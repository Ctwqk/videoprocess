# AutoFlow Content Quality Design

Date: 2026-05-17

## Summary

VideoProcess should follow the media quality/stability pass with a focused
AutoFlow P1 content quality pass. The goal is to make selected clips, titles,
thumbnail text, and pacing feel tied to the actual source material instead of
being driven mostly by keyword matching, fixed templates, and uniform shot
durations.

The selected approach is an AI-assisted path with deterministic fallback:

1. Rank candidate clips with embedding relevance, visual signals, and recent-use
   novelty penalties.
2. Generate titles and thumbnail text from selected clip facts through the local
   LLM gateway when available.
3. Fall back to deterministic, conservative scoring and metadata when AI
   services fail.
4. Add platform pacing profiles so storyboard duration and ranking preferences
   differ across short-video and long-form platforms.
5. Persist recently selected asset ids so repeated runs avoid returning the same
   clips when alternatives exist.

## Goals

- Replace string-only topic relevance with semantic relevance when embedding
  infrastructure is reachable.
- Use existing visual analysis fields such as face presence, scene diversity,
  brightness, object labels, and dominant action in clip scoring.
- Penalize clips used in the last seven days across AutoFlow runs.
- Generate title and thumbnail candidates from real selected-clip fields instead
  of fixed templates that may describe nonexistent events.
- Keep every AI-assisted feature optional at runtime. Missing or unhealthy LLM,
  embedding, or Qdrant services must not block AutoFlow plan generation.
- Add platform-specific pacing defaults for TikTok/Douyin-style short video,
  YouTube, Bilibili, and generic fallback platforms.
- Preserve compatibility with `backend/app/schemas/pipeline.py` and
  `validate_pipeline()`.

## Non-Goals

- Do not implement orchestrator intermediate artifact caching in this phase.
- Do not change orchestrator final-artifact or leaf-node failure semantics in
  this phase.
- Do not implement VMAF QA gates, MediaPipe pose/face composition analysis, or
  batched CLIP frame scoring.
- Do not require a new model service or new external SaaS dependency.
- Do not let LLM output define arbitrary workflow graphs.
- Do not publicly publish external platform assets without explicit human
  review.
- Do not remove existing deterministic AutoFlow templates or fallback builders.

## Current State

The current code still has the P1 issues identified by review:

- `backend/app/autoflow/clip_ranker.py` computes `_topic_relevance()` by
  checking whether normalized topic tokens occur in candidate title,
  description, tags, or platform name.
- `clip_ranker.py` only uses motion and watermark visual signals directly,
  while richer visual analysis fields are available elsewhere.
- `_dedupe()` only removes duplicates within a single ranking call; it does not
  know what the user selected in previous runs.
- `backend/app/autoflow/metadata_generator.py` contains hardcoded title and
  thumbnail templates and currently selects the first title deterministically.
- `backend/app/autoflow/content_strategy.py` records target platforms but does
  not provide platform-specific pacing constraints.
- `backend/app/autoflow/storyboard_generator.py` distributes shot durations with
  a simple `total / len(shots)` fit instead of using platform pacing curves.

Constructure local infra can support this design, but should not be required for
correctness:

- `llm-gateway` exposes `http://127.0.0.1:8000` and includes a
  `videoprocess-generic-chat` route.
- Shared Qdrant exposes HTTP on `127.0.0.1:6333` and gRPC on `127.0.0.1:6334`.
- The embedding gateway is environment-specific and must be configured rather
  than hardcoded.

## Design

### 1. Service Boundaries

Add small AutoFlow services rather than putting infrastructure calls directly in
ranker, metadata generator, or storyboard code.

`EmbeddingRelevanceService`:

- Input: user intent, candidate list, and optional platform profile.
- Output: mapping from candidate asset id to relevance score in `[0.0, 1.0]`.
- Primary path: use configured embedding infrastructure to compare
  `intent.subject`, `intent.goal`, and intent keywords against candidate
  `title`, `description`, `tags`, object labels, and dominant action.
- Fallback path: use an improved deterministic token relevance score compatible
  with the existing `_topic_relevance()` behavior.
- Failure behavior: return fallback scores and a warning string; do not raise
  for normal network, timeout, or invalid-response failures.

`RecentClipUsageStore`:

- Input: database session and user or project scope when available.
- Output: set of asset ids selected in the last seven days.
- Write path: after a run selects clips, persist `run_id`, `asset_id`,
  `selected_at`, source platform, and optional candidate metadata.
- Failure behavior: ranking proceeds without novelty penalties and records a
  warning in AutoFlow plan or run metadata.

`MetadataCandidateService`:

- Input: intent, selected clips, visual facts, platform profile, and content
  strategy.
- Output: title candidates, selected title, thumbnail text candidates, selected
  thumbnail text, tags, and warnings.
- Primary path: call `llm-gateway` with a constrained prompt that includes only
  selected clip facts and asks for JSON output.
- Fallback path: use conservative deterministic templates that avoid unverifiable
  claims such as "last two seconds" unless the supplied clip facts explicitly
  support that claim.
- Failure behavior: discard malformed LLM output, record a warning, and use
  fallback metadata.

`PlatformProfileService`:

- Input: target platform names from intent or AutoFlow request.
- Output: a merged platform profile with shot-duration constraints, hook length,
  pacing curve, metadata tone hints, and ranking preferences.
- Failure behavior: return the generic profile.

These boundaries keep ranker and storyboard code testable without requiring live
LLM or vector services.

### 2. Ranking Model

`ClipRanker` should continue to expose a stable ranking API, but its scoring
input expands to include optional service-provided signals:

```text
score =
  0.30 * semantic_relevance
  + 0.20 * visual_motion
  + 0.12 * source_quality
  + 0.10 * intent_fit
  + 0.06 * face_present
  + 0.05 * scene_change_diversity
  + 0.04 * brightness_fit
  + 0.03 * platform_fit
  - 0.10 * watermark_penalty
  - 0.15 * recent_used_penalty
```

The exact constants can be adjusted in implementation if current local score
ranges require normalization, but the important contract is:

- Semantic relevance replaces string-only topic relevance when embeddings are
  available.
- Visual motion weight increases compared with the current behavior.
- Face, scene diversity, and brightness become first-class scoring features.
- Recent use is a penalty, not a hard ban. A recently used clip can still win if
  the candidate pool is weak, but alternatives should usually outrank it.

Candidate visual fields should be read defensively from existing candidate
metadata so old candidate objects still rank correctly. Missing visual fields
evaluate to neutral scores rather than zeroing the whole candidate.

### 3. Recent-Use Persistence

Add an `autoflow_used_clips` table and model. The minimum useful schema is:

```text
id: uuid or integer primary key
run_id: foreign key or string run id
asset_id: string, indexed
source_platform: nullable string
candidate_title: nullable string
selected_at: datetime, indexed
metadata_json: nullable json
```

Indexes:

- `(asset_id, selected_at)` for recent-use lookup.
- `(run_id)` for debugging which clips were selected by a run.

Retention can be logical in this phase. The query reads `selected_at >= now - 7
days`; physical cleanup can be a later maintenance task.

The write should happen after clip selection is known and before run completion
is reported. If the write fails, run execution should not fail; the error should
be recorded in run metadata or warnings so the issue is diagnosable.

### 4. Metadata Generation

The metadata generator should become material-aware:

1. Build a compact fact list from selected clips:
   - title
   - description
   - tags
   - visual object labels
   - dominant action
   - rough duration
   - source platform
2. Ask `llm-gateway` for a bounded JSON response:
   - `titles`: up to 10 strings
   - `thumbnail_texts`: up to 5 strings
   - `tags`: up to 12 strings
   - `rationale`: short internal explanation for debugging
3. Validate output:
   - strings must be short enough for UI and platform constraints;
   - no public-upload or rights claims;
   - no claims about events absent from clip facts;
   - thumbnail text must be grounded in at least one selected clip fact.
4. Select the best candidate by platform profile:
   - short-video profiles prefer high-hook, short titles;
   - YouTube profiles allow clearer, slightly longer titles;
   - Bilibili profiles can prefer more descriptive Chinese titles.

Fallback metadata uses clip-aware deterministic templates. For example, a
fallback title can combine intent subject plus strongest observed action or
object label. It should not claim a twist, reveal, or exact moment unless that
data is actually present.

### 5. Platform Profiles

Add a profile object used by content strategy, storyboard, metadata, and ranker.

Minimum profile fields:

```text
platform_key: string
min_shot_seconds: float
max_shot_seconds: float
hook_seconds: float
pacing_curve: enum["front_loaded", "steady", "long_form"]
preferred_aspect_ratios: list[str]
title_max_chars: int
thumbnail_text_max_chars: int
motion_preference: float
novelty_preference: float
```

Initial defaults:

- `tiktok` / `douyin`: short-video profile, `0.5-2.0s` shots,
  `hook_seconds=1.0`, front-loaded pacing, high motion preference.
- `youtube`: long-form profile, `3.0-5.0s` shots, `hook_seconds=3.0`,
  long-form pacing, moderate motion preference.
- `bilibili`: balanced profile, `2.0-4.0s` shots, `hook_seconds=2.0`,
  steady pacing, slightly higher descriptive-title allowance.
- `generic`: balanced fallback, `1.5-3.5s` shots, `hook_seconds=2.0`.

When multiple platforms are selected, merge conservatively:

- use the shortest `max_shot_seconds`;
- use the shortest title and thumbnail text limits;
- use the highest motion and novelty preferences;
- keep all preferred aspect ratios for downstream selection.

### 6. Storyboard Duration Fit

`StoryboardGenerator` should no longer only divide total duration evenly. It
should accept a `PlatformProfile` and fit shot durations in this order:

1. Assign the first shot a hook duration close to `hook_seconds`.
2. Assign remaining shots according to the profile pacing curve.
3. Clamp every shot to `min_shot_seconds` and `max_shot_seconds`.
4. Redistribute any remainder without breaking clamps when possible.
5. If requested total duration is impossible with the shot count and clamps,
   prefer preserving total duration and emit a warning that platform pacing was
   relaxed.

This keeps output duration predictable while making the cut rhythm platform
aware.

### 7. Configuration

Add environment-driven configuration, all optional:

```text
AUTOFLOW_LLM_GATEWAY_URL=http://127.0.0.1:8000
AUTOFLOW_LLM_SOURCE=videoprocess
AUTOFLOW_LLM_PROFILE=generic_chat
AUTOFLOW_EMBEDDING_URL=
AUTOFLOW_QDRANT_URL=http://127.0.0.1:6333
AUTOFLOW_AI_TIMEOUT_SECONDS=8
AUTOFLOW_AI_ENABLED=true
```

If `AUTOFLOW_AI_ENABLED=false`, the code path must use deterministic fallback
without attempting network calls. This gives local tests and constrained worker
environments a stable mode.

### 8. Error Handling And Observability

Every optional AI dependency should return structured warnings instead of
blocking the run:

- `embedding_relevance_unavailable`
- `recent_clip_usage_unavailable`
- `recent_clip_usage_write_failed`
- `metadata_llm_unavailable`
- `metadata_llm_invalid_json`
- `metadata_llm_ungrounded_claims_removed`
- `platform_pacing_relaxed`

Warnings should be persisted where AutoFlow already stores plan or run
diagnostics. They should also be returned in API responses so the UI can show
why a plan used fallback behavior.

The implementation should log exception details server-side, but API-visible
warnings should avoid leaking credentials, full prompts, or raw provider
responses.

### 9. Tests And Verification

Add focused tests rather than relying on live infra:

- `test_clip_ranker_uses_embedding_relevance_when_available`
- `test_clip_ranker_falls_back_when_embedding_service_fails`
- `test_clip_ranker_penalizes_recently_used_asset_ids`
- `test_clip_ranker_uses_visual_face_scene_and_brightness_signals`
- `test_metadata_generator_uses_clip_facts_for_title_candidates`
- `test_metadata_generator_falls_back_without_unverifiable_claims`
- `test_metadata_generator_rejects_ungrounded_thumbnail_text`
- `test_platform_profile_merges_short_form_constraints`
- `test_storyboard_fit_uses_platform_hook_and_clamps`
- migration test or model smoke test for `autoflow_used_clips`
- service-level tests for recent-use read/write behavior

Required project checks after implementation:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Frontend checks are only required if warnings or metadata UI display changes in
this phase:

```bash
cd frontend
npm install
npm run build
npm run lint || true
```

## Rollout Plan

Implement in four commits:

1. Add platform profiles and storyboard pacing tests.
2. Add recent-use persistence and ranker scoring changes.
3. Add embedding relevance service with deterministic fallback.
4. Add material-aware metadata generation with LLM gateway fallback.

Each commit should leave AutoFlow usable without live LLM, embedding, or Qdrant
services. The final verification should run the backend test suite and targeted
AutoFlow tests.

## Open Decisions Resolved

- The selected priority is P1 content quality, not the remaining P2/P3 system
  items.
- AI infrastructure may be used, but it must be optional and fallback-safe.
- Existing local Constructure `llm-gateway` and shared Qdrant are acceptable
  dependencies for enhanced behavior, but code must not assume they are healthy.
- Recent-use dedupe is a scoring penalty, not a hard exclusion.
- Metadata generation must be grounded in selected clip facts.
