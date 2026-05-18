# Artifact Cache And Aspect Ratio Design

Date: 2026-05-17

## Summary

VideoProcess should follow the AutoFlow content quality pass with a focused P2
system efficiency and strategy pass. This phase reduces repeated deterministic
media work and makes `concat_many` aspect-ratio behavior explicit without
changing platform publishing, external search, or AI generation semantics.

The selected approach is conservative:

1. Cache only deterministic transform nodes through an allowlist.
2. Reuse cached intermediate artifacts in the orchestrator before Redis worker
   dispatch.
3. Write cache entries only after a worker-created artifact is committed and
   known to exist.
4. Add `aspect_ratio` as a first-class `concat_many` config while preserving
   existing `width` and `height` behavior.
5. Keep P3 quality-system work as a roadmap, not part of this implementation.

## Goals

- Avoid recomputing the same deterministic media transform when input artifacts
  and node config are unchanged.
- Keep cache hits invisible to downstream nodes by reusing a normal artifact id.
- Make cache behavior safe by default and opt-out through `disable_cache=true`.
- Keep non-deterministic, external, platform, and side-effecting nodes out of
  cache.
- Let `concat_many` infer output dimensions from `aspect_ratio` when explicit
  dimensions are absent.
- Preserve existing pipeline-definition compatibility and existing `width` /
  `height` node configs.
- Add focused regression tests for cache key stability, cache hit/miss behavior,
  cache invalidation, and aspect-ratio sizing.

## Non-Goals

- Do not cache external search, external download, upload, publishing, LLM, or
  video-generation nodes.
- Do not implement stream-copy graph optimization in this phase.
- Do not implement VMAF QA gates, two-pass loudnorm, MediaPipe composition
  scoring, diarization, batched CLIP scoring, or LLM rerank.
- Do not change final artifact delivery semantics already implemented in the
  prior media quality phase.
- Do not make cache cleanup or retention a complex scheduler. A simple stale
  entry guard is enough for this phase.
- Do not change public publication review policy.

## Current State

Current code already includes some items from the original review:

- `JobEngine._maybe_finalize_job()` treats failed or skipped leaf nodes as job
  failure and only promotes successful leaf outputs to final artifacts.
- `concat_many` uses 48 kHz silent audio and intermediate video encode helpers.
- AutoFlow `PipelineBuilder` already passes `width`, `height`, and
  `aspect_ratio` into `montage_assembler`.
- AutoFlow platform pacing profiles were added in the P1 content quality phase.

Remaining P2 gaps:

- `JobEngine._resolve_source_nodes()` creates a new source artifact every run.
- Worker-created artifacts are always new records; identical deterministic
  transforms are recomputed and re-stored.
- There is no `intermediate_artifact_cache` table or service.
- `concat_many` node registry does not expose `aspect_ratio`, and the worker
  defaults to `1080x1920` when `width` / `height` are absent.
- Pipeline-generated `concat_many` fallback writes dimensions, but standalone
  node usage and planner-generated graphs still depend on node defaults.

## Design

### 1. Cache Scope

Use a strict allowlist of deterministic transform node types:

- `trim`
- `transcode`
- `vertical_crop`
- `concat_many`
- `montage_assembler`
- `subtitle`
- `title_overlay`
- `bgm`
- `watermark`
- `concat_timeline`
- `concat_vertical_timeline`

The allowlist can be extended later when a node has a stable deterministic
contract. Nodes are cache-ineligible when any of these conditions is true:

- `node_config.disable_cache` is truthy.
- The node type is not in the allowlist.
- The node has no input artifacts.
- Any input artifact cannot be loaded.
- A cache entry points to a missing artifact.
- A cache entry points to an artifact whose storage object no longer exists
  when local storage can cheaply verify it.
- The config contains transient or internal fields only usable during worker
  execution.

Source nodes are not part of the transform cache in this phase. Source artifact
dedupe can be added later as a separate source-resolution optimization.

### 2. Cache Table

Add `intermediate_artifact_cache`:

```text
id: uuid primary key
cache_key: string unique index
node_type: string index
node_config_hash: string
input_signature_hash: string
output_artifact_id: uuid foreign key artifacts.id on delete cascade
created_at: datetime
last_used_at: datetime
hit_count: integer
metadata_json: json
```

`metadata_json` should include enough debugging context to understand a cache
entry without recomputing the key:

```json
{
  "cache_schema_version": 1,
  "node_id": "trim_1",
  "input_artifact_ids": ["..."],
  "config_keys": ["duration", "start_time"],
  "created_by_job_id": "..."
}
```

### 3. Cache Key

The key is a SHA-256 digest over stable JSON:

```json
{
  "schema_version": 1,
  "node_type": "trim",
  "node_config": {},
  "inputs": [
    {
      "handle": "input",
      "artifact_id": "...",
      "storage_backend": "local",
      "storage_path": "artifacts/source.mp4",
      "file_size": 12345,
      "media_info_hash": "..."
    }
  ]
}
```

Rules:

- Sort JSON keys.
- Preserve input order by target handle. For dynamic video inputs this means
  `video_1`, `video_2`, `video_3`, and so on.
- Exclude worker-internal config keys beginning with `_`.
- Exclude known transient keys such as `disable_cache`, `cache_key`,
  `retry_count`, `worker_id`, timestamps, and debug-only fields.
- Include a `schema_version` so future codec or filter changes can invalidate
  old entries globally.
- Hash `media_info` through sorted JSON so changes in source metadata invalidate
  transforms that depend on dimensions, duration, streams, or color data.

This intentionally includes artifact identity and storage signature. It avoids
unsafe reuse when two artifacts share a path-like value but differ in metadata
or storage contents.

### 4. Orchestrator Flow

Before dispatching a ready node, `JobEngine._dispatch_ready_nodes()` should:

1. Resolve input artifact ids from upstream successful executions.
2. Ask `IntermediateArtifactCacheService` whether this node is cache eligible.
3. If not eligible, dispatch normally.
4. If eligible, compute the cache key and look up a valid entry.
5. On cache hit:
   - mark the current `NodeExecution` as `SUCCEEDED`;
   - set `output_artifact_id` to the cached artifact id;
   - set progress to `100`;
   - record `cache_hit=true` and cache key in a suitable execution metadata
     surface if one exists, otherwise only in logs;
   - increment `hit_count` and update `last_used_at`;
   - do not push a Redis task.
6. On cache miss, dispatch normally and include the computed cache key in the
   task payload.

After a worker completes a cache-eligible node, the orchestrator handles the
`node_completed` event and writes the cache entry:

1. Load node execution, input artifact ids, node config, and output artifact.
2. Recompute the cache key from persisted state.
3. Upsert `intermediate_artifact_cache`.
4. Do not fail the job if cache write fails; log a warning and continue.

Recomputing the key after completion prevents trusting task payload state alone.
The task payload can carry a cache key as an optimization, but persisted DB
state is authoritative.

### 5. Artifact Lifetime

Cache entries point at existing `artifacts` rows. This phase does not duplicate
files. Reuse means multiple node executions can reference the same artifact id.

Cleanup considerations:

- `output_artifact_id` foreign key should use `ondelete=CASCADE` so deleting an
  artifact removes its cache entry.
- Artifact cleanup code should avoid deleting cached artifacts that are still
  referenced by cache entries unless the cleanup operation explicitly asks to
  clear cache.
- On lookup, a cache entry with a missing artifact is a miss and should be
  removed.

### 6. `concat_many` Aspect Ratio

Add `aspect_ratio` to `backend/app/node_registry/builtin/concat_many.py`:

```text
name: "aspect_ratio"
type: select
default: "9:16"
options: ["9:16", "16:9", "1:1", "auto"]
```

Worker sizing rules in `ConcatManyHandler`:

1. If `width` and `height` are explicitly present, use them.
2. Otherwise infer from `aspect_ratio`:
   - `9:16` -> `1080x1920`
   - `16:9` -> `1920x1080`
   - `1:1` -> `1080x1080`
3. If `aspect_ratio="auto"`, read the first input artifact metadata from
   `_input_artifact_meta` and infer:
   - width > height -> `1920x1080`
   - height > width -> `1080x1920`
   - width == height -> `1080x1080`
4. If metadata is missing or invalid, fallback to `1080x1920`.

AutoFlow `PipelineBuilder` should keep writing explicit dimensions for
deterministic outputs, and also include `aspect_ratio` when building a
`concat_many` fallback node. This keeps old workflows stable while making new
planner-created or manual nodes more self-describing.

### 7. Error Handling

Cache must never be required for correct execution:

- Cache lookup failure is treated as miss.
- Cache write failure is logged but does not fail the node or job.
- Invalid cache rows are deleted or ignored.
- A cache hit is only accepted when the output artifact exists and is loadable
  enough for downstream nodes.
- `disable_cache=true` always bypasses lookup and write.

### 8. Tests

Add tests for cache behavior:

- Cache key is stable when config key order changes.
- Cache key changes when input artifact, node type, config, or media info
  changes.
- Deterministic node cache miss dispatches a Redis task.
- Deterministic node cache hit marks node `SUCCEEDED` and does not dispatch.
- Non-allowlisted nodes never hit or write cache.
- Cache entry with missing artifact is treated as miss.
- Worker completion writes cache entry for allowlisted node.
- `disable_cache=true` bypasses lookup and write.

Add tests for `concat_many`:

- `aspect_ratio=16:9` without width/height produces `1920x1080` scale/pad.
- `aspect_ratio=1:1` without width/height produces `1080x1080`.
- Explicit width/height override `aspect_ratio`.
- `aspect_ratio=auto` uses first input artifact metadata when available.

Required project checks after implementation:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Frontend checks are not required unless implementation changes API response
shapes consumed by frontend.

## P3 Roadmap

The following are explicitly separate future specs:

- Two-pass loudnorm for final audio export and generated speech.
- VMAF QA gate for final exports with automatic quality retry.
- MediaPipe subject/framing analysis as ranker input.
- Whisper diarization and large-v3 deployment policy.
- Batched CLIP scoring and reranker support for smart trim.
- LLM rerank for smart trim text scoring.

These need dependency checks, runtime budgets, and separate verification plans.

## Open Decisions Resolved

- P2 cache uses the conservative deterministic transform allowlist.
- Source node dedupe is not part of this implementation.
- Non-deterministic and side-effecting nodes are not cached.
- P3 is documented only as roadmap in this spec.
