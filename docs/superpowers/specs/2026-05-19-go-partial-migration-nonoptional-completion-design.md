# Go Partial Migration Non-Optional Completion Design

Status: Draft for user review
Date: 2026-05-19
Repo: `/home/taiwei/.codex/worktrees/2562/videoprocess`
Governing baseline: `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`

## Summary

The original Go partial migration spec is not fully implemented yet. The completed work covers the Go read-only API foundation, the `trim` Go worker cutover, and the worker production contract needed for one routed Go node. The remaining non-optional scope is:

- Go node registry parity.
- Go validator parity and production guard.
- Phase 4 first-wave pure ffmpeg node migration.
- Phase 5 selective Go API writes.
- Observability and production cutover acceptance.

This design deliberately excludes Phase 6, the optional Go orchestrator slice. Python remains the authoritative orchestrator and event listener.

## Current Completion State

Completed or materially complete:

- Go sidecars in Docker Compose.
- Go API `/health`, `/readyz`, read list endpoints, detail endpoints, and real schedule status.
- Production fail-closed stub-store behavior.
- Go worker task runtime adapter.
- Go `trim` path-level handler and `ffmpeg_go` registration.
- Artifact row creation and non-empty `output_artifact_id`.
- PEL reclaim, heartbeat, host affinity, `WORKER_CONCURRENCY`, graceful shutdown, and during-execution cancellation.
- Strict API read parity and Go trim mixed-mode smoke.

Not complete:

- Go registry still covers only a small hand-written subset.
- Go validator is not production-ready for all migrated write paths.
- First-wave pure ffmpeg nodes beyond `trim` are not migrated.
- Go API write routes are not implemented.
- Metrics and production acceptance drills are not complete.

## Completion Boundary

This completion milestone implements only the non-optional portions of the original spec.

In scope:

- Registry parity for Python builtin node types.
- Validator parity for deterministic workflow graphs.
- Explicit unsupported or fallback behavior for AutoFlow/dynamic/LLM/platform-heavy graph shapes.
- Pure ffmpeg worker migration for the first-wave node list in the original spec.
- Selective Go API writes after read parity and validator guards are in place.
- Metrics, logs, staging jobs, p95 comparison, failure/retry/cancel drills, and rollback drills.

Out of scope:

- Go orchestrator/event listener/recovery ownership.
- Go AutoFlow planner rewrite.
- Go ML/ASR/TTS/search/platform publish handlers.
- Alembic replacement or schema ownership changes.
- Frontend migration.
- Python code deletion.
- Default public publication behavior changes.

Ownership remains:

```text
Python API          remains fallback and owns Python-only routes
Python orchestrator owns scheduling, retry, downstream state, final artifact state
Python event loop   owns vp:events consumption
Python Alembic      owns schema
Go API              owns selected read/write HTTP routes after parity gates
Go worker           owns selected pure ffmpeg node execution on vp:tasks:ffmpeg_go
```

## Registry Parity

The Go registry must stop being a manually maintained small subset. The Python builtin registry should be the authoring source.

Design:

- Add a Python manifest exporter that serializes builtin node definitions to stable JSON.
- Commit the generated manifest as a contract artifact.
- Go loads or generates registry definitions from that manifest.
- Go `/api/v1/node-types` returns all builtin node types with Python-compatible type set and response shape.
- Strict parity tests compare Python and Go registry output.

Required parity fields:

- `type_name`
- `display_name`
- `category`
- `inputs`
- `outputs`
- `worker_type`
- port names, types, and required flags

Rules:

- Python registry remains the live dispatch source.
- Go registry worker types are API and validation reference data, not independent scheduling ownership.
- Registry generation must not mutate Python definitions.
- Missing or incompatible registry entries fail strict parity tests.

## Validator Parity

Go `POST /api/v1/pipelines/validate` can become production-eligible only after deterministic graph parity is proven.

Supported production validation:

- valid source to transform to export graph
- unknown node type
- invalid edge source or target
- port type mismatch
- duplicate input port
- cycle detection
- missing required input
- missing source asset binding

Unsupported or fallback validation:

- AutoFlow-generated dynamic graph shapes not covered by fixtures
- `zip_records`
- platform search nodes
- material search/vector-backed nodes
- LLM-heavy planner paths
- Python-only external platform publishing nodes

Behavior:

- For supported graphs, Go returns a Python-compatible `ValidationResult`.
- For unsupported graphs, Go must return an explicit unsupported/fallback response or the route must stay on Python.
- Go must not silently accept graphs it cannot fully validate.
- Pipeline create/update writes in Phase 5 must call validated Go validation only for supported graphs; unsupported graph writes stay routed to Python.

## Phase 4 Pure Ffmpeg Node Migration

The first-wave pure ffmpeg nodes are migrated one node at a time through `ffmpeg_go`.

Batch 4A, simple single-input transforms:

- `transcode`
- `export`
- `vertical_crop`
- `watermark`
- `title_overlay`

Batch 4B, audio/video composition:

- `bgm`
- `replace_audio`
- `concat_horizontal`
- `concat_vertical`
- `concat_many`

Batch 4C, timeline/layout heavy:

- `concat_timeline`
- `concat_vertical_timeline`
- `montage_assembler`

Per-node gate:

- Audit Python handler contract.
- Freeze parameters, defaults, output extension, media metadata, and failure semantics.
- Implement a Go path-level handler.
- Add exact-argument tests.
- Add media fixture tests with `ffprobe`.
- Add runtime artifact/storage/event tests.
- Add a mixed-mode pipeline test.
- Switch that node's Python registry `worker_type` to `ffmpeg_go`.
- Run rollback drill by switching it back to `ffmpeg`.

Runtime changes needed before multi-input nodes:

- Extend artifact resolution beyond the single `"input"` port.
- Preserve port-name to path mapping for handlers.
- Keep artifact/runtime logic in the shared `MediaTaskHandler` adapter.
- Keep path-level handlers free of Redis and DB ownership.

Special case:

- `export` must preserve Python terminal-output semantics. It cannot be reduced to a generic transcode if Python also writes a terminal export file or metadata used by downstream UI/API behavior.

Nodes that stay Python:

- `smart_trim`
- `speech_to_subtitle`
- `subtitle_translate`
- `subtitle_to_speech`
- `url_download`
- `material_library_ingest`
- search/material/platform nodes
- external upload/publish nodes

## Phase 5 Selective Go API Writes

Go API writes are migrated only after registry and validator guards are in place. Python schema and Alembic remain authoritative.

Batch 5A, validation and pipeline CRUD:

- `POST /api/v1/pipelines/validate`
- `POST /api/v1/pipelines`
- `PUT /api/v1/pipelines/{id}`
- `DELETE /api/v1/pipelines/{id}`
- `POST /api/v1/pipelines/{id}/duplicate`

Batch 5B, job lifecycle:

- `POST /api/v1/jobs`
- `POST /api/v1/jobs/batch`
- `POST /api/v1/jobs/{id}/cancel`
- `POST /api/v1/jobs/{id}/rerun`
- `DELETE /api/v1/jobs/{id}`

Batch 5C, asset/artifact/schedule operations:

- `POST /api/v1/assets/upload`
- `GET /api/v1/assets/{id}/download`
- `DELETE /api/v1/assets/{id}`
- `GET /api/v1/artifacts/{id}/download`
- `DELETE /api/v1/artifacts/cleanup`
- `POST /internal/schedule/video/open`
- `POST /internal/schedule/video/drain`
- `POST /internal/schedule/video/close`

Rules:

- Use explicit SQL and existing storage backends.
- Do not add Go migrations.
- Preserve Python response JSON and error shapes.
- Preserve deletion constraints and conflict behavior.
- Keep external publishing routes out of this phase.
- Keep public publishing defaults unchanged.

Job start handoff:

`POST /jobs` is the riskiest write route because Python currently creates a job and invokes Python-owned start/defer behavior. Since Phase 6 is excluded, Go must not partially implement the orchestrator.

Preferred handoff:

- Go creates the job records and emits a Python-owned start-job signal through an existing queue/outbox or a small explicitly owned handoff endpoint.

Acceptable interim:

- Keep `POST /jobs` and job batch/rerun routed to Python until a stable handoff exists.

Not acceptable:

- Go directly schedules the DAG using a partial orchestrator.

## Observability

Go API should expose `/metrics` with at least:

- `http_requests_total`
- `http_request_duration_seconds`
- `http_request_errors_total`

Go worker should expose metrics or write scrapeable counters for:

- `vp_worker_tasks_total`
- `vp_worker_task_duration_seconds`
- `vp_worker_task_failures_total`
- `vp_worker_task_cancellations_total`
- `vp_worker_pending_reclaims_total`
- `vp_worker_heartbeat_failures_total`
- `vp_ffmpeg_runs_total`
- `vp_ffmpeg_failures_total`
- `vp_ffmpeg_gpu_fallbacks_total`

Logs should consistently include:

- `service`
- `version`
- `worker_type`
- `worker_id`
- `job_id`
- `node_execution_id`
- `node_id`
- `node_type`
- `request_id`
- `redis_msg_id`

## Acceptance Gates

Required local checks:

```bash
go test ./...
go vet ./...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Frontend checks are required only if frontend files change.

Strict parity gates:

- API read parity.
- API write parity for migrated endpoints.
- Registry parity.
- Validator parity.
- Worker mixed-mode parity per migrated node.

Production cutover gates:

- At least 20 staging jobs per migrated node.
- Redis pending does not grow after jobs finish.
- No missing `output_artifact_id` events.
- No artifact rows with missing `storage_path`.
- p95 runtime is not worse than Python by more than 20 percent unless explicitly accepted.
- Failure and retry live drill passes.
- Cancellation live drill passes.
- Rollback drill passes for every migrated node and route batch.

Docker gates:

- Rebuild `api-go` and `ffmpeg-worker-go`.
- Verify running binaries.
- Verify `/health`, `/readyz`, and `/metrics`.
- Run a mixed-mode pipeline covering the maximum available pure ffmpeg Go nodes.

## Rollout And Rollback

Rollout is independent per node and per endpoint.

Worker rollout:

- Change one Python registry `worker_type` at a time to `ffmpeg_go`.
- Keep Python `ffmpeg-worker` running.
- Verify mixed-mode success and rollback before moving to the next node.

Worker rollback:

- Change that node's `worker_type` back to `ffmpeg`.
- Stop or ignore the Go worker for that stream.
- No DB restore is required.

API rollout:

- Route one endpoint or endpoint batch to Go only after parity gates pass.
- Keep Python API available for fallback.
- Do not route the whole `/api/v1` namespace to Go during this milestone.

API rollback:

- Route the endpoint back to Python.
- No schema or DB restore is required.

## Completion Definition

The original non-optional Go partial migration spec is complete when:

- Go node registry parity is strict-test clean.
- Go validator handles supported deterministic graphs and refuses or falls back for unsupported shapes.
- All first-wave pure ffmpeg nodes listed in Phase 4 are migrated through `ffmpeg_go` with per-node rollback drills.
- Selected Go API writes are implemented or explicitly left on Python where Phase 6 ownership would otherwise be required.
- Observability metrics and structured logs exist.
- Production acceptance evidence is recorded for every migrated node and route batch.
- Python remains a functioning reference and rollback path.

