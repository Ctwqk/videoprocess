# Go API And Ffmpeg Worker Migration Design

## Summary

VideoProcess will move the API control plane and the pure ffmpeg worker path from Python to Go through a strangler migration. The existing Python services remain the reference implementation and rollback path until Go parity is proven by contract tests, Docker smoke tests, and visible media-output verification.

The first migration wave builds Go sidecar services rather than replacing containers in place:

- `vp-api-go`: a Go API service that progressively implements the existing HTTP contract.
- `vp-ffmpeg-worker-go`: a Go Redis Streams worker for pure media-transform nodes.
- Python remains active for `vision-worker`, ASR, TTS-heavy nodes, and any API groups not yet proven equivalent.

This design intentionally keeps the existing Postgres schema, Alembic migrations, Redis stream payloads, storage paths, frontend API shape, AutoFlow safety policy, and publication privacy rules.

## Goals

- Reimplement the API layer in Go without changing frontend routes, response shapes, or database ownership semantics.
- Reimplement the ffmpeg worker runner and pure ffmpeg handlers in Go while preserving Redis Streams task/event contracts.
- Keep the migration reversible at every phase.
- Preserve all AutoFlow constraints: generated workflows must pass `validate_pipeline()`, LLM output cannot directly define arbitrary executable graphs, and external/public publishing remains review-gated.
- Improve operational behavior where Go is likely to help: long-running service memory footprint, worker cancellation, process supervision, Redis stream handling, and API concurrency.

## Non-Goals

- No Postgres schema redesign in the first migration wave.
- No replacement of Alembic with a Go migration system during the first migration wave.
- No Go rewrite of `smart_trim`, `speech_to_subtitle`, or GPU/CTranslate2/faster-whisper execution in the first migration wave.
- No Go rewrite of XTTS, MiniMax image generation, or LLM prompt rewriting in the first migration wave.
- No direct public publishing behavior change.
- No frontend route or API base URL change.

## Current System Boundaries

The current backend is a Python FastAPI application under `backend/app/` with service, model, schema, AutoFlow, node registry, and orchestrator modules. The ffmpeg and vision workers share `backend/worker/main.py` and dispatch node-specific behavior through `backend/worker/handlers/`.

Important current contracts:

- Frontend calls `/api/v1` through `frontend/src/api/client.ts`.
- API routes live in `backend/app/api/`.
- Pipeline JSON is shaped by `backend/app/schemas/pipeline.py`.
- AutoFlow request/plan/run JSON is shaped by `backend/app/schemas/autoflow.py`.
- Node registry definitions live in `backend/app/node_registry/builtin/`.
- Orchestrator dispatches to Redis Streams using `vp:tasks:{worker_type}`.
- Workers report completion/failure through `vp:events`.
- Artifacts are recorded in Postgres and stored under existing local or MinIO paths.
- Docker currently exposes `api`, `ffmpeg-worker`, and `vision-worker`; GPU support is layered through `docker-compose.gpu.yml`.

## Target Architecture

The Go code will live beside the Python code:

```text
go.mod
cmd/vp-api/main.go
cmd/vp-ffmpeg-worker/main.go
internal/config/
internal/contracts/
internal/httpapi/
internal/store/
internal/redisstream/
internal/storage/
internal/pipeline/
internal/orchestrator/
internal/worker/
internal/worker/ffmpeg/
internal/worker/handlers/
```

The service split is:

- `cmd/vp-api`: HTTP API, route wiring, startup recovery, event listener, and job dispatch.
- `cmd/vp-ffmpeg-worker`: Redis Streams consumer group, task claim/reclaim, cancellation watcher, ffmpeg process execution, artifact creation, and event publishing.
- `internal/contracts`: Go structs that mirror existing JSON contracts and DB enum strings.
- `internal/store`: Postgres access with explicit query methods. It uses the existing schema and enum values.
- `internal/storage`: local filesystem and MinIO storage implementations with the same storage path semantics as Python.
- `internal/pipeline`: node registry, pipeline validation, topological sorting, dynamic input validation, and capability manifest.
- `internal/worker/handlers`: pure ffmpeg handlers.

Python remains authoritative for behavior that depends on Python-native ML/media libraries until Go parity is separately designed.

## API Migration Scope

The API layer will be migrated in batches.

Batch 1: API shell and read-only surfaces

- `GET /health`
- `GET /api/v1/node-types`
- `GET /api/v1/node-types/{type_name}`
- `GET /api/v1/pipelines`
- `GET /api/v1/templates`
- `GET /api/v1/pipelines/{pipeline_id}`
- `GET /api/v1/assets`
- `GET /api/v1/assets/{asset_id}`
- `GET /api/v1/artifacts/{artifact_id}`
- `GET /api/v1/jobs`
- `GET /api/v1/jobs/{job_id}`
- `GET /internal/schedule/video/status`

Batch 2: core writes and orchestration

- pipeline create/update/delete/duplicate
- pipeline validation
- template execution and batch execution
- asset upload/delete/download
- artifact download/cleanup
- job create/cancel/rerun/delete
- schedule open/drain/close
- startup stale-job recovery
- Redis event listener
- job dispatch

Batch 3: AutoFlow deterministic surfaces

- AutoFlow request schema
- intent parsing
- template selection
- material selection over existing DB/search clients
- deterministic pipeline builder
- validation repair
- rights policy
- plan persistence
- execute path
- capability manifest

Batch 4: AutoFlow graph planner and external planner surfaces

- `POST /api/v1/autoflow/plan/graph`
- LLM gateway call through `EXO_WATCHDOG_URL`
- policy repair and validation parity
- fallback to deterministic planner when graph planning is unavailable

Batch 5: Channel Agent

- Channel Agent routes and runner behavior are migrated after core API and AutoFlow are stable. It is not a first-cut dependency for Go ffmpeg worker cutover.

## Worker Migration Scope

The Go ffmpeg worker starts with a new worker type, `ffmpeg_go`, so it cannot accidentally consume existing Python worker tasks before parity is proven. Node registry entries can be switched one node at a time from `ffmpeg` to `ffmpeg_go`.

First wave pure ffmpeg/media handlers:

- `trim`
- `transcode`
- `vertical_crop`
- `concat_horizontal`
- `concat_vertical`
- `concat_many`
- `concat_timeline`
- `concat_vertical_timeline`
- `title_overlay`
- `watermark`
- `bgm`
- `replace_audio`
- `export`
- `montage_assembler`

Second wave network/platform handlers:

- `url_download`
- `material_library_ingest`
- `youtube_upload`
- `x_upload`
- `xiaohongshu_upload`

Retained in Python for the first migration wave:

- `smart_trim`, because it combines visual scoring, ASR scoring, and fallback media decisions.
- `speech_to_subtitle`, because it depends on `faster-whisper`, CUDA/CTranslate2, and Python-native ASR integration.
- `subtitle_translate`, because it is LLM/service dependent and not a pure ffmpeg operation.
- `subtitle_to_speech`, because it combines TTS fallback, LLM rewrite, SRT parsing, alignment, resynthesis, and ffmpeg mixing.

## Contract Preservation

The following contracts must not drift:

- HTTP path, method, status code, and JSON field names.
- Pydantic default equivalents, especially empty lists, empty dicts, `null`, and omitted optional values.
- DB enum strings for `JobStatus`, `NodeStatus`, and `ArtifactKind`.
- Redis stream names and payload keys.
- Artifact storage path format: `artifacts/{job_id}/{node_execution_id}.{ext}` unless a handler intentionally returns a cache/storage override.
- Upload privacy: default to `private` or `unlisted`.
- External source policy: external platform assets require explicit human review before publishing.
- `PipelineDefinition` compatibility with `backend/app/schemas/pipeline.py`.
- AutoFlow generated workflow validity under `validate_pipeline()`.

## Data Flow

API execution flow:

1. Frontend sends the existing `/api/v1` request.
2. Go API validates request JSON into `internal/contracts`.
3. Go API reads/writes the existing Postgres schema.
4. Go API validates pipeline definitions using Go parity code.
5. Go API creates `Job` and `NodeExecution` rows with the same status semantics as Python.
6. Go orchestrator resolves source nodes, computes dependencies, and dispatches ready nodes to Redis Streams.
7. Python and Go workers both publish completion/failure events to `vp:events`.
8. Go event listener updates job/node state and dispatches downstream nodes.

Worker execution flow:

1. Go worker consumes `vp:tasks:ffmpeg_go`.
2. It loads the node execution and job state from Postgres.
3. It resolves input artifacts to local file paths, downloading from MinIO when needed.
4. It creates an output path under the configured storage root.
5. It runs the handler with a cancellable context and ffmpeg process supervision.
6. It probes/records artifact metadata where the Python handler currently does so.
7. It stores the output locally or in MinIO.
8. It creates an artifact row.
9. It emits `node_completed` or `node_failed` to `vp:events`.

## Rollout Strategy

1. Run Go API on a side port or side container.
2. Run parity tests comparing Python and Go responses for stable fixtures.
3. Run Go ffmpeg worker on `ffmpeg_go`.
4. Switch one node registry entry at a time to `ffmpeg_go`.
5. Validate each switched node with media fixture tests and visible smoke output.
6. Switch frontend/API proxy to Go only after core API parity is proven.
7. Leave Python API and Python ffmpeg worker available for rollback until live jobs have completed under Go for a full video window.

Rollback is simple by design:

- route traffic back to Python API,
- change node registry worker types back to `ffmpeg`,
- stop `vp-ffmpeg-worker-go`,
- leave database and storage untouched.

## Verification

Required verification before replacing any Python service:

- Python backend tests still pass for unchanged contracts.
- Go unit tests pass.
- Golden JSON parity tests pass.
- Go API live smoke passes against Postgres, Redis, and storage.
- Go ffmpeg worker media fixture tests pass.
- Docker compose sidecar smoke passes.
- GPU compose smoke confirms ffmpeg sees hardware encoders where expected.
- A visible AutoFlow smoke produces non-placeholder media through a deterministic pipeline.

## Risks And Mitigations

Risk: Pydantic and Go JSON defaults differ.

Mitigation: Golden JSON parity tests for every migrated endpoint and schema.

Risk: Redis Streams pending-entry handling changes and strands jobs.

Mitigation: Keep worker type isolated as `ffmpeg_go` until reclaim, ack, cancel, and failure tests pass.

Risk: Go API writes rows that Python workers cannot read.

Mitigation: Use existing schema and enum strings. Run mixed-mode tests where Go API dispatches to Python worker and Python API observes Go-created rows.

Risk: ffmpeg argument generation drifts.

Mitigation: Add exact argument unit tests based on current Python handler behavior before moving each handler.

Risk: publication safety regresses.

Mitigation: Implement privacy and source-policy tests before enabling upload-capable routes or handlers.

Risk: AutoFlow graph planning bypasses deterministic safety.

Mitigation: Keep graph output as draft-only input. Compile through capability manifest, validate pipeline structure, apply policy repair, and require review for external source or public publishing.

## Acceptance Criteria

The migration design is complete when:

- Go sidecar services can be built without removing Python services.
- Go API can serve core read/write endpoints with response parity.
- Go orchestrator can dispatch and complete a simple deterministic pipeline.
- Go ffmpeg worker can execute first-wave pure media nodes through Redis Streams.
- Python vision/ASR/TTS paths still run unchanged.
- Existing frontend continues to work without changing `/api/v1`.
- A rollback to Python API and Python ffmpeg worker does not require a database restore.
