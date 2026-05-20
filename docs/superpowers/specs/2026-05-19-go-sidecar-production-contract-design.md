# Go Sidecar Production Contract Design

Status: Draft for user review
Date: 2026-05-19
Repo: `/home/taiwei/.codex/worktrees/2562/videoprocess`
Governing baseline: `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`

## Summary

The next Go migration milestone is not to migrate more node types. It is to close the gap between the current working Go sidecar smoke path and the original migration spec's production cutover requirements.

The milestone is:

```text
Go sidecar production contract completion before expanding nodes.
```

This milestone completes the remaining Go API Phase 1 read-only parity work and the Go worker Phase 3 production contract work. `trim` remains the only Go-routed worker node and becomes the validation target for production semantics.

## Current State

The branch already has meaningful Go migration progress:

- `api-go` and `ffmpeg-worker-go` are wired in Docker Compose.
- Go API has `/health`, `/readyz`, list endpoints, middleware, and fail-closed stub-store behavior.
- Go worker has a Redis Streams consumer, task runtime adapter, storage resolution, artifact creation, and `trim` execution.
- Python `trim` registry is currently routed to `ffmpeg_go`.
- A mixed-mode smoke generated a real media artifact through Go `trim` and Python downstream ffmpeg nodes.

The remaining risk is that smoke success can be mistaken for production readiness. The original spec still requires detail read parity, schedule-state safety, PEL reclaim, heartbeat, affinity, concurrency, graceful shutdown, cancellation parity, retry behavior, and stronger cutover gates.

## Migration Boundary

This milestone freezes new node migration.

In scope:

- Complete Go API read-only Phase 1 gaps.
- Complete Go worker production semantics for the existing `trim` cutover.
- Add parity and mixed-mode tests that prove contract behavior.
- Keep Python API, orchestrator, event listener, and worker as reference and rollback paths.

Out of scope:

- Migrating `transcode`, `export`, `vertical_crop`, `watermark`, or any other new node type.
- Replacing Python API globally.
- Moving API write routes to Go.
- Moving AutoFlow, LLM, materials, external platform publishing, or scheduler ownership to Go.
- Deleting old Python code.
- Replacing Python orchestrator or event listener.

Ownership remains:

```text
Python API          owns write APIs, AutoFlow, LLM/material/platform routes
Python orchestrator owns scheduling, retries, job state transitions
Python event loop   owns worker event consumption
Python worker       remains reference and rollback implementation
Go API              owns selected read-only parity sidecar routes
Go worker           owns only selected ffmpeg_go tasks, currently trim
```

## Go API Completion

The Go API work is limited to read-only parity and safe schedule behavior.

Routes to add or complete:

```text
GET /api/v1/pipelines/{pipeline_id}
GET /api/v1/assets/{asset_id}
GET /api/v1/artifacts/{artifact_id}
GET /api/v1/jobs/{job_id}
GET /internal/schedule/video/status
```

Implementation rules:

- Keep DB access in `internal/store/*` with explicit SQL.
- Mirror Python response schema names and JSON shapes.
- Use FastAPI-compatible errors for migrated API routes, especially `{"detail": "..."}`.
- Return 404 for unknown detail IDs in the same shape as Python.
- Preserve existing pagination defaults and limits for list endpoints.
- Keep `store == nil && allowStubStore == false` fail-closed for all migrated routes.
- Do not return empty pages or fake objects when production dependencies are unavailable.

Schedule status must stop returning a fixed `OPEN`.

Acceptable outcomes:

- Preferred: read the same durable schedule state source Python uses.
- Acceptable interim: return explicit unsupported or unavailable status in production and keep production routing on Python.
- Not acceptable: silently report `OPEN` when the real system may be drained or closed.

`POST /api/v1/pipelines/validate` remains non-production in this milestone. Go validator may be tested, but it must not replace Python validation for AutoFlow or unsupported graph shapes.

## Go Worker Production Contract

The Go worker work targets production semantics for `trim` only.

Required behavior:

- Consume only `vp:tasks:ffmpeg_go`.
- Never consume the Python `vp:tasks:ffmpeg` stream.
- Continue emitting events to `vp:events` for the Python event listener.
- Publish `node_completed` only after creating a real artifact row and obtaining a non-empty `output_artifact_id`.
- Publish `node_failed` and ack for confirmed handler failures.
- Do not ack when event publication cannot be confirmed.

Production features to add:

```text
PEL reclaim
Heartbeat for active tasks
Host affinity defer/bounce
WORKER_CONCURRENCY
Graceful shutdown with active-task timeout
During-execution cancellation watcher
Retry/failure parity with Python worker expectations
```

### PEL Reclaim

Go worker should reclaim stale pending entries on startup and periodically during runtime.

Config:

```text
WORKER_PEL_MIN_IDLE_MS=900000
WORKER_PEL_RECLAIM_INTERVAL_SECONDS=60
```

Reclaim must not steal fresh long-running tasks that are actively heartbeating.

### Heartbeat

Each active task should refresh its Redis pending idle time while ffmpeg is running.

Config:

```text
WORKER_HEARTBEAT_INTERVAL_SECONDS=15
```

Heartbeat warnings should be logged. If Redis state cannot support reliable event publication, the task must not be acknowledged as complete.

### Host Affinity

Go worker should implement Python-compatible `preferred_hosts` handling.

Config:

```text
WORKER_AFFINITY_WAIT_SECONDS=20
WORKER_AFFINITY_MAX_BOUNCES=6
```

If the current host is not preferred and the wait or bounce budget remains, the worker re-enqueues the task with incremented `affinity_bounces` and acks the current message. If budget is exhausted, it processes locally.

### Concurrency

Go worker should honor:

```text
WORKER_CONCURRENCY
```

Default remains `2`, matching Python. Implementation must use a bounded worker pool or semaphore. Each in-flight task owns its own heartbeat and cancellation scope. No unbounded goroutine spawning is allowed.

### Graceful Shutdown

On SIGTERM or context cancellation:

- Stop claiming new tasks.
- Let active tasks finish until a configured timeout.
- Ack tasks only after completion or confirmed cancellation.
- Leave unknown-state active tasks pending for reclaim.

### Cancellation

Confirmed cancellation keeps the current desired contract:

```text
ack task
emit no node_completed event
emit no node_failed event
cleanup temp files
```

The missing part is during-execution cancellation. Go worker must periodically reload job/node cancellation state and cancel the active ffmpeg context when cancellation is detected.

## Testing And Gates

Required local commands:

```bash
go test ./...
go vet ./...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Frontend is unchanged by this milestone, but if frontend files change, run the AGENTS-required frontend checks.

Go API tests:

- Detail route success for pipeline, asset, artifact, and job.
- Detail route unknown-ID behavior.
- Fail-closed behavior when store is unavailable and stub mode is disabled.
- Schedule route never returns a fake fixed `OPEN` in production mode.
- Go/Python parity for selected read endpoints under `VP_GO_PARITY_STRICT=1`.

Go worker tests:

- PEL reclaim claims stale pending tasks.
- Heartbeat prevents active tasks from being reclaimed as stale.
- Host affinity re-enqueues and acks within budget.
- Host affinity processes locally after budget exhaustion.
- `WORKER_CONCURRENCY` limits active tasks.
- Graceful shutdown stops new claims and handles active tasks correctly.
- During-execution cancellation kills ffmpeg, cleans temp files, acks, and emits no event.
- Handler failure emits `node_failed` and acks.
- Event publication failure leaves the task pending.

Mixed-mode tests:

```text
Python API creates job
Python orchestrator dispatches trim to ffmpeg_go
Go worker executes trim
Go worker emits node_completed with output_artifact_id
Python event listener marks node SUCCEEDED
Python orchestrator dispatches downstream or finalizes
Python API returns SUCCEEDED
Redis pending does not grow after completion
```

## Docker Verification

After implementation, rebuild and test at least:

```text
api-go
ffmpeg-worker-go
api
ffmpeg-worker
redis
postgres
minio
```

Verification:

- `api-go /health` returns `{"status":"ok"}`.
- `api-go /readyz` reports required dependencies.
- Go API detail endpoints match Python for the same live records.
- `ffmpeg-worker-go` process is the Go binary.
- `ffmpeg-worker-go` consumes `vp:tasks:ffmpeg_go`.
- `trim` output artifact has non-empty ID, storage path, and media file.
- Redis `XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers` returns zero after the smoke finishes.

YouTube upload is not part of this milestone. External publication remains private or unlisted by default and requires explicit human review for public publishing.

## Rollout

Rollout stays sidecar-first:

- Keep Python services running.
- Keep `trim` routed to `ffmpeg_go` only after contract tests pass.
- Do not route frontend globally to Go API.
- Use side-port parity or explicit route splitting for Go API read routes.
- Keep schedule status on Python if Go cannot read the real schedule state safely.

This milestone should produce evidence that `trim` is safe as a Go-routed node before any next node is migrated.

## Rollback

Worker rollback:

```text
backend/app/node_registry/builtin/trim.py
worker_type = "ffmpeg_go" -> "ffmpeg"
stop ffmpeg-worker-go
keep Python ffmpeg-worker running
```

API rollback:

```text
stop routing selected read endpoints to api-go
keep Python API serving /api/v1
```

Rollback must not require DB restore, artifact deletion, Redis stream deletion, or Python code removal.

## Acceptance Criteria

The milestone is complete when:

- Go API Phase 1 read detail endpoints are implemented or explicitly kept on Python where unsafe.
- Go schedule status no longer lies with a fixed `OPEN`.
- Go worker has PEL reclaim, heartbeat, affinity, concurrency, graceful shutdown, and during-execution cancellation.
- All new behavior has unit or mixed-mode tests.
- Strict Go/Python API parity tests pass for the selected read surface.
- Strict Go worker smoke passes with `trim` routed to `ffmpeg_go`.
- Docker verification proves the running services are the new Go binaries.
- Redis pending state is clean after smoke.
- Rollback remains a worker-type or route change, not a data restore.

After this milestone, a separate design can plan Phase 4 node expansion, beginning with the lowest-risk pure ffmpeg node after `trim`.
