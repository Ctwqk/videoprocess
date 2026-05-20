# Go Orchestrator Phase 6 Design

> Status: Approved design draft
> Date: 2026-05-20
> Source spec: `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`
> Scope: Phase 6 Go orchestrator slice for Go-eligible pure ffmpeg jobs.

## Summary

Phase 6 makes `api-go` own job creation, scheduling, event listening, recovery, retry, downstream skip, and final artifact marking for a narrow class of jobs: pure first-wave ffmpeg pipelines that are fully eligible for Go execution.

This is not a full orchestrator replacement. Python remains authoritative for Python-created jobs, AutoFlow, ML/ASR/TTS/search/material/platform publish paths, Alembic schema migration ownership, and rollback for non-Go paths.

The selected approach is:

```text
api-go embeds a Go orchestrator.
api-go creates only Go-eligible jobs.
Go-owned tasks emit events to vp:events:go.
Go event listener consumes only vp:events:go.
No fallback or proxy to Python for non-eligible jobs.
```

## Non-Goals

- No AutoFlow planner Go rewrite.
- No LLM, ASR, TTS, search, material, or platform publishing orchestration in Go.
- No Python event listener replacement for Python-owned jobs.
- No shared event stream between Python and Go listeners.
- No fallback/proxy from Go API to Python API.
- No deletion of Python orchestrator, Python API, Python workers, or Alembic migrations.

## Architecture

`api-go` will start the HTTP server and, when enabled, background orchestration goroutines. The orchestration code must remain isolated from HTTP handlers:

```text
cmd/vp-api
  loads config and dependencies
  starts HTTP server
  starts Go orchestrator only when enabled

internal/httpapi
  POST /api/v1/jobs
  POST /api/v1/jobs/batch
  POST /api/v1/jobs/{id}/rerun
  validates request shape and delegates to job service

internal/orchestrator/engine
  start job
  resolve source nodes
  dispatch ready nodes
  handle completion/failure
  retry once
  skip downstream
  finalize jobs and leaf artifacts

internal/orchestrator/events
  consumer group orchestrator-go
  event stream vp:events:go
  reclaim pending events
  ack only after DB updates succeed

internal/orchestrator/recovery
  startup and periodic recovery for Go-owned jobs

internal/store
  owner-guarded job, node, artifact, schedule, and event helper queries
```

The implementation should favor small packages with explicit interfaces so the engine can be unit tested without an HTTP server or real Redis.

## Ownership Model

Add a first-class job owner field:

```text
jobs.orchestrator_owner = "python" | "go"
```

Rules:

- New Go-created jobs are stored with `orchestrator_owner='go'`.
- Existing rows and Python-created jobs default to `orchestrator_owner='python'`.
- The schema change is added through Python-owned Alembic migration, not Go migrations.
- Go orchestrator queries and updates must guard on `orchestrator_owner='go'`.
- Python orchestrator remains responsible for Python-owned jobs.
- Go event listener ignores any event whose DB job is not Go-owned.
- Go job routes reject Python-owned reruns.

If a migration-safe enum is too heavy, a constrained text column is acceptable:

```text
orchestrator_owner TEXT NOT NULL DEFAULT 'python'
CHECK (orchestrator_owner IN ('python', 'go'))
```

## Eligibility

`api-go` accepts job creation only for Go-owned pure ffmpeg pipelines.

Eligible conditions:

- Pipeline passes the Go validator.
- All non-source nodes are in the first-wave Go ffmpeg set:
  - `trim`
  - `transcode`
  - `export`
  - `vertical_crop`
  - `watermark`
  - `title_overlay`
  - `bgm`
  - `replace_audio`
  - `concat_horizontal`
  - `concat_vertical`
  - `concat_many`
  - `concat_timeline`
  - `concat_vertical_timeline`
  - `montage_assembler`
- Every `source` node has an `asset_id` after applying request inputs.
- The graph has no unsupported AutoFlow dynamic shape.
- The graph contains no ML, ASR, TTS, search, material, external platform, upload, or publish node.
- The whole job can be scheduled by Go; mixed ownership inside one job is not allowed in Phase 6.

Non-eligible pipelines are rejected before a job row is created:

```json
{"detail":"job orchestration for this pipeline remains Python-owned: <reason>"}
```

Use `501 Not Implemented` for this response. It means the route exists, but Go orchestration for that pipeline class is not implemented.

## Job Creation API

Phase 6 covers these routes:

```text
POST /api/v1/jobs
POST /api/v1/jobs/batch
POST /api/v1/jobs/{job_id}/rerun
```

`POST /api/v1/jobs` flow:

1. Load pipeline by `pipeline_id`.
2. Apply Python-compatible input overrides:
   - top-level `asset_id`
   - `node_id.field`
   - `node_id: { ... }`
3. Build the runtime snapshot for deterministic pipelines.
4. Validate the snapshot.
5. Run eligibility classification.
6. In one DB transaction:
   - create `jobs` row with `orchestrator_owner='go'` and `status='PENDING'`
   - create one `node_executions` row per pipeline node with `status='PENDING'`
7. Commit the transaction.
8. Start the job asynchronously through the Go engine.
9. Return a Python-compatible `JobDetailResponse`.

`POST /api/v1/jobs/batch`:

- Uses all-or-nothing validation.
- If any item is not eligible, no job is created.
- Successful requests create Go-owned jobs and start each through the Go engine.

`POST /api/v1/jobs/{job_id}/rerun`:

- Only accepts historical jobs with `orchestrator_owner='go'`.
- Creates a new Go-owned job from the old job snapshot.
- Re-runs eligibility before creating the new job.
- Rejects Python-owned jobs with the same no-fallback error shape.

Template execute routes are out of scope for this first Phase 6 slice.

## Scheduling Flow

`StartJob(job_id)` must be owner-guarded and idempotent.

Flow:

1. Load job and node executions.
2. Require `orchestrator_owner='go'`.
3. If job is terminal, return.
4. Read video schedule state.
5. If schedule is `CLOSED`, park job as `WAITING_WINDOW`.
6. If schedule is `DRAINING` and the job is a fresh submission, park job as `WAITING_WINDOW`.
7. Set job `PLANNING`, compute topo order and dependency map, then set job `RUNNING`.
8. Resolve source nodes:
   - load source asset
   - create `INTERMEDIATE` artifact pointing at asset storage
   - mark source node `SUCCEEDED`
9. Dispatch ready nodes:
   - dependencies all `SUCCEEDED`
   - optional artifact cache reuse if implemented for Go
   - set node `QUEUED`
   - write task to `vp:tasks:ffmpeg_go`

Go task payloads must include the existing worker contract keys plus explicit event ownership:

```json
{
  "job_id": "...",
  "node_execution_id": "...",
  "node_id": "...",
  "node_type": "...",
  "config": "{}",
  "input_artifacts": "{}",
  "preferred_hosts": "[]",
  "affinity_enqueued_at": "1779120000",
  "affinity_bounces": "0",
  "event_stream": "vp:events:go",
  "orchestrator_owner": "go"
}
```

Go worker behavior:

- If `event_stream` is present, publish completion/failure there.
- If absent, keep current behavior and publish to `vp:events` for backward compatibility.
- This preserves Python-owned jobs and old task fixtures.

## Event Listener

The Go event listener is enabled only when `VP_GO_ORCHESTRATOR_ENABLED=true`.

Redis stream contract:

```text
stream: vp:events:go
consumer group: orchestrator-go
consumer name: orchestrator-go-<host>-<pid>
```

Processing rules:

- Create the group with `MKSTREAM` on startup.
- Reclaim stale pending events with `XAUTOCLAIM`.
- Read new events with `XREADGROUP`.
- Parse `node_completed` and `node_failed`.
- Load DB job and require `orchestrator_owner='go'`.
- Acknowledge only after the DB update and downstream/finalization work succeeds.
- Unknown or Python-owned events are logged and acked after confirming they should not be handled by Go.
- Malformed events are logged and can be acked or dead-lettered; the first implementation can ack malformed events after metric/log emission to avoid permanent PEL growth.

Completion:

- Require non-empty `output_artifact_id`.
- Require referenced artifact row exists.
- Mark node `SUCCEEDED`, progress `100`, completed timestamp, and output artifact id.
- Dispatch newly ready downstream nodes.
- If all nodes are terminal, finalize the job.

Failure:

- If node `retry_count < 1`, increment retry count and re-dispatch.
- If retry is exhausted:
  - mark node `FAILED`
  - set error message
  - skip all downstream `PENDING` nodes
  - finalize job if no active nodes remain

Cancellation:

- If job is `CANCELLED`, ignore completion/failure events and ack them.
- Do not resurrect cancelled nodes during recovery.

## Finalization

When all nodes are terminal:

- All nodes `SUCCEEDED`:
  - job `SUCCEEDED`
  - `completed_at=now`
  - successful leaf artifacts marked `FINAL`
- Any failed/skipped/cancelled node:
  - if a leaf failed or no leaf succeeded, job `FAILED`
  - otherwise job `PARTIALLY_FAILED`
  - successful leaf artifacts marked `FINAL` only for partial success

Leaf detection should use the same dependency graph semantics already present in Go `internal/orchestrator`.

## Startup Recovery

Recovery runs only when `VP_GO_ORCHESTRATOR_ENABLED=true`.

At startup and optionally every interval:

1. Query Go-owned jobs with status:
   - `PENDING`
   - `WAITING_WINDOW`
   - `PLANNING`
   - `RUNNING`
2. For stale `QUEUED` or `RUNNING` nodes older than the configured threshold:
   - reset to `PENDING`
   - clear `worker_id`, queued/started/completed timestamps, progress, error fields, and input artifact ids
3. If schedule is `CLOSED`, keep or move job to `WAITING_WINDOW`.
4. If schedule is `DRAINING` and job is a fresh submission, keep or move job to `WAITING_WINDOW`.
5. If schedule is `OPEN`, start or resume the job.
6. If nodes are already terminal, finalize rather than dispatch.

Recovery must not query or mutate Python-owned jobs.

## Configuration

Add Go config fields:

```text
VP_GO_ORCHESTRATOR_ENABLED=false
VP_GO_ORCHESTRATOR_JOB_WRITES=false
VP_GO_EVENT_STREAM=vp:events:go
VP_GO_ORCHESTRATOR_RECOVERY_INTERVAL_SECONDS=60
VP_GO_ORCHESTRATOR_STALE_NODE_SECONDS=600
```

Meaning:

- `VP_GO_ORCHESTRATOR_ENABLED` starts the Go event listener and recovery loop.
- `VP_GO_ORCHESTRATOR_JOB_WRITES` enables Go-owned `POST /jobs`, `/jobs/batch`, and `/jobs/{id}/rerun`.
- If writes are enabled but the orchestrator is disabled, `/readyz` should report not ready or the write routes should fail closed.
- Both flags default false to prevent accidental production takeover.

Docker Compose can enable both flags for local/staging acceptance.

## Observability

Add metrics:

```text
vp_go_orchestrator_jobs_started_total
vp_go_orchestrator_jobs_finalized_total
vp_go_orchestrator_events_total
vp_go_orchestrator_event_failures_total
vp_go_orchestrator_dispatches_total
vp_go_orchestrator_retries_total
vp_go_orchestrator_recoveries_total
vp_go_orchestrator_pending_reclaims_total
```

Logs must include:

```text
service
orchestrator_owner
job_id
node_execution_id
node_id
node_type
redis_msg_id
event_stream
```

The acceptance evidence must prove Go ownership through DB owner, Go event stream, Go worker id, and Go listener state transitions.

## Testing

Go unit tests:

- eligibility accepts first-wave ffmpeg graphs.
- eligibility rejects AutoFlow, ML, ASR, TTS, search, material, upload, publish, and unknown nodes.
- input override parity for top-level `asset_id`, dotted paths, and nested node dictionaries.
- create Go-owned job and node executions in one transaction.
- owner-guarded updates cannot touch Python-owned rows.
- DAG dependency and leaf detection.
- source resolution creates intermediate artifacts.
- dispatch payload includes `event_stream=vp:events:go`.
- completion event is idempotent and dispatches downstream once.
- duplicate completion does not re-dispatch.
- failure retries once.
- retry exhaustion skips downstream and finalizes.
- cancelled jobs ack/ignore events.
- startup recovery resets stale Go-owned queued/running nodes only.
- final artifact marking covers success and partial failure.

Live strict tests:

- `api-go POST /api/v1/jobs` creates an eligible Go-owned job.
- Go-created job reaches terminal success using `ffmpeg_go`.
- Python and Go job detail responses agree for the completed Go-owned job.
- task events appear on `vp:events:go`, not `vp:events`.
- `XPENDING vp:events:go orchestrator-go == 0` after completion.
- `XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers == 0` after completion.
- non-eligible pipeline returns explicit no-fallback error and creates no job.
- `/jobs/batch` is all-or-nothing.
- `/jobs/{id}/rerun` works only for Go-owned jobs.

Acceptance runner:

- Add `scripts/go_phase6_acceptance.py`, or extend `scripts/go_migration_acceptance.py` with a Phase 6 mode.
- It must create jobs through `api-go`, not Python API.
- It must run at least 20 Go-created jobs over a representative multi-node pure ffmpeg graph.
- It must verify:
  - every job has `orchestrator_owner=go`
  - every migrated worker node has `worker_id` containing `ffmpeg_go-worker@`
  - every job reaches terminal success
  - final artifact exists and is downloadable
  - Go event stream pending count is zero
  - Go task stream pending count is zero
  - non-eligible rejection creates no job

## Rollback

Normal rollback:

1. Set `VP_GO_ORCHESTRATOR_JOB_WRITES=false`.
2. Keep `VP_GO_ORCHESTRATOR_ENABLED=true` until existing Go-owned jobs drain.
3. Optionally set schedule to `DRAINING` or `CLOSED` to stop new starts.
4. After Go-owned jobs are terminal, disable `VP_GO_ORCHESTRATOR_ENABLED`.

Emergency rollback:

- Disable Go job writes immediately.
- Leave Python API/orchestrator/listener running.
- Recreate or rerun affected work through Python API if needed.
- Existing Python-owned jobs are unaffected because streams and DB owner guards are isolated.

No DB restore should be required for normal rollback. The owner column remains as audit metadata.

## Acceptance Criteria

Phase 6 is complete when:

- `api-go` can create, start, listen, retry, skip downstream, and finalize Go-owned eligible jobs without Python orchestration.
- Go-owned tasks publish to `vp:events:go`.
- Go listener consumes `vp:events:go` and leaves no pending event growth.
- Go owner guards prevent changes to Python-owned jobs.
- Non-eligible pipelines are rejected without fallback and without creating jobs.
- Batch and rerun routes follow the same Go-only ownership rules.
- Startup recovery resumes only Go-owned jobs.
- Final artifacts are marked correctly for success and partial failure.
- Existing non-Phase-6 migration acceptance still passes.
- Python-owned paths continue to pass existing backend tests.
