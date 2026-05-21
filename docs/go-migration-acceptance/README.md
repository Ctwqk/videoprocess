# Go Migration Acceptance Evidence

Scope: completed Go partial migration for `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`, including the narrow Phase 6 Go orchestrator slice.

Python remains authoritative outside the Go-eligible first-wave ffmpeg slice, and remains the schema migration and rollback reference implementation.

Evidence sections:

1. Registry parity.
2. Validator parity and unsupported graph refusal.
3. Per-node Go worker migration gate.
4. Per-route Go API write gate.
5. Docker health, readiness, and metrics.
6. Staging jobs, Redis pending, artifacts, p95, failure, cancellation, and rollback.
7. Phase 6 Go-owned job creation, scheduling, event listening, recovery, and final artifact marking.

## Per-Node Worker Cutover

Command:

```bash
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
redis-cli -u redis://127.0.0.1:6380/0 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Expected evidence:

```text
14 passed
XPENDING summary count: 0
```

## Job Write Ownership

`POST /api/v1/jobs`, `POST /api/v1/jobs/batch`, and `POST /api/v1/jobs/{id}/rerun` are Go-owned only for pipelines that the deterministic Go eligibility classifier accepts. Non-eligible pipelines are rejected without Python fallback so mixed ownership cannot leak into a single Phase 6 job.

## Docker And Strict Parity

Commands run:

```bash
docker compose up -d --build api-go ffmpeg-worker-go
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18081/health
curl -fsS http://127.0.0.1:18081/readyz
curl -fsS http://127.0.0.1:18081/metrics
curl -fsS http://127.0.0.1:19091/metrics
docker compose exec -T ffmpeg-worker-go sh -lc 'tr "\0" " " < /proc/1/cmdline && printf "\n" && printenv WORKER_TYPE'
VP_GO_PARITY_STRICT=1 VP_GO_API=http://127.0.0.1:18081 VP_PYTHON_API=http://127.0.0.1:18080 python3 -m pytest tests/go_migration/test_go_api_parity.py tests/go_migration/test_go_api_read_parity.py tests/go_migration/test_go_registry_parity.py tests/go_migration/test_go_validator_parity.py -q
VP_GO_WORKER_SMOKE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 VP_PYTHON_API=http://127.0.0.1:18080 python3 -m pytest tests/go_migration/test_go_trim_worker_smoke.py -q
VP_GO_WORKER_NODE_STRICT=1 VP_REDIS_URL=redis://127.0.0.1:6380/0 VP_PYTHON_API=http://127.0.0.1:18080 python3 -m pytest tests/go_migration/test_go_worker_nodes.py -q
VP_GO_WRITE_STRICT=1 VP_GO_API_URL=http://127.0.0.1:18081 python3 -m pytest tests/go_migration/test_go_api_write_parity.py -q
redis-cli -u redis://127.0.0.1:6380/0 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Observed result:

```text
Python health: {"status":"ok"}
Go health: {"status":"ok"}
Go readyz: {"postgres":"ok","redis":"ok","status":"ready","storage":"ok"}
Go API metrics exposed http_requests_total, http_request_duration_seconds, and http_request_errors_total.
Go worker metrics exposed vp_worker_tasks_total and vp_ffmpeg_runs_total.
Worker process: vp-ffmpeg-worker-go
WORKER_TYPE: ffmpeg_go
API parity: 22 passed
Trim worker smoke: 1 passed
Per-node worker cutover: 14 passed
Write parity: 5 passed
Redis XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers: 0
```

## Production-Style Acceptance

Commands run:

```bash
python3 scripts/go_migration_acceptance.py --help
python3 -m py_compile scripts/go_migration_acceptance.py
python3 scripts/go_migration_acceptance.py --api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 1
python3 scripts/go_migration_acceptance.py --api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
redis-cli -u redis://127.0.0.1:6380/0 XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers
```

Observed result:

```text
--help includes --api-url, --redis-url, --count, and --timeout-seconds.
py_compile: pass
count=1 smoke: every migrated node completed=1, redis_pending=0, missing_output_artifact_id=0, missing_storage_path=0, wrong_worker=0
count=20 acceptance: every migrated node completed=20, redis_pending=0, missing_output_artifact_id=0, missing_storage_path=0, wrong_worker=0
Redis XPENDING after acceptance: 0
p95_seconds range: 2.039856790192425 to 4.04803975042887
```

## Phase 6 Go Orchestrator Acceptance

Commands:

```bash
docker compose up -d --build api api-go ffmpeg-worker-go
VP_GO_PHASE6_STRICT=1 VP_GO_API_URL=http://127.0.0.1:18081 VP_PYTHON_API=http://127.0.0.1:18080 VP_REDIS_URL=redis://127.0.0.1:6380/0 python3 -m pytest tests/go_migration/test_go_orchestrator_phase6.py -q
python3 scripts/go_phase6_acceptance.py --api-go-url http://127.0.0.1:18081 --python-api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20
```

Expected result:

```text
Go API creates Go-owned jobs.
Go worker emits events to vp:events:go.
Go listener finalizes jobs.
Python API agrees on terminal status.
Redis pending counts are zero.
Non-eligible pipeline is rejected without fallback.
```

Observed result:

```text
docker compose up -d --build api-go ffmpeg-worker-go: pass
Python API health: {"status":"ok"}
Go API readyz: {"postgres":"ok","redis":"ok","status":"ready","storage":"ok"}

go test ./cmd/... ./internal/...: pass
go vet ./cmd/... ./internal/...: pass
gofmt -l $(find cmd internal -name '*.go' -type f): no output
cd backend && python3 -m pytest: 338 passed, 8 warnings
cd backend && python3 -m ruff check . || true: /usr/bin/python3: No module named ruff
cd backend && python3 -m mypy app || true: /usr/bin/python3: No module named mypy

Phase 6 strict live pytest: 2 passed
Phase 6 count=1 acceptance:
  jobs_completed=1, missing_final_artifact=0, non_eligible_rejected=true,
  wrong_owner=0, wrong_worker=0, go_event_pending=0, go_task_pending=0
Phase 6 count=20 acceptance:
  jobs_completed=20, missing_final_artifact=0, non_eligible_rejected=true,
  wrong_owner=0, wrong_worker=0, go_event_pending=0, go_task_pending=0
Schedule gate live check:
  waiting_status=WAITING_WINDOW, terminal_status=SUCCEEDED, final_artifact_ok=true

Redis XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers: 0
Redis XPENDING vp:events:go orchestrator-go: 0
Schedule state after live check: OPEN, waiting_jobs=0, queued_nodes=0, running_nodes=0
```

## Baseline

Commands run before non-Phase-6 completion work:

```bash
git status --short --branch
go test ./...
go vet ./...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

Observed result:

```text
git branch: codex/go-partial-migration
go test ./...: pass
go vet ./...: pass
backend pytest: 331 passed, 8 warnings
ruff: /usr/bin/python3: No module named ruff
mypy: /usr/bin/python3: No module named mypy
```

## Final Spec Audit

Source spec:

```text
/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md
```

Implemented scope:

```text
Phase 0 Baseline/Gates:
- Go sidecars run beside Python services.
- /readyz reports postgres/redis/storage readiness.
- production stub-store behavior is fail-closed unless explicitly enabled.

Phase 1 Go API read-only parity:
- /api/v1 read routes and detail routes have Python-shape parity tests.
- /internal/schedule/video/status reads real schedule state instead of fixed OPEN.
- request id/logging/metrics middleware is present.

Phase 2 Go worker trim MVP:
- vp-ffmpeg-worker-go registers task-level media handlers.
- runtime resolves input artifacts, writes output media, creates artifact rows, emits non-empty output_artifact_id, and cleans temp files.
- cancellation ack contract is aligned with Python for confirmed cancellation.
- Python orchestrator can dispatch to ffmpeg_go and complete jobs through the Python event listener.

Phase 3 Worker production semantics:
- PEL reclaim, heartbeat, host affinity defer, bounded concurrency, graceful shutdown, and worker metrics are implemented and covered by Go tests.

Phase 4 First-wave pure ffmpeg nodes:
- Node registry cutover routes the first-wave pure ffmpeg nodes to worker_type=ffmpeg_go:
  trim, transcode, export, vertical_crop, watermark, title_overlay, bgm, replace_audio, concat_horizontal, concat_vertical, concat_many, concat_timeline, concat_vertical_timeline, montage_assembler.
- Strict per-node mixed-mode tests verify real job execution through the Go worker.

Phase 5 Selective Go API writes:
- pipeline validation and deterministic pipeline/asset/artifact/schedule/job write surfaces are implemented with mixed-mode ownership guards.
- job create/batch/rerun are Go-owned for eligible Phase 6 pipelines and reject non-eligible pipelines without fallback.

Phase 6 Go orchestrator slice:
- Go API creates owner-tagged Go jobs for fully eligible first-wave ffmpeg pipelines.
- Go engine schedules source resolution, DAG dispatch, retries, downstream skip, and final artifact promotion.
- Go workers emit to vp:events:go and the Go event listener finalizes jobs.
- Startup recovery resumes Go-owned PENDING, WAITING_WINDOW, PLANNING, and RUNNING jobs.
- Schedule CLOSED/DRAINING gates park fresh Go jobs in WAITING_WINDOW and release them when schedule opens.
- Go local storage can download both relative and Python-style absolute local artifact paths.

Spec non-goals retained:
- AutoFlow graph planner Go rewrite.
- LLM/ASR/TTS/search/material/external platform publish handler Go rewrite.
- public publishing behavior changes.
- Alembic replacement or Postgres schema ownership transfer.
```

Python code deletion status:

```text
Python worker handlers for Go cutover first-wave ffmpeg nodes have been removed:
trim, transcode, export, vertical_crop, watermark, title_overlay, bgm,
replace_audio, concat_horizontal, concat_vertical, concat_many,
concat_timeline, concat_vertical_timeline, montage_assembler.

The node registry definitions are intentionally retained so frontend, AutoFlow,
validation, and Python orchestration can still route those node types to worker_type=ffmpeg_go.

Old Python API, Python orchestrator, non-Go worker handlers, schemas, and Alembic code are intentionally retained.
The source spec keeps Python as the reference implementation outside the Go-eligible slice and as the rollback path.
Rollback for migrated nodes requires restoring the removed Python handlers if direct Python ffmpeg fallback is needed again.
For eligible Phase 6 jobs, rollback is disabling Go job-write/orchestrator flags and routing new jobs back to Python.
```

Fresh final verification:

```text
go test ./cmd/... ./internal/...: pass
go vet ./cmd/... ./internal/...: pass
gofmt -l $(find cmd internal -name '*.go' -type f): no output
cd backend && python3 -m pytest: 315 passed, 8 warnings
cd backend && python3 -m ruff check . || true: /usr/bin/python3: No module named ruff
cd backend && python3 -m mypy app || true: /usr/bin/python3: No module named mypy

Python API health: {"status":"ok"}
Go API readyz: {"postgres":"ok","redis":"ok","status":"ready","storage":"ok"}

API parity/read/registry/validator strict gate: 22 passed
trim worker strict smoke: 1 passed
first-wave worker cutover strict gate: 14 passed
Go write strict gate: 5 passed
Phase 6 strict live pytest: 2 passed
Phase 6 count=20 acceptance:
  jobs_completed=20, missing_final_artifact=0, non_eligible_rejected=true,
  wrong_owner=0, wrong_worker=0, go_event_pending=0, go_task_pending=0
Single-job max-coverage acceptance:
  command: python3 scripts/go_max_coverage_acceptance.py --api-go-url http://127.0.0.1:18081 --python-api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --timeout-seconds 600
  pipeline_id=22412042-4129-4ea5-ac04-bdc1884282bf
  job_id=a70c7888-10da-49f5-8ea5-4b96cd21f8ba
  status=SUCCEEDED, orchestrator_owner=go, worker_node_count=16
  covered_types=14, missing_types=0, failed_nodes=0, wrong_workers=0
  final_artifact_id=d4fba92c-99e3-4e28-9f18-b59cd682154f, final_artifact_ok=true
  go_event_pending=0, go_task_pending=0
Redis XPENDING vp:tasks:ffmpeg_go ffmpeg_go-workers: 0
Redis XPENDING vp:events:go orchestrator-go: 0
```

Fresh production-style acceptance:

```text
python3 scripts/go_migration_acceptance.py --api-url http://127.0.0.1:18080 --redis-url redis://127.0.0.1:6380/0 --count 20

Result:
- every migrated node completed=20
- redis_pending=0 for every migrated node
- missing_output_artifact_id=0 for every migrated node
- missing_storage_path=0 for every migrated node
- wrong_worker=0 for every migrated node
- p95_seconds range: 2.040999120241031 to 4.050431395694614
```
