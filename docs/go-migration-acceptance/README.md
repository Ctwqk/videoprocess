# Go Migration Acceptance Evidence

Scope: non-Phase-6 completion for `/home/taiwei/Constructure-repos/videoprocess/docs/videoprocess-go-partial-migration-spec.md`.

Python remains authoritative for orchestration, event listening, schema migration, and rollback.

Evidence sections:

1. Registry parity.
2. Validator parity and unsupported graph refusal.
3. Per-node Go worker migration gate.
4. Per-route Go API write gate.
5. Docker health, readiness, and metrics.
6. Staging jobs, Redis pending, artifacts, p95, failure, cancellation, and rollback.

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

`POST /api/v1/jobs`, `POST /api/v1/jobs/batch`, and `POST /api/v1/jobs/{id}/rerun` remain Python-owned unless a Python start-job handoff endpoint is explicitly configured. This preserves the Phase 6 exclusion: Go does not schedule DAGs or listen to worker events in this milestone.

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
