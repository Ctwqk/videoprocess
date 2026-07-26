# Managed Worker Storage Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every automatic VP deployment prove scratch, MinIO, and managed-vision API connectivity before accepting the worker release.

**Architecture:** A focused async service performs bounded scratch and MinIO round trips and an optional artifact-API health request. A small CLI emits only stable sanitized JSON. The Swarm deployment resolves one running local task container per managed Python worker and executes the CLI after replica and placement convergence, returning failure to the existing rollback transaction.

**Tech Stack:** Python 3.12, asyncio, httpx, existing `StorageBackend`, pytest, Bash, Docker Swarm contract fakes.

## Global Constraints

- No database or Redis connection may be opened.
- No application task, artifact row, upload operation, publication, or feedback record may be created.
- No YouTube or YouTubeManager upload endpoint may be called.
- MinIO objects must use `health/deploy-readiness/` plus a random UUID and must be deleted in `finally`.
- Scratch files must use a dedicated readiness directory and must be deleted in `finally`.
- Probe payloads, connection URLs, credentials, and exception text must never be emitted.
- Host `10.0.0.126`, `CASPERs-Mac-mini`, and `colima-swarmbridged` are forbidden for execution, fallback, and rollback.
- A missing, duplicate, stopped, or non-local service container fails readiness.
- All new service behavior must be test-driven.

---

### Task 1: Storage Readiness Service

**Files:**
- Create: `backend/app/services/worker_storage_readiness.py`
- Test: `backend/tests/services/test_worker_storage_readiness.py`

**Interfaces:**
- Consumes: `StorageBackend.save/read/delete/exists`, environment values `STORAGE_BACKEND`, `STORAGE_LOCAL_ROOT`, and `VP_ARTIFACT_DOWNLOAD_BASE_URL`.
- Produces: `ReadinessFailure(code: str)`, `artifact_api_health_url(base_url: str) -> str`, and `async probe_worker_storage(env: Mapping[str, str], *, require_artifact_api: bool, storage: StorageBackend | None = None, http_client_factory: Callable[..., httpx.AsyncClient] | None = None) -> dict[str, object]`.

- [ ] **Step 1: Write the failing scratch and MinIO success test**

Create a fake `StorageBackend` that stores bytes in memory. Call
`probe_worker_storage` with `STORAGE_BACKEND=minio` and a temporary
`STORAGE_LOCAL_ROOT`. Assert:

```python
assert result == {
    "status": "ready",
    "components": {
        "scratch": "ready",
        "minio": "ready",
        "artifact_api": "not_required",
    },
}
assert fake_storage.objects == {}
assert list((tmp_path / "deploy-readiness").iterdir()) == []
```

- [ ] **Step 2: Run the success test and verify RED**

Run:

```bash
cd backend
python3 -m pytest tests/services/test_worker_storage_readiness.py::test_probe_round_trips_scratch_and_minio -q
```

Expected: import failure because `app.services.worker_storage_readiness` does
not exist.

- [ ] **Step 3: Implement the minimal round trips**

Create:

```python
class ReadinessFailure(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


async def probe_worker_storage(
    env: Mapping[str, str],
    *,
    require_artifact_api: bool,
    storage: StorageBackend | None = None,
    http_client_factory: Callable[..., httpx.AsyncClient] | None = None,
) -> dict[str, object]:
    ...
```

Use the fixed payload `b"vp-worker-storage-readiness-v1\n"`. Create the
scratch directory at
`Path(env["STORAGE_LOCAL_ROOT"]) / "deploy-readiness"`, write a UUID-named
file with mode `0600`, read and compare the exact payload, and unlink it in
`finally`.

Require `STORAGE_BACKEND == "minio"`. Resolve storage with
`storage or get_storage("minio")`. Save to
`health/deploy-readiness/<uuid>.probe`, require the returned byte count and
read content to match, then delete in `finally`. After deletion, require
`exists(path)` to be false.

Map failures to stable codes:

```python
scratch_unavailable
scratch_mismatch
minio_unavailable
minio_mismatch
configuration_invalid
cleanup_failed
```

- [ ] **Step 4: Run the success test and verify GREEN**

Run the command from Step 2.

Expected: `1 passed`.

- [ ] **Step 5: Add RED tests for mismatch, failure, and cleanup**

Add tests that inject:

- wrong scratch bytes by monkeypatching the module's focused
  `_read_scratch_probe(path: Path) -> bytes` helper;
- MinIO `save`, `read`, and `delete` exceptions;
- a mismatched MinIO payload;
- an object that still exists after delete;
- `STORAGE_BACKEND=local`;
- an empty `STORAGE_LOCAL_ROOT`.

Each test asserts only the stable `ReadinessFailure.code`, and every case that
reaches a created file or object asserts cleanup was attempted.

- [ ] **Step 6: Run the failure tests and verify RED**

Run:

```bash
cd backend
python3 -m pytest tests/services/test_worker_storage_readiness.py -q
```

Expected: the new edge-case tests fail on missing stable mappings or cleanup
checks.

- [ ] **Step 7: Implement the stable failure and cleanup behavior**

Preserve the primary failure unless cleanup also fails; cleanup failure must
produce `cleanup_failed`. Do not interpolate paths, environment values, or
exception text into `ReadinessFailure`.

- [ ] **Step 8: Add and implement artifact API health tests**

Test `artifact_api_health_url` with:

```python
assert artifact_api_health_url(
    "http://vp-api-swarm:8080/api/v1"
) == "http://vp-api-swarm:8080/health"
```

Add fake async HTTP client cases for status `200`, status `503`, timeout, and
invalid/non-HTTP base URLs. When `require_artifact_api=True`, require a finite
five-second timeout, `follow_redirects=False`, and map all request failures to
`api_unavailable`. When false, do not construct a client.

- [ ] **Step 9: Run Task 1 tests**

Run:

```bash
cd backend
python3 -m pytest tests/services/test_worker_storage_readiness.py -q
python3 -m ruff check app/services/worker_storage_readiness.py tests/services/test_worker_storage_readiness.py
python3 -m mypy app/services/worker_storage_readiness.py
```

Expected: all commands exit `0`.

- [ ] **Step 10: Commit Task 1**

```bash
git add backend/app/services/worker_storage_readiness.py backend/tests/services/test_worker_storage_readiness.py
git commit -m "feat(workers): probe managed storage readiness"
```

### Task 2: Sanitized Readiness CLI

**Files:**
- Create: `backend/app/channel_agent/worker_storage_readiness_cli.py`
- Test: `backend/tests/channel_agent/test_worker_storage_readiness_cli.py`

**Interfaces:**
- Consumes: `probe_worker_storage(...)` and `ReadinessFailure.code` from Task 1.
- Produces: `async run(argv: Sequence[str] | None = None) -> int` and `main(argv: Sequence[str] | None = None) -> int`.

- [ ] **Step 1: Write the failing CLI tests**

Monkeypatch `probe_worker_storage` and assert:

```python
assert await cli.run([]) == 0
assert json.loads(capsys.readouterr().out) == {
    "status": "ready",
    "components": {
        "scratch": "ready",
        "minio": "ready",
        "artifact_api": "not_required",
    },
}
```

For `--require-artifact-api`, assert the service receives
`require_artifact_api=True`. For `ReadinessFailure("minio_unavailable")`,
assert exit `3` and exactly:

```json
{"status":"failed","code":"minio_unavailable"}
```

For an unexpected exception containing a URL and password, assert exit `3`,
`code=unexpected_failure`, and neither secret substring appears in stdout or
stderr.

- [ ] **Step 2: Run the CLI tests and verify RED**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_worker_storage_readiness_cli.py -q
```

Expected: import failure because the CLI module does not exist.

- [ ] **Step 3: Implement the minimal CLI**

Use `argparse` with only `--require-artifact-api`. Pass `os.environ` to the
service. Emit compact sorted JSON through `print(json.dumps(...,
sort_keys=True, separators=(",", ":")))`. Return `0` for ready and `3` for
all readiness/runtime failures. Do not print exception objects.

- [ ] **Step 4: Run Task 2 tests and checks**

Run:

```bash
cd backend
python3 -m pytest tests/channel_agent/test_worker_storage_readiness_cli.py -q
python3 -m ruff check app/channel_agent/worker_storage_readiness_cli.py tests/channel_agent/test_worker_storage_readiness_cli.py
python3 -m mypy app/channel_agent/worker_storage_readiness_cli.py
```

Expected: all commands exit `0`.

- [ ] **Step 5: Commit Task 2**

```bash
git add backend/app/channel_agent/worker_storage_readiness_cli.py backend/tests/channel_agent/test_worker_storage_readiness_cli.py
git commit -m "feat(workers): add storage readiness CLI"
```

### Task 3: Swarm Deployment Gate

**Files:**
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Modify: `deploy/four-machine-topology.md`

**Interfaces:**
- Consumes: `python -m app.channel_agent.worker_storage_readiness_cli` from Task 2.
- Produces: `vp_require_managed_worker_storage_ready(service: str, require_artifact_api: bool = false)`.

- [ ] **Step 1: Add failing deployment contract assertions**

Extend the fake `docker` command so:

```text
docker container ls --filter label=com.docker.swarm.service.name=<service>
                    --filter status=running --format {{.ID}}
```

returns one deterministic container ID for each managed Python worker. Record
`docker exec` calls in `$CALLS`.

Assert each service invokes:

```text
docker|exec|<container-id>|python|-m|app.channel_agent.worker_storage_readiness_cli
```

and only vision includes `--require-artifact-api`. Assert the vision probe call
appears before the immutable-ID removal of `vp_vision_worker_1`.

- [ ] **Step 2: Run the deployment contract and verify RED**

Run:

```bash
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: failure because no worker-storage readiness command was recorded.

- [ ] **Step 3: Implement the deploy helper**

Add:

```bash
vp_require_managed_worker_storage_ready() {
  local service="$1"
  local require_artifact_api="${2:-false}"
  local containers
  containers="$(
    docker container ls \
      --filter "label=com.docker.swarm.service.name=$service" \
      --filter status=running \
      --format '{{.ID}}'
  )" || return 1
  if [[ "$(printf '%s\n' "$containers" | awk 'NF { count++ } END { print count+0 }')" -ne 1 ]]; then
    echo "managed worker storage readiness requires exactly one local running task: $service" >&2
    return 1
  fi
  local args=(python -m app.channel_agent.worker_storage_readiness_cli)
  if [[ "$require_artifact_api" == true ]]; then
    args+=(--require-artifact-api)
  elif [[ "$require_artifact_api" != false ]]; then
    echo "invalid managed worker artifact API readiness mode" >&2
    return 1
  fi
  if ! docker exec "$containers" "${args[@]}" >/dev/null; then
    echo "managed worker storage readiness failed: $service" >&2
    return 1
  fi
  log "managed worker storage readiness passed: $service"
}
```

Call it after `swarm_service_running` and `vp_require_service_node` in:

```text
vp_deploy_python_worker(..., false)
vp_deploy_vision_worker(..., true)
vp_deploy_publisher(..., false)
```

The vision call must remain before `vp_retire_legacy_vision_worker`.

- [ ] **Step 4: Run the deployment contract and verify GREEN**

Run the command from Step 2.

Expected: exit `0`.

- [ ] **Step 5: Add RED failure-path tests**

Add fake modes for:

- no matching container;
- two matching containers;
- `docker exec` nonzero for each managed service;
- invalid artifact API mode.

Assert each fails before subsequent managed services or legacy retirement.
Assert no recorded command contains `10.0.0.126`, `CASPERs-Mac-mini`,
`colima-swarmbridged`, MinIO credentials, or database URLs.

- [ ] **Step 6: Implement any minimal failure-path corrections**

Keep the helper fail-closed and preserve the existing outer snapshot rollback.
Do not add retries that could hide a consistently broken release.

- [ ] **Step 7: Document the readiness gate**

In `deploy/four-machine-topology.md`, document that all managed Python workers
must pass scratch and MinIO round trips after each update, vision additionally
checks API reachability, and a failure rejects the release before any canary or
soak activation.

- [ ] **Step 8: Run focused verification**

Run:

```bash
bash -n deploy/swarm/deploy-sync-extension.sh
bash tests/test_vp_deploy_sync_extension.sh
cd backend
python3 -m pytest \
  tests/services/test_worker_storage_readiness.py \
  tests/channel_agent/test_worker_storage_readiness_cli.py -q
```

Expected: all commands exit `0`.

- [ ] **Step 9: Run repository-required verification**

Run:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
cd ../frontend
npm install
npm run build
npm run lint || true
cd ..
go test ./...
bash tests/test_vp_deploy_ci_gate.sh
bash tests/test_channelops_soak_watch.sh
bash tests/test_vp_unlisted_canary.sh
```

Record historical advisory Ruff, Mypy, lint, and build warnings separately
from regressions. Required test/build commands must exit `0`.

- [ ] **Step 10: Commit Task 3**

```bash
git add deploy/swarm/deploy-sync-extension.sh tests/test_vp_deploy_sync_extension.sh deploy/four-machine-topology.md
git commit -m "feat(deploy): gate managed worker storage"
```

### Task 4: CI, Automatic Deployment, And Production Readiness Evidence

**Files:**
- No source file changes expected.
- Evidence: `.runtime/youtube-canary/` and controller logs, never committed.

**Interfaces:**
- Consumes: exact pushed VP commit and existing controller cron.
- Produces: verified exact-SHA CI, automatic 127/150 deployment, worker probe log lines, and unchanged safety state.

- [ ] **Step 1: Push the exact commit**

Push `main` and record the full SHA. Do not modify or stage
`vp_autonomous_production_feedback_loop_plan.md`.

- [ ] **Step 2: Verify GitHub Actions**

Require Go, backend/migrations, deployment contracts, and frontend jobs to
complete successfully for the exact SHA.

- [ ] **Step 3: Observe automatic deployment**

Do not manually run a live canary. Wait for the controller's managed
`*/15` VP cron and require:

- all VP services `1/1`;
- exact `deploy-<12 hex>` images;
- GPU, vision, and publisher on `ccttww-lap`;
- runtime services on `colima-127`;
- no service task on host 126;
- one successful storage-readiness log line for each managed Python worker.

- [ ] **Step 4: Run read-only canary preflight**

Run `scripts/run_vp_unlisted_canary.py --preflight-only` with the established
127/150 tunnel topology. Require all audited Redis streams pending `0`, exact
managed consumers, no runnable backlog, schedule `CLOSED`, and no guarded job.

- [ ] **Step 5: Verify no external side effect**

Read production state and require no new upload operation, publication, public
visibility, or live canary run from this deployment. Confirm the soak state
file remains absent.

- [ ] **Step 6: Stop before live execution**

Do not pass `--confirm-live-unlisted` until the operator provides a new,
single-attempt authorization naming the next canary.
