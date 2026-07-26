# Managed Vision Worker Deployment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the current commit-tagged vision worker an automatically deployed Swarm service on host 150 and retire the incompatible legacy Compose consumer.

**Architecture:** Extend the existing Python worker deployment path with a dedicated `vp-vision-worker-swarm` service, then remove only the exact legacy Compose container after the managed replacement is healthy. Treat the vision Redis stream and consumer identity as required canary and soak readiness signals.

**Tech Stack:** Bash, Docker Swarm, Docker Compose labels, Redis Streams, Python/pytest, shell contract tests

## Global Constraints

- Host 126 must not build, deploy, run, watch, publish, fail over, or receive rollback work.
- The video schedule must be `CLOSED` before production migration.
- External assets must not be publicly published without explicit human review.
- The fifth authorization is consumed and must not be reused.
- Do not replay or acknowledge unverifiable legacy events.
- Do not modify or stage `vp_autonomous_production_feedback_loop_plan.md`.

---

### Task 1: Managed Vision Service Deployment

**Files:**
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Modify: `deploy/swarm/deploy-sync-extension.sh`

**Interfaces:**
- Consumes: `vp_deploy_python_worker`, `vp_service_values`, `swarm_service_running`, and the commit-tagged Python worker image.
- Produces: `vp_deploy_vision_worker(image)`, `vp_retire_legacy_vision_worker()`, and the service constant `VP_VISION_WORKER_SERVICE=vp-vision-worker-swarm`.

- [ ] **Step 1: Write failing deployment contract assertions**

Add fake Docker state for the managed vision service and exact legacy container
labels. Assert that a normal app deployment:

```bash
grep -Fq 'vp-vision-worker-swarm' "$CALLS"
grep -Fq -- '--env-add WORKER_TYPE=vision' "$CALLS"
grep -Fq -- '--env-add WORKER_HOST=150-vision' "$CALLS"
grep -Fq -- '--constraint-add node.labels.vp.gpu==true' "$CALLS"
grep -Fq -- '--mount-add type=volume,src=vp-vision-worker-scratch,dst=/data/storage' "$CALLS"
grep -Fq 'docker|rm -f vp_vision_worker_1' "$CALLS"
```

Add negative cases proving that an unexpected Compose project/service label
fails closed without calling `docker rm`, and that removal occurs only after
`running|vp-vision-worker-swarm`.

- [ ] **Step 2: Run the shell contract and verify RED**

Run:

```bash
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: FAIL because no managed vision service or exact legacy retirement
contract exists.

- [ ] **Step 3: Implement the managed service**

Add:

```bash
VP_VISION_WORKER_SERVICE="vp-vision-worker-swarm"
VP_APP_SERVICES="... $VP_PYTHON_WORKER_SERVICE $VP_VISION_WORKER_SERVICE $VP_PUBLISHER_SERVICE"
```

Implement `vp_vision_worker_env()` with shared Postgres, Redis, and MinIO
settings plus:

```bash
WORKER_TYPE=vision
WORKER_HOST=150-vision
WORKER_CONCURRENCY=1
VIDEO_USE_GPU=false
VIDEO_GPU_FALLBACK_TO_CPU=true
```

Implement create/update/restore behavior parallel to the existing managed
Python worker, using `node.labels.vp.gpu==true`, `vp-pipeline-net`, stop-first,
one replica, and `vp-vision-worker-scratch:/data/storage`.

Implement `vp_retire_legacy_vision_worker()` so it:

1. Returns success when `vp_vision_worker_1` is absent.
2. Reads both Compose labels from `docker inspect`.
3. Requires project `videoprocess` and service `vision-worker`.
4. Calls `docker rm -f vp_vision_worker_1` only after the managed service is
   verified running.
5. Returns failure on missing or mismatched labels.

- [ ] **Step 4: Run the deployment contract and verify GREEN**

Run:

```bash
bash tests/test_vp_deploy_sync_extension.sh
bash -n deploy/swarm/deploy-sync-extension.sh
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_vp_deploy_sync_extension.sh deploy/swarm/deploy-sync-extension.sh
git commit -m "fix(deploy): manage the production vision worker"
```

### Task 2: Vision Stream Readiness

**Files:**
- Modify: `backend/tests/services/test_unlisted_canary_runner.py`
- Modify: `scripts/run_vp_unlisted_canary.py`
- Modify: `tests/test_channelops_soak_watch.sh`
- Modify: `deploy/swarm/channelops-soak-watch.sh`

**Interfaces:**
- Consumes: Redis stream/group readiness audits.
- Produces: the required tuple `vp:tasks:vision|vision-workers|vision-worker@150-vision:<pid>`.

- [ ] **Step 1: Write failing canary and soak assertions**

Extend the approved Redis audit fixture with:

```python
"vp:tasks:vision": {
    "group": "vision-workers",
    "pending": 0,
    "active_consumers": ["vision-worker@150-vision:1"],
    "stale_consumer_count": 0,
}
```

Add rejection cases for missing, stale, duplicate, malformed, or pending vision
consumers. Add shell assertions that the soak watcher requires the managed
vision service and exact stream identity.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cd backend
python3 -m pytest tests/services/test_unlisted_canary_runner.py -q
cd ..
bash tests/test_channelops_soak_watch.sh
```

Expected: FAIL because production readiness does not include vision.

- [ ] **Step 3: Implement readiness checks**

Add to `REDIS_PENDING_STREAM_GROUPS` and `REDIS_ACTIVE_CONSUMER_PATTERNS`:

```python
("vp:tasks:vision", "vision-workers")
"vp:tasks:vision": re.compile(r"^vision-worker@150-vision:[1-9][0-9]*$")
```

Add `vp-vision-worker-swarm` to the soak service inventory and add:

```text
vp:tasks:vision|vision-workers|^vision-worker@150-vision:[1-9][0-9]*$
```

to its stream-group contract.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the same commands from Step 2 plus:

```bash
bash tests/test_vp_unlisted_canary_scripts.sh
bash -n deploy/swarm/channelops-soak-watch.sh
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/tests/services/test_unlisted_canary_runner.py \
  scripts/run_vp_unlisted_canary.py \
  tests/test_channelops_soak_watch.sh \
  deploy/swarm/channelops-soak-watch.sh
git commit -m "fix(channelops): require the managed vision consumer"
```

### Task 3: Topology, Verification, and Production Convergence

**Files:**
- Modify: `deploy/four-machine-topology.md`

**Interfaces:**
- Consumes: Tasks 1 and 2.
- Produces: documented 127 control-plane / 150 worker topology and verified automatic deployment evidence.

- [ ] **Step 1: Update topology documentation**

Document `vp-vision-worker-swarm` as a host-150 managed service using the exact
vision stream identity. State that the old Compose worker is forbidden after
convergence and host 126 remains excluded.

- [ ] **Step 2: Run repository verification**

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
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_channelops_soak_watch.sh
bash tests/test_vp_unlisted_canary_scripts.sh
```

Expected: required tests and builds pass; advisory lint/type output is recorded.

- [ ] **Step 3: Commit and push**

```bash
git add deploy/four-machine-topology.md
git commit -m "docs: record the managed vision worker topology"
git push origin main
```

- [ ] **Step 4: Verify automatic deployment**

Wait for the exact commit CI and cron deployment. Confirm:

```text
vp-vision-worker-swarm  1/1  vp-ffmpeg-worker-python:deploy-<exact sha>
vp_vision_worker_1      absent
```

Confirm `vp:tasks:vision` has one active
`vision-worker@150-vision:<pid>` consumer, no stale consumers, and zero pending
entries after the failed-canary cleanup.

- [ ] **Step 5: Preserve the authorization boundary**

Keep the schedule `CLOSED`. Do not run another live canary until the user gives
a new explicit authorization for exactly one unlisted canary.
