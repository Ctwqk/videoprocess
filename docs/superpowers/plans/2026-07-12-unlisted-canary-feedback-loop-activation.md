# Unlisted Canary Feedback Loop Activation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Safely run one owned-material VideoProcess canary through generation, a single real unlisted YouTube upload, publication linkage, and feedback collection.

**Architecture:** Route YouTube writes to a dedicated publisher stream on 150. A Postgres upload-operation ledger reserves each side effect before YouTubeManager receives media, resumes manager polling after worker restarts, replays completed receipts, and blocks ambiguous retries. Quarantine the historical soak backlog before opening the global video schedule, then create and run one atomic-intake-paused canary channel. The 2026-07-12 halt-after-selection procedure is superseded.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy async, Alembic, httpx, pytest, Go 1.24, pgx, Redis Streams, Docker Swarm, Bash, FFmpeg, YouTubeManager HTTP API.

## Global Constraints

- Automated publication is `unlisted` for this canary; `public` is rejected.
- OAuth files remain mounted only in YouTubeManager, never in VP workers.
- One production task owns at most one upload operation and one publication.
- Existing APIs remain available; defaults become safer without removing fields.
- External-platform assets are excluded from the canary.
- Existing backlog rows are retained and auditable, not deleted.
- 126 receives no VP runtime or publisher label.
- Every generated pipeline must pass `validate_pipeline()`.

---

### Task 1: Publisher Routing And Fail-Closed Defaults

**Files:**
- Modify: `backend/app/node_registry/builtin/youtube_upload.py`
- Modify: `backend/app/autoflow/capability_manifest.py`
- Modify: `backend/app/schemas/channel_agent.py`
- Modify: `backend/app/services/worker_admission.py`
- Modify: `backend/tests/autoflow/test_capability_manifest.py`
- Modify: `backend/tests/channel_agent/test_models_bcd.py`
- Modify: `backend/tests/worker/test_worker_admission.py`
- Modify: `backend/tests/golden/go_migration/node_registry_manifest.json`
- Modify: `internal/pipeline/testdata/node_registry_manifest.json`

**Interfaces:**
- Produces: worker type `youtube_publisher` for `youtube_upload`.
- Produces: production admission contract for the dedicated publisher.
- Produces: private defaults for new account and format schemas.

- [ ] **Step 1: Add failing routing, defaults, and admission tests**

Assert the following exact behavior:

```python
upload = next(node for node in get_capability_manifest().nodes if node.type_name == "youtube_upload")
assert upload.worker_type == "youtube_publisher"
assert upload.execution.worker_type == "youtube_publisher"
assert PublishingAccountCreate(account_label="canary").default_privacy == "private"
assert LaneFormatCreate().default_publish_visibility == "private"
```

For production publisher admission, use:

```python
env = {
    "DEPLOY_MODE": "production",
    "REDIS_URL": "redis://10.0.0.150:6380/0",
    "WORKER_TYPE": "youtube_publisher",
    "WORKER_HOST": "150-publisher",
    "STORAGE_BACKEND": "minio",
    "MINIO_ENDPOINT": "10.0.0.150:9000",
    "MINIO_ACCESS_KEY": "x",
    "MINIO_SECRET_KEY": "y",
    "MINIO_BUCKET": "videoprocess",
    "YOUTUBE_MANAGER_URL": "http://10.0.0.150:18999",
    "YOUTUBE_PUBLISH_ENABLED": "true",
    "PUBLIC_PUBLISH_ENABLED": "false",
}
assert validate_worker_admission(env).allowed
```

Also assert rejection for a local manager URL, missing MinIO settings,
`PUBLIC_PUBLISH_ENABLED=true`, disabled publishing, and any
`YOUTUBE_CREDENTIALS_DIR` on either production publisher or FFmpeg workers.

- [ ] **Step 2: Run the focused tests and confirm they fail**

```bash
cd backend
.venv/bin/python -m pytest \
  tests/autoflow/test_capability_manifest.py \
  tests/channel_agent/test_models_bcd.py \
  tests/worker/test_worker_admission.py -q
```

Expected: assertions still observe `ffmpeg` and `public`, and publisher
admission lacks the new guards.

- [ ] **Step 3: Implement routing, defaults, and admission**

Set both the node definition and execution override to
`worker_type="youtube_publisher"`. Change only schema defaults from `public`
to `private`; explicit legacy values remain accepted. Factor shared MinIO
validation so it applies to `ffmpeg` and `youtube_publisher`, then add the
publisher-specific checks listed in Step 1.

- [ ] **Step 4: Regenerate both canonical manifests**

```bash
cd backend
.venv/bin/python -m scripts.export_node_registry_manifest
```

- [ ] **Step 5: Run focused Python and Go contract tests**

```bash
cd backend
.venv/bin/python -m pytest \
  tests/autoflow/test_capability_manifest.py \
  tests/channel_agent/test_models_bcd.py \
  tests/worker/test_worker_admission.py \
  tests/test_node_registry_manifest.py -q
cd ..
go test ./internal/pipeline ./internal/config
```

Expected: all pass and both manifests show `youtube_publisher`.

- [ ] **Step 6: Commit**

```bash
git add backend/app/node_registry/builtin/youtube_upload.py \
  backend/app/autoflow/capability_manifest.py \
  backend/app/schemas/channel_agent.py \
  backend/app/services/worker_admission.py \
  backend/tests/autoflow/test_capability_manifest.py \
  backend/tests/channel_agent/test_models_bcd.py \
  backend/tests/worker/test_worker_admission.py \
  backend/tests/golden/go_migration/node_registry_manifest.json \
  internal/pipeline/testdata/node_registry_manifest.json
git commit -m "feat: isolate youtube publisher routing"
```

### Task 2: Durable Upload Operation Ledger

**Files:**
- Create: `backend/app/models/youtube_upload_operation.py`
- Create: `backend/app/services/youtube_upload_operations.py`
- Create: `backend/alembic/versions/023_youtube_upload_operations.py`
- Create: `backend/tests/services/test_youtube_upload_operations.py`
- Modify: `backend/app/models/__init__.py`

**Interfaces:**
- Produces: `YouTubeUploadOperation` ORM model.
- Produces: `UploadOperationClaim(action, operation)` where action is
  `submit`, `resume`, `replay`, or `block`.
- Produces: `YouTubeUploadOperationStore.claim()`, `mark_submitted()`,
  `mark_succeeded()`, `mark_uncertain()`, and `mark_failed()`.

- [ ] **Step 1: Write failing operation-state tests**

Use an in-memory SQLite async engine with `ProductionTask`,
`YouTubeUploadOperation`, and supporting tables. Cover:

```python
claim = await store.claim(context)
assert claim.action == "submit"
assert claim.operation.status == "reserved"

again = await store.claim(context)
assert again.action == "block"

await store.mark_submitted(claim.operation.id, "manager-task-1")
assert (await store.claim(context)).action == "resume"

await store.mark_succeeded(claim.operation.id, "abcdefghijk", receipt)
assert (await store.claim(context)).action == "replay"
```

Also assert that a second node for the same production task and a duplicate
platform video ID raise a deterministic conflict.

- [ ] **Step 2: Run and confirm the model/service tests fail**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_youtube_upload_operations.py -q
```

Expected: import failure for the new model/service.

- [ ] **Step 3: Add model and migration**

Use string states and UUID foreign keys. The migration must execute conflict
preflights before adding:

```sql
CREATE UNIQUE INDEX ux_publication_records_production_task
ON publication_records (production_task_id);

CREATE UNIQUE INDEX ux_publication_records_platform_content
ON publication_records (platform, platform_content_id);
```

Create partial unique indexes for non-null operation production-task and
platform-video IDs. Raise from the migration when historical duplicates exist.

- [ ] **Step 4: Implement the operation store**

`claim()` resolves `ProductionTask.id` from `job_id`, computes a single
transactional insert-or-read outcome, and never converts an existing
`reserved` operation back into `submit`. State transitions commit before
returning. `mark_succeeded()` stores a JSON receipt containing only video ID,
URL, title, privacy, tags, and quota estimate.

- [ ] **Step 5: Run focused tests and migration syntax checks**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_youtube_upload_operations.py -q
.venv/bin/python -m compileall app alembic/versions/023_youtube_upload_operations.py
```

- [ ] **Step 6: Commit**

```bash
git add backend/app/models/youtube_upload_operation.py \
  backend/app/services/youtube_upload_operations.py \
  backend/app/models/__init__.py \
  backend/alembic/versions/023_youtube_upload_operations.py \
  backend/tests/services/test_youtube_upload_operations.py
git commit -m "feat: reserve durable youtube uploads"
```

### Task 3: Manager-Backed Idempotent Upload Handler

**Files:**
- Modify: `backend/worker/handlers/youtube_upload.py`
- Modify: `backend/worker/main.py`
- Create: `backend/tests/worker/test_youtube_upload_handler.py`
- Modify: `backend/tests/worker/test_worker_startup.py`

**Interfaces:**
- Consumes: `YouTubeUploadOperationStore` from Task 2.
- Consumes: internal config keys `_job_id`, `_node_execution_id`, and
  `_input_artifact_ids` injected by the worker.
- Produces: artifact media info under `{"youtube": receipt}`.

- [ ] **Step 1: Write fake-HTTP handler tests**

Use `httpx.MockTransport` and a fake operation store. Cover:

- public privacy rejected before HTTP;
- publishing disabled rejected before HTTP;
- auth false and quota below 1,600 rejected;
- fresh claim posts one multipart upload, stores manager task ID, polls to
  completed, and stores one video receipt;
- submitted claim performs no upload POST and resumes polling;
- replay claim performs no HTTP and returns the stored receipt;
- blocked claim performs no HTTP;
- missing manager task, timeout, and transport ambiguity call
  `mark_uncertain()` and never submit again.

- [ ] **Step 2: Run and confirm failures**

```bash
cd backend
.venv/bin/python -m pytest tests/worker/test_youtube_upload_handler.py -q
```

- [ ] **Step 3: Replace direct OAuth upload with the manager adapter**

The handler constructor accepts an operation store, optional `httpx` client,
poll interval, and timeout. `execute()` validates internal context, computes
SHA-256, claims the operation, calls `/api/auth/status`, submits `/api/upload`,
persists the manager task ID, polls `/api/status/{task_id}`, and copies the
input file to the output path only after a durable success or replay.

Do not read OAuth files or import Google client libraries in the VP worker.

- [ ] **Step 4: Inject worker execution context and the real store**

Before handler construction, add:

```python
config["_job_id"] = job_id
config["_node_execution_id"] = node_execution_id
config["_input_artifact_ids"] = dict(input_artifacts_map)
```

Construct `YouTubeUploadHandler` with the process session factory only when
`node_type == "youtube_upload"`; preserve the existing class-map behavior for
all other handlers.

- [ ] **Step 5: Run worker tests**

```bash
cd backend
.venv/bin/python -m pytest \
  tests/worker/test_youtube_upload_handler.py \
  tests/worker/test_worker_startup.py \
  tests/worker/test_redis_client.py -q
```

- [ ] **Step 6: Commit**

```bash
git add backend/worker/handlers/youtube_upload.py backend/worker/main.py \
  backend/tests/worker/test_youtube_upload_handler.py \
  backend/tests/worker/test_worker_startup.py
git commit -m "feat: upload through durable youtube manager tasks"
```

### Task 4: Explicit Owned Asset Propagation

**Files:**
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/handlers_test.go`
- Modify: `backend/app/schemas/channel_agent.py`

**Interfaces:**
- Produces: manual-seed `constraints_json.input_asset_id` as AutoFlow request
  top-level `input_asset_id`.

- [ ] **Step 1: Add a failing Go request-shape test**

Build a `ProductionTaskRow` snapshot whose manual seed constraints are:

```go
map[string]any{
    "input_asset_id": "00000000-0000-0000-0000-000000000123",
    "source_strategy": "input_video",
    "planning_mode": "template",
}
```

Assert `AutoFlowRequestForTask()` returns that exact top-level asset ID,
`source_strategy == "input_video"`, and `source_policy == "owned_only"`.

- [ ] **Step 2: Run and confirm failure**

```bash
go test ./internal/channelops -run TestAutoFlowRequestForTaskOwnedInputAsset -count=1
```

- [ ] **Step 3: Implement propagation and safe validation**

Copy a valid UUID-like string to the request; empty values remain absent. The
manual seed still cannot select external source platforms for this canary.

- [ ] **Step 4: Run ChannelOps and AutoFlow validation tests**

```bash
go test ./internal/channelops
cd backend
.venv/bin/python -m pytest \
  tests/channel_agent/test_service.py \
  tests/autoflow/test_pipeline_builder.py \
  tests/autoflow/test_pipeline_policy.py -q
```

- [ ] **Step 5: Commit**

```bash
git add internal/channelops/handlers.go internal/channelops/handlers_test.go \
  backend/app/schemas/channel_agent.py
git commit -m "feat: pass owned canary assets to autoflow"
```

### Task 5: Deploy The Dedicated Publisher On 150

**Files:**
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Modify: `deploy/four-machine-topology.md`
- Modify: `docs/constructure/infra-services.md`

**Interfaces:**
- Produces: Swarm service `vp-youtube-publisher-swarm`.
- Produces: `node.labels.vp.publisher == true` placement on 150.

- [ ] **Step 1: Extend the failing shell harness**

Require one service create/update containing:

```text
WORKER_TYPE=youtube_publisher
WORKER_HOST=150-publisher
YOUTUBE_MANAGER_URL=http://10.0.0.150:18999
YOUTUBE_PUBLISH_ENABLED=true
PUBLIC_PUBLISH_ENABLED=false
node.labels.vp.publisher==true
```

Assert there is no credentials mount or `YOUTUBE_CREDENTIALS_DIR`, the service
is included in health checks and rollback snapshots, and repeat deployment is
idempotent.

- [ ] **Step 2: Run and confirm the shell test fails**

```bash
bash tests/test_vp_deploy_sync_extension.sh
```

- [ ] **Step 3: Add publisher deploy helpers**

Add `VP_PUBLISHER_SERVICE`, `VP_PUBLISHER_CONSTRAINT`,
`vp_publisher_env()`, and `vp_deploy_publisher()`. Reuse the Python worker
image, add the pipeline network and a scratch volume, set concurrency to one,
and label only `VP_MANAGER_NODE`. Include optional-service creation/removal in
snapshot rollback logic.

Call the existing HTTP health helper for `/api/auth/status` before creating the
publisher, then let the worker perform the authenticated/quota check before
each side effect.

- [ ] **Step 4: Update topology documentation**

Document the new service on 150, credential isolation, the dedicated stream,
and the rule that 126 never receives the publisher label.

- [ ] **Step 5: Run deploy tests**

```bash
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_macos_deploy_paths.sh
```

- [ ] **Step 6: Commit**

```bash
git add deploy/swarm/deploy-sync-extension.sh \
  tests/test_vp_deploy_sync_extension.sh \
  deploy/four-machine-topology.md docs/constructure/infra-services.md
git commit -m "ops: deploy isolated youtube publisher"
```

### Task 6: Backlog Quarantine And Canary Runner

**Files:**
- Create: `backend/app/services/channelops_quarantine.py`
- Create: `backend/tests/services/test_channelops_quarantine.py`
- Create: `scripts/quarantine_channelops_backlog.py`
- Create: `scripts/run_vp_unlisted_canary.py`
- Create: `tests/test_vp_unlisted_canary_scripts.sh`

**Interfaces:**
- Produces: dry-run/apply quarantine report with retained IDs.
- Produces: canary evidence JSON containing asset, channel, task, job,
  operation, publication, video, metrics, privacy, and schedule state.

- [ ] **Step 1: Write quarantine service tests**

Seed one halted channel with terminal and non-terminal tasks, jobs, nodes, and
queue rows. Assert dry-run changes nothing. Assert apply:

- leaves measured/publication-backed rows unchanged;
- cancels non-terminal jobs and nodes;
- moves non-terminal tasks without publications to `held` with
  `operator_quarantine_before_unlisted_canary`;
- dead-letters only non-terminal queue rows for that channel;
- is idempotent on a second apply.

- [ ] **Step 2: Write shell contract tests for both scripts**

Assert quarantine defaults to dry-run and requires `--apply` for mutation.
Assert the canary runner contains schedule close in a `finally` block, uses
`unlisted`, sets `external_asset_auto_publish=false`, limits cadence to one,
uses the atomic intake pause after exactly one task, blocks only new
`agent_tick` and `ingest_discovery` intake, keeps all existing downstream work
and mature metrics running on success, fully halts on failure, and refuses to
run while pre-existing runnable jobs remain.

- [ ] **Step 3: Run and confirm failures**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_channelops_quarantine.py -q
cd ..
bash tests/test_vp_unlisted_canary_scripts.sh
```

- [ ] **Step 4: Implement quarantine CLI**

The CLI accepts `--channel-id`, `--evidence`, and optional `--apply`, reads
`DATABASE_URL`, uses the service transaction, and writes a JSON report even in
dry-run mode. It never prints the database URL.

- [ ] **Step 5: Implement the canary runner**

The runner:

1. generates an 8-second 1080x1920 owned MP4 with FFmpeg;
2. uploads it to `/api/v1/assets/upload`;
3. writes explicit `license=owned`, `provenance=generated`, and generation
   timestamp metadata through its database session;
4. creates one private-by-default channel, lane, unlisted account/format, and
   manual seed with `input_asset_id`;
5. enables the channel and enqueues one guarded tick whose transaction creates
   exactly one task and atomically pauses intake;
6. opens the schedule only after no other runnable job remains, drains after
   the canary starts, and closes in `finally`; success keeps intake paused for
   downstream and mature metrics, while failure fully halts the channel;
7. waits for one publication, enqueues immediate promotion and metrics through
   existing APIs, and waits for one feedback snapshot;
8. verifies manager status is processed/unlisted and writes evidence under
   `.runtime/youtube-canary/`.

- [ ] **Step 6: Run script unit/contract checks**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_channelops_quarantine.py -q
cd ..
bash tests/test_vp_unlisted_canary_scripts.sh
backend/.venv/bin/python -m py_compile \
  scripts/quarantine_channelops_backlog.py \
  scripts/run_vp_unlisted_canary.py
```

- [ ] **Step 7: Commit**

```bash
git add backend/app/services/channelops_quarantine.py \
  backend/tests/services/test_channelops_quarantine.py \
  scripts/quarantine_channelops_backlog.py \
  scripts/run_vp_unlisted_canary.py \
  tests/test_vp_unlisted_canary_scripts.sh
git commit -m "ops: run one guarded youtube canary"
```

### Task 7: Full Verification, Deploy, And Live Canary

**Files:**
- Modify only when verification exposes a scoped defect.
- Generate: `.runtime/youtube-canary/*.json` and `.runtime/youtube-canary/*.mp4` (ignored evidence).

**Interfaces:**
- Consumes all prior tasks.
- Produces one real unlisted publication and feedback evidence.

- [ ] **Step 1: Run all required local checks**

```bash
cd backend
.venv/bin/python -m pytest
.venv/bin/python -m ruff check . || true
.venv/bin/python -m mypy app || true
cd ..
go test ./...
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_vp_colima_node.sh
bash tests/test_vp_production_smoke_script.sh
bash tests/test_macos_deploy_paths.sh
bash tests/test_vp_unlisted_canary_scripts.sh
```

Record existing Ruff/mypy baseline separately from changed-file checks.

- [ ] **Step 2: Review and push**

Use `superpowers:requesting-code-review`, address verified findings, fast-forward
to `main`, and push. Confirm `main...origin/main` is `0 0`.

- [ ] **Step 3: Let the scoped 150 controller deploy**

Observe the normal cron or run the same scoped command once:

```bash
/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh \
  --apply --project vp-app --project vp-feature-aggregator --project vp-pds
```

Require all VP services, including `vp-youtube-publisher-swarm`, to be `1/1`.
Verify the publisher is on `ccttww-lap`, app services are on `colima-127`, and
no VP task is on 126.

- [ ] **Step 4: Quarantine the old soak backlog**

Run dry-run first, inspect counts, then apply with evidence:

```bash
DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL" \
backend/.venv/bin/python scripts/quarantine_channelops_backlog.py \
  --channel-id feb629de-ce7f-4c29-936a-c937b08799ab \
  --evidence .runtime/youtube-canary/backlog-dry-run.json

DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL" \
backend/.venv/bin/python scripts/quarantine_channelops_backlog.py \
  --channel-id feb629de-ce7f-4c29-936a-c937b08799ab \
  --apply --evidence .runtime/youtube-canary/backlog-applied.json
```

- [ ] **Step 5: Run exactly one live canary**

```bash
DATABASE_URL="$VP_PYTHON_WORKER_DATABASE_URL" \
backend/.venv/bin/python scripts/run_vp_unlisted_canary.py \
  --api-url http://10.0.0.127:18080 \
  --youtube-manager-url http://10.0.0.150:18999
```

- [ ] **Step 6: Verify live invariants**

Require one new operation, one new publication, one unique video ID, manager
status processed/unlisted, feedback response, durable metrics queue work, no
public rows, no pending publisher messages, final schedule `CLOSED`, and no VP
tasks/consumers on 126. Restart the publisher and API services, then re-query
the same operation/publication/video to prove persistence.

The next approval is exactly `批准第四次 unlisted canary`; no earlier attempt
phrase authorizes another run.

- [ ] **Step 7: Complete branch cleanup**

Use `superpowers:verification-before-completion` and
`superpowers:finishing-a-development-branch`. Preserve the user's untracked
`vp_autonomous_production_feedback_loop_plan.md`.
