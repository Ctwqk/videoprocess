# YouTube Discovery Scheduler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in, auditable scheduler that materializes YouTube trend metadata into `discovery_signals` without downloading assets or creating, uploading, promoting, or publishing content.

**Architecture:** The Go ChannelOps runner remains the only scheduler and enqueues one channel-scoped `ingest_discovery` item per bounded bucket. A dedicated Go client calls a queue-authority-fenced Python endpoint, which reuses `YouTubeTrendIngester`, writes one durable run audit, and makes completed retries side-effect free. Existing agent ticks remain the sole owner that may later convert signals into reviewed production candidates.

**Tech Stack:** Go 1.25, Python 3.12, FastAPI, SQLAlchemy 2 async, PostgreSQL 16, Alembic, and pytest.

## Global Constraints

- Discovery defaults to disabled; deployment cannot enable an existing channel.
- Search writes metadata only and never creates a `production_task`.
- Public publication and promotion remain disabled.
- External-platform assets require explicit human review before upload or publication.
- Go remains the only production ChannelOps scheduler; add no Python timer or host cron.
- Queue authority stays channel scoped and fails closed across halt/quarantine races.
- An intake pause blocks only `agent_tick` and `ingest_discovery`; existing
  downstream work, reconciliation, and mature metrics remain eligible.
- Provider errors, URLs, credentials, and response bodies must not enter run audit errors.
- Do not deploy or place VideoProcess work on host 126.
- Every new service has focused tests and follows red-green-refactor.

---

### Task 1: Durable Discovery Run Schema

**Files:**
- Create: `backend/alembic/versions/029_channelops_discovery_ingestion_runs.py`
- Modify: `backend/app/models/channel_agent.py`
- Modify: `backend/tests/migrations/test_final_review3_postgres.py`
- Modify: `backend/tests/migrations/test_final_review4_postgres.py`
- Modify: `backend/tests/migrations/test_final_review10_postgres.py`
- Test: `backend/tests/migrations/test_channelops_discovery_ingestion_runs_postgres.py`

**Interfaces:**
- Produces: `DiscoveryIngestionRun`, uniquely keyed by channel, source, and bucket.
- Produces: Alembic head `029_channelops_discovery_ingestion_runs`.
- Consumes: existing channel and queue-item UUID keys.

- [ ] **Step 1: Write the failing PostgreSQL migration test**

Upgrade a fresh database and assert the table contains channel/queue IDs,
source, bucket, query version, status, counters, policy snapshot, timestamps,
and fixed error code. Insert duplicate channel/source/bucket and duplicate
non-null queue IDs and require both unique constraints to reject them. Then
downgrade to 028, assert the table is absent, and upgrade to head again.

- [ ] **Step 2: Run the test and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/migrations/test_channelops_discovery_ingestion_runs_postgres.py -q
```

Expected: FAIL because revision 029 and the model do not exist.

- [ ] **Step 3: Add the model and expand-first migration**

Define the constraints explicitly:

```python
UniqueConstraint(
    "channel_profile_id", "source", "scheduler_bucket",
    name="uq_discovery_ingestion_run_channel_source_bucket",
)
UniqueConstraint("queue_item_id", name="uq_discovery_ingestion_run_queue_item")
CheckConstraint("source = 'youtube_search'", name="ck_discovery_ingestion_run_source")
CheckConstraint(
    "status IN ('running','succeeded','failed')",
    name="ck_discovery_ingestion_run_status",
)
CheckConstraint("attempt_count >= 1", name="ck_discovery_ingestion_run_attempt_count")
```

Use `ON DELETE CASCADE` for channel, `ON DELETE SET NULL` for queue item,
non-negative counters, timezone-aware timestamps, JSON policy snapshot, and a
64-character error code. Update only tests that assert the current head; keep
the dedicated 028 migration test pinned to 028.

- [ ] **Step 4: Run migration tests and verify GREEN**

```bash
cd backend
.venv/bin/python -m pytest \
  tests/migrations/test_channelops_discovery_ingestion_runs_postgres.py \
  tests/migrations/test_final_review3_postgres.py \
  tests/migrations/test_final_review4_postgres.py \
  tests/migrations/test_final_review10_postgres.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/alembic/versions/029_channelops_discovery_ingestion_runs.py \
  backend/app/models/channel_agent.py backend/tests/migrations
git commit -m "feat: persist discovery ingestion runs"
```

---

### Task 2: Policy Parser And Idempotent Python Service

**Files:**
- Create: `backend/app/channel_agent/discovery_policy.py`
- Create: `backend/app/services/discovery_ingestion.py`
- Modify: `backend/app/channel_agent/trend_ingesters/youtube_search.py`
- Test: `backend/tests/channel_agent/test_discovery_policy.py`
- Test: `backend/tests/services/test_discovery_ingestion.py`
- Modify: `backend/tests/channel_agent/test_trend_ingester.py`

**Interfaces:**
- Produces: `DiscoveryPolicy.from_content_mix(value: object) -> DiscoveryPolicy`.
- Produces: `DiscoveryIngestionService.ingest(db, request, now) -> DiscoveryIngestionResult`.
- Produces: `TrendIngestResult(created_count, refreshed_count, expired_count, query_count)`.
- Consumes: `YouTubeManagerClient.search_videos` and a committed running queue item.

- [ ] **Step 1: Write failing policy tests**

Require defaults to be disabled, valid nested settings to parse exactly, and
wrong types or out-of-range values to raise `DiscoveryPolicyError`:

```python
assert DiscoveryPolicy.from_content_mix({}).enabled is False
policy = DiscoveryPolicy.from_content_mix({
    "youtube_discovery": {
        "enabled": True,
        "interval_minutes": 360,
        "max_queries_per_run": 3,
        "max_results_per_query": 10,
        "min_view_count": 1000,
        "region_code": "US",
    }
})
assert policy.enabled is True
```

Bounds are interval `60..1440`, queries `1..5`, results `1..25`, views
`0..1000000000`, and two uppercase ASCII letters for region.

- [ ] **Step 2: Run policy tests and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/channel_agent/test_discovery_policy.py -q
```

Expected: FAIL because the parser does not exist.

- [ ] **Step 3: Implement the frozen policy dataclass**

Use exact booleans and integers without string coercion. Support the legacy
top-level `region_code` only when the nested region is absent.

- [ ] **Step 4: Run policy tests and verify GREEN**

Run Step 2 again. Expected: PASS.

- [ ] **Step 5: Write failing ingester/service tests**

Prove lane ordering and query limits, provider argument bounds, create/refresh/
expire counts, converted-signal preservation, no inner commit, succeeded replay
without a second provider call, recent-running conflict, stale/failed reclaim,
provider rollback, and fixed non-sensitive error codes.

- [ ] **Step 6: Run service tests and verify RED**

```bash
cd backend
.venv/bin/python -m pytest \
  tests/channel_agent/test_trend_ingester.py \
  tests/services/test_discovery_ingestion.py -q
```

Expected: FAIL for missing counters and service behavior.

- [ ] **Step 7: Implement bounded ingestion and run idempotency**

Limit enabled lanes by policy, flush without committing inside the ingester,
and use a 15-minute stale-running threshold. A succeeded replay returns stored
counters. Failed/stale runs increment attempts. Provider failure rolls back
signal changes and stores one of `provider_auth`, `provider_quota`,
`provider_timeout`, or `provider_contract`. Recheck channel enabled/halted
before terminal commit. Estimate quota as `query_count * 100`.

- [ ] **Step 8: Run service tests and verify GREEN**

Run Step 6 again. Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add backend/app/channel_agent/discovery_policy.py \
  backend/app/channel_agent/trend_ingesters/youtube_search.py \
  backend/app/services/discovery_ingestion.py \
  backend/tests/channel_agent/test_discovery_policy.py \
  backend/tests/channel_agent/test_trend_ingester.py \
  backend/tests/services/test_discovery_ingestion.py
git commit -m "feat: ingest YouTube discovery signals idempotently"
```

---

### Task 3: Queue-Authority-Fenced FastAPI Endpoint

**Files:**
- Modify: `backend/app/api/channel_agent.py`
- Modify: `backend/app/schemas/channel_agent.py`
- Test: `backend/tests/api/test_channel_agent_discovery.py`

**Interfaces:**
- Produces: `POST /api/v1/channel-agent/internal/discovery/ingest`.
- Consumes: channel ID, queue item ID, literal source, and scheduler bucket.
- Returns: run identity, matching source/bucket identity, status, and counters.

- [ ] **Step 1: Write failing endpoint tests**

Use a real async database fixture and dependency override. Reject before any
provider call when the queue item is missing, wrong kind, not running, channel
mismatched, payload mismatched, channel disabled/halted, or policy disabled/
invalid. The accepted case returns `succeeded`; replay returns the same run ID
without a second provider request.

- [ ] **Step 2: Run endpoint tests and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/api/test_channel_agent_discovery.py -q
```

Expected: FAIL with route not found.

- [ ] **Step 3: Add strict schemas and endpoint**

Use UUID request fields and constrain source to `youtube_search`. Validate the
committed queue row without taking completion ownership from Go. Map authority,
policy, and in-progress errors to HTTP 409; missing rows to 404; provider errors
to HTTP 502 with fixed detail codes.

- [ ] **Step 4: Run endpoint tests and verify GREEN**

Run Step 2 again. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/app/api/channel_agent.py backend/app/schemas/channel_agent.py \
  backend/tests/api/test_channel_agent_discovery.py
git commit -m "feat: expose fenced discovery ingestion API"
```

---

### Task 4: Go Policy Scheduling And Queue Authority

**Files:**
- Create: `internal/channelops/discovery_policy.go`
- Modify: `internal/channelops/types.go`
- Modify: `internal/channelops/scheduler.go`
- Modify: `internal/channelops/queue.go`
- Test: `internal/channelops/discovery_policy_test.go`
- Modify: `internal/channelops/scheduler_test.go`
- Modify: `internal/channelops/integration_test.go`

**Interfaces:**
- Produces: `DiscoveryPolicyFromContentMix(map[string]any) (DiscoveryPolicy, error)`.
- Produces: `DiscoveryIdempotencyKey(channelID, source, bucket string) string`.
- Produces: channel-scoped queue kind `QueueIngestDiscovery`.
- Consumes: `ChannelProfileRow.ContentMixPolicyJSON`.

- [ ] **Step 1: Write failing policy and scheduler tests**

Mirror Python defaults and bounds. Enable one fixture channel, run the
scheduler twice in one six-hour bucket, and require exactly one priority-80
queue row with this key:

```go
"ingest_discovery:" + channelID + ":youtube_search:" + bucket
```

Prove default-disabled and invalid policies enqueue no discovery item while
normal agent ticks remain unchanged.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
go test ./internal/channelops -run 'Discovery|SchedulerRunOnce' -count=1
```

Expected: FAIL because the policy, key, and queue kind are missing.

- [ ] **Step 3: Implement policy parsing, scheduling, and authority**

Validate the full nested policy for cross-language parity. Enqueue channel ID,
source, bucket, and scheduler bucket. Extend every queue-authority CASE that
treats `agent_tick` and `learning_recompute` as payload-channel items to include
`ingest_discovery`; it must never be global.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run Step 2 again. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/channelops/discovery_policy.go \
  internal/channelops/discovery_policy_test.go internal/channelops/types.go \
  internal/channelops/scheduler.go internal/channelops/scheduler_test.go \
  internal/channelops/queue.go internal/channelops/integration_test.go
git commit -m "feat: schedule channel-scoped discovery ingestion"
```

---

### Task 5: Go Discovery Client And Queue Handler

**Files:**
- Create: `internal/channelops/discovery_client.go`
- Create: `internal/channelops/discovery_client_test.go`
- Modify: `internal/channelops/config.go`
- Modify: `internal/channelops/config_test.go`
- Modify: `internal/channelops/handlers.go`
- Modify: `internal/channelops/handlers_test.go`
- Modify: `internal/channelops/runner.go`
- Modify: `internal/channelops/runner_test.go`

**Interfaces:**
- Produces: `DiscoveryClient.Ingest(ctx, request) (DiscoveryObservation, error)`.
- Produces: strict `HTTPDiscoveryClient` using the Python API base URL.
- Produces: `HandlerService.HandleIngestDiscovery` outside a long row-lock transaction.
- Consumes: queue identity and `CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS`.

- [ ] **Step 1: Write failing client tests**

Use `httptest.Server` to assert the POST path and JSON fields. Reject non-2xx,
malformed JSON, source/channel/bucket mismatch, blank run ID, status other than
`succeeded`, and negative counters.

- [ ] **Step 2: Run client tests and verify RED**

```bash
go test ./internal/channelops -run 'DiscoveryClient' -count=1
```

Expected: FAIL because the client does not exist.

- [ ] **Step 3: Implement the strict client**

Use JSON content type, a dedicated timeout, bounded response parsing, and error
text that excludes provider bodies. Send queue item ID, channel ID, source, and
bucket.

- [ ] **Step 4: Write failing handler/config tests**

Require timeout default 120 and range `30..300`. Missing queue identity must
fail without a client call; a matching succeeded observation passes; client
errors reach normal queue retry. `ClaimableKinds` includes discovery only when
a client is configured.

- [ ] **Step 5: Run handler/config tests and verify RED**

```bash
go test ./internal/channelops -run 'Discovery|Config' -count=1
```

Expected: FAIL for missing handler/config behavior.

- [ ] **Step 6: Wire client and split handler path**

Add `DiscoveryTimeout` to config, construct `HTTPDiscoveryClient` from
`AutoFlowBaseURL`, and add it to `HandlerService`. Special-case discovery in
`Handle` so the external call does not hold the queue execution-fence
transaction. Runner completion/retry remains lease-aware.

- [ ] **Step 7: Run ChannelOps tests and verify GREEN**

```bash
go test ./internal/channelops -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add internal/channelops/discovery_client.go \
  internal/channelops/discovery_client_test.go internal/channelops/config.go \
  internal/channelops/config_test.go internal/channelops/handlers.go \
  internal/channelops/handlers_test.go internal/channelops/runner.go \
  internal/channelops/runner_test.go
git commit -m "feat: execute discovery ingestion through ChannelOps"
```

---

### Task 6: Canary Classification, Deployment Contract, And Runbook

**Files:**
- Modify: `scripts/run_vp_unlisted_canary.py`
- Modify: `backend/tests/services/test_unlisted_canary_runner.py`
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Modify: `deploy/four-machine-topology.md`

**Interfaces:**
- Produces: discovery classified as non-publishing maintenance for preflight.
- Produces: runner environment `CHANNELOPS_DISCOVERY_TIMEOUT_SECONDS=120`.
- Documents: disabled-by-default activation and rollback.

- [ ] **Step 1: Write failing canary/deploy tests**

Show queued/running discovery does not enter `unsafe_queue_item_ids`, while a
publishing queue item still does. Require one timeout environment entry on the
Go runner and no new service or 126 target.

- [ ] **Step 2: Run tests and verify RED**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py \
  -k 'backlog and discovery' -q
cd ..
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: FAIL because discovery is not classified or deployed.

- [ ] **Step 3: Implement classification and runner environment**

Add discovery beside `cleanup_expired` in the fixed non-publishing set. Add the
timeout only to `vp-channel-agent-runner-swarm`; no publisher or worker receives
it.

- [ ] **Step 4: Update the topology runbook**

Document policy JSON, initial six-hour limits, audit SQL, human-review and
no-public boundaries, disabled deployment state, rollback, and continued 126
exclusion.

- [ ] **Step 5: Run contracts and verify GREEN**

```bash
cd backend
.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py \
  -k 'backlog and discovery' -q
cd ..
bash tests/test_vp_deploy_sync_extension.sh
bash -n deploy/swarm/deploy-sync-extension.sh
git diff --check
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/run_vp_unlisted_canary.py \
  backend/tests/services/test_unlisted_canary_runner.py \
  deploy/swarm/deploy-sync-extension.sh tests/test_vp_deploy_sync_extension.sh \
  deploy/four-machine-topology.md
git commit -m "ops: deploy disabled YouTube discovery scheduling"
```

---

### Task 7: Full Verification, Review, Push, And Disabled Deploy

**Files:**
- Modify only files required by confirmed review findings.
- Runtime evidence: `.runtime/youtube-canary/unlisted-canary-preflight-<run-id>.json`.

**Interfaces:**
- Consumes: exact successful GitHub Actions SHA.
- Produces: API/runner/migration deployed at one commit with discovery disabled.

- [ ] **Step 1: Run full repository verification**

```bash
cd backend
.venv/bin/python -m pytest
.venv/bin/python -m ruff check . || true
.venv/bin/python -m mypy app || true
cd ..
go test ./...
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_channelops_soak_watch.sh
bash tests/test_vp_unlisted_canary_scripts.sh
git diff --check
```

Expected: pytest, Go, and shell contracts pass. Record existing Ruff/mypy
advisories and fix every new-file violation.

- [ ] **Step 2: Request review and address confirmed findings**

Review schema constraints, queue authority, idempotency, transaction
boundaries, error redaction, policy parity, and default-disabled deployment.
Re-run focused and full checks after accepted fixes.

- [ ] **Step 3: Fast-forward main, push, and require exact-SHA CI success**

The latest `ci.yml` push run for the exact commit must be
`completed/success`; an older successful run does not qualify.

- [ ] **Step 4: Deploy only VideoProcess projects through 150**

```bash
/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh \
  --apply --project vp-app --project vp-feature-aggregator
```

Do not deploy or modify PDS for this VideoProcess-only increment. PDS remains
an independent repository and its automatic deployment is not triggered by a
VideoProcess push.

- [ ] **Step 5: Prove disabled production state**

Require migration head 029, all services at desired replicas, normal services
on 127, GPU/publisher on 150, zero VP tasks on 126, every production discovery
policy absent or disabled, zero deployment-created discovery runs/queue items,
schedule `CLOSED`, zero public/active-upload/unsafe rows, and soak watcher still
disabled.

- [ ] **Step 6: Re-run read-only YouTube preflight**

Require exact source/deployed SHA, closed schedule, empty publishing backlog,
authenticated manager, publisher/runner `1/1`, Redis pending zero,
`external_side_effects=false`, evidence mode `0600`, and no sensitive strings.

- [ ] **Step 7: Leave activation behind the live-canary gate**

Do not patch production discovery policy or run live mode without the exact
per-attempt phrase:

```text
批准第四次 unlisted canary
```

After that canary succeeds, activation begins at interval 360, max queries 3,
max results 10, and the existing one-publication-per-24h guarded soak.
