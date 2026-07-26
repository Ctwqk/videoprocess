# Legacy Worker Event Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a fail-closed, audited maintenance path that resolves only an exact set of terminal legacy worker events without weakening normal Redis pending checks.

**Architecture:** A focused service validates an operator-provided Redis ID/hash manifest against the complete PEL and locked Postgres terminal state. It records immutable evidence before ACKing exact IDs, then records acknowledgement and proves the PEL is empty. A thin CLI supplies dry-run/apply modes and sanitized evidence output.

**Tech Stack:** Python 3.12, FastAPI backend models, SQLAlchemy async ORM, Alembic, redis-py asyncio, pytest.

## Global Constraints

- Default operation mode is dry-run.
- Apply requires an explicit operator identity and exact event ID/hash manifest.
- Only `node_failed` events missing both `worker_id` and `started_at` are eligible.
- Jobs and nodes must be `CANCELLED`; tasks must be `held`; channels must be halted.
- The global schedule must be `CLOSED`, have no guarded job, and have no queued/running nodes.
- Archive records commit before Redis acknowledgement.
- Redis stream entries are never deleted.
- Normal canary and soak pending checks remain unchanged and require raw pending zero.
- Host 126 must not participate.
- No upload, publication, visibility change, or canary execution is permitted.

---

### Task 1: Durable Resolution Model

**Files:**
- Create: `backend/app/models/legacy_worker_event_resolution.py`
- Create: `backend/alembic/versions/033_legacy_worker_event_resolutions.py`
- Modify: `backend/app/models/__init__.py`
- Test: `backend/tests/models/test_legacy_worker_event_resolution.py`

**Interfaces:**
- Produces: `LegacyWorkerEventResolution`, keyed by `(redis_stream, consumer_group, message_id)`.
- Consumes: existing `Base`, `UUIDPrimaryKeyMixin`, SQLAlchemy JSON/UUID/timestamp conventions.

- [ ] **Step 1: Write the failing model and migration contract tests**

Assert exact table name, unique identity, nonempty checks, SHA-256 length check,
JSON payload field, terminal-state snapshots, operator identity, and nullable
acknowledgement timestamp.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cd backend && python3 -m pytest tests/models/test_legacy_worker_event_resolution.py -q`

Expected: collection/import failure because the model and migration do not
exist.

- [ ] **Step 3: Add the minimal model and Alembic migration**

Define `LegacyWorkerEventResolution` with immutable event identity, payload,
terminal-state snapshots, operator, reason, `recorded_at`, and
`acknowledged_at`. Add database uniqueness and nonempty/hash checks.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run: `cd backend && python3 -m pytest tests/models/test_legacy_worker_event_resolution.py -q`

Expected: PASS.

- [ ] **Step 5: Commit the model**

```bash
git add backend/app/models/legacy_worker_event_resolution.py backend/app/models/__init__.py backend/alembic/versions/033_legacy_worker_event_resolutions.py backend/tests/models/test_legacy_worker_event_resolution.py
git commit -m "feat(channelops): archive legacy worker events"
```

### Task 2: Fail-Closed Resolution Service

**Files:**
- Create: `backend/app/services/legacy_worker_event_resolution.py`
- Test: `backend/tests/services/test_legacy_worker_event_resolution.py`

**Interfaces:**
- Produces: `parse_expected_event(value: str) -> ExpectedLegacyEvent`.
- Produces: `canonical_payload_sha256(payload: Mapping[str, str]) -> str`.
- Produces: `resolve_legacy_worker_events(db, redis_client, request) -> LegacyEventResolutionReport`.
- Consumes: `LegacyWorkerEventResolution`, `RuntimeSchedule`, `Job`, `NodeExecution`, `ProductionTask`, and `ChannelProfile`.

- [ ] **Step 1: Write failing pure-function tests**

Cover strict Redis message ID parsing, lowercase 64-character SHA-256 parsing,
duplicate ID rejection, deterministic mapping hashing, and type rejection.

- [ ] **Step 2: Run the pure-function tests and verify RED**

Run: `cd backend && python3 -m pytest tests/services/test_legacy_worker_event_resolution.py -q`

Expected: import failure because the service does not exist.

- [ ] **Step 3: Implement expectation parsing and canonical hashing**

Add frozen request/report dataclasses and strict validation without database or
Redis side effects.

- [ ] **Step 4: Run the pure-function tests and verify GREEN**

Run the command from Step 2. Expected: the pure-function tests pass.

- [ ] **Step 5: Write failing dry-run and rejection tests**

Use a real SQLite SQLAlchemy session plus a stateful fake Redis client. Assert
dry-run performs no insert and no `XACK`. Cover PEL set drift, payload drift,
execution claims, `node_completed`, malformed UUIDs, active schedule, guarded
job, active nodes, non-cancelled job/node, non-held task, and non-halted
channel.

- [ ] **Step 6: Run the service tests and verify RED**

Run the command from Step 2. Expected: failures because resolution validation
is not implemented.

- [ ] **Step 7: Implement locked validation and dry-run reporting**

Require an exact complete PEL manifest and all terminal database invariants.
Return only sanitized event identity and state in the report.

- [ ] **Step 8: Run the service tests and verify GREEN**

Run the command from Step 2. Expected: dry-run and rejection tests pass.

- [ ] **Step 9: Write failing apply and recovery tests**

Assert the audit row is committed before `XACK`, exact IDs only are
acknowledged, partial ACK fails, existing exact rows are reused, and a rerun
after an already-observed ACK can complete without inventing a new event.

- [ ] **Step 10: Run the apply tests and verify RED**

Run the command from Step 2. Expected: apply tests fail because acknowledgement
is not implemented.

- [ ] **Step 11: Implement archive, ACK, and recovery**

Insert or verify immutable audit rows, commit, re-read exact payloads, ACK the
expected IDs, update acknowledgement timestamps, and require final PEL zero.

- [ ] **Step 12: Run the service tests and verify GREEN**

Run the command from Step 2. Expected: all service tests pass.

- [ ] **Step 13: Commit the service**

```bash
git add backend/app/services/legacy_worker_event_resolution.py backend/tests/services/test_legacy_worker_event_resolution.py
git commit -m "feat(channelops): resolve terminal legacy events"
```

### Task 3: Operator CLI And Evidence

**Files:**
- Create: `backend/app/channel_agent/legacy_event_resolution_cli.py`
- Test: `backend/tests/channel_agent/test_legacy_event_resolution_cli.py`
- Modify: `docs/superpowers/specs/2026-07-25-managed-vision-worker-deployment-design.md`

**Interfaces:**
- Produces: `python -m app.channel_agent.legacy_event_resolution_cli`.
- Consumes: the Task 2 service and `DATABASE_URL` / `REDIS_URL`.

- [ ] **Step 1: Write failing CLI tests**

Assert dry-run default, `--apply` operator requirement, repeated exact
expectations, dependency cleanup, exit status, sanitized evidence, and `0600`
permissions. Verify no secret or connection URL appears in serialized output.

- [ ] **Step 2: Run the CLI tests and verify RED**

Run: `cd backend && python3 -m pytest tests/channel_agent/test_legacy_event_resolution_cli.py -q`

Expected: module import failure.

- [ ] **Step 3: Implement the thin async CLI**

Parse arguments, create the SQLAlchemy engine and Redis client, invoke the
service, write evidence atomically with mode `0600`, and close both clients in
all paths.

- [ ] **Step 4: Run the CLI tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 5: Update the managed vision design**

Replace the unconditional no-ACK statement with the exact terminal-resolution
contract and reference this design.

- [ ] **Step 6: Commit the CLI**

```bash
git add backend/app/channel_agent/legacy_event_resolution_cli.py backend/tests/channel_agent/test_legacy_event_resolution_cli.py docs/superpowers/specs/2026-07-25-managed-vision-worker-deployment-design.md
git commit -m "feat(channelops): add legacy event resolution CLI"
```

### Task 4: Verification And Production Closure

**Files:**
- Modify only if verification reveals a scoped defect.

**Interfaces:**
- Consumes: all prior tasks.
- Produces: deployment and maintenance evidence; no live canary.

- [ ] **Step 1: Run targeted and full backend checks**

```bash
cd backend
python3 -m pytest tests/models/test_legacy_worker_event_resolution.py tests/services/test_legacy_worker_event_resolution.py tests/channel_agent/test_legacy_event_resolution_cli.py tests/services/test_unlisted_canary_runner.py -q
python3 -m pytest
python3 -m ruff check .
python3 -m mypy app
```

- [ ] **Step 2: Run cross-project checks**

```bash
cd frontend && npm install && npm run build && npm run lint
cd ../backend-go && go test ./...
cd .. && bash scripts/test_ci_workflow_contract.sh
git diff --check
```

- [ ] **Step 3: Push and verify the exact-SHA automatic deployment**

Push `main`, wait for GitHub Actions success, then verify app, feature
aggregator, managed GPU, vision, and publisher services use the exact commit.
Verify all running tasks remain off host 126.

- [ ] **Step 4: Run a production dry-run**

Invoke the CLI with the three reviewed Redis ID/hash pairs but without
`--apply`. Preserve mode-`0600` evidence and require the exact terminal
candidates to match with zero mutation.

- [ ] **Step 5: Apply the exact resolution**

Invoke the same manifest with `--apply` and the operator identity. Require three
durable audit rows, `XACK=3`, final PEL zero, and unchanged upload/publication
counts.

- [ ] **Step 6: Re-run read-only canary preflight**

Require schedule `CLOSED`, empty workload backlog, healthy exact consumer
identities, raw Redis pending zero, authenticated YouTubeManager, and no upload
or publication side effect.

- [ ] **Step 7: Stop before any sixth canary**

Report that the maintenance blocker is closed. A sixth live unlisted canary
still requires a new explicit single-canary authorization.
