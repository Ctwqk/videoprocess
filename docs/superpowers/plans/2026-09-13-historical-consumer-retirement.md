# Historical Consumer Retirement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Repair multi-generation consumer retirement and consolidate all local work into main.

**Architecture:** Extend existing pins to version 2 while preserving version 1. Capture only required bounded ancestor chains, revalidate them under the existing deployment authority and retire them atomically per stream.

**Tech Stack:** Python, PostgreSQL 16, Redis 7.4 Lua, Docker Swarm, Git.

**Spec:** `docs/superpowers/specs/2026-09-13-historical-consumer-retirement-design.md`

## Global Constraints

- At most 64 retiring identities per service and 260 total pinned identities.
- Existing version 1 bytes, SQL guard and deployment ownership remain compatible.
- CLOSED/no work, zero PEL/lag, current readiness, revoked/expired chain and unknown-identity refusal remain mandatory.
- Existing timeouts, schedule and uncertain-attempt fences do not change.
- Parent alone owns SSH, Docker, PG, Redis, integration commits and pushes.
- No publishing, activation, node 126 deployment or unrelated features.

## Task 1: Pure Protocol And Lua

Files: `backend/app/services/registered_consumer_reconcile.py`,
`backend/tests/services/test_registered_consumer_history.py`,
`backend/tests/services/registered_consumer_history_fixtures.py`,
`backend/tests/services/test_registered_consumer_reconcile_redis.py`.

Interfaces: preserve existing APIs; add `WorkerPin.ancestors=()`,
`WorkerPin.retiring`, `EvalCommand.predecessors`, version 2 decoding and fixed
Lua. Existing test helpers `document`, `decode`, `facts`, `inventories` provide
the legacy fixture; new helpers build extra revoked generations explicitly.

- [x] Write and observe RED for version 2 multi-generation decode/assessment.

```python
from tests.services.test_registered_consumer_reconcile import document, decode

def test_v2_empty_ancestry_round_trip():
    payload = document()
    payload['version'] = 2
    for worker in payload['workers']:
        worker['ancestors'] = []
    pins = decode(payload)
    assert pins.version == 2
    assert all(worker.retiring == (worker.predecessor,) for worker in pins.workers)
```

- [x] Add bounded ancestors, exact chain checks, all-old inventory assessment
  and per-stream versioned Lua/result validation, preserving version 1 bytes.
- [x] Test three-generation success, missing/foreign/forked identities,
  duplicate/overflow, stale current, young old consumers, active leases,
  zero/partial old-name presence and non-replay after uncertainty.
- [x] Run old and new pure tests; prepare opt-in real Redis cases for parent.
- [x] Parent reviews and commits this tested unit without staging other units.

## Task 2: Capture And Managed Transport

Files: `backend/app/services/registered_consumer_reconcile_job.py`,
new `backend/app/services/registered_consumer_history_capture.py`,
`backend/tests/services/test_registered_consumer_reconcile_job.py`,
new `backend/tests/services/test_registered_consumer_history_capture.py`,
the native capture-result equality check in
`deploy/swarm/worker-admission-transaction.py` and its
`tests/test_registered_runtime_deploy.py` tests.

Interfaces: `build_capture_pins(..., history=None)` accepts a mapping from each
fixed service to its newest-first retiring IdentityPin sequence. Native current
capture supplies that mapping; legacy callers remain version 1. The helper
`capture_history(connection, client, snapshot, baseline)` returns this mapping.

- [x] Write failing tests proving historical capture reaches an old Redis
  identity through absent intermediate consumers and refuses a broken link.
- [x] Implement bounded fixed-stream Redis reads and recursive read-only DB
  capture to the oldest required name, retaining complete grant provenance.
- [x] Extend stdlib-only binding validation to version 2 without importing ORM
  dependencies on the deploy host. Bind command hashes to the complete pin set.

```python
def test_legacy_binding_still_valid(tmp_path):
    from tests.services.test_registered_consumer_reconcile_job import setup_protocol
    job, request, files, binding = setup_protocol(tmp_path)
    job.validate_binding(binding)
    assert binding['pin_json'] == request.pins.canonical_json
```

- [x] Keep 15-second capture bound, direct no-retry Redis connection and bounded
  cleanup. Test changed identities, unknown names, overflow and cancellation.
- [ ] Run managed-job and new capture tests; parent reviews and commits unit.

## Task 3: Restricted Guard And Runtime

Files: new `backend/alembic/versions/045_registered_consumer_history.py`,
`backend/app/services/registered_consumer_reconcile_runtime.py`,
`backend/app/services/worker_control_role_cli.py`,
`backend/app/services/worker_deployment_cli.py`,
`scripts/channelops_policy_snapshot_preflight.py` release-head constant,
migration/runtime/role/head tests.

Interfaces: new SQL guard name from spec with the existing typed row shape;
runtime selects it only for version 2 and passes all `worker.retiring` IDs.

- [x] Write failing migration/runtime tests for new guard, bound and selection.

```python
def test_release_head_includes_historical_guard():
    from app.services.worker_deployment_cli import EXPECTED_MIGRATION_HEAD
    assert EXPECTED_MIGRATION_HEAD == '045_registered_consumer_history'
```

- [x] Derive the new guard from existing 039/041 safety semantics, keep old
  guard untouched, enforce bounded complete monotonic same-service chains,
  revoke PUBLIC and add only the operator function allowlist entry.
- [x] Extend endpoint and row decoding checks across every retiring pin while
  preserving all timeout/authority/cleanup behavior.
- [x] Run focused offline tests and parent-only real PG/Redis qualification;
  verify wrong roles, chain changes, row locking and rollback cleanup.
- [x] Parent reviews and commits the integrated fix after full backend checks.

## Task 4: Consolidate And Verify

- [x] Save original refs, worktree status and untracked document digest in an
  ignored audit artifact. Confirm live origin/main and absence of origin/master.
- [ ] Merge all five uncovered branch tips into the fixed integration branch;
  resolve conflicts against current behavior, preserving new tests and APIs.
- [ ] Run full backend required checks, frontend install/build/lint, Go and
  deployment contracts on final merged source. Independently review changes.
- [ ] Fast-forward local main and push normally. Verify exact CI/native deploy
  without replay, manual metadata deletion, forced updates or fake receipts.
- [ ] Fast-forward every clean existing worktree and remaining local branch;
  preserve dirty/untracked work instead of resetting it. Update covered remote
  feature branches to final main using normal fast-forward pushes.
- [x] Dry-run and prune only the eight proven missing worktree metadata entries.
- [ ] Verify all original branch tips are ancestors, every local/remote branch
  and existing worktree HEAD matches main, and original document digest matches.
