# Task 2 Fix Round 3 Report

Status: fix round 3 implementation complete; re-review pending

Base implementation:
`ce25840 feat(workers): register python consumers before redis`

Prior fixups:

- `38fc590 fixup! feat(workers): register python consumers before redis`
- `2da204b fixup! feat(workers): register python consumers before redis`

This report is included in the round 3 fixup commit.

## Round 3 Findings Closed

- Registered affinity now validates the orchestrator dispatch proof before the
  affinity decision. A defer performs zero `XADD` and zero `XACK`, leaves the
  exact original message pending, and changes no payload hash, message ID, or
  dispatch key. Legacy unregistered bounce remains compatible.
- Added dispatch delivery and resolution state. Cancellation before delivery
  becomes undelivered-cancelled without Redis. Delivered cancellation uses
  exact control-plane authorize/require/ack functions around `XACK`; worker or
  receipt ACKs also mark the exact dispatch acknowledged. Deletion accepts only
  exact-acknowledged or undelivered-cancelled dispatches.
- PostgreSQL cancellation uses an auditable no-lock candidate scan and then
  locks each item in job, node, sorted registration, attestation, dispatch
  order. A PG16 cleanup-versus-cancel test holds the registration fence and
  proves cleanup and cancellation serialize without deadlock.
- Startup recovery uses the database-clock
  `vp_recover_registered_worker_node` function. Live claims are untouched,
  expired/revoked unresolved claims are held without duplicate dispatch, and a
  resolved stale claim recovers exactly once. A partial unique index prevents
  two unresolved initial dispatches for one node.
- Worker node claim and source-delivery attestation are atomic in
  `vp_claim_worker_node`. A real worker principal can execute that exact
  function but cannot directly `UPDATE node_executions`. The design and Task 4
  plan remove broad node and YouTube operation UPDATE privileges.
- Dispatch delivery now commits `pending -> attempting` before Redis. The Lua
  marker has no TTL. A stale attempt with a marker records the exact message;
  a missing marker transitions to `uncertain`, performs zero `XADD`, and
  requires repair. Concurrent reconcilers create one message.
- Event accept/alias/quarantine and cleanup share the reviewed lock order. A
  PG16 race proves cleanup waits behind a late alias job lock and then fails
  closed on the newly committed unacknowledged delivery.
- URL download is a zero-input deterministic
  `IntermediateArtifactCache` node. The handler no longer owns a separate
  remote cache, and a later job reuses the normal DB-backed cached artifact.
- MinIO has finite connect/read/operation deadlines and retries. Registered
  remote saves use claim-unique staging keys. Save runs on a dedicated daemon
  thread, so an SDK call that ignores every deadline cannot hold
  `asyncio.run()` or process exit on the default executor; any late write can
  target only staging and is janitor-owned.
- The design and Task 3/Task 4 plan now document atomic claim/attestation,
  cancellation reconciliation, startup recovery, non-expiring markers,
  Redis `noeviction` plus persistence/readiness, exact control-role functions,
  Go dispatch files, MinIO staging, and immutable Docker secrets.

## RED Evidence

- Registered affinity initially emitted a replacement `XADD` and ACKed the
  proven source message; the new zero-call assertions failed.
- Delivered but unclaimed cancellation initially had no dispatch resolution
  and deletion accepted the orphaned PEL message.
- An 11-minute live registered claim initially reset to pending, while an
  expired unresolved claim could stage a competing initial dispatch.
- The worker claim path initially performed direct node mutation before a
  separate attestation call.
- Redis-success/database-failure replay initially depended on an expiring
  marker and could mint another message after marker loss.
- Cleanup and late alias acceptance initially used different lock order and
  were not serialized by the job authority lock.
- Remote URL cache lookup had no matching cache fill after a miss.
- A MinIO call that never returned kept the default executor alive after the
  coroutine timeout.
- PG16 first exposed that receipt ACK attempted a direct
  `FOR UPDATE worker_task_dispatches`; the final path uses a receipt/authorized
  attestation-bound SECURITY DEFINER function instead.

## Verification

- Focused Task 2 startup/admission/lifecycle/YouTube/authority/handler/event/
  cache/storage suite: `393 passed, 2 skipped, 5 warnings in 3.66s`.
- PostgreSQL 16 migration, downgrade/static surface, separate principals,
  atomic claim, exact cancellation, recovery, marker/receipt authority,
  cleanup-versus-alias, cleanup-versus-cancel, and revoke/takeover suite:
  `3 passed in 9.45s`.
- Full backend: `1246 passed, 74 skipped, 17 warnings in 71.96s`.
- Targeted Ruff: `All checks passed!`.
- Targeted Mypy with skipped transitive imports:
  `Success: no issues found in 13 source files`.
- `git diff --check`: passed.

## Remaining Concerns And Boundaries

- No deployment, push, production mutation, canary, upload, schedule change,
  or publication occurred.
- Migration 034 remains undeployed and requires re-review before Task 4 grants
  the exact worker and independent orchestrator-control function surfaces.
- Redis `noeviction`, persistence/readiness, exact EVAL/GET/SET/XADD ACLs,
  marker janitor, and uncertain-dispatch operator repair are Task 4 deployment
  requirements. Registered traffic must remain drained/held until they exist.
- Daemon-thread timeout cannot stop an uncooperative SDK call. Safety comes
  from the unique staging namespace and janitor; process convergence no longer
  waits for that thread.
- Task 3 Go registration and Task 4 role/deployment wiring remain pending.
- Existing `datetime.utcnow()` deprecation warnings remain outside this fix.
- Default publication remains private/unlisted; registered-worker YouTube is
  an intentional requirement and public publication remains disabled without
  explicit human review.

# Task 2 Fix Round 4 Report

Status: round-4 implementation complete; independent re-review pending

Base reviewed: `0c8a188`

## Files Changed

Implementation and schema:

- `backend/alembic/versions/034_worker_registrations.py`
- `backend/app/api/artifacts.py`
- `backend/app/channel_agent/staging_object_janitor_cli.py`
- `backend/app/models/artifact.py`
- `backend/app/models/registered_worker_event_receipt.py`
- `backend/app/orchestrator/artifact_cache.py`
- `backend/app/orchestrator/engine.py`
- `backend/app/services/external_url_identity.py`
- `backend/app/services/job_execution_authority.py`
- `backend/app/services/registered_worker_event_receipt.py`
- `backend/app/services/staging_object_janitor.py`
- `backend/app/services/worker_storage_readiness.py`
- `backend/app/services/youtube_upload_operations.py`
- `backend/worker/handlers/url_download.py`
- `backend/worker/main.py`

Tests:

- `backend/tests/channel_agent/test_staging_object_janitor_cli.py`
- `backend/tests/migrations/test_worker_control_plane_postgres.py`
- `backend/tests/migrations/test_worker_registrations_postgres.py`
- `backend/tests/orchestrator/test_artifact_cache.py`
- `backend/tests/services/test_registered_worker_event_receipt.py`
- `backend/tests/services/test_staging_object_janitor.py`
- `backend/tests/services/test_worker_storage_readiness.py`
- `backend/tests/services/test_youtube_upload_operations.py`
- `backend/tests/worker/test_worker_affinity_redis.py`
- `backend/tests/worker/test_worker_startup.py`

Design/report:

- `docs/superpowers/plans/2026-07-26-production-worker-registration.md`
- `docs/superpowers/specs/2026-07-26-production-worker-registration-design.md`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-2-report.md`
- `.superpowers/sdd/2026-07-26-production-worker-registration/progress.md`
  was already modified when round 4 started and was not edited by this
  implementer; it remains in the commit because the request says to commit all
  tracked changes.

## Per-Finding Resolution

1. Added immutable `worker_event_emissions` outbox rows binding source
   attestation, job/node, registration/epoch/worker/start claim, event
   stream/group/message ID, canonical payload hash/JSON, and event type.
   Registered event publication is durable prepare, idempotent exact Redis
   `XADD`, then fenced emitted recording. Source-task authorization stores the
   exact emission ID and pre-XACK checks lock and validate the active
   job/node, registration, attestation, emission, receipt/delivery, and exact
   dispatch. Observer handoff repairs only a matching prepared emission.
   Recovery and cleanup retain prepared/emitted authority. Cancellation-only
   ACK now requires no event-emission row in any state.
2. Removed registered Python paths' direct `SELECT ... FOR UPDATE`
   dependency. Claim checks, artifact insert, event outbox mutation, and
   YouTube reserve/transition use fixed-search-path, PUBLIC-revoked
   `SECURITY DEFINER` functions. A real restricted login executes
   `_claim_node_execution`, `_persist_artifact_for_current_claim`, registered
   event publication/task ACK, and `YouTubeUploadOperationStore`; direct
   artifact/YouTube insert, node update, and broad DML remain denied.
3. `vp_recover_registered_worker_node` returns `terminal` unless the job is
   `RUNNING` and node is `QUEUED|RUNNING`. It preserves `retry_count` and
   explicitly resets all claim/retry-owned timestamps, errors, input/output
   pointers, and progress. Real-role terminal job/node snapshots remain
   byte-for-byte unchanged.
4. Added preferred-host PEL scanning and exact-message `XCLAIM` during the
   20-second affinity window. It verifies the unchanged stream entry and skips
   messages already owned by the preferred consumer, preventing duplicate
   self-processing. Registered defer performs zero `XADD`/`XACK`; arbitrary
   consumers cannot use this path before relaxation. Redis 7.4 integration
   covers ownership, exact identity, no replacement, and repeated scans.
5. Replaced Task 4 direct artifact and YouTube DML with exact worker APIs.
   Runtime schedule state is only read/locked through claim authority; workers
   receive no schedule mutation surface. Plan/design now deny direct
   INSERT/UPDATE/DELETE on artifacts, schedules, upload operations, nodes, and
   authority tables.
6. Event/task/cancellation ACK paths now use job/node, sorted registration
   fence, attestation, emission, receipt, delivery, dispatch order. Python
   reconcilers lock all receipt/delivery aliases before the exact dispatch.
   PostgreSQL concurrency tests cover cleanup versus authorized task ACK and
   cleanup versus event ACK without deadlock or premature cleanup.
7. URL identity normalization now precedes cache key construction, canonical
   platform recognition is restricted to actual platform hostname families,
   and unrelated hosts cannot alias. Cache rows durably snapshot storage and
   media facts, source artifact deletion uses `SET NULL`, and each later job
   materializes its own artifact row. Cleanup preserves cache-owned/shared
   storage. Tests delete the source before a hit and after a later materialized
   hit.
8. Added the exact MinIO staging janitor service and CLI. It scans only
   claim-shaped `staging/artifacts/...` keys older than 86,400 seconds,
   excludes exact artifact and durable cache storage pointers, writes atomic
   mode-0600 status, and records errors/counts. Worker readiness can require a
   successful status no older than 900 seconds. Tests cover old/fresh/invalid
   keys, successful artifact pointers, cache-only pointers after source
   deletion, errors, grace bounds, CLI failure, and readiness.

## TDD And Test Files

RED was recorded before each final audit fix:

- migration lock/cancellation contract:
  `1 failed` because pre-XACK did not call live node authority;
- active registration fail-closed PG assertion:
  old terminal/revoked pre-XACK expectation failed with
  `job_authority_changed`;
- unrelated URL host normalization:
  `1 failed` because `notyoutube.com` aliased to YouTube;
- cache-owned staging retention:
  `1 failed` because the janitor deleted the cache-only pointer;
- repeated preferred reclaim:
  `1 failed` because a second scan processed the preferred consumer's own
  message again.

Those cases are now covered in the migration, cache, janitor, Redis affinity,
worker startup, receipt, readiness, and YouTube test files listed above.

## Exact Verification

PostgreSQL 16 migration/control-plane tests:

```text
CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  backend/.venv/bin/python -m pytest \
  backend/tests/migrations/test_worker_registrations_postgres.py \
  backend/tests/migrations/test_worker_control_plane_postgres.py -q
3 passed in 10.29s
```

Focused affected tests with PostgreSQL 16 and a disposable Redis 7.4
container at `127.0.0.1:56379`:

```text
CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
WORKER_REDIS_TEST_URL=redis://127.0.0.1:56379/0 \
  .venv/bin/python -m pytest \
  tests/migrations/test_worker_registrations_postgres.py \
  tests/migrations/test_worker_control_plane_postgres.py \
  tests/orchestrator/test_artifact_cache.py \
  tests/services/test_job_execution_authority.py \
  tests/services/test_registered_worker_event_receipt.py \
  tests/services/test_staging_object_janitor.py \
  tests/services/test_worker_storage_readiness.py \
  tests/services/test_youtube_upload_operations.py \
  tests/channel_agent/test_staging_object_janitor_cli.py \
  tests/worker/test_worker_affinity_redis.py \
  tests/worker/test_worker_startup.py \
  tests/worker/test_url_download_handler.py -q
201 passed in 11.75s
```

The disposable Redis container was stopped and removed after the test.

Full backend:

```text
cd backend
.venv/bin/python -m pytest
1258 passed, 75 skipped, 17 warnings in 71.41s
```

Changed Python Ruff:

```text
.venv/bin/python -m ruff check <all changed Python source and tests>
All checks passed!
```

Targeted Mypy:

```text
.venv/bin/python -m mypy --follow-imports=skip <14 changed source files>
Success: no issues found in 14 source files
```

Required broad baseline checks:

```text
.venv/bin/python -m ruff check .
Found 15 errors.

.venv/bin/python -m mypy app
Found 62 errors in 22 files (checked 159 source files)
```

The broad findings are pre-existing and outside the round-4 changed-source
set; no round-4 source appears in the final broad Mypy findings.

Diff validation:

```text
git diff --check 0c8a188
passed (no output)
```

## Self-Review

- Read the complete diff from `0c8a188`, including every migration function,
  trigger, upgrade/downgrade path, ORM/service change, test, and Task 4
  design/plan edit.
- Confirmed every new callable is fixed-search-path, PUBLIC-revoked, and
  restricted to non-owner/non-superuser worker runtime principals where
  applicable.
- Confirmed there is no broad worker UPDATE/INSERT grant, no public privacy
  path, and no production/deploy/SSH/schedule/channel/host action.
- Confirmed registration remains before Redis, lease/heartbeat remain
  180/60 seconds, uncertainty fails closed, and final writes/XACKs retain the
  same active registration/epoch and exact dispatch/event proof.
- Confirmed Host126 is untouched and no deployment files changed.

## Concerns And Boundaries

- Repository-wide Ruff and Mypy retain the pre-existing baseline findings
  recorded above; changed-source Ruff and targeted Mypy are clean.
- The pre-existing tracked `progress.md` modification is included unchanged.
- Task 4 must still wire grants, service scheduling, monitoring, secrets, and
  deployment readiness. This round changed only code/design/test surfaces and
  performed no external or production mutation.
- Public publication remains blocked; no upload, publication, canary,
  schedule, channel, policy, deploy, push, or SSH action occurred.

# Task 2 Fix Round 5 Report

Status: round-5 implementation complete; final verification and commit follow
this report.

Base reviewed:
`e1c5f0fbce61fbae72d9d5f0110f76c0d889a525`

## Scope And Files

Schema and application implementation:

- `backend/alembic/versions/034_worker_registrations.py`
- `backend/app/channel_agent/staging_object_janitor_cli.py`
- `backend/app/channel_agent/worker_storage_readiness_cli.py`
- `backend/app/services/job_execution_authority.py`
- `backend/app/services/staging_janitor_status.py`
- `backend/app/services/worker_storage_readiness.py`
- `backend/app/services/youtube_upload_operations.py`
- `backend/worker/main.py`

Tests:

- `backend/tests/channel_agent/test_staging_object_janitor_cli.py`
- `backend/tests/channel_agent/test_worker_storage_readiness_cli.py`
- `backend/tests/migrations/test_worker_control_plane_postgres.py`
- `backend/tests/migrations/test_worker_registrations_postgres.py`
- `backend/tests/services/test_job_execution_authority.py`
- `backend/tests/services/test_staging_janitor_status.py`
- `backend/tests/services/test_worker_storage_readiness.py`
- `backend/tests/services/test_youtube_upload_operations.py`
- `backend/tests/test_startup_recovery.py`
- `backend/tests/worker/test_worker_event_emission_replay.py`
- `backend/tests/worker/test_worker_startup.py`

Plan, design, and implementation records:

- `docs/superpowers/plans/2026-07-26-production-worker-registration.md`
- `docs/superpowers/specs/2026-07-26-production-worker-registration-design.md`
- `.superpowers/sdd/2026-07-26-production-worker-registration/task-2-report.md`
- `.superpowers/sdd/2026-07-26-production-worker-registration/progress.md`
  was the sole pre-existing worktree change. This implementer did not edit it;
  it is preserved and included as required.

No production, deploy script, SSH host, Redis production, schedule, channel,
task, upload, publication, YouTube, or main-checkout plan state was changed.

## Finding Resolutions

1. **Registered YouTube submission no longer self-blocks.** The registered
   `submission_fence` now owns one session across each protocol phase.
   `mark_attempting` commits durable `request_attempted_at` before the POST,
   then opens a new transaction and re-proves the exact claim, active
   registration/epoch, 150-second database-clock margin, and upload operation.
   That transaction holds job/node, registration advisory, and operation
   authority across the POST. `mark_submitted` records the canonical manager
   task in that same transaction after the response. Heartbeat remains live
   because `vp_require_worker_node_claim` retains the shared registration
   advisory fence but no longer takes a registration row lock. Failure,
   cancellation, and ambiguous response paths persist `failed` or `uncertain`
   under the active phase when possible and otherwise fail closed with the
   durable attempting evidence intact. No broad upload-operation DML was
   added.

2. **Janitor readiness has a concrete cross-host durable transport and an
   exact recurring execution contract.** Migration 034 adds a singleton
   `staging_janitor_status` row and fixed-search-path, PUBLIC-revoked
   `vp_begin_staging_janitor_run`, `vp_finish_staging_janitor_run`, and
   `vp_staging_janitor_readiness` functions. Begin is advisory-lock serialized,
   rejects overlap, and permits stale takeover after 600 seconds. Readiness
   uses the database status visible from workers on both hosts, treats missing,
   stale success, latest error, and stale active run as unready, and accepts
   only a successful canonical result no older than 900 seconds. The janitor
   CLI rejects `DATABASE_URL`, reads a mode-0400 dedicated database secret, and
   records begin/finish status. Worker readiness uses its existing mode-0400
   database secret and no longer treats a container-local file as authority.
   Mode-0600 local evidence remains optional.

   The Task 4 plan and design now specify the exact host-150
   `deploy-sync-extension.sh` output: a mode-0700 runner, one marked `*/5`
   crontab block, fixed-name one-replica Swarm `replicated-job`, restart
   condition `none`, `node.hostname==ccttww-lap`, terminal-job replacement,
   running-job skip, existing network/current reviewed image, dedicated
   secrets, fixed log, monitoring semantics, deploy tests, and explicit host
   126 denial.

3. **Prepared worker events have a bounded sender and live reconciler.** The
   worker commits the immutable canonical payload first, then each of at most
   three send attempts reloads it through
   `vp_load_worker_prepared_event_emission` under the exact live
   registration/epoch, job/node claim, source attestation, and unresolved
   dispatch locks. The exact emission-ID marker Lua script atomically returns
   an existing message ID or performs one `XADD` plus marker `SET`. A definite
   pre-EVAL failure leaves the row prepared; the five-second reconciler scans
   only that registration's prepared rows and retries it. Redis success
   followed by database-mark failure reuses the same non-expiring marker and
   cannot duplicate `XADD`. Cancellation rolls back database work and stops
   promptly. Recovery and cleanup continue to hold unresolved emission
   authority rather than redispatching or erasing it.

4. **Recovery accepts all state-preserving database outcomes.**
   `recover_registered_worker_node` now accepts `terminal` and
   `held_unresolved_event` in addition to the prior outcomes. Startup recovery
   logs either outcome, preserves the node/job exactly, and continues instead
   of aborting startup. Wrapper and startup-path tests cover both values.

5. **Task 4 has the exact worker event-emission Redis ACL.** Each worker's
   task-stream selector remains limited to its own service stream. Three
   additional selectors grant only `EVAL` over `vp:events` plus
   `vp:worker-event-emission:*`, `GET`/`SET` over that marker prefix, and
   `XADD` over `vp:events`. Cross-worker task streams, `~*`, `+@all`, expiry,
   deletion, script loading, and administrative commands remain denied. The
   plan also specifies no-TTL marker lifecycle, durable AOF/noeviction
   readiness, exact event and dispatch cleanup proofs, monitoring, and
   failure-window tests.

6. **The actual restricted-role YouTube sequence is proven end to end.** The
   PostgreSQL 16 control-plane test uses the real restricted worker login and
   `YouTubeUploadOperationStore` to execute
   `submission_fence -> mark_attempting -> simulated POST boundary ->
   mark_submitted` inside a three-second timeout. It verifies attempting is
   externally committed before the boundary, a concurrent heartbeat completes
   inside one second while the POST fence is held, and the operation reaches
   `submitted`. Direct node, artifact, and upload-operation DML remain denied.

The six round-4 findings marked ADDRESSED remain covered by the complete
control-plane and backend regressions: exact event proof for ACK, terminal
recovery boundaries, preferred-host exact reclaim, constrained worker DML,
cleanup-compatible ACK lock order, and normalized durable URL cache ownership.

## Deferred Minor

The preferred-host reclaim now hands an exact `XCLAIM`ed message to the same
semaphore-backed scheduler used by normal consumption. It no longer performs
the full task inline in the consumer loop, while the existing exact dispatch,
claim, and affinity checks are unchanged. A focused test proves the scheduler
handoff and rejects inline processing.

## TDD RED Evidence

The new tests were run before their implementations. The observed failures
were:

```text
.venv/bin/python -m pytest \
  tests/services/test_job_execution_authority.py \
  tests/test_startup_recovery.py -q
JobExecutionAuthorityBlocked: registered worker node recovery result is invalid
```

```text
CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m pytest \
  tests/migrations/test_worker_control_plane_postgres.py -q
restricted submission timed out in mark_attempting before the simulated POST
```

```text
.venv/bin/python -m pytest \
  tests/worker/test_worker_event_emission_replay.py -q
AttributeError: module 'worker.main' has no attribute '_send_prepared_event_emission'
```

```text
.venv/bin/python -m pytest \
  tests/services/test_staging_janitor_status.py \
  tests/channel_agent/test_staging_object_janitor_cli.py \
  tests/channel_agent/test_worker_storage_readiness_cli.py -q
ModuleNotFoundError: No module named 'app.services.staging_janitor_status'
```

The live PostgreSQL janitor test initially reported missing
`vp_begin_staging_janitor_run`, and the preferred-reclaim regression initially
failed with:

```text
TypeError: _reclaim_preferred_pending() got an unexpected keyword argument 'message_scheduler'
```

## Exact Verification

Focused janitor status, readiness, CLI, and migration surface:

```text
cd backend
.venv/bin/python -m pytest \
  tests/services/test_staging_janitor_status.py \
  tests/services/test_worker_storage_readiness.py \
  tests/channel_agent/test_staging_object_janitor_cli.py \
  tests/channel_agent/test_worker_storage_readiness_cli.py \
  tests/migrations/test_worker_registrations_postgres.py::test_worker_registration_migration_emits_complete_additive_schema_and_functions \
  -q
54 passed in 1.31s
```

PostgreSQL 16 durable janitor status and restricted roles:

```text
CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m pytest \
  tests/migrations/test_worker_registrations_postgres.py::test_postgres_16_staging_janitor_status_is_cross_role_and_overlap_safe \
  -q
1 passed in 1.15s
```

Focused authority, YouTube, recovery, event, startup, lifecycle, and handler
tests:

```text
.venv/bin/python -m pytest \
  tests/services/test_job_execution_authority.py \
  tests/services/test_youtube_upload_operations.py \
  tests/test_startup_recovery.py \
  tests/worker/test_worker_event_emission_replay.py \
  tests/worker/test_worker_startup.py \
  tests/worker/test_worker_registration_lifecycle.py \
  tests/worker/test_youtube_upload_handler.py -q
201 passed in 2.59s
```

PostgreSQL 16 restricted control plane, including the bounded full YouTube
path and both event failure windows:

```text
CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m pytest \
  tests/migrations/test_worker_control_plane_postgres.py -q
1 passed in 5.88s
```

Full PostgreSQL 16 migration suite:

```text
CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m pytest \
  tests/migrations/test_worker_registrations_postgres.py -q
3 passed in 7.37s
```

Final combined PostgreSQL 16 rerun:

```text
CHANNEL_OPS_POSTGRES_TEST_URL=postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m pytest \
  tests/migrations/test_worker_registrations_postgres.py \
  tests/migrations/test_worker_control_plane_postgres.py -q
4 passed in 13.20s
```

Disposable Redis 7.4 at `127.0.0.1:56379`, removed after the run:

```text
WORKER_REDIS_TEST_URL=redis://127.0.0.1:56379/15 \
  .venv/bin/python -m pytest \
  tests/worker/test_worker_affinity_redis.py \
  tests/worker/test_worker_event_emission_replay.py -q
3 passed in 1.04s
```

Preferred-reclaim scheduler regression after the Minor fix:

```text
.venv/bin/python -m pytest tests/worker/test_worker_startup.py -q
39 passed in 0.33s
```

Full backend:

```text
.venv/bin/python -m pytest
1272 passed, 77 skipped, 17 warnings in 72.39s
```

Changed-source Ruff and targeted Mypy:

```text
.venv/bin/python -m ruff check <19 changed/new Python files>
All checks passed!

.venv/bin/python -m mypy --follow-imports=skip <9 affected source files>
Success: no issues found in 9 source files
```

The affected source files also passed `python -m py_compile`.

Required broad baseline checks:

```text
.venv/bin/python -m ruff check .
Found 15 errors.

.venv/bin/python -m mypy app
Found 62 errors in 22 files (checked 160 source files)
```

The broad findings are pre-existing and outside the round-5 changed-source
set. No round-5 source appears in the broad Mypy findings.

## Self-Review

- Read the complete diff from
  `e1c5f0fbce61fbae72d9d5f0110f76c0d889a525`, including all migration
  upgrade/downgrade paths, privilege surfaces, application call chains, tests,
  plan/design changes, and new files.
- Confirmed all new functions are fixed-search-path `SECURITY DEFINER`,
  PUBLIC-revoked, and intended only for exact non-owner roles. No worker table
  DML grant or direct protected-table mutation was added.
- Confirmed the YouTube fence records attempting before POST, holds exact
  operation/job/node/registration authority across POST, permits the 60-second
  heartbeat, records submitted after the response, and preserves
  failed/uncertain recovery evidence.
- Confirmed prepared event replay always reloads the database payload and
  exact live authority, uses a non-expiring emission-ID marker, has bounded
  per-send retries, performs no duplicate `XADD`, and cancels cleanly.
- Confirmed janitor readiness is database-backed and cross-host, while local
  mode-0600 evidence is non-authoritative.
- Confirmed registration still precedes Redis construction/commands,
  lease/heartbeat remain 180/60 seconds, uncertainty fails closed, and final
  writes/XACKs retain exact registration, dispatch, claim, and event proof.
- Confirmed publication defaults remain private/unlisted, public remains
  blocked, and external publication still requires explicit human review.
- Confirmed no tests were weakened or deleted and no host 126, production,
  deploy, upload, schedule, channel, task, publication, or YouTube action was
  performed.

## Concerns And Boundaries

- Repository-wide Ruff and Mypy retain the pre-existing baseline findings
  recorded above; changed-source Ruff and targeted Mypy are clean.
- Task 4 still has to implement and test the specified scheduler, database
  role grants, Redis ACL users, secrets, monitoring, and deployment readiness.
  This round intentionally did not change deploy files or external state.
- A prepared emission is replayed only while its exact registration/epoch and
  claim remain live. After authority loss it stays durably held for exact
  observer/operator repair; it is never replayed under a replacement worker.
- The pre-existing tracked `progress.md` round-4 ledger entry is included
  unchanged as directed.
- No production, push, SSH, deployment, Redis production, schedule, channel,
  task, upload, publication, or YouTube operation occurred.
