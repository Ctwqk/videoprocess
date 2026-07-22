# Guarded Schedule Job Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the canary's global `OPEN` window durably executable by exactly
one approved job, including jobs inserted after guarded open commits.

**Architecture:** Migration 031 adds nullable `runtime_schedules.guarded_job_id`.
Guarded open stores the exact job UUID, and both Python and Go scheduling and
execution fences park or reject every mismatch. Legacy open clears the guard
and retains its existing behavior.

**Tech Stack:** PostgreSQL 16, Alembic, SQLAlchemy async, FastAPI, Python 3.12,
Go 1.24+, pgx, pytest, GitHub Actions, Docker Swarm.

## Global Constraints

- `halted_at` remains the full channel kill switch.
- `intake_paused_at` blocks only `agent_tick` and `ingest_discovery`.
- The guarded canary remains owned-only and `unlisted`; public and
  external-asset automatic publication remain forbidden.
- Existing no-parameter schedule APIs remain compatible.
- Guard mismatch and infrastructure errors fail closed without raw database,
  URL, credential, SQL, or upstream-response leakage.
- 127 remains the application node, 150 remains GPU/publisher/control, and 126
  remains excluded.
- No live upload occurs until exact-SHA CI, deployment, a fresh preflight, and
  the exact approval `批准第四次 unlisted canary`.

---

### Task 1: Persist guarded schedule authority

**Files:**
- Create: `backend/alembic/versions/031_guarded_schedule_job_authority.py`
- Modify: `backend/app/models/schedule.py`
- Modify: `backend/app/schemas/schedule.py`
- Modify: `backend/app/services/schedule_service.py`
- Create: `backend/tests/migrations/test_guarded_schedule_job_authority_postgres.py`
- Modify: `backend/tests/api/test_internal_schedule.py`
- Modify: `backend/tests/test_schedule_service.py`

**Interfaces:**
- Produces: `RuntimeSchedule.guarded_job_id: UUID | None`.
- Produces: `VideoScheduleStatusResponse.guarded_job_id: str | None`.
- Produces:
  `should_defer_job_start(job, state, guarded_job_id: uuid.UUID | None = None) -> bool`.

- [ ] **Step 1: Write migration and schedule-state RED tests**

Add these named tests before production changes:

```python
def test_open_guard_allows_only_exact_job():
    guarded_job_id = uuid.uuid4()
    assert not should_defer_job_start(
        _job(id=guarded_job_id), VideoScheduleState.OPEN, guarded_job_id
    )
    assert should_defer_job_start(
        _job(id=uuid.uuid4()), VideoScheduleState.OPEN, guarded_job_id
    )


def test_legacy_open_without_guard_remains_unrestricted():
    assert not should_defer_job_start(_job(id=uuid.uuid4()), VideoScheduleState.OPEN)
```

`test_guarded_schedule_job_authority_postgres.py` must also contain an offline
DDL test and a live PostgreSQL test. The live test upgrades from
`030_channelops_intake_pause`, verifies a pre-existing schedule row survives
with `guarded_job_id IS NULL`, stores an existing job UUID, proves deleting that
job raises `asyncpg.ForeignKeyViolationError`, downgrades to 030 and proves the
column is gone, then upgrades to 031 again.

- [ ] **Step 2: Run RED**

```bash
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  backend/tests/migrations/test_guarded_schedule_job_authority_postgres.py \
  backend/tests/api/test_internal_schedule.py -q
```

Expected: missing migration/model/response field assertions fail.

- [ ] **Step 3: Add migration and model/schema fields**

Migration 031 must use these revision identifiers and a named restrictive
foreign key. Omitting `ondelete` deliberately gives PostgreSQL `NO ACTION`:

```python
revision = "031_guarded_schedule_job_authority"
down_revision = "030_channelops_intake_pause"

op.add_column(
    "runtime_schedules",
    sa.Column("guarded_job_id", postgresql.UUID(as_uuid=True), nullable=True),
)
op.create_foreign_key(
    "fk_runtime_schedules_guarded_job_id_jobs",
    "runtime_schedules",
    "jobs",
    ["guarded_job_id"],
    ["id"],
)
```

The ORM uses `Mapped[uuid.UUID | None]`, PostgreSQL `UUID(as_uuid=True)`, and
`ForeignKey("jobs.id")`. The API schema remains backward compatible with:

```python
guarded_job_id: str | None = None
```

- [ ] **Step 4: Make state transitions explicit**

`open_video_schedule_for_job()` sets both fields in the existing transaction:

```python
schedule.state = VideoScheduleState.OPEN.value
schedule.guarded_job_id = expected_job_id
```

Every generic `set_video_schedule_state()` transition, including legacy
`OPEN`, assigns `schedule.guarded_job_id = None`. Status responses serialize
`str(schedule.guarded_job_id)` or `None`. `should_defer_job_start()` first
checks an `OPEN` guard mismatch and otherwise preserves the existing
`OPEN`/`DRAINING`/`CLOSED` behavior.

- [ ] **Step 5: Run GREEN and commit**

```bash
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  backend/tests/migrations/test_guarded_schedule_job_authority_postgres.py \
  backend/tests/api/test_internal_schedule.py backend/tests/test_schedule_service.py -q
git add backend/alembic/versions/031_guarded_schedule_job_authority.py \
  backend/app/models/schedule.py backend/app/schemas/schedule.py \
  backend/app/services/schedule_service.py \
  backend/tests/migrations/test_guarded_schedule_job_authority_postgres.py \
  backend/tests/api/test_internal_schedule.py backend/tests/test_schedule_service.py
git commit -m "feat: persist guarded schedule authority"
```

### Task 2: Enforce guarded authority in Python execution paths

**Files:**
- Modify: `backend/app/services/job_runtime.py`
- Modify: `backend/app/orchestrator/engine.py`
- Modify: `backend/app/services/job_execution_authority.py`
- Modify: `backend/app/autoflow/service.py`
- Modify: `backend/app/main.py`
- Modify: `backend/tests/services/test_job_runtime.py`
- Modify: `backend/tests/channel_agent/test_operator_quarantine_postgres.py`
- Modify: `backend/tests/autoflow/test_execute_idempotency_postgres.py`
- Create: `backend/tests/test_startup_recovery.py`

**Interfaces:**
- Consumes: `RuntimeSchedule.guarded_job_id` and
  `should_defer_job_start(job, state, guarded_job_id)`.
- Produces: no Python job can start or cross node execution authority while a
  different guarded UUID owns the window.

- [ ] **Step 1: Add RED tests at every execution boundary**

Add these behavior cases before production changes:

```text
test_start_or_defer_partitions_exact_guard_from_mismatches
  exact guarded UUID -> start_jobs_background([exact])
  other UUID -> WAITING_WINDOW

test_guarded_mismatch_is_parked_before_initial_or_node_dispatch
  OPEN + different guarded_job_id -> job WAITING_WINDOW, zero Redis dispatch

test_execution_authority_rejects_guarded_mismatch_before_node_work
  require_active_execution_authority(...) raises JobExecutionAuthorityBlocked

test_startup_recovery_restarts_only_exact_guarded_job
  exact UUID scheduled with engine.start_job; mismatch parked

test_ordinary_autoflow_created_during_guard_is_parked
  durable run/job may be created, but job is WAITING_WINDOW and starter is not called

test_bound_autoflow_guard_rejects_new_job_without_rows
  OPEN guard for another UUID -> PermissionError before run/pipeline/job creation

test_bound_autoflow_replay_resumes_only_exact_guarded_job
  existing exact run/job may call starter; mismatch raises without calling starter
```

The PostgreSQL cases belong in the two existing PostgreSQL test modules so CI's
forced database run exercises real schedule/channel/task/job locks. The startup
test may use a fake async session, but it must assert the exact set of job IDs
passed to `asyncio.create_task`/`engine.start_job`.

- [ ] **Step 2: Run focused RED**

Run:

```bash
cd backend
CHANNELOPS_REQUIRE_DATABASE=1 \
  /Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_job_runtime.py \
  tests/channel_agent/test_operator_quarantine_postgres.py \
  tests/autoflow/test_execute_idempotency_postgres.py \
  tests/test_startup_recovery.py -q
```

Expected: the new tests fail only because guarded IDs are not yet consulted.
If no isolated PostgreSQL test URL is available locally, run the two unit
modules for RED and retain the PostgreSQL tests for the mandatory CI gate; do
not point tests at production.

- [ ] **Step 3: Implement the shared decision**

`start_or_defer_jobs()` must load the full schedule record and partition jobs,
because a batch can contain the exact allowed UUID plus mismatches:

```python
deferred = [
    job
    for job in jobs
    if should_defer_job_start(job, schedule_state, schedule.guarded_job_id)
]
started = [job for job in jobs if job not in deferred]
```

Park `deferred` durably before calling `start_jobs_background()` for `started`.
Pass `authority.schedule.guarded_job_id` into the same decision in
`JobEngine._lock_initial_launch_authority()`. In
`require_active_execution_authority()`, reject when the schedule is `OPEN`, a
guard is present, and `authority.job.id` differs.

Startup recovery loads the schedule record once, applies
`should_defer_job_start()` to every job, and never schedules a mismatching UUID.
Ordinary AutoFlow creation may persist a mismatch but must park it. A
ChannelOps-bound execution holding the schedule lock may materialize its
durable job while the schedule is `CLOSED` with no guard, but must park that job
in `WAITING_WINDOW` without a start handoff. It must reject a new execution
under any non-null guard; an existing idempotent job may resume only when
`existing.job_id == schedule.guarded_job_id`.

- [ ] **Step 4: Run focused GREEN and commit**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_job_runtime.py tests/test_startup_recovery.py \
  tests/channel_agent/test_operator_quarantine_postgres.py \
  tests/autoflow/test_execute_idempotency_postgres.py -q
git add app/services/job_runtime.py app/orchestrator/engine.py \
  app/services/job_execution_authority.py app/autoflow/service.py app/main.py \
  tests/services/test_job_runtime.py \
  tests/channel_agent/test_operator_quarantine_postgres.py \
  tests/autoflow/test_execute_idempotency_postgres.py \
  tests/test_startup_recovery.py
git commit -m "feat: fence python jobs to guarded schedule authority"
```

### Task 3: Enforce guarded authority in Go scheduling

**Files:**
- Modify: `internal/store/schedule.go`
- Modify: `internal/store/schedule_test.go`
- Modify: `internal/orchestrator/engine.go`
- Modify: `internal/orchestrator/store_adapter.go`
- Modify: `internal/orchestrator/engine_test.go`
- Modify: `internal/httpapi/schedule_controller.go`
- Modify: `internal/httpapi/schedule_controller_test.go`
- Modify: `internal/httpapi/schedule_writes.go`

**Interfaces:**
- Produces: `store.VideoScheduleStatusRow.GuardedJobID *string`.
- Produces:

```go
type VideoScheduleAuthority struct {
	State        string
	GuardedJobID string
}

GetVideoScheduleAuthority(ctx context.Context) (VideoScheduleAuthority, error)
```

- [ ] **Step 1: Add Go RED tests**

Add these named cases before production changes:

```text
TestStartJobRunsExactGuardedJob
TestStartJobParksGuardedMismatchBeforePlanningOrDispatch
TestStartJobKeepsLegacyUnguardedOpenBehavior
TestSetVideoScheduleStateClearsGuard
TestOpenVideoScheduleForJobStoresExactGuard
TestCoordinatedGuardedOpenRequiresMatchingPythonAndLocalGuard
TestCoordinatedGuardedOpenUncertainErrorClosesPythonAndLocalAfterRequestCancellation
TestCoordinatedGuardedOpenKnownConflictDoesNotRetryPythonClose
TestGuardedScheduleRouteSanitizesInfrastructureError
```

The cancelled-context test must make both fake `SetState("CLOSED")` methods
record `ctx.Err() == nil`; merely asserting that they were called is
insufficient. The HTTP test injects a raw error containing a fake database URL
and asserts the response body equals a stable
`{"detail":"guarded_schedule_open_failed"}` payload without that secret.

- [ ] **Step 2: Run RED**

```bash
go test -count=1 ./internal/orchestrator ./internal/httpapi ./internal/store
```

Expected: missing guarded status/authority fields and error sanitization fail.

- [ ] **Step 3: Implement Go authority and close recovery**

`getVideoScheduleStatus()` selects `guarded_job_id::text` and scans the nullable
value into `GuardedJobID`. Generic transitions execute:

```sql
SET state = EXCLUDED.state,
    guarded_job_id = NULL,
    updated_by = EXCLUDED.updated_by,
    updated_at = NOW()
```

Guarded standalone open sets `guarded_job_id = $2::uuid` in the same
transaction that changes state and releases the expected Go job. Update the
store integration fixture to close/clear the guard before deleting a guarded
job, because migration 031 intentionally restricts deletion.

The Go orchestrator reads `VideoScheduleAuthority`. For `OPEN` with a non-empty
guard, only `job.ID == GuardedJobID` proceeds; a mismatch is marked
`WAITING_WINDOW` before planning or dispatch. Empty guard preserves legacy
behavior.

The coordinated controller accepts success only when both Python and shared
local status report `OPEN`, `ReleasedJobs == 1`, and the same exact guarded UUID.
For a known `ErrScheduleGuardMismatch`, return without cleanup: the Python-first
request made no mutation, and the local controller observes the same durable
schedule row, so a generic local close could erase foreign authority. For every
transport, decode, timeout, 5xx, or ambiguous error, attempt Python and local
close independently, each with a fresh context made by:

```go
cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
defer cancel()
```

Never write `err.Error()` to the guarded HTTP response; log the internal error
and return only `guarded_schedule_open_failed`.

- [ ] **Step 4: Run Go GREEN and commit**

```bash
go test -count=1 ./internal/orchestrator ./internal/httpapi ./internal/store
go test -count=1 ./...
git add internal/store internal/orchestrator internal/httpapi
git commit -m "feat: fence go jobs to guarded schedule authority"
```

### Task 4: Require exact authority in the canary runner

**Files:**
- Modify: `scripts/run_vp_unlisted_canary.py`
- Modify: `backend/tests/services/test_unlisted_canary_runner.py`
- Modify: `tests/test_vp_unlisted_canary_scripts.sh`
- Modify: `deploy/four-machine-topology.md`

**Interfaces:**
- Consumes: schedule status `guarded_job_id`.
- Produces: open evidence that names the exact guarded canary job.

- [ ] **Step 1: Add runner RED tests**

Add the following assertions before runner changes:

```python
assert initial_schedule["state"] == "CLOSED"
assert initial_schedule.get("guarded_job_id") is None

assert opened["state"] == "OPEN"
assert opened["released_jobs"] == 1
assert opened["guarded_job_id"] == str(job.id)
```

Cover both `--preflight-only` and live startup. Also require `DRAINING` and final
`CLOSED` responses to clear `guarded_job_id`. Preserve every cleanup
cancellation and stale-state regression from commit `55f9a64` unchanged.

- [ ] **Step 2: Implement and run GREEN**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_unlisted_canary_runner.py -q
cd ..
bash tests/test_vp_unlisted_canary_scripts.sh
```

Expected: runner tests and shell source-contract tests pass, including an
explicit source assertion that guarded-open validation names
`guarded_job_id`.

- [ ] **Step 3: Update runbook and commit**

Document durable exact-job schedule authority, legacy compatibility, automatic
guard clearing on drain/close, and the unchanged fourth-attempt approval.

```bash
git add scripts/run_vp_unlisted_canary.py \
  backend/tests/services/test_unlisted_canary_runner.py \
  tests/test_vp_unlisted_canary_scripts.sh deploy/four-machine-topology.md
git commit -m "fix: require exact guarded canary authority"
```

### Task 5: Verify, review, deploy, and preflight

- [ ] Run the complete local verification set:

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest -q
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m ruff check \
  app/models/schedule.py app/schemas/schedule.py app/services/schedule_service.py \
  app/services/job_runtime.py app/services/job_execution_authority.py \
  app/orchestrator/engine.py app/autoflow/service.py app/main.py \
  tests/migrations/test_guarded_schedule_job_authority_postgres.py \
  tests/api/test_internal_schedule.py tests/test_schedule_service.py \
  tests/services/test_job_runtime.py tests/test_startup_recovery.py \
  tests/channel_agent/test_operator_quarantine_postgres.py \
  tests/autoflow/test_execute_idempotency_postgres.py \
  tests/services/test_unlisted_canary_runner.py
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m ruff check . || true
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m mypy app || true
cd ..
go test -count=1 ./...
bash tests/test_vp_unlisted_canary_scripts.sh
bash tests/test_ci_workflow_contract.sh
bash tests/test_vp_deploy_ci_gate.sh
bash tests/test_vp_deploy_sync_extension.sh
git diff --check
```

The full backend count may include PostgreSQL skips locally. Only exact-SHA CI
with its isolated PostgreSQL service can satisfy the mandatory migration/store
integration gate.
- [ ] Independently review the complete branch and fix confirmed findings with
  RED tests first.
- [ ] Fast-forward main, push, and require exact-SHA CI success. CI must run
  migration 031 and forced PostgreSQL tests for `internal/channelops` and
  `internal/store`.
- [ ] Verify automatic deployment to 127/150 at the exact SHA, migration head
  031, all expected replicas, zero VP placement on 126, and unchanged PDS.
- [ ] Verify schedule `CLOSED`, `guarded_job_id=NULL`, zero unsafe/public/upload
  backlog, and zero Redis pending; save a fresh mode-0600 preflight artifact.
- [ ] Ask for the fresh exact phrase `批准第四次 unlisted canary`; do not reuse
  any prior approval or upload before it arrives.
