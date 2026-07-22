# Task 3 Report: Enforce Guarded Authority in Go Scheduling

## Scope

Implemented Task 3 only in the supplied `codex/canary-intake-pause` worktree,
starting from `994aefe1be7488620f4426e862163bb14306a8ad`. No Python, runner,
deployment, or external-system files were changed.

## Baseline

Before edits:

```text
go test -count=1 ./...
PASS (all Go packages)
```

The worktree was clean and linked to the requested branch and base.

## RED

The nine required named tests were added before production changes:

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

Command:

```bash
go test -count=1 ./internal/orchestrator ./internal/httpapi ./internal/store
```

Observed RED, exit 1:

```text
internal/store/schedule_test.go:149:11: row.GuardedJobID undefined
internal/httpapi/schedule_controller_test.go:212:59: unknown field GuardedJobID
--- FAIL: TestStartJobParksGuardedMismatchBeforePlanningOrDispatch
    engine_test.go:170: job status = "RUNNING"; want WAITING_WINDOW
FAIL github.com/Ctwqk/videoprocess/internal/orchestrator
FAIL github.com/Ctwqk/videoprocess/internal/httpapi [build failed]
FAIL github.com/Ctwqk/videoprocess/internal/store [build failed]
```

These failures matched the missing behavior: Go status did not expose the
guard, and the orchestrator ran a mismatching job through planning instead of
parking it.

## Implementation

- Added nullable `guarded_job_id` to Go schedule status reads and JSON output.
- Generic schedule transitions clear the guard; guarded standalone open stores
  the expected UUID in the same transaction that releases exactly that Go job.
- Reordered and deferred store fixture cleanup so the guard is cleared before
  deletion of its restrictively referenced job.
- Replaced state-only orchestrator reads with `VideoScheduleAuthority`; guarded
  mismatches park before planning or dispatch, while exact and legacy
  unguarded `OPEN` jobs still run.
- Coordinated guarded open calls Python first and accepts success only for exact
  `OPEN` guards on both responses plus Python `ReleasedJobs == 1`.
- Known Python guard conflicts close local authority only. Uncertain outcomes
  close Python and local independently with separate five-second contexts made
  from `context.WithoutCancel(ctx)`.
- Guarded HTTP 500 responses log the internal error and return only
  `{"detail":"guarded_schedule_open_failed"}`.

## GREEN

Focused command after implementation:

```bash
go test -count=1 ./internal/orchestrator ./internal/httpapi ./internal/store
```

Observed GREEN:

```text
ok github.com/Ctwqk/videoprocess/internal/orchestrator
ok github.com/Ctwqk/videoprocess/internal/httpapi
ok github.com/Ctwqk/videoprocess/internal/store
```

Final required verification, run after the last edit:

```bash
gofmt -w <all eight changed Go files>
git diff --check
go test -count=1 ./internal/orchestrator ./internal/httpapi ./internal/store
go test -count=1 ./...
```

Observed GREEN, exit 0:

```text
Focused: orchestrator, httpapi, and store all passed.
Full: all Go packages passed; command packages reported no test files.
git diff --check: no output.
```

## Database Test Status

No isolated PostgreSQL is configured locally, so the two new integration cases
skip unless CI sets `CHANNELOPS_REQUIRE_DATABASE=1`, as required:

```text
--- SKIP: TestSetVideoScheduleStateClearsGuard
--- SKIP: TestOpenVideoScheduleForJobStoresExactGuard
PASS
```

No production database, Docker, external system, or `126` environment was used.

## Self-Review

- Verified guarded coordination never legacy-opens local authority first.
- Verified exact UUID comparison on Python and shared local status, with Python
  release count exactly one.
- Verified only `ErrScheduleGuardMismatch` suppresses Python close recovery.
- Verified each close attempt creates its own fresh bounded cleanup context and
  the canceled-parent test records `ctx.Err() == nil` for both closes.
- Verified guarded 500 output is stable and contains neither the injected raw
  database URL nor an upstream response.
- Verified generic state transitions clear guards and guarded open sets the
  guard inside the release transaction.
- Verified mismatch parking occurs before planning and dispatch, while exact
  and legacy unguarded open cases proceed.
- Verified only the eight brief-listed Go files and this required report are in
  scope; no API was removed.

## Concerns

The PostgreSQL-backed assertions could not execute locally because the required
isolated database is intentionally unavailable. CI must run them with
`CHANNELOPS_REQUIRE_DATABASE=1`; all non-database focused and repository-wide Go
tests pass locally.
