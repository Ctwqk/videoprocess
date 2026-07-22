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

## Review Fix: Atomic Planning Claim and Independent Cleanup Deadlines

### Scope

Addressed both Task 3 review findings without changing Python, runners,
deployment, Docker, production databases, `126`, or external systems.

### TOCTOU RED

The engine race test supplied initially matching authority but configured the
planning claim to observe a later close/mismatch and park the job.

```bash
go test -count=1 ./internal/orchestrator \
  -run 'TestStartJobRevalidatesAuthorityWhenPlanningClaimLosesRace$' -v
```

```text
=== RUN   TestStartJobRevalidatesAuthorityWhenPlanningClaimLosesRace
    engine_test.go:209: planning claim count = 0; want 1
--- FAIL: TestStartJobRevalidatesAuthorityWhenPlanningClaimLosesRace (0.00s)
FAIL
FAIL github.com/Ctwqk/videoprocess/internal/orchestrator
```

The store tests were also added before the claim API and decision function.

```bash
go test -count=1 ./internal/store \
  -run 'Test(GoJobPlanningActionForAuthority|ClaimGoJobPlanningWaitsForConcurrentScheduleClose)$' -v
```

```text
internal/store/go_jobs_test.go:94:72: s.ClaimGoJobPlanning undefined
internal/store/go_jobs_test.go:117:10: undefined: goJobPlanningAction
internal/store/go_jobs_test.go:119:78: undefined: goJobPlanningPark
FAIL github.com/Ctwqk/videoprocess/internal/store [build failed]
```

### TOCTOU GREEN

`ClaimGoJobPlanning` now locks the `videoprocess` schedule row first and the
owned Go job second, then makes and commits one state/guard/status decision.
The engine uses this claim instead of `MarkGoJobPlanning`; the legacy method
remains available for compatibility.

```bash
go test -count=1 ./internal/orchestrator \
  -run 'TestStartJob(RevalidatesAuthorityWhenPlanningClaimLosesRace|RunsExactGuardedJob|ParksGuardedMismatchBeforePlanningOrDispatch|KeepsLegacyUnguardedOpenBehavior)$' -v
```

```text
--- PASS: TestStartJobRunsExactGuardedJob (0.00s)
--- PASS: TestStartJobParksGuardedMismatchBeforePlanningOrDispatch (0.00s)
--- PASS: TestStartJobKeepsLegacyUnguardedOpenBehavior (0.00s)
--- PASS: TestStartJobRevalidatesAuthorityWhenPlanningClaimLosesRace (0.00s)
PASS
ok github.com/Ctwqk/videoprocess/internal/orchestrator
```

```bash
go test -count=1 ./internal/store \
  -run 'Test(GoJobPlanningActionForAuthority|ClaimGoJobPlanningWaitsForConcurrentScheduleClose)$' -v
```

```text
--- PASS: TestGoJobPlanningActionForAuthority (0.00s)
    --- PASS: closed_pending_parks
    --- PASS: closed_running_parks
    --- PASS: draining_pending_parks
    --- PASS: draining_waiting_parks
    --- PASS: draining_validating_claims
    --- PASS: draining_planning_claims
    --- PASS: draining_running_claims
    --- PASS: open_mismatched_guard_parks
    --- PASS: open_exact_guard_claims
    --- PASS: open_legacy_guard_claims
    --- PASS: unknown_state_preserves_claim
    --- PASS: terminal_success_skips
    --- PASS: terminal_failure_skips
    --- PASS: terminal_cancellation_skips
    --- PASS: terminal_partial_failure_skips
--- SKIP: TestClaimGoJobPlanningWaitsForConcurrentScheduleClose (0.00s)
PASS
ok github.com/Ctwqk/videoprocess/internal/store
```

The PostgreSQL race test skips locally because
`CHANNELOPS_REQUIRE_DATABASE=1` is not configured. Under CI it holds a
concurrent schedule-row lock, observes the claim waiting in `pg_stat_activity`,
commits `CLOSED`, and requires `claimed == false` with the job stored as
`WAITING_WINDOW`.

### Cleanup RED/GREEN

The focused test was added before the package timeout injection:

```bash
go test -count=1 ./internal/httpapi \
  -run 'TestCoordinatedGuardedOpenLocalCloseGetsFreshContextAfterPythonCloseTimeout$' -v
```

```text
internal/httpapi/schedule_controller_test.go:372:21: undefined: guardedScheduleCleanupTimeout
FAIL github.com/Ctwqk/videoprocess/internal/httpapi [build failed]
```

After adding the package-level timeout with the production default retained at
five seconds, the separate-context implementation passed in 20 ms. To prove
the test detects the actual regression, production was temporarily mutated to
share one context across both closes:

```text
=== RUN   TestCoordinatedGuardedOpenLocalCloseGetsFreshContextAfterPythonCloseTimeout
    schedule_controller_test.go:401: local close context errors = []error{context.deadlineExceededError{}}; want [nil]
--- FAIL: TestCoordinatedGuardedOpenLocalCloseGetsFreshContextAfterPythonCloseTimeout (0.02s)
FAIL
FAIL github.com/Ctwqk/videoprocess/internal/httpapi
```

The shared-context mutation was then removed and the correct implementation
was re-verified:

```bash
go test -count=1 ./internal/httpapi \
  -run 'TestCoordinatedGuardedOpen(UncertainErrorClosesPythonAndLocalAfterRequestCancellation|LocalCloseGetsFreshContextAfterPythonCloseTimeout|KnownConflictDoesNotRetryPythonClose)$' -v
```

```text
--- PASS: TestCoordinatedGuardedOpenUncertainErrorClosesPythonAndLocalAfterRequestCancellation (0.00s)
--- PASS: TestCoordinatedGuardedOpenLocalCloseGetsFreshContextAfterPythonCloseTimeout (0.02s)
--- PASS: TestCoordinatedGuardedOpenKnownConflictDoesNotRetryPythonClose (0.00s)
PASS
ok github.com/Ctwqk/videoprocess/internal/httpapi
```

### Required GREEN

```bash
go test -count=1 ./internal/orchestrator ./internal/httpapi ./internal/store
```

```text
ok github.com/Ctwqk/videoprocess/internal/orchestrator
ok github.com/Ctwqk/videoprocess/internal/httpapi
ok github.com/Ctwqk/videoprocess/internal/store
```

```bash
go test -count=1 ./...
```

```text
All Go packages passed; command packages reported no test files.
```

```bash
gofmt -w internal/orchestrator/engine.go internal/orchestrator/engine_test.go \
  internal/orchestrator/store_adapter.go internal/store/go_jobs.go \
  internal/store/go_jobs_test.go internal/store/schedule_test.go \
  internal/httpapi/schedule_controller.go internal/httpapi/schedule_controller_test.go
git diff --check
```

```text
git diff --check produced no output (exit 0).
```

### Files

- `internal/orchestrator/engine.go`
- `internal/orchestrator/engine_test.go`
- `internal/orchestrator/store_adapter.go`
- `internal/store/go_jobs.go`
- `internal/store/go_jobs_test.go`
- `internal/store/schedule_test.go`
- `internal/httpapi/schedule_controller.go`
- `internal/httpapi/schedule_controller_test.go`
- `.superpowers/sdd/task-3-report.md`

### Review Self-Check

- Schedule authority is locked before the owned Go job, matching guarded-open
  lock ordering and preventing the reviewed read/plan TOCTOU.
- `CLOSED` parks every nonterminal job; `DRAINING` parks fresh `PENDING` and
  `WAITING_WINDOW` jobs while allowing resumed nonterminal jobs to claim.
- `OPEN` with a non-null mismatched guard parks; exact and legacy unguarded
  `OPEN` claim. Unknown states retain the prior claim behavior.
- Terminal races commit and return false without changing the job. Authorized
  claims write `PLANNING`, `execution_plan`, and `started_at` atomically.
- A false claim stops before running, source resolution, queueing, or dispatch.
- `MarkGoJobPlanning` and its adapter remain intact for compatibility.
- Each cleanup close still receives a fresh context derived with
  `context.WithoutCancel`; the production timeout remains five seconds.

### Review-Fix Concerns

The PostgreSQL lock-wait integration test could not run locally because no
isolated database is configured. CI must execute it with
`CHANNELOPS_REQUIRE_DATABASE=1`; no production or external database was used.

## Review Fix: Cancellation-Safe Running Transition

### Finding and Scope

After `ClaimGoJobPlanning` committed `PLANNING`, `StartJob` separately called
`MarkGoJobRunning`. A concurrent `CancelJob` could commit `CANCELLED` between
those operations, and the running update previously filtered only by job ID
and Go ownership, allowing it to revive the cancelled job. This fix is limited
to the running transition and its engine/store regressions; no APIs or other
job transitions changed.

### RED

The deterministic engine regression was added while the fake running
transition still unconditionally wrote `RUNNING`:

```bash
go test -count=1 ./internal/orchestrator -run 'TestStartJobDoesNotReviveCancellationBetweenPlanningAndRunning$' -v
```

```text
=== RUN   TestStartJobDoesNotReviveCancellationBetweenPlanningAndRunning
    engine_test.go:242: StartJob error = <nil>; want running transition rejection
--- FAIL: TestStartJobDoesNotReviveCancellationBetweenPlanningAndRunning (0.00s)
FAIL
FAIL github.com/Ctwqk/videoprocess/internal/orchestrator
```

The PostgreSQL regression was also added first, but no isolated local database
is configured, so its local run skipped instead of reaching the old SQL:

```bash
go test -count=1 ./internal/store -run 'TestMarkGoJobRunningDoesNotReviveCancelledPlanningJob$' -v
```

```text
=== RUN   TestMarkGoJobRunningDoesNotReviveCancelledPlanningJob
    schedule_test.go:266: guarded schedule store integration requires CHANNELOPS_REQUIRE_DATABASE=1
--- SKIP: TestMarkGoJobRunningDoesNotReviveCancelledPlanningJob (0.00s)
PASS
ok github.com/Ctwqk/videoprocess/internal/store
```

### Implementation

- `MarkGoJobRunning` now updates only a Go-owned job whose current status is
  exactly `PLANNING`. A concurrent cancellation or terminal transition yields
  zero affected rows, which `guardedExecResult` maps to `pgx.ErrNoRows`.
- The engine fake now enforces the same precondition. The regression simulates
  cancellation immediately after a successful planning claim and verifies
  that `StartJob` returns the transition error while the job remains
  `CANCELLED`; source nodes, queued nodes, and the dispatcher remain untouched.
- The PostgreSQL integration regression creates a `PLANNING` Go job, cancels
  it, rejects the running transition with `pgx.ErrNoRows`, and verifies the
  stored status remains `CANCELLED`.

### GREEN

Focused regressions:

```bash
go test -count=1 ./internal/orchestrator -run 'TestStartJobDoesNotReviveCancellationBetweenPlanningAndRunning$' -v
go test -count=1 ./internal/store -run 'TestMarkGoJobRunningDoesNotReviveCancelledPlanningJob$' -v
```

```text
=== RUN   TestStartJobDoesNotReviveCancellationBetweenPlanningAndRunning
--- PASS: TestStartJobDoesNotReviveCancellationBetweenPlanningAndRunning (0.00s)
PASS
ok github.com/Ctwqk/videoprocess/internal/orchestrator
=== RUN   TestMarkGoJobRunningDoesNotReviveCancelledPlanningJob
    schedule_test.go:266: guarded schedule store integration requires CHANNELOPS_REQUIRE_DATABASE=1
--- SKIP: TestMarkGoJobRunningDoesNotReviveCancelledPlanningJob (0.00s)
PASS
ok github.com/Ctwqk/videoprocess/internal/store
```

Required focused packages:

```bash
go test -count=1 ./internal/orchestrator ./internal/store
```

```text
ok github.com/Ctwqk/videoprocess/internal/orchestrator 0.554s
ok github.com/Ctwqk/videoprocess/internal/store 0.658s
```

Required full suite:

```bash
go test -count=1 ./...
```

```text
? github.com/Ctwqk/videoprocess/cmd/channelops-live-smoke [no test files]
? github.com/Ctwqk/videoprocess/cmd/channelops-runner [no test files]
? github.com/Ctwqk/videoprocess/cmd/vp-api [no test files]
? github.com/Ctwqk/videoprocess/cmd/vp-ffmpeg-worker [no test files]
ok github.com/Ctwqk/videoprocess/internal/channelops 2.103s
ok github.com/Ctwqk/videoprocess/internal/config 0.502s
ok github.com/Ctwqk/videoprocess/internal/contracts 0.345s
ok github.com/Ctwqk/videoprocess/internal/httpapi 0.614s
ok github.com/Ctwqk/videoprocess/internal/orchestrator 0.703s
ok github.com/Ctwqk/videoprocess/internal/pipeline 0.899s
ok github.com/Ctwqk/videoprocess/internal/redisstream 1.058s
ok github.com/Ctwqk/videoprocess/internal/storage 1.300s
ok github.com/Ctwqk/videoprocess/internal/store 0.763s
ok github.com/Ctwqk/videoprocess/internal/worker 5.996s
ok github.com/Ctwqk/videoprocess/internal/worker/ffmpeg 1.913s
ok github.com/Ctwqk/videoprocess/internal/worker/handlers 2.814s
```

Formatting and whitespace:

```bash
gofmt -w internal/store/go_jobs.go internal/orchestrator/engine_test.go internal/store/schedule_test.go
git diff --check
```

```text
git diff --check produced no output (exit 0).
```

### Files

- `internal/store/go_jobs.go`
- `internal/orchestrator/engine_test.go`
- `internal/store/schedule_test.go`
- `.superpowers/sdd/task-3-report.md`

### Self-Review and Concerns

- The production predicate is exactly `status = 'PLANNING'`; the API signature
  and `guardedExecResult` behavior remain unchanged.
- The engine already returns immediately when `MarkGoJobRunning` fails, and
  the regression proves this occurs before source resolution, queueing, or
  dispatch.
- The store regression could not execute locally because
  `CHANNELOPS_REQUIRE_DATABASE=1` is unset. CI must force it against its
  isolated PostgreSQL fixture. No production database, Docker, port 126, or
  external system was used.
