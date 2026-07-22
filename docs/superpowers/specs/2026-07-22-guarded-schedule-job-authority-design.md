# Guarded Schedule Job Authority Design

## Status

Approved under the operator's standing preapproval for ideas, specifications,
and plans. This design supersedes the assumption that locking the currently
visible jobs can make a global `OPEN` window exclusive.

## Problem

The unlisted canary must release exactly one reviewed job. A transaction can
lock the schedule row and every currently visible runnable job, but PostgreSQL
`READ COMMITTED` row locks do not prevent a concurrent job insert. After the
guarded transaction commits `OPEN`, a newly submitted job can observe the open
schedule and start. The safety property therefore cannot live only in the open
transaction; it must remain durable for the lifetime of the open window.

## Chosen Model

Add nullable `runtime_schedules.guarded_job_id` as a foreign key to `jobs.id`
with restrictive deletion. Its meanings are:

- `OPEN` plus `guarded_job_id IS NULL`: legacy open window; normal job release.
- `OPEN` plus an exact job UUID: only that job may start or continue crossing
  initial execution authority. Every other new or recovered job is parked in
  `WAITING_WINDOW`.
- `DRAINING` or `CLOSED`: no guarded open authority; state transition clears
  `guarded_job_id` because the existing state already prevents fresh starts.

The foreign key must not use `ON DELETE SET NULL`: deleting the guarded job must
never silently turn a restricted window into an unrestricted one.

## Guarded Open

`POST /internal/schedule/video/open?expected_job_id=<uuid>` remains the canary
entry point. The Python schedule service locks schedule authority, verifies the
schedule is `CLOSED`, locks and checks the currently visible runnable jobs and
active nodes, requires exactly the expected Python `WAITING_WINDOW` job, then
sets `OPEN`, stores `guarded_job_id`, and releases only that job in one
transaction. The response exposes the guarded job ID and exactly one release.

The Go coordinated controller calls the Python guarded endpoint first and then
verifies the local shared-database status has the same `OPEN` state and guarded
job ID. It never invokes legacy local `OPEN` first. A known HTTP 409 means no
Python mutation and returns without cleanup, because a generic close against
the shared row could erase foreign authority. Every outcome-uncertain error
best-effort closes Python and local authorities using a short context detached
from request cancellation.
External responses use stable error identifiers and never raw database or
upstream error strings.

While the schedule is `CLOSED` with no guard, a ChannelOps-bound AutoFlow
execution may create its durable pipeline/run/job records, but the new job is
parked in `WAITING_WINDOW` and receives no start handoff. This gives guarded
open the exact job UUID it must authorize without opening a global execution
window first.

Legacy no-parameter `POST /open` remains compatible. It clears guarded
authority and performs the existing all-waiting-job release.

## Execution Fences

Durable authority is checked at both scheduling and execution boundaries.

Python:

- `start_or_defer_jobs` parks a non-matching job even while state is `OPEN`.
- orchestrator start locks and refreshes the schedule, then parks a mismatch.
- job execution authority rejects a non-matching guarded job before node work.
- AutoFlow paths that already hold schedule authority may resume the exact
  guarded job but cannot create or start a different job during the window.
- startup recovery applies the same guarded decision.

Go:

- schedule status includes the optional guarded job UUID.
- orchestrator start receives state plus guarded job authority and parks a
  mismatch before planning or queue dispatch.
- generic `OPEN`, `DRAINING`, and `CLOSED` transitions clear stale authority;
  guarded open sets it only for the exact expected Go job in standalone mode.

These checks make a phantom insert harmless: the row may be created, but it
cannot cross into execution or an external side effect during the canary
window.

## Cleanup

The existing Task 4 cleanup hardening remains required:

- all cleanup ORM locking reads force fresh database state;
- an invalid active session falls back to a new engine/session;
- fallback cleanup is bounded and shielded from repeated cancellation;
- failure fully halts the channel and cancels its actual job/nodes;
- raw connection errors and credentials never enter evidence.

## Migration And Compatibility

Migration 031 adds one nullable UUID column and its restrictive foreign key.
Existing rows remain unchanged. Downgrade drops the constraint and column.
API response schemas add an optional field, so existing clients remain valid.
No existing endpoint is removed.

## Tests

- Migration upgrade/downgrade and existing-row preservation.
- Python schedule decisions for guarded match/mismatch and legacy open.
- Python orchestration and execution-authority tests proving a job created
  after guarded open is parked or rejected before node work.
- Go orchestrator tests for guarded match, mismatch, and unguarded open.
- Go cancellation and finalization serialize on the job row so a cancelled job
  cannot be revived or have artifacts promoted by a stale finalizer.
- PostgreSQL guarded-open/store tests run mandatorily in CI.
- Coordinated-controller outcome-uncertain close tests use a cancelled request
  context and prove both close attempts still run without leaking errors.
- Runner and shell contracts require exact guarded authority in the open
  response.

## Deployment Gate

No live attempt is permitted until migration 031 is deployed to 127/150,
exact-SHA CI proves PostgreSQL tests ran, the schedule is `CLOSED` with no
guarded job, and a new read-only preflight succeeds. The failed third approval
is not reusable; the next attempt requires exactly
`批准第四次 unlisted canary`.
