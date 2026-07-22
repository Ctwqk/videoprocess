# Canary Intake Pause Design

Status: pre-approved for implementation on 2026-07-22.

## Context

The third owned-only unlisted canary created exactly one production task, then
stopped before planning. No job, upload operation, YouTube video, publication,
or feedback row was created.

The canary runner was built on 2026-07-12 around this sequence:

1. create one guarded `agent_tick`;
2. wait for exactly one selected production task;
3. set `channel_profiles.halted_at`;
4. approve the selected task and release its delayed `plan_task`.

The production queue authority fence added on 2026-07-19 correctly treats a
halted channel as a full kill switch. It rejects every channel-scoped queue
kind, including `plan_task`, `execute_task`, `publish_task`, promotion,
reconciliation, and metrics. The old canary sequence therefore deadlocks by
construction. Weakening the halt fence would also prevent a halt from being a
reliable incident response control.

The interrupted runner then exposed a second safety issue: cleanup on the
cancelled database connection failed with `DBAPIError`. The canary was manually
recovered through a fresh connection. The runner needs the same fallback.

## Goal

Introduce a durable channel state that stops new content intake while allowing
one already-selected task to complete its production, unlisted publication,
reconciliation, and feedback lifecycle. Use that state atomically in the live
canary and preserve `halted_at` as the full channel kill switch.

## Non-Goals

- No public publication path.
- No exception that lets a halted channel claim queue work.
- No activation of YouTube discovery.
- No automatic reuse of the third-attempt approval for another upload.
- No change to PDS deployment or the 126 host.
- No automatic deletion of any successfully uploaded video.

## Approaches Considered

### Durable intake pause with an atomic canary tick (selected)

Add `intake_paused_at` and `intake_pause_reason` to channel profiles. Scheduler
and intake queue kinds honor the pause; downstream queue kinds continue to
honor only the existing enabled/halted execution fence. A guarded canary tick
creates exactly one task and pauses intake in the same transaction.

This makes the operational state explicit, closes scheduler races, preserves
the halt kill switch, and allows delayed metrics to continue after upload.

### Canary exception for halted channels (rejected)

A queue predicate could recognize a canary approval payload and permit selected
downstream rows on a halted channel. This weakens the meaning of `halted_at`,
requires exceptions in both claim and execution fences, and risks allowing
future work past an incident halt.

### Long scheduler interval and aligned idempotency key (rejected)

The runner could leave the channel executable and use a very long tick bucket
to suppress another task. This relies on timing and idempotency details instead
of a durable operator-visible state, and it does not express the intended
long-running feedback behavior.

## State Semantics

Channel state has three independent controls:

| State | New intake | Existing task pipeline | Reconcile/metrics |
| --- | --- | --- | --- |
| disabled | blocked | blocked | blocked |
| halted | blocked | blocked | blocked |
| intake paused | blocked | allowed | allowed |
| enabled, not halted, intake open | allowed | allowed | allowed |

For this increment, intake queue kinds are:

- `agent_tick`, which can select and create production tasks;
- `ingest_discovery`, which acquires new external discovery signals.

Maintenance, account health, learning recompute, task planning/execution,
publication, promotion, reconciliation, and metrics are not intake.

## Data Model And API Visibility

Migration `030_channelops_intake_pause` adds nullable
`channel_profiles.intake_paused_at` and
`channel_profiles.intake_pause_reason`. Existing channels remain intake-open.

The Python ORM and `ChannelProfileRead` expose both fields. Channel responses
include them so operators can distinguish a quiescent canary from a halted
channel. The existing halt/resume API keeps its current meaning and does not
clear intake pause implicitly.

Manual tick enqueue and discovery ingestion reject intake-paused channels. The
Python scheduler excludes them, preserving parity even though the Go runner is
the only production ChannelOps owner.

## Go Queue And Execution Fences

The Go scheduler excludes intake-paused channels. The queue claim predicate
continues to require an enabled, non-halted authoritative channel for all
channel-scoped work, and additionally requires intake to be open for
`agent_tick` and `ingest_discovery`.

The transactional execution fence performs the same intake check for those two
kinds. This closes the race where a second intake row is claimed before the
first guarded tick commits its pause.

No halted-channel exception is added.

## Atomic Guarded Tick

The canary-created `agent_tick` includes:

```json
{
  "channel_id": "<uuid>",
  "canary_run_id": "<uuid>",
  "plan_delay_seconds": 300,
  "pause_intake_after_selection": true
}
```

The Go handler validates that the pause flag is a JSON boolean, the run ID is a
UUID, and a positive guarded plan delay is present. In the same transaction it
must:

1. evaluate the tick;
2. require exactly one task to be created;
3. insert that task and its delayed `plan_task`;
4. set the channel intake pause with reason
   `operator_preapproved_live_unlisted_canary`.

Zero or multiple selected tasks roll back the tick, task rows, queue rows, and
pause together. Ordinary scheduler ticks keep their current behavior.

## Canary Runner

After the task appears, the runner locks and verifies all of the following
before recording approval:

- exactly one task exists for the canary channel;
- exactly one queued `plan_task` refers to it;
- intake is paused with the exact canary reason;
- the channel is enabled and not halted;
- the selected task still has no job or publication side effect.

The runner then records the per-attempt operator approval and advances only the
guarded `plan_task` time. It does not set `halted_at`.

Evidence records `channel_intake_paused_after_exactly_one_task=true`. The open
gate rechecks the pause and the absence of a halt immediately before releasing
the one waiting job.

On success the channel remains intake-paused so durable 1h/6h/24h/72h/7d
metrics and reconciliation may run while no new content task can be selected.
On failure, existing cleanup sets the full halt and dead-letters active canary
queue work.

## Interruption Cleanup

Failure cleanup remains idempotent. The runner first attempts cleanup through
its active session. If that connection was invalidated by cancellation or a
database error, it retries cleanup once through a fresh database connection
while the shared-service tunnel is still open. Evidence records the first
cleanup error type and the successful fallback report without connection
strings or credentials.

Schedule close remains in the outer `finally` path. Redis pending audit runs
before the tunnel is torn down.

## Safety Invariants

- `halted_at` continues to block every channel-scoped queue claim and handler.
- Intake pause never authorizes work; it only removes intake authority.
- The guarded tick atomically creates exactly one task or creates none.
- Only private or unlisted visibility is accepted; this canary is unlisted.
- External assets remain ineligible without explicit human review.
- The global schedule starts and ends `CLOSED`.
- 126 receives no VideoProcess task or service.
- A failed attempt does not grant approval to a later attempt.

## Verification

Automated tests cover migration shape, Python scheduler/API behavior, Go
scheduler/claim/fence behavior, atomic one-task pause and rollback, canary
preapproval/evidence, resilient cleanup fallback, and all existing halt tests.

Deployment acceptance requires exact-SHA CI success, migration head 030,
127/150 services at desired replicas, no VideoProcess work on 126, schedule
`CLOSED`, zero unsafe backlog, zero Redis pending entries, and a fresh read-only
canary preflight. A fourth live attempt requires a new exact approval phrase.

