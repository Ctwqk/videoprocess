# Read-Only Unlisted Canary Preflight Design

**Date:** 2026-07-19

## Context

`scripts/run_vp_unlisted_canary.py` currently has one execution mode. Once
`--confirm-live-unlisted` is present, it performs readiness checks and then
continues into media generation, database setup, asset upload, scheduling, and
the external YouTube upload. Operators need a repeatable way to prove that the
same production dependencies are ready without consuming a live-canary
approval or changing production state.

## Decision

Add `--preflight-only` to the existing runner. Reusing the runner keeps the
readiness contract aligned with the live canary and avoids a second script that
could drift.

The two execution flags are mutually exclusive:

- `--preflight-only` runs read-only checks and does not require
  `--confirm-live-unlisted`.
- `--confirm-live-unlisted` retains the existing single live unlisted canary.
- Supplying neither or both exits with status 2 before opening a database
  connection.

## Read-Only Contract

Preflight performs only these operations:

1. Acquire the existing PostgreSQL advisory lock so a live canary and a
   preflight cannot overlap.
2. Read the global video schedule and require `CLOSED`.
3. Read global job, node, ChannelOps, upload, and publication backlog and
   require it to be empty.
4. Run the existing deployment readiness checks for the 127 runtime and 150
   publisher placement.
5. Read YouTubeManager authentication and quota readiness.
6. Read Redis stream pending counts.
7. Write a sanitized local evidence JSON file with mode `preflight_only` and
   permissions `0600`.

It must not generate media, upload an asset, create or update application rows,
call schedule mutation endpoints, enqueue work, or call a YouTube upload or
publication endpoint. Releasing the session-scoped advisory lock is allowed.

## Live Canary Compatibility

The live path continues to use the same readiness checks, then performs the
existing guarded single unlisted canary. Its failure cleanup and final
schedule-close behavior remain unchanged. The preflight path never invokes
that cleanup because it creates no canary-owned state.

## Evidence And Failure Behavior

Evidence records the selected mode, schedule status, backlog, deployment,
manager readiness, Redis pending audit, start/end timestamps, and either
`succeeded` or a sanitized failure. A failed read or failed invariant exits
nonzero. A preflight failure does not attempt a schedule write; the observed
schedule state is preserved in evidence.

## Tests

Tests must prove:

- CLI mode validation rejects neither/both and accepts each single mode.
- Preflight records readiness evidence and succeeds without calling the live
  canary executor or schedule mutation helper.
- Preflight failure is sanitized, persisted, and still performs no cleanup
  mutation.
- The existing live path still closes the schedule and preserves its cleanup
  behavior.

The implementation is complete only after targeted tests, the full backend
suite, the shell canary contract test, and `git diff --check` pass.
