# Active Redis Consumer Identity Guard Design

**Date:** 2026-07-23

## Context

VideoProcess production uses four Redis stream consumer groups:

- `vp:tasks:ffmpeg` / `ffmpeg-workers`
- `vp:tasks:ffmpeg_go` / `ffmpeg_go-workers`
- `vp:tasks:youtube_publisher` / `youtube_publisher-workers`
- `vp:events` / `orchestrator`

Redis retains consumer records after worker replacement. Production currently
has many old ffmpeg consumer records, but only one recently active consumer in
each group. Existing canary preflight and soak checks verify pending work and
lag, but do not prove that the active consumers belong to the managed 127/150
deployment.

## Decision

Add a read-only active-consumer identity audit to the canary runner and the
managed soak watcher.

An active consumer is one whose Redis `idle` value is at most 120,000
milliseconds. Each managed stream must have exactly one active consumer, and
its name must match the production topology:

| Stream | Allowed active consumer |
| --- | --- |
| `vp:tasks:ffmpeg` | `ffmpeg-worker@150-gpu:<positive pid>` |
| `vp:tasks:ffmpeg_go` | `ffmpeg_go-worker@colima-127:<positive pid>` |
| `vp:tasks:youtube_publisher` | `youtube_publisher-worker@150-publisher:<positive pid>` |
| `vp:events` | `orchestrator-api-<positive ordinal>` |

Every Redis consumer record, active or stale, must have a non-empty string
name. Historical consumers outside the idle window are not failures and are not
matched against the active-name allowlist. The evidence records their count,
not their full names.

## Alternatives

1. Reject every non-allowlisted Redis consumer record.

   This is simple but incorrectly treats Redis's retained history as a live
   worker. It would fail production immediately on harmless stale records.

2. Use the active idle window and topology allowlist.

   This gives an immediate, read-only production ownership gate while
   tolerating retained history. It is the selected approach.

3. Require persistent worker registration and signed admission.

   This remains the stronger target architecture, but it requires a schema,
   token lifecycle, heartbeat/revocation protocol, and coordinated rollout.
   It should follow this operational guard rather than block the next canary.

## Canary Runner

The Redis readiness audit reads `XPENDING` and `XINFO CONSUMERS` for all four
managed stream groups. For each stream it records:

- expected group;
- pending count;
- active consumer names;
- stale consumer count.

Preflight fails closed when any group is unavailable, pending is nonzero,
consumer data is malformed, there is not exactly one active consumer, or the
active name is outside the allowlist.

The live canary repeats the same readiness audit immediately before generating
media or mutating production state. Finalization keeps a separate final Redis
audit so startup evidence is not overwritten.

## Soak Watcher

When soak state is enabled, the watcher continues to validate service health,
placement, pending counts, and lag. It additionally calls `XINFO CONSUMERS`
for each managed stream through the host-networked VP Redis listener using
`redis-cli -p 6380 --raw`, then applies the same idle window and allowlist.

Missing consumer data, no active approved consumer, multiple active consumers,
or an unknown active identity adds the fixed external condition
`redis_consumer_identity_invalid`. The condition is critical and participates
in the existing optional auto-hold path. The watcher never deletes Redis
consumers and never creates activation state.

## Failure And Rollout Behavior

- The audit is read-only.
- A failed identity check prevents a live canary before media generation,
  schedule opening, enqueue, or upload.
- Disabled soak state remains a zero-side-effect early exit.
- Rollout updates the worker and publisher services before the event listener,
  but the identity guard itself changes only operator/preflight and watcher
  behavior.
- Existing services remain running if the new check detects a problem; the
  system fails closed by refusing new canary activity or by invoking the
  existing channel-scoped guard.

## Tests

Tests must prove:

- stale consumer records do not fail readiness;
- one approved active consumer per group passes;
- unknown, missing, duplicate, malformed, or unavailable active consumers
  fail closed;
- nonzero pending work still fails;
- live startup runs the audit before media generation;
- soak watcher emits `redis_consumer_identity_invalid` for each invalid case;
- disabled watcher behavior remains unchanged;
- no test path deletes consumers, opens a schedule, enqueues work, or uploads.
