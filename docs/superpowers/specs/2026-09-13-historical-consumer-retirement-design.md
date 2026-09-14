# Historical Consumer Retirement

## Scope

Repair only the deployment failure caused by multiple retained generations of
registered worker Redis consumers. Then integrate the five uncovered local
branch ancestries, resolve conflicts, push main normally and fast-forward all
local branches/worktrees to the resulting main. Preserve untracked files and
ignored evidence. Do not activate channels, generate/publish videos, change
schedules, involve node 126, or expand the production-feedback goal.

The user has explicitly approved this fix, conflict resolution and branch
consolidation, and previously preapproved designs/plans. No new approval gate
is required for this scope.

## Evidence And Choice

At source 1af8723 the current pin document permits only one direct predecessor.
Redis retains older CPU and publisher identities whose database registrations
and grants are revoked. Native rollback creates another generation but cannot
retire these older identities, so another deployment cannot converge by waiting.

Use a bounded extension of the existing immutable pin/job/attempt protocol.
A one-time metadata repair would leave subsequent rollback cycles vulnerable;
an independent background cleaner would introduce a second mutation authority.
Neither alternative is included.

## Pin Contract

- Keep version 1 decoding, canonical bytes, command bytes and behavior intact.
- Version 2 adds `ancestors` to each worker: an immutable tuple of IdentityPin
  values, newest to oldest, following `predecessor`. The existing predecessor
  remains exactly the baseline-captured direct predecessor.
- `WorkerPin.retiring` returns `(predecessor, *ancestors)` with absent predecessor
  omitted. Ancestors without a predecessor are refused.
- At most 64 retiring identities per service, including the direct predecessor;
  at most 260 identities across four services including current registrations.
  Vision uses its existing direct predecessor and has no extra ancestors.
- Registration/instance/consumer IDs are unique. Grant IDs are unique except
  for the precisely defined retiring restart pairs below. Every adjacent pair
  has the same fixed service topology and strictly increasing epoch/generation
  toward the current identity. Foreign IDs, duplicate rows, forks, broken links,
  cycles, overflow or incomplete facts are refusals, never truncation.

### Verified Same-Grant Restart Exception

Read-only production evidence additionally shows CPU epoch268 was revoked with
`worker_redis_continuity_unready` and no `superseded_by`, followed six seconds
later by epoch269 under the exact same grant/generation. This is not a transient
missing current worker. Version2 must prove this narrow restart edge without
editing historical records; version1 stays unchanged.

Registration, instance and consumer IDs remain globally unique. Grant IDs may
repeat only for contiguous retiring registrations with identical grant-bound
identity fields (service/type/host/slot/capability/release/image/principal and all
endpoint fingerprints), equal generation, adjacent increasing lease epochs and
strictly increasing registration time. Current-to-predecessor still requires a
new grant/generation. Every repeated SQL grant fact must be exactly identical;
collapse only the exact pin-required multiplicity to one logical grant fact.

An absent successor pointer is accepted only for such a same-grant adjacent
retiring pair when the older registration is revoked for precisely
`worker_redis_continuity_unready` and its revocation time lies between its own
registration time and the newer registration time. A nonnull wrong pointer,
other reason, missing/reordered epoch, different grant, or changed identity is
still refused. The shared retiring grant must be revoked and both old leases
expired before mutation, as with other retiring identities. Explicit same-grant
successor links may use the same adjacent identity proof.

Capture and the restricted SQL guard use the same exact edge predicate; no
generic chronological fallback, schema/data repair, active-grant consumer
retirement, or broader cleanup authority is introduced.

## Capture

Baseline and current snapshot envelopes stay unchanged. For the current
capture, read the three fixed Redis consumer inventories with the mounted
control credential and verify the actual principal. Select only historical
identities actually present, plus the immutable baseline predecessor. Query
their registrations and grants with the existing read-only capture identity.
Build the complete chain to the captured current registration, including absent
intermediate consumers needed to prove provenance. A bounded recursive query
stops at the oldest needed identity; it does not capture all historical rows.
Use a sentinel beyond the bound to refuse overflow. Recheck current pin identity
and all required names; never guess an ancestor by name syntax alone.

`build_capture_pins(..., history=None)` keeps the legacy version 1 result.
With history, each service maps to its full newest-first retiring IdentityPin
sequence; the resulting version 2 pins and command hashes are stored in the
existing journal and mounted immutable secret. Redis reads and all connection
cleanup remain bounded; no credential appears in diagnostics.

## Database And Runtime

Add migration `045_registered_consumer_history`, after 044. Preserve the existing
version 1 SQL guard. Add the same restricted signature under the new name
`vp_registered_consumer_reconcile_history_guard(text,uuid[],uuid[])` for version 2.
It retains the existing principal/role, CLOSED/no guarded job, global no-work,
terminal-upload proof, row locks, observed time and typed result checks.

The new guard accepts at most 256 retiring registration IDs and verifies each
superseded_by link stays within the selected set, on the same service/host/slot,
with strictly increasing epoch and increasing grant generation, except the
same-grant retiring restart proof above. Only the four current IDs
may be active roots. Thus every selected retired chain must terminate at its
corresponding current identity. Preserve SECURITY DEFINER, pg_catalog search
path and operator-only execution; no new table privilege is granted.

Runtime sends all retiring IDs to the version-appropriate guard, verifies every
pin's endpoint and typed facts, and checks each old row's exact successor,
revoked grant/registration, expired lease and reason. Existing total/IO/lock
timeouts, cleanup, authority callbacks and replay prohibition are unchanged.

## Redis Mutation

All five PEL checks, three zero-lag checks and vision/events membership checks
remain. Inventories may contain only current plus explicitly pinned retiring
identities. All present old identities must be idle for more than 120 seconds;
the current identity must be present and ready. Absent intermediate identities
are acceptable; unpinned identities are not.

Version 1 keeps its Lua bytes. Version 2 uses a fixed bounded Lua program with
current plus up to 64 exact old names. Validate all names, complete inventory,
PEL, lag and age before deleting any old consumer. Execute at most one EVAL per
stream using the existing durable attempt reservation. A successful `retired`
reply attests that the complete requested old-name set is absent, not that every
name was deleted in this invocation. Already-absent replies and the final
read-only assessment remain supported. Partial/uncertain execution does not
authorize a second EVAL. No broad wildcard, DEL, XTRIM or stream deletion.

## Acceptance

1. Version 1 canonical/hash/Lua compatibility and existing negatives still pass.
2. Multi-generation forward/rollback fixtures retire only pinned old consumers;
   absence, unknown identity, changed/forked/missing chain, unexpired lease,
   backlog, changed credentials and uncertain attempt remain fail-closed.
3. Real disposable PG16 tests prove restricted role and complete chain locks;
   real disposable Redis 7.4 tests exercise Lua and natural aging.
4. Backend, frontend, Go and deployment contracts pass on integrated source.
5. Normal push and native deploy provide a terminal success receipt with exact
   live release identities. Do not substitute CI success for deployment success.
6. Main contains all five original local branch tips as ancestors; local main,
   origin/main and all local branch/worktree HEADs equal the final commit.
   Prune only verified missing worktree metadata, preserving existing files.
