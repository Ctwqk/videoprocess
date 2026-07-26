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
