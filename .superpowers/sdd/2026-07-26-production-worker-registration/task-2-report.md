# Task 2 Fix Round 2 Report

Status: fix round 2 implementation complete; re-review pending

Base implementation:
`ce25840 feat(workers): register python consumers before redis`

Prior fixup:
`38fc590 fixup! feat(workers): register python consumers before redis`

This report is included in the round 2 fixup commit.

## Rereview Findings Closed

- Replaced worker-reported source identity with an orchestrator-created
  `worker_task_dispatches` authority row. Every registered task has a mandatory
  dispatch key, canonical payload hash, stream, granted group, job/node, and
  delivered Redis message ID. A worker-principal function can attest only an
  exact delivered dispatch while its live lease and node claim are fenced.
- Added immutable `worker_task_delivery_attestations`, registered event
  receipts, and per-event deliveries. Receipt acceptance uses the control-plane
  observer under the registration-shared advisory fence and verifies the exact
  attestation before locking node authority and applying completion/failure.
- Deduplicates by both event identity and source attestation. An exact
  same-attestation replay aliases to the applied receipt without reapplying.
  A payload/claim mismatch creates a durable quarantined delivery with a reason
  code; only that proven event is ACKed and the source task remains unresolved.
- Made initial QUEUED state, input artifact IDs, and dispatch-row staging one
  authority transaction. Downstream and retry dispatches are staged in the
  receipt transaction. An independent DB poller delivers pending dispatch rows,
  so event PEL retention is not required for outbox progress.
- Removed receipt-count deletion assumptions. Source-task ACK state now belongs
  to the attestation and moves monotonically through pending, authorized, and
  acknowledged. Both live-worker and receipt-authorized XACK paths update it
  idempotently. Terminal cancellation with no event can be deleted once its
  exact task is ACKed and all existing deliveries/dispatches are resolved.
- Added a durable pre-XACK authorization state. If Redis XACK succeeds and the
  final DB acknowledgement commit fails, the independent reconciler may repeat
  only the exact authorized XACK under the registration-shared fence. Arbitrary
  stale task identities remain in the PEL.
- Removed receipt/outbox CASCADE loss paths. PostgreSQL guarded cleanup and the
  SQLite fallback block deletion until every dispatch is delivered, every
  attestation is acknowledged, and every existing receipt/event delivery is
  resolved; cleanup then deletes authority rows in controlled order.
- Moved URL-download object persistence into the worker's fenced artifact
  callback. MinIO thread uploads are shielded and joined before cleanup, and the
  worker rechecks the DB-clock lease after remote save but before pointer insert.
  Revocation/expiry during save rolls back and cleans the completed object.
- Cache application now resolves newly ready cached descendants to a fixed
  point and finalizes terminal jobs/final artifacts in the same receipt
  transaction. Completion-derived writes and downstream outbox rows cannot
  precede receipt handoff.
- Preserved worker-principal-only `vp_require_worker_lease`. The PUBLIC-revoked
  observer is read/lock-only, has no worker-principal bypass, and shares the
  registration advisory fence. YouTube keeps the DB-clock 150-second margin,
  120-second POST bound, and 15-second durable submitted-transition bound.
- Updated the design and Task 3/Task 4 plan for reviewed PostgreSQL functions,
  exact attestation/receipt fields, Go Redis stream and artifact files, nine
  worker functions, an independent least-privilege orchestrator role, and Redis
  EVAL/GET/SET/XADD marker ACL plus 30-day marker retention/cleanup.

## RED Evidence

- Registered task without a dispatch key or an exact orchestrator-created,
  delivered dispatch row failed claim/attestation tests.
- Cross-node, wrong stream/group/message/hash and ungranted principal cases
  failed separate-principal PostgreSQL tests before exact attestation checks.
- Same-attestation replay with a new event ID initially reapplied or raised;
  mismatch initially had no durable quarantine resolution.
- Initial node QUEUED state committed before dispatch staging in the injected
  crash-window test.
- Pending dispatch delivery initially depended on event processing and lacked
  an independent poller.
- Redis-success/DB-failure task ACK initially had no durable authorization
  state from which exact reconciliation could recover.
- Job deletion initially used receipt-count equality and blocked an acknowledged
  cancelled/no-event task.
- Delayed thread upload cancellation initially allowed cleanup to race an
  upload that was still running.
- Leaf cache-hit completion initially stopped before fixed-point finalization.

## Verification

- Focused Task 2/startup/admission/lifecycle/YouTube/handler/receipt/cache/storage
  suite: `281 passed, 13 warnings in 2.11s`.
- PostgreSQL 16 migration, separate-principal, privilege, exact-attestation,
  concurrency, handoff, ACK recovery, and guarded-deletion suite:
  `3 passed in 8.42s`.
- Full backend: `1228 passed, 74 skipped, 17 warnings in 71.62s`.
- Targeted Ruff: `All checks passed!`.
- Targeted Mypy with skipped transitive imports:
  `Success: no issues found in 9 source files`.
- `git diff --check`: passed.
- Ordinary targeted Mypy also exposed 22 inherited transitive errors outside
  this Task 2 diff; the four errors in changed files were fixed before the
  isolated clean run.

## Remaining Concerns And Boundaries

- No deployment, push, production mutation, canary, upload, or publication
  occurred.
- Migration 034 remains undeployed and must be reviewed before Task 4 grants
  the nine worker functions or the separate observer/receipt/outbox privileges.
- Redis dispatch markers use a renewable 30-day retention window. Task 4 must
  install the documented least-privilege EVAL/GET/SET/XADD ACL and operational
  cleanup/hold policy before registered production traffic is admitted.
- Task 3 Go registration and Task 4 deployment wiring remain pending.
- The full suite still reports existing `datetime.utcnow()` deprecation
  warnings.
- Default publication remains private/unlisted; registered-worker YouTube is an
  intentional requirement.
