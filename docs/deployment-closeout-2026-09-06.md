# Deployment Closeout, 2026-09-06

## Scope

Finish the approved 127/150 deployment. Do not enable unreviewed public
publication or place VideoProcess services on 126. The fifth unlisted canary
already ran on July 26; its authorization is not unused.

## Current Checkpoint, September 6 At 16:25 UTC

- The first registered-worker rollout, release `fab36e3a818b`, is verified.
  Transaction `tx-d01da0bb551fb3e07da99515c5c5be0a` is archived as `DONE`,
  `succeeded`, revision 85. Worker, marker, and control promotion completed;
  the VERSION 2 control manifest retains all seven selected secret identities.
- Release `3ecb936d2f48` passed all four CI jobs (run `34041768295`). The
  scheduled 16:00 deployment pulled and built it automatically, but stopped
  safely before service changes during vision-consumer verification. Runtime
  services remain on `fab36e3a818b`, not the newer built image.
- Redis records now include the worker-registration UUID. The pending repair
  recognizes canonical UUID identities, waits for superseded consumers to be
  idle for more than two minutes, and atomically rechecks every record before
  removing only zero-pending stale records. The Swarm waiter preserves exit 10
  for a failed check-only task and allows the full reconciliation window.
  Each rolling replacement now runs pre-apply and final safety gates followed
  by reconciliation, even when its pre-update consumer audit was converged.
- PDS remains independently deployed at
  `6b8f8be32399fb0cf4278198e3d26d77cc8e8fd6`. VP and PDS retain separate
  15-minute polling schedules. No VideoProcess deployment targets 126.
- Fifth-canary evidence is
  `.runtime/youtube-canary/unlisted-canary-a3148595-a060-440d-a0d0-5826ee7a4e96.json`.
  It ran from 02:43 to 03:16 UTC on July 26, timed out, and was cancelled.
  It created no upload operation, publication, or YouTube video. A sixth live
  test requires fresh approval; no such upload has been started at this
  checkpoint. A successful full production/feedback loop is still unproven.
- Remaining acceptance is exact-commit CI and automatic rollout of this repair,
  a fresh read-only preflight, and separately authorized live publication.
- Final local repair verification: 1585 backend tests passed, 166 optional
  integrations skipped, 17 deprecation warnings. The 92 vision-cutover tests
  include 12 real isolated Redis 7.4.7 cases with no skips. All 184 canary-runner
  tests and the worker-admission deployment contract passed. Changed Python
  files pass Ruff and the changed service passes mypy; full-tree advisory
  checks retain the existing 15 lint and 61 type findings.

## Automatic Rollout Follow-up, 17:20 UTC

`c2bb0de7b2e653b1c0db9a056f7ce25e2242b22b` passed all four GitHub CI jobs
(`34045807906`). The normal 17:15 timer automatically built and deployed it;
no manual deployment trigger was used. Its vision precheck correctly returned
10, its safety check passed, and API/AutoFlow/frontend were updated on 127.

At 17:20:04 the forward deployment failed before any worker replacement.
Read-only observation reproduced the marker readiness cache disappearing at
17:25:01 during its periodic refresh and returning `ready` at 17:25:05. The
deployment gate now waits at most 60 checks for this exact missing-status
response; invalid, stale, unready, and unexpected responses still fail
immediately. Deployment and marker-control tests passed locally, and focused
review found no concrete P1/P2 issue in this change.

Automatic rollback then stopped before creating its first marker database
secret: the durable helper accepts prepared-secret operations only during
forward phases. Transaction `tx-e84fb31f632be927e6abe9ffb642fc79` remains
`ROLLBACK_PREPARING`; its selected rollback marker is `m-rb-e84fb31f632b-1`.
The rollback-phase authorization repair is implemented: only the allocated
marker's three database secrets and provisioned runtime authorities bound to
the selected baseline control are accepted. `ROLLBACK_APPLYING` permits only
idempotent reuse of already recorded identities. No schema or production
journal rewrite is required. All 15 focused tests, existing rollback contracts,
marker-control tests, and recovery-executor tests passed; the real revision-71
journal and its three marker bindings validate offline. Focused review found
no concrete P1/P2 issue. Exact-commit CI and production recovery remain pending.
Workers still run `fab36e3a818b`; the mixed deployment must not be reported
as converged. No upload or public production has been started.

## Recovery Closeout, 18:50 UTC

Release `7bcb9044cb3cd6363125e9b6def96b63c48cd89e` passed all four CI jobs
(`34049348630`), including 1736 backend tests (15 optional skips). Its ordinary
recovery entry exposed a missing live-network initialization before loading the
prior marker configuration. After resolving the network with the existing
identity validator, recovery restored API/frontend and reached
`ROLLBACK_VERIFIED`, revision 79. AutoFlow retains the migration-compatible
attempted release; the four workers were never replaced by this transaction.

The final repair batch is limited to this interrupted recovery:

- Resolve and verify the canonical network before reading prior marker config.
- Represent zero attempted rollback workers as a verified empty candidate set,
  with all four current manifests retained as immutable promotion preconditions.
  Preserve ordinary durable intent, replay, and ordered promotion checks.
- A no-op worker commit does not drain unrelated retirement journals.
- Validate untouched workers against their baseline identity during control
  rollback, rather than requiring nonexistent rollback candidates.
- On restart in rollback retirement, clean failed/stale candidate generations
  and namespaces before archiving, matching uninterrupted recovery behavior.

Tests and exact-commit CI must pass before deploying this batch. The release
is not yet converged. No sixth canary or public upload has been started.

Pre-push verification: 23 transaction, nine prior-marker-config, and seven
control-finalizer tests passed. The complete rollback contract, worker admission
deployment, marker control, recovery executor, CI workflow, shell syntax, and
changed-file lint checks passed. Independent review's single fixture finding
was fixed and re-reviewed with no remaining P1/P2 findings. A fresh production
status query returned `CLOSED`, no guarded job, and zero waiting/active jobs or
queued/running nodes. PDS independently polled its unchanged revision at 18:52.

CI `34053626183` for `906c8680144aaadb873fd2fe249fa3dd1731a752` passed Go,
frontend, backend, and all 39 focused tests. The deployment job failed at an
older control-manifest fixture that invoked rollback finalization without an
active journal. The follow-up changes only test setup and CI ordering/log labels:
it supplies validated durable state and exact live identities, retains both
byte-for-byte manifest assertions, and runs short contracts before the long
entry-point suite. No production validation is relaxed and no service rollout
has been authorized by this failed CI run.

The notes below are earlier checkpoints, not the current deployment state.

## Earlier Verified State

- GitHub main before this repair: `8940cac83c0f9dd374cbc7408f393370b8967f3d`.
- 150 is reachable from this workstation through SSH jump host 127.
- 150 hosts the Swarm manager, GPU/vision workers, and YouTube publisher.
- 127 hosts the application runtime and CPU worker.
- PDS independently polls its repository every 15 minutes. Its deployed commit
  is `6b8f8be32399fb0cf4278198e3d26d77cc8e8fd6`; a fresh poll succeeded.
- The VP app deployment timer is restored, independently of the PDS timer.
  A fresh scheduled poll pulled `17894bfe3598` and correctly waited for CI.
- The production schedule is CLOSED; no fifth upload has been started.

## Production Bootstrap Applied

`deploy/swarm/bootstrap-deploy-migrator.sql` committed successfully against
`videoprocess` on 150 after a schema backup and isolated PostgreSQL 16 rehearsal.
The database and its 211 public tables/indexes now belong to
`vp_deploy_migrator`. The deploy principal is not a superuser and has no usable
inherited/SET-role parent. Other databases retain their original ownership.
The script is intentionally one-shot; a repeat fails without changes.

## Local Verification

- Full backend suite: 1469 passed, 128 skipped, 17 deprecation warnings.
- Real non-superuser PostgreSQL lifecycle: provision/retry/revoke passed.
- Bootstrap rehearsal: 13 checks passed, including full transaction rollback.
- Worker admission deploy, rollback, recovery-executor, and CI contracts passed.
- Changed Python files pass Ruff and targeted mypy. Full checks still report
  unrelated baseline issues (Ruff 15; mypy 61 across 21 files).

## Remaining Acceptance

- Commit `17894bfe3598` passed all four GitHub CI jobs (run `34018440716`).
  Production recovery archived the old interrupted admission transaction.
- New images built on 127 and 150, but pre-apply vision verification stopped
  deployment: the task exited zero while Swarm log retrieval failed. The waiter
  now preserves the verified task exit code regardless of log transport.
- Verify exact-commit CI and 127/150 rollout for the waiter correction.
- Complete one approved unlisted canary and verify publication/feedback evidence.

## Runtime Follow-up

`5e05ded24918` passed all CI jobs (run `34020963732`) and reached service apply.
The same Swarm log transport fault then blocked marker readiness. Exact local
task/container output retrieval has been verified on 150 and now retains strict
identity, exit-code, and protocol checks. Marker compensation also retains its
baseline snapshot until transaction cleanup, preventing recovery hydration loss.
The interrupted production transaction must be recovered before canary approval
is consumed; API/frontend were updated, while workers still use prior images.

`d7e6415fb502` passed CI (`34023454874`) and marker readiness on 150. The staging
janitor also completed with exit zero, but its launcher rejected Docker's
`Complete` desired state. All corresponding janitor state checks now support
replicated jobs. Recovery also handles the tightly verified legacy-bootstrap
case where every worker authority is prepared but no worker service was touched;
this avoids requiring a prior control configuration that never existed. The live
transaction `tx-9fa931affc65f08856c615aa58e9c8c1` passed its read-only eligibility
check. No fifth canary upload has started.

`f945bf0eb324` passed CI (`34026467549`). The legacy pre-apply recovery
restored application snapshots and removed the unused staging/marker jobs and
all 18 prepared secrets. Its durable transaction is now `ABORTING`: runtime
grant revocation exposed the same PG16 creator-edge issue inside the database
operator functions, separate from the already fixed Python role lifecycle.
Migration `035_worker_creator_edges` replaces only activation/revocation bodies
and retains their signatures, owners, and ACLs. It preserves only the exact
bootstrap-granted, admin-only edge to a safe non-superuser function owner.
The janitor recovery preflight also resolves its pipeline network before
validating the pinned descriptor. These fixes require exact-commit CI and a
verified migration before resuming the interrupted transaction; no fifth
canary upload or public publishing has been enabled by this checkpoint.

Local verification: 1471 backend tests passed (128 optional integrations skipped),
plus 39 dedicated real PG16.14 operator lifecycle tests passed. CI runs that
focused lifecycle before other role-mutating suites and rejects skipped tests.
The deployment rollback regression also passed. Advisory full-tree Ruff/mypy
retain their unchanged 15/61 baseline findings.

After aligning legacy integration assertions with the new migration head,
the complete backend suite against isolated PG16 + Redis passed: 1623 passed,
15 optional integrations skipped, 30 deprecation warnings (247.89 seconds).
The earlier CI attempts stopped only at stale migration-version assertions;
the dedicated PG16 lifecycle and Go registration integration gates passed.

## First Worker Cutover

`fab36e3a818b` passed all four CI jobs (`34031018815`). Production migration
035 and recovery of transaction `tx-9fa931affc65f08856c615aa58e9c8c1` completed.
The following rollout registered the new CPU worker successfully, but its
observer lacked EXECUTE on `public.vp_worker_endpoint_fingerprints(jsonb)`.
A narrowly scoped grant to the verified deploy-read principal fixed the real
readiness command; generation `178869778511496` returned status `ok`.

Transaction `tx-d01da0bb551fb3e07da99515c5c5be0a` remains in rollback preparation:
this first cutover has no prior managed control environment. The explicit
`resume-legacy-forward` journal operation permits the original release to
continue only before any rollback control, marker, worker, promotion, or
retirement effect. It verifies complete prepared authority, credential file
identity, transaction/commit/revision, and applied worker snapshots. It keeps
the original failure evidence and all worker stages; it never marks deployment
verified. The operator must verify live service identities first, then run the
ordinary deployment/readiness/promotion checks. No fifth upload has started.
