# Deployment Closeout, 2026-09-06

## Scope

Finish the approved 127/150 deployment and fifth unlisted canary. Do not enable
unreviewed public publication or place VideoProcess services on 126.

## Verified State

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
