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
