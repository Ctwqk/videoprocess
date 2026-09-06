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
- The VP app deployment timer is still paused pending recovery. Do not report
  automatic VP deployment as ready until that timer is restored and verified.
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

- Push the tested repair and verify exact-commit CI and 127/150 rollout.
- Restore the VP app deployment timer without altering the independent PDS timer.
- Complete one approved unlisted canary and verify publication/feedback evidence.
