# Diagnostic Project Scope Incident

## Summary

At 20:42 UTC a VideoProcess diagnostic invoked the shared deployment entry
incorrectly. It sourced the entry without arguments, then passed project
arguments to `main`. Argument parsing occurs while sourcing, so the project
selection remained empty and `main` selected all projects, starting with
ForWin on 126. This was an operator error by Codex, not a user request.

## Effects

- The ForWin build directory on 126 was synchronized to
  `3bd7d5e6a4f87525fba4a672c1029b9035d0e657`. The 150 mirror already held that
  commit at 20:13. Both resulting image builds reused all application COPY
  layers. The target build directory has no Git history; its exact prior
  filesystem contents were not snapshotted and are not claimed restored.
- The app update to `forwin-forwin:deploy-3bd7d5e6a4f8` failed immediately.
  The shared rollback returned the app to `compat-6809782`, but also rolled
  back five services that this attempt had never updated.
- All six ForWin services mount the persistent `forwin-swarm-data` volume;
  none mounts the synchronized source directory. Source synchronization
  excluded `data/`, `output/`, and `.env`. No data deletion or publication
  action was intentionally issued. Those exclusions and service mounts do
  not constitute a complete application-data integrity audit.

## Containment and Recovery

The erroneous main process was terminated. The already-running rollback
subprocess finished at 20:44:11; fresh process inspection confirmed no related
ForWin deployment subprocess remained. No other project was reached.

Docker's retained PreviousSpec and task history proved the exact immediate
pre-incident versions. Recovery checked image identity, incident update time,
and service version before each operation, refusing concurrent changes.
The five unintended rollbacks were reversed individually. Current and prior
specifications were compared without printing environment or secret values.
Docker populated two previously implicit defaults: ten-second stop grace and
the standard rollback configuration. No other specification differences were
accepted. These defaults are documented in the
[Docker service reference](https://docs.docker.com/reference/cli/docker/service/create/).

At 20:52 and again after 21:10, all six services were running at:

| Service | Restored Image |
| --- | --- |
| forwin-app-swarm | forwin-forwin:compat-6809782 |
| forwin-mcp-swarm | forwin-forwin:compat-6809782 |
| forwin-generation-worker-swarm | forwin-forwin:compat-6809782 |
| forwin-publisher-worker-swarm | forwin-forwin:deploy-d4fceac68343 |
| forwin-outbox-worker-swarm | forwin-forwin:deploy-d4fceac68343 |
| forwin-publisher-browser-swarm | forwin-publisher-browser:deploy-d4fceac68343 |

Do not restore the older 18:53 inventory: independent ForWin work had changed
these services before the incident. The exact immediate baseline takes priority.

## Prevention and Evidence

The corrected diagnostic supplies arguments before sourcing and asserts the
exact two-project selection. An additional dispatch wrapper refuses any project
other than `vp-app` and `vp-feature-aggregator`. The normal production entry
continues to receive its arguments directly. No shared ForWin deployment code
was modified as part of this containment.

Local ignored evidence:

- `.runtime/youtube-canary/22f0-full-deploy-diagnostic.log`: erroneous attempt.
- `.runtime/youtube-canary/restore-forwin-incident.py`: guarded restoration.
- `.runtime/youtube-canary/audit-forwin-spec.py`: nonsecret difference audit.
- `.runtime/youtube-canary/22f0-scoped-deploy-diagnostic.log`: corrected VP-only attempt.

VideoProcess placement remains 127/150. No sixth canary or public upload was
started. The incident is separate from the remaining VP database session
retirement permission failure, reproduced with a real isolated PG16 connection.
