# Task 6 Report: Backlog Quarantine And Canary Runner

## Status

Implemented offline at base commit `855e9dcf8daa423d22267442def22a171f5de014`.
No live quarantine, deployment, schedule opening, YouTube upload, promotion, or
metrics mutation was run as part of Task 6.

## Delivered

- Added a channel-scoped, single-transaction quarantine service with dry-run
  default behavior, exact retained/changed IDs, idempotent apply behavior, and
  protection for terminal, measured, and publication-backed task/job evidence.
- Added a quarantine CLI requiring `DATABASE_URL`, `--channel-id`, and an
  evidence path. Mutation requires explicit `--apply`; evidence is written
  atomically with mode `0600` and no database URL is emitted.
- Added a guarded live-unlisted canary runner requiring
  `--confirm-live-unlisted` and `DATABASE_URL`.
- The canary holds a PostgreSQL advisory lock for its lifetime, verifies the
  closed schedule and empty unsafe backlog, checks deployed commit/publisher
  readiness and manager auth/quota, creates deterministic owned 8-second
  1080x1920 media, attests its SHA-256/provenance, and creates one isolated
  channel graph.
- The runner performs the manual-seed approval handoff atomically before plan
  processing, releases exactly one waiting job, drains after observing RUNNING,
  closes the schedule in `finally`, and never deletes the YouTube video.
- The runner requires one durable succeeded upload operation/publication/video,
  replaces the delayed auto-promotion with one immediate unlisted promotion,
  probes manager status/metrics, and distinguishes immediate platform feedback
  from pending age-appropriate durable metrics work in restrictive evidence.
- Failure after task creation halts the canary channel, holds remaining tasks,
  dead-letters its runnable queue rows, cancels active jobs/nodes, closes the
  schedule, and preserves durable audit rows without retrying an upload.

## TDD Evidence

The initial focused pytest run failed with missing
`app.services.channelops_quarantine`; the initial shell contract failed because
both scripts were absent. Implementations were added only after those expected
red failures.

## Verification

- Focused quarantine: `4 passed`.
- Canary shell/AST/helper contract: passed.
- Script `py_compile`: passed.
- Targeted Ruff on all owned Python files: passed.
- Targeted mypy on the service and both scripts: passed.
- Independent read-only Codex review completed. It identified caller-owned
  directory permission mutation and an incomplete quota-cost comparison; both
  were fixed with focused regression coverage. Its suggestion to cancel
  publication-backed runnable jobs was not applied because the binding
  requirement explicitly says to retain publication-backed task/job evidence;
  the canary preflight still refuses to open while any such job is runnable.
- Deterministic FFmpeg check: two independent files produced SHA-256
  `e6212dd80d5abbc67745a0bad40b375e9f50bba58b371e52c7afab9051b6a8ae`,
  duration 8 seconds, 1080x1920.
- Full backend: `516 passed, 11 warnings` in the final 63.48-second run. Warnings are existing
  `datetime.utcnow()` deprecations outside Task 6.
- Project-wide Ruff baseline: 21 existing findings outside owned files.
- Project-wide mypy baseline: 37 existing findings in 15 files outside owned
  files.

## Residual Concerns

- Live-only behavior still requires Task 7 deployment and the explicit live
  command. This task intentionally did not contact production PostgreSQL,
  Redis, Swarm, the deployed API, or YouTubeManager.
- Deployment readiness assumes the documented runtime marker path on 127, the
  publisher service name `vp-youtube-publisher-swarm`, and the documented 150
  placement constraints. The live runner fails closed if any differ.
- YouTube processing and metric availability are inherently asynchronous. The
  runner persists immediate feedback only when the manager exposes recognized
  fields; otherwise it requires and records pending durable metrics work and
  fabricates no snapshot.

## Production Reachability Fix

Live topology probing found that `10.0.0.127:18080` is the Go control-plane API
and returns 404 for `/api/v1/channel-agent/*`; the Python ChannelAgent API is
internal-only and unreachable from the intended host runner. A red-green fix
removed all ChannelAgent HTTP calls from the canary runner and now uses the
existing ORM plus `ChannelOpsQueueService` for equivalent transactional work:

- creates the complete owned/unlisted graph and one `agent_tick` atomically;
- revalidates the Asset as an owned generated video before graph commit;
- atomically cancels the delayed promotion and enqueues one immediate unlisted
  promotion with the API-equivalent key, payload, and priority;
- enqueues the immediate metrics probe with the API-equivalent hourly key.

Red evidence:

- `backend/.venv/bin/python -m pytest tests/services/test_unlisted_canary_runner.py -q`
  failed 4 tests because the old HTTP signature and local queue helpers were
  absent.
- `bash tests/test_vp_unlisted_canary_scripts.sh` failed because the old script
  still contained `/api/v1/channel-agent`.

Green verification:

- focused runner database tests: `4 passed`;
- quarantine plus runner database tests: `8 passed`;
- full backend: `520 passed, 11 warnings` in 63.68 seconds;
- `go test ./...`: passed;
- canary, deploy-sync, and production-smoke shell contracts: passed;
- script `py_compile`: passed;
- targeted Ruff: passed;
- targeted mypy with `MYPYPATH=backend`: passed;
- project Ruff baseline remains 21 pre-existing findings outside owned files;
- project mypy baseline remains 66 pre-existing findings outside owned files.

## Final Integration Review Fixes

The whole-branch review found and fixed two release-blocking cross-runtime
issues:

- The deployed `vp-channel-agent-runner-swarm` is the Go runner and claims the
  next queue row immediately after `agent_tick`; there is no five-second
  approval window. Canary ticks now carry a strictly bounded
  `plan_delay_seconds=300`. Both Python and Go runners propagate the delay to
  `plan_task.run_after`; the canary preapproval transaction halts the channel,
  records operator approval, and advances that one plan row to the current
  time. Invalid, fractional, boolean, string, negative, and over-one-hour Go
  payloads fail closed.
- A schedule close exception raised from `finally` could leave evidence marked
  `succeeded`. Close failure now always records `status=failed` and
  `schedule.final_state=UNKNOWN` without overwriting an earlier root failure.

Final fresh verification:

- full backend: `528 passed, 11 warnings` in 63.59 seconds;
- fresh PostgreSQL 16 migration: revisions `001` through
  `023_youtube_upload_operations` applied successfully;
- Go plan-delay integration test against that migrated database: passed and
  observed exactly `run_after = now + 300 seconds`;
- full `go test ./...` against the migrated disposable database: passed;
- all required deployment, topology, smoke, and canary shell contracts: passed;
- owned changed-file Ruff: passed;
- canary script targeted mypy: passed;
- script `py_compile` and `git diff --check`: passed.
