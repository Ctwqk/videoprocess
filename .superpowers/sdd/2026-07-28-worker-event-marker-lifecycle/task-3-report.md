# Task 3 Report: Worker Continuity Gate And Host-150 Controls

## Status

Implemented the approved Task 3 scope in the isolated
`codex/worker-registration` worktree. No push, deployment, SSH, production
migration, production Redis mutation, schedule/channel/task change, upload,
publication, or canary was performed.

## RED Evidence

The initial worker RED command was:

```bash
cd backend
.venv/bin/python -m pytest tests/worker/test_worker_startup.py -q
```

Result: `6 failed, 39 passed`. The failures proved that the database
continuity helper was absent, Redis construction occurred before continuity,
`ACL WHOAMI` was not the first Redis command, and missing/stale/error/running
database states reached Redis.

The initial launcher and deployment RED commands were:

```bash
bash tests/test_worker_redis_marker_control.sh
bash tests/test_vp_deploy_sync_extension.sh
```

Results:

- marker-control test failed with `missing worker Redis marker launcher`;
- deployment test failed because no marker-continuity gate preceded registered
  Python worker updates.

Additional focused RED cycles caught and then guarded:

- incomplete active control config accepted for rollback;
- generation retirement accepted an empty Swarm task set;
- generation retirement accepted an unlabeled fixed-name job;
- malformed managed cron could remove a prior launcher/config;
- control services omitted explicit Swarm `replicated-job` mode.

## GREEN Evidence

Focused Python startup:

```bash
cd backend
.venv/bin/python -m pytest tests/worker/test_worker_startup.py -q
```

Result: `45 passed in 0.46s`.

Focused live marker lifecycle:

```bash
CHANNEL_OPS_POSTGRES_TEST_URL='postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test' \
WORKER_REDIS_MARKER_TEST_URL='redis://127.0.0.1:56379/14' \
  .venv/bin/python -m pytest -q \
  tests/worker/test_worker_startup.py \
  tests/services/test_worker_redis_marker_control.py \
  tests/migrations/test_worker_redis_marker_lifecycle_postgres.py \
  tests/worker/test_worker_redis_marker_lifecycle_redis.py
```

Result: `94 passed in 30.27s`.

Host-control and preserved deployment contracts:

```bash
bash tests/test_worker_redis_marker_control.sh
bash tests/test_vp_deploy_sync_extension.sh
```

Results: marker-control test printed
`worker Redis marker control tests passed`; the existing deployment suite
exited `0`.

Full language suites:

```bash
go test ./...
cd backend
.venv/bin/python -m pytest -q
```

Results: all Go packages passed; backend reported
`1323 passed, 102 skipped, 17 warnings in 73.09s`.

Scoped static checks:

```bash
.venv/bin/python -m ruff check \
  worker/main.py tests/worker/test_worker_startup.py
.venv/bin/python -c "import importlib; [importlib.import_module(name) for name in ('app.channel_agent.worker_redis_marker_readiness_cli','app.channel_agent.worker_redis_marker_janitor_cli','app.services.worker_redis_marker_repair_cli','app.services.worker_marker_control_role_cli','app.services.worker_redis_marker_control')]"
```

Result: Ruff passed and every Python worker/control image module imported.

Repository advisory baselines remain unchanged:

- `ruff check .`: 15 existing findings, none in Task 3 Python files;
- `mypy app`: 62 existing errors in 22 files;
- targeted `mypy worker/main.py`: 21 transitive existing errors in 9 other
  files and no `worker/main.py` error.

The required full run with both optional live endpoints produced
`1372 passed, 3 skipped, 50 failed`. Forty-nine failures came from the
provided PostgreSQL database being behind the application model
(`channel_profiles.intake_paused_at` is absent). One existing AOF restart test
returned unready once in the full run and then passed immediately in isolation
(`1 passed in 2.58s`). The focused combined PostgreSQL/Redis marker run above
then passed all 94 tests.

## Implemented Surfaces

- Registered Python startup now opens PostgreSQL, registers, calls
  `public.vp_require_worker_redis_continuity(90)`, constructs Redis, proves the
  configured named identity with `ACL WHOAMI`, and only then creates the
  consumer group.
- All continuity database failures collapse to
  `worker_redis_continuity_unready`, revoke the startup registration, and
  perform zero Redis construction.
- `worker-redis-marker-control.sh` exposes only `readiness`, `janitor`, and
  `status`, with mode locks, strict config/status parsing, fixed
  `replicated-job` services, exact ccttww-lap placement, bounded completion,
  and sanitized output.
- The deploy extension validates exact constructure-runtime
  generation/identity/AOF/noeviction state and three independent existing
  Redis secrets before provisioning.
- Readiness, janitor, and repair database roles and database URL secrets are
  fresh and generation-scoped. Swarm receives database URLs through stdin.
  Scheduled jobs mount only their matching database/Redis secrets at mode
  `0400`; repair is never scheduled.
- The marked cron block preserves independent VP, PDS, feature, schedule, and
  channel entries. Candidate readiness precedes every registered Python
  worker update.
- Rollback creates and proves a fresh prior-image control generation before
  revoking failed or superseded roles/secrets.
- The four required July 26 plan/design documents now identify the exact SQL,
  CLI, ACL, launcher, cron, secret, status, test, and conservative repair
  surfaces. No repair path authorizes `XADD`.

## Changed Files

- `backend/worker/main.py`
- `backend/tests/worker/test_worker_startup.py`
- `backend/Dockerfile.worker`
- `deploy/swarm/worker-redis-marker-control.sh`
- `tests/test_worker_redis_marker_control.sh`
- `deploy/swarm/deploy-sync-extension.sh`
- `tests/test_vp_deploy_sync_extension.sh`
- `docs/superpowers/plans/2026-07-26-production-worker-registration.md`
- `docs/superpowers/specs/2026-07-26-production-worker-registration-design.md`
- `docs/superpowers/plans/2026-07-26-production-redis-acl-and-infra-auto-deploy.md`
- `docs/superpowers/specs/2026-07-26-production-redis-acl-and-infra-auto-deploy-design.md`
- `.superpowers/sdd/2026-07-28-worker-event-marker-lifecycle/task-3-report.md`

`backend/Dockerfile.ffmpeg-worker-go` is an intentional no-op. Task 3 control
jobs run the reviewed Python worker/control image, and adding Python to the Go
image would violate the resolved runtime boundary. Go worker
registration/continuity remains the subsequent original worker-registration
Task 3.

## Self-Review

- No Redis client constructor, `PING`, `ACL WHOAMI`, group command, reconciler,
  or consumer can run before the database continuity function succeeds.
- Task 2 `error` and overlap/running states fail through the same stable
  unready reason as missing and stale status.
- Neither launcher command nor deploy command carries a database/Redis URL or
  password in argv or environment. CLI logs are accepted only through exact
  sanitized JSON shapes.
- Runtime state and active config use exact field allowlists; host-126 IPs,
  aliases, and placement strings are rejected.
- Fixed-name jobs require valid generation labels and exactly one terminal
  task before their database roles/secrets can be retired.
- Repair database credentials are versioned for explicit operator use but
  mounted into neither readiness nor janitor.
- No new Go registration code, worker graph behavior, publication behavior,
  schedule/channel control, or unrelated refactor was introduced.

## Concerns

- The optional live PostgreSQL database must be migrated to the repository
  schema before the repository-wide live integration run can be green. This
  task did not migrate it.
- The existing Redis AOF restart test was transiently unready once during the
  full run but passed immediately alone and in the subsequent 94-test focused
  run.
- On a first-ever marker-control deployment with no prior active generation,
  candidate readiness failure intentionally leaves failed candidate
  roles/secrets in place. Revoking them without first proving a fresh prior
  generation would violate the rollback contract; an operator must establish
  a ready reviewed control generation before retirement.
- Production remains deliberately fail-closed until the independent
  constructure-runtime plan supplies the exact runtime state and Redis ACL
  secrets.
