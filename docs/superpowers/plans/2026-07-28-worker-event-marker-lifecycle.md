# Worker Event Marker Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the worker-registration breaker with executable Redis marker
continuity readiness, immutable cleanup authorization, an exact janitor, and a
conservative operator repair path.

**Architecture:** PostgreSQL remains the authority for marker expectations and
safe cleanup tombstones. Three separate non-worker control principals inspect,
delete, or repair exact Redis markers through bounded CLIs. A database-recorded
continuity result gates registered worker Redis construction on hosts 127 and
150, while host 150 runs recurring readiness and janitor jobs.

**Tech Stack:** Python 3.12, FastAPI service modules, SQLAlchemy 2, PostgreSQL
16 PL/pgSQL, Redis 7.4 Streams/Lua/ACL, Docker Swarm, Bash deployment tests,
pytest.

## Global Constraints

- This plan starts from commit `c992acfb02312b057c2ff64dad0666e840a7463c`
  or a descendant containing design commit `352f3ba`.
- Registration succeeds before
  `public.vp_require_worker_redis_continuity(90)` and both succeed before the
  first Redis client construction or command.
- Lease duration remains exactly 180 seconds; heartbeat remains exactly 60
  seconds.
- Event and dispatch markers have no TTL and Redis uses AOF plus
  `maxmemory-policy noeviction`.
- No command in this plan synthesizes a stream message, calls `XADD` during
  repair, changes an existing marker value, expires a marker, or trims a
  stream.
- Worker principals receive no marker deletion, `SCAN`, `CONFIG`, `INFO`,
  repair, or cross-worker stream privilege.
- Readiness, janitor, and repair use separate non-owner PostgreSQL and Redis
  principals and mode-0400 secret files.
- Raw database/Redis URLs, passwords, marker values, and event payloads never
  enter service environments, arguments, logs, evidence, or application audit
  rows.
- Fixed-search-path `SECURITY DEFINER` functions revoke `PUBLIC`, validate
  `session_user`, and reject owners, superusers, and unexpected principals.
- Host 150 owns recurring marker control. Registered workers remain on hosts
  127 and 150. Host 126 is forbidden.
- Public publication remains blocked. No deployment, upload, schedule opening,
  channel resume, soak activation, policy activation, or canary is authorized
  by this plan.

---

### Task 1: Add Marker Authority And Continuity Schema

**Files:**
- Modify: `backend/alembic/versions/034_worker_registrations.py`
- Modify: `backend/app/models/registered_worker_event_receipt.py`
- Create: `backend/app/services/worker_marker_control_role_cli.py`
- Create: `backend/tests/services/test_worker_marker_control_role_cli.py`
- Modify: `backend/tests/migrations/test_worker_registrations_postgres.py`
- Modify: `backend/tests/migrations/test_worker_control_plane_postgres.py`
- Create: `backend/tests/migrations/test_worker_redis_marker_lifecycle_postgres.py`

**Interfaces:**
- Produces
  `public.worker_redis_marker_cleanup_authorizations`,
  `public.worker_redis_continuity_status`, and
  `public.worker_redis_marker_repair_audits`.
- Produces
  `vp_list_worker_redis_marker_expectations(text,integer)`,
  `vp_begin_worker_redis_continuity_check(uuid,integer)`,
  `vp_finish_worker_redis_continuity_check(uuid,text,text,text,bigint,bigint)`,
  `vp_require_worker_redis_continuity(integer)`,
  `vp_claim_worker_redis_marker_cleanup(uuid,integer,integer)`,
  `vp_finish_worker_redis_marker_cleanup(uuid,uuid,text,text)`,
  `vp_load_worker_redis_marker_repair(text,uuid)`, and
  `vp_promote_observed_worker_event_emission(uuid,text,text)`.
- Extends `vp_resolve_worker_event_authority_for_job_deletion(uuid)` to insert
  exact cleanup authorizations before deleting source rows.
- Provides exact `EXECUTE` surfaces for stable
  `vp_marker_readiness_runtime`, `vp_marker_janitor_runtime`, and
  `vp_marker_repair_runtime` NOLOGIN roles. Versioned LOGIN roles inherit only
  one stable role.
- Produces:

```text
python -m app.services.worker_marker_control_role_cli \
  provision --generation <service-generation> --state-dir <absolute-path>
python -m app.services.worker_marker_control_role_cli \
  revoke --generation <service-generation> --state-dir <absolute-path>
```

- Reads the owner URL only from
  `WORKER_MARKER_CONTROL_OWNER_DATABASE_URL_FILE`. Provision writes three
  independent mode-0400 database URL files below a mode-0700 generation
  directory and prints only role names and stable reason codes.

- [ ] **Step 1: Write migration surface tests**

Add static assertions for all tables, constraints, function signatures,
fixed search paths, `PUBLIC` revocations, downgrade order, and model parity:

```python
def test_worker_registration_migration_has_marker_lifecycle_surface() -> None:
    sql = migration_source()
    for signature in (
        "vp_list_worker_redis_marker_expectations(text,integer)",
        "vp_begin_worker_redis_continuity_check(uuid,integer)",
        "vp_finish_worker_redis_continuity_check("
        "uuid,text,text,text,bigint,bigint)",
        "vp_require_worker_redis_continuity(integer)",
        "vp_claim_worker_redis_marker_cleanup(uuid,integer,integer)",
        "vp_finish_worker_redis_marker_cleanup(uuid,uuid,text,text)",
        "vp_load_worker_redis_marker_repair(text,uuid)",
        "vp_promote_observed_worker_event_emission(uuid,text,text)",
    ):
        assert signature in sql
        assert f"REVOKE EXECUTE ON FUNCTION public.{signature} FROM PUBLIC" in sql
```

Assert immutable proof columns:

```python
EXPECTED_PROOF_COLUMNS = {
    "marker_kind",
    "source_id",
    "marker_key",
    "redis_stream",
    "expected_message_id",
    "payload_sha256",
    "authorized_at",
}
assert EXPECTED_PROOF_COLUMNS <= cleanup_authorization_columns()
```

- [ ] **Step 2: Write PostgreSQL 16 RED tests**

The new live test must:

1. migrate an empty PostgreSQL 16 database;
2. create separate readiness, janitor, repair, and worker LOGIN roles;
3. deny every role direct access to all three new tables;
4. prove worker can call only
   `vp_require_worker_redis_continuity(90)`;
5. build one fully acknowledged event/dispatch authority chain;
6. call guarded job cleanup and prove a tombstone with exact key/message/hash
   survives source-row deletion;
7. prove unresolved authority creates no tombstone and cleanup rolls back;
8. race cleanup against a janitor claim and prove lock-order convergence;
9. prove continuity overlap, stale takeover at 300 seconds, fresh success at 90
   seconds, stale/error/missing failure, and invalid result rejection;
10. prove repair promotion accepts only an exact prepared emission/hash/message
    and writes a sanitized append-only audit row;
11. provision the three stable/versioned role pairs through the real CLI,
    verify exact function execution and direct-table denial, verify URL file
    mode `0400`, and revoke only the selected generation.

Use:

```python
await asyncio.wait_for(
    exercise_marker_lifecycle_under_real_roles(database_url),
    timeout=15,
)
```

- [ ] **Step 3: Run RED tests**

```bash
cd backend
CHANNEL_OPS_POSTGRES_TEST_URL=\
postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m pytest \
  tests/services/test_worker_marker_control_role_cli.py \
  tests/migrations/test_worker_redis_marker_lifecycle_postgres.py \
  tests/migrations/test_worker_registrations_postgres.py \
  tests/migrations/test_worker_control_plane_postgres.py -q
```

Expected: fail because the lifecycle tables and functions do not exist.

- [ ] **Step 4: Implement additive tables and immutability**

Add ORM and migration definitions equivalent to:

```python
class WorkerRedisMarkerCleanupAuthorization(Base):
    __tablename__ = "worker_redis_marker_cleanup_authorizations"
    id: Mapped[uuid.UUID]
    marker_kind: Mapped[str]
    source_id: Mapped[uuid.UUID]
    marker_key: Mapped[str]
    redis_stream: Mapped[str]
    expected_message_id: Mapped[str]
    payload_sha256: Mapped[str]
    authorization_state: Mapped[str]
    authorized_at: Mapped[datetime]
    claimed_by_run_id: Mapped[uuid.UUID | None]
    claim_expires_at: Mapped[datetime | None]
    finished_at: Mapped[datetime | None]
    result_code: Mapped[str | None]
```

Use unique constraints on `(marker_kind, source_id)` and `marker_key`. Permit
only:

```text
pending -> claimed -> deleted
pending -> claimed -> absent
pending -> claimed -> conflict
claimed -> claimed
```

The same run may renew an unexpired claim. Another run may take over only after
`claim_expires_at`. A trigger rejects changes to proof columns.

`worker_redis_continuity_status` is a singleton with `run_id`, `state`,
`reason_code`, `redis_run_id`, counts, `started_at`, and `finished_at`.
`worker_redis_marker_repair_audits` stores only source UUID, action, result
code, principal, and timestamp.

- [ ] **Step 5: Implement exact functions, cleanup insertion, and roles**

Use the existing global lock order:

```text
job -> node -> sorted registration fences -> attestation -> emission ->
receipt -> delivery -> dispatch -> cleanup authorization
```

Before source deletion, insert event and dispatch cleanup rows with
`ON CONFLICT DO NOTHING`, then reload and compare every immutable proof field.
Any mismatch raises `marker_cleanup_proof_mismatch` and rolls back.

Expectation listing pages by `marker_key > p_after_key`, limits 1-500, and
returns exact active rows plus tombstones. It never returns secrets.

Continuity begin uses an advisory transaction lock and returns `begun` or
`overlap`. Finish accepts only `ready` or `error`, stable reason codes, and the
exact current run. Require returns normally only when the latest result is
`ready` and `finished_at >= clock_timestamp() - max_age`.

Cleanup claim uses `FOR UPDATE SKIP LOCKED`. Finish accepts only
`deleted|absent|conflict` for the exact unexpired claim.

Repair promotion requires an existing prepared emission with matching stored
payload SHA-256, an acknowledged source dispatch proof, and the repair
principal. It never writes Redis and never accepts a replacement registration.

The role CLI validates generation as `^[a-z0-9][a-z0-9-]{0,62}$`, derives
bounded quoted role identifiers from its SHA-256 suffix, creates three stable
NOLOGIN roles and three independent versioned LOGIN roles, and grants each
LOGIN role exactly one stable role. Stable roles receive only schema `USAGE`
and exact function `EXECUTE`; every new table is explicitly revoked.

Generate each password independently with `secrets.token_urlsafe(48)`. Write
database URL files through a temporary mode-0400 file, `fsync`, and atomic
rename under a mode-0700 generation directory. Never put a password in process
arguments, environment variables, stdout, stderr, or SQL logs. `revoke`
terminates only sessions for the selected versioned roles, revokes membership,
drops the LOGIN roles, and removes only that generation's credential files.

- [ ] **Step 6: Run PostgreSQL and migration GREEN tests**

```bash
cd backend
CHANNEL_OPS_POSTGRES_TEST_URL=\
postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
  .venv/bin/python -m pytest \
  tests/services/test_worker_marker_control_role_cli.py \
  tests/migrations/test_worker_redis_marker_lifecycle_postgres.py \
  tests/migrations/test_worker_registrations_postgres.py \
  tests/migrations/test_worker_control_plane_postgres.py -q
.venv/bin/python -m ruff check \
  alembic/versions/034_worker_registrations.py \
  app/models/registered_worker_event_receipt.py \
  app/services/worker_marker_control_role_cli.py \
  tests/services/test_worker_marker_control_role_cli.py \
  tests/migrations/test_worker_redis_marker_lifecycle_postgres.py
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add backend/alembic/versions/034_worker_registrations.py \
  backend/app/models/registered_worker_event_receipt.py \
  backend/app/services/worker_marker_control_role_cli.py \
  backend/tests/services/test_worker_marker_control_role_cli.py \
  backend/tests/migrations/test_worker_registrations_postgres.py \
  backend/tests/migrations/test_worker_control_plane_postgres.py \
  backend/tests/migrations/test_worker_redis_marker_lifecycle_postgres.py
git commit -m "feat(workers): add redis marker lifecycle authority"
```

### Task 2: Implement Readiness, Janitor, And Repair Controls

**Files:**
- Create: `backend/app/services/worker_redis_marker_control.py`
- Create: `backend/app/channel_agent/worker_redis_marker_readiness_cli.py`
- Create: `backend/app/channel_agent/worker_redis_marker_janitor_cli.py`
- Create: `backend/app/services/worker_redis_marker_repair_cli.py`
- Create: `backend/tests/services/test_worker_redis_marker_control.py`
- Create: `backend/tests/channel_agent/test_worker_redis_marker_readiness_cli.py`
- Create: `backend/tests/channel_agent/test_worker_redis_marker_janitor_cli.py`
- Create: `backend/tests/services/test_worker_redis_marker_repair_cli.py`
- Create: `backend/tests/worker/test_worker_redis_marker_lifecycle_redis.py`

**Interfaces:**
- Produces `MarkerExpectation`, `ContinuityResult`,
  `MarkerCleanupAuthorization`, and `MarkerRepairEvidence`.
- Produces
  `check_worker_redis_continuity(db, redis, expected_user)`,
  `run_worker_redis_marker_janitor(db, redis, run_id)`, and
  `audit_worker_redis_markers(db, redis)`.
- Produces:

```text
python -m app.channel_agent.worker_redis_marker_readiness_cli check
python -m app.channel_agent.worker_redis_marker_janitor_cli run
python -m app.services.worker_redis_marker_repair_cli audit
python -m app.services.worker_redis_marker_repair_cli \
  restore-marker --source-id <uuid> --apply
python -m app.services.worker_redis_marker_repair_cli \
  promote-prepared --emission-id <uuid> --apply
```

- Reads only
  `WORKER_REDIS_MARKER_DATABASE_URL_FILE` and
  `WORKER_REDIS_MARKER_REDIS_URL_FILE`, both bounded regular files with mode
  `0400` in production.

- [ ] **Step 1: Write service and CLI RED tests**

Test secret loading before client construction:

```python
def test_marker_cli_rejects_environment_credentials_before_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret")
    monkeypatch.setenv("REDIS_URL", "redis://secret")
    with pytest.raises(MarkerControlConfigError):
        load_marker_control_config(production=True)
    assert client_factory_calls == []
```

Test stable readiness outcomes:

```python
@pytest.mark.parametrize(
    ("condition", "reason"),
    [
        ("loading", "redis_loading"),
        ("aof_disabled", "redis_aof_disabled"),
        ("aof_error", "redis_aof_unhealthy"),
        ("wrong_eviction", "redis_eviction_policy_invalid"),
        ("marker_missing", "active_marker_missing"),
        ("marker_mismatch", "active_marker_mismatch"),
        ("stream_missing", "event_stream_entry_missing"),
        ("payload_mismatch", "event_payload_mismatch"),
        ("incomplete_page", "expectation_page_incomplete"),
    ],
)
async def test_continuity_fails_closed(condition: str, reason: str) -> None:
    assert await continuity_reason_for(condition) == reason
```

Test compare-and-delete:

```python
COMPARE_DELETE = """
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
if current ~= ARGV[1] then return -1 end
return redis.call('DEL', KEYS[1])
"""
```

Test repair restore:

```python
RESTORE_IF_ABSENT = """
local current = redis.call('GET', KEYS[1])
if current then
  if current == ARGV[1] then return 0 end
  return -1
end
redis.call('SET', KEYS[1], ARGV[1], 'NX')
return 1
"""
```

The tests must prove neither script contains `XADD`, `XACK`, expiry, stream
trim, or value replacement.

- [ ] **Step 2: Write Redis 7.4 RED integration tests**

With `WORKER_REDIS_MARKER_TEST_URL=redis://127.0.0.1:56379/14`, prove:

- exact active marker and stream payload pass;
- missing/mismatched marker fails;
- prepared-without-message and no marker is consistent;
- exact janitor deletion succeeds once;
- absent janitor marker converges to `absent`;
- mismatched janitor marker remains and becomes `conflict`;
- repair restores only an absent exact marker after `XRANGE` hash proof;
- prepared promotion requires marker plus exact stream event;
- concurrent janitors delete once;
- Redis restart with AOF preserves markers and passes;
- restart without marker continuity fails;
- readiness/janitor/repair ACL users deny each other's mutation surfaces.

- [ ] **Step 3: Run RED tests**

```bash
cd backend
WORKER_REDIS_MARKER_TEST_URL=redis://127.0.0.1:56379/14 \
  .venv/bin/python -m pytest \
  tests/services/test_worker_redis_marker_control.py \
  tests/channel_agent/test_worker_redis_marker_readiness_cli.py \
  tests/channel_agent/test_worker_redis_marker_janitor_cli.py \
  tests/services/test_worker_redis_marker_repair_cli.py \
  tests/worker/test_worker_redis_marker_lifecycle_redis.py -q
```

Expected: fail because the services and CLIs do not exist.

- [ ] **Step 4: Implement canonical expectation verification**

`MarkerExpectation` includes:

```python
@dataclass(frozen=True)
class MarkerExpectation:
    marker_kind: Literal["event_emission", "task_dispatch"]
    source_id: uuid.UUID
    marker_key: str
    redis_stream: str
    expected_message_id: str | None
    payload_sha256: str
    payload: dict[str, str] | None
    source_state: str
    absence_allowed: bool
```

Hash Redis stream fields with sorted keys and compact JSON, matching
`backend/worker/main.py`. Require every field and value to be UTF-8 strings.
Page until a short page, reject repeated/non-increasing cursors, and cap one
run at 100,000 expectations.

Use `ACL WHOAMI`, `INFO persistence`, `INFO server`, and exact `CONFIG GET`
checks before expectations. Sanitize all exceptions to stable reason codes.

- [ ] **Step 5: Implement janitor and repair**

The janitor gets exact claimed rows from PostgreSQL and invokes
`COMPARE_DELETE` with one key and one expected value. It records the database
outcome in a separate transaction for each row so one conflict does not lose
prior progress.

Repair `audit` is read-only. `restore-marker` and `promote-prepared` require
`--apply`; without it they print a sanitized dry-run result. Before either
mutation:

1. load exact database repair evidence;
2. `XRANGE` the exact message ID;
3. verify canonical payload SHA-256;
4. verify current marker state;
5. apply only the exact atomic marker restore or exact database promotion.

Do not add a resend, force, delete, or arbitrary key option.

- [ ] **Step 6: Run focused GREEN and Redis restart tests**

```bash
cd backend
WORKER_REDIS_MARKER_TEST_URL=redis://127.0.0.1:56379/14 \
  .venv/bin/python -m pytest \
  tests/services/test_worker_redis_marker_control.py \
  tests/channel_agent/test_worker_redis_marker_readiness_cli.py \
  tests/channel_agent/test_worker_redis_marker_janitor_cli.py \
  tests/services/test_worker_redis_marker_repair_cli.py \
  tests/worker/test_worker_redis_marker_lifecycle_redis.py -q
.venv/bin/python -m ruff check \
  app/services/worker_redis_marker_control.py \
  app/channel_agent/worker_redis_marker_readiness_cli.py \
  app/channel_agent/worker_redis_marker_janitor_cli.py \
  app/services/worker_redis_marker_repair_cli.py \
  tests/services/test_worker_redis_marker_control.py \
  tests/channel_agent/test_worker_redis_marker_readiness_cli.py \
  tests/channel_agent/test_worker_redis_marker_janitor_cli.py \
  tests/services/test_worker_redis_marker_repair_cli.py \
  tests/worker/test_worker_redis_marker_lifecycle_redis.py
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add backend/app/services/worker_redis_marker_control.py \
  backend/app/channel_agent/worker_redis_marker_readiness_cli.py \
  backend/app/channel_agent/worker_redis_marker_janitor_cli.py \
  backend/app/services/worker_redis_marker_repair_cli.py \
  backend/tests/services/test_worker_redis_marker_control.py \
  backend/tests/channel_agent/test_worker_redis_marker_readiness_cli.py \
  backend/tests/channel_agent/test_worker_redis_marker_janitor_cli.py \
  backend/tests/services/test_worker_redis_marker_repair_cli.py \
  backend/tests/worker/test_worker_redis_marker_lifecycle_redis.py
git commit -m "feat(workers): control redis marker lifecycle"
```

### Task 3: Gate Workers And Install Host-150 Controls

**Files:**
- Modify: `backend/worker/main.py`
- Modify: `backend/tests/worker/test_worker_startup.py`
- Create: `deploy/swarm/worker-redis-marker-control.sh`
- Create: `tests/test_worker_redis_marker_control.sh`
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Modify: `backend/Dockerfile.worker`
- Modify: `backend/Dockerfile.ffmpeg-worker-go`
- Modify: `docs/superpowers/plans/2026-07-26-production-worker-registration.md`
- Modify: `docs/superpowers/specs/2026-07-26-production-worker-registration-design.md`
- Modify: `docs/superpowers/plans/2026-07-26-production-redis-acl-and-infra-auto-deploy.md`
- Modify: `docs/superpowers/specs/2026-07-26-production-redis-acl-and-infra-auto-deploy-design.md`

**Interfaces:**
- Python startup order becomes:

```text
load bounded secret files
open PostgreSQL
register worker
require fresh database-recorded Redis continuity
construct Redis client
prove worker ACL identity
create group and start event reconciler/consumer
```

- Produces:

```text
deploy/swarm/worker-redis-marker-control.sh readiness
deploy/swarm/worker-redis-marker-control.sh janitor
deploy/swarm/worker-redis-marker-control.sh status
```

- Installs marked cron entries:

```text
* * * * * .../worker-redis-marker-control.sh readiness
*/5 * * * * .../worker-redis-marker-control.sh janitor
```

- Uses fixed Swarm job names
  `vp-worker-redis-marker-readiness-job` and
  `vp-worker-redis-marker-janitor-job`, one replica, restart condition `none`,
  and placement `node.hostname==ccttww-lap`.
- Calls `worker_marker_control_role_cli provision` before creating Swarm
  secrets and mounts the generated readiness, janitor, and repair database URLs
  only into their matching control jobs. Rollback provisions a fresh prior
  generation before revoking the failed generation.

- [ ] **Step 1: Write startup-order RED tests**

Instrument registration, continuity, Redis constructor, and first command:

```python
assert calls == [
    "database_open",
    "registration_complete",
    "continuity_ready",
    "redis_constructed",
    "redis_whoami",
    "redis_group_create",
]
```

Add negative cases for missing, stale, error, and overlapping continuity
status. Assert Redis construction count remains zero.

- [ ] **Step 2: Write deployment RED tests**

`tests/test_worker_redis_marker_control.sh` must prove:

- dry-run mutates no cron, service, secret, marker, or status;
- exact job names, image, network, dedicated secrets, mode, and host placement;
- running job causes a clean skip;
- only its own completed job is removed;
- readiness runs every minute and janitor every five minutes;
- repair is never scheduled;
- logs contain reason codes and counts but no URLs, credentials, marker values,
  or payloads;
- `10.0.0.126`, host aliases for 126, and 126 placement are rejected;
- missing ACL identity, AOF, noeviction, secrets, or fresh DB status fails
  before registered worker update.
- generated database roles and secret files are generation-scoped, independent,
  mode `0400`, never printed, and revoked only after their jobs stop.

- [ ] **Step 3: Run RED tests**

```bash
cd backend
.venv/bin/python -m pytest tests/worker/test_worker_startup.py -q
cd ..
bash tests/test_worker_redis_marker_control.sh
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: fail because the startup gate and launcher do not exist.

- [ ] **Step 4: Implement worker gate**

Add a database-only continuity call to the registered startup object. The call
must happen after registration and before `redis.from_url`,
`redis.Redis`, group creation, `PING`, or `ACL WHOAMI`. On
`worker_redis_continuity_unready`, release/revoke the startup registration,
emit one sanitized reason code, and exit nonzero.

Legacy unregistered development startup remains unchanged outside production.
Registered production cannot bypass the check.

- [ ] **Step 5: Implement host-150 launcher and deploy integration**

The launcher:

```bash
case "$1" in
  readiness) launch_job "vp-worker-redis-marker-readiness-job" ;;
  janitor) launch_job "vp-worker-redis-marker-janitor-job" ;;
  status) print_sanitized_status ;;
  *) exit 64 ;;
esac
```

Use one nonblocking lock per mode, an exact allowlist of environment names,
secret-file mounts only, bounded service convergence, and sanitized output.
The deploy extension installs one marked cron block idempotently without
changing VP/PDS deploy cron or schedule cron.

Provision marker-control database roles first, create versioned Swarm secrets
from the generated files through stdin, deploy the jobs, and run one readiness
check. On failure, deploy a fresh prior-image marker-control generation and
prove it ready before revoking the failed roles and secrets. Never reuse a
failed generation's passwords.

Update both Dockerfiles so the CLIs and service modules are present in the
reviewed worker/control image.

- [ ] **Step 6: Make ACL and deployment plans executable**

Replace prose-only marker lifecycle requirements in both existing designs and
plans with exact references to:

- the migration functions and stable PostgreSQL roles from Task 1;
- the three CLIs and exact commands from Task 2;
- the three named Redis ACL users and command/key selectors;
- the launcher, exact cron entries, jobs, secrets, status gate, and tests from
  Task 3;
- the conservative repair matrix, with no `XADD` or automatic mutation.

The independent `constructure-runtime` plan remains responsible for creating
the Redis users and AOF/noeviction server configuration. VP deployment refuses
registered traffic until that exact runtime commit is deployed and readiness
passes.

- [ ] **Step 7: Run focused and full verification**

```bash
go test ./...
cd backend
CHANNEL_OPS_POSTGRES_TEST_URL=\
postgresql+asyncpg://postgres:postgres@127.0.0.1:55439/videoprocess_test \
WORKER_REDIS_MARKER_TEST_URL=redis://127.0.0.1:56379/14 \
  .venv/bin/python -m pytest
.venv/bin/python -m ruff check . || true
.venv/bin/python -m mypy app || true
cd ..
bash tests/test_worker_redis_marker_control.sh
bash tests/test_vp_deploy_sync_extension.sh
git diff --check
```

Expected: all Go, backend, marker-control, deployment, and diff checks pass.
Existing repository-wide Ruff/Mypy baseline findings may remain only when no
changed file appears in them; changed-source Ruff and targeted Mypy must pass.

- [ ] **Step 8: Commit**

```bash
git add backend/worker/main.py \
  backend/tests/worker/test_worker_startup.py \
  deploy/swarm/worker-redis-marker-control.sh \
  tests/test_worker_redis_marker_control.sh \
  deploy/swarm/deploy-sync-extension.sh \
  tests/test_vp_deploy_sync_extension.sh \
  backend/Dockerfile.worker \
  backend/Dockerfile.ffmpeg-worker-go \
  docs/superpowers/plans/2026-07-26-production-worker-registration.md \
  docs/superpowers/specs/2026-07-26-production-worker-registration-design.md \
  docs/superpowers/plans/2026-07-26-production-redis-acl-and-infra-auto-deploy.md \
  docs/superpowers/specs/2026-07-26-production-redis-acl-and-infra-auto-deploy-design.md
git commit -m "feat(deploy): gate workers on redis marker continuity"
```

## Final Review And Breaker Recheck

After all three tasks pass their independent task reviews:

1. generate one whole-plan review package from design commit `352f3ba` to
   `HEAD`;
2. dispatch a fresh high-capability final reviewer;
3. require it to re-review the original Task 2 round-5 finding:
   `Redis event-marker lifecycle/readiness/repair is still not concrete`;
4. require zero Critical or Important findings;
5. append the clean verdict to both SDD ledgers;
6. only then resume Task 3 of
   `2026-07-26-production-worker-registration.md`.

No merge, push, deployment, migration, Redis mutation, schedule change, or
canary occurs in this implementation plan.
