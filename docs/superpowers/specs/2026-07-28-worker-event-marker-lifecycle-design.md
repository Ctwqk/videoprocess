# Worker Event Marker Lifecycle Design

Status: pre-approved for implementation on 2026-07-28.

## Context

Registered workers publish completion and failure events with a two-key Redis
Lua operation. The operation returns the existing
`vp:worker-event-emission:<emission_uuid>` marker when present; otherwise it
performs one `XADD` to `vp:events` and stores the returned message ID in the
marker. The marker has no TTL because it is the only durable Redis-side proof
that `XADD` may already have succeeded when the database transaction did not.

PostgreSQL stores the canonical event payload, payload hash, exact task
attestation, dispatch, registration, lease epoch, and Redis message ID. That
closes worker replay while the source authority exists. The remaining
load-bearing gap is lifecycle control after events and dispatches are fully
resolved:

- Redis continuity is not checked by an executable fail-closed readiness
  service.
- Database cleanup deletes the source rows needed to prove that a marker can
  be removed.
- No exact marker janitor principal or command exists.
- No conservative operator command can repair a missing marker without risking
  a duplicate `XADD`.

Task 3 and production registration deployment remain blocked until this
increment is implemented and independently reviewed.

## Goals

- Make Redis persistence and marker continuity an explicit, database-recorded
  readiness gate before any registered worker constructs a Redis client.
- Preserve an immutable cleanup authorization before source authority rows are
  deleted.
- Delete only an exact marker whose immutable database proof says deletion is
  safe.
- Provide operator audit and repair commands that never synthesize a stream
  event and never guess whether an `XADD` occurred.
- Give readiness, janitor, and repair separate non-worker PostgreSQL and Redis
  principals with minimum privileges.
- Install exact recurring host-150 jobs while excluding host 126.

## Non-Goals

- This increment does not deploy migration 034, enable registered production
  traffic, open a schedule, resume a channel, run a canary, upload, or publish.
- It does not add TTLs, Redis eviction, stream trimming, arbitrary marker
  deletion, or a force-resend command.
- It does not give worker principals marker deletion, `CONFIG`, `INFO`,
  `SCAN`, administrative, or cross-worker stream access.
- It does not replace the independent Redis ACL rollout owned by
  `constructure-runtime`; it defines and tests the exact client-side contract
  that rollout must provision.

## Chosen Architecture

### Immutable Cleanup Authorization

Migration 034 adds `worker_redis_marker_cleanup_authorizations`. A row contains:

- `marker_kind`: `event_emission` or `task_dispatch`;
- immutable source UUID, exact marker key, Redis stream, message ID, and
  payload SHA-256;
- `authorization_state`: `pending`, `claimed`, `deleted`, `absent`, or
  `conflict`;
- authorization, claim, completion, and sanitized error timestamps/codes.

The row has no foreign key to a job, dispatch, emission, attestation, or
registration because it must survive their guarded deletion. Immutable proof
columns cannot change after insertion.

`vp_resolve_worker_event_authority_for_job_deletion` inserts cleanup
authorizations in the same transaction, after it has locked all authority rows
and proved:

- the event emission is `resolved`;
- the source attestation is `acknowledged`;
- every event receipt is applied and acknowledged;
- every event delivery is acknowledged; and
- the source dispatch is acknowledged, or was never delivered and is safely
  cancelled.

Only after those rows exist may the function delete the source authority.
Rollback removes both the authorizations and the attempted cleanup.

### Exact Database Surfaces

All functions are fixed-search-path `SECURITY DEFINER`, revoke `PUBLIC`, use
`session_user`, and reject owners, superusers, and unexpected principals.

- `vp_list_worker_redis_marker_expectations(text, integer)` returns current
  event/dispatch marker expectations plus cleanup authorizations in stable
  order. It includes exact marker key, expected message ID, stream, payload
  hash, source state, and whether absence is allowed.
- `vp_begin_worker_redis_continuity_check(uuid, integer)` and
  `vp_finish_worker_redis_continuity_check(uuid, text, text, text, bigint,
  bigint)` serialize checks and record a singleton result visible to both
  worker hosts.
- `vp_require_worker_redis_continuity(integer)` returns only `ready`
  for the latest successful check no older than 90 seconds.
- `vp_claim_worker_redis_marker_cleanup(uuid, integer, integer)` leases
  only `pending` authorizations for 300 seconds.
- `vp_finish_worker_redis_marker_cleanup(uuid, uuid, text, text)` accepts only
  the exact claimed row and records `deleted`, `absent`, or `conflict`.
- `vp_load_worker_redis_marker_repair(text, uuid)` returns exact repair
  evidence without direct table access.
- `vp_promote_observed_worker_event_emission(uuid, text, text)` changes
  `prepared` to `emitted` only for the exact stored payload hash and observed
  Redis message ID under the operator-control principal.

Workers receive only `EXECUTE` on
`vp_require_worker_redis_continuity(90)`. Readiness, janitor, and repair
principals receive only their named functions and no direct table access.

`python -m app.services.worker_marker_control_role_cli provision|revoke`
creates three stable NOLOGIN roles plus independent generation-scoped LOGIN
roles. It reads the owner database URL only from a bounded mode-0400 file,
writes three independent generation database URL files atomically with mode
`0400`, and never prints credentials. Deployment creates Swarm secrets from
those files through stdin. Rollback provisions and proves a fresh prior-image
generation before revoking the failed roles and secrets.

### Continuity Readiness

`python -m app.channel_agent.worker_redis_marker_readiness_cli check` reads
mode-0400 PostgreSQL and Redis URL files. It:

1. acquires the database continuity run;
2. verifies `ACL WHOAMI` is the expected readiness user;
3. verifies Redis is available, not loading, AOF is enabled and healthy, and
   `maxmemory-policy` is `noeviction`;
4. pages through every database marker expectation;
5. checks the exact Redis marker value;
6. verifies the exact `XRANGE <message_id> <message_id>` payload hash whenever
   a stream message is required as repair evidence;
7. records `ready` only when every active expectation is consistent and no
   cleanup authorization is in `conflict`.

An absent marker is acceptable only for:

- a `prepared` event with no database message ID and no observed stream
  message; or
- a cleanup authorization already recorded as `absent` or `deleted`.

An absent active emitted marker, mismatched value, missing exact stream entry,
payload mismatch, Redis restart without healthy AOF, stale run, or incomplete
page is unready. The CLI emits stable reason codes without marker values,
payloads, Redis URLs, or credentials.

The latest result is stored in PostgreSQL. Registered Python workers call
`vp_require_worker_redis_continuity(90)` after registration succeeds and
before the first Redis client construction. The Go worker will consume the
same function in Task 3. The minute watcher also treats stale/unready status as
an automatic global hold.

### Marker Janitor

`python -m app.channel_agent.worker_redis_marker_janitor_cli run` uses separate
mode-0400 database and Redis URL files. It:

1. claims up to 100 cleanup authorizations;
2. executes an atomic compare-and-delete Lua operation for each exact marker;
3. records `deleted` when the value matched and was deleted;
4. records `absent` when the marker was already absent;
5. records `conflict`, deletes nothing, and makes readiness fail when the value
   differed.

The janitor never scans arbitrary keys and never derives marker keys from
Redis. It receives exact keys and expected values from PostgreSQL. It has no
`XADD`, `XACK`, `SET`, expiry, stream mutation, or administrative permission.

### Operator Repair

`python -m app.services.worker_redis_marker_repair_cli` provides:

- `audit`: read-only, lists stable source IDs and reason codes;
- `restore-marker --source-id <uuid> --apply`: restores an absent marker only
  when PostgreSQL has an exact message ID and `XRANGE` returns that exact entry
  with the stored canonical payload hash;
- `promote-prepared --emission-id <uuid> --apply`: marks a prepared emission
  emitted only when the marker points to an exact stream entry whose payload
  hash matches PostgreSQL.

Repair uses an atomic `SET ... NX` script and fails on any concurrent value.
It never calls `XADD`, never changes a marker value, never repairs a missing
stream entry, and never replays under a replacement registration. A prepared
event with no marker after lease loss remains held for human investigation.

Every applied repair records an append-only sanitized audit row with source ID,
action, result code, operator principal, and timestamp. It stores no raw
payload or credential.

## Redis Principals

The independent runtime ACL rollout provisions:

- `vp-marker-readiness`: `PING`, `ACL WHOAMI`, read-only `INFO`, exact
  `CONFIG GET`, `GET`, and `XRANGE` on marker prefixes and reviewed streams;
- `vp-marker-janitor`: `EVAL`, `GET`, and `DEL` only on
  `vp:worker-event-emission:*` and `vp:worker-task-dispatch:*`;
- `vp-marker-repair`: `EVAL`, `GET`, `SET`, and `XRANGE` only on exact marker
  prefixes and reviewed streams.

All three deny `XADD`, `XACK`, expiry, `FLUSH*`, `SCRIPT LOAD`, `~*`, `+@all`,
task consumption, and cross-namespace keys. Raw credentials exist only in
mode-0600 host-150 state and mode-0400 service secret files.

## Scheduling And Deployment

`deploy/swarm/deploy-sync-extension.sh` installs two marked, idempotent
host-150 cron entries:

- every minute: one fixed-name readiness replicated job;
- every five minutes: one fixed-name marker-janitor replicated job.

Each launcher uses a nonblocking host lock, skips when the prior job is
running, removes only its own completed job, uses the exact reviewed image and
network, mounts only its dedicated secrets, writes a bounded sanitized log,
and constrains placement to `node.hostname==ccttww-lap`. Host 126 is rejected
by tests and runtime preflight. Repair remains an explicit operator command and
is never scheduled.

Registered worker deployment remains drained until:

- migration and exact function grants are installed;
- Redis AOF/noeviction and all three ACL users are active;
- one continuity check is `ready`;
- janitor backlog has no conflict; and
- worker startup and watcher checks pass.

## Failure Rules

- Redis unavailable/loading/AOF-unhealthy/wrong eviction: record unready,
  start no registered Redis client, and hold runtime activity.
- Missing or mismatched active marker: record unready; do not replay or repair
  automatically.
- Janitor mismatch: delete nothing, record conflict, alert, and hold.
- Stale readiness or janitor run: fail closed; a new run may take over only
  after its exact database lease expires.
- Repair cannot prove exact stream identity and payload: make no mutation.
- Deployment rollback restores the prior image and credentials but never opens
  a schedule, resumes a channel, acknowledges work, or retries an upload.

## Verification

- PostgreSQL 16 migration upgrade/downgrade, immutability, source cleanup race,
  separate principals, run leases, exact cleanup claims, repair authorization,
  and direct-table denial.
- Redis 7.4 integration with AOF and `noeviction`, restart continuity,
  active-marker mismatch, exact compare/delete, exact marker restore,
  prepared-event promotion, concurrent janitors, and cross-user denial.
- Worker startup proves continuity is checked after registration and before
  the first Redis client construction.
- Deployment tests prove exact cron blocks, job replacement, secret mounts,
  host-150 placement, host-126 rejection, dry-run idempotency, and sanitized
  logs.
- Full backend, Go, deployment contract, Ruff, Mypy, and diff checks.

## Completion Boundary

This prerequisite is complete only when:

- all functions, CLIs, principals, schedules, and tests above exist;
- a fresh independent review finds no Critical or Important issue;
- the original Task 2 breaker is re-reviewed against the implementation; and
- no production state has been changed.

Production rollout, Task 3 Go registration, Task 4 grants/ACL deployment, T12,
and any canary remain subsequent gated work.
