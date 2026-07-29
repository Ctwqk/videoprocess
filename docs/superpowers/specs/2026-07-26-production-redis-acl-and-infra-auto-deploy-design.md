# Production Redis ACL And Infrastructure Auto-Deploy Design

Status: pre-approved for implementation on 2026-07-26.

## Context

VideoProcess production Redis is the `constructure_vp_redis` container on host
150. It is owned by the independent private repository
`Ctwqk/constructure-runtime`, specifically
`infra/shared-infra/docker-compose.yml`. The service binds host port `6380`,
uses Redis 7, and currently starts with no authentication or ACL file.

The live server reports only the `default` ACL user, and managed VP services
connect as that user. The same Redis server also carries PDS database 1 and
feature-aggregator database 2.

The runtime repository's daily sync cron is outbound-only: it commits and
pushes server-local changes. A GitHub push does not pull or deploy on host 150.
Therefore Redis ACL work first requires a CI-gated inbound deployment path for
that repository.

## Goal

Make GitHub `main` the source of truth for shared-infra configuration, deploy a
successful runtime commit automatically on host 150, and require every Redis
client to use a least-privilege named ACL user before disabling the unauthenticated
default user.

This is the remaining Redis ACL/namespace portion of T05. It does not authorize
a canary, upload, schedule opening, channel resume, soak activation, or public
publication.

## Repository Ownership

- `constructure-runtime`: Redis server configuration, ACL template, safe
  pull/deploy controller, CI, health/rollback, and generated credential paths.
- `videoprocess`: VP control-plane, four worker, and watcher Redis secret
  loading; exact key/command contracts; deployment readiness.
- `policy-decision-service`: PDS Redis credential loading and DB 1 key contract.
- VP feature aggregator: DB 2 credential loading and key contract.

Host 126 is excluded from source state, credential generation, deployment,
runtime placement, health checks, and rollback.

## Inbound Runtime Deployment

`constructure-runtime` owns a host-150 poller installed as one marked cron
entry. Every 15 minutes it:

1. acquires a nonblocking lock;
2. requires the checkout and protected credential state to be clean/separate;
3. fetches `origin/main` without merging;
4. queries GitHub Actions for the exact 40-character commit and requires the
   latest `ci.yml` run to be completed/success;
5. stages the commit in a separate directory;
6. validates Compose and ACL templates without exposing secrets;
7. applies only allowlisted shared-infra services;
8. runs bounded health and authenticated identity checks;
9. records the deployed commit atomically;
10. restores the prior staged configuration on failure.

GitHub Actions receives no production SSH credential. The 150 host remains the
pull-based deployment authority.

The old outbound sync must not auto-commit or auto-push the runtime repository
itself. Manual server drift is reported and blocks deployment instead of
becoming a new GitHub commit.

## Credential Model

The deploy controller generates independent random credentials for:

- VP control plane;
- Go ffmpeg worker;
- Python GPU ffmpeg worker;
- vision worker;
- YouTube publisher;
- worker-registration watcher;
- PDS;
- feature aggregator.

Raw credentials live only in mode `0600` host state and versioned Docker
secrets. They never enter Git, service environment variables, command
arguments, logs, evidence, or application database rows.

The generated ACL file contains Redis password hashes, not raw passwords. It is
mounted read-only into the Redis container and persisted outside Git. Client
URLs are mounted as mode `0400` secrets and loaded before Redis client
construction.

## ACL Contract

Every user starts with `reset`, `off`, `sanitize-payload`, no key/channel
patterns, and no commands. Deployment then adds only the reviewed contract.

### VP workers

Each worker may access only its exact task stream and the shared event stream.
The allowed command set covers connection health, group join, consume,
pending/reclaim, task affinity bounce, event append, and acknowledgement:

- `PING`;
- `XGROUP CREATE`;
- `XREADGROUP`;
- `XPENDING`;
- `XAUTOCLAIM`;
- `XCLAIM`;
- `XADD`;
- `XACK`.

Key patterns are the exact `vp:tasks:<worker_type>` stream and `vp:events`.
Workers cannot inspect or mutate another worker's task stream.

### VP control plane

The API/orchestrator/event listener/runner identity receives only the reviewed
VP queue, event, lock, and scheduling key patterns and the commands proven by
source-level inventory tests. Broad `+@all` and `~*` are forbidden.

### Watcher

The watcher receives `PING`, `XINFO`, and `XPENDING` for the four exact task
streams. It cannot add, read payloads, claim, acknowledge, or delete entries.

### PDS and feature aggregator

Each repository defines canonical key prefixes and an exact command inventory.
Database numbers remain compatibility routing, not a security boundary: Redis
ACL key patterns are the enforced boundary. `SELECT` is allowed only where a
client library needs it, and tests prove that cross-prefix access is denied.

## Worker Marker Continuity Runtime

Worker marker continuity is an executable prerequisite to the client cutover,
not a prose-only audit. Migration `034_worker_registrations` provides
`vp_list_worker_redis_marker_expectations`,
`vp_begin_worker_redis_continuity_check`,
`vp_finish_worker_redis_continuity_check`,
`vp_record_worker_redis_marker_observation`,
`vp_claim_worker_redis_marker_cleanup`,
`vp_finish_worker_redis_marker_cleanup`,
`vp_load_worker_redis_marker_repair`,
`vp_promote_observed_worker_event_emission`, and
`vp_require_worker_redis_continuity`. They revoke public execution and bind
the stable NOLOGIN roles `vp_marker_readiness_runtime`,
`vp_marker_janitor_runtime`, and `vp_marker_repair_runtime`.

VideoProcess provisions and revokes generation-scoped LOGIN roles only through:

```text
python -m app.services.worker_marker_control_role_cli provision --generation <generation> --state-dir /control-state
python -m app.services.worker_marker_control_role_cli revoke --generation <generation> --state-dir /control-state
```

The reviewed Python worker/control image supplies:

```text
python -m app.channel_agent.worker_redis_marker_readiness_cli check
python -m app.channel_agent.worker_redis_marker_janitor_cli run
python -m app.services.worker_redis_marker_repair_cli audit
python -m app.services.worker_redis_marker_repair_cli restore-marker --source-id <uuid> --apply
python -m app.services.worker_redis_marker_repair_cli promote-prepared --emission-id <uuid> --apply
```

The runtime repository creates three independent ACL users:

- `vp-marker-readiness` receives `PING`, `ACL WHOAMI`, read-only `INFO server`,
  `INFO persistence`, `CONFIG GET maxmemory-policy`, `GET`, and `XRANGE` only
  for `vp:worker-event-emission:*`, `vp:worker-task-dispatch:*`, `vp:events`,
  and reviewed `vp:tasks:<worker_type>` streams;
- `vp-marker-janitor` receives `EVAL`, `GET`, and `DEL` only for the two marker
  prefixes;
- `vp-marker-repair` receives `EVAL`, `GET`, `SET`, and `XRANGE` only for the
  marker prefixes and reviewed streams.

All three explicitly deny `XADD`, `XACK`, expiration, trimming, `FLUSH*`,
`SCRIPT LOAD`, `+@all`, `~*`, task consumption, and cross-namespace keys.

Constructure-runtime publishes a mode-`0400` non-secret state file containing
an exact 40-character `GENERATION`, `ACL_IDENTITY=vp-marker-acl-v1`,
`AOF_ENABLED=yes`, `AOF_STATUS=ok`,
`MAXMEMORY_POLICY=noeviction`, `NETWORK=vp-pipeline-net`, and distinct existing
readiness, janitor, and repair Redis Swarm secret names.
`VP_WORKER_REDIS_RUNTIME_GENERATION` must match exactly. Constructure-runtime
alone owns generation of the Redis ACL secrets, user definitions, AOF, and
eviction settings. VideoProcess refuses registered traffic when any field or
secret is absent; it has no fallback generator.

On host 150, `deploy/swarm/worker-redis-marker-control.sh` exposes only
`readiness`, `janitor`, and `status`. It uses fixed services
`vp-worker-redis-marker-readiness-job` and
`vp-worker-redis-marker-janitor-job`, `replicated-job` mode, one replica, restart condition `none`,
`vp-pipeline-net`, and `node.hostname==ccttww-lap`. Nonblocking mode locks
are kernel-released after process death. Jobs use the exact configured image
without registry resolution. Destructive fixed-name removal requires exact
labels, mode, generation, image, one-completion shape, restart policy, network,
placement, mode-specific secrets, environment, command, and exactly one
terminal task. Cron contains one marked block:

```text
* * * * * .../worker-redis-marker-control.sh readiness
*/5 * * * * .../worker-redis-marker-control.sh janitor
```

Repair is not scheduled. Readiness and janitor each receive only their own
database and Redis secret mounts at mode `0400`. The repair database secret is
versioned for explicit operator execution and mounted into neither job.
Database secrets are streamed to Swarm through stdin and never appear in argv,
environment, cron, status, or logs.

The VP deploy transaction provisions database roles first, installs the
launcher and marked cron without changing the independent VP/PDS/feature or
schedule/channel entries, and runs one readiness job. It parses the complete
status file once with an exact field allowlist, then requires fresh matching
`status` immediately before each ffmpeg, vision, and publisher mutation and
before matching rollback snapshot mutations. Registered Python startup then
opens PostgreSQL, registers, calls
`public.vp_require_worker_redis_continuity(90)`, constructs Redis, proves
`ACL WHOAMI`, and only then creates its group. Missing, stale, error, and
overlapping/running continuity all release registration, construct no Redis
client, and expose only `worker_redis_continuity_unready`.

A terminal never-ready candidate is deactivated by restoring the prior managed
launcher/config/cron state, its exact jobs are removed and proved absent, and
only then are its roles, database secrets, and credential files removed. With
no prior generation this restores prior absence. A nonterminal task blocks
revocation. Rollback creates a new prior-image role generation and new
passwords. If it is unready, candidate managed state is restored before the
failed rollback generation is cleaned in the same order. A ready rollback is
proven before failed and superseded roles and secrets are revoked. The Go image
remains Go-only; its registration/continuity work stays in the subsequent
worker-registration Task 3.

### Conservative Repair Matrix

| Evidence | Action |
| --- | --- |
| exact marker, exact stream entry, canonical payload hash | readiness records observation |
| exact cleanup authorization and marker value | janitor compare-deletes |
| cleanup marker absent | janitor records `absent` |
| absent marker, exact stored message ID, exact `XRANGE` hash | explicit operator may `restore-marker --apply` with `SET NX` |
| prepared event, exact marker/stream/hash | explicit operator may `promote-prepared --apply` in PostgreSQL |
| marker conflict, missing stream entry, payload mismatch, wrong ACL identity, loading/AOF/eviction failure | hold; no mutation |

There is no automatic repair, `XADD`, stream synthesis, acknowledgement,
schedule/channel change, upload retry, or publication action. The contract is
enforced by `backend/tests/worker/test_worker_startup.py`,
`tests/test_worker_redis_marker_control.sh`, and
`tests/test_vp_deploy_sync_extension.sh`.

## Two-Phase Cutover

### Phase A: provision without enforcement

1. Close the VP runtime schedule and require zero active jobs/nodes and zero
   Redis pending entries.
2. Deploy an ACL file containing all named users while keeping `default on
   nopass` temporarily.
3. Restart Redis once and require persistence, PING, and data-integrity checks.
4. Mount named-user secrets into VP, PDS, aggregator, and watcher clients.
5. Verify `ACL WHOAMI`, exact key denial, service health, and reconnect behavior
   for every client.

No phase-A success claim is allowed while any production client still reports
`default`.

### Phase B: enforce

1. Re-run the closed/idle/pending-zero gate.
2. Change the ACL file to `user default off`.
3. Restart Redis and require every named identity and dependency health check.
4. Confirm unauthenticated PING and cross-user key access fail.
5. Record an immutable sanitized evidence bundle.

Schedule and channels remain closed. Resuming operation is a separate explicit
decision after worker registration and ACL preflights both pass.

## Failure And Rollback

- CI unknown/failing: do not pull or deploy.
- Dirty runtime checkout: fail closed and report; never auto-commit.
- ACL render or Compose validation failure: do not restart Redis.
- Phase-A client failure: keep default temporarily available, restore the prior
  client service generation, and keep VP held.
- Phase-B failure: restore the prior ACL file and container configuration,
  verify all dependencies, and keep VP held.
- Unknown authenticated user or default-user reappearance after enforcement:
  global VP auto-hold and nonzero infrastructure health.
- Credential rotation uses new versioned secrets and a staged overlap; old
  users/secrets are revoked only after all clients prove the new identity.

Rollback restores availability but never opens the schedule, resumes a channel,
acknowledges pending work, or retries an upload.

## Verification

- runtime repository CI for shell syntax, Compose rendering, ACL template
  rendering, secret redaction, exact GitHub Actions gate, dirty-tree refusal,
  idempotency, rollback, and host-126 exclusion;
- Redis integration tests proving every allowed command/key and every denied
  cross-stream/cross-prefix operation;
- VP/PDS/aggregator tests proving secret-file-before-client startup and no
  default-user fallback in production;
- production phase-A and phase-B read-only audits recording commit, ACL user
  names, `ACL WHOAMI` per service, zero pending work, health, and default-user
  state without password hashes or raw credentials;
- restart test proving ACL persistence and client reconnection.

## Completion Boundary

T05 Redis hardening is complete only when:

- runtime pushes are CI-gated and automatically deployed from GitHub;
- every production Redis client uses an expected named user;
- each worker is restricted to its stream/event key contract;
- PDS and aggregator are restricted to their canonical prefixes;
- watcher is read-only;
- `default` is off and unauthenticated access fails;
- restart/rollback tests and production evidence pass;
- worker durable registration and unknown-consumer auto-hold are also deployed.
