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

