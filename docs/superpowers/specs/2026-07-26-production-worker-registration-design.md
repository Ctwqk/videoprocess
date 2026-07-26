# Production Worker Registration Design

Status: pre-approved for implementation on 2026-07-26.

## Context

VideoProcess currently performs strong static startup checks for managed Python
workers and deploy-time storage probes for the GPU, vision, and YouTube
publisher services. The Go ffmpeg worker and Python workers still join Redis
consumer groups without a durable registration lease.

Static environment checks cannot revoke a running process, distinguish a stale
container from its replacement, or prove that a Redis consumer belongs to the
currently deployed service. This leaves T05 incomplete and blocks another real
canary and unattended soak.

The production topology is fixed:

- `vp-ffmpeg-worker-go-swarm` on `colima-127`;
- `vp-ffmpeg-worker-gpu-swarm` on `ccttww-lap`;
- `vp-vision-worker-swarm` on `ccttww-lap`;
- `vp-youtube-publisher-swarm` on `ccttww-lap`;
- host 126 is forbidden.

## Goal

Require every production Redis worker consumer to hold a short, revocable,
database-backed registration lease bound to its service, worker type, host,
capabilities, deployed image, and dependency fingerprints before it performs
its first Redis command.

## Approaches Considered

### Database-native registration with service-scoped secrets (selected)

Both the Go and Python workers already require PostgreSQL before processing a
task. Each worker reads a service-specific Docker secret, registers directly
in PostgreSQL, and renews a 180-second lease every 60 seconds. A registration
epoch fences a replaced process.

This adds no new always-on service and lets registration remain available
whenever the database required for durable task execution is available.

### Internal HTTP registration API

An API would centralize validation and SQL, but it adds an extra boot
dependency, an HTTP authentication surface, and a second health requirement
before workers can recover. It does not remove the need for service-scoped
credentials or durable lease storage.

### Controller-only registration

Having the deploy controller register a service without worker participation
cannot prove which process joined Redis and cannot support heartbeat or runtime
revocation. It is insufficient by itself.

## Identity And Credentials

The deploy controller issues one 256-bit random credential per managed worker
service and release generation. Credentials live only in:

- a mode `0600` transactional state file below
  `$DEPLOY_GITHUB_SYNC_ROOT/state/vp-worker-admission/` on host 150;
- a generation-scoped Docker secret mounted at
  `/run/secrets/vp-worker-admission-token`.

Credentials are never placed in service environment variables, command
arguments, logs, evidence, or database rows. PostgreSQL stores only SHA-256
hashes.

The deployment writes one active grant per service generation. A grant binds:

- exact Swarm service name;
- exact 40-character release commit and deploy image;
- exact versioned non-owner PostgreSQL login principal;
- worker type;
- expected host identity;
- required capabilities;
- exact Redis stream/group and canonical non-secret endpoint bindings;
- token hash;
- pending/active/revoked state and issuer timestamps.

The only production capability sets in this increment are:

- `vp-ffmpeg-worker-go-swarm`: `media_cpu`;
- `vp-ffmpeg-worker-gpu-swarm`: `media_gpu`;
- `vp-vision-worker-swarm`: `vision_gpu`;
- `vp-youtube-publisher-swarm`: `youtube_publisher`.

The runtime watcher independently compares the copied grant/image facts with
the actual Swarm service image. Rollback receives a fresh generation and token
bound to the prior image.

## Data Model

### `worker_admission_grants`

- `id`: UUID primary key.
- `service_name` and monotonically increasing `generation`.
- `worker_type`, `worker_host`: expected identity.
- `capabilities_json`: sorted unique capability strings.
- `release_commit`: exact 40-character Git commit.
- `image_identity`: exact deploy image tag.
- `database_principal`: exact PostgreSQL `session_user` expected for this
  service generation.
- `redis_stream` and `redis_group`.
- `endpoint_bindings_json`: canonical non-secret dependency identities.
- `token_sha256`: unique 64-character lowercase hash.
- `state`: `pending`, `active`, or `revoked`.
- `issued_at`, `issued_by`, `activated_at`.
- `revoked_at`, `revoke_reason`.
- `created_at`, `updated_at`.

Grant changes are controlled by the deployment/operator CLI and audited. Raw
tokens never enter PostgreSQL.

### `worker_registrations`

- `id`: UUID primary key.
- `grant_id` and copied service/type/host/capability facts.
- `database_principal`: PostgreSQL `session_user` observed by the registration
  function.
- `worker_instance_id`: process-unique UUID and fixed Swarm slot.
- `redis_consumer_id`: exact consumer name used by Redis.
- `image_identity`: exact `name:deploy-<12 hex>` image.
- `database_fingerprint`, `redis_fingerprint`, `storage_fingerprint`: SHA-256
  hashes of canonical non-secret dependency identities.
- `lease_epoch`: monotonically increasing per service.
- `lease_secret_sha256`: registration-specific heartbeat credential.
- `status`: `active`, `revoked`, or `expired`.
- `registered_at`, `heartbeat_at`, `lease_expires_at`.
- `revoked_at`, `revoke_reason`, `superseded_by`.

Only one active registration per service and slot is allowed. Registering a
replacement grant generation locks the service, revokes any prior active
registration, advances the epoch, and inserts the replacement atomically.

No endpoint URL, database password, Redis password, MinIO credential, token,
prompt, media metadata, or task payload is stored.

## Registration Contract

The shared semantic request contains:

- service name, worker type, host, instance UUID, consumer ID;
- database principal observed from PostgreSQL `session_user`;
- exact sorted capabilities;
- release commit, image identity, Redis stream/group, and endpoint bindings;
- three dependency fingerprints;
- requested lease duration fixed at 180 seconds.

Registration:

1. validates static worker admission;
2. reads a bounded token file with restrictive permissions;
3. opens PostgreSQL;
4. locks and validates the matching grant and token hash;
5. rejects a superuser, table owner, or principal with direct write privileges
   on the grant/registration tables;
6. validates exact database principal, service/type/host/capability/image/
   release/stream/group and endpoint claims;
7. revokes the old active service registration;
8. inserts a new epoch and returns its ID and expiry.

Any failure exits before Redis client construction, consumer-group creation,
PEL reclaim, `XREADGROUP`, or event publication.

Registration returns a separate random lease secret whose hash is stored.
Heartbeat runs every 60 seconds and extends the lease to 180 seconds only when
registration ID, service, instance ID, epoch, status, and lease secret still
match.
Failure, revocation, or expiry cancels the worker context. In-flight work is
cancelled and left pending without acknowledgement.

Graceful shutdown performs a bounded best-effort revoke. A failed shutdown
revoke cannot keep the process alive; the lease expires naturally.

## Lease Advisory-Lock Protocol

Lease fencing uses transaction-scoped PostgreSQL advisory locks, not row-lock
side effects. Keys are derived with `hashtextextended` from distinct
`vp-worker-service:` and `vp-worker-registration:` namespaces. A hash collision
may over-serialize unrelated workers but cannot weaken fencing.

The global acquisition order is:

1. service-scoped exclusive lock, when an operation needs one;
2. registration-scoped exclusive locks in ascending registration UUID order;
3. grant or registration row locks and mutations.

No code may acquire a service lock after a registration lock.
`vp_worker_register` takes the service-exclusive lock, then the prior active
registration-exclusive lock before takeover. `vp_worker_release` takes the
registration-exclusive lock before mutation. Grant activation/revocation and
operator registration revocation must follow the same order.

Routine grant and registration lifecycle changes use only the schema-qualified
`public.vp_worker_grant_upsert`, `public.vp_worker_grant_activate`,
`public.vp_worker_grant_revoke`, `public.vp_worker_registration_revoke`, and
`public.vp_worker_registration_expire` functions. Each is `SECURITY DEFINER`
with `search_path=pg_catalog`, has no `PUBLIC` execution, takes the
service-exclusive lock and every affected registration-exclusive lock before
row locks or mutation, and returns only sanitized identifiers or booleans.
The versioned deployment operator principal receives exact `EXECUTE` grants on
these functions and no direct grant/registration table privilege. Automation,
including replacement-generation rollback, principal retirement, and expiry
cleanup, must use this surface. Table-owner writes are a manual, audited
break-glass procedure only and are never available to deployment automation.

`vp_require_worker_lease` and `vp_worker_heartbeat` take a
registration-scoped shared transaction lock. Require then reads and validates
without `FOR UPDATE`; heartbeat remains compatible with a held require fence.
Takeover and release wait for all shared fences to end. Each function computes
database time only after all advisory-lock waits complete.

Task 2 holds the require function's shared transaction lock through the
irreversible YouTube POST and the durable `submitted` transition, including
the manager task ID. The existing `request_attempted_at` uncertain-operation
fence remains mandatory. Immediately before entering that transaction, the
worker obtains a fresh heartbeat and requires at least 150 seconds of lease
margin. The irreversible POST is bounded to 120 seconds and the durable
submission transition to 15 seconds. The normal 60-second heartbeat continues
under the compatible shared lock; heartbeat-loss cancellation aborts the
request context, preserves uncertain state, and performs no acknowledgement.

YouTube execution intentionally requires a durable registered-worker claim in
every environment, including local development. Generationless compatibility
remains available for non-publication worker paths only; an irreversible
external publication must not fall back to the legacy authority model.

### Task delivery and event authority handoff

Every registered production task has a mandatory `dispatch_key` backed by an
orchestrator-created `worker_task_dispatches` row. The initial `QUEUED` node
state, authoritative input artifact IDs, and dispatch row commit in one node
authority transaction. Downstream and retry dispatches follow the same rule.
Redis delivery is post-commit and idempotent: a Lua `GET`/`XADD`/`SET` marker
binds the dispatch key to one Redis message ID, and an independent database
poller retries pending dispatches even if the originating event is trimmed.
Markers use a renewable 30-day TTL and expire after the database row is
durably `delivered`.

On claim, the worker calls `public.vp_attest_worker_task_delivery` while its
worker-principal live lease and node authority transaction are held. The
immutable attestation binds dispatch key, canonical task payload hash,
stream/group/message, job/node, registration/epoch/worker/start, and source
task ACK state. The worker role has no direct attestation DML.

The independent orchestrator principal calls
`public.vp_observe_worker_task_delivery`, not
`public.vp_require_worker_lease`. The observer acquires the same
registration-shared advisory transaction lock and verifies the live
registration, admission grant, delivered dispatch, and exact attestation
without accepting the orchestrator as the worker principal. Under that fence
plus locked node authority, the orchestrator atomically accepts a
`registered_worker_event_receipts` handoff and its
`registered_worker_event_deliveries` row. Cache writes, final-artifact
mutation, downstream/retry dispatch staging, and job finalization occur only
after this handoff and in the same transaction.

Receipts deduplicate both event identity and source-attestation identity. A new
event ID with the same exact result aliases the applied receipt without
reapplying. A same-attestation payload or claim mismatch creates a durable
quarantined delivery with the actual event Redis identity and reason code; it
may ACK only that quarantined event and never the source task. Receipt-backed
source-task and event ACKs remain valid after lease loss. The attestation
`pending -> authorized -> acknowledged` lifecycle also closes the live-worker
XACK/commit window: live authority is committed before XACK, and an independent
database poller can replay an exact authorized XACK after worker failure.

Job deletion uses
`public.vp_resolve_worker_event_authority_for_job_deletion`. It requires every
dispatch delivered, every attestation acknowledged, every existing receipt
applied and event-acknowledged, and every event delivery acknowledged. It does
not require one receipt per attestation, so a safely ACKed cancelled task with
no completion/failure event can be cleaned up. Receipt and attestation foreign
keys are restrictive; the cleanup function deletes authority rows explicitly.

## Dependency Fingerprints

Fingerprint builders parse structured URLs and endpoints, remove credentials,
normalize scheme/host/port/database or bucket identity, then hash canonical
JSON. They reject malformed or local production endpoints using the existing
admission rules. Registration accepts only the exact nested schemas below,
rejects extra or secret-bearing fields, recomputes each canonical SHA-256 in
both Python and PostgreSQL, and compares it with the supplied fingerprint.
Database identity deliberately excludes the login principal and credentials,
so versioned runtime principals do not change the admitted database endpoint.

Endpoint ports and the Redis database index use semantic JSON-number parity,
not lexical integer spelling. PostgreSQL accepts exact `jsonb` numeric values;
Python accepts exact `int` values (but not booleans) and finite `Decimal`
values. Both paths require the mathematical value to be integral and within
the field's range, then normalize it to an integer before canonical JSON
serialization and SHA-256. Thus exact `1`, `1.0`, and `1e0` inputs have one
canonical identity. Python rejects every binary `float`, because its original
JSON precision cannot be recovered. Booleans, non-integral numbers,
NaN/infinity mapping values, invalid negatives, and out-of-range values are
rejected.

Any JSON trust boundary that feeds registration claims must preserve decimal
and exponent tokens with `json.loads(..., parse_float=Decimal)` and reject
non-standard numeric constants. Ordinary `json.loads` is not an acceptable
security boundary for these claims because it creates binary floats. Normal
production endpoint bindings are derived from environment configuration and
already use exact integer ports and database indexes.

The public lease service is PostgreSQL-only by default. A non-PostgreSQL ORM
path requires an explicit test-only opt-in plus an injected principal resolver;
it is used for deterministic unit tests and never claims production locking or
security parity. Its validation and stable error contract match PostgreSQL,
while live PostgreSQL 16 tests remain authoritative for concurrency, role, and
stored-function behavior.

- database: driver family, host, port, database name;
- Redis: scheme, host, port, database index;
- storage: backend, MinIO host/port, bucket;
- non-storage workers use an explicit `not_applicable` storage fact.

The Python and Go implementations share fixed cross-language fixtures so the
same environment produces identical hashes.

## Worker Lifecycle

### Durable execution binding

`node_executions` gains nullable `worker_registration_id` and
`worker_lease_epoch`. Production task claim stores the current registration
facts. Artifact/object writes and worker event publication require the exact
live lease under the existing node execution authority lock. Completion and
failure effects move to the immutable receipt handoff above; no
completion-derived write occurs before acceptance. Lease loss before either
the receipt handoff or a durable task-ACK authorization leaves the delivery
pending and cannot finalize a result.

Historical node rows remain nullable and are not backfilled with invented
registrations.

### Python workers

Worker identity becomes process-unique rather than PID-only. Startup order is:

1. static admission;
2. database configuration;
3. durable registration;
4. 60-second lease heartbeat task;
5. Redis client creation and group join;
6. PEL recovery and consume loop.

The heartbeat cancellation signal controls the consume loop and all spawned
message tasks. The YouTube submission fence and every durable completion path
also require the exact registration epoch.

### Go ffmpeg worker

The Go worker opens PostgreSQL and storage, creates its durable registration,
starts heartbeat, and only then constructs or uses the Redis client. The
registration context is the parent of the consumer context and the registration
epoch is part of every durable node authority check.

## Deployment

The migration must run before any new worker image is started. The application
deployment order becomes:

1. API/frontend;
2. backend migration service;
3. verify migration head;
4. create a versioned non-owner PostgreSQL login principal for each worker
   service and mount its database URL as a Docker secret;
5. issue pending generation-scoped grants and admission Docker secrets through
   the restricted operator function surface;
6. activate exact grants through the new Python operator CLI;
7. update and verify the managed workers;
8. revoke the replaced login principals;
9. continue runner and remaining service convergence.

A stable `NOLOGIN` worker runtime role owns this explicit privilege allowlist:

- `SELECT` on `jobs`, `node_executions`, `artifacts`, `channel_profiles`,
  `production_tasks`, `runtime_schedules`, and
  `youtube_upload_operations`;
- `INSERT` on `artifacts`, `runtime_schedules`, and
  `youtube_upload_operations`;
- `UPDATE` on `node_executions` and `youtube_upload_operations`;
- `EXECUTE` only on `vp_worker_register`, `vp_worker_heartbeat`,
  `vp_worker_release`, `vp_require_worker_lease`,
  `vp_require_worker_lease_margin`, `vp_attest_worker_task_delivery`,
  `vp_authorize_worker_task_ack`, `vp_require_worker_task_ack_receipt`, and
  `vp_acknowledge_worker_task_delivery`.

It receives no `DELETE`, `TRUNCATE`, DDL, sequence, `alembic_version`, grant-
table, or registration-table privilege. Each service generation receives a
fresh `LOGIN` role that inherits only that stable role. It is never a superuser,
never owns application objects, has no privileged role membership, and has no
direct grant/registration table reads or writes. Deployment does not use blanket
table grants or default privileges.

A separate stable `NOLOGIN` orchestrator-control role executes only
`vp_observe_worker_lease`, `vp_observe_worker_task_delivery`, and
`vp_resolve_worker_event_authority_for_job_deletion`. It has exact
`SELECT`/`INSERT` and monotonic state-column `UPDATE` privileges on
`worker_task_dispatches`, `worker_task_delivery_attestations`,
`registered_worker_event_receipts`, and
`registered_worker_event_deliveries`, plus the existing least-privilege
job/node/cache/artifact columns needed by receipt application. It receives no
`DELETE`, protected-table ownership, worker registration functions, or direct
grant/registration access. Event authority cleanup is available only through
the reviewed cleanup function.

Schema migration/DCL uses the protected deploy-migrator credential. Migration
head and readiness probes use a separate deploy-read credential. Neither
credential is mounted into a worker.

A separate versioned `LOGIN` deployment operator principal owns no objects and
has no direct grant/registration table access. It can execute only the five
worker operator functions required for grant and registration lifecycle
mutations. Replacement runtime-role revocation occurs only after the operator
surface has fenced and revoked the replaced grant/registration; role DCL never
substitutes for that durable mutation.

Each worker service receives:

- `WORKER_SERVICE_NAME`;
- `WORKER_RELEASE_COMMIT`;
- `WORKER_IMAGE_IDENTITY`;
- `WORKER_CAPABILITIES`;
- `WORKER_REDIS_STREAM`;
- `WORKER_REDIS_GROUP`;
- `WORKER_DATABASE_URL_FILE`;
- `WORKER_ADMISSION_TOKEN_FILE`;
- a versioned service/generation database URL secret;
- the service-scoped Docker secret.

Production startup rejects a worker database URL supplied only through the
Swarm environment. The worker reads the bounded mode `0400` secret before
opening PostgreSQL. It reads exactly the initial descriptor size, rejects
premature EOF or growth, and compares final descriptor identity, mode, size,
mtime, and ctime before accepting the value. Docker secret mounts for the
database URL and admission token must remain immutable for the full worker
process lifetime; rotation creates a new generation and restarts the worker
instead of modifying a mounted secret in place. Local development may continue
using `DATABASE_URL`.

The controller verifies the running service has exactly the expected secret,
identity variables, registration row, consumer ID, image, host, capabilities,
and unexpired lease. A registration readiness failure enters the existing
deployment rollback path.

Credential state is created only on host 150. Host 126 is never used for state,
secret creation, grant writes, worker placement, readiness, or rollback.

## Security Boundary And Follow-Up

The stored functions are `SECURITY DEFINER`, set a fixed `search_path`, use
`session_user` for principal checks, and revoke execution from `PUBLIC`.
Production deployment grants execution only to the stable worker runtime role
and keeps direct registration-table writes unavailable to it. If the versioned
principal cannot satisfy that separation during this rollout, the preflight reports
`worker_database_role_not_isolated` and this increment is not considered the
complete T05 security boundary.

This increment authenticates workers before Redis and continuously joins
consumer observations to registrations. Task 4 gives each worker only its
granted task stream/group commands and gives the orchestrator-control Redis
principal the event-group commands plus `EVAL`, `GET`, `SET`, and `XADD` for
`vp:worker-task-dispatch:*` markers and admitted task streams. Lua execution is
restricted to the reviewed idempotent dispatch script. Marker TTL is renewed
while delivery reconciliation is pending and is 30 days after the last
attempt; the durable `delivered` row prevents redispatch after expiry. No broad
key pattern or unrestricted scripting grant is permitted.

## Unknown Consumer Guard

The repository-owned watcher runs every minute. A registration guard
queries each required Redis stream/group and compares every consumer name with
an unexpired active registration and the actual expected service facts supplied
by the Swarm watcher.

Any unknown, expired, revoked, mismatched, or duplicate consumer is an
immediate hard failure. In one database transaction, the guard:

1. closes the global runtime schedule;
2. halts every enabled non-dry-run channel;
3. holds non-terminal tasks;
4. cancels linked non-terminal jobs and nodes;
5. dead-letters runnable ChannelOps queue rows;
6. emits only stable reason codes and affected IDs.

The operation is idempotent and only reduces activity. It never resumes a
channel, opens a schedule, acknowledges Redis work, retries an upload, or
changes publication privacy.

An absent worker with no Redis consumer is reported by existing service health
checks and does not by itself trigger this global database mutation. A process
that loses its lease exits; its remaining Redis consumer record becomes
unknown and is caught within one watcher interval. The five-minute requirement
therefore has four minutes of detection margin.

## Failure Handling

- missing/unsafe token file: worker exits before Redis;
- missing, disabled, revoked, or mismatched grant: worker exits before Redis;
- database outage at startup: worker exits before Redis;
- heartbeat database outage: worker cancels and exits without durable write or
  ack;
- replacement registration: stale epoch heartbeat fails;
- deployment grant/secret failure: service update is not attempted;
- readiness mismatch: existing deployment rollback runs;
- unknown consumer: global fail-closed hold;
- watcher database/Redis uncertainty: nonzero result, never reports healthy;
- cleanup failure: lease expiry remains the final fence.

## Testing

- PostgreSQL 16 migration, constraints, active-service uniqueness, stored
  registration functions, and concurrent takeover;
- token hashing and no-secret persistence/logging;
- versioned non-owner database principals, secret-file-only production startup,
  principal/grant binding, and direct registration-table denial;
- exact claim validation and endpoint fingerprint fixtures in Python and Go;
- Python startup ordering, heartbeat cancellation, revoke, and no Redis calls
  on denial;
- vision storage admission and fingerprint requirements;
- node claim/final-write/upload/event/XACK fencing against a lost registration
  epoch;
- Go startup ordering, heartbeat cancellation, epoch takeover, and no Redis
  calls on denial;
- in-flight work is not acknowledged after lease loss;
- deploy credential/secret/grant lifecycle, migration order, readiness,
  rollback, and 126 exclusion;
- watcher unknown/expired/mismatched/duplicate consumer auto-hold and healthy
  no-write behavior;
- one-minute managed cron and five-minute detection-SLO contract;
- full backend, Go, race, frontend, deployment, canary preflight, and soak
  guard checks.

## Rollout

The deployment first migrates and issues pending grants while old workers
continue under the existing safety controls. It then activates and updates one
worker service at a time and requires a healthy registration before proceeding.
Rollback issues a fresh generation and secret bound to the prior image; it
never reactivates an old token. It also creates a fresh database login principal
instead of reusing a prior generation's credential.

Before another canary, a read-only production audit must show:

- exactly four expected active registrations;
- all leases fresh;
- every Redis consumer mapped to one registration;
- actual service image/host/capabilities match;
- no unknown consumers, pending work, or tasks on host 126.

No canary, upload, schedule opening, channel resume, soak activation, or public
publication is part of this rollout.
