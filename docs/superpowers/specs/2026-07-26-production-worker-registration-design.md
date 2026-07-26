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
in PostgreSQL, and renews a 45-second lease every 15 seconds. A registration
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

The deploy controller owns one persistent 256-bit random credential per
managed worker service. Credentials live only in:

- a mode `0600` state file below
  `$DEPLOY_GITHUB_SYNC_ROOT/state/vp-worker-admission/` on host 150;
- a service-scoped Docker secret mounted at
  `/run/secrets/vp-worker-admission-token`.

Credentials are never placed in service environment variables, command
arguments, logs, evidence, or database rows. PostgreSQL stores only SHA-256
hashes.

The deployment writes one active grant per service. A grant binds:

- exact Swarm service name;
- worker type;
- expected host identity;
- required capabilities;
- token hash;
- enabled/revoked state.

The only production capability sets in this increment are:

- `vp-ffmpeg-worker-go-swarm`: `media_cpu`;
- `vp-ffmpeg-worker-gpu-swarm`: `media_gpu`;
- `vp-vision-worker-swarm`: `vision_gpu`;
- `vp-youtube-publisher-swarm`: `youtube_publisher`.

The image is recorded by each registration from an exact deploy-injected image
identity. The runtime watcher independently compares it with the actual Swarm
service image. This preserves rollback while still detecting a false image
claim.

## Data Model

### `worker_admission_grants`

- `id`: UUID primary key.
- `service_name`: unique managed Swarm service.
- `worker_type`, `worker_host`: expected identity.
- `capabilities_json`: sorted unique capability strings.
- `token_sha256`: unique 64-character lowercase hash.
- `enabled`: admission switch.
- `revoked_at`, `revoke_reason`.
- `created_at`, `updated_at`.

Grant changes are controlled by the deployment/operator CLI and audited. Raw
tokens never enter PostgreSQL.

### `worker_registrations`

- `id`: UUID primary key.
- `grant_id` and copied service/type/host/capability facts.
- `worker_instance_id`: process-unique UUID.
- `redis_consumer_id`: exact consumer name used by Redis.
- `image_identity`: exact `name:deploy-<12 hex>` image.
- `database_fingerprint`, `redis_fingerprint`, `storage_fingerprint`: SHA-256
  hashes of canonical non-secret dependency identities.
- `lease_epoch`: monotonically increasing per service.
- `status`: `active`, `revoked`, or `expired`.
- `registered_at`, `heartbeat_at`, `lease_expires_at`.
- `revoked_at`, `revoke_reason`.

Only one active registration per service is allowed. Registering a replacement
locks the grant, revokes any prior active registration, advances the epoch, and
inserts the replacement atomically.

No endpoint URL, database password, Redis password, MinIO credential, token,
prompt, media metadata, or task payload is stored.

## Registration Contract

The shared semantic request contains:

- service name, worker type, host, instance UUID, consumer ID;
- exact sorted capabilities;
- image identity;
- three dependency fingerprints;
- requested lease duration fixed at 45 seconds.

Registration:

1. validates static worker admission;
2. reads a bounded token file with restrictive permissions;
3. opens PostgreSQL;
4. locks and validates the matching grant and token hash;
5. validates exact service/type/host/capability claims;
6. revokes the old active service registration;
7. inserts a new epoch and returns its ID and expiry.

Any failure exits before Redis client construction, consumer-group creation,
PEL reclaim, `XREADGROUP`, or event publication.

Heartbeat runs every 15 seconds and extends the lease to 45 seconds only when
registration ID, service, instance ID, epoch, status, and token still match.
Failure, revocation, or expiry cancels the worker context. In-flight work is
cancelled and left pending without acknowledgement.

Graceful shutdown performs a bounded best-effort revoke. A failed shutdown
revoke cannot keep the process alive; the lease expires naturally.

## Dependency Fingerprints

Fingerprint builders parse structured URLs and endpoints, remove credentials,
normalize scheme/host/port/database or bucket identity, then hash canonical
JSON. They reject malformed or local production endpoints using the existing
admission rules.

- database: driver family, host, port, database name;
- Redis: scheme, host, port, database index;
- storage: backend, MinIO host/port, bucket;
- non-storage workers use an explicit `not_applicable` storage fact.

The Python and Go implementations share fixed cross-language fixtures so the
same environment produces identical hashes.

## Worker Lifecycle

### Python workers

Worker identity becomes process-unique rather than PID-only. Startup order is:

1. static admission;
2. database configuration;
3. durable registration;
4. lease heartbeat task;
5. Redis client creation and group join;
6. PEL recovery and consume loop.

The heartbeat cancellation signal controls the consume loop and all spawned
message tasks.

### Go ffmpeg worker

The Go worker opens PostgreSQL and storage, creates its durable registration,
starts heartbeat, and only then constructs or uses the Redis client. The
registration context is the parent of the consumer context.

## Deployment

The migration must run before any new worker image is started. The application
deployment order becomes:

1. API/frontend;
2. backend migration service;
3. verify migration head;
4. ensure service credential state and Docker secrets;
5. upsert exact grants through the new Python grant CLI;
6. update and verify the managed workers;
7. continue runner and remaining service convergence.

Each worker service receives:

- `WORKER_SERVICE_NAME`;
- `WORKER_IMAGE_IDENTITY`;
- `WORKER_CAPABILITIES`;
- `WORKER_ADMISSION_TOKEN_FILE`;
- the service-scoped Docker secret.

The controller verifies the running service has exactly the expected secret,
identity variables, registration row, consumer ID, image, host, capabilities,
and unexpired lease. A registration readiness failure enters the existing
deployment rollback path.

Credential state is created only on host 150. Host 126 is never used for state,
secret creation, grant writes, worker placement, readiness, or rollback.

## Unknown Consumer Guard

The repository-owned watcher runs every five minutes. A registration guard
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
unknown and is caught within one watcher interval.

## Failure Handling

- missing/unsafe token file: worker exits before Redis;
- missing, disabled, revoked, or mismatched grant: worker exits before Redis;
- database outage at startup: worker exits before Redis;
- heartbeat database outage: worker cancels and exits without ack;
- replacement registration: stale epoch heartbeat fails;
- deployment grant/secret failure: service update is not attempted;
- readiness mismatch: existing deployment rollback runs;
- unknown consumer: global fail-closed hold;
- watcher database/Redis uncertainty: nonzero result, never reports healthy;
- cleanup failure: lease expiry remains the final fence.

## Testing

- PostgreSQL 16 migration, constraints, active-service uniqueness, and
  concurrent takeover;
- token hashing and no-secret persistence/logging;
- exact claim validation and endpoint fingerprint fixtures in Python and Go;
- Python startup ordering, heartbeat cancellation, revoke, and no Redis calls
  on denial;
- vision storage admission and fingerprint requirements;
- Go startup ordering, heartbeat cancellation, epoch takeover, and no Redis
  calls on denial;
- in-flight work is not acknowledged after lease loss;
- deploy credential/secret/grant lifecycle, migration order, readiness,
  rollback, and 126 exclusion;
- watcher unknown/expired/mismatched/duplicate consumer auto-hold and healthy
  no-write behavior;
- five-minute managed cron contract;
- full backend, Go, race, frontend, deployment, canary preflight, and soak
  guard checks.

## Rollout

The deployment first migrates and provisions grants while old workers continue
under the existing safety controls. It then updates one worker service at a
time and requires a healthy registration before proceeding.

Before another canary, a read-only production audit must show:

- exactly four expected active registrations;
- all leases fresh;
- every Redis consumer mapped to one registration;
- actual service image/host/capabilities match;
- no unknown consumers, pending work, or tasks on host 126.

No canary, upload, schedule opening, channel resume, soak activation, or public
publication is part of this rollout.
