# Production Worker Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Require every production worker to hold a short, revocable, service-bound registration lease before joining Redis and automatically hold production when an unknown consumer appears.

**Architecture:** PostgreSQL stores generation-scoped token grants and epoch-fenced 180-second registrations. Python and Go workers use versioned non-owner database principals, register directly before their first Redis command, renew every 60 seconds, and recheck the same epoch before every durable completion; the deploy controller provisions versioned Docker secrets/grants and the one-minute watcher applies a global fail-closed hold for unknown consumers.

**Tech Stack:** PostgreSQL 16, Alembic, SQLAlchemy 2 async, Python 3.12, Go 1.25/pgx, Redis Streams, Docker Swarm, Bash.

## Global Constraints

- Registration is required before the first Redis client construction or command in production.
- Lease duration is exactly 180 seconds; heartbeat interval is exactly 60 seconds.
- Raw admission tokens and database URLs/passwords never enter service
  environment variables, command arguments, logs, evidence, or application
  tables. PostgreSQL retains only its internal login verifier.
- Every grant is scoped to one service generation, full commit, image, database
  principal, worker type, host, stream/group, endpoint bindings, and exact
  sorted capability set.
- Every registration records the exact deployed image and non-secret dependency fingerprints.
- Replacement registration revokes and fences the prior service epoch atomically.
- Registration or heartbeat uncertainty fails closed.
- Production node claims and all final writes/XACKs require the same active
  registration ID and epoch.
- Stored-function execution is limited to a non-owner worker runtime principal;
  each service generation gets a fresh login role and secret, and when it cannot
  prove that separation the preflight fails with
  `worker_database_role_not_isolated`.
- The four managed worker services run only on hosts 127 and 150; host 126 is forbidden.
- Public publication remains blocked and external platform assets still require explicit human review.
- No canary, upload, schedule opening, channel resume, soak activation, or policy activation is part of this plan.
- Per-stream Redis ACL users are a separate required T05 hardening increment;
  this plan must not be used to claim that all of T05 is complete.

---

### Task 1: Add Registration Schema And Python Lease Service

**Files:**
- Create: `backend/alembic/versions/034_worker_registrations.py`
- Create: `backend/app/models/worker_registration.py`
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/app/models/job.py`
- Create: `backend/app/services/worker_registration.py`
- Create: `backend/tests/services/test_worker_registration.py`
- Create: `backend/tests/migrations/test_worker_registrations_postgres.py`
- Create: `tests/fixtures/worker_registration/fingerprints-v1.json`

**Interfaces:**
- Produces `WorkerAdmissionGrant` and `WorkerRegistration` ORM models.
- Produces `WorkerRegistrationClaims`, `WorkerLease`, and
  `WorkerRegistrationService.register/heartbeat/revoke`.
- Produces PostgreSQL functions `vp_worker_register`,
  `vp_worker_heartbeat`, `vp_worker_release`, and
  `vp_require_worker_lease` with fixed `search_path`.
- Produces canonical `dependency_fingerprints(env)`.
- Produces stable errors `grant_missing`, `grant_disabled`, `token_invalid`,
  `claim_mismatch`, `database_principal_mismatch`,
  `database_principal_privileged`, `lease_expired`, and `lease_fenced`.

- [ ] **Step 1: Write failing model, migration, and service tests**

Cover all schema fields and checks from the design, unique service grants,
unique token hashes, one active registration per service, exact claim
validation including `session_user`/grant principal binding, rejection of
superusers/table owners/direct or assumable-role registration-table writers,
NULL fail-closed behavior, NULL-safe CHECK behavior, canonical endpoint/
fingerprint binding, token hashing, no raw secret persistence, 180-second expiry,
60-second renewal semantics, separate lease-secret hashing, concurrent
takeover, stale epoch heartbeat, expiry, revoke, nullable historical
`node_executions` registration fields, and cross-language canonical fingerprint
fixtures. Live PostgreSQL tests hold a require fence and prove heartbeat stays
available while takeover/release wait, and prove database time is read after
advisory-lock waits.
The shared fixture defines database, Redis, MinIO, and not-applicable inputs
with their exact canonical JSON and SHA-256 results.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_worker_registration.py \
  tests/migrations/test_worker_registrations_postgres.py -q
```

Expected: fail because revision 034 and the service do not exist.

- [ ] **Step 3: Implement additive schema and lease service**

Compare token hashes with `hmac.compare_digest`. Normalize capabilities by
sorting and rejecting empty, duplicate, or unsupported values. A grant records
generation, pending/active/revoked state, full commit, exact image, exact
database principal, stream/group, and canonical endpoint bindings whose
fingerprints are recomputed before admission.
Use the design's global advisory-lock order: service-exclusive, then
registration-exclusive locks in ascending UUID order, then row locks/mutation.
Register serializes on the service lock and takes the prior registration's
exclusive lock before takeover. Require and heartbeat take the compatible
registration-shared transaction lock; require performs no row lock. Release
takes the registration-exclusive lock and is idempotent. All database time is
computed after lock waits. SQL functions are schema-qualified
`SECURITY DEFINER`, set `search_path` to `pg_catalog`, use `session_user`,
fail closed on NULL, reject direct/column/assumable-role privileged callers,
and revoke public execution.

- [ ] **Step 4: Run focused tests and PostgreSQL migration checks**

Run the Step 2 command with
`CHANNEL_OPS_POSTGRES_TEST_URL` pointing to PostgreSQL 16. Expected: pass and
Alembic head is `034_worker_registrations`.

- [ ] **Step 5: Commit**

```bash
git add backend/alembic/versions/034_worker_registrations.py \
  backend/app/models/worker_registration.py \
  backend/app/models/__init__.py \
  backend/app/models/job.py \
  backend/app/services/worker_registration.py \
  backend/tests/services/test_worker_registration.py \
  backend/tests/migrations/test_worker_registrations_postgres.py \
  tests/fixtures/worker_registration/fingerprints-v1.json
git commit -m "feat(workers): add durable registration leases"
```

### Task 2: Fence Python Workers Before Redis

**Files:**
- Create: `backend/worker/registration.py`
- Create: `backend/worker/secret_config.py`
- Modify: `backend/worker/main.py`
- Modify: `backend/app/services/worker_admission.py`
- Modify: `backend/app/services/job_execution_authority.py`
- Modify: `backend/app/services/youtube_upload_operations.py`
- Modify: `backend/worker/handlers/youtube_upload.py`
- Modify: `backend/tests/worker/test_worker_startup.py`
- Modify: `backend/tests/worker/test_worker_admission.py`
- Modify: `backend/tests/services/test_job_execution_authority.py`
- Modify: `backend/tests/services/test_youtube_upload_operations.py`
- Modify: `backend/tests/worker/test_youtube_upload_handler.py`
- Create: `backend/tests/worker/test_worker_registration_lifecycle.py`

**Interfaces:**
- Consumes `WorkerRegistrationService` from Task 1.
- Produces `PythonWorkerRegistration.start()`, `wait_lost()`, and `close()`.
- Produces a process-unique consumer ID containing the registration instance
  UUID.

- [ ] **Step 1: Write failing startup and lifecycle tests**

Require static admission, database setup, token-file validation, durable
registration, and heartbeat start to finish before `_redis()` is called.
Assert denied registration performs zero Redis construction/group/read/reclaim
calls. Cover restrictive token-file size/permissions, lease loss cancellation,
in-flight task cancellation without `XACK`, graceful revoke, bounded shutdown,
sanitized logs, and vision workers requiring the same MinIO identity checks as
other artifact consumers. Claim a node with registration ID/epoch, then revoke
or supersede the lease and assert artifact, YouTube submission, completion,
failure, event, and acknowledgement paths cannot commit.
For YouTube submission, prove `vp_require_worker_lease` is called inside the
existing durable authority transaction and its registration-shared advisory
lock remains held through the irreversible POST and committed `submitted`
transition. Prove heartbeat remains available while release/takeover wait,
lease-loss cancellation preserves `request_attempted_at` uncertainty, and the
120-second POST / 15-second transition bounds plus 150-second fresh-lease
margin are enforced.
Production tests require `WORKER_DATABASE_URL_FILE`, reject environment-only
`DATABASE_URL`, enforce bounded mode `0400` secret reads, and prove no database
credential appears in logs.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/worker/test_worker_startup.py \
  tests/worker/test_worker_registration_lifecycle.py -q
```

Expected: fail because the lifecycle wrapper is absent and Redis is currently
constructed without registration.

- [ ] **Step 3: Implement Python lifecycle fencing**

Generate one UUID at process startup and derive the Redis consumer ID from it.
Load the production database URL from `/run/secrets/vp-worker-database-url`
before creating the SQLAlchemy engine; preserve environment fallback only for
non-production development/tests.
Read `/run/secrets/vp-worker-admission-token` through
`WORKER_ADMISSION_TOKEN_FILE`. Start a heartbeat task before constructing
Redis. Race the consume loop against lease loss; on loss cancel all spawned
message tasks and leave their deliveries pending. Store registration ID/epoch
when claiming the node and require the same live lease inside existing durable
execution-authority locks before all final writes. Revoke with a fresh bounded
context in `finally`.
Immediately heartbeat before entering the YouTube submission fence and reject
less than 150 seconds of remaining lease margin. Inside
`YouTubeUploadOperationStore.submission_fence`, call
`public.vp_require_worker_lease` on the same transaction before yielding; keep
that transaction open across the POST and `mark_submitted` commit. Bound the
irreversible POST to 120 seconds and the durable transition to 15 seconds while
the normal heartbeat continues under the compatible shared advisory lock.

- [ ] **Step 4: Run focused and full Python worker tests**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/worker/test_worker_startup.py \
  tests/worker/test_worker_registration_lifecycle.py \
  tests/worker/test_worker_youtube_binding.py -q
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add backend/worker/registration.py \
  backend/worker/secret_config.py \
  backend/worker/main.py \
  backend/app/services/worker_admission.py \
  backend/app/services/job_execution_authority.py \
  backend/app/services/youtube_upload_operations.py \
  backend/worker/handlers/youtube_upload.py \
  backend/tests/worker/test_worker_startup.py \
  backend/tests/worker/test_worker_admission.py \
  backend/tests/services/test_job_execution_authority.py \
  backend/tests/services/test_youtube_upload_operations.py \
  backend/tests/worker/test_youtube_upload_handler.py \
  backend/tests/worker/test_worker_registration_lifecycle.py
git commit -m "feat(workers): register python consumers before redis"
```

### Task 3: Fence The Go Worker Before Redis

**Files:**
- Create: `internal/worker/registration.go`
- Create: `internal/worker/registration_test.go`
- Create: `internal/worker/secret_config.go`
- Create: `internal/worker/secret_config_test.go`
- Modify: `internal/worker/worker.go`
- Modify: `internal/worker/consumer.go`
- Modify: `internal/worker/consumer_test.go`
- Modify: `internal/redisstream/streams.go`
- Modify: `internal/redisstream/streams_test.go`
- Modify: `internal/store/node_executions.go`
- Create: `internal/store/worker_task_dispatches.go`
- Create: `internal/store/worker_task_dispatches_test.go`
- Modify: `internal/store/artifacts.go`
- Modify: `internal/store/artifact_writes.go`
- Create: `internal/store/worker_registration.go`
- Create: `internal/store/worker_registration_test.go`
- Modify: `internal/worker/runtime.go`
- Modify: `cmd/vp-ffmpeg-worker/main.go`
- Create: `cmd/vp-ffmpeg-worker/main_test.go`

**Interfaces:**
- Produces Go `RegistrationClaims` and `RegistrationLease` wrappers over the
  reviewed schema-qualified PostgreSQL functions. It must not reimplement
  registration with direct table DML or ad hoc row locks.
- Consumes `tests/fixtures/worker_registration/fingerprints-v1.json` and
  produces the exact same canonical fingerprints as Python.
- Consumer `Run` accepts a registration-owned context and returns
  `ErrRegistrationLost` when fenced.

- [ ] **Step 1: Write failing Go store and process-order tests**

Cover token/claim checks, 180-second lease, 60-second heartbeat, concurrent
takeover, stale epoch, expiry, revoke, fingerprint fixture parity, no Redis
client/group/read call before registration, lease-loss consumer cancellation,
and registration-epoch checks before remote object save plus artifact pointer,
event publication, completion/failure, and XACK writes. Every registered task
must require its orchestrator-created `dispatch_key`, canonical payload hash,
delivered stream/group/message identity, and claim-time delivery attestation.
The exact task proof must be copied into completion/failure events. All Redis
`XADD`/`XACK` must run while the same transaction holds the
registration-shared fence. A registered affinity defer performs zero
`XADD`/`XACK` and leaves the exact original delivery pending until preferred
reclaim or affinity age expiry. The preferred host inspects the exact PEL
entry and `XCLAIM`s that unchanged message during the 20-second window; add a
real Redis integration test proving there is no replacement `XADD` or defer
`XACK`. Test live ACK authorization, applied
receipt recovery, Redis-success/DB-failure replay, and arbitrary
stream/group/message/hash/dispatch rejection. A lost epoch with neither
durable ACK authorization nor receipt must produce no final write or
acknowledgement. Require sanitized errors. In production, require a bounded mode `0400`
`WORKER_DATABASE_URL_FILE`, reject environment-only database credentials, and
prove no secret is logged.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
go test ./internal/worker ./cmd/vp-ffmpeg-worker \
  -run 'Test(Registration|WorkerStartup|ConsumerRegistrationLoss)' -count=1
```

Expected: compile failure because registration types are absent.

- [ ] **Step 3: Implement Go registration and lifecycle**

Use pgx transactions that call the reviewed `public.vp_worker_register`,
`public.vp_worker_heartbeat`, `public.vp_worker_release`,
`public.vp_require_worker_lease`,
`public.vp_claim_worker_node`,
`public.vp_authorize_worker_task_ack`,
`public.vp_require_worker_task_ack_receipt`, and
`public.vp_acknowledge_worker_task_delivery` functions. Do not duplicate their
row/advisory-lock implementation in Go. PostgreSQL tests use
`CHANNEL_OPS_GO_POSTGRES_TEST_URL`. Read the
database URL and admission token from separate bounded secret files and build a
process UUID consumer ID. Preserve `DATABASE_URL` fallback only outside
production. Open storage and
PostgreSQL, register, start heartbeat, then construct/use Redis. Registration
loss cancels the consumer context. Store the registration ID/epoch on node
claim through the atomic claim-and-attest function; do not directly update
`node_executions` or insert attestations. Implement initial/downstream/retry
dispatch reads and exact fields in `internal/store/worker_task_dispatches.go`.
Hold one shared-fence transaction across remote object save and artifact
pointer insertion, with a second database-time lease check immediately before
the pointer. Use finite MinIO deadlines/retries and claim-unique staging keys;
shutdown must not wait on an unbounded Go or SDK call, and a staging janitor
owns abandoned objects. Bind node completion/failure and every task XACK to the immutable
delivery attestation; leave PEL pending on unproven lease loss.

- [ ] **Step 4: Run focused, full, and race tests**

```bash
go test ./internal/worker ./cmd/vp-ffmpeg-worker \
  -run 'Test(Registration|WorkerStartup|ConsumerRegistrationLoss)' -count=1
go test ./...
go test -race ./internal/worker ./cmd/vp-ffmpeg-worker
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/worker/registration.go \
  internal/worker/registration_test.go \
  internal/worker/secret_config.go \
  internal/worker/secret_config_test.go \
  internal/worker/worker.go \
  internal/worker/consumer.go \
  internal/worker/consumer_test.go \
  internal/redisstream/streams.go \
  internal/redisstream/streams_test.go \
  internal/store/node_executions.go \
  internal/store/worker_task_dispatches.go \
  internal/store/worker_task_dispatches_test.go \
  internal/store/artifacts.go \
  internal/store/artifact_writes.go \
  internal/store/worker_registration.go \
  internal/store/worker_registration_test.go \
  internal/worker/runtime.go \
  cmd/vp-ffmpeg-worker/main.go \
  cmd/vp-ffmpeg-worker/main_test.go
git commit -m "feat(workers): register go consumer before redis"
```

### Task 4: Provision Grants And Secrets During Deployment

**Files:**
- Create: `backend/app/services/worker_registration_operator_cli.py`
- Create: `backend/tests/services/test_worker_registration_operator_cli.py`
- Create: `backend/app/services/worker_runtime_role_cli.py`
- Create: `backend/tests/services/test_worker_runtime_role_cli.py`
- Create: `backend/app/services/worker_control_role_cli.py`
- Create: `backend/tests/services/test_worker_control_role_cli.py`
- Create: `backend/app/channel_agent/staging_object_janitor_cli.py`
- Create: `backend/app/services/staging_object_janitor.py`
- Create: `backend/tests/channel_agent/test_staging_object_janitor_cli.py`
- Create: `backend/tests/services/test_staging_object_janitor.py`
- Modify: `backend/app/services/worker_storage_readiness.py`
- Modify: `backend/tests/services/test_worker_storage_readiness.py`
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`
- Modify: `backend/Dockerfile.ffmpeg-worker-go`
- Modify: `backend/Dockerfile.worker`

**Interfaces:**
- Produces
  `python -m app.services.worker_registration_operator_cli upsert|activate|revoke-grant|revoke-registration|expire-registration`.
- Produces `python -m app.services.worker_runtime_role_cli provision|revoke`.
- Uses only `public.vp_worker_grant_upsert`,
  `public.vp_worker_grant_activate`, `public.vp_worker_grant_revoke`,
  `public.vp_worker_registration_revoke`, and
  `public.vp_worker_registration_expire` for routine protected-table
  mutations.
- Produces service credential state below
  `$DEPLOY_GITHUB_SYNC_ROOT/state/vp-worker-admission/`.
- Produces service secret target
  `/run/secrets/vp-worker-admission-token`.
- Produces versioned non-owner PostgreSQL login principals and database URL
  secret target `/run/secrets/vp-worker-database-url`.
- Produces deployment registration readiness verification.
- Produces an independent orchestrator-control database role and per-service
  Redis ACL users; neither is inherited by a worker login principal.

- [ ] **Step 1: Write failing CLI and deployment contract tests**

Require mode `0600` transactional state, fresh 256-bit admission/database
credentials and versioned secrets per service/release generation, no credential
in commands/logs/env/database output, exact commit/image/service/database-
principal/type/host/capabilities/stream/group/endpoint claims, migration before
principal/grant and worker update, embedded full build commit, registration
readiness before proceeding, fresh prior-image grant/principal on rollback,
idempotent repeat deployment, and no host 126 command. Create real PostgreSQL
test roles and prove login principals are non-superuser, own no application
objects, cannot write grant/registration tables directly, and can execute only
the reviewed registration, lease, claim, artifact, event-emission, task-ACK,
and YouTube reserve/transition functions.

Grant only direct `SELECT` columns required to load task inputs and returned
upload-operation rows. Grant no direct `INSERT`, `UPDATE`, or `DELETE` on
`artifacts`, `runtime_schedules`, `youtube_upload_operations`,
`node_executions`, authority tables, or dispatches. Artifact persistence uses
`vp_persist_worker_artifact`; runtime schedule authority is read and locked
only through `vp_claim_worker_node` and `vp_require_worker_node_claim`, and
workers never create or mutate schedules. YouTube reserve and every legal
operation transition use `vp_reserve_worker_youtube_upload` and
`vp_transition_worker_youtube_upload` with exact operation/artifact/node claim,
registration epoch, database-clock margin, privacy, and allowed-column
enforcement. Exercise `_claim_node_execution`,
`_persist_artifact_for_current_claim`, registered event publication/task ACK,
and `YouTubeUploadOperationStore` through the real restricted login; deny
direct DML in the same PostgreSQL test. Deny `DELETE`, `TRUNCATE`, DDL, sequences,
`alembic_version`, all direct grant/registration table access, privileged role
membership, and ownership. Use separate deploy-migrator and deploy-read
credentials; neither may be mounted into workers.
Create a separate non-owner orchestrator-control role. Grant it `EXECUTE` only
on the reviewed observer, proven-task ACK, cancelled-dispatch, registered-node
recovery, and cleanup functions:
`vp_observe_worker_lease`, `vp_observe_worker_task_delivery`,
`vp_observe_worker_event_emission`,
`vp_acknowledge_proven_worker_task_dispatch`,
`vp_authorize_cancelled_worker_task_ack`,
`vp_require_cancelled_worker_task_ack`,
`vp_acknowledge_cancelled_worker_task`,
`vp_recover_registered_worker_node`, and
`vp_resolve_worker_event_authority_for_job_deletion`. Grant exact
`SELECT`/`INSERT` and state-column `UPDATE` on `worker_task_dispatches`,
`worker_task_delivery_attestations`,
`registered_worker_event_receipts`, and
`registered_worker_event_deliveries`; and the exact job, node execution,
artifact, and node-artifact-cache columns exercised by a real
`RegisteredWorkerEventReceiptService.accept_and_apply()` transaction. Deny
worker registration functions, direct grant/registration access, protected
table ownership, broad `UPDATE`, `DELETE`, and `TRUNCATE`. Prove observer
success requires explicit `EXECUTE`, while `vp_require_worker_lease` still
rejects the orchestrator principal, and takeover/revoke waits behind observer
fences.

Deploy `python -m app.channel_agent.staging_object_janitor_cli` as the
`vp-staging-object-janitor` one-shot service on a five-minute managed interval
using the same MinIO bucket and a read-only database principal. Its only
deletion candidates are exact claim-shaped
`staging/artifacts/{job_uuid}/{node_uuid}-{claim_token}.{ext}` objects older
than 86,400 seconds. It excludes every exact `artifacts.storage_path` and
durable `intermediate_artifact_cache.storage_path`, including successful
staging-prefixed pointers whose source job was deleted. Write status
atomically with mode 0600 to
`/run/videoprocess/staging-janitor/status.json`; alert on nonzero errors,
missing runs, or age above 900 seconds. Set
`VP_REQUIRE_STAGING_JANITOR=true` and
`VP_STAGING_JANITOR_STATUS_FILE` for registered workers and fail readiness
closed until a fresh successful status exists. Tests cover old orphan deletion,
fresh/invalid retention, exact pointer protection, status monitoring, and the
grace bound against every operation/retry/clock-skew window.
Create per-service Redis users scoped to their admitted task stream/group.
Create a separate orchestrator Redis user for the event group and admitted
task streams. Its explicit command allowlist includes stream consume/reclaim
and ACK operations plus `EVAL`, `GET`, `SET`, and `XADD` for the reviewed
idempotent dispatch script and `vp:worker-task-dispatch:*` keys. Deny broad key
patterns and arbitrary scripts. Test marker recovery across Redis-success/
database-failure windows. Require Redis `noeviction`, durable persistence, and
restart readiness before admitting registered traffic. Mark the database row
`attempting` before `EVAL`; the Lua marker has no TTL while a row can be
pending, attempting, or uncertain. A missing marker after an attempt moves the
row to `uncertain`, emits a hold/alert, and performs zero `XADD`. An explicit
janitor may delete a marker only after exact database proof that the dispatch
is delivered and resolved. Test concurrent reconcilers and operator repair;
never allow expiry or eviction to mint a replacement stream message.
Create a versioned non-owner operator `LOGIN` principal with exact `EXECUTE`
on the five schema-qualified operator functions and no direct table or column
read/write privileges. Exercise pending upsert, activation, grant revocation,
operator registration revocation, and expiry through that principal. For each
mutation, hold `public.vp_require_worker_lease` open in another transaction and
prove the CLI waits until the shared fence ends. Prove replacement-generation
and rollback role retirement waits for fenced grant/registration revocation
before role DCL, emits only stable sanitized errors, and never returns or logs
token hashes, lease hashes, admission tokens, or database passwords. Direct
table-owner writes are documented and tested as manual break-glass only; no
automation path may invoke them.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_worker_registration_operator_cli.py \
  tests/services/test_worker_runtime_role_cli.py \
  tests/services/test_worker_control_role_cli.py -q
cd ..
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: fail because grants, secrets, and readiness are absent.

- [ ] **Step 3: Implement grant CLI and deploy transaction**

Run backend migration and verify head before updating workers. Embed the exact
40-character commit in both worker images. Create a stable `NOLOGIN` worker
runtime role with an explicit privilege allowlist, then create a fresh
service/generation `LOGIN` role with a random password and no object ownership.
Create the independent stable orchestrator-control role and the exact
PostgreSQL/Redis grants above. Exercise a real receipt acceptance, concurrent
duplicate alias, quarantine, dispatch reconcile, source/event ACK reconcile,
cancel-before-delivery, cancel-after-delivery-before-claim, cleanup-versus-
cancel lock contention, registration-aware startup recovery, and guarded job
deletion under that role; do not substitute a table owner in these tests.
Create pending grant through `public.vp_worker_grant_upsert` and credential
state atomically with `umask 077`; create
generation-scoped database URL and admission Swarm secrets from stdin without
echoing them. Mount only that service's secrets with mode `0400` and inject
`WORKER_SERVICE_NAME`, `WORKER_RELEASE_COMMIT`, `WORKER_IMAGE_IDENTITY`,
`WORKER_CAPABILITIES`, `WORKER_REDIS_STREAM`, `WORKER_REDIS_GROUP`, and
the two secret file paths. Remove `DATABASE_URL` from the production service
environment. Activate the hashed grant through
`public.vp_worker_grant_activate`. After service convergence, query
registration readiness through a sanitized CLI, fence and revoke the replaced
grant/registration through the operator surface, and only then revoke the
replaced login role. Expiry cleanup uses
`public.vp_worker_registration_expire`. On failure, issue and activate a fresh
prior-image generation with a fresh database principal through the same
surface before entering the existing rollback path; never reuse failed
credentials. The operator principal has exact function execution and no direct
protected-table writes; table-owner mutation is break-glass only.
Keep migration `034` additive during image rollback; downgrade is permitted
only after all registration-aware clients have been removed.

- [ ] **Step 4: Run CLI, deployment, and topology contracts**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_worker_registration_operator_cli.py \
  tests/services/test_worker_runtime_role_cli.py -q
cd ..
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_vp_deploy_ci_gate.sh
bash tests/test_vp_production_smoke_script.sh
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/worker_registration_operator_cli.py \
  backend/tests/services/test_worker_registration_operator_cli.py \
  backend/app/services/worker_runtime_role_cli.py \
  backend/tests/services/test_worker_runtime_role_cli.py \
  backend/app/services/worker_control_role_cli.py \
  backend/tests/services/test_worker_control_role_cli.py \
  deploy/swarm/deploy-sync-extension.sh \
  tests/test_vp_deploy_sync_extension.sh \
  backend/Dockerfile.ffmpeg-worker-go \
  backend/Dockerfile.worker
git commit -m "feat(deploy): provision worker admission grants"
```

### Task 5: Auto-Hold Unknown Redis Consumers

**Files:**
- Create: `backend/app/services/worker_registration_guard.py`
- Create: `backend/app/channel_agent/worker_registration_guard_cli.py`
- Create: `backend/tests/services/test_worker_registration_guard.py`
- Create: `backend/tests/channel_agent/test_worker_registration_guard_cli.py`
- Modify: `backend/app/services/channelops_quarantine.py`
- Modify: `backend/tests/services/test_channelops_quarantine.py`
- Modify: `deploy/swarm/channelops-soak-watch.sh`
- Modify: `tests/test_channelops_soak_watch.sh`
- Modify: `deploy/swarm/deploy-sync-extension.sh`
- Modify: `tests/test_vp_deploy_sync_extension.sh`

**Interfaces:**
- Produces read-only assessment and `--apply-auto-hold` modes.
- Produces stable reasons `unknown_consumer`, `expired_registration`,
  `revoked_registration`, `claim_mismatch`, and `duplicate_consumer`.
- Managed watcher interval becomes exactly one minute; five minutes remains the
  maximum detection SLO.

- [ ] **Step 1: Write failing guard and watcher tests**

Cover healthy exact mapping with zero writes; every unsafe reason; immediate,
idempotent and single-transaction schedule close plus quarantine of all enabled
non-dry-run channels; rollback of the entire hold when any channel mutation
fails;
retention of publications/feedback; no Redis acknowledgement/deletion; database
or Redis uncertainty; sanitized output; one-minute cron with a five-minute
detection SLO; and no mutation when
the registration assessment is healthy.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_worker_registration_guard.py \
  tests/channel_agent/test_worker_registration_guard_cli.py -q
cd ..
bash tests/test_channelops_soak_watch.sh
```

Expected: fail because the guard is absent and the watcher runs every 30
minutes.

- [ ] **Step 3: Implement assessment and fail-closed hold**

Use structured Redis APIs and SQLAlchemy queries. Map every observed consumer
to one unexpired active registration and compare expected image/host/type/
capabilities supplied from inspected Swarm service facts. Refactor the existing
quarantine service to expose a caller-transaction helper, then atomically close
the runtime schedule and quarantine every enabled non-dry-run channel in one
transaction. Preserve publication and feedback evidence. Emit stable JSON
without endpoints, tokens, task payloads, or exception details.

- [ ] **Step 4: Run focused and full safety contracts**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_worker_registration_guard.py \
  tests/channel_agent/test_worker_registration_guard_cli.py \
  tests/services/test_channelops_soak_guard.py -q
cd ..
bash tests/test_channelops_soak_watch.sh
bash tests/test_vp_deploy_sync_extension.sh
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add backend/app/services/worker_registration_guard.py \
  backend/app/channel_agent/worker_registration_guard_cli.py \
  backend/tests/services/test_worker_registration_guard.py \
  backend/tests/channel_agent/test_worker_registration_guard_cli.py \
  backend/app/services/channelops_quarantine.py \
  backend/tests/services/test_channelops_quarantine.py \
  deploy/swarm/channelops-soak-watch.sh \
  tests/test_channelops_soak_watch.sh \
  deploy/swarm/deploy-sync-extension.sh \
  tests/test_vp_deploy_sync_extension.sh
git commit -m "feat(workers): auto-hold unknown consumers"
```

### Task 6: Verify And Document Production Admission

**Files:**
- Modify: `deploy/four-machine-topology.md`
- Modify: `docs/channelops-go-live-runner.md`
- Create: `scripts/worker_registration_preflight.py`
- Create: `backend/tests/services/test_worker_registration_preflight.py`

**Interfaces:**
- Produces a read-only preflight with exact expected registration cardinality,
  fresh leases, consumer mapping, service facts, and forbidden-host checks.
- The preflight performs no database, Redis, schedule, queue, or service write.

- [ ] **Step 1: Write failing preflight tests**

Require exactly four managed registrations, fresh lease margin, one-to-one
consumer mapping, expected 127/150 placement, zero 126 facts, restrictive
evidence file permissions, no secret output, isolated worker database-role
evidence including non-superuser/non-owner/direct-write denial and exact
principal/grant binding, an explicit `redis_acl_not_enforced` residual until
scoped Redis users exist, and strict source-level zero-write behavior.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest \
  tests/services/test_worker_registration_preflight.py -q
```

Expected: fail because the preflight is absent.

- [ ] **Step 3: Implement preflight and runbooks**

Default to sanitized stdout JSON. Permit an optional evidence path written
atomically with mode `0600`. Document registration, revocation, token rotation,
lease-loss recovery, unknown-consumer hold, rollback, and the fixed 127/150
topology with 126 excluded. Do not report the full T05 gate as closed while
`redis_acl_not_enforced` is present.

- [ ] **Step 4: Run all required checks**

```bash
go test ./...
go test -race ./internal/worker ./cmd/vp-ffmpeg-worker
cd backend
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m pytest
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m ruff check . || true
/Users/wenjieliu/videoprocess/backend/.venv/bin/python -m mypy app || true
cd ../frontend
npm install
npm run build
npm run lint || true
cd ..
bash tests/test_vp_deploy_sync_extension.sh
bash tests/test_channelops_soak_watch.sh
bash tests/test_vp_unlisted_canary_scripts.sh
git diff --check
```

Expected: tests and builds pass; advisory baselines do not gain owned-file
findings.

- [ ] **Step 5: Commit**

```bash
git add deploy/four-machine-topology.md \
  docs/channelops-go-live-runner.md \
  scripts/worker_registration_preflight.py \
  backend/tests/services/test_worker_registration_preflight.py
git commit -m "docs(workers): verify production registration"
```
