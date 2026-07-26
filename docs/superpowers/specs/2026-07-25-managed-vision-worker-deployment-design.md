# Managed Vision Worker Deployment Design

## Context

The fifth live unlisted canary reached its single guarded job, but the three
`smart_trim` nodes were claimed by `vp_vision_worker_1`, an unmanaged Compose
container on host 150 built on 2026-05-19. That worker resolved a remote
`local` artifact as a nonexistent local path and emitted `node_failed` events
without the current execution-claim fields. The current orchestrator correctly
left those unverifiable events pending.

The repository-managed Swarm deployment updates the current Python worker image
for FFmpeg and YouTube publishing, but it does not run a `vision` consumer.
This allows the old Compose worker to remain the only consumer of
`vp:tasks:vision`.

## Decision

Deploy a dedicated `vp-vision-worker-swarm` service on host 150 from the same
commit-tagged `vp-ffmpeg-worker-python` image used by the managed GPU and
publisher services.

The service has:

- `WORKER_TYPE=vision`
- `WORKER_HOST=150-vision`
- exactly one replica
- `node.labels.vp.gpu==true` plus `node.hostname==ccttww-lap` placement
- the private pipeline network
- MinIO-backed production storage with a dedicated scratch volume
- the shared production Postgres and Redis endpoints
- no YouTube credentials or publisher capability

Host 126 remains forbidden for build, deploy, runtime, watcher, failover, and
rollback.

## Deployment Order

1. Build the commit-tagged Python worker image on host 150.
2. Determine whether this is a migration: the exact legacy container exists,
   the managed service is absent, or a read-only Redis audit does not find
   exactly one zero-pending managed vision consumer.
3. Only for a migration, require the schedule to be `CLOSED`, no guarded job,
   no queued/running nodes, and zero vision pending/lag before any service
   mutation.
4. Create or update `vp-vision-worker-swarm` stop-first and converge to the
   exact environment, network, secret, config, mount, and placement contract.
5. Verify the service is `1/1` and its only running task is on `ccttww-lap`.
6. Inspect the legacy worker's immutable container ID, exact name, running
   state, Compose project, and Compose service; remove it by ID only when all
   fields match.
7. Wait for exactly one `vision-worker@150-vision:<positive pid>` consumer,
   then atomically require every vision consumer to have zero pending entries
   and delete all non-managed consumer records in one Redis Lua script. Verify
   that only the managed record remains.
8. Include the managed vision service in deployment snapshots, rollback,
   service inventory, and the installed soak watcher.

The video schedule must be `CLOSED` and the vision stream must have no pending
work before this production migration is applied. Once the managed service has
converged and the legacy container is absent, later push-driven updates skip
the migration-only gate so normal automatic deployment remains available while
the schedule is open. Deployment does not replay or acknowledge unverifiable
legacy events.

## Rollback

If an existing managed vision service fails to update, restore its captured
image with the same placement and environment contract. If the deployment
created the service, remove it on rollback. Do not restore the legacy Compose
worker because its event format and artifact handling are incompatible with
the current orchestrator.

## Readiness

The soak watcher treats the following as required:

- `vp-vision-worker-swarm` is healthy and uses the trusted Python image.
- The GPU, vision, and publisher services use the same commit-tagged
  `vp-ffmpeg-worker-python:deploy-<12 hex>` image. Missing consensus or an
  untrusted tag is a configuration error before any image receives database
  credentials.
- `vp:tasks:vision` / `vision-workers` has exactly one active consumer matching
  `vision-worker@150-vision:<positive pid>`.
- Every audited stream has zero stale consumers; the vision stream also has
  zero pending entries during a closed migration check.
- No managed service is placed on host 126.
- Deployment configuration is fixed to runtime host `10.0.0.127`, runtime node
  `colima-127`, and manager node `ccttww-lap`; runtime services also require
  `node.hostname==colima-127`. The same gate runs before all application,
  feature-aggregator, and independent PDS build and deploy entry points.

The live canary preflight applies the same Redis consumer identity check before
any future authorization can be consumed.

## Tests

Shell deployment contract tests cover create, update, immutable
legacy-container identity checks, removal ordering, migration-only gating,
rollback, exact environment/network convergence, task-node verification,
service inventory, and the prohibition on host 126.

Canary and soak watcher tests cover the required vision stream group and exact
consumer identity. A dedicated cutover service test covers zero-pending
consumer cleanup, a pending-arrival race against real Redis 7.4.7, and final
convergence. Existing backend, frontend, Go, CI, and deployment checks remain
required.

## Success Criteria

- A push to `main` automatically deploys the exact commit image to the managed
  vision worker on 150.
- `vp_vision_worker_1` is absent after successful convergence.
- `vp:tasks:vision` has one current consumer and zero legacy consumers.
- The failed fifth canary is closed without a YouTube upload or publication.
- No sixth live canary runs without a new explicit single-canary authorization.
