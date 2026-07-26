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
- `node.labels.vp.gpu==true` placement
- the private pipeline network
- MinIO-backed production storage with a dedicated scratch volume
- the shared production Postgres and Redis endpoints
- no YouTube credentials or publisher capability

Host 126 remains forbidden for build, deploy, runtime, watcher, failover, and
rollback.

## Deployment Order

1. Build the commit-tagged Python worker image on host 150.
2. Create or update `vp-vision-worker-swarm` stop-first.
3. Verify the service is `1/1` and its image, placement, worker type, worker
   host, network, and scratch mount match the desired contract.
4. Remove only a running container named exactly `vp_vision_worker_1` when its
   Compose labels identify project `videoprocess` and service `vision-worker`.
5. Fail closed on an identity mismatch instead of removing an unknown
   container.
6. Include the managed vision service in deployment snapshots, rollback,
   service inventory, and the installed soak watcher.

The video schedule must be `CLOSED` and the vision stream must have no pending
work before this production migration is applied. Deployment does not replay
or acknowledge unverifiable legacy events.

## Rollback

If an existing managed vision service fails to update, restore its captured
image with the same placement and environment contract. If the deployment
created the service, remove it on rollback. Do not restore the legacy Compose
worker because its event format and artifact handling are incompatible with
the current orchestrator.

## Readiness

The soak watcher treats the following as required:

- `vp-vision-worker-swarm` is healthy and uses the trusted Python image.
- `vp:tasks:vision` / `vision-workers` has exactly one active consumer matching
  `vision-worker@150-vision:<positive pid>`.
- The vision stream has zero pending entries during a closed readiness check.
- No managed service is placed on host 126.

The live canary preflight applies the same Redis consumer identity check before
any future authorization can be consumed.

## Tests

Shell deployment contract tests cover create, update, exact legacy-container
identity checks, removal ordering, rollback, environment, placement, network,
mount, service inventory, and the prohibition on host 126.

Canary and soak watcher tests cover the required vision stream group and exact
consumer identity. Existing backend, frontend, Go, CI, and deployment checks
remain required.

## Success Criteria

- A push to `main` automatically deploys the exact commit image to the managed
  vision worker on 150.
- `vp_vision_worker_1` is absent after successful convergence.
- `vp:tasks:vision` has one current consumer and zero legacy consumers.
- The failed fifth canary is closed without a YouTube upload or publication.
- No sixth live canary runs without a new explicit single-canary authorization.
