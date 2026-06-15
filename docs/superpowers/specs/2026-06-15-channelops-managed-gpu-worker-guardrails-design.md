# ChannelOps Managed GPU Worker Guardrails Design

Date: 2026-06-15

## Problem

The ChannelOps soak exposed a production queue ownership problem. A legacy
standalone Python ffmpeg worker on host `10.0.0.150` was still consuming
`vp:tasks:ffmpeg`, but it was not managed by the current Swarm deployment and
did not have production MinIO settings. When that worker claimed a
`youtube_upload` task whose input artifact lived in MinIO, it used the Python
default `MINIO_ENDPOINT=localhost:9000` and failed before upload.

Host `10.0.0.150` is also the only NVIDIA-capable machine, so the fix must
allow workers on 150. The fix is to make the 150 worker a first-class managed
production worker and prevent unmanaged or underconfigured workers from joining
production queues.

## Goals

- Run the 150 Python ffmpeg worker as an explicit Swarm service, constrained to
  the 150 GPU node.
- Give that service complete production configuration for Postgres, Redis,
  MinIO, YouTube credentials, scratch storage, and worker identity.
- Add worker startup guardrails so production queue consumers fail fast when
  required storage settings are missing.
- Add soak monitoring that reports unexpected `vp:tasks:ffmpeg` consumers and
  production task failures, not only service health and Redis lag.
- Keep the current queue model for this change: `youtube_upload` and other
  Python ffmpeg nodes continue to use `vp:tasks:ffmpeg`.

## Non-Goals

- Do not create a new `ffmpeg_gpu` queue in this change.
- Do not migrate YouTube upload to Go.
- Do not change PDS policy, publication privacy defaults, or task scheduling
  cadence.
- Do not remove local development support for the Python ffmpeg worker.

## Proposed Architecture

Add a Swarm-managed service named `vp-ffmpeg-worker-gpu-swarm` for the Python
ffmpeg worker. The service runs on the 150 node via the node label constraint
`node.labels.vp.gpu == true`. Deployment must label host `10.0.0.150` with
that key before creating or updating the service. A hostname constraint is not
the default because node labels make the intent explicit.

The service uses the same worker image lineage as the existing Python
`ffmpeg-worker`, but it is deployed through the production Swarm path instead
of the local `docker compose` runtime. It consumes `WORKER_TYPE=ffmpeg` and
sets `WORKER_HOST=150-gpu` so Redis consumer names are stable and monitorable.

Required production environment:

- `DEPLOY_MODE=shared`
- `DATABASE_URL=postgresql+asyncpg://vp:vp_secret@10.0.0.150:5435/videoprocess`
- `REDIS_URL=redis://10.0.0.150:6380/0`
- `STORAGE_BACKEND=minio`
- `MINIO_ENDPOINT=10.0.0.150:9000`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET=videoprocess`
- `YOUTUBE_CREDENTIALS_DIR=/app/youtube_credentials`
- `WORKER_TYPE=ffmpeg`
- `WORKER_HOST=150-gpu`
- `VIDEO_USE_GPU=true`
- `VIDEO_GPU_FALLBACK_TO_CPU=true`
- `NVIDIA_VISIBLE_DEVICES=all`
- `NVIDIA_DRIVER_CAPABILITIES=compute,video,utility`

The service mounts YouTube credentials read-only and provides a local scratch
directory for temporary input/output files. Artifact durability remains in
MinIO, not in that scratch mount.

## Worker Admission Guard

Add a small startup validation step in the Python worker before it opens Redis
or Postgres connections.

The guard classifies a worker as a production queue consumer when either:

- `DEPLOY_MODE` is `shared` or `production`, or
- `REDIS_URL` points to a non-localhost address.

For production queue consumers:

- If `WORKER_TYPE=ffmpeg`, require `STORAGE_BACKEND=minio`.
- Require non-empty `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`,
  `MINIO_SECRET_KEY`, and `MINIO_BUCKET`.
- Reject known unsafe MinIO endpoints for production, including
  `localhost:9000` and `127.0.0.1:9000`.
- Require `WORKER_HOST` to be explicit so consumer names are auditable.

The guard exits with a clear fatal log message. It does not prevent local
development workers from using local storage when they use local Redis.

## Compose Safety

The existing local `ffmpeg-worker` service stays available for development, but
it should no longer be easy to start accidentally on a production host as a
background production consumer.

Add an explicit Compose profile named `local-python-worker` to the local
Python `ffmpeg-worker` service. Documentation and runbooks should show that
production uses the Swarm service, while local development must opt in with:

```bash
docker compose --profile local-python-worker up ffmpeg-worker
```

This does not remove existing APIs or worker handlers. It changes the default
operational entry point to reduce accidental legacy consumers.

## Monitoring

The soak watch should report four categories:

1. Swarm service health, including `vp-ffmpeg-worker-gpu-swarm`.
2. Redis stream health for `vp:tasks:ffmpeg`, `vp:tasks:ffmpeg_go`, and
   `vp:events`, including pending and lag.
3. Active consumer audit for `vp:tasks:ffmpeg`. Consumers should match this
   initial allowlist: `ffmpeg-worker@wenjie:*` and `ffmpeg-worker@150-gpu:*`.
4. ChannelOps data and task health:
   - counts by `production_tasks.state`
   - recent `failed` and `held` tasks
   - publication count and privacy distribution
   - material ledger rows
   - feedback and reward rows
   - decision audit rows and populated `pds_decision_json`
   - takedown count

The watch should not silently pass when services are healthy but business tasks
are failing.

## Data Flow

1. ChannelOps creates a production task and AutoFlow job.
2. Go workers handle Go-owned media nodes on `vp:tasks:ffmpeg_go`.
3. Python ffmpeg workers handle Python-owned nodes on `vp:tasks:ffmpeg`,
   including `youtube_upload`.
4. The Swarm-managed 150 GPU worker may claim work from `vp:tasks:ffmpeg`.
   Because it is configured with MinIO and YouTube credentials, it can safely
   process both GPU-capable nodes and upload nodes.
5. The worker writes node events back to `vp:events`; publication and metrics
   flow continues unchanged.

## Failure Handling

- A production worker with unsafe storage config fails at startup and never
  joins the Redis consumer group.
- An unexpected active consumer is reported by soak watch.
- New `failed` or `held` production tasks are visible in soak watch output.
- If the 150 GPU node is down, Swarm reports `0/1` for
  `vp-ffmpeg-worker-gpu-swarm`; the existing remote Mac worker can still
  process non-GPU Python ffmpeg work if it is healthy.

## Testing

Automated checks:

- Unit tests for the worker admission guard:
  - local Redis plus local storage is allowed.
  - production Redis plus missing MinIO config is rejected.
  - production Redis plus `localhost:9000` MinIO is rejected.
  - production Redis plus full MinIO config and explicit `WORKER_HOST` is
    allowed.
- Compose config test or text test confirming local `ffmpeg-worker` is behind
  an explicit profile.
- Script syntax or focused test for the soak watch active-consumer and
  data-coverage sections.

Operational verification:

- Deploy labels the 150 node as GPU-capable.
- `docker service ls` shows `vp-ffmpeg-worker-gpu-swarm 1/1`.
- Redis `XINFO CONSUMERS vp:tasks:ffmpeg ffmpeg-workers` shows expected
  active consumers only.
- A ChannelOps smoke task can progress through upload without MinIO localhost
  errors.
- Soak watch reports service health, queue health, active consumers, task
  states, and data coverage.

## Rollout

1. Add worker admission guard and tests.
2. Add Compose profile safety for local Python `ffmpeg-worker`.
3. Add deploy-sync support that builds the Python worker image and creates or
   updates `vp-ffmpeg-worker-gpu-swarm` with the required env, mounts, network,
   and `node.labels.vp.gpu == true` placement constraint.
4. Add or update the soak watch script in the repository deployment assets.
5. Deploy to production.
6. Remove or keep stopped any legacy standalone `vp_ffmpeg_worker_1` container
   on 150 after confirming the Swarm worker is healthy.

## Success Criteria

- No unmanaged 150 standalone worker is required for production ChannelOps.
- A production `ffmpeg` worker with missing MinIO config cannot start and claim
  tasks.
- The 150 NVIDIA-capable worker is visible as a Swarm service and has stable
  consumer identity.
- The soak monitor catches unexpected consumers and task-level failures.
- The next ChannelOps upload smoke does not fail with
  `HTTPConnectionPool(host='localhost', port=9000)`.
