# Worker Admission and Compose Safety Design

Date: 2026-07-10

## Context

The autonomous production feedback-loop audit identifies unmanaged Python
`ffmpeg` workers as the latest proven production blocker. A legacy worker on
the production Redis stream consumed `vp:tasks:ffmpeg` with the default MinIO
endpoint, `localhost:9000`, then failed before upload because production
artifacts lived in MinIO on another host.

The current local Compose service `ffmpeg-worker` starts by default in
`DEPLOY_MODE=shared`, points at production-like host services, and does not set
`STORAGE_BACKEND=minio`. The Python worker also builds DB/Redis clients at
module import time and creates the Redis consumer group without a startup
configuration gate. This lets an underconfigured process join production
queues before the platform can reject it.

## Goals

- Prevent production Python `ffmpeg` workers from joining Redis streams unless
  their storage and identity settings are explicit and safe.
- Keep local development workers supported when they use local Redis and local
  storage.
- Make the local Python `ffmpeg-worker` an explicit opt-in Compose service via
  a `local-python-worker` profile.
- Add focused tests for the admission guard and Compose profile safety.

## Non-Goals

- Do not change queue names or move `youtube_upload` to a dedicated publisher
  queue in this update.
- Do not add worker registration tables, admission tokens, Redis ACLs, or
  migrations.
- Do not implement upload-operation idempotency, staged metrics, or soak
  evidence collection in this slice.
- Do not remove Python worker support for local development.

## Design

Add `backend/app/services/worker_admission.py` with a pure validation function
that accepts a worker environment snapshot and returns an allow/deny decision.
The worker is classified as a production queue consumer when either:

- `DEPLOY_MODE` is `shared` or `production`; or
- `REDIS_URL` resolves to a non-localhost host.

For production queue consumers with `WORKER_TYPE=ffmpeg`, require:

- `STORAGE_BACKEND=minio`;
- non-empty `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and
  `MINIO_BUCKET`;
- `MINIO_ENDPOINT` must not point at `localhost`, `127.0.0.1`, `0.0.0.0`, or
  `::1`;
- `WORKER_HOST` must be explicitly set in the environment, not only inferred
  from the container hostname.

`backend/worker/main.py` will call the guard before opening Redis, creating a
consumer group, or initializing database engine state. A rejected worker logs a
clear fatal message and exits non-zero.

The local Compose `ffmpeg-worker` service will be moved behind
`profiles: ["local-python-worker"]` and will set `DEPLOY_MODE=local` by
default. Developers can still opt in with:

```bash
docker compose --profile local-python-worker up ffmpeg-worker
```

## Error Handling

Admission denial is fail-closed. The worker exits before stream consumption, so
it cannot create or join the Redis consumer group. Denial reasons are explicit
and non-secret, for example missing MinIO settings, unsafe MinIO endpoint, or
missing explicit worker host.

Local development remains fail-open only for genuinely local Redis hosts and
non-production deploy modes. If a developer points a local worker at remote
Redis, the production guard applies.

## Testing

Add unit tests for:

- local Redis plus local storage is allowed;
- remote Redis plus missing MinIO config is rejected;
- remote Redis plus `localhost:9000` MinIO is rejected;
- remote Redis plus complete MinIO config and explicit `WORKER_HOST` is
  allowed;
- `DEPLOY_MODE=shared` is treated as production even with localhost Redis.

Add a Compose safety test that parses `docker-compose.yml` and asserts
`ffmpeg-worker` includes the `local-python-worker` profile.

## Rollout

1. Merge the guard and tests.
2. Rebuild any Python worker images.
3. Start production workers only from managed deployment assets with explicit
   MinIO and `WORKER_HOST` settings.
4. Start local Python workers only with the explicit Compose profile.

## Success Criteria

- An underconfigured shared/production Python `ffmpeg` worker cannot join
  `vp:tasks:ffmpeg`.
- The default local Compose stack no longer starts `ffmpeg-worker`.
- Local development can still opt into the Python worker deliberately.
- Tests prove the admission and Compose safety behavior.
