# Managed Worker Storage Readiness Design

## Context

The fifth live unlisted canary failed because the unmanaged legacy vision
worker could not resolve an artifact stored on another host. The legacy worker
has since been replaced by the commit-tagged `vp-vision-worker-swarm` service,
and an isolated production check proved that the managed worker can download,
hash, probe, and process the original fifth-canary artifact.

The automatic deployment still treats a running Swarm task as sufficient
worker readiness. It validates the desired storage environment and scratch
mount shape, but it does not prove that the deployed process can write its
scratch directory or authenticate to MinIO. A bad endpoint, credential,
bucket, mount, or network route can therefore survive deployment and fail only
after a real production task is released.

## Decision

Add a bounded worker-storage readiness CLI to the trusted Python worker image
and run it inside each managed Python worker task after the service reaches
`1/1`:

- `vp-ffmpeg-worker-gpu-swarm`
- `vp-vision-worker-swarm`
- `vp-youtube-publisher-swarm`

The CLI verifies:

1. `STORAGE_LOCAL_ROOT` exists or can be created.
2. A random scratch probe can be written, read exactly, and deleted.
3. `STORAGE_BACKEND=minio`.
4. A random MinIO object can be saved, read exactly, and deleted.
5. For the vision worker, `VP_ARTIFACT_DOWNLOAD_BASE_URL` has a reachable
   API health endpoint.

The CLI emits one sanitized JSON object. It never prints connection URLs,
credentials, object contents, or exception details that may contain secrets.
The deployment invokes it by immutable service name and exact running
container ID. A missing, duplicate, stopped, or remote task fails readiness.

## Alternatives Rejected

### Reuse A Historical Artifact In Canary Preflight

This exercises the real artifact route but makes preflight depend on mutable
historical business data. Artifact retention or cleanup would turn a healthy
deployment into a false failure. The preflight-only contract should also
remain free of application-state mutation.

### Create A Dedicated Probe Artifact API

An API-created media fixture could exercise the entire artifact database and
download path, but it requires a new lifecycle, authorization, retention, and
cleanup protocol. The current gap is deployment-time storage readiness, so
that broader mechanism is deferred.

## Safety Boundaries

- No database or Redis connection is opened.
- No application task, artifact row, upload operation, publication, or
  feedback record is created.
- No YouTube or YouTubeManager upload endpoint is called.
- MinIO uses a random `health/deploy-readiness/` object and always attempts
  deletion in `finally`.
- Scratch uses a random file below a dedicated readiness directory and always
  attempts deletion in `finally`.
- Probe payloads are small fixed bytes and are never logged.
- The command has a finite API timeout.
- Host `10.0.0.126`, `CASPERs-Mac-mini`, and `colima-swarmbridged` remain
  forbidden for build, execution, fallback, and rollback.

## Deployment Order

For each managed Python worker service:

1. Create or update the exact commit-tagged service.
2. Require `1/1` and the expected `ccttww-lap` placement.
3. Resolve exactly one running local container for the service.
4. Execute the storage readiness CLI.
5. Require a successful result before continuing.

The probe runs before legacy vision retirement for the migration case. A
managed replacement that cannot access storage must not displace the old
worker. For normal updates, a probe failure returns an error to the existing
deployment transaction so its captured-image rollback path remains active.

## Error Handling

The CLI reports a stable component and reason code:

- `scratch_unavailable`
- `scratch_mismatch`
- `minio_unavailable`
- `minio_mismatch`
- `api_unavailable`
- `configuration_invalid`

It exits nonzero on any failure. Cleanup failure also exits nonzero because a
readiness probe that leaves objects behind is not healthy. The deployment
wrapper reports only the service and stable failure status.

## Tests

Python tests cover:

- successful scratch and MinIO round trips;
- scratch mismatch and cleanup;
- MinIO mismatch and cleanup;
- MinIO save/read/delete failures;
- required MinIO configuration;
- vision API health success, non-200 response, and timeout;
- sanitized JSON output.

Shell deployment contract tests cover:

- each managed Python service runs the probe after `1/1` and placement checks;
- probe failure fails deployment;
- vision probe completes before legacy worker retirement;
- no probe command targets host 126;
- no credential appears in the command or output.

## Success Criteria

- Every automatic VP deployment proves scratch and MinIO access from all three
  managed Python worker identities.
- The managed vision worker also proves API health reachability.
- A readiness failure activates the existing deployment failure/rollback path.
- The probe has no database, queue, publication, or external-platform side
  effects.
- Production placement remains restricted to hosts 127 and 150.
