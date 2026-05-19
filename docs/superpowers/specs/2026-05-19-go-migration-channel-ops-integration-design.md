# Go Migration ChannelOps Integration Design

Date: 2026-05-19
Status: Approved for implementation
Current branch: `codex/go-partial-migration`
Merge target: `codex/channel-ops-remaining-sprints`

## 1. Decision

Merge `codex/channel-ops-remaining-sprints` into the completed Go partial
migration branch, then deploy and verify the integrated version through
Docker. The merge must preserve the Go migration boundary: Go owns the
sidecar API readiness/middleware, the Go ffmpeg worker runtime, the Redis
task/artifact contract, and the migrated `ffmpeg_go` trim path. ChannelOps
agent orchestration remains Python-owned unless a specific conflict proves
that a changed behavior now belongs to a Go-owned surface.

The generated verification video will be uploaded to YouTube with `private`
privacy. Public upload is out of scope.

## 2. Context

The Go migration branch has completed the partial migration scope:

- Go API sidecar health/readiness and read-only parity checks.
- Go ffmpeg worker runtime with artifact-result enforcement.
- Python registry cutover for `trim` to `ffmpeg_go`.
- Docker sidecars for `api-go` and `ffmpeg-worker-go`.
- Parity and strict worker smoke tests.

The ChannelOps branch is a Python channel-agent hardening branch. Its expected
changes are in runner, scheduler, service, config, tests, and ChannelOps docs.
Those files are not themselves Go migration targets. The integration should
therefore merge them as Python changes while checking whether any task,
artifact, storage, publish, or worker contract changes must be mirrored into
Go-side validation.

## 3. Merge Boundary

During conflict resolution, classify every conflict by owner:

- **Go-owned:** `cmd/`, `internal/`, Go Dockerfiles, Go worker task streams,
  Go readiness checks, migrated node registry mapping for `trim`.
- **Python-owned:** `backend/app/channel_agent/`, ChannelOps tests,
  AutoFlow/YouTube publication orchestration, scheduler loops, PDS handling.
- **Shared contract:** environment variables, Docker Compose topology,
  Redis stream names, artifact payload shape, storage URLs, node type
  registry behavior.

Only shared-contract conflicts should trigger a Go-side code change. Python
ChannelOps behavior should not be ported to Go in this integration unless it
directly changes a shared contract already consumed by the Go sidecars.

## 4. Spec Path Cleanup

The completed migration spec currently exists under
`docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md`.
Because the requested path is `docs/videoprocess-go-partial-migration-spec.md`,
the integration will add that root docs path as a pointer or copy so future
readers can open the referenced file without knowing the Superpowers docs
layout.

## 5. Deployment Verification

After merge and tests, rebuild/recreate the Docker deployment for this
checkout. Verification must prove:

- Python API and worker services still start.
- Go API readiness is healthy.
- Go ffmpeg worker consumes the migrated `ffmpeg_go` trim stream.
- Redis pending entries are not left stuck for `ffmpeg_go`.
- A real workflow can complete and produce a playable video artifact.

If MinIO or another service has a host-port collision, prefer reusing the
already-running compatible service or overriding only the conflicting host
port. Do not weaken storage/artifact checks to bypass deployment friction.

## 6. Maximal Video Smoke

The smoke workflow should use as many practical nodes, workers, and functions
as the local deployment can execute reliably. It must include the migrated Go
trim path. Prefer generated or local fixture media over external platform
assets unless a live external integration is explicitly required for the
smoke. The final artifact must be inspected before upload.

If a broad workflow fails because a nonessential node depends on unavailable
credentials, GPU hardware, or an external service, narrow that node out and
record the skipped dependency. Do not count the deployment as successful until
the remaining workflow produces a playable video.

## 7. YouTube Upload

Upload the verified artifact to YouTube with `private` privacy. The upload
step may use the repository's existing YouTube integration or the adjacent
YouTubeManager runtime if that is the established operational path.

The report must include the upload status and the resulting YouTube identifier
or URL when available. Public publication, promotion, and privacy escalation
are not part of this task.

## 8. Acceptance Criteria

The task is complete only when all of the following are true:

- The ChannelOps branch is merged into `codex/go-partial-migration`.
- The root migration spec path exists.
- Required backend and Go checks have run, with any unavailable optional tools
  called out explicitly.
- Docker services are rebuilt or recreated from the merged checkout.
- A video workflow completes through Docker and uses `ffmpeg_go` trim.
- The generated artifact is playable.
- The artifact is uploaded to YouTube as private.
- The final report includes merge summary, verification commands, Docker
  service state, artifact path, and YouTube upload result.
