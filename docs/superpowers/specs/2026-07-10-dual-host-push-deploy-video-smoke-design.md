# Dual-Host Push Deploy And Video Smoke Design

## Context

The intended VideoProcess production topology is not a standalone Compose stack on one Mac:

- `10.0.0.150` is the Docker Swarm manager and shared support host. It owns Postgres, VideoProcess Redis, MinIO, Qdrant, Redpanda, embedding, browser/account infrastructure, and GPU capacity.
- `10.0.0.127` is the primary VideoProcess runtime. It should run the frontend, APIs, Go FFmpeg worker, PDS, feature aggregator, and related application services through its Colima/Swarm node.
- `10.0.0.126` is the ForWin and news/embedding runtime. It has an auxiliary VideoProcess checkout but is not the designed automatic VideoProcess backup.

The live state has drifted from that design. The 127 `swarmbridged` Colima instance is absent. The only application Swarm node is the 126 VM at `10.0.0.121`, and every VideoProcess service uses the broad constraint `node.labels.role==app`, so VideoProcess has landed on 126. The 127 public entry points refuse connections.

The 150 deploy-sync job is also disabled. Its last VideoProcess attempt copied commit `c4b760e` to 127, then failed while building because the 127 Docker socket and Lima instance did not exist. The installed deploy script currently defaults VideoProcess health checks to 126 and does not rebuild/update the Go FFmpeg worker or the Python GPU worker as part of `vp-app`.

## Goals

- Restore the intended 150 support/control plus 127 VideoProcess runtime topology.
- Keep 126 isolated to ForWin and news during normal operation.
- Make a push to `main` deploy VideoProcess through a scoped 150 deploy-sync job without re-enabling unrelated ForWin automation.
- Deploy all runtime images affected by this repository, including the Python API image, Go API, frontend, Go ChannelOps runner, Go FFmpeg worker, and the managed Python worker image on 150.
- Prove the deployment with a real `source -> trim -> export` job using 150 Postgres/Redis/MinIO and workers placed on the intended hosts.
- Retain a downloadable MP4 and machine-readable run evidence.
- Keep all smoke output private; do not start or invoke a public upload.

## Non-Goals

- Do not treat 126 as an automatic VideoProcess failover node in this phase.
- Do not redeploy or change ForWin/news services on 126.
- Do not enable autonomous public publication.
- Do not delete shared Postgres, Redis, MinIO, or their data.
- Do not rely on the stale host-native worker under `~/Constructure/services/vp-worker`.
- Do not claim high availability or long-term production readiness from one recovery and smoke run.

## Options Considered

### 1. Restore the intended dual-host path (selected)

Use 150 as the manager, shared support host, deploy controller, and GPU-worker host. Recreate the 127 Colima worker node with an explicit VideoProcess label and constrain VideoProcess application services to it. Keep the existing GitHub-polling deploy controller and re-enable only a VideoProcess-scoped schedule after a successful manual deployment.

This follows the checked-in topology and avoids adding host SSH credentials to GitHub Actions.

### 2. Keep VideoProcess on 126

The current services are reachable through the 126 VM, so this is the shortest temporary path. It mixes VideoProcess with ForWin/news, makes the documented 127 entry points false, and leaves deploy builds pointed at a missing 127 Docker engine. It is not selected.

### 3. Add GitHub Actions that SSH directly to both hosts

This could react immediately to pushes, but it duplicates the existing deploy-sync state machine, rollback logic, and host knowledge while introducing another secret boundary. It is not selected.

## Target Distribution

### Host 150: support, control, and managed Python worker

- Docker Swarm manager and deployment controller.
- Shared Postgres `:5435`, VideoProcess Redis `:6380`, MinIO `:9000`, Qdrant, Redpanda, embedding, and browser/account services.
- GitHub mirror and scoped deploy-sync schedule.
- Managed `vp-ffmpeg-worker-gpu-swarm` Python worker constrained by `node.labels.vp.gpu==true`.
- No app-local replacement Postgres, Redis, or MinIO.

### Host 127: primary VideoProcess runtime

- Colima profile `swarmbridged` with VM hostname `colima-127` and a reachable LAN address.
- Swarm node label `vp.runtime=true`.
- `vp-api-swarm`.
- `vp-autoflow-api-swarm`.
- `vp-frontend-swarm`.
- `vp-channel-agent-runner-swarm`.
- `vp-event-outbox-relay-swarm`.
- `vp-feature-aggregator-swarm`.
- `vp-ffmpeg-worker-go-swarm`.
- `vp-pds-swarm`.
- Host-forwarded frontend and API entry points on `10.0.0.127:3001` and `10.0.0.127:18080`.

### Host 126: separate application node

- Continue running ForWin and news/embedding workloads.
- Do not carry `vp.runtime=true`.
- Do not receive VideoProcess services during normal scheduling.
- Do not participate in normal VideoProcess builds, deploys, health gates, or automatic failover.
- Its auxiliary VideoProcess checkout is not a deployment target or automatic failover contract.
- A future cold-standby design would require explicit image distribution, placement switching, endpoint rerouting, capacity checks, and a tested failback procedure.

## Push-To-Deploy Flow

```text
developer push to GitHub main
          |
          v
150 scoped deploy-sync poll
          |
          +--> fetch exact commit and stage source
          +--> sync runtime source to 127
          +--> build 127-consumed images on 127
          +--> build Python worker image on 150
          +--> update/create Swarm services with host-specific constraints
          +--> wait for service convergence and health
          +--> write commit markers and deployment state
          v
127 serves frontend/API and processes Go media work
150 serves shared state/artifacts and Python media work in CPU mode
```

The first deployment remains manual and scoped:

```bash
/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh \
  --apply --force \
  --project vp-app --project vp-feature-aggregator --project vp-pds
```

Only after that succeeds is a dedicated cron entry enabled for the same projects. The previously disabled all-project cron remains disabled so this work cannot redeploy ForWin.

## Node Recovery And Placement

The 127 Colima profile is recreated with persisted CPU, memory, disk, VM hostname, bridged networking, and SSH port forwarding settings. It joins the 150 Swarm as a worker. The manager applies these labels:

- 150: `vp.gpu=true`.
- 127 node: `vp.runtime=true`.
- 126 node: no `vp.runtime` label.

VideoProcess application services replace `node.labels.role==app` with `node.labels.vp.runtime==true`. The managed Python worker uses `node.labels.vp.gpu==true` as a 150 placement label only; it does not claim that Swarm GPU device allocation is ready. This prevents opportunistic scheduling onto 126.

A user LaunchAgent on 127 starts the named Colima profile after login. Swarm restart policies remain responsible for restarting containers after the node returns.

## Deploy-Sync Coverage

The `vp-app` deployment must build and update these image/service relationships:

| Image | Build host | Services |
| --- | --- | --- |
| Go API | 127 | `vp-api-swarm` |
| Frontend | 127 | `vp-frontend-swarm` |
| Python backend API | 127 | `vp-autoflow-api-swarm`, `vp-event-outbox-relay-swarm` |
| Go ChannelOps runner | 127 | `vp-channel-agent-runner-swarm` |
| Go FFmpeg worker | 127 | `vp-ffmpeg-worker-go-swarm` |
| Python worker | 150 | `vp-ffmpeg-worker-gpu-swarm` |

The Go API service explicitly enables `VP_GO_ORCHESTRATOR_ENABLED` and `VP_GO_ORCHESTRATOR_JOB_WRITES`. These match the repository Compose defaults and make the documented production `/api/v1/jobs` entry point operational for Go-eligible, validated pipelines.

The 150 Python worker receives explicit production Postgres, Redis, MinIO, bucket, worker-host, and CPU-safe video settings. Database and MinIO credentials have no source-controlled fallback; the deploy fails before service mutation unless all required settings are present. Platform-publication credentials are deliberately absent until a separate worker and explicit human-review gate exist. GPU processing remains disabled until Swarm service-level device allocation and task-level verification are configured. The worker-admission guard must pass before it opens Redis or Postgres. Any legacy standalone Python FFmpeg process remains stopped.

The deploy controller records the exact Git commit only after builds, service updates, and health gates succeed. A failure leaves diagnostic logs and does not advance the deployment marker. Before mutation, the extension snapshots current images. On failure it restores those images while retaining `vp.runtime`/`vp.gpu` placement, and removes a Python worker created by the failed attempt. It never invokes generic Swarm spec rollback because that could restore `node.labels.role==app` and move work back to 126.

## Real Video Smoke

The smoke uses the production entry point without invoking any publication node:

1. Generate a short H.264/AAC source clip with FFmpeg test sources.
2. Upload it through the 127 API.
3. Create a validated `source -> trim -> export` pipeline.
4. Submit the job and wait for a terminal state.
5. Require all nodes to succeed and require media nodes to report the expected managed worker identity.
6. Download the final artifact from shared MinIO through the API.
7. Verify the MP4 with `ffprobe`, including a video stream and positive duration.
8. Persist MP4 plus JSON evidence containing commit, deployment marker, asset, pipeline, job, artifact, worker, probe, and SHA-256 values.

## Health And Long-Running Checks

- 150 reports both `ccttww-lap` and `colima-127` as Ready.
- Every normal VideoProcess service is `1/1`; the feature aggregator is not left at `0/1`.
- `docker service ps` shows application services on `colima-127` and the CPU-mode Python worker on the 150 node.
- 127 frontend and API host forwards answer on their documented ports.
- Redis consumer groups have no unexpected active Python FFmpeg consumer and no pending message belonging to the smoke node executions.
- The deployed source marker equals the pushed commit.
- Restarting API and worker services preserves the completed job and artifact.
- A later timed soak can measure long-duration reliability; it is not implied by this initial run.

## Failure Handling

- If 127 Colima cannot start or join Swarm, stop before moving service placement.
- If an image cannot be built on its target node, do not update the corresponding service.
- If a service cannot converge on its intended node, retain the current service and collect task logs; do not silently fall back to 126.
- If 127 is unavailable, fail the VideoProcess deployment closed instead of scheduling or building on 126.
- If the scoped manual deployment fails, keep the VP cron disabled.
- If the video job fails, preserve job payload, service logs, Redis state, and artifacts for systematic diagnosis.
- If the 150 Python worker fails admission, it must remain outside the production queue.

## Testing And Evidence

- Backend pytest, targeted Ruff, and Go test suites run before push.
- Deployment script syntax and contract checks cover image list, service list, placement labels, 127 health endpoint, and scoped project selection.
- A dry run proves the exact Git commit and target paths without updating services.
- The first scoped apply is captured in a dedicated log.
- Swarm node/service placement, host endpoints, deployment markers, Redis consumers, and the real video evidence are checked after deployment.
- No public platform API is called.

## Success Criteria

- `main` is pushed and the 150 mirror observes the exact commit.
- 127 is a Ready Swarm node named `colima-127` with `vp.runtime=true`.
- Normal VideoProcess services run on 127, not 126.
- The managed Python worker runs on 150 with admission-approved CPU-mode configuration.
- The VP-only deploy schedule is enabled while the all-project schedule remains disabled.
- A real pipeline produces a retained, playable MP4 through 127 workers and 150 shared storage.
- The completed job and artifact remain readable after service restart.
