# Active Runtime Topology

This file is the fast-entry summary for agents working in `VideoProcess`.

Status date: 2026-07-11.

## Agent Quick View

- VideoProcess application services run in Docker Swarm on the 127 Colima node.
- User traffic enters through `http://10.0.0.127:3001`.
- API traffic is forwarded through `http://10.0.0.127:18080`.
- Shared Postgres, Redis, MinIO, Qdrant, Redpanda, embedding, dashboard, and
  IBKR infrastructure stay on `10.0.0.150`.
- The managed Python FFmpeg worker, YouTubeManager, and dedicated YouTube
  publisher run on 150; normal VP application services and the Go FFmpeg worker
  run on 127.
- 126 is not a VideoProcess automatic failover target. It remains the
  ForWin/news node during normal VP builds, deploys, and runtime placement.
- The deployment directory on 127 is not the source-of-truth git workspace.
- A clean developer checkout exists on 246 at `/home/kikuhiko/videoprocess`.
  This is the preferred Codex project location for normal VideoProcess changes.
- A clean git checkout also exists on 126 at `/Users/magi1/VideoProcess-app`;
  the 150 repository `/home/taiwei/Constructure-repos/videoprocess` also tracks
  `Ctwqk/videoprocess` but may contain local unpushed work.

## Active Machines

| Role | Host | Address | Main runtime |
| --- | --- | --- | --- |
| Infra / Swarm manager | `ccttww-lap` | `10.0.0.150` | shared data stores, GPU/embedding, Redpanda bridge, dashboard, IBKR |
| ForWin/news node | `CASPERs-Mac-mini.local` | `10.0.0.126` | ForWin and news Swarm tasks |
| VideoProcess node | `Wenjies-Mac-mini.local` | `10.0.0.127` | VP frontend/API/worker, PDS, feature aggregator, arb app services |

## Service Distribution

### `10.0.0.127`

- `vp-frontend-swarm`
- `vp-api-swarm`
- `vp-autoflow-api-swarm`
- `vp-channel-agent-runner-swarm`
- `vp-event-outbox-relay-swarm`
- `vp-ffmpeg-worker-go-swarm`
- `vp-pds-swarm`
- `vp-feature-aggregator-swarm`
- `arb-resolver-swarm` and `arb-validator-swarm` when the arb window is open

Host forwards:

- `10.0.0.127:3001` -> VP frontend
- `10.0.0.127:18080` -> VP API

### `10.0.0.150`

- Swarm manager
- Shared Postgres `10.0.0.150:5435`
- VP Redis `10.0.0.150:6380`
- MinIO `10.0.0.150:9000`
- Qdrant `10.0.0.150:6333/6334`
- Redpanda host bridge `10.0.0.150:19092`, overlay `redpanda:9092`
- Embedding gateway `http://10.0.0.150:8080`
- Managed Python FFmpeg worker (CPU fallback until Swarm GPU allocation is configured)
- YouTubeManager and `vp-youtube-publisher-swarm` for the dedicated unlisted
  publication stream
- Browser/account infrastructure that remains 150-local

## Entry Points And Ports

| Surface | URL / Port | Notes |
| --- | --- | --- |
| Frontend | `http://10.0.0.127:3001` | main user-facing entry |
| API | `http://10.0.0.127:18080` | Go control plane; AutoFlow Python services remain internal |
| Node catalog smoke test | `http://10.0.0.127:18080/api/v1/node-types` | should return JSON list |
| Redpanda | `redpanda:9092` on overlay, `10.0.0.150:19092` from host/LAN | PDS and aggregator event path |
| Redis | `redis://10.0.0.150:6380` or overlay env | queues and scheduling state |
| MinIO | `http://10.0.0.150:9000` or overlay env | artifacts and object storage |
| Postgres | `10.0.0.150:5435` or overlay env | metadata, jobs, pipeline state |

## Request Path

```text
Browser
  -> 10.0.0.127:3001 VP frontend
  -> 10.0.0.127:18080 API
  -> 150 shared Postgres / Redis / MinIO / Qdrant / Redpanda
  -> 127 Swarm workers
  -> artifact output back to 150 MinIO
  -> API status/result back to browser
```

## Source, Deploy, And Codex Project Guidance

Use one of these as a Codex coding project:

- Preferred clean VP checkout: `10.0.0.246:/home/kikuhiko/videoprocess`
- Auxiliary VP checkout (not a runtime/deploy target): `10.0.0.126:/Users/magi1/VideoProcess-app`
- 150 Constructure source repo: `10.0.0.150:/home/taiwei/Constructure-repos/videoprocess`

Do not use `10.0.0.127:/Users/wenjieliu/VideoProcess-app` as a long-lived code
project. It is the deploy output marked by `.deploy-sync-project` and
`.deploy-sync-source-commit`.

Production updates flow through GitHub and the scoped 150 deploy controller,
then into Swarm services on 127. Normal GitHub pushes are pulled and deployed
by the scoped 15-minute cron; do not enable the unscoped all-project deploy
job as part of a VideoProcess release. A manual first deployment is reserved
for exceptional migration or runbook changes, not for every commit.

The scoped controller deploys `vp-app` and the in-repository
`vp-feature-aggregator` project from this repository. PDS remains an
independent repository and deploy project: a PDS change is deployed from its
own repository through the scoped `vp-pds` project, without requiring a
VideoProcess operations-asset rewrite.

The normal scoped deployment command, for exceptional manual deployment or
runbook work, is:

```bash
/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh \
  --apply --project vp-app --project vp-feature-aggregator --project vp-pds
```

## ChannelOps Managed Soak Guard

The scoped deploy controller on `10.0.0.150` owns the ChannelOps soak watcher.
After the VP services converge, it validates and atomically installs the
repository watcher at:

```text
/home/taiwei/deploy-github-sync/bin/channelops-soak-watch.sh
```

The controller owns exactly one marked `VIDEOPROCESS SOAK WATCH` cron block on
150. Its managed command runs every 30 minutes and writes to the controller
log directory:

```text
*/30 * * * * DEPLOY_GITHUB_SYNC_ROOT=/home/taiwei/deploy-github-sync /home/taiwei/deploy-github-sync/bin/channelops-soak-watch.sh >> /home/taiwei/deploy-github-sync/logs/channelops-soak-watch.log 2>&1
```

The watcher is disabled by default. Its only activation state is:

```text
/home/taiwei/deploy-github-sync/state/vp-soak-watch.env
```

When that file is absent, the watcher exits successfully with
`status=disabled reason=state_missing`. A present file is still disabled unless
`VP_SOAK_WATCH_ENABLED=true`; any other value reports
`status=disabled reason=not_enabled`. Invalid state, unavailable credentials,
or an unavailable trusted worker image is a non-zero configuration error and
does not perform a speculative mutation.

The state file is parsed as literal `KEY=value` data using an explicit key
allowlist; it is never sourced as shell code. `VP_SOAK_FORBIDDEN_NODE_PATTERN`
may add placement exclusions, but it cannot replace the immutable
`CASPERs-Mac-mini`, `colima-swarmbridged`, and `10.0.0.126` baseline.

Creating or enabling the state file is a separate human activation action and
requires a separately approved, successful unlisted canary. The required
approval for the next attempt is `批准第三次 unlisted canary`. Deploying code may
replace the watcher and its managed cron entry, but it cannot create activation
state, activate a channel, resume a halted channel, or reopen the video
schedule.

External-platform asset automatic publication remains disabled. Explicit human
review is required before any external-platform asset upload or publication.
Public publication and promotion remain disabled.

Once explicitly enabled, the watcher checks VP service health, Redis consumer
groups, and task placement before running the channel-scoped database guard.
On a critical result with `VP_SOAK_AUTO_HOLD=true`, the guard quarantines only
the configured channel: it halts the channel, holds non-terminal tasks,
cancels non-terminal jobs and nodes, dead-letters runnable queue items, and
sets the global VideoProcess runtime schedule to `CLOSED` in the same
transaction. Its mutations only reduce activity; it never uploads, publishes,
enqueues, resumes, or opens a schedule. A guard trip remains non-zero for cron
visibility.

The 150 controller, watcher, and all normal VP deployment placement remain
strictly scoped to 150 and 127. Host 126 is forbidden for VP builds, deployment,
watcher placement, publisher placement, and automatic failover. A missing 127
runtime fails the VP deployment closed rather than moving work to 126.

## Canary Shared-Service Transport

The canary operator may reach 127 directly while direct TCP routes to the 150
Postgres, Redis, or YouTubeManager ports are unavailable. Use the runner-owned
SSH forwarding mode instead of maintaining separate `ssh -L` processes. After
loading `DATABASE_URL` and `REDIS_URL` from the protected deploy environment
without printing them, the read-only command is:

```bash
PYTHONPATH=backend backend/.venv/bin/python scripts/run_vp_unlisted_canary.py \
  --preflight-only \
  --manager-ssh-jump 10.0.0.127 \
  --shared-services-ssh-host 10.0.0.127
```

The runner forwards only the configured database, Redis, and YouTubeManager
connections. It still reaches the VP API directly on 127, keeps the existing
127-to-150 jump for Swarm checks, and removes the forwarding process on every
exit path. Evidence records logical routing only, not ports or connection URLs.

For an approved live attempt, replace `--preflight-only` with
`--confirm-live-unlisted`. That CLI flag and the SSH transport do not constitute
human approval: the per-attempt approval phrase above must already have been
provided. The live path remains owned-source-only, unlisted, one-task, and
fail-closed.

## Placement Policy

- Label the 127 Swarm node with `node.labels.vp.runtime == true`.
- Constrain normal VP services to `node.labels.vp.runtime == true`.
- Label the 150 manager with `node.labels.vp.gpu == true` and constrain the
  managed Python worker to that label.
- Label only the 150 manager with `node.labels.vp.publisher == true`. Constrain
  `vp-youtube-publisher-swarm` with both that label and
  `node.hostname == ccttww-lap`; the hostname constraint prevents stale labels
  from placing the publisher on another node.
- The publisher attaches to the pipeline network, but its shared VP Postgres,
  Redis, and MinIO settings use the `10.0.0.150` host endpoints. It validates
  YouTubeManager's read-only auth-status route before deployment and publishes
  only through its dedicated unlisted stream. Do not supply OAuth credentials
  through its Swarm environment or mounts.
- Do not add `vp.runtime` to the 126 node.
- Never add `vp.publisher` to the 126 node or treat it as a publisher failover
  target.
- If 127 is unavailable, fail the VP deployment instead of scheduling or
  building on 126.

## Task-To-Entry Mapping

| Task type | Start here | Usually redeploy |
| --- | --- | --- |
| Frontend page/proxy issue | `frontend/` | `vp-frontend-swarm` |
| API or job orchestration issue | `cmd/vp-api/`, `internal/orchestrator/`, and `backend/app/` | `vp-api-swarm` and sometimes `vp-autoflow-api-swarm` |
| Worker/node execution issue | `backend/worker/` and node handlers | `vp-channel-agent-runner-swarm` and sometimes API |
| PDS integration | VP event/outbox code plus `Ctwqk/policy-decision-service` | `vp-pds-swarm`, `vp-event-outbox-relay-swarm` |
| Feature aggregation | VP event schema plus `services/vp-feature-aggregator` | `vp-feature-aggregator-swarm` |
| Browser/platform upload issue | VP platform/browser manager code and 150 browser infra | service-specific; check credentials and browser state |

## Failure Domains

- `10.0.0.150` down: shared data stores, Redpanda, embedding, dashboard, and
  artifact storage are unavailable; VP API/workers lose dependencies.
- `10.0.0.127` down: VP frontend/API/workers and PDS/aggregator are unavailable.
- `10.0.0.126` down: ForWin/news capacity drops, but VP should not lose core
  execution unless a workflow explicitly calls news services.
- `10.0.0.127` down does not authorize automatic failover to 126; recovery
  restores the 127 Colima/Swarm node or follows a separately approved cold
  standby runbook.

## Triage Start Points

```bash
# Browser check
open http://10.0.0.127:3001

# API check
curl http://10.0.0.127:18080/api/v1/node-types

# Swarm placement and health from manager
ssh 10.0.0.150 'docker service ls | grep -E "vp-|pds|feature|redpanda"'

# Confirm VP placement stays off 126
ssh 10.0.0.150 'docker service ps vp-api-swarm vp-ffmpeg-worker-go-swarm'

# Confirm the 127 host forwards
ssh 10.0.0.127 'lsof -nP -iTCP:3001 -iTCP:18080 -sTCP:LISTEN'
```
