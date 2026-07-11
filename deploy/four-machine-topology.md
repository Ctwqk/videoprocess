# Active Runtime Topology

This file is the fast-entry summary for agents working in `VideoProcess`.

Status date: 2026-07-11.

## Agent Quick View

- VideoProcess application services run in Docker Swarm on the 127 Colima node.
- User traffic enters through `http://10.0.0.127:3001`.
- API traffic is forwarded through `http://10.0.0.127:18080`.
- Shared Postgres, Redis, MinIO, Qdrant, Redpanda, embedding, dashboard, and
  IBKR infrastructure stay on `10.0.0.150`.
- The managed Python FFmpeg worker runs on 150; normal VP application services
  and the Go FFmpeg worker run on 127.
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

Production updates flow through GitHub and the 150 deploy sync job, then into
Swarm services on 127. If queue payloads, API contracts, or worker behavior
change, redeploy the affected VP services together.

The normal scoped deployment command is:

```bash
/home/taiwei/deploy-github-sync/bin/deploy-github-sync.sh \
  --apply --project vp-app --project vp-feature-aggregator --project vp-pds
```

The first deployment of a commit is run manually and verified. Only then may
the same scoped command run from cron. Do not enable the unscoped all-project
deploy job as part of a VideoProcess release.

## Placement Policy

- Label the 127 Swarm node with `node.labels.vp.runtime == true`.
- Constrain normal VP services to `node.labels.vp.runtime == true`.
- Label the 150 manager with `node.labels.vp.gpu == true` and constrain the
  managed Python worker to that label.
- Do not add `vp.runtime` to the 126 node.
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
