# Active Runtime Topology

This file is the fast-entry summary for agents working in `VideoProcess`.

The canonical cluster-wide runtime document lives in:

- [cluster-runtime-map.md](/home/taiwei/k8s-Constructure/k8s-constructure/cluster-runtime-map.md)

Use this file first when you need the short version.
Jump to `cluster-runtime-map.md` when you need the full cluster picture.

## Agent Quick View

- Main host is the control plane and shared state machine.
- Mac 1 runs the offloaded `vp-worker` process.
- Mac 3 runs the news and embedding services, not VideoProcess workers.
- Browser traffic enters through the main host frontend.
- VideoProcess jobs coordinate through API + Redis + MinIO on the main host.
- Remote workers are execution targets, not source-of-truth development checkouts.

## Chinese Quick View

- 主机 `ccttww-lap` 是控制面，也是共享状态所在机器。
- Mac 1 负责跑 `vp-worker`，属于远程执行层。
- Mac 3 不参与 VideoProcess DAG 执行，主要跑新闻和 embedding 相关服务。
- 浏览器先进入主机前端，再经 `/api` 和 `/youtube` 代理访问后端与 YouTube 侧车。
- VideoProcess 的核心依赖 `Postgres`、`Redis`、`MinIO` 都在主机上。
- 改 worker 代码后，除了主机 worker，还要记得把 Mac 1 的 offloaded worker 一起更新。
- Mac 上的 `vp-worker` 运行在 repo-local `.venv` 里；`url_download` 依赖的 `yt-dlp` 也从这套环境解析，不要只假设系统 PATH 正确。

## Active Machines

| Role | Host | Address | Main runtime |
| --- | --- | --- | --- |
| Main host | `ccttww-lap` | `10.0.0.150`, `192.168.20.4` | K3s, frontend, API, worker, Postgres, Redis, MinIO |
| Mac 1 | `Wenjies-Mac-mini.local` | `10.0.0.127` | host-native `vp-worker` |
| Mac 3 | `CASPERs-Mac-mini.local` | `10.0.0.126`, service IP `192.168.20.1` | `embedding-gateway`, `news-server`, `news-collector` |

## Service Distribution

### Main host

- `videoprocess-frontend`
- `videoprocess-api`
- `videoprocess-worker`
- `videoprocess-postgres`
- `videoprocess-redis`
- `videoprocess-minio`
- `qdrant`
- `exo-watchdog`

### Mac 1

- `vp-worker`

### Mac 3

- `embedding-gateway`
- `news-server`
- `news-collector`

## Entry Points And Ports

| Surface | URL / Port | Notes |
| --- | --- | --- |
| Frontend | `http://localhost:3001` | main user-facing entry |
| API | `http://localhost:8080` | FastAPI control plane |
| Frontend dev | `http://localhost:5173` | Vite dev server |
| Frontend -> API | `/api/*` | proxied by frontend |
| Frontend -> YouTube | `/youtube/*` | proxied to youtube sidecar |
| Redis | main host internal | shared queue for job/node execution |
| MinIO | main host internal | artifact/object storage |
| Postgres | main host internal | metadata/job/pipeline state |

## Interaction Model

### Topology at a glance

```text
                        ┌───────────────────────────────┐
                        │ Main host: ccttww-lap         │
                        │ 10.0.0.150 / 192.168.20.4     │
                        │                               │
Browser ───────────────>│ frontend :3001               │
                        │ api :8080                    │
                        │ youtube-manager sidecar      │
                        │ postgres / redis / minio     │
                        │ k8s vp-worker (GPU path)     │
                        │ qdrant / exo-watchdog        │
                        └──────────────┬────────────────┘
                                       │ shared Redis queue
                                       │
                             ┌─────────▼─────────┐
                             │ Mac 1             │
                             │ 10.0.0.127        │
                             │ host-native       │
                             │ vp-worker         │
                             └───────────────────┘

                        ┌───────────────────────────────┐
                        │ Mac 3: CASPERs-Mac-mini.local │
                        │ SSH 10.0.0.126                │
                        │ Service IP 192.168.20.1       │
                        │ embedding-gateway             │
                        │ news-server                   │
                        │ news-collector                │
                        └───────────────────────────────┘
```

### VideoProcess request path

```text
Browser
  -> main host frontend (:3001)
  -> main host API (/api/*)
  -> Postgres / Redis / MinIO on main host
  -> shared Redis queue
  -> worker on main host or Mac 1
  -> artifact output back to MinIO
  -> API status/result back to browser
```

### YouTube path

```text
Browser
  -> main host frontend (:3001)
  -> /youtube/* proxy
  -> youtube-manager sidecar on main host
  -> Google / yt-dlp / upload flow
```

### News and embedding path

```text
Mac 3 apps
  -> local embedding-gateway (127.0.0.1:8080)
  -> main host postgres-news / qdrant / exo-watchdog

K8s services
  -> service shims
  -> Mac 3 service IP (192.168.20.1)
```

## Runtime Boundaries

- K8s lives on the main host.
- The active Macs are not K8s nodes.
- Mac workers are host-native processes deployed remotely.
- Redis is the shared coordination point for VideoProcess execution.
- MinIO is the shared artifact store for VideoProcess.

## Core Dependency Contracts

- API depends on Postgres, Redis, and MinIO on the main host.
- Frontend depends on API proxy and YouTube proxy being correct.
- Workers depend on Redis for task intake and MinIO for artifact handoff.
- Offloaded Mac workers must stay protocol-compatible with the main-host API and queue payload format.
- YouTube flows depend on frontend proxy -> youtube sidecar -> Google / yt-dlp path being intact.

## What To Change Where

### If you changed frontend or API behavior

- Edit in this repo on the main host.
- Validate locally.
- Restart or redeploy the main-host frontend or API workloads.
- Do not redeploy Mac 1 unless queue payloads or worker behavior changed too.

### If you changed worker execution behavior

- Edit in this repo on the main host.
- Validate locally with `make smoke-test`.
- Restart the main-host worker.
- Redeploy the offloaded worker on Mac 1.

### If you changed news or embedding services

- Check `k8s-constructure` cluster docs first.
- Treat Mac 3 as the runtime target for those services.
- Do not assume VideoProcess worker deploy steps apply to Mac 3.

## Task-To-Entry Mapping

| Task type | Start here | Usually touch | Usually redeploy |
| --- | --- | --- | --- |
| Frontend page or proxy issue | `frontend/` | Vite, nginx config, React pages/components | main-host frontend |
| API contract or job orchestration issue | `backend/app/` | FastAPI routes, services, orchestrator | main-host API |
| ffmpeg node execution issue | `backend/worker/` | worker handlers, queue consumer | main-host worker + Mac 1 worker |
| Node schema or editor node catalog issue | `backend/app/node_registry/` and `frontend/src/components/editor/` | node defs, config UI, palette | API and often frontend |
| YouTube login/search/upload issue | `YouTubeManager/` plus frontend `/youtube` callers | auth flow, search, upload, proxy | frontend and youtube sidecar |
| Mac offload deploy issue | `deploy/macos/` and `k8s-constructure` deploy scripts | remote worker rollout logic | offloaded services |
| News / embedding issue | `k8s-constructure` docs first | Mac 3 services and service shims | Mac 3 services |

## Failure Domains

- Main host down:
  - frontend, API, Redis, MinIO, Postgres, and K8s worker all go unavailable
  - Mac workers lose their queue and storage backends
- Mac 1 down:
  - queue still works
  - only offloaded worker capacity drops
- Mac 3 down:
  - news and embedding flows degrade
  - VideoProcess DAG execution should still work
- Redis unhealthy:
  - no new VideoProcess node execution can be coordinated
- MinIO unhealthy:
  - workers may run but artifact handoff and final outputs will fail
- Postgres unhealthy:
  - job state, pipeline metadata, and API control-plane operations fail

## Triage Start Points

### Browser shows stale or wrong UI

- Check frontend endpoint first:
  - `http://localhost:3001`
  - `http://localhost:5173`
- Then confirm the proxy target:
  - `/api/*` should resolve to the main-host API
  - `/youtube/*` should resolve to the main-host youtube sidecar path

### Job is stuck in `RUNNING` or nodes stop advancing

- Check main-host API logs and worker logs first.
- Check Redis health before inspecting node handlers.
- Then confirm the offloaded worker is alive on Mac 1.

### Node fails only on some machines

- Suspect environment drift or missing redeploy on Mac 1.
- Compare main-host worker behavior with remote worker logs.

### Upload/download or result retrieval is broken

- Check MinIO reachability on the main host.
- Then inspect artifact paths and storage metadata.

### YouTube auth or upload looks broken

- Start from frontend `:3001`.
- Confirm `/youtube/api/auth/status` returns JSON.
- Then inspect `YouTubeManager` auth state and credentials on the main host.

### Deploy completed but behavior still looks old

- Suspect that only the main host was restarted.
- Confirm whether the Mac 1 worker was redeployed too.
- For frontend issues, make sure you are not comparing `3001` and `5173` accidentally.

## Repo Ownership

Cluster-level deploy ownership:

- [cluster-runtime-map.md](/home/taiwei/k8s-Constructure/k8s-constructure/cluster-runtime-map.md)
- `/home/taiwei/k8s-Constructure/k8s-constructure/scripts/deploy-offloaded-services.sh`

Repo-local implementation details:

- [README.md](/home/taiwei/Constructure/apps/VideoProcess/deploy/macos/README.md)
- [common.sh](/home/taiwei/Constructure/apps/VideoProcess/deploy/macos/common.sh)
- [deploy_videoprocess_workers.sh](/home/taiwei/Constructure/apps/VideoProcess/deploy/macos/deploy_videoprocess_workers.sh)
- [deploy_videoprocess_everywhere.sh](/home/taiwei/Constructure/apps/VideoProcess/deploy/macos/deploy_videoprocess_everywhere.sh)
- [deploy_news_stack.sh](/home/taiwei/Constructure/apps/VideoProcess/deploy/macos/deploy_news_stack.sh)
- [offload_to_macs.sh](/home/taiwei/Constructure/apps/VideoProcess/deploy/macos/offload_to_macs.sh)

`offload_to_macs.sh` is now only a compatibility wrapper. Normal deploys should start from `~/k8s-Constructure/k8s-constructure`.

## Agent Rules Of Thumb

- When editing VideoProcess worker code, edit on the main host checkout only.
- After worker changes, validate locally first, then redeploy offloaded workers.
- Do not assume remote Macs auto-sync from this repo.
- Do not treat Mac 3 as part of the VideoProcess worker pool.
- If runtime and docs disagree, trust live runtime first and update docs second.

## Common Checks

```bash
curl http://localhost:8080/health
curl http://localhost:3001/api/v1/node-types
curl http://localhost:3001/youtube/api/auth/status
kubectl get pods -A -o wide
ssh wenjie '~/Constructure/services/vp-worker/status.sh'
ssh magi1 '~/Constructure/services/embedding-gateway/status.sh'
```

## Deploy Sequence Cheatsheet

### VideoProcess API or frontend only

1. Edit and validate on the main host.
2. Run `make smoke-test` if the change affects user flow or API behavior.
3. Restart the main-host API or frontend workload.

### VideoProcess worker behavior

1. Edit and validate on the main host.
2. Run `make smoke-test`.
3. Restart the main-host worker.
4. Redeploy offloaded workers from `k8s-constructure`.

### Cross-repo or cross-machine uncertainty

1. Read [cluster-runtime-map.md](/home/taiwei/k8s-Constructure/k8s-constructure/cluster-runtime-map.md).
2. Confirm whether the target is a K8s workload, a sidecar, or a host-native Mac process.
3. Deploy from the owning repo and entrypoint, not from habit.

## Handoff Notes For Agents

- If the task is about queueing, node lifecycle, worker handlers, or ffmpeg behavior, start in `VideoProcess/backend`.
- If the task is about where a service runs, how cross-machine traffic is routed, or which deploy command is authoritative, read `cluster-runtime-map.md`.
- If the task is about why a code change is not visible on Mac workers, assume redeploy is missing before assuming the code is wrong.
- If the task is about YouTube login or upload, remember the browser enters through frontend `:3001`, then `/youtube/*` is proxied to the sidecar on the main host.

## Standard Validation Entry

Run the current repo smoke test from:

```bash
cd /home/taiwei/Constructure/apps/VideoProcess
make smoke-test
```

For cluster-wide placement, rollout order, and cross-repo deploy ownership, read:

- [cluster-runtime-map.md](/home/taiwei/k8s-Constructure/k8s-constructure/cluster-runtime-map.md)
