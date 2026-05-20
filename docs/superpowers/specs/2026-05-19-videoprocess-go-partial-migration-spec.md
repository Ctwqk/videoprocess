# VideoProcess 渐进式 Go 迁移 Spec

> Status: Draft v0.2  
> Date: 2026-05-19  
> Scope: VideoProcess monorepo 的部分 Go 化迁移，不做全量重写  
> Suggested repo path: `docs/superpowers/specs/2026-05-19-videoprocess-go-partial-migration-spec.md`

---

## 1. Summary

VideoProcess 当前已经不是“准备引入 Go”的状态，而是已经有 Go 迁移雏形：

- 根目录已有 `go.mod`。
- 已有 `cmd/vp-api` 和 `cmd/vp-ffmpeg-worker` 两个 Go binary 入口。
- `docker-compose.yml` 已经定义 `api-go` 和 `ffmpeg-worker-go` sidecar 服务。
- Go API 已实现部分 read-only API。
- Go worker 已实现 Redis Streams consumer 框架、`ffmpeg` runner、编码参数生成、`trim` path-level handler。
- Python FastAPI API、Python orchestrator、Python worker 仍然是完整参考实现和回滚路径。

本 spec 的方向是：

```text
不是把 VideoProcess 全量转成 Go，
而是把 API control-plane 的稳定部分和 pure ffmpeg worker path 渐进迁到 Go。
```

第一阶段应把现有 Go 雏形补齐到“可并行、可灰度、可回滚、可对比”的 sidecar，而不是直接替换 Python API 或 Python worker。

---

## 2. Code Baseline

### 2.1 Monorepo 当前结构

VideoProcess 是多服务 media workflow monorepo，当前主要组件包括：

```text
backend/                 Python FastAPI API、orchestrator、models、schemas、services
frontend/                React + TypeScript + Vite UI
PlatformBrowserManager/  Browser/profile automation
YouTubeManager/          YouTube 管理服务
FasterWhisper/           transcription experiments
TextToAudio/             TTS/XTTS service
voice_chat_bot/          streaming voice chat experiments
cmd/                     Go binary entrypoints
internal/                Go internal packages
deploy/                  部署文档
docker-compose.yml       本地/sidecar 服务拓扑
```

当前前端 API client 使用固定 base URL：

```text
/api/v1
```

因此 Go API 迁移不得要求前端改 base URL。Go API 必须保持 `/api/v1` 路由、状态码和 JSON shape 兼容。

### 2.2 Python API 是参考实现

`backend/app/main.py` 当前负责：

- 创建 FastAPI app。
- 注册 CORS。
- 注册 node types、autoflow、channel agent、pipelines、assets、artifacts、jobs、llm、materials、internal schedule routes。
- 启动 orchestrator event listener。
- 启动 stale-job recovery。
- 暴露 `/health`。

Go API 第一阶段不能替代这些完整职责。它应先实现低风险 read-only API 和少量确定性写 API。

### 2.3 Python orchestrator 是任务调度参考实现

`backend/app/orchestrator/engine.py` 当前负责：

- 从 pipeline DAG 生成 execution plan。
- 解析 source node。
- 根据 dependency map 派发 ready nodes。
- 根据 `NodeTypeRegistry` 中的 `worker_type` 选择 Redis stream。
- 写入 Redis Streams task payload。
- 监听 worker event。
- 更新 `Job` / `NodeExecution` 状态。
- 失败重试。
- downstream skip。
- final artifact 标记。
- artifact cache。

Go worker 初期应接入 Python orchestrator，而不是同时迁移 orchestrator。

### 2.4 Python worker 是 worker contract 参考实现

`backend/worker/main.py` 当前 worker 行为包括：

- `WORKER_TYPE` 默认 `ffmpeg`。
- 消费 `vp:tasks:{WORKER_TYPE}`。
- 使用 consumer group `{WORKER_TYPE}-workers`。
- 解析 task payload。
- 检查取消状态。
- 将 node execution 置为 `RUNNING`。
- 解析 input artifacts。
- 支持 local storage 和 MinIO 下载到临时文件。
- 执行 handler。
- 创建 artifact row。
- 写 artifact storage。
- 发送 `node_completed` 或 `node_failed` 到 `vp:events`。
- PEL reclaim。
- heartbeat。
- host affinity defer。
- concurrency semaphore。

Go worker cutover 必须以这些行为为 contract。

### 2.5 当前 Go module

当前 Go module：

```text
module github.com/Ctwqk/videoprocess
go 1.25
```

直接依赖包括：

```text
github.com/go-chi/chi/v5
github.com/jackc/pgx/v5
github.com/redis/go-redis/v9
github.com/minio/minio-go/v7
github.com/alicebob/miniredis/v2
```

这与当前迁移方向匹配：

- `chi`：HTTP router。
- `pgx`：Postgres。
- `go-redis`：Redis Streams。
- `minio-go`：object storage。
- `miniredis`：worker unit tests。

### 2.6 当前 Go API 状态

Go API 当前入口是：

```text
cmd/vp-api/main.go
```

现状：

- 加载 `internal/config`。
- 打开 `internal/store`。
- DB 不可用时降级为 stub list endpoints。
- 启动 `internal/httpapi.Server`。
- 支持 graceful shutdown。
- HTTP server 设置 `ReadHeaderTimeout`。

当前 Go HTTP routes：

```text
GET /health
GET /api/v1/node-types
GET /api/v1/node-types/{typeName}
GET /api/v1/pipelines
GET /api/v1/templates
GET /api/v1/assets
GET /api/v1/jobs
GET /internal/schedule/video/status
```

当前 Go API 特征：

- `Server` 的 store 可为 nil。
- store 为 nil 时 list endpoints 返回 `{ "items": [], "total": 0 }`。
- `node-types` 使用 Go 内建 registry。
- `pipelines/jobs/assets` 通过 `internal/store` 直接查询 Postgres。
- `scheduleStatus` 当前固定返回 `{ "state": "OPEN" }`。

这些实现适合作为 read-only API sidecar 的基础，但还不能作为生产替代 API。

### 2.7 当前 Go store 状态

`internal/store/store.go` 当前实现：

- `Open(ctx, databaseURL)` 创建 `pgxpool` 并 ping。
- `ListPipelines`
- `ListJobs`
- `ListAssets`
- `CountByQuery`
- `uuidString`

当前 row struct 注释明确说明要 mirror Python schema：

- `PipelineRow` mirrors `backend/app/schemas/pipeline.py PipelineResponse`
- `JobRow` mirrors `backend/app/schemas/job.py JobResponse`
- `AssetRow` mirrors `backend/app/schemas/asset.py AssetResponse`

这是正确方向。后续 Go DB 层应继续保持“显式 query + Python schema parity”，不要直接引入新的 schema ownership。

### 2.8 当前 Go pipeline 状态

`internal/pipeline` 当前包括：

```text
registry.go
validate.go
validate_test.go
```

当前 Go registry 只包含：

```text
source
trim
transcode
export
smart_trim
```

Python builtin registry 包含更多 node：

```text
source
concat_horizontal
concat_vertical
concat_timeline
concat_many
montage_assembler
concat_vertical_timeline
trim
watermark
subtitle
speech_to_subtitle
smart_trim
subtitle_translate
subtitle_to_speech
bgm
replace_audio
transcode
title_overlay
url_download
vertical_crop
material_library_ingest
material_search
youtube_search
x_search
xiaohongshu_search
bilibili_search
zip_records
export
youtube_upload
x_upload
```

当前 Go validator 已实现：

- unknown node type。
- invalid edge。
- port type mismatch。
- duplicate input port。
- cycle detection。
- missing required input。
- source asset binding check。

但 validator 注释中已经说明 AutoFlow-specific shapes 尚未完整实现。Go API 不应在 AutoFlow routes 中使用 Go validator 替换 Python validator，直到 parity 完成。

### 2.9 当前 Go worker 状态

Go worker 当前入口是：

```text
cmd/vp-ffmpeg-worker/main.go
```

现状：

- 加载 worker config。
- 默认 worker type 为 `ffmpeg_go`。
- 解析 Redis URL。
- 创建 `worker.Consumer`。
- 目前 main 中没有注册任何 handler。

当前 `internal/worker` 已实现：

- `DefaultWorkerType() -> "ffmpeg_go"`。
- worker id 格式：`<worker_type>-worker@<host>:<pid>`。
- Redis stream：`vp:tasks:{worker_type}`。
- consumer group：`{worker_type}-workers`。
- `TaskMessage` 对应 Python task payload。
- `XGroupCreateMkStream`。
- `XReadGroup`。
- 成功后发送 `node_completed`。
- 失败后发送 `node_failed`。
- 未注册 handler 时发送 failure 并 ack。
- cancellation 当前设计为不 ack、不发 event。
- task payload 中 `config`、`input_artifacts`、`preferred_hosts` 做 JSON decode。

当前 `internal/redisstream` 已定义：

```text
EventStream = "vp:events"
TaskStream(workerType) = "vp:tasks:" + workerType
```

当前 Go worker 还缺：

- task-level handler 注册。
- NodeExecution / Job 状态更新。
- artifact input resolution。
- local/MinIO storage path resolution。
- artifact row 创建。
- output artifact id 回传。
- heartbeat。
- PEL reclaim。
- affinity defer。
- cancellation contract 与 Python 行为对齐。
- concurrency。
- handler result metadata。
- temp file cleanup。
- production batch processing。

### 2.10 当前 Go ffmpeg 状态

`internal/worker/ffmpeg` 当前实现：

- `Runner.Run(ctx, args)`。
- context cancellation detection。
- stderr tail。
- GPU capacity error detection。
- hardware args CPU fallback rewrite。
- `VideoEncodeArgs`。
- NVENC / VideoToolbox / CPU codec selection。

`internal/worker/handlers/trim.go` 当前实现：

- `TrimHandler.Args(inputPath, outputPath, config)`。
- `TrimHandler.Execute(ctx, inputPath, outputPath, config)`。
- 默认使用 `libx264`、`aac`、`yuv420p`、`+faststart`。

但它不是 `worker.Handler`，因为当前 `worker.Handler` 接口是：

```go
type Handler interface {
    NodeType() string
    Execute(ctx context.Context, task TaskMessage) error
}
```

而 `TrimHandler` 当前是 path-level handler：

```go
Execute(ctx, inputPath, outputPath, config) error
```

因此需要一个 task-level adapter/runtime，把 Redis task 转为 input/output path，再调用 path-level ffmpeg handler。

---

## 3. Goals

### 3.1 Primary Goals

1. **保留 Python 作为 reference implementation 和 rollback path。**

2. **把 Go API 从 smoke sidecar 提升为 read-only parity sidecar。**

3. **把 Go ffmpeg worker 从 Redis consumer skeleton 提升为可执行 `trim` 的 production-grade worker。**

4. **通过 `ffmpeg_go` worker type 实现 node-by-node cutover。**

5. **不改变前端 `/api/v1` contract。**

6. **不改变 Postgres schema 和 Alembic ownership。**

7. **不改变 Redis stream task/event contract。**

8. **不改变 storage path contract。**

9. **引入可对比、可灰度、可回滚的迁移 gate。**

### 3.2 Non-Goals

第一阶段不做：

- 全量 Go 重写。
- 前端迁移。
- Alembic 替换为 Go migrations。
- Postgres schema redesign。
- Python API 全替换。
- AutoFlow graph planner Go rewrite。
- LLM prompt rewriting Go rewrite。
- `smart_trim` Go rewrite。
- `speech_to_subtitle` Go rewrite。
- `subtitle_to_speech` Go rewrite。
- FasterWhisper / CTranslate2 / XTTS 路径 Go rewrite。
- YouTube/X/小红书 upload handler Go rewrite。
- 外部平台发布策略变更。
- public publishing 默认行为变更。

---

## 4. Migration Principle

### 4.1 Strangler Sidecar

Go 服务只作为 sidecar 逐步承接一部分路径：

```text
Frontend
  |
  | /api/v1
  v
Routing / proxy / compose side port
  |
  +--> Python API    reference + fallback
  |
  +--> Go API        read-only parity first

Python Orchestrator
  |
  | Redis Streams
  v
vp:tasks:ffmpeg       -> Python ffmpeg worker
vp:tasks:ffmpeg_go    -> Go ffmpeg worker
vp:tasks:vision       -> Python vision worker

Workers
  |
  | vp:events
  v
Python event listener initially
```

### 4.2 Contract First

任何迁移前必须先明确 contract：

- HTTP route contract。
- JSON field contract。
- DB enum contract。
- Redis task payload contract。
- Redis event payload contract。
- Storage path contract。
- Artifact metadata contract。
- Cancellation/ack contract。
- Retry/PEL/heartbeat contract。

### 4.3 One Node At A Time

Go worker 使用 `ffmpeg_go`，不能消费 `ffmpeg` stream。切换通过 Python node registry 的 `worker_type` 逐个 node 修改：

```text
trim: ffmpeg -> ffmpeg_go
```

每次只切一个 node type。通过后再切下一个。

### 4.4 Python Keeps Ownership Until Parity

第一阶段：

- Python API owns writes。
- Python orchestrator owns scheduling。
- Python event listener owns job state transitions。
- Python Alembic owns schema。
- Go worker only executes selected pure ffmpeg tasks and emits events.

---

## 5. Target Scope By Component

### 5.1 Go API Scope

#### In Scope: Batch A

```text
GET /health
GET /readyz
GET /api/v1/node-types
GET /api/v1/node-types/{type_name}
GET /api/v1/pipelines
GET /api/v1/templates
GET /api/v1/assets
GET /api/v1/jobs
GET /internal/schedule/video/status
```

Notes:

- `/health` 保持现有 shape：`{"status":"ok"}`。
- 新增 `/readyz`，用于 DB/Redis/storage readiness。
- `/internal/schedule/video/status` 不得继续固定 `OPEN`；必须从现有 schedule state source 读取，或在 production 标记为 not implemented/fallback to Python。

#### In Scope: Batch B

```text
GET /api/v1/pipelines/{pipeline_id}
GET /api/v1/assets/{asset_id}
GET /api/v1/artifacts/{artifact_id}
GET /api/v1/jobs/{job_id}
POST /api/v1/pipelines/validate
```

Notes:

- `POST /api/v1/pipelines/validate` 只有在 Go validator 与 Python validator fixtures parity 后才能用于生产。
- 若 Go validator 不支持 AutoFlow shape，必须 fallback to Python validator 或返回 explicit unsupported，不得 silently accept。

#### In Scope: Batch C

仅在 Batch A/B parity 完成后考虑：

```text
pipeline create/update/delete/duplicate
job create/cancel/rerun/delete
asset upload/delete/download
artifact download/cleanup
schedule open/drain/close
```

这些是 write APIs，必须有 contract tests、mixed-mode tests 和 rollback plan。

#### Out of Scope Initially

```text
/api/v1/autoflow/*
/api/v1/channel-agent/*
/api/v1/llm/*
/api/v1/materials/* if backed by search/vector/LLM-heavy behavior
external upload/publish routes
```

### 5.2 Go Worker Scope

#### First Go Worker Node

```text
trim
```

Rationale:

- 已有 path-level `TrimHandler`。
- 纯 ffmpeg。
- 输入/输出清晰。
- 可以用小 fixture mp4 做可视 smoke。
- 不涉及外部平台、LLM、ASR、TTS。

#### First Wave Pure ffmpeg Nodes

```text
trim
transcode
export
vertical_crop
concat_horizontal
concat_vertical
concat_many
concat_timeline
concat_vertical_timeline
title_overlay
watermark
bgm
replace_audio
montage_assembler
```

#### Keep Python In First Wave

```text
smart_trim
speech_to_subtitle
subtitle_translate
subtitle_to_speech
url_download
material_library_ingest
youtube_upload
x_upload
xiaohongshu_upload
material_search
youtube_search
x_search
xiaohongshu_search
bilibili_search
zip_records
```

Rationale:

- ML / ASR / TTS / LLM / platform automation / search / external publish / dynamic planner paths should stay Python until separate specs exist.

---

## 6. Required Design Decisions

### 6.1 DB Failure Behavior In Go API

Current behavior:

```text
DB unavailable -> serve stub list endpoints
```

Required behavior:

```text
dev/test: stub mode allowed
production: DB unavailable must fail readiness
production: read endpoints must not return empty pages as if data is real
```

Add config:

```text
VP_API_GO_ALLOW_STUB_STORE=false
```

Rules:

- Default false in production.
- True only in tests/dev smoke.
- `/health` can remain ok.
- `/readyz` must fail if DB is required and unavailable.

### 6.2 Cancellation Ack Semantics

Current Python behavior:

- Known cancellation does not emit failure.
- Process task returns.
- Message is acked by `_process_message`.

Current Go test expectation:

- `ffmpeg.ErrCancelled` leaves message pending and emits no event.

This is a contract mismatch.

Recommendation:

```text
Match Python for confirmed cancellation:
- no node_completed event
- no node_failed event
- ack task after cancellation is confirmed in DB
```

No-ack should be reserved for process crash or unknown state. If Go keeps cancelled tasks pending, PEL reclaim may keep reprocessing intentionally cancelled nodes.

Spec requirement:

- Add explicit test comparing Python cancellation semantics and Go cancellation semantics.
- Do not cut over any node to `ffmpeg_go` until this decision is resolved.
- Preferred implementation: Go worker loads Job/NodeExecution cancellation state before and during execution; if cancelled, kill ffmpeg, cleanup temp files, ack, emit no event.

### 6.3 Output Artifact Contract

Current Python worker:

- creates artifact row;
- gets `artifact.id`;
- emits `node_completed` with `output_artifact_id`.

Current Go worker:

- `publishCompleted` currently emits `output_artifact_id` as empty string.

Required:

```text
Go worker must not publish node_completed for real jobs until it has created an artifact row and has a non-empty output_artifact_id.
```

Rules:

- Empty `output_artifact_id` allowed only in unit tests with explicit test fixture mode.
- Production worker must fail task if artifact creation fails.
- `node_completed` event must include `output_artifact_id`.

### 6.4 Handler Interface Split

Keep two layers:

```go
// Task-level runtime interface
type TaskHandler interface {
    NodeType() string
    Execute(ctx context.Context, env RuntimeEnv, task TaskMessage) (NodeResult, error)
}

// Path-level pure media interface
type MediaHandler interface {
    Args(inputPath string, outputPath string, config map[string]any) []string
    Execute(ctx context.Context, inputPath string, outputPath string, config map[string]any) error
}
```

`TrimHandler` should remain path-level. Add an adapter:

```text
worker runtime:
Redis TaskMessage
  -> load DB node/job
  -> resolve input artifact IDs
  -> resolve input local paths
  -> allocate output path
  -> call path-level handler
  -> save/upload output
  -> create artifact row
  -> emit event
```

### 6.5 Node Registry Parity

Go registry currently contains only five node types. That is acceptable for `node-types` smoke but not for real `/api/v1/node-types` replacement.

Required:

- Either generate Go node registry from Python builtin registry/capability manifest, or
- keep `/api/v1/node-types` routed to Python until Go registry parity is complete.

Recommended short-term:

```text
Go API can serve /api/v1/node-types only in dev/staging until parity test confirms same set and compatible shape.
```

### 6.6 Schedule Status

Current Go route returns constant:

```json
{"state":"OPEN"}
```

Required:

- Read schedule state from existing DB/settings source, or
- route `/internal/schedule/video/status` to Python, or
- mark route unsupported in production.

A fixed `OPEN` state can break drain/close semantics.

---

## 7. Go API Detailed Spec

### 7.1 Entry Point

File:

```text
cmd/vp-api/main.go
```

Required responsibilities:

- Load config.
- Initialize logger.
- Open Postgres pool.
- Optionally open Redis client for readiness.
- Optionally initialize storage backend for readiness.
- Build `httpapi.Server`.
- Start HTTP server.
- Handle SIGINT/SIGTERM graceful shutdown.
- Expose `/health` and `/readyz`.

### 7.2 HTTP Middleware

Add:

```text
internal/httpapi/middleware.go
```

Required middleware:

- request id
- structured logging
- panic recovery
- timeout
- max body size for future writes/uploads
- CORS parity if Go becomes frontend-facing
- optional metrics

### 7.3 Error Shape

For migrated FastAPI-compatible endpoints, preserve current FastAPI style unless the route already defines another shape:

```json
{
  "detail": "..."
}
```

Do not introduce a new generic error envelope for existing `/api/v1` routes unless frontend and Python parity tests are updated.

### 7.4 Pagination

Current Go `PageOptions`:

```text
skip default: 0
limit default: 50
limit max: 100
```

This matches Python `Query(default=50, le=100)` style and must be retained.

### 7.5 Store Package Split

Current `internal/store/store.go` should be split after it grows:

```text
internal/store/store.go          pool/open/close/common
internal/store/pipelines.go      pipeline queries
internal/store/assets.go         asset queries
internal/store/artifacts.go      artifact queries
internal/store/jobs.go           job/node execution queries
internal/store/schedule.go       schedule state queries
```

Rules:

- No ORM.
- Use explicit SQL.
- Mirror Python schema names.
- Add tests for JSON shape, nil list handling, and UUID formatting.
- Add context timeout for every query path.

### 7.6 Readiness

Add:

```text
GET /readyz
```

Response examples:

Success:

```json
{
  "status": "ready",
  "postgres": "ok",
  "redis": "ok",
  "storage": "ok"
}
```

Failure:

```json
{
  "status": "not_ready",
  "postgres": "error"
}
```

Status code:

```text
200 if all required dependencies are ready
503 otherwise
```

### 7.7 API Parity Tests

Add tests:

```text
tests/go_migration/test_go_api_read_parity.py
```

Compare Python and Go for:

```text
GET /health
GET /api/v1/node-types
GET /api/v1/pipelines?skip=0&limit=50
GET /api/v1/templates
GET /api/v1/assets
GET /api/v1/jobs
```

Assertions:

- HTTP status same.
- Top-level keys same.
- `items` must be array, not null.
- UUID format same.
- timestamp serialization compatible.
- unknown ID behavior same.
- pagination clamp same.

---

## 8. Go Worker Detailed Spec

### 8.1 Entry Point

File:

```text
cmd/vp-ffmpeg-worker/main.go
```

Current issue:

```text
worker.NewConsumer(client, cfg /* handlers go here as they land */)
```

Required:

```text
- initialize config
- initialize Postgres store
- initialize storage backend
- initialize ffmpeg runner
- register task-level handlers
- run consumer with concurrency
```

For `trim` MVP:

```go
consumer := worker.NewConsumer(
    client,
    cfg,
    worker.NewMediaTaskHandler(
        "trim",
        runtimeEnv,
        handlers.TrimHandler{Runner: ffmpeg.NewRunner()},
    ),
)
```

### 8.2 Runtime Environment

Create:

```text
internal/worker/runtime.go
```

Suggested struct:

```go
type RuntimeEnv struct {
    Store       *store.Store
    Storage     storage.Backend
    LocalRoot   string
    WorkerID    string
    WorkerType  string
    Logger      *slog.Logger
}
```

### 8.3 Task Execution Flow

Required flow for each task:

```text
1. Decode Redis task payload.
2. Load NodeExecution and Job from Postgres.
3. If job/node cancelled, ack and emit no event.
4. Set NodeExecution RUNNING, started_at, worker_id.
5. Resolve input artifact IDs from task.input_artifacts.
6. For each input artifact:
   - load artifact row
   - if local storage: use local path
   - if MinIO: download to temp file
7. Prepare output path:
   artifacts/{job_id}/{node_execution_id}.{ext}
8. Execute handler with cancellable context.
9. Verify output file exists.
10. If storage backend is MinIO, upload output.
11. Create artifact row.
12. Emit node_completed with output_artifact_id.
13. Ack task.
14. Cleanup temp files.
```

Failure flow:

```text
handler error
  -> emit node_failed with error[:2000]
  -> ack task
```

Crash / Redis outage:

```text
do not ack if event publication could not be confirmed
```

### 8.4 Redis Stream Contract

Task stream:

```text
vp:tasks:{worker_type}
```

For Go ffmpeg worker:

```text
vp:tasks:ffmpeg_go
```

Consumer group:

```text
{worker_type}-workers
```

Task payload keys:

```json
{
  "job_id": "...",
  "node_execution_id": "...",
  "node_id": "...",
  "node_type": "...",
  "config": "{}",
  "input_artifacts": "{}",
  "preferred_hosts": "[]",
  "affinity_enqueued_at": "1779120000",
  "affinity_bounces": "0"
}
```

Completion event:

```json
{
  "event": "node_completed",
  "job_id": "...",
  "node_execution_id": "...",
  "output_artifact_id": "..."
}
```

Failure event:

```json
{
  "event": "node_failed",
  "job_id": "...",
  "node_execution_id": "...",
  "error": "..."
}
```

### 8.5 PEL Reclaim

Go worker must implement Python-equivalent PEL reclaim before cutover.

Config:

```text
WORKER_PEL_MIN_IDLE_MS=900000
WORKER_PEL_RECLAIM_INTERVAL_SECONDS=60
```

Behavior:

- On startup, reclaim stale pending entries.
- Periodically reclaim stale pending entries.
- Do not reclaim fresh long-running tasks if heartbeat is active.

### 8.6 Heartbeat

Go worker must heartbeat long-running task while handler is active.

Config:

```text
WORKER_HEARTBEAT_INTERVAL_SECONDS=15
```

Behavior:

- Use `XCLAIM` to refresh PEL idle time.
- Stop heartbeat on completion/failure/cancel.
- Log but do not immediately fail task on heartbeat warning unless Redis unavailable prevents event publishing.

### 8.7 Host Affinity

Go worker must implement Python-equivalent host affinity before multi-worker cutover.

Config:

```text
WORKER_AFFINITY_WAIT_SECONDS=20
WORKER_AFFINITY_MAX_BOUNCES=6
```

Behavior:

- Parse `preferred_hosts`.
- If current host not preferred and wait/bounce budget not exceeded:
  - re-enqueue same task with incremented `affinity_bounces`;
  - ack current message.
- If budget exceeded, process locally.

### 8.8 Concurrency

Current Go consumer reads one task at a time for deterministic tests. Production must support:

```text
WORKER_CONCURRENCY
```

Rules:

- Use bounded worker pool or semaphore.
- Default 2, matching Python.
- Each in-flight task must have its own heartbeat.
- No unbounded goroutine spawning.
- Graceful shutdown should stop claiming new tasks and let active tasks finish up to timeout.

### 8.9 Artifact Row Creation

Add store methods:

```text
GetArtifact(id)
CreateArtifact(...)
UpdateNodeRunning(...)
```

Required artifact fields:

```text
job_id
node_execution_id
kind = INTERMEDIATE
filename
mime_type
file_size
storage_backend
storage_path
media_info
```

Output filename:

```text
{node_execution_id}.{ext}
```

Output storage path:

```text
artifacts/{job_id}/{node_execution_id}.{ext}
```

For local backend, preserve Python behavior:

```text
artifact.storage_backend = "local"
artifact.storage_path = output_local_path
```

For MinIO backend:

```text
artifact.storage_backend = "minio"
artifact.storage_path = artifacts/{job_id}/{node_execution_id}.{ext}
```

### 8.10 Handler Registration

First task-level handler:

```text
trim
```

Add tests before node registry switch:

```text
internal/worker/handlers/trim_test.go
internal/worker/runtime_test.go
tests/go_migration/test_go_trim_worker_smoke.py
```

Required tests:

- exact ffmpeg args for start/duration.
- exact ffmpeg args for start/end.
- output exists.
- artifact row created.
- event contains non-empty `output_artifact_id`.
- message acked.
- failure event emitted and acked.
- cancellation ack semantics resolved.

---

## 9. Node Registry Cutover Spec

### 9.1 Current Dispatch Mechanism

Python orchestrator chooses stream by:

```text
node_def.worker_type
stream_key = "vp:tasks:{worker_type}"
```

Therefore Go worker cutover requires only node registry worker type change.

### 9.2 Cutover Example

Before:

```text
trim.worker_type = "ffmpeg"
```

After:

```text
trim.worker_type = "ffmpeg_go"
```

### 9.3 Cutover Gate

Before changing any node type:

```text
[ ] Go worker can process task fixture.
[ ] Go worker creates artifact row.
[ ] Go worker emits output_artifact_id.
[ ] Python event listener updates node to SUCCEEDED.
[ ] Python orchestrator dispatches downstream node.
[ ] Visible media output is correct.
[ ] Failure path retries once through Python orchestrator.
[ ] Rollback to ffmpeg tested.
```

### 9.4 Rollback

To rollback a node:

```text
trim.worker_type = "ffmpeg_go" -> "ffmpeg"
stop vp-ffmpeg-worker-go
leave Python ffmpeg-worker running
```

No DB restore should be required.

---

## 10. Storage Spec

### 10.1 Local Storage

Current Go `LocalBackend` is aligned with Python path semantics.

Required:

- Use `LocalPath` for ffmpeg direct input.
- Save output under existing storage root.
- Preserve path format.

### 10.2 MinIO Storage

Current Go `MinIOBackend` supports read/save/exists/delete and returns no local path.

Required worker behavior:

- Download MinIO input object to temp file before ffmpeg.
- Upload output file to MinIO after handler success.
- Cleanup temp files.
- Never pass MinIO object key directly to ffmpeg.

### 10.3 Storage Backend Selection

Use existing config:

```text
STORAGE_BACKEND
STORAGE_LOCAL_ROOT
MINIO_ENDPOINT
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
MINIO_BUCKET
MINIO_SECURE
```

Unknown backend:

- Dev may fallback to local.
- Production should fail readiness.

---

## 11. Pipeline Validation Spec

### 11.1 Required Parity

Go validator must match Python `validate_pipeline()` for migrated API usage.

Test categories:

```text
valid source -> trim -> export
unknown node type
invalid edge source
invalid edge target
port type mismatch
duplicate input port
cycle detected
missing required input
source missing asset
zip_records / dynamic source inputs
AutoFlow generated workflows
```

### 11.2 Routing Rule

Until Go validator supports AutoFlow-specific shapes:

```text
AutoFlow validation stays Python.
Go validator can be used only for fixtures it explicitly supports.
```

### 11.3 Registry Rule

Go `BuiltinRegistry()` must not drift from Python builtins if Go API serves `/api/v1/node-types`.

Recommended:

```text
Generate Go node registry from a shared manifest, not by hand.
```

Interim acceptable:

```text
Route /api/v1/node-types to Python for production.
Use Go route only in dev/staging.
```

---

## 12. Deployment Spec

### 12.1 Existing Compose Services

`docker-compose.yml` already defines:

```text
api
api-go
channel-agent-runner
ffmpeg-worker
ffmpeg-worker-go
vision-worker
youtube-manager
platform-browser-manager
xiaohongshu-browser-manager
frontend
postgres
redis
minio
```

Keep both Python and Go services deployed during migration.

### 12.2 Ports

Current sidecar ports:

```text
Python API: ${API_PORT:-18080}:8080
Go API:     ${API_GO_PORT:-18081}:8080
Frontend:   ${FRONTEND_PORT:-3001}:80
```

Do not repoint frontend to Go globally until API parity is proven.

### 12.3 Routing Options

Option A: manual side port testing:

```text
Python API: http://localhost:18080
Go API:     http://localhost:18081
```

Option B: proxy route split:

```text
GET /api/v1/pipelines  -> Go API
other /api/v1/*        -> Python API
```

Option C: full API proxy to Go only after parity:

```text
/api/v1/* -> Go API
```

Use Option A/B first.

### 12.4 Dockerfile Requirements

Existing Go Dockerfiles already do multi-stage build.

Add:

- non-root user where feasible.
- healthcheck for API container.
- explicit ffmpeg version smoke in worker image.
- `go test` in CI, not in runtime Docker build.

---

## 13. Observability Spec

### 13.1 Logs

Use `log/slog`.

Required fields:

```text
service
version
worker_type
worker_id
job_id
node_execution_id
node_id
node_type
request_id
trace_id
redis_msg_id
```

### 13.2 Metrics

Expose API metrics:

```text
http_requests_total
http_request_duration_seconds
http_request_errors_total
```

Expose worker metrics:

```text
vp_worker_tasks_total
vp_worker_task_duration_seconds
vp_worker_task_failures_total
vp_worker_task_cancellations_total
vp_worker_pending_reclaims_total
vp_worker_heartbeat_failures_total
vp_ffmpeg_runs_total
vp_ffmpeg_failures_total
vp_ffmpeg_gpu_fallbacks_total
```

### 13.3 Health

API:

```text
/health -> process liveness
/readyz -> dependency readiness
```

Worker:

- no HTTP endpoint required for MVP.
- log startup config.
- optionally expose metrics port in later phase.

---

## 14. Testing Spec

### 14.1 Required Commands

Go:

```bash
go test ./...
go vet ./...
```

Python:

```bash
cd backend
python3 -m pytest
```

Frontend unchanged:

```bash
cd frontend
npm run build
```

### 14.2 Go Unit Tests

Required packages:

```text
internal/config
internal/httpapi
internal/store
internal/pipeline
internal/redisstream
internal/storage
internal/worker
internal/worker/ffmpeg
internal/worker/handlers
```

### 14.3 Contract Tests

Add cross-language tests:

```text
backend/tests/test_go_contract_fixtures.py
tests/go_migration/test_go_api_read_parity.py
tests/go_migration/test_go_worker_trim_parity.py
```

### 14.4 Media Fixtures

Add minimal fixture assets:

```text
backend/tests/fixtures/media/sample_3s_720p.mp4
backend/tests/fixtures/media/sample_audio.wav
```

Generate output for:

```text
trim duration 1s
transcode mp4/webm
export
```

Assertions:

- output exists.
- mime type correct.
- file size > 0.
- ffprobe duration within tolerance.
- Python and Go ffmpeg args/outputs comparable where feasible.

### 14.5 Mixed-Mode Tests

Required before cutover:

```text
Python API creates job
Python orchestrator dispatches trim to ffmpeg_go
Go worker executes trim
Go worker emits node_completed
Python event listener marks node SUCCEEDED
Python orchestrator dispatches downstream export or finalizes job
Python API returns job status SUCCEEDED
```

---

## 15. Phase Plan

### Phase 0: Baseline Audit And Gates

Deliverables:

```text
[ ] Record current Go implementation status.
[ ] Confirm existing Go tests pass.
[ ] Confirm Python backend tests pass.
[ ] Confirm docker compose starts Python API, Go API, Python worker, Go worker.
[ ] Add /readyz to Go API.
[ ] Add config flag for stub store behavior.
[ ] Add explicit production fail-closed behavior.
```

Exit criteria:

```text
Go sidecars can run without replacing Python services.
```

### Phase 1: Go API Read-Only Parity

Deliverables:

```text
[ ] Complete GET /api/v1/pipelines/{id}
[ ] Complete GET /api/v1/assets/{id}
[ ] Complete GET /api/v1/artifacts/{id}
[ ] Complete GET /api/v1/jobs/{id}
[ ] Replace fixed schedule OPEN with real state or route fallback.
[ ] Add parity tests comparing Python and Go response shape.
[ ] Add readiness and request logging.
```

Exit criteria:

```text
Selected read-only routes can be routed to Go in staging without frontend changes.
```

### Phase 2: Go Worker Trim MVP

Deliverables:

```text
[ ] Build task-level runtime adapter.
[ ] Register trim handler in cmd/vp-ffmpeg-worker.
[ ] Add DB state updates.
[ ] Resolve input artifacts.
[ ] Create output artifact row.
[ ] Emit non-empty output_artifact_id.
[ ] Implement temp file cleanup.
[ ] Resolve cancellation ack contract.
[ ] Add miniredis + Postgres integration tests.
[ ] Add media smoke test.
```

Exit criteria:

```text
Python orchestrator can dispatch trim to ffmpeg_go and complete a real job.
```

### Phase 3: Worker Production Semantics

Deliverables:

```text
[ ] Implement PEL reclaim.
[ ] Implement heartbeat.
[ ] Implement affinity defer.
[ ] Implement WORKER_CONCURRENCY.
[ ] Implement graceful shutdown for active tasks.
[ ] Add metrics.
[ ] Add failure/retry mixed-mode test.
```

Exit criteria:

```text
Go worker can run in parallel with Python worker without stranding jobs.
```

### Phase 4: First-Wave Pure ffmpeg Nodes

Migration order:

```text
trim
transcode
export
vertical_crop
watermark
title_overlay
bgm
replace_audio
concat_horizontal
concat_vertical
concat_many
concat_timeline
concat_vertical_timeline
montage_assembler
```

Each node requires:

```text
[ ] exact arg unit tests
[ ] media fixture test
[ ] artifact metadata test
[ ] mixed-mode pipeline test
[ ] node registry worker_type switch
[ ] rollback test
```

Exit criteria:

```text
Pure ffmpeg path can run through Go worker; Python remains for ML/platform nodes.
```

### Phase 5: Selective Go API Writes

Only after API read parity and Go worker stability:

```text
pipeline validate
pipeline CRUD
job create/cancel/rerun
asset upload/download
artifact download
```

Exit criteria:

```text
Go API can create a deterministic job that Python or Go workers can execute.
```

### Phase 6: Optional Go Orchestrator Slice

Only after Phase 5:

```text
Go event listener
Go startup recovery
Go job dispatch
Go retry/downstream skip/final artifact marking
```

Rule:

```text
Do not run Python event listener and Go event listener over the same event stream without explicit ownership partitioning.
```

---

## 16. Acceptance Criteria

### MVP Acceptance

```text
[ ] api-go starts in docker compose.
[ ] api-go /health returns {"status":"ok"}.
[ ] api-go /readyz fails when required DB is unavailable.
[ ] api-go read-only endpoints match Python shape for golden fixtures.
[ ] ffmpeg-worker-go starts in docker compose.
[ ] ffmpeg-worker-go consumes only vp:tasks:ffmpeg_go.
[ ] trim task produces output media.
[ ] trim task creates artifact row.
[ ] trim task emits node_completed with output_artifact_id.
[ ] Python event listener completes the job after Go worker event.
[ ] Rollback to Python ffmpeg worker requires only worker_type revert.
```

### Production Cutover Acceptance For One Node

```text
[ ] At least 20 staging jobs complete with node_type routed to ffmpeg_go.
[ ] No pending Redis stream growth after jobs finish.
[ ] No missing output_artifact_id events.
[ ] No artifact rows with missing storage_path.
[ ] p95 task runtime is not worse than Python by agreed threshold.
[ ] Error handling and retry behavior match Python.
[ ] Cancellation test passes.
[ ] Rollback drill passes.
```

---

## 17. Risks And Mitigations

| Risk | Current evidence | Mitigation |
|---|---|---|
| Go API returns fake empty data when DB is down | current `cmd/vp-api` can serve stub endpoints | production fail readiness; stub only in dev/test |
| Go worker publishes completion without artifact id | current `publishCompleted` has empty output artifact comment | require artifact row before completion event |
| Cancellation semantics drift | Python acks known cancellation; Go test expects pending | resolve contract before cutover |
| Go worker lacks PEL reclaim/heartbeat | current Go consumer comments say follow-up | implement before real traffic |
| Node registry drift | Go registry has five nodes; Python has many builtins | generate or parity-test registry before routing `node-types` |
| Schedule route lies | Go returns fixed OPEN | implement real state or route to Python |
| Go handler interface mismatch | `TrimHandler` is path-level, worker needs task-level | add runtime adapter |
| MinIO input not directly ffmpeg-readable | Go MinIO LocalPath returns false | download temp file and cleanup |
| Write APIs break mixed mode | Go store only list queries today | migrate reads first; writes only after contract tests |
| AutoFlow validation incomplete | Go validator notes unsupported AutoFlow shapes | keep AutoFlow validation Python until parity |

---

## 18. Recommended Immediate PRs

### PR 1: Go API Safety Baseline

Files:

```text
cmd/vp-api/main.go
internal/config/config.go
internal/httpapi/health.go
internal/httpapi/router.go
internal/httpapi/readiness.go
internal/httpapi/middleware.go
```

Changes:

```text
[ ] Add /readyz.
[ ] Add VP_API_GO_ALLOW_STUB_STORE.
[ ] Fail readiness when DB unavailable and stub disabled.
[ ] Add request logging.
[ ] Add production-safe error handling.
```

### PR 2: Worker Runtime Adapter

Files:

```text
internal/worker/runtime.go
internal/worker/artifacts.go
internal/store/artifacts.go
internal/store/node_executions.go
internal/worker/handlers/adapter.go
cmd/vp-ffmpeg-worker/main.go
```

Changes:

```text
[ ] Add RuntimeEnv.
[ ] Resolve input artifacts.
[ ] Allocate output path.
[ ] Call TrimHandler.
[ ] Create artifact row.
[ ] Emit output_artifact_id.
[ ] Register trim handler.
```

### PR 3: Worker Contract Completion

Files:

```text
internal/worker/consumer.go
internal/worker/heartbeat.go
internal/worker/reclaim.go
internal/worker/affinity.go
internal/worker/cancel.go
```

Changes:

```text
[ ] Implement heartbeat.
[ ] Implement PEL reclaim.
[ ] Implement affinity defer.
[ ] Resolve cancellation ack behavior.
[ ] Add concurrency.
```

### PR 4: Parity Test Harness

Files:

```text
tests/go_migration/test_go_api_read_parity.py
tests/go_migration/test_go_worker_trim_parity.py
backend/tests/fixtures/media/*
```

Changes:

```text
[ ] Compare Python and Go API shapes.
[ ] Run trim through Python orchestrator + Go worker.
[ ] Assert artifact/event/job status correctness.
```

---

## 19. Final Recommendation

Proceed with the existing Go sidecar direction, but narrow the next milestone:

```text
Milestone: "Go trim worker + read-only API parity"
```

Do not expand to full Go API, Go orchestrator, or AutoFlow rewrite yet.

The correct near-term cut is:

```text
Python API + Python orchestrator stay authoritative.
Go API becomes read-only parity sidecar.
Go worker executes only trim on ffmpeg_go.
Node registry cutover happens one node at a time.
Rollback stays a worker_type revert.
```

This gives VideoProcess practical Go adoption without turning the project into a risky full rewrite.
