# ChannelOps Staging Soak Preflight Spec

> 目标：在启动 staging 7 天 unlisted soak 之前，补齐 Go runner 缺失的运维基础设施。
>
> 本 spec 只覆盖 **soak 启动前的硬阻断**（B1/B2/B3）和 **soak 期间并行补的项**（C1-C4）。Phase 0/A/B/C 的业务代码已完成，本 spec 不重复。
>
> 分支基线：`codex/channelops-go-live-agent`，最新提交 `398e06e`。
> 上游 spec：`Design/videoprocess_channelops_live_agent_spec.md`。

---

## 0. 现状与缺口

Go runner（`internal/channelops/`, `cmd/channelops-runner/`）已实现完整 live 链路，但相比 Python 旧 runner 缺三类运维能力：

| 缺口 | 证据 | 影响 |
| - | - | - |
| 告警链路未接 | `runner.go` / `handlers.go` 无 alert 代码；`config.go:43-44` 读了 `SlackWebhookURL` / `AlertEmailTo` 但无消费方 | quota 低 / PDS 失联 / 上传失败无人知 |
| 无 `/healthz` / 探针 | `cmd/channelops-runner/main.go` 无 HTTP server | runner 卡死不会被 restart 探测到 |
| `cleanup_expired` 未接 | Go 无 cleanup 代码；`ClaimableKinds()`（`handlers.go:64-74`）9 个 kind 无此项 | 队列 / 审计 / 反馈表无限增长 |
| Phase D recompute 不触发 | `api/channel_agent.py:470-473` 是 stub；无调度调用 | `learning_states` 永远空表 |
| `DecisionAuditEntry.pds_decision_json` 写空 | `store_tick.go:392` 强制 `[]byte("{}")` | 候选审计看不到 PDS 决策 |

`ClaimableKinds()` 当前返回（`handlers.go:64-74`）：
```
agent_tick, plan_task, execute_task, observe_job, publish_task,
promote_publication, reconcile_publication, collect_metrics, account_health
```
缺 `send_alert` 和 `cleanup_expired`。

---

## 1. B1（硬阻断）：Alert 链路接通

### 1.1 范围

在 Go runner 加告警发送能力，覆盖 Python 旧 runner 已有的告警类型。

### 1.2 设计

新建 `internal/channelops/alerts.go`：

```go
package channelops

type AlertSink interface {
    Send(ctx context.Context, alert AlertPayload) error
}

type AlertPayload struct {
    Kind        string         // quota_low / pds_outage / material_low_supply / upload_failed
    Severity    string         // info / warning / critical
    ChannelID   string
    ResourceID  string
    Message     string
    Details     map[string]any
}
```

实现两个 sink：
- `SlackAlertSink{WebhookURL string}` — POST JSON 到 webhook，`config.SlackWebhookURL` 非空时启用。
- `LogAlertSink{}` — 始终启用，`slog.Warn` 兜底，保证告警至少进日志。
- Email 可选：如果 `config.AlertEmailTo` 非空再加 `EmailAlertSink`；MVP 阶段可只做 Slack + Log，email 留 TODO。

用 `MultiAlertSink []AlertSink` 组合，任一 sink 失败不阻断其它。

### 1.3 接入点

**两个选择，选 A（推荐）：**

**A. 加 `send_alert` queue kind handler。** 与 Python 旧 runner 行为一致：
- `handlers.go` 的 `ClaimableKinds()` 加 `QueueSendAlert`。
- `Handle()` switch 加 `case QueueSendAlert:` → 解析 `payload_json` 成 `AlertPayload` → 调 `MultiAlertSink.Send`。
- 现有那些 enqueue 告警的代码路径（quota guard / pds health / material low supply）改成 enqueue `send_alert` item。
- 好处：告警异步、可重试、走统一队列；与 Python 语义对齐。

**B. 同步直发。** 在告警源处直接调 `AlertSink.Send`。更简单但失败无重试，且告警发送会拖慢主链路。**不推荐。**

采用 A。`QueueSendAlert = "send_alert"` 加到 `types.go`。

### 1.4 必须覆盖的告警

确认 Go 链路里这几个点会 enqueue `send_alert`（对照 Python `service.py`）：
- `quota_low`：`handle_publish_task` 里 quota < 0.2（Go 对应 publish handler）
- `pds_outage`：PDS 连续失败（如 Go 未实现 PDS health monitor，本期至少在 PDS 调用失败时发一次 warning）
- `material_low_supply`：lane 候选枯竭
- `upload_failed` / `platform_rejected`：reconcile 命中 severe takedown

### 1.5 验收

- 单测：`MultiAlertSink` 一个 sink 失败不影响另一个。
- 单测：`SlackAlertSink` 用 `httptest.Server` 验证 POST body 含 `message` / `severity`。
- 集成：手工 enqueue 一条 `send_alert` item，runner 消费后 Slack（或 mock webhook）收到。
- 触发真实 quota < 20%（或 mock YouTube quota），确认 Slack 收到 `quota_low`。

---

## 2. B2（硬阻断）：`/healthz` Endpoint

### 2.1 范围

`channelops-runner` 进程内起一个最小 HTTP server，暴露健康检查。

### 2.2 设计

新建 `internal/channelops/health.go` + 改 `cmd/channelops-runner/main.go`：

- HTTP server 监听 `:8080`（或 `CHANNELOPS_HEALTH_PORT`，默认 8080）。
- `GET /healthz`：
  - DB ping（`store.Pool.Ping`）
  - 最近一次 scheduler run 距今 < `2 * SchedulerPollSeconds`（用 `runner.lastSchedulerRun`，或查 `internal_scheduler_runs` 最新 `ran_at`）
  - 返回 200 + JSON `{"status":"ok","db":"ok","last_scheduler_run":"..."}`；任一失败返回 503 + 失败项。
- `GET /readyz`（可选）：仅 DB ping，用于启动探针。

实现要点：
- `main.go` 用 goroutine 起 server，`signal.NotifyContext` 取消时 graceful shutdown。
- `Runner` 需要暴露 `HealthCheck(ctx) error` 方法供 handler 调用；`lastSchedulerRun` 已是 `Runner` 字段（`runner.go:14`），加并发保护（mutex 或 atomic）。

### 2.3 Compose 接入

`docker-compose.yml` 的 `channelops-runner-go` 加：
```yaml
healthcheck:
  test: ["CMD", "wget", "-qO-", "http://localhost:8080/healthz"]
  interval: 30s
  timeout: 5s
  retries: 3
  start_period: 30s
ports:
  - "${CHANNELOPS_HEALTH_PORT:-8090}:8080"
```
（注意 Dockerfile 需有 `wget` 或 `curl`；若是 distroless 镜像，改用 Go 自带的健康检查 binary 或换 base image。先确认 `backend/Dockerfile.channelops-runner-go` 的 base。）

### 2.4 验收

- `docker compose --profile channelops-go up`，`docker compose ps` 显示 `health: healthy`。
- `docker compose stop postgres`（或断开 DB），30 秒内 health 变 `unhealthy`，`/healthz` 返回 503。
- 单测：`HealthCheck` 在 DB 不可达时返回 error。

---

## 3. B3（硬阻断）：Live Smoke 全程绿

### 3.1 范围

用现有 `cmd/channelops-live-smoke` 对 staging 真实环境跑一次全链路，验收 6 项数据断言。**这是 gate，不是新代码**（除非 smoke CLI 缺断言项需要补）。

### 3.2 前置

- A 节环境全部就位（staging DB / YouTubeManager / PDS / 测试账号 / channel + lanes + seeds）。
- 所有 alembic 迁移 apply 到 `022_channelops_feedback_learning`。

### 3.3 执行

```bash
docker compose --profile standalone --profile channelops-go run --rm \
  --entrypoint channelops-live-smoke \
  channelops-runner-go -channel-id <staging_channel_profile_id>
```

### 3.4 验收（6 项，全过才能开 soak）

跑完后对 staging DB 查询：

```sql
-- 1. material ledger 写入（验证 Phase 0 P0-1 真通）
SELECT count(*) FROM material_usage_ledger
WHERE channel_profile_id = '<channel>';            -- 期望 >= 1

-- 2. 候选审计写入（验证 Phase B）
SELECT count(*) FROM decision_audit_entries
WHERE channel_profile_id = '<channel>';            -- 期望 >= 候选数

-- 3. 指标完整度非零（验证 Phase A）
SELECT max(metrics_completeness_score) FROM feedback_snapshots f
JOIN publication_records p ON p.id = f.publication_id
JOIN production_tasks t ON t.id = p.production_task_id
WHERE t.channel_profile_id = '<channel>';          -- 期望 > 0

-- 4. reward 写入（验证 Phase D 数据面）
SELECT count(*) FROM feedback_snapshots f
JOIN publication_records p ON p.id = f.publication_id
JOIN production_tasks t ON t.id = p.production_task_id
WHERE t.channel_profile_id = '<channel>'
  AND f.reward_score IS NOT NULL;                  -- 期望 >= 1

-- 5. 无 severe takedown
SELECT count(*) FROM takedown_events te
JOIN publication_records p ON p.id = te.publication_id
JOIN production_tasks t ON t.id = p.production_task_id
WHERE t.channel_profile_id = '<channel>'
  AND te.severity = 'severe';                       -- 期望 0

-- 6. 无 failed task（held 可接受）
SELECT count(*) FROM production_tasks
WHERE channel_profile_id = '<channel>'
  AND state = 'failed';                             -- 期望 0
```

任一不过：停下查根因，**不要开 soak**。

---

## 4. C1（soak 期间并行）：`cleanup_expired` Handler

### 4.1 范围

把 Python `cleanup_expired()` 逻辑迁到 Go，并加调度触发。7 天内表不会爆，但 prod 前必须有。

### 4.2 设计

- `types.go` 加 `QueueCleanupExpired = "cleanup_expired"`。
- `handlers.go` `ClaimableKinds()` + `Handle()` 加该 kind。
- 新建 `internal/channelops/cleanup.go`，`Store.CleanupExpired(ctx, now, retentionDays)`：
  - 删 `channel_ops_queue_items`：`status IN ('succeeded','dead_letter') AND updated_at < now - queue_days`
  - 删 `agent_tick_audits`：`started_at < now - audit_days`（注意 `decision_audit_entries` 有 FK `ON DELETE CASCADE` 到 `agent_tick_audits`，会连带删，符合预期）
  - 删 `feedback_snapshots`：`collected_at < now - feedback_days`
- `config.go` 加 env：
  - `CHANNELOPS_RETENTION_QUEUE_DAYS`（默认 30）
  - `CHANNELOPS_RETENTION_AUDIT_DAYS`（默认 90）
  - `CHANNELOPS_RETENTION_FEEDBACK_DAYS`（默认 365）
- 调度：runner 每天跑一次。可在 `runOnce` 里加"距上次 cleanup > 24h 则 enqueue 一条 `cleanup_expired`"，或 scheduler 加独立 daily bucket。

### 4.3 验收

- 单测：插入超期 + 未超期行，`CleanupExpired` 后只剩未超期。
- 单测：删 `agent_tick_audits` 时 `decision_audit_entries` 被 CASCADE 删除。
- 集成：enqueue `cleanup_expired`，runner 消费成功。

---

## 5. C2（soak 期间并行）：Phase D Recompute 接通

### 5.1 范围

`learning_states` 当前永远空。接通计算触发，soak 期间能沉淀学习数据。

### 5.2 设计

`Store.RecomputeLearningStateForSources` 已实现（`learning.go:35-119`），只缺触发：

- **API stub 接通**：`backend/app/api/channel_agent.py:470-473` 的 `recompute_learning` 改成真触发。两个选项：
  - **选项 A**：Go runner 暴露一个内部 admin endpoint（如 `POST /internal/learning/recompute?channel_id=...`），Python API 转调。需要 B2 的 HTTP server 已存在，复用即可。
  - **选项 B**：Python 用 SQLAlchemy 复写一份聚合逻辑。会产生两套实现，**不推荐**。
  - 采用 A，复用 B2 的 server。
- **调度触发**：runner 每 6 小时对每个 enabled channel 跑一次 `RecomputeLearningStateForSources(channelID, 7)`。可在 scheduler 加独立 bucket（`learning_recompute:<channel>:<6h-bucket>`）走队列，或 runner 内定时。
  - 建议走队列：`types.go` 加 `QueueLearningRecompute`，handler 调 store。windowDays 跑 7 和 30 两个窗口。

### 5.3 验收

- 集成：插入若干 `feedback_snapshots`（`reward_score` 非空、`metrics_completeness_score >= 0.4`），调 recompute，`learning_states` 出现按 source 聚合的行。
- API：`POST /channels/{id}/learning/recompute` 返回成功后，`GET /channels/{id}/learning` 返回非空。
- 确认 `learning_states` 不影响 tick 选择（`learning_influence_test.go:9-18` 已守护，保持绿）。

---

## 6. C3（soak 期间并行）：`pds_decision_json` 真写入

### 6.1 范围

候选审计目前看不到 PDS 决策。`store_tick.go:392` 写死 `{}`。

### 6.2 设计

- tick 阶段的候选 PDS 决策（`candidate_accept` action）应在 evaluate 时保存到 candidate 结构，写 `decision_audit_entries.pds_decision_json`。
- plan_task / publish 阶段的 PDS 决策可回填到对应候选的 audit entry（用 `created_task_id` 关联），或在 task 的 `agent_approval_evidence_json` 已有体现——确认不重复存即可。
- MVP：先把 tick 阶段 candidate_accept 的 verdict + decision_id + metadata 写进去。

### 6.3 验收

- 集成：tick 后查 `decision_audit_entries.pds_decision_json`，含 `verdict` 字段（非 `{}`）。

---

## 7. C4（soak 启动前确认，非代码）：`send_alert` 兼容性

- grep 确认 B1 实现前，Go 代码没有任何路径 enqueue `kind="send_alert"`（否则会进 dead letter）。
- 若有，B1 必须先于 soak 完成（B1 本来就是硬阻断，所以一般无冲突）。

---

## 8. D（soak 启动前最终检查，非代码）

- [ ] D1. Python `channel-agent-runner` 已停，`docker compose ps` 不同时存在两个 runner
- [ ] D2. `alembic upgrade head` 成功，`019/020/021/022` 全 apply
- [ ] D3. channel `enabled=true AND halted_at IS NULL AND dry_run=false`
- [ ] D4. staging `channel_ops_queue_items` 已清空或无历史 dead letter
- [ ] D5. 监控面板可访问：`/health` `/metrics/funnel` `/decisions` `/failures` `/queue`
- [ ] D6. 168 小时后日历提醒已设
- [ ] D7. on-call 知会，谁看 Slack 告警已明确

---

## 9. 执行顺序与依赖

```
B2 (/healthz HTTP server)
   ├─> B1 (alert，可独立，但 email sink 不强求)
   ├─> C2 选项A (learning recompute 复用 B2 的 server)
   └─> Compose healthcheck

B1 + B2 + B3 全绿  ──> 开 soak
                       │
                       ├─ 并行 C1 (cleanup)
                       ├─ 并行 C2 (learning trigger)
                       └─ 并行 C3 (pds_decision_json)
```

- **B2 先做**（B1 的 email 和 C2 的 recompute 都可复用 HTTP server，但 B1 的 Slack 不依赖它）。
- **B1 / B3 是 gate**。
- **C1 是 prod 前硬阻断**，soak 期间补完即可。
- **C2 / C3 是数据质量项**，soak 期间补完最好，不补 soak 也能跑（只是 learning 空表 + 审计缺 PDS）。

---

## 10. 工程量估算

| 项 | 人 · 天 |
| - | - |
| B1 alert (Slack + Log sink + send_alert handler) | 1.0 |
| B2 /healthz + compose healthcheck | 0.5 |
| B3 live smoke 执行 + 6 项验收（非编码，含排障预留） | 0.5 |
| C1 cleanup_expired + 调度 | 0.75 |
| C2 learning recompute 触发 + API 接通 | 0.75 |
| C3 pds_decision_json 写入 | 0.5 |
| **合计** | **~4 人 · 天** + soak 168h 等待 |

---

## 11. 不做什么

- 不做 email sink 之外的告警通道（PagerDuty / OpsGenie）——MVP 阶段 Slack + Log 够用。
- 不做 prometheus metrics endpoint——本期只做 `/healthz`；metrics 留到 prod 前。
- 不做 Python runner 互斥物理锁——本期靠 D1 人工确认；prod 前再加 advisory lock。
- 不动任何 Phase 0/A/B/C 已完成的业务逻辑。
- 不上 Phase E / F。
