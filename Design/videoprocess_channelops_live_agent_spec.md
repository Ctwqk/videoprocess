# VideoProcess ChannelOps Live Agent Spec

> 目标：把 `Ctwqk/videoprocess` 的 ChannelOps Agent 从"控制面齐全的早期 Beta"推进到"YouTube 单平台、unlisted-only、单频道少账号、7 天无人值守可跑、30 天低事故"的可运营 Agent v0.1。
>
> 本文综合了内部 review 与外部 gpt pro review 的结论。**不追求一次性自主选题、多平台、多账号或 bandit 学习**，这些放到后续阶段；当前阶段唯一目标是让 live 链路真正跑稳，并把目前结构存在、实际不通的几个保护机制修通。

---

## 0. 适用范围

本计划面向 Codex / 工程代理执行，覆盖：

- 现有 ChannelOps 链路（scheduler → tick → plan → execute → publish → reconcile → metrics）的 live 阻断点修复。
- AutoFlow 与 ChannelOps 之间数据契约的修补（特别是 `material_id`）。
- 已经实现但当前不会触发的保护逻辑（MaterialUsageLedger、RepetitionGuard、TakedownEvent dedup）的真实启用。
- 选题发现层从 `ManualSeed` 中解耦的最小迁移。
- 7 天 unlisted soak test 的可执行验收脚本与指标。

不包含：

- 多平台（X / 小红书 / B 站）扩展。
- Bandit 或 Contextual learning。
- Public 自动发布。
- 多账号矩阵策略（仅做单频道 1–3 账号场景）。

---

## 1. 现状判断

### 1.1 已落地（直接复用）

| 能力 | 关键文件 | 备注 |
| - | - | - |
| 真实 YouTubeManagerClient | `backend/app/channel_agent/clients.py:368-446` | 6 个 endpoint 全有 |
| Runner 强制注入真实 client | `backend/app/channel_agent/runner.py:31-34` | `YOUTUBE_MANAGER_URL` 空直接 raise |
| AutoFlow agent approval bridge | `backend/app/autoflow/service.py:524-564, 1134-1144` | `agent_approved_by` 与 `review_approved_at` 等价 |
| 主动 fetch metrics | `backend/app/channel_agent/service.py:1489-1497` | 失败 N 次后 held |
| Reconcile publication 状态 | `backend/app/channel_agent/service.py:1428-1476` | severe → TakedownEvent |
| MaterialUsageLedger 与 RepetitionGuard | `backend/app/channel_agent/material_usage.py`, `service.py:606,1222,1253` | 结构在，**今天不会触发**（见 P0-1） |
| 内部 scheduler | `backend/app/channel_agent/scheduler.py`, `runner.py:80-91` | `tick_interval_minutes` 已支持 |
| YouTubeTrendIngester | `backend/app/channel_agent/trend_ingesters/youtube_search.py` | 单源，输出 `ManualSeed` |
| 规则型 score | `backend/app/channel_agent/candidate_scoring.py` | 当前只 observe，**未参与选择** |
| PDS fail-policy | `backend/app/pds_client.py:12-17` | publish/promote 默认 block |

### 1.2 与目标的差距（按是否阻塞 live 排序）

1. **AutoFlow candidate schema 没有 `material_id`**：`AutoFlowClipCandidate`（`backend/app/schemas/autoflow.py:88-99`）和 `_material_metadata`（`backend/app/autoflow/search_service.py:201-219`）都只写 `library_id` / `source_asset_id` / `asset_id`，而 `extract_material_references`（`backend/app/channel_agent/material_usage.py:133`）要求 `material_id` 才生成 reference。**ledger 永远写不出有效行，repetition guard 永远 pass。**
2. **plan_approval flag/block 路径漏判**：`service.py:977-986` 把 `task.state = TASK_PLANNING` 和 `enqueue execute_task` 都放在 `if decision.verdict == "allow"` 之外。flag/block 时仍进 execute path，到 `_assert_execute_allowed` 因为 `agent_approved_by` 为 None 抛 PermissionError，最终落 `TASK_FAILED` 而不是 `TASK_HELD`。
3. **NoopPDSClient 默认 fail-closed**：`backend/app/pds_client.py:46-48` + `pds_enabled=False` 默认 → publish/promote 返回 `block`。本地 / dev / staging 没有真实 PDS 时所有 publish 会卡。
4. **TakedownEvent 无 dedup**：`service.py:1465` 每次 reconcile 都 `db.add(TakedownEvent(...))`，rejected 视频持续触发会污染 KPI。
5. **trend_youtube → ManualSeed 的命名冲突**：`youtube_search.py:68` 写 `source_policy="trend_youtube"`，下游 `_publish_time_material_usage_guard` 又对 `task.source == "manual_seed"` 做静默 override（`service.py:1734-1742`），**trend 任务因此享受人工 seed 的特权，绕过 repetition guard**。
6. **FeedbackSnapshot partial-aware 不完整**：`retention_curve_json` / `ctr` / `impressions` 是 nullable，但 `views/likes/comments/shares` 默认 0；没有 `metrics_completeness_score` 字段。下游 learning 拿到的 "retention=0" 可能其实是 "未拿到"。
7. **观测粒度不足以排查**：审计停在 `AgentTickAudit.decision_summary_json` 一个 JSON dict，无候选级 entry，无 failure_category enum。
8. **discovery / learning 没有独立实体**：trend 信号、人工 seed、候选评分都挤在 `ManualSeed` + `ProductionTask.score_breakdown_json` 里，长期不可持续。

### 1.3 内部判断 vs gpt pro 判断

| 维度 | gpt pro 判断 | 本 spec 判断 | 差异原因 |
| - | - | - | - |
| `live_unlisted` 完成度 | 70%-80% | 55%-65% | gpt pro 未确认 AutoFlow candidate schema，不知道 ledger 是空跑 |
| 7 天无人值守可跑 | 50%-60% | 40%-50% | 同上 + plan flag 路径 bug + PDS fail-closed |
| 多账号自驱 | 30%-40% | 30%-40% | 一致 |

---

## 2. 分期目标

```
Phase 0 (本周内)        P0 修复，让 soak 不会被三件结构性问题污染
Phase A (1-2 周)        Live unlisted smoke + 7 天 soak
Phase B (2-3 周)        候选级可解释化 + 失败分类 + Takedown dedup
Phase C (2-3 周)        Discovery 与 ManualSeed 解耦
Phase D (后续，等数据)   Feedback learning v1，规则型调权
Phase E (后续，等门槛)   受控 public 发布
Phase F (远期)           Bandit / Portfolio learning
```

> Phase D / E / F 不在本 spec 详细展开；进入条件参见第 6 节。

---

## 3. Phase 0：P0 修复（**Phase A 之前必须完成**）

### P0-1：AutoFlow candidate 输出 `material_id`

**问题定位**：`extract_material_references` 要求 `material_id`；AutoFlow 上游不产出。

**修改范围**：

- `backend/app/schemas/autoflow.py:88-99`：`AutoFlowClipCandidate` 增加 `material_id: str | None = None` 字段（不破坏既有调用，加默认值）。
- `backend/app/autoflow/search_service.py:180-219`：
  - `_candidate_from_material_result`：把 `result.get("material_id")` 或 fallback 到 `materialized_asset_id` 传给 `material_id`。
  - `_material_metadata`：同步把 `material_id` 写入 metadata（双写以兼容 grep 路径）。
- `backend/app/autoflow/service.py:1013` 附近的 `source_type="material"` 构造也要带上 `material_id`。
- `backend/app/autoflow/clip_ranker.py:237` 附近：selected clip 输出时确保 `material_id` 进入 `AutoFlowPlanCandidate` / `AutoFlowRun.artifacts_json`。
- `backend/app/channel_agent/material_usage.py:132-149`：`_reference_from_dict` 增加 fallback——若没有 `material_id`，则用 `asset_id` 当作 material_id（避免 AutoFlow 链路其它产出的旧数据丢失）。这是兜底，新数据应走显式 `material_id`。

**验收**：

```sql
-- soak 启动 24 小时后
SELECT COUNT(*) FROM material_usage_ledger
WHERE channel_profile_id = '<test_channel>'
  AND used_at >= now() - interval '24 hours';
-- 应当 > 0
```

加一个集成测试：`tests/channel_agent/test_material_ledger_writes.py`，用 LocalAutoFlowClient 走一次完整 publish，断言 ledger 行数 >= 1。

---

### P0-2：plan_approval flag/block 路径 held

**修改范围**：`backend/app/channel_agent/service.py:957-997` 的 `handle_plan_task`。

**目标行为**：

```python
if task.approval_mode == "agent":
    decision = await self._decide_pds(...)
    evidence = _pds_decision_event_metadata(decision)

    if decision.verdict == "allow":
        await self.autoflow_client.approve_plan(...)
        task.agent_approval_evidence_json = evidence
        # 走原本的 TASK_PLANNING + enqueue execute_task
    else:
        # flag 或 block：不 approve，直接 held，不 enqueue execute_task
        task.state = TASK_HELD
        task.blocked_by_guard = (
            "pds_blocked" if decision.verdict == "block" else "pds_flagged_for_review"
        )
        task.failure_reason = (
            f"PDS {decision.verdict} on plan_approval (decision_id={decision.decision_id})"
        )
        task.agent_approval_evidence_json = evidence
        task.transition_history_json = [
            *list(task.transition_history_json or []),
            _transition(task.state, TASK_HELD, "plan_task_pds", self.clock.now()),
        ]
        await db.commit()
        await db.refresh(task)
        return task
```

`task.approval_mode == "human"`（manual_seed 路径）保持现有行为（不 approve、不 held、由人审）。

**验收**：

- 新增 `tests/channel_agent/test_plan_task_pds_flag.py`：mock PDSClient 返回 flag → 断言 `task.state == TASK_HELD` 且 `blocked_by_guard == "pds_flagged_for_review"` 且队列里没有 `execute_task:<task.id>`。
- block 同理。

---

### P0-3：dev/staging PDS 必须能 allow

**问题**：`NoopPDSClient.decide` 当前对 `publish` / `promote_publication` 返回 fail-policy block，导致 `pds_enabled=False` 时本地 smoke 卡在 publish。

**两个可选方案**（推荐方案 A）：

**A. 拆分 Noop / FailClosed 两个 client**

- `backend/app/pds_client.py` 增加 `AllowAllPDSClient`，所有 verdict 都 `allow`，metadata 标 `dev_allow_all`。
- `backend/app/channel_agent/runner.py:21-28` `_build_pds_client` 改成：

```python
def _build_pds_client() -> PolicyDecisionClient:
    if settings.pds_enabled:
        return PDSClient(...)
    if settings.channel_agent_dev_allow_all_pds:
        return AllowAllPDSClient()
    return NoopPDSClient()  # 保持 fail-closed 作为最严格的默认
```

- `Settings` 增加 `channel_agent_dev_allow_all_pds: bool = False`，仅在 `.env.local` 打开。
- 文档（`docs/` 下新增 `channelops-dev-pds.md`）说明 dev 必须显式打开这个 flag。

**B. 起独立 mock PDS 服务**：成本高、对 onboarding 不友好，**不推荐**。

**验收**：本地 `PDS_ENABLED=false CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true` 启动 runner，跑一次完整 tick → publish → schedule，无 PDS 阻断；`PDS_ENABLED=false CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=false` 启动，publish 仍 held（保留生产 fail-closed 语义）。

---

### P0-4：TakedownEvent dedup

**修改范围**：`backend/app/channel_agent/service.py:1465-1472`。

**目标行为**：写入前查询当日是否已有 `(publication_id, event_type)` 的同类事件；若有，则只追加到现有 event 的 `raw_payload_json.repeats[]`，不新建 row。简化做法：

```python
if publish_status in severe_states:
    today_utc = self.clock.now().replace(hour=0, minute=0, second=0, microsecond=0)
    existing = (
        await db.execute(
            select(TakedownEvent)
            .where(TakedownEvent.publication_id == publication.id)
            .where(TakedownEvent.event_type == publish_status)
            .where(TakedownEvent.detected_at >= today_utc)
            .order_by(TakedownEvent.detected_at.desc())
            .limit(1)
        )
    ).scalars().first()
    if existing is None:
        db.add(TakedownEvent(...))
    else:
        repeats = list(existing.auto_actions_taken_json or [])
        repeats.append({"detected_at": self.clock.now().isoformat(), "status": dict(status)})
        existing.auto_actions_taken_json = repeats
```

加上 `Index("ix_takedown_events_publication_event_day", "publication_id", "event_type", "detected_at")` 的迁移。

**验收**：mock reconcile 路径连续触发 3 次 severe → `takedown_events` 行数 == 1，`auto_actions_taken_json` 长度 == 2（首次写入 + 2 次 append）。

---

### P0 完成判定

```
✓ P0-1 ledger 真实写入
✓ P0-2 plan flag/block → held
✓ P0-3 dev 可 allow publish
✓ P0-4 TakedownEvent dedup
✓ 全部 4 项有对应单测或集成测试
```

四项任一未完成，不进入 Phase A。

---

## 4. Phase A：Live Unlisted Smoke + 7 天 Soak

**目标**：在 P0 修复之上，做一次真实账号的 unlisted 发布，证明完整链路跑通；随后启动 7 天无人值守 soak。

### 4.1 前置准备

| 项 | 内容 |
| - | - |
| 测试频道 | 1 个 `ChannelProfile`，`dry_run=false`、`enabled=true`、`tick_interval_minutes=60` |
| 测试 lane | 2 个 `TopicLane`，每个有 1 个 `LaneFormatMatrix`（`default_publish_visibility=unlisted`） |
| 测试账号 | 1 个真实 YouTube 账号，`default_privacy=unlisted`、`external_asset_auto_publish=true` |
| YouTubeManager | 真实服务部署，6 个 endpoint 全部连通 |
| PDS | staging PDS 或 `AllowAllPDSClient`（dev 路径） |
| Seed | 至少 5 条 `ManualSeed`，分布在 2 个 lane |

### 4.2 trend_youtube → 不享受 manual_override（P0.5 / Phase A 内）

**问题**：`youtube_search.py:68` 写 `source_policy="trend_youtube"`，但 `_publish_time_material_usage_guard:1734` 仅判 `task.source == "manual_seed"` 即 override。

**短期修复**（不引入新表）：

- `service.py:1734` 改为 `manual_override = (task.source == "manual_seed" and (task.rationale_json or {}).get("source_kind") != "trend_youtube")`。
- `service.py` 在从 `ManualSeed` 构建候选时，把 `seed.source_policy` 写入 `rationale_json["source_kind"]`。

**长期修复**：放到 Phase C（DiscoverySignal/IdeaSeed 解耦后，trend 不再走 ManualSeed）。

### 4.3 FeedbackSnapshot partial-awareness

**修改**：

- `backend/app/models/channel_agent.py`：`FeedbackSnapshot` 新增字段：
  - `metrics_completeness_score: float = 0.0`（0-1，按拿到的关键字段比例算）
  - `available_fields_json: list[str]`（["views", "likes", "retention", ...]）
- 迁移 `018_feedback_snapshot_completeness.py`。
- `service.py:1529-1545`：写 snapshot 时计算 completeness，记录哪些字段是 None vs 0。建议 weight：

```python
WEIGHTS = {
    "views": 0.15, "likes": 0.10, "comments": 0.05, "shares": 0.05,
    "avg_view_duration_sec": 0.20, "retention_curve_json": 0.20,
    "ctr": 0.10, "impressions": 0.15,
}
```

`metrics_completeness_score = sum(WEIGHTS[k] for k in available_fields)`。

**验收**：mock fetch_metrics 只返回 `views/likes` → snapshot 写入后 `metrics_completeness_score ≈ 0.25`，`available_fields_json == ["views", "likes"]`。

### 4.4 Smoke 脚本

新建 `backend/scripts/live_smoke_unlisted.py`：

```
1. 健康检查：YouTubeManager /api/auth/status 返回 authenticated=true，quota_estimate 存在
2. 健康检查：PDS（如启用）/healthz 返回 ok
3. 调用 POST /channels/<id>/enqueue-tick 触发一次 tick
4. 轮询 GET /channels/<id>/tasks，等到至少 1 个 task 进入 TASK_SCHEDULED
5. 校验 publication.publish_status == "scheduled"，platform_content_id 非空
6. 等待 scheduled_at + 30 min，触发 reconcile
7. 校验 publication.current_privacy == "unlisted"
8. 等待 metrics_poll 周期，校验 FeedbackSnapshot 至少 1 行
9. 校验 material_usage_ledger 至少 1 行（P0-1 验收）
10. 校验 takedown_events == 0
```

### 4.5 7 天 Soak 验收

```
✓ scheduler 自动 tick 7 * 24 = 168 次（误差 < 5%）
✓ tasks_created >= 14（保守估计，2/天）
✓ tasks 进入 TASK_MEASURED 比例 >= 70%
✓ publications 进入 reconciled 比例 == 100%（含 held/rejected）
✓ severe takedown == 0
✓ material_usage_ledger 行数 > 0
✓ AgentTickAudit.error_message 比例 < 5%
✓ ChannelOpsQueueItem dead_letter_at != null 比例 < 1%
✓ 无人工干预（不修代码、不重启 runner、不手动 enqueue）
```

任一不达标，分析根因后回到 P0 或 Phase A 的具体修复项。

---

## 5. Phase B：候选级可解释化 + 失败分类

**目标**：让 soak 期间的任何一条 ProductionTask 都能回答：为什么创建、为什么选这个账号、用了什么素材、哪些 guard 通过/阻止、发出后表现。

### 5.1 DecisionAuditEntry（新表）

```python
class DecisionAuditEntry(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "decision_audit_entries"
    __table_args__ = (
        Index("ix_decision_audit_entries_tick", "tick_audit_id"),
        Index("ix_decision_audit_entries_channel_created", "channel_profile_id", "created_at"),
    )

    tick_audit_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("agent_tick_audits.id", ondelete="CASCADE"), nullable=False
    )
    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    candidate_id: Mapped[str] = mapped_column(String(255), nullable=False)
    candidate_source: Mapped[str] = mapped_column(String(64), nullable=False)  # manual_seed / lane_seed / trend_youtube
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    target_account_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    score_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    guard_results_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)
    pds_decision_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    selected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    rejection_reason: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_task_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
```

迁移 `019_decision_audit_entries.py`。`service.py:_evaluate_tick_candidates` 在每个候选最终决议时写一行。**`AgentTickAudit.decision_summary_json` 保留作为汇总。**

### 5.2 failure_category enum

`backend/app/channel_agent/constants.py` 新增：

```python
FAILURE_CATEGORY_AUTH = "auth"
FAILURE_CATEGORY_QUOTA = "quota"
FAILURE_CATEGORY_UPLOAD = "upload"
FAILURE_CATEGORY_RENDER = "render"
FAILURE_CATEGORY_PLANNING = "planning"
FAILURE_CATEGORY_VALIDATION = "validation"
FAILURE_CATEGORY_PDS = "pds"
FAILURE_CATEGORY_YOUTUBE_STATUS = "youtube_status"
FAILURE_CATEGORY_METRICS = "metrics"
FAILURE_CATEGORY_OTHER = "other"
```

`ProductionTask` 新增 `failure_category: Mapped[str | None]`（迁移 `020_production_task_failure_category.py`）。所有 `task.failure_reason = "..."` 的地方同步赋 `task.failure_category = FAILURE_CATEGORY_*`。

### 5.3 Status API 扩展

`backend/app/api/channel_agent.py` 增加：

- `GET /channels/{id}/decisions?tick_audit_id=...&limit=100`：返回最近的 DecisionAuditEntry。
- `GET /tasks/{id}/audit`：包含 task 自身、对应 publication、material_usage_ledger 行、对应 DecisionAuditEntry。
- `GET /channels/{id}/failures?days=7`：按 `failure_category` 聚合计数。

### 5.4 Phase B 验收

```
✓ 任意 ProductionTask 的 GET /tasks/{id}/audit 能返回完整决策证据
✓ 7 天内的 failure 按 category 分布可见，且 "other" 类 < 10%
✓ soak 期间的 dashboard 可手动定位任一 held/failed 任务的根因
```

---

## 6. Phase C：Discovery 与 ManualSeed 解耦

**目标**：把 trend 信号、idea seed 与人工 seed 区分开，为 Phase D 的 learning 留干净的数据形状。

### 6.1 新增 DiscoverySignal

```python
class DiscoverySignal(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "discovery_signals"
    __table_args__ = (
        Index("ix_discovery_signals_channel_lane_observed", "channel_profile_id", "topic_lane_id", "observed_at"),
        UniqueConstraint("channel_profile_id", "source", "source_external_id",
                         name="uq_discovery_signal_channel_source_external"),
    )

    channel_profile_id: Mapped[uuid_mod.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("channel_profiles.id", ondelete="CASCADE"), nullable=False
    )
    topic_lane_id: Mapped[uuid_mod.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    source: Mapped[str] = mapped_column(String(64), nullable=False)  # youtube_search / rss / competitor / manual_curated
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(Text, default="", nullable=False)
    summary: Mapped[str] = mapped_column(Text, default="", nullable=False)
    keywords_json: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    trend_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    novelty_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    raw_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active", nullable=False)
```

迁移 `021_discovery_signals.py`。

### 6.2 IdeaSeed（最小版本）

短期可以**只加一个 `idea_seed_id` 字段到 `ProductionTask`，先不建独立 IdeaSeed 表**——因为一对一关系下 IdeaSeed 价值有限。等真的需要"1 个 trend → 多个 idea"再拆。

`ProductionTask` 新增：

- `discovery_signal_id: Mapped[uuid_mod.UUID | None]`（迁移 `022_production_task_discovery.py`）。

### 6.3 YouTubeTrendIngester 改写

`backend/app/channel_agent/trend_ingesters/youtube_search.py`：

- `ingest_channel` 写 `DiscoverySignal` 而不是 `ManualSeed`。
- 新增 `DiscoveryToCandidateBuilder`（`backend/app/channel_agent/discovery.py`）：在 tick 中把 active 且未过期、未转化的 `DiscoverySignal` 转成 candidate dict，标 `source="trend_youtube"`。
- `service.py:_build_tick_candidates` 增加从 discovery 拉取候选的分支。
- `ManualSeed.source_policy == "trend_youtube"` 的旧数据通过一次数据迁移转成 `DiscoverySignal`（迁移 `021_discovery_signals.py` 的 data migration 部分）。

### 6.4 Discovery dedup / decay / 配额

- dedup：`source_external_id` 已有 unique 约束。
- decay：`expires_at` 过期后 ingest 主动标 `status="expired"`。
- 配额：每个 lane 同时 active 的 DiscoverySignal 上限（建议先 50）。

### 6.5 Phase C 验收

```
✓ DiscoverySignal 表每天有新增（基于 YouTube search 频率）
✓ Discovery → candidate → task 链路打通，rationale_json 包含 discovery_signal_id
✓ ManualSeed.source_policy == "trend_youtube" 旧行全部迁移
✓ 候选 source 区分清晰：manual / lane_seed / trend_youtube
✓ trend 候选不再享受 manual_override（Phase A 4.2 的临时 hack 可以撤掉）
```

---

## 7. Phase D / E / F（不在本 spec 详细展开）

### 7.1 进入 Phase D（Feedback learning v1）条件

```
✓ Phase A 7 天 soak 通过
✓ Phase B 完成（决策可解释）
✓ Phase C 完成（discovery 解耦）
✓ FeedbackSnapshot 累计 >= 100 行 with metrics_completeness_score >= 0.6
```

骨架：

- `PublicationMetricSeries`：在 `FeedbackSnapshot` 上加 `snapshot_stage` enum（`1h` / `6h` / `24h` / `72h` / `7d`），允许同一 publication 多行。
- `LearningState`：按 dimension（topic_lane / lane_format / publish_window / template）聚合 `avg_reward`，规则型周报。
- reward 公式先用 gpt pro 建议的 weighted sum，retention 缺失走 partial reward。

### 7.2 进入 Phase E（受控 public）条件

```
✓ Phase A 后连续 30 天 unlisted soak 0 severe takedown
✓ Phase B 完成
✓ metrics_completeness_score 平均 > 0.6
✓ PDS 正常运行率 > 99%
✓ 平均 retention 达到阈值（待定，需 Phase D 数据）
✓ repetition_guard 误杀/漏杀 0 重大事故
```

骨架：`PublicGatePolicy` + manual override + public publish audit + delist mechanism。

### 7.3 Phase F：bandit / portfolio learning

至少 100-500 条真实 publications 后启动，从 epsilon-greedy per lane 起步。

---

## 8. 数据模型变更汇总

| 迁移 | 内容 | 阶段 |
| - | - | - |
| `018_feedback_snapshot_completeness.py` | `FeedbackSnapshot.metrics_completeness_score`, `available_fields_json` | Phase A |
| `019_decision_audit_entries.py` | 新表 `decision_audit_entries` | Phase B |
| `020_production_task_failure_category.py` | `ProductionTask.failure_category` | Phase B |
| `021_discovery_signals.py` | 新表 `discovery_signals` + 数据迁移 from ManualSeed | Phase C |
| `022_production_task_discovery.py` | `ProductionTask.discovery_signal_id` | Phase C |
| `023_takedown_events_dedup_index.py` | `ix_takedown_events_publication_event_day` | P0 |

> 所有迁移保持单向加列 / 加表 / 加索引，不动既有列含义；rollback 走 alembic downgrade。

---

## 9. 监控与指标

### 9.1 Soak 期间必须自动采集的指标

| 指标 | 数据源 | 告警阈值 |
| - | - | - |
| scheduler tick 准点率 | `internal_scheduler_runs` | 24h 准点率 < 95% 告警 |
| tick 成功率 | `agent_tick_audits.error_message` | 24h 失败率 > 5% 告警 |
| task 成功率（reached MEASURED / created） | `production_tasks` | 7d < 60% 告警 |
| publication reconcile 完成率 | `publication_records.publish_status` | 24h 内 scheduled 未 reconcile > 0 告警 |
| severe takedown 计数 | `takedown_events` | 任一 severe = page on-call |
| material_usage_ledger 增长 | 行数对比 | 7d 增长 = 0 告警（暗示 P0-1 回退） |
| PDS 成功率 | runner 内统计 | 24h < 99% 告警 |
| YouTubeManager 调用错误率 | runner 内统计 | 24h > 2% 告警 |
| metrics_completeness 均值 | `feedback_snapshots` | 7d < 0.5 告警 |

### 9.2 Dashboard

最小子集（Phase A 必须）：

- `/channels/{id}/health`（已存在，加上 `last_takedown_at`, `metrics_completeness_avg_7d`）
- `/channels/{id}/metrics/funnel`（已存在，加上 `failure_by_category`）
- `/channels/{id}/queue`（已存在）
- `/channels/{id}/decisions`（Phase B 新增）

---

## 10. 风险与 Rollback

| 风险 | 触发条件 | Rollback |
| - | - | - |
| P0-1 broken AutoFlow → ledger 行错乱 | ledger 行 material_id 与实际素材不符 | downgrade `material_id` 字段，回退 schema |
| P0-2 改了 plan flag 路径导致正常 task 也 held | held 比例 > 30% | 还原 if 范围，恢复旧行为 |
| P0-3 dev allow_all 误开到 staging | `CHANNEL_AGENT_DEV_ALLOW_ALL_PDS=true` 在 staging | 文档 + secret check + alert |
| Phase C trend 迁移丢数据 | `ManualSeed→DiscoverySignal` 数据迁移失败 | 单独的 data migration，保留 ManualSeed 原行 90 天不删 |
| 7 天 soak 卡死无法继续 | 任一 P0 验收回归 | 立刻暂停 runner，回到 P0 修复 |

---

## 11. 时间预估

| 阶段 | 工程量（人 · 天） | 备注 |
| - | - | - |
| Phase 0 (P0-1 → P0-4) | 4-6 | 含测试 |
| Phase A 准备 + smoke | 2-3 | 不含 soak 等待 |
| Phase A 7 天 soak | 7 | 监控为主 |
| Phase B | 5-7 | DecisionAuditEntry + failure_category + API |
| Phase C | 6-8 | DiscoverySignal + ingester 改写 + 数据迁移 |
| **合计到 Phase C 完成** | **24-31 天 + 7 天 soak** | 单人节奏 |

并行可压缩到 ~3 周，但 7 天 soak 不可压缩。

---

## 12. 不做什么（明确剔除）

- **不做多平台**：X / 小红书 / B 站全部排除在本 spec 外。
- **不做 bandit / 强化学习**：等 Phase F。
- **不做 public 自动发布**：等 Phase E。
- **不做多账号矩阵策略**：当前 1-3 账号场景够用。
- **不重构 AutoFlow planner**：只在 P0-1 加 `material_id` 字段，不动 planning 逻辑。
- **不引入新的 trend 来源**（RSS / 竞品 watcher）：YouTube search 单源够 Phase C 验证。
- **不做 IdeaSeed 独立表**：用 `discovery_signal_id` 在 ProductionTask 上即可，等真有 1:N 需求再拆。

---

## 13. 执行检查清单

```
[ ] P0-1 AutoFlow material_id 贯通
[ ] P0-2 plan_approval flag/block → held
[ ] P0-3 dev PDS allow_all 通道
[ ] P0-4 TakedownEvent dedup
[ ] P0 集成测试全部通过
[ ] Phase A 前置环境就位（真实账号、YouTubeManager、PDS）
[ ] live_smoke_unlisted.py 跑通
[ ] FeedbackSnapshot partial-awareness
[ ] trend_youtube 不享受 manual_override
[ ] 7 天 soak 启动
[ ] 7 天 soak 验收 9 项指标
[ ] Phase B DecisionAuditEntry
[ ] Phase B failure_category
[ ] Phase B status API 扩展
[ ] Phase C DiscoverySignal
[ ] Phase C ingester 改写
[ ] Phase C 旧数据迁移
[ ] 决策 Phase D 是否进入
```
