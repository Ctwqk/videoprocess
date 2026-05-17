# VideoProcess AutoFlow 升级改造计划

> 目标：把 `Ctwqk/videoprocess` 从“手动搭建视频处理工作流的平台”升级为“用户用自然语言描述内容需求，系统自动规划、选材、拼接、预览、导出、可控发布，并持续从表现数据中优化选题和模板”的内容自动化平台。

---

## 0. 适用范围

本计划面向 Codex/工程代理执行，覆盖：

- 后端 AutoFlow 规划层。
- PipelineDefinition 自动生成与自动修复。
- 搜索、素材库、选材、评分、版权策略后端化。
- 新增集锦类视频所需节点与 worker handler。
- 前端 AutoFlow 页面与人工审核流程。
- 趋势、指标、反馈闭环。
- 测试、CI、文档、可观测性。

本计划不要求一次性把所有能力做成“完全自主发布”。默认策略是：

1. 先自动生成草稿和预览。
2. 外部平台下载素材默认不得直接公开发布。
3. 发布节点默认 `private` 或 `unlisted`。
4. 公开发布必须经过人工审核或显式白名单策略。

---

## 1. 当前项目能力判断

### 1.1 已有能力

从仓库现有文件看，VideoProcess 已有以下基础：

- 后端 FastAPI orchestration API：`backend/app/main.py`。
- Pipeline CRUD、template、validate、execute、batch execute：`backend/app/api/pipelines.py`。
- Job 创建、batch job、rerun、取消、删除：`backend/app/api/jobs.py`。
- PipelineDefinition schema：`backend/app/schemas/pipeline.py`。
- DAG 校验、拓扑排序、端口校验：`backend/app/orchestrator/dag.py`。
- planner 节点类型与 planner runtime definition 剥离：`backend/app/orchestrator/planner.py`。
- Redis Stream worker dispatch：`backend/app/orchestrator/engine.py`。
- worker handler 映射：`backend/worker/handlers/__init__.py`。
- 节点注册表：`backend/app/node_registry/registry.py`。
- 内置节点：`backend/app/node_registry/builtin/`。
- React Flow 编辑器：`frontend/src/pages/EditorPage.tsx`。
- 节点面板、配置面板、搜索结果选择、batch run：`frontend/src/components/editor/`。
- 素材库、material search、embedding/Qdrant/clip refine：`backend/app/api/materials.py`、`backend/app/services/material_service.py`。

### 1.2 主要欠缺

当前系统离“我要小猫视频集锦”这类自然语言自动生成内容，主要缺：

1. 自然语言意图解析服务。
2. 后端 AutoFlow planner。
3. 节点能力图谱和模板库。
4. 自动生成 `PipelineDefinition` 的 builder。
5. validate 失败后的自动修复 loop。
6. 后端化搜索、候选素材选择、候选素材评分。
7. 多片段集锦节点，例如 `concat_many`、`montage_assembler`、`trim_many`。
8. 自动元数据生成：标题、描述、tags、封面文字、平台文案。
9. 版权/来源策略与人工审核 gating。
10. 趋势数据、发布指标、A/B 测试和反馈闭环。
11. 面向 Codex 的 AGENTS.md、任务拆分、测试与验收标准。

---

## 2. 总体目标

### 2.1 用户体验目标

用户输入：

```text
我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要直接公开发布。
```

系统输出：

1. 结构化意图：主题、风格、目标平台、时长、来源策略、审核策略。
2. 内容策略：选材关键词、模板、节奏、字幕、BGM、标题候选。
3. 自动 workflow：可视化 pipeline definition。
4. 候选素材：可审阅、可替换、可锁定。
5. 预览视频：artifact/export。
6. 可选发布：默认 private/unlisted，人工确认后公开。
7. 指标回收：播放量、完播率、点赞率、CTR、评论等进入反馈系统。

### 2.2 工程目标

- LLM 不直接随意画任意图，而是在能力图谱和模板库中受控规划。
- 所有 AutoFlow 生成的 pipeline 必须通过现有 `validate_pipeline()`。
- 失败时进入 repair loop，最多重试 3 次。
- 所有自动发布必须经过 `rights_policy` 和 `review_policy`。
- 每个 AutoFlow plan、run、artifact、publication、metric 都可追踪。
- 新增功能要有单元测试、集成测试和最少 3 个端到端样例。

---

## 3. 目标架构

```text
User Prompt
  ↓
AutoFlow API
  ↓
Intent Parser
  ↓
Context Loader
  ├─ Node Capability Manifest
  ├─ Workflow Template Library
  ├─ Material Libraries
  ├─ Trend Signals
  ├─ Platform/Search Services
  └─ Rights Policy
  ↓
Workflow Planner
  ↓
PipelineDefinition Builder
  ↓
validate_pipeline()
  ↓
Validation Repair Loop
  ↓
Plan Preview + Candidate Assets
  ↓
Human Approval / Auto Policy Gate
  ↓
create_pipeline() → create_job() / batch
  ↓
Worker Execution
  ↓
Preview / Export / Private Upload
  ↓
Metrics Feedback
  ↓
Template + Ranking Optimization
```

---

## 4. 关键设计原则

### 4.1 模板优先，不让 LLM 任意连图

不要让 LLM 直接生成任意 nodes/edges。正确做法：

1. 后端维护模板库。
2. 后端维护节点能力 manifest。
3. LLM 只负责意图解析、模板选择、slot 填充、候选策略建议。
4. `PipelineBuilder` 根据模板和 slots 生成确定性的 `PipelineDefinition`。
5. 生成后必须 validate。

### 4.2 默认人工审核

AutoFlow 的默认输出是：

- 预览视频；
- private/unlisted upload；
- 待审核发布草稿。

不得默认 public 发布外部平台下载素材。

### 4.3 素材库优先

优先使用：

1. 自有素材库。
2. 明确授权素材。
3. 用户上传资产。
4. 外部搜索结果只允许做草稿或 research preview。

### 4.4 指标闭环

每个自动生成内容都必须记录：

- prompt；
- template；
- chosen clips；
- metadata；
- publish target；
- performance metrics；
- failure logs。

否则后续无法优化“高流量内容”。

---

## 5. 代码组织总览

建议新增和修改以下目录。

```text
backend/app/api/autoflow.py
backend/app/autoflow/
  __init__.py
  schemas.py
  models.py
  service.py
  intent_parser.py
  capability_manifest.py
  template_library.py
  pipeline_builder.py
  validation_repair.py
  search_service.py
  material_selector.py
  clip_ranker.py
  rights_policy.py
  metadata_generator.py
  trend_service.py
  metrics_service.py
  examples.py

backend/app/models/autoflow.py
backend/app/schemas/autoflow.py
backend/app/node_registry/builtin/
  concat_many.py
  trim_many.py
  montage_assembler.py
  clip_ranker.py
  vertical_crop.py
  title_overlay.py
  metadata_generate.py
  rights_check.py
  thumbnail_generate.py

backend/worker/handlers/
  concat_many.py
  trim_many.py
  montage_assembler.py
  vertical_crop.py
  title_overlay.py
  metadata_generate.py
  rights_check.py
  thumbnail_generate.py

frontend/src/pages/AutoFlowPage.tsx
frontend/src/components/autoflow/
  AutoFlowPromptBox.tsx
  AutoFlowPlanPanel.tsx
  AutoFlowWorkflowPreview.tsx
  AutoFlowCandidateClips.tsx
  AutoFlowReviewGate.tsx
  AutoFlowRunStatus.tsx
  AutoFlowMetricsPanel.tsx

frontend/src/api/autoflow.ts
frontend/src/types/autoflow.ts

docs/autoflow/
  architecture.md
  templates.md
  rights-policy.md
  codex-task-guide.md

AGENTS.md
```

---

## 6. 分阶段实施计划

## Phase 0：工程准备与稳定性修复

### 0.1 新增 AGENTS.md

在仓库根目录新增 `AGENTS.md`，指导 Codex：

```md
# AGENTS.md

## Project
VideoProcess is a FastAPI + React media workflow platform.

## Backend
- Python package root: backend/
- API modules: backend/app/api/
- Services: backend/app/services/
- Node registry: backend/app/node_registry/
- Worker handlers: backend/worker/handlers/

## Frontend
- React + TypeScript + Vite root: frontend/
- Pages: frontend/src/pages/
- API client: frontend/src/api/
- Editor components: frontend/src/components/editor/

## Required checks
Run the following when files change:

### Backend
cd backend
python -m pytest
python -m ruff check . || true
python -m mypy app || true

### Frontend
cd frontend
npm install
npm run build
npm run lint || true

## Rules
- Do not remove existing APIs unless explicitly requested.
- Add tests for all new services.
- Keep generated pipeline definitions compatible with backend/app/schemas/pipeline.py.
- All AutoFlow-generated workflows must pass validate_pipeline().
- Default publication privacy must be private or unlisted.
```

### 0.2 修复 `JobEngine.on_node_failed()` retry bug

现有 `on_node_failed()` retry 分支里使用 `deps`，但该变量在当前作用域未定义。应改为从 `dep_map` 获取当前节点依赖：

```python
node_deps = dep_map.get(ne.node_id, [])
preferred_hosts = self._preferred_hosts_for_node(ne_by_node_id, node_deps)
```

新增测试：

```text
backend/tests/orchestrator/test_engine_retry.py
```

验收：

- 模拟节点失败一次，retry 正常重新入队。
- 不抛 `NameError`。
- preferred_hosts 为空或正确继承 upstream host。

### 0.3 建立测试目录和基础 fixtures

新增：

```text
backend/tests/fixtures/pipelines.py
backend/tests/fixtures/node_types.py
backend/tests/autoflow/
```

至少提供：

- 单 source → trim → export pipeline fixture。
- two source → concat_timeline → export pipeline fixture。
- planner search → zip_records → url_download fixture。

---

## Phase 1：AutoFlow 后端基础

### 1.1 新增 AutoFlow schemas

文件：

```text
backend/app/schemas/autoflow.py
```

核心 schema：

```python
from __future__ import annotations
from typing import Any, Literal
from pydantic import BaseModel, Field

class AutoFlowRequest(BaseModel):
    prompt: str
    target_platforms: list[str] = Field(default_factory=list)
    duration_sec: int | None = None
    aspect_ratio: Literal['9:16', '16:9', '1:1', 'auto'] = 'auto'
    source_policy: Literal[
        'owned_only',
        'licensed_only',
        'public_domain_or_cc',
        'research_only',
        'remix_with_review'
    ] = 'owned_only'
    publish_mode: Literal['preview_only', 'private_upload', 'unlisted_upload', 'public_after_review'] = 'preview_only'
    material_library_ids: list[str] = Field(default_factory=list)
    user_constraints: dict[str, Any] = Field(default_factory=dict)

class AutoFlowIntent(BaseModel):
    intent_type: str
    subject: str
    style: str = 'auto'
    duration_sec: int = 30
    aspect_ratio: str = '9:16'
    target_platforms: list[str] = Field(default_factory=list)
    source_policy: str = 'owned_only'
    publish_mode: str = 'preview_only'
    keywords: list[str] = Field(default_factory=list)
    negative_keywords: list[str] = Field(default_factory=list)
    needs_voiceover: bool = False
    needs_subtitles: bool = True
    needs_bgm: bool = True

class AutoFlowClipCandidate(BaseModel):
    id: str
    title: str
    source_type: str
    url: str | None = None
    asset_id: str | None = None
    start_sec: float | None = None
    end_sec: float | None = None
    score: float = 0
    score_breakdown: dict[str, float] = Field(default_factory=dict)
    rights_status: str = 'unknown'
    metadata: dict[str, Any] = Field(default_factory=dict)

class AutoFlowMetadata(BaseModel):
    title_candidates: list[str] = Field(default_factory=list)
    selected_title: str | None = None
    description: str = ''
    tags: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    thumbnail_text_candidates: list[str] = Field(default_factory=list)
    platform_payloads: dict[str, dict[str, Any]] = Field(default_factory=dict)

class AutoFlowPlan(BaseModel):
    plan_id: str
    request: AutoFlowRequest
    intent: AutoFlowIntent
    template_id: str
    pipeline_definition: dict[str, Any]
    candidates: list[AutoFlowClipCandidate] = Field(default_factory=list)
    metadata: AutoFlowMetadata = Field(default_factory=AutoFlowMetadata)
    validation: dict[str, Any] = Field(default_factory=dict)
    rights: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    needs_review: bool = True

class AutoFlowExecuteRequest(BaseModel):
    plan_id: str | None = None
    plan: AutoFlowPlan | None = None
    save_as_template: bool = False
    execute: bool = True
    review_approved: bool = False
```

### 1.2 新增 AutoFlow DB models

文件：

```text
backend/app/models/autoflow.py
```

建议表：

#### `autoflow_plans`

| 字段 | 类型 | 说明 |
|---|---|---|
| id | UUID | plan id |
| prompt | text | 用户原始输入 |
| intent_json | JSONB | 解析后的 intent |
| template_id | string | 使用的模板 |
| pipeline_definition | JSONB | 生成的 workflow |
| candidates_json | JSONB | 候选素材 |
| metadata_json | JSONB | 标题、描述、tags 等 |
| rights_json | JSONB | 权限/来源判断 |
| validation_json | JSONB | validate 结果 |
| status | string | drafted/validated/approved/executed/rejected |
| created_at | datetime | 创建时间 |
| updated_at | datetime | 更新时间 |

#### `autoflow_runs`

| 字段 | 类型 | 说明 |
|---|---|---|
| id | UUID | run id |
| plan_id | UUID | 对应 plan |
| pipeline_id | UUID | 创建出的 pipeline |
| job_id | UUID | 创建出的 job |
| status | string | pending/running/succeeded/failed |
| artifacts_json | JSONB | 输出 artifact |
| publish_json | JSONB | 发布状态 |
| created_at | datetime | 创建时间 |
| updated_at | datetime | 更新时间 |

#### `content_metrics`

| 字段 | 类型 | 说明 |
|---|---|---|
| id | UUID | metric id |
| run_id | UUID | 对应 AutoFlow run |
| platform | string | youtube/x/xiaohongshu/bilibili |
| platform_content_id | string | 平台内容 id |
| views | int | 播放 |
| likes | int | 点赞 |
| comments | int | 评论 |
| shares | int | 分享 |
| watch_time_sec | float | 总观看时长 |
| avg_view_duration_sec | float | 平均观看时长 |
| retention_json | JSONB | 留存曲线 |
| collected_at | datetime | 采集时间 |

#### `trend_signals`

| 字段 | 类型 | 说明 |
|---|---|---|
| id | UUID | signal id |
| source | string | google_trends/x/youtube/bilibili/xiaohongshu/manual |
| keyword | string | 热词 |
| score | float | 趋势分 |
| metadata_json | JSONB | 原始数据 |
| observed_at | datetime | 观测时间 |

### 1.3 新增 API router

文件：

```text
backend/app/api/autoflow.py
```

路由：

```text
POST /api/v1/autoflow/plan
GET  /api/v1/autoflow/plans
GET  /api/v1/autoflow/plans/{plan_id}
POST /api/v1/autoflow/plans/{plan_id}/approve
POST /api/v1/autoflow/execute
GET  /api/v1/autoflow/runs
GET  /api/v1/autoflow/runs/{run_id}
POST /api/v1/autoflow/runs/{run_id}/collect-metrics
GET  /api/v1/autoflow/templates
GET  /api/v1/autoflow/capabilities
```

修改：

```text
backend/app/main.py
```

加入：

```python
from app.api.autoflow import router as autoflow_router
app.include_router(autoflow_router)
```

### 1.4 AutoFlow service 骨架

文件：

```text
backend/app/autoflow/service.py
```

核心流程：

```python
class AutoFlowService:
    async def plan(self, request: AutoFlowRequest) -> AutoFlowPlan:
        intent = await self.intent_parser.parse(request)
        capabilities = self.capability_manifest.build()
        template = self.template_library.select(intent, capabilities)
        candidates = await self.material_selector.find_candidates(intent, request)
        ranked_candidates = await self.clip_ranker.rank(intent, candidates)
        metadata = await self.metadata_generator.generate(intent, ranked_candidates)
        definition = self.pipeline_builder.build(template, intent, ranked_candidates, metadata)
        validation = validate_pipeline(PipelineDefinition.model_validate(definition))
        if not validation.valid:
            definition, validation = await self.validation_repair.repair(definition, validation, max_attempts=3)
        rights = self.rights_policy.evaluate(request, ranked_candidates, metadata)
        return AutoFlowPlan(...)

    async def execute(self, request: AutoFlowExecuteRequest) -> AutoFlowRun:
        # load plan
        # verify rights + review policy
        # create pipeline
        # create job
        # start job
        # persist run
```

### 1.5 验收标准

- `GET /api/v1/autoflow/capabilities` 返回所有节点、端口、参数、worker_type。
- `POST /api/v1/autoflow/plan` 能基于一个固定模板生成有效 pipeline。
- 生成的 pipeline 能被 `validate_pipeline()` 验证。
- `POST /api/v1/autoflow/execute` 能保存 pipeline 并提交 job。
- 外部来源且 `publish_mode=public_after_review` 时，未审核不得执行公开发布。

---

## Phase 2：能力图谱与模板库

### 2.1 Capability Manifest

文件：

```text
backend/app/autoflow/capability_manifest.py
```

输出结构：

```json
{
  "nodes": [
    {
      "type_name": "trim",
      "category": "transform",
      "inputs": [{"name": "input", "type": "video"}],
      "outputs": [{"name": "output", "type": "video"}],
      "params": [{"name": "start_time", "type": "string"}],
      "worker_type": "ffmpeg",
      "autoflow_tags": ["clip", "duration", "preprocess"],
      "suitable_for": ["compilation", "shorts", "remix"]
    }
  ]
}
```

不要只返回 registry 原始数据。需要增加 AutoFlow 用标签：

- `source`
- `search`
- `planner`
- `clip_selection`
- `transform`
- `timeline`
- `layout`
- `audio`
- `subtitle`
- `metadata`
- `publish`
- `safety`

### 2.2 Template Library

文件：

```text
backend/app/autoflow/template_library.py
```

模板对象：

```python
class WorkflowTemplate(BaseModel):
    id: str
    name: str
    description: str
    intent_types: list[str]
    required_capabilities: list[str]
    default_slots: dict[str, Any]
    node_blueprint: list[dict[str, Any]]
    edge_blueprint: list[dict[str, Any]]
    slot_mapping: dict[str, Any]
```

### 2.3 必备 3 个模板

#### Template 1：`animal_compilation_short`

适用：

- 小猫视频集锦。
- 小狗搞笑集锦。
- 宠物治愈视频。

默认流程：

```text
material_search / platform_search
  → clip_ranker
  → source/url_download
  → trim_many
  → vertical_crop
  → montage_assembler
  → bgm
  → title_overlay
  → transcode
  → export
  → optional youtube_upload(private/unlisted)
```

#### Template 2：`hot_topic_explainer_short`

适用：

- 30 秒解释热点。
- “一张图看懂”。
- “今天发生了什么”。

默认流程：

```text
trend_discovery
  → script_generate
  → material_search / image_search / source
  → subtitle_to_speech
  → montage_assembler
  → subtitle
  → transcode
  → export
```

#### Template 3：`material_library_remix`

适用：

- 从已有素材库中找片段并生成短视频。
- 最低版权风险的自有素材自动创作。

默认流程：

```text
material_search
  → clip_ranker
  → source
  → trim_many
  → montage_assembler
  → metadata_generate
  → export
```

### 2.4 验收标准

- 至少 3 个模板可通过单元测试加载。
- 每个模板的 required capabilities 能在 NodeTypeRegistry 中找到。
- 模板 slot 缺失时返回明确错误。
- `animal_compilation_short` 可基于 “小猫视频集锦” 生成 pipeline。

---

## Phase 3：自然语言意图解析

### 3.1 Intent Parser

文件：

```text
backend/app/autoflow/intent_parser.py
```

功能：

- 把用户 prompt 转成 `AutoFlowIntent`。
- 支持 fallback rule-based parser。
- 支持 LLM parser，但 LLM 输出必须经过 Pydantic 校验。
- 不得把 LLM 输出直接当成 workflow。

示例：

输入：

```text
我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览。
```

输出：

```json
{
  "intent_type": "animal_compilation",
  "subject": "小猫",
  "style": "cute_fast_montage",
  "duration_sec": 30,
  "aspect_ratio": "9:16",
  "target_platforms": ["youtube_shorts"],
  "source_policy": "owned_only",
  "publish_mode": "preview_only",
  "keywords": ["小猫", "可爱", "搞笑", "kitten", "cat"],
  "needs_subtitles": true,
  "needs_bgm": true
}
```

### 3.2 Rule-based fallback

规则示例：

```python
if any(word in prompt for word in ["小猫", "猫", "cat", "kitten"]):
    intent_type = "animal_compilation"
    subject = "cat"

if "竖屏" in prompt or "shorts" in prompt.lower():
    aspect_ratio = "9:16"

if "不要发布" in prompt or "预览" in prompt:
    publish_mode = "preview_only"
```

### 3.3 验收标准

- 不依赖 LLM 时，至少能解析：小猫集锦、热点解释、素材库混剪。
- 解析失败时返回 `intent_type=generic_video` 和需要确认的问题。
- LLM 输出字段缺失时能补默认值。

---

## Phase 4：后端化搜索、选材和评分

### 4.1 Search Service

文件：

```text
backend/app/autoflow/search_service.py
```

将前端当前做的 remote search 能力迁到后端：

- YouTube search。
- X search。
- 小红书 search。
- Bilibili search。
- Material search。

服务接口：

```python
class SearchService:
    async def search_youtube(self, query: str, max_results: int) -> list[AutoFlowClipCandidate]: ...
    async def search_x(self, query: str, max_results: int) -> list[AutoFlowClipCandidate]: ...
    async def search_xiaohongshu(self, query: str, max_results: int) -> list[AutoFlowClipCandidate]: ...
    async def search_bilibili(self, query: str, max_results: int) -> list[AutoFlowClipCandidate]: ...
    async def search_material(self, intent: AutoFlowIntent, library_ids: list[str]) -> list[AutoFlowClipCandidate]: ...
```

### 4.2 Material Selector

文件：

```text
backend/app/autoflow/material_selector.py
```

优先级：

1. `source_policy=owned_only`：只查 material library 和 uploaded assets。
2. `licensed_only`：查带 license metadata 的素材。
3. `research_only`：可查平台搜索，但不允许 public publish。
4. `remix_with_review`：可查平台搜索，但需要人工审核。

### 4.3 Clip Ranker

文件：

```text
backend/app/autoflow/clip_ranker.py
```

评分模型先用规则：

```text
clip_score =
  0.25 * topic_relevance
+ 0.15 * duration_fit
+ 0.15 * visual_motion_score
+ 0.10 * first_seconds_hook_score
+ 0.10 * aspect_ratio_fit
+ 0.10 * quality_score
+ 0.05 * source_reputation
+ 0.05 * novelty_score
- 0.20 * copyright_risk
- 0.10 * duplicate_penalty
- 0.10 * watermark_penalty
```

字段：

```json
{
  "topic_relevance": 0.9,
  "duration_fit": 0.8,
  "visual_motion_score": 0.7,
  "first_seconds_hook_score": 0.6,
  "aspect_ratio_fit": 1.0,
  "quality_score": 0.8,
  "copyright_risk": 0.2,
  "duplicate_penalty": 0.0,
  "watermark_penalty": 0.0
}
```

### 4.4 去重

实现：

- URL 去重。
- asset_id 去重。
- title + duration 相似度去重。
- 同一 source video 的时间段 overlap 去重。

### 4.5 验收标准

- `owned_only` 不返回外部 URL candidates。
- `research_only` 返回外部 URL 但 rights status 为 `review_required`。
- 小猫集锦至少能返回 5 个候选素材或明确提示素材不足。
- ranker 输出 score 和 score_breakdown。

---

## Phase 5：PipelineDefinition Builder 与自动修复

### 5.1 Pipeline Builder

文件：

```text
backend/app/autoflow/pipeline_builder.py
```

职责：

- 根据 template + intent + candidates 生成 nodes。
- 自动生成稳定 node id。
- 自动填默认参数。
- 自动连边。
- 自动布局 position。
- 根据来源策略决定用 `source` 还是 `url_download`。
- 根据 publish_mode 决定是否加 upload 节点。

node id 规则：

```text
src_1, src_2, src_3
trim_1, trim_2, trim_3
montage_1
bgm_1
overlay_1
transcode_1
export_1
youtube_upload_1
```

### 5.2 Auto layout

简单布局：

```python
x = stage_index * 260
y = row_index * 140
```

阶段：

```text
source/search → clip operations → assembly → enhancement → output
```

### 5.3 Validation Repair

文件：

```text
backend/app/autoflow/validation_repair.py
```

修复规则：

| 错误 | 修复方式 |
|---|---|
| `unknown_node_type` | 替换为 template 中 fallback node，或删除 optional node |
| `port_type_mismatch` | 插入 transcode 或替换连接 handle |
| `missing_required_input` | 补 source/url_download，或删除依赖该输入的 node |
| `invalid_param` | 使用 NodeTypeRegistry 默认值或 AutoFlow 默认值 |
| `cycle_detected` | 拒绝 plan，不自动修复 |
| `missing_asset` | 若有 candidate asset_id，填入；否则返回素材不足 |

### 5.4 验收标准

- Builder 输出结构符合 `PipelineDefinition`。
- 每个模板至少一个 fixture 能 validate 通过。
- invalid_param 能被默认值修复。
- cycle_detected 不被沉默处理。
- repair 过程有日志和 `validation_json`。

---

## Phase 6：新增节点与 worker handler

### 6.1 `concat_many`

#### Node definition

文件：

```text
backend/app/node_registry/builtin/concat_many.py
```

功能：顺序拼接多个视频。

参数：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| input_count | number | 6 | 输入视频数量 |
| output_format | select | mp4 | 输出格式 |
| transition | select | none | none/fade/dissolve |
| transition_duration | number | 0.3 | 转场时长 |
| target_duration | number | 30 | 目标总时长，0 表示不限制 |
| normalize_resolution | boolean | true | 是否统一分辨率 |
| width | number | 1080 | 输出宽 |
| height | number | 1920 | 输出高 |

动态端口：

当前 registry 的端口是静态 dataclass。可以先实现固定最大 12 输入：

```text
video_1 ... video_12
```

`input_count` 控制实际使用数量。

#### Handler

文件：

```text
backend/worker/handlers/concat_many.py
```

实现方式：

- 使用 ffmpeg concat demuxer 或 filter_complex。
- 统一分辨率、fps、音频采样率。
- 支持无音频视频。
- target_duration 超出时按顺序截断最后一段。

验收：

- 2、3、6 个输入都能输出 mp4。
- 输入无音频不失败。
- 输入不同分辨率能统一输出。

### 6.2 `trim_many`

功能：批量裁剪多个 source。

两种实现路径：

#### 推荐路径 A：不做多输出节点

因为当前 orchestration 假设每个 node 产生一个 output artifact，多输出会侵入较大。建议 MVP 不做 `trim_many` 真实多输出，而是由 PipelineBuilder 生成多个 `trim` 节点：

```text
source_1 → trim_1
source_2 → trim_2
source_3 → trim_3
```

#### 长期路径 B：新增 artifact list port

新增 `PortType.ARTIFACT_LIST`，使 `trim_many` 输出列表，再由 `concat_many` 接收列表。这需要修改 orchestrator artifact mapping，暂缓。

### 6.3 `montage_assembler`

文件：

```text
backend/app/node_registry/builtin/montage_assembler.py
backend/worker/handlers/montage_assembler.py
```

定位：比 `concat_many` 更智能，适合短视频。

参数：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| style | select | cute_fast | cute_fast/fail_compilation/explainer/cinematic |
| target_duration | number | 30 | 目标秒数 |
| aspect_ratio | select | 9:16 | 9:16/16:9/1:1 |
| beat_sync | boolean | false | 是否按音乐节拍切换 |
| max_clip_duration | number | 5 | 单片段最长 |
| min_clip_duration | number | 1.5 | 单片段最短 |
| intro_hook | boolean | true | 是否把最高分片段放开头 |

MVP 可直接复用 `concat_many` 逻辑；后续再加 beat sync。

### 6.4 `vertical_crop`

功能：将任意视频转换为竖屏。

参数：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| mode | select | center_crop | center_crop/blur_bg/smart_subject |
| width | number | 1080 | 宽 |
| height | number | 1920 | 高 |
| background | select | blur | blur/black/white |

MVP：center crop + blur background。

长期：接视觉主体检测。

### 6.5 `title_overlay`

功能：给视频首屏或全程加大字标题。

参数：

| 参数 | 类型 | 默认 | 说明 |
|---|---|---|---|
| text | string | '' | 覆盖文字 |
| position | select | top | top/center/bottom |
| start_time | number | 0 | 开始秒 |
| duration | number | 3 | 显示秒数 |
| font_size | number | 72 | 字号 |
| safe_area | boolean | true | 避免平台 UI 遮挡 |

### 6.6 `metadata_generate`

建议先作为 backend service，不一定进入 worker。若作为 node：

输入：video/artifact。  
输出：metadata artifact JSON。

MVP 更简单：在 AutoFlow plan 阶段生成 metadata，写入 upload 节点参数。

### 6.7 `rights_check`

功能：对候选素材和发布计划做 gate。

输入：metadata 或 candidates。  
输出：rights report JSON。

MVP：作为 AutoFlow service 实现，不作为 worker node。

### 6.8 注册节点

修改：

```text
backend/app/node_registry/builtin/__init__.py
backend/worker/handlers/__init__.py
```

把新增 node definition 和 handler 加进去。

### 6.9 验收标准

- 新节点出现在 `/api/v1/node-types`。
- 新节点可在前端 NodePalette 显示。
- handler map 能处理新 node_type。
- `concat_many` 能跑通 worker。
- AutoFlow 小猫集锦模板能用新节点生成有效 pipeline。

---

## Phase 7：版权与发布安全策略

### 7.1 Rights Policy

文件：

```text
backend/app/autoflow/rights_policy.py
```

策略：

```python
class RightsDecision(BaseModel):
    status: Literal['allowed', 'review_required', 'blocked']
    reasons: list[str]
    allowed_publish_modes: list[str]
```

规则：

| 来源 | source_policy | 默认动作 |
|---|---|---|
| uploaded asset | owned_only | allowed |
| material library owned | owned_only | allowed |
| material library licensed | licensed_only | allowed |
| YouTube/X/Bilibili/小红书 URL | research_only | preview only |
| YouTube/X/Bilibili/小红书 URL | remix_with_review | private/unlisted + review_required |
| unknown source | any | blocked or review_required |

### 7.2 发布节点参数强制改写

如果 plan 未审核：

- `youtube_upload.privacy` 强制为 `private`。
- X upload 不执行，或只生成 draft 文案。
- 小红书发布不执行，或只生成 draft。

### 7.3 made_for_kids 判断

不要自动设置 `made_for_kids=yes`。默认：

```text
made_for_kids = not_set
```

如果内容明确面向儿童，提示人工审核。

### 7.4 验收标准

- 外部下载素材不得自动 public 发布。
- 未审核 plan 执行 public_after_review 必须返回 400。
- rights report 会写入 `autoflow_plans.rights_json`。
- 前端显示 blocked/review_required 原因。

---

## Phase 8：自动元数据与内容策略

### 8.1 Metadata Generator

文件：

```text
backend/app/autoflow/metadata_generator.py
```

输出：

- title_candidates。
- selected_title。
- description。
- tags。
- hashtags。
- thumbnail_text_candidates。
- platform_payloads。

### 8.2 标题生成规则

对小猫集锦：

```text
- 30 秒内看完 8 只小猫的离谱瞬间
- 这些小猫的反应也太可爱了
- 小猫：我只是路过，结果翻车了
- 今日份小猫快乐源泉
- 这只小猫的最后 2 秒太好笑了
```

### 8.3 平台差异化

#### YouTube Shorts

- 标题短。
- Tags 可多。
- 描述简洁。
- 默认 privacy private/unlisted。

#### X

- 文案更短。
- 可加一个问题引导评论。
- 不默认自动发。

#### 小红书

- 标题偏生活化。
- 正文可加 emoji，但由配置控制。
- 封面文字更重要。

#### Bilibili

- 标题可梗化。
- 描述可稍长。

### 8.4 验收标准

- 至少生成 5 个标题候选。
- 每个平台 payload 独立。
- upload 节点能接收 selected_title、description、tags。
- 用户可以在前端改 metadata 再执行。

---

## Phase 9：前端 AutoFlow 页面

### 9.1 路由

新增：

```text
/frontend/src/pages/AutoFlowPage.tsx
```

更新路由配置，让 `/autoflow` 可访问。

### 9.2 API client

新增：

```text
frontend/src/api/autoflow.ts
frontend/src/types/autoflow.ts
```

函数：

```ts
export async function createAutoFlowPlan(payload: AutoFlowRequest): Promise<AutoFlowPlan>
export async function approveAutoFlowPlan(planId: string): Promise<AutoFlowPlan>
export async function executeAutoFlowPlan(planId: string, options: ExecuteOptions): Promise<AutoFlowRun>
export async function getAutoFlowRun(runId: string): Promise<AutoFlowRun>
export async function listAutoFlowTemplates(): Promise<WorkflowTemplate[]>
export async function getAutoFlowCapabilities(): Promise<CapabilityManifest>
```

### 9.3 UI 组件

#### `AutoFlowPromptBox.tsx`

包含：

- prompt 输入框。
- 目标平台选择。
- 时长。
- 画幅。
- 来源策略。
- 发布模式。
- 素材库选择。

#### `AutoFlowPlanPanel.tsx`

展示：

- intent。
- template。
- warnings。
- rights decision。
- validation result。

#### `AutoFlowWorkflowPreview.tsx`

展示生成 pipeline。可复用 React Flow，但只读。

#### `AutoFlowCandidateClips.tsx`

展示候选素材：

- title。
- thumbnail。
- duration。
- score。
- score breakdown。
- rights status。
- 勾选/替换/锁定。

#### `AutoFlowReviewGate.tsx`

展示：

- 是否允许执行。
- 是否允许发布。
- 需要人工确认的事项。

#### `AutoFlowRunStatus.tsx`

展示：

- pipeline id。
- job id。
- 当前状态。
- 输出 artifact。
- 下载/预览链接。

### 9.4 用户流程

```text
/autoflow
  1. 输入 prompt
  2. 点 Generate Plan
  3. 看 intent + candidates + workflow
  4. 必要时编辑候选素材/标题/发布设置
  5. Approve
  6. Execute
  7. 查看 run status 和 preview artifact
```

### 9.5 验收标准

- 用户不进入 EditorPage 也能生成 plan。
- 用户可以点击查看 workflow。
- 用户可以执行 preview_only plan。
- 权限 blocked 时 Execute 按钮禁用。
- plan 执行后能跳到 job detail 或 run detail。

---

## Phase 10：视觉理解与素材库增强

### 10.1 新增素材分析服务

文件：

```text
backend/app/services/visual_analysis_service.py
```

能力：

- scene detect。
- object labels。
- OCR text。
- motion score。
- audio peak score。
- watermark detection。
- aspect ratio and crop suggestion。

MVP 可以先用 ffmpeg + OpenCV 简化实现：

- scene detect：基于 ffmpeg scene threshold。
- motion score：帧差。
- OCR：暂缓或接外部服务。
- object labels：暂缓或接外部模型。
- watermark：规则检测画面角落高对比固定区域，先弱实现。

### 10.2 MaterialClip metadata 扩展

在 `MaterialClip.metadata_json` 中加入：

```json
{
  "visual": {
    "motion_score": 0.73,
    "scene_score": 0.41,
    "object_labels": ["cat", "person"],
    "ocr_text": "",
    "watermark_score": 0.2,
    "suggested_crop": {"x": 0.2, "y": 0.0, "w": 0.6, "h": 1.0}
  }
}
```

### 10.3 修改 material search ranking

在 `material_service.py` 的结果里把 metadata 带出，供 AutoFlow ranker 使用。

### 10.4 验收标准

- ingest 后至少写入 motion_score、aspect_ratio、duration。
- AutoFlow ranker 可读取 metadata。
- 小猫集锦优先选择 motion_score 较高、duration 适中的片段。

---

## Phase 11：趋势与高流量内容系统

### 11.1 Trend Service

文件：

```text
backend/app/autoflow/trend_service.py
```

数据源分层：

#### MVP

- 手动录入趋势关键词。
- 从已有平台搜索结果中统计热门标题词。
- 从你自己的 content_metrics 中统计高表现主题。

#### 后续

- Google Trends / Trending Now。
- YouTube 搜索建议。
- X 热点。
- Bilibili 热榜。
- 小红书关键词。

### 11.2 Opportunity Score

```text
opportunity_score =
  0.30 * trend_growth
+ 0.20 * cross_platform_mentions
+ 0.20 * historical_performance_fit
+ 0.15 * material_availability
+ 0.10 * low_competition
- 0.20 * rights_risk
```

### 11.3 内容模板推荐

新增接口：

```text
GET /api/v1/autoflow/trend-suggestions
```

返回：

```json
[
  {
    "keyword": "小猫迷惑行为",
    "opportunity_score": 0.82,
    "recommended_template": "animal_compilation_short",
    "reason": "material library has 34 matching clips and previous pet videos performed well"
  }
]
```

### 11.4 验收标准

- 可以手动创建 trend signal。
- AutoFlow plan 可选择使用 trend signal。
- Dashboard 能展示趋势建议。

---

## Phase 12：指标回收与反馈闭环

### 12.1 Metrics Service

文件：

```text
backend/app/autoflow/metrics_service.py
```

功能：

- 保存手动导入 metrics。
- 从 YouTube API 拉取指标。
- X/小红书/Bilibili 先支持手动或 browser automation 采集。
- 计算 derived metrics。

Derived metrics：

```text
like_rate = likes / max(views, 1)
comment_rate = comments / max(views, 1)
share_rate = shares / max(views, 1)
avg_retention = avg_view_duration_sec / max(video_duration_sec, 1)
virality_score = weighted_sum(...)
```

### 12.2 Template performance

新增统计：

- template_id 平均播放。
- style 平均播放。
- subject 平均播放。
- title pattern 表现。
- clip source 表现。
- first 2s hook 表现。

### 12.3 验收标准

- 可以给 run 录入 metrics。
- 可以按 template 聚合 metrics。
- AutoFlow ranker 可读取历史表现作为加分项。

---

## Phase 13：自动化内容点子引擎

### 13.1 Content Strategy Service

文件：

```text
backend/app/autoflow/content_strategy.py
```

功能：

- 根据 trend signals 生成选题。
- 根据 material library 可用素材生成选题。
- 根据历史 metrics 推荐模板。
- 根据低版权风险优先级排序。

### 13.2 推荐类型

1. 宠物集锦。
2. 热点解释。
3. 评论区二创。
4. Top 5 / Top 10。
5. 反常识科普。
6. 失败瞬间集锦。
7. 治愈系素材混剪。
8. 新闻/事件时间线。
9. 产品/工具快速介绍。
10. 平台差异化重包装。

### 13.3 接口

```text
POST /api/v1/autoflow/ideas
```

输入：

```json
{
  "target_platforms": ["youtube_shorts"],
  "material_library_ids": ["..."],
  "count": 10,
  "source_policy": "owned_only"
}
```

输出：

```json
[
  {
    "idea_id": "...",
    "prompt": "做一个 30 秒小猫迷惑行为集锦",
    "template_id": "animal_compilation_short",
    "opportunity_score": 0.77,
    "estimated_material_count": 18,
    "risk": "low"
  }
]
```

---

## Phase 14：发布流程与审核状态机

### 14.1 状态机

Plan 状态：

```text
drafted
  → validated
  → needs_review
  → approved
  → executed
  → rejected
```

Run 状态：

```text
pending
  → running
  → succeeded
  → failed
  → published_private
  → published_unlisted
  → published_public
```

### 14.2 审核项

审核页面必须展示：

- 使用的素材来源。
- 权限状态。
- 输出标题/描述/tags。
- 是否含外部下载素材。
- 是否可能面向儿童。
- 发布平台和 privacy。
- 预览 artifact。

### 14.3 验收标准

- blocked plan 不能 approve。
- review_required plan 必须人工 approve 后才可执行上传。
- public publish 必须显式 approve_public。

---

## Phase 15：端到端样例

### 15.1 小猫视频集锦

Prompt：

```text
我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要公开发布。
```

期望：

- intent_type = `animal_compilation`。
- template_id = `animal_compilation_short`。
- source_policy 默认 `owned_only`。
- 若素材库有猫素材，使用 material_search。
- pipeline 通过 validate。
- run 输出 mp4 artifact。
- 不创建 public upload。

### 15.2 热点解释

Prompt：

```text
做一个 45 秒的热点解释短视频，解释今天大家为什么讨论某个 AI 工具，竖屏，有字幕和旁白。
```

期望：

- intent_type = `hot_topic_explainer`。
- template_id = `hot_topic_explainer_short`。
- 生成脚本、字幕、旁白、素材搜索 plan。
- 默认 preview_only。

### 15.3 素材库混剪

Prompt：

```text
从我的旅行素材库里找海边、日落、人物背影，做一个 20 秒治愈混剪。
```

期望：

- intent_type = `material_library_remix`。
- 只使用指定 material library。
- 输出治愈风格 montage。

---

## 7. Codex 任务拆分

下面是建议按 PR/任务提交给 Codex 的顺序。每个任务都应独立可运行、可测试、可 review。

---

### PR-00：新增 AGENTS.md 与测试基础

**Codex Prompt：**

```text
在 Ctwqk/videoprocess 仓库中新增根目录 AGENTS.md，说明 backend/frontend 结构、测试命令、代码规则和 AutoFlow 约束。然后新增 backend/tests 目录和最小 pytest 配置/fixture，使后续 AutoFlow 测试可以复用 PipelineDefinition fixture。不要修改现有功能。运行可用测试或说明无法运行原因。
```

**验收：**

- 有 `AGENTS.md`。
- 有基础测试目录。
- 不影响现有启动。

---

### PR-01：修复 JobEngine retry bug

**Codex Prompt：**

```text
修复 backend/app/orchestrator/engine.py 中 on_node_failed retry 分支使用未定义 deps 的问题。应从 dep_map 中获取当前节点依赖，并把它传给 _preferred_hosts_for_node。新增单元测试覆盖节点失败后 retry 重新入队，不再抛 NameError。保持现有行为不变。
```

**验收：**

- retry 不抛异常。
- 测试覆盖失败重试。

---

### PR-02：AutoFlow schemas 与 models

**Codex Prompt：**

```text
新增 backend/app/schemas/autoflow.py 和 backend/app/models/autoflow.py，定义 AutoFlowRequest、AutoFlowIntent、AutoFlowClipCandidate、AutoFlowMetadata、AutoFlowPlan、AutoFlowExecuteRequest，以及 autoflow_plans、autoflow_runs、content_metrics、trend_signals ORM models。不要接入业务逻辑，只完成 schema/model 和基础单元测试。
```

**验收：**

- Pydantic schema 可实例化。
- ORM model 可 import。
- 默认值符合计划。

---

### PR-03：Capability Manifest

**Codex Prompt：**

```text
实现 backend/app/autoflow/capability_manifest.py，从 NodeTypeRegistry 读取现有节点，生成 AutoFlow capability manifest。manifest 需要包含 type_name、category、inputs、outputs、params、worker_type、autoflow_tags、suitable_for。新增 GET /api/v1/autoflow/capabilities endpoint，并在 app/main.py 注册 router。新增测试确保至少包含 source、trim、url_download、material_search、youtube_upload。
```

**验收：**

- endpoint 返回 JSON。
- 包含已有节点。
- 不破坏 `/api/v1/node-types`。

---

### PR-04：Template Library

**Codex Prompt：**

```text
实现 backend/app/autoflow/template_library.py，新增 WorkflowTemplate schema 和三个内置模板：animal_compilation_short、hot_topic_explainer_short、material_library_remix。模板只定义 blueprint 和 slots，不执行。新增 GET /api/v1/autoflow/templates。测试模板可加载，并验证 required capabilities 在 capability manifest 中存在。
```

**验收：**

- 3 个模板可列出。
- required capabilities 校验通过。

---

### PR-05：Intent Parser

**Codex Prompt：**

```text
实现 backend/app/autoflow/intent_parser.py。先做 rule-based parser，支持小猫/宠物集锦、热点解释、素材库混剪三类 prompt，输出 AutoFlowIntent。支持 duration、aspect_ratio、publish_mode、source_policy 的基础解析。新增单元测试覆盖中文 prompt。
```

**验收：**

- “小猫视频集锦”解析为 animal_compilation。
- “热点解释”解析为 hot_topic_explainer。
- “旅行素材库混剪”解析为 material_library_remix。

---

### PR-06：Pipeline Builder MVP

**Codex Prompt：**

```text
实现 backend/app/autoflow/pipeline_builder.py，基于模板、intent 和候选素材生成 PipelineDefinition。先支持 animal_compilation_short 和 material_library_remix。若 candidate 有 asset_id 使用 source 节点；若 candidate 有 url 且策略允许草稿则使用 url_download。生成多个 trim 节点和 concat_many/montage placeholder，如果 concat_many 还未实现，则暂时使用二叉 concat_timeline fallback。输出必须能调用 validate_pipeline。新增测试生成 2 段、3 段素材 pipeline。
```

**验收：**

- 生成 PipelineDefinition。
- 通过 validate 或清楚标记缺少节点。
- 有稳定 node id 和 position。

---

### PR-07：Validation Repair

**Codex Prompt：**

```text
实现 backend/app/autoflow/validation_repair.py，支持 invalid_param 默认值修复、missing_asset 根据候选素材填充、port_type_mismatch 的基础错误报告。不要尝试修复 cycle_detected。新增测试覆盖 invalid_param、missing_asset 和不可修复错误。
```

**验收：**

- 可修复项能修复。
- 不可修复项返回明确 reason。

---

### PR-08：AutoFlow plan API MVP

**Codex Prompt：**

```text
实现 backend/app/autoflow/service.py 和 backend/app/api/autoflow.py 的 POST /api/v1/autoflow/plan。流程：parse intent → select template → mock/fixture candidates if no library provided → build pipeline → validate → repair → rights evaluate placeholder → persist plan。先不执行 job。新增集成测试调用 API。
```

**验收：**

- API 能返回 AutoFlowPlan。
- plan 写入 DB。
- validation 结果包含 valid/errors/warnings。

---

### PR-09：Rights Policy

**Codex Prompt：**

```text
实现 backend/app/autoflow/rights_policy.py。根据 source_policy、candidate source_type、url/asset_id 和 publish_mode 输出 allowed/review_required/blocked。未审核的外部 URL 不允许 public publish；owned asset 可 preview/export；YouTube/X/Bilibili/小红书 URL 默认 review_required。接入 AutoFlow plan API，并新增测试。
```

**验收：**

- 外部 URL public 被拦截。
- owned asset preview allowed。
- reasons 清晰。

---

### PR-10：AutoFlow execute API

**Codex Prompt：**

```text
实现 POST /api/v1/autoflow/execute：加载 plan，检查 rights/review 状态，创建 pipeline，提交 job，启动执行，创建 autoflow_run 记录。preview_only 只 export，不上传；public_after_review 必须 review_approved=true。新增测试覆盖 blocked、preview_only、private_upload 三种路径。
```

**验收：**

- 能创建 pipeline/job/run。
- blocked plan 返回 400。
- run 可查询。

---

### PR-11：concat_many node + handler

**Codex Prompt：**

```text
新增 concat_many 节点定义和 worker handler。节点最多支持 12 个 video 输入，input_count 控制实际数量。handler 使用 ffmpeg 将多个视频统一分辨率后顺序拼接，支持无音频输入。注册到 node_registry builtin 和 worker HANDLER_MAP。新增 handler 测试或最小 ffmpeg 集成测试。
```

**验收：**

- `/api/v1/node-types` 能看到 concat_many。
- 2/3 个输入可拼接。
- AutoFlow builder 改用 concat_many。

---

### PR-12：vertical_crop + title_overlay

**Codex Prompt：**

```text
新增 vertical_crop 和 title_overlay 节点定义与 worker handler。vertical_crop 支持 center_crop 和 blur_bg；title_overlay 用 ffmpeg drawtext 在视频指定时间段添加标题文字。注册节点和 handler。新增测试用短视频 fixture 验证输出存在。
```

**验收：**

- 竖屏输出尺寸正确。
- title_overlay 输出文件存在。
- AutoFlow 模板可选择加入这些节点。

---

### PR-13：Metadata Generator

**Codex Prompt：**

```text
实现 backend/app/autoflow/metadata_generator.py，基于 intent 和候选素材生成 title_candidates、description、tags、hashtags、thumbnail_text_candidates 和 platform_payloads。先用规则模板，不调用外部 LLM。接入 AutoFlow plan。测试小猫集锦至少生成 5 个标题候选。
```

**验收：**

- metadata 字段完整。
- upload 节点参数可从 metadata 填充。

---

### PR-14：Backend material selector + clip ranker

**Codex Prompt：**

```text
实现 backend/app/autoflow/material_selector.py 和 clip_ranker.py。selector 在 owned_only 下调用 material search 或已上传素材；research_only/remix_with_review 可返回外部 search candidates。ranker 输出 score 和 score_breakdown，包含 topic_relevance、duration_fit、quality_score、copyright_risk、duplicate_penalty。接入 AutoFlow plan，替换 mock candidates。新增测试。
```

**验收：**

- owned_only 不返回 URL-only candidates。
- ranker 输出可解释评分。
- 候选不足时 plan warnings 说明素材不足。

---

### PR-15：Frontend AutoFlowPage MVP

**Codex Prompt：**

```text
新增 frontend/src/pages/AutoFlowPage.tsx、frontend/src/api/autoflow.ts、frontend/src/types/autoflow.ts 和基础组件。页面支持输入 prompt、选择 source_policy/publish_mode/material libraries，调用 /api/v1/autoflow/plan，展示 intent、metadata、candidates、validation、rights，并能调用 /api/v1/autoflow/execute。更新路由和导航。运行 npm build。
```

**验收：**

- `/autoflow` 可访问。
- 可生成 plan。
- 可执行 preview_only。
- build 通过。

---

### PR-16：Workflow Preview UI

**Codex Prompt：**

```text
在 AutoFlow 页面增加只读 workflow preview，复用 React Flow 展示 plan.pipeline_definition 的 nodes 和 edges。显示节点 label、node type、validation warnings。不要影响 EditorPage。新增必要组件和类型。
```

**验收：**

- 可视化展示生成 workflow。
- 点击节点可看配置。

---

### PR-17：Review Gate UI

**Codex Prompt：**

```text
在 AutoFlow 页面增加 ReviewGate 组件，展示 rights decision、source policy、publish mode、是否需要审核。blocked 时禁用 execute；review_required 时显示 approve 按钮，调用 /autoflow/plans/{plan_id}/approve 后才能执行上传。新增后端 approve endpoint 如尚未实现。
```

**验收：**

- blocked 禁用。
- review_required 可 approve。
- approve 状态持久化。

---

### PR-18：Visual analysis MVP

**Codex Prompt：**

```text
实现 backend/app/services/visual_analysis_service.py，使用 ffmpeg/OpenCV 计算视频 duration、aspect_ratio、motion_score、scene_change_score。将结果写入 MaterialClip.metadata_json 或 Asset.media_info。material search 返回结果时带出 metadata。AutoFlow ranker 使用 motion_score 和 aspect_ratio_fit 加分。新增测试。
```

**验收：**

- 可分析短视频。
- metadata 写入。
- ranker 使用 metadata。

---

### PR-19：Trend signals 与 ideas API

**Codex Prompt：**

```text
实现 trend_signals CRUD 和 POST /api/v1/autoflow/ideas。MVP 使用手动 trend signals、material availability 和历史 content_metrics 计算 opportunity_score，返回推荐 prompt/template/risk。新增前端简单列表或后端测试即可。
```

**验收：**

- 可创建 trend signal。
- ideas API 返回推荐选题。

---

### PR-20：Metrics service

**Codex Prompt：**

```text
实现 backend/app/autoflow/metrics_service.py 和 content_metrics API。支持给 autoflow_run 手动录入 views、likes、comments、shares、avg_view_duration_sec，并按 template_id/intent_type 聚合表现。AutoFlow ranker 可读取历史表现加分。新增测试。
```

**验收：**

- 可录入 metrics。
- 可聚合 metrics。
- ranker 可使用历史表现。

---

### PR-21：端到端测试与文档

**Codex Prompt：**

```text
新增 docs/autoflow/architecture.md、docs/autoflow/templates.md、docs/autoflow/rights-policy.md、docs/autoflow/codex-task-guide.md。新增至少 3 个端到端测试或脚本：小猫视频集锦、热点解释、素材库混剪。确保 AutoFlow plan → validate → execute preview 流程可测试。运行后端测试和前端 build。
```

**验收：**

- 文档完整。
- 3 个样例可执行或有清楚 mock。
- 测试通过。

---

## 8. 关键 API 规格

### 8.1 `POST /api/v1/autoflow/plan`

请求：

```json
{
  "prompt": "我要一个 30 秒小猫视频集锦，竖屏，可爱快节奏，先导出预览，不要公开发布。",
  "target_platforms": ["youtube_shorts"],
  "duration_sec": 30,
  "aspect_ratio": "9:16",
  "source_policy": "owned_only",
  "publish_mode": "preview_only",
  "material_library_ids": ["library-id"]
}
```

响应：

```json
{
  "plan_id": "uuid",
  "intent": {
    "intent_type": "animal_compilation",
    "subject": "cat",
    "style": "cute_fast_montage",
    "duration_sec": 30,
    "aspect_ratio": "9:16"
  },
  "template_id": "animal_compilation_short",
  "pipeline_definition": {
    "nodes": [],
    "edges": [],
    "viewport": {"x": 0, "y": 0, "zoom": 1}
  },
  "candidates": [],
  "metadata": {
    "title_candidates": [],
    "description": "",
    "tags": []
  },
  "validation": {
    "valid": true,
    "errors": [],
    "warnings": []
  },
  "rights": {
    "status": "allowed",
    "reasons": [],
    "allowed_publish_modes": ["preview_only", "private_upload"]
  },
  "needs_review": false
}
```

### 8.2 `POST /api/v1/autoflow/execute`

请求：

```json
{
  "plan_id": "uuid",
  "execute": true,
  "review_approved": false
}
```

响应：

```json
{
  "run_id": "uuid",
  "plan_id": "uuid",
  "pipeline_id": "uuid",
  "job_id": "uuid",
  "status": "running"
}
```

### 8.3 `POST /api/v1/autoflow/ideas`

请求：

```json
{
  "target_platforms": ["youtube_shorts"],
  "material_library_ids": ["library-id"],
  "count": 10,
  "source_policy": "owned_only"
}
```

响应：

```json
[
  {
    "idea_id": "uuid",
    "prompt": "做一个 30 秒小猫迷惑行为集锦",
    "template_id": "animal_compilation_short",
    "opportunity_score": 0.82,
    "estimated_material_count": 18,
    "risk": "low"
  }
]
```

---

## 9. 数据与配置建议

### 9.1 环境变量

新增：

```env
AUTOFLOW_ENABLED=true
AUTOFLOW_DEFAULT_SOURCE_POLICY=owned_only
AUTOFLOW_DEFAULT_PUBLISH_MODE=preview_only
AUTOFLOW_MAX_REPAIR_ATTEMPTS=3
AUTOFLOW_MAX_CANDIDATES=20
AUTOFLOW_MIN_CANDIDATES=5
AUTOFLOW_REQUIRE_REVIEW_FOR_EXTERNAL_URL=true
AUTOFLOW_ALLOW_PUBLIC_UPLOAD=false
AUTOFLOW_TREND_COLLECTION_ENABLED=false
```

### 9.2 默认策略

```python
DEFAULT_AUTOFLOW_POLICY = {
    "source_policy": "owned_only",
    "publish_mode": "preview_only",
    "require_review_for_external_url": True,
    "allow_public_upload": False,
    "max_repair_attempts": 3,
}
```

---

## 10. 测试计划

### 10.1 后端单元测试

```text
backend/tests/autoflow/test_intent_parser.py
backend/tests/autoflow/test_capability_manifest.py
backend/tests/autoflow/test_template_library.py
backend/tests/autoflow/test_pipeline_builder.py
backend/tests/autoflow/test_validation_repair.py
backend/tests/autoflow/test_rights_policy.py
backend/tests/autoflow/test_metadata_generator.py
backend/tests/autoflow/test_clip_ranker.py
backend/tests/autoflow/test_autoflow_api.py
```

### 10.2 worker 测试

```text
backend/tests/worker/test_concat_many_handler.py
backend/tests/worker/test_vertical_crop_handler.py
backend/tests/worker/test_title_overlay_handler.py
```

### 10.3 前端测试/构建

```bash
cd frontend
npm run build
npm run lint
```

### 10.4 端到端脚本

```text
scripts/autoflow_demo_cat_compilation.py
scripts/autoflow_demo_material_remix.py
scripts/autoflow_demo_hot_topic.py
```

### 10.5 验收 Checklist

- [ ] `/api/v1/autoflow/capabilities` 正常。
- [ ] `/api/v1/autoflow/templates` 返回 3 个模板。
- [ ] 小猫 prompt 能生成 plan。
- [ ] plan.pipeline_definition 能 validate。
- [ ] preview_only 能 execute。
- [ ] rights blocked 能阻止执行。
- [ ] AutoFlow 页面能显示 plan。
- [ ] concat_many worker 能输出视频。
- [ ] metadata 生成至少 5 个标题。
- [ ] metrics 可录入。
- [ ] ideas API 可返回推荐选题。

---

## 11. 风险与处理

### 11.1 LLM 输出不稳定

处理：

- LLM 不直接生成 graph。
- 使用 Pydantic schema 校验。
- 使用模板和 deterministic builder。
- 提供 rule-based fallback。

### 11.2 外部平台版权风险

处理：

- 默认 `owned_only`。
- 外部 URL 默认 `review_required`。
- public publish 默认关闭。
- 权限状态进入 plan 和 UI。

### 11.3 动态多输入节点复杂

处理：

- MVP 使用固定 12 输入端口。
- 长期再做 `PortType.ARTIFACT_LIST`。

### 11.4 素材不足

处理：

- plan 返回 warnings。
- ideas API 根据 material availability 打分。
- 支持生成“素材采集任务”。

### 11.5 任务太大导致 Codex 一次性失败

处理：

- 严格按 PR-00 到 PR-21 拆分。
- 每个 PR 只实现一层。
- 每个 PR 有验收和测试。

---

## 12. 最终完成定义

完成后，系统应满足：

1. 用户可在 `/autoflow` 输入自然语言 prompt。
2. 后端能解析 intent。
3. 后端能选择模板。
4. 后端能从素材库或允许的平台来源找候选素材。
5. 后端能给候选素材评分、去重、排序。
6. 后端能生成 `PipelineDefinition`。
7. 生成的 workflow 能通过 validate 或给出明确不可修复原因。
8. 用户能预览 workflow、候选素材、metadata、rights。
9. 用户能执行 preview/export job。
10. 系统默认不公开发布未经审核的外部来源内容。
11. 至少支持小猫视频集锦、热点解释、素材库混剪 3 类自动化内容。
12. 新增 concat_many/vertical_crop/title_overlay 等集锦必要节点。
13. 可以录入或采集发布表现 metrics。
14. 可以根据趋势和历史表现生成内容 idea。
15. 所有关键路径有测试和文档。

---

## 13. 建议的开发顺序总表

| 顺序 | 任务 | 优先级 | 依赖 |
|---:|---|---|---|
| 0 | AGENTS.md + 测试基础 | P0 | 无 |
| 1 | 修复 retry bug | P0 | 无 |
| 2 | AutoFlow schemas/models | P0 | 0 |
| 3 | Capability manifest | P0 | 2 |
| 4 | Template library | P0 | 3 |
| 5 | Intent parser | P0 | 2 |
| 6 | Pipeline builder | P0 | 3,4,5 |
| 7 | Validation repair | P0 | 6 |
| 8 | Plan API | P0 | 2-7 |
| 9 | Rights policy | P0 | 8 |
| 10 | Execute API | P0 | 8,9 |
| 11 | concat_many | P1 | 6 |
| 12 | vertical_crop/title_overlay | P1 | 11 |
| 13 | metadata generator | P1 | 8 |
| 14 | material selector/ranker | P1 | 8 |
| 15 | AutoFlow frontend | P1 | 8,10 |
| 16 | workflow preview UI | P1 | 15 |
| 17 | review gate UI | P1 | 9,15 |
| 18 | visual analysis MVP | P2 | 14 |
| 19 | trend ideas API | P2 | 20 |
| 20 | metrics service | P2 | 10 |
| 21 | docs/e2e | P0 | 全部核心任务 |

---

## 14. 第一轮 MVP 只做这些

如果想最快看到“我要小猫视频集锦”跑起来，第一轮只做：

1. PR-00 AGENTS.md。
2. PR-01 retry bug。
3. PR-02 schemas/models。
4. PR-03 capability manifest。
5. PR-04 template library。
6. PR-05 intent parser。
7. PR-06 pipeline builder。
8. PR-08 plan API。
9. PR-09 rights policy。
10. PR-10 execute API。
11. PR-11 concat_many。
12. PR-13 metadata generator。
13. PR-15 AutoFlow 页面。

先不做趋势、视觉理解、metrics，也能形成可用闭环：

```text
prompt → plan → workflow → validate → preview/export
```

---

## 15. 第二轮再做增长闭环

第二轮做：

1. material selector + ranker。
2. visual analysis。
3. metrics service。
4. trend signals。
5. ideas API。
6. template performance。
7. 自动化 A/B metadata。

形成：

```text
prompt/idea → auto video → publish draft → metrics → better ranking/template
```

---

## 16. 给 Codex 的总提示词

可以把下面这段作为 Codex 的总任务说明，然后再按 PR 拆分执行：

```text
你正在升级 Ctwqk/videoprocess。目标是实现 AutoFlow：用户输入自然语言内容需求，例如“我要一个 30 秒小猫视频集锦”，系统自动解析意图、选择工作流模板、选材、生成 PipelineDefinition、验证/修复、保存 pipeline、提交 job、导出预览，并在权限允许时进行 private/unlisted 发布。

重要约束：
1. 不要让 LLM 任意生成 workflow graph。必须通过 capability manifest + workflow template + deterministic pipeline builder。
2. 所有生成的 pipeline 必须符合 backend/app/schemas/pipeline.py，并通过 validate_pipeline()。
3. 外部平台 URL 素材默认不得公开发布。public publish 必须人工审核。
4. 默认 source_policy=owned_only，publish_mode=preview_only。
5. 每个功能都要有测试。
6. 不要破坏现有 EditorPage、Pipeline API、Job API、worker handler。
7. 新节点必须同时注册 node_registry builtin 和 worker HANDLER_MAP。
8. 前端 AutoFlow 页面应独立于现有 EditorPage，但可以复用 React Flow 只读预览。

请按 PR-00 到 PR-21 的顺序逐步实现，每次只做一个 PR 的范围，运行相关测试，并给出修改摘要和测试结果。
```

---

## 17. 交付后推荐运行命令

```bash
# Backend
cd backend
python -m pytest
python -m ruff check .
python -m mypy app

# Frontend
cd frontend
npm install
npm run build
npm run lint

# Full stack smoke test
cd ..
docker compose up --build
```

---

## 18. 最终产品形态

完成后，VideoProcess 将从当前的“节点式视频处理工具”升级为：

```text
内容自动化平台
  = 自然语言创作入口
  + 受控 workflow planner
  + 智能素材库
  + 自动剪辑/拼接/字幕/BGM/导出
  + 可控发布
  + 趋势和指标反馈闭环
```

这会让“我要小猫视频集锦”变成一个完整自动化链路，而不是只在编辑器里手动拖节点。
