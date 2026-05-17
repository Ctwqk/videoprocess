# VideoProcess AutoFlow Mode C Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `Design/videoprocess_autoflow_upgrade_plan.md` target as two merge-tested phases with parallel feature worktrees.

**Architecture:** Phase 1 builds the usable AutoFlow MVP: controlled prompt-to-plan backend, safe execution gate, compilation nodes, and `/autoflow` UI. Phase 2 adds the growth loop: material selection/ranking, visual analysis, metrics, trend ideas, docs, and end-to-end examples.

**Tech Stack:** FastAPI, SQLAlchemy async ORM, Pydantic, pytest, React, TypeScript, Vite, React Flow, ffmpeg worker handlers.

---

## Baseline

- Repository root: `/home/taiwei/Constructure-repos/videoprocess`
- Integration branch: `codex/autoflow-mode-c`
- Worktree directory: `.worktrees/`
- Design source: `Design/videoprocess_autoflow_upgrade_plan.md`
- Baseline backend check: `cd backend && python3 -m pytest -q`
- Baseline frontend check: `cd frontend && npm install && npm run build`

## Phase 1: AutoFlow MVP

Phase 1 target: `prompt -> intent -> template -> PipelineDefinition -> validate/repair -> rights gate -> plan API -> execute preview/private job -> AutoFlow UI`.

### Branch 1: `codex/autoflow-p1-foundation`

**Worktree:** `.worktrees/autoflow-p1-foundation`

**Files:**
- Create: `AGENTS.md`
- Create: `backend/tests/fixtures/pipelines.py`
- Create: `backend/tests/fixtures/node_types.py`
- Create: `backend/tests/orchestrator/test_engine_retry.py`
- Modify: `backend/tests/conftest.py`

- [ ] Step 1: Add `AGENTS.md` with backend/frontend structure, required checks, and AutoFlow constraints from the design document.
- [ ] Step 2: Add reusable pipeline and node-type fixtures for source/trim/export, two-source concat/export, and planner search/url download flows.
- [ ] Step 3: Add a retry regression test proving `JobEngine.on_node_failed()` redispatches without a `NameError` and uses upstream preferred hosts.
- [ ] Step 4: Run `cd backend && python3 -m pytest -q`.

### Branch 2: `codex/autoflow-p1-backend-planner`

**Worktree:** `.worktrees/autoflow-p1-backend-planner`

**Files:**
- Create: `backend/app/autoflow/__init__.py`
- Create: `backend/app/autoflow/capability_manifest.py`
- Create: `backend/app/autoflow/intent_parser.py`
- Create: `backend/app/autoflow/metadata_generator.py`
- Create: `backend/app/autoflow/pipeline_builder.py`
- Create: `backend/app/autoflow/template_library.py`
- Create: `backend/app/autoflow/validation_repair.py`
- Create: `backend/app/schemas/autoflow.py`
- Create: `backend/app/models/autoflow.py`
- Create: `backend/alembic/versions/004_autoflow.py`
- Create: `backend/tests/autoflow/test_capability_manifest.py`
- Create: `backend/tests/autoflow/test_intent_parser.py`
- Create: `backend/tests/autoflow/test_metadata_generator.py`
- Create: `backend/tests/autoflow/test_pipeline_builder.py`
- Create: `backend/tests/autoflow/test_schemas_models.py`
- Create: `backend/tests/autoflow/test_template_library.py`
- Create: `backend/tests/autoflow/test_validation_repair.py`

- [ ] Step 1: Write failing tests for AutoFlow schemas, defaults, ORM imports, capability manifest tags, three templates, three Chinese prompt classes, metadata generation, pipeline builder validation, and repair behavior.
- [ ] Step 2: Implement schemas and ORM models matching the design, with default `owned_only` and `preview_only` safety.
- [ ] Step 3: Implement capability manifest from `NodeTypeRegistry` with AutoFlow tags and suitable-use metadata.
- [ ] Step 4: Implement the rule-based intent parser, metadata generator, template library, deterministic builder, and repair service.
- [ ] Step 5: Run `cd backend && python3 -m pytest -q tests/autoflow`.

### Branch 3: `codex/autoflow-p1-execution-nodes`

**Worktree:** `.worktrees/autoflow-p1-execution-nodes`

**Files:**
- Create: `backend/app/api/autoflow.py`
- Create: `backend/app/autoflow/rights_policy.py`
- Create: `backend/app/autoflow/service.py`
- Create: `backend/app/node_registry/builtin/concat_many.py`
- Create: `backend/app/node_registry/builtin/title_overlay.py`
- Create: `backend/app/node_registry/builtin/vertical_crop.py`
- Create: `backend/worker/handlers/concat_many.py`
- Create: `backend/worker/handlers/title_overlay.py`
- Create: `backend/worker/handlers/vertical_crop.py`
- Create: `backend/tests/autoflow/test_autoflow_api.py`
- Create: `backend/tests/autoflow/test_rights_policy.py`
- Create: `backend/tests/worker/test_concat_many_handler.py`
- Create: `backend/tests/worker/test_title_overlay_handler.py`
- Create: `backend/tests/worker/test_vertical_crop_handler.py`
- Modify: `backend/app/main.py`
- Modify: `backend/app/node_registry/builtin/__init__.py`
- Modify: `backend/worker/handlers/__init__.py`

- [ ] Step 1: Write failing tests for rights decisions, plan/approve/execute API paths, node registration, and ffmpeg handler output.
- [ ] Step 2: Implement rights policy and AutoFlow service/API endpoints.
- [ ] Step 3: Implement `concat_many`, `vertical_crop`, and `title_overlay` node definitions and handlers.
- [ ] Step 4: Register router, nodes, and worker handlers.
- [ ] Step 5: Run `cd backend && python3 -m pytest -q tests/autoflow tests/worker`.

### Branch 4: `codex/autoflow-p1-frontend`

**Worktree:** `.worktrees/autoflow-p1-frontend`

**Files:**
- Create: `frontend/src/api/autoflow.ts`
- Create: `frontend/src/components/autoflow/AutoFlowCandidateClips.tsx`
- Create: `frontend/src/components/autoflow/AutoFlowPlanPanel.tsx`
- Create: `frontend/src/components/autoflow/AutoFlowPromptBox.tsx`
- Create: `frontend/src/components/autoflow/AutoFlowReviewGate.tsx`
- Create: `frontend/src/components/autoflow/AutoFlowRunStatus.tsx`
- Create: `frontend/src/components/autoflow/AutoFlowWorkflowPreview.tsx`
- Create: `frontend/src/pages/AutoFlowPage.tsx`
- Create: `frontend/src/types/autoflow.ts`
- Modify: `frontend/src/App.tsx`
- Modify: `frontend/src/components/layout/Sidebar.tsx`

- [ ] Step 1: Add TypeScript types and API client for plan, approve, execute, templates, and capabilities.
- [ ] Step 2: Build `/autoflow` as a working app surface for prompt, settings, generated plan, readonly workflow preview, candidates, rights gate, and run status.
- [ ] Step 3: Wire route and sidebar navigation without changing `EditorPage`.
- [ ] Step 4: Run `cd frontend && npm install && npm run build`.

### Phase 1 Merge And Verification

- [ ] Merge order: foundation -> backend-planner -> execution-nodes -> frontend.
- [ ] Resolve conflicts on `backend/app/api/autoflow.py`, `backend/app/main.py`, and shared AutoFlow modules by preserving the controlled planner architecture.
- [ ] Run `cd backend && python3 -m pytest -q`.
- [ ] Run `cd frontend && npm run build`.
- [ ] Run `git diff --check`.

## Phase 2: Material, Metrics, Trends, Docs

Phase 2 target: improve planning quality and close the feedback loop after the MVP is merged.

### Branch 5: `codex/autoflow-p2-material-analysis`

**Worktree:** `.worktrees/autoflow-p2-material-analysis`

**Files:**
- Create: `backend/app/autoflow/clip_ranker.py`
- Create: `backend/app/autoflow/material_selector.py`
- Create: `backend/app/autoflow/search_service.py`
- Create: `backend/app/services/visual_analysis_service.py`
- Create: `backend/tests/autoflow/test_clip_ranker.py`
- Create: `backend/tests/autoflow/test_material_selector.py`
- Create: `backend/tests/services/test_visual_analysis_service.py`
- Modify: `backend/app/services/material_service.py`
- Modify: `backend/app/autoflow/service.py`

- [ ] Step 1: Write failing tests for `owned_only`, `research_only`, dedupe, score breakdown, and visual metadata.
- [ ] Step 2: Implement selector, search adapters with safe stubs, ranker, and visual analysis MVP.
- [ ] Step 3: Integrate selector/ranker into `AutoFlowService.plan()`.
- [ ] Step 4: Run `cd backend && python3 -m pytest -q tests/autoflow tests/services`.

### Branch 6: `codex/autoflow-p2-feedback-growth`

**Worktree:** `.worktrees/autoflow-p2-feedback-growth`

**Files:**
- Create: `backend/app/autoflow/content_strategy.py`
- Create: `backend/app/autoflow/metrics_service.py`
- Create: `backend/app/autoflow/trend_service.py`
- Create: `backend/tests/autoflow/test_content_strategy.py`
- Create: `backend/tests/autoflow/test_metrics_service.py`
- Create: `backend/tests/autoflow/test_trend_service.py`
- Modify: `backend/app/api/autoflow.py`
- Modify: `frontend/src/api/autoflow.ts`
- Modify: `frontend/src/components/autoflow/AutoFlowMetricsPanel.tsx`
- Modify: `frontend/src/pages/AutoFlowPage.tsx`
- Modify: `frontend/src/types/autoflow.ts`

- [ ] Step 1: Write failing tests for metrics save/aggregate, trend suggestions, and ideas scoring.
- [ ] Step 2: Implement metrics, trend, and content strategy services.
- [ ] Step 3: Add `trend-suggestions`, `ideas`, and metrics API routes.
- [ ] Step 4: Add a compact metrics/trend panel to the AutoFlow page.
- [ ] Step 5: Run `cd backend && python3 -m pytest -q tests/autoflow` and `cd frontend && npm run build`.

### Branch 7: `codex/autoflow-p2-docs-e2e`

**Worktree:** `.worktrees/autoflow-p2-docs-e2e`

**Files:**
- Create: `docs/autoflow/architecture.md`
- Create: `docs/autoflow/codex-task-guide.md`
- Create: `docs/autoflow/rights-policy.md`
- Create: `docs/autoflow/templates.md`
- Create: `scripts/autoflow_demo_cat_compilation.py`
- Create: `scripts/autoflow_demo_hot_topic.py`
- Create: `scripts/autoflow_demo_material_remix.py`
- Create: `backend/tests/autoflow/test_e2e_examples.py`

- [ ] Step 1: Write backend e2e-style tests for cat compilation, hot-topic explainer, and material-library remix plan generation.
- [ ] Step 2: Add demo scripts that call the API through HTTP and fail clearly on invalid plans.
- [ ] Step 3: Add architecture, template, rights policy, and Codex task-guide docs.
- [ ] Step 4: Run `cd backend && python3 -m pytest -q tests/autoflow/test_e2e_examples.py`.

### Phase 2 Merge And Verification

- [ ] Merge order: material-analysis -> feedback-growth -> docs-e2e.
- [ ] Resolve conflicts by keeping Phase 1 API compatibility and the safety defaults from rights policy.
- [ ] Run `cd backend && python3 -m pytest -q`.
- [ ] Run `cd frontend && npm run build`.
- [ ] Run `git diff --check`.

## Completion Definition

- `/api/v1/autoflow/capabilities` works.
- `/api/v1/autoflow/templates` returns three templates.
- A cat-compilation prompt produces a validated plan or a clear material-shortage warning.
- `preview_only` execution creates a pipeline/job/run path.
- External URL public publishing is blocked unless explicitly approved.
- `/autoflow` can generate and display plans without entering `EditorPage`.
- New compilation nodes are registered and worker handlers are mapped.
- Metrics and ideas APIs exist for the growth loop.
- Backend tests and frontend build pass after each phase merge.
