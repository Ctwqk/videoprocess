# AI Pipeline Planner Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add B+ AI graph planning so AutoFlow can accept a strict model-generated pipeline graph while backend contracts, validation, policy gates, and repair remain authoritative.

**Architecture:** Extend the current node registry manifest into a planner-grade capability contract, add strict `PipelineDraft` schemas and conversion into the existing `PipelineDefinition`, validate graph structure with `validate_pipeline()`, validate behavior with a new policy gate, and wire an explicit `planning_mode=ai_graph` path through AutoFlow. The first implementation supports injected/test drafts, deterministic dog/cat vertical-timeline graph planning, and a provider seam for future LLM output without allowing model-defined contracts.

**Tech Stack:** FastAPI, Pydantic v2, SQLAlchemy async models already present, React TypeScript/Vite frontend types, pytest.

---

### Task 1: Planner-Grade Capability Manifest

**Files:**
- Modify: `backend/app/autoflow/capability_manifest.py`
- Test: `backend/tests/autoflow/test_capability_manifest.py`

- [ ] **Step 1: Write failing manifest tests**

Add tests asserting `concat_many` exposes dynamic `video_{n}` inputs, `youtube_upload` exposes upload policy metadata, and `concat_vertical_timeline` exposes planner hints.

Run: `cd backend && python3 -m pytest tests/autoflow/test_capability_manifest.py -v`
Expected: FAIL because these manifest fields do not exist yet.

- [ ] **Step 2: Implement manifest contract fields**

Add Pydantic models for `DynamicInputContract`, `ExecutionContract`, `PolicyContract`, and `PlannerHints`. Populate them from registry definitions plus local overrides for planner nodes, media transforms, source nodes, and upload nodes.

- [ ] **Step 3: Verify manifest tests pass**

Run: `cd backend && python3 -m pytest tests/autoflow/test_capability_manifest.py -v`
Expected: PASS.

### Task 2: PipelineDraft Schemas and Compiler

**Files:**
- Modify: `backend/app/schemas/autoflow.py`
- Create: `backend/app/autoflow/graph_planner.py`
- Test: `backend/tests/autoflow/test_graph_planner.py`

- [ ] **Step 1: Write failing draft schema/compiler tests**

Add tests that validate a dog/cat draft converts into a `PipelineDefinition`, rejects unknown node types, rejects unknown ports after conversion validation, and auto-generates missing edge ids/positions.

Run: `cd backend && python3 -m pytest tests/autoflow/test_graph_planner.py -v`
Expected: FAIL because `PipelineDraft` and compiler do not exist.

- [ ] **Step 2: Add strict draft models**

Add `PlanningMode`, `PipelineDraft`, `DraftNode`, `DraftEdge`, `PipelineDraftPatch`, `GraphPlanningAttempt`, and `GraphPlanningResult` schemas. Extend `AutoFlowRequest` with `planning_mode`, `max_repair_attempts`, and `allow_experimental_graph_planning` defaults.

- [ ] **Step 3: Add draft compiler**

Implement `pipeline_definition_from_draft()` in `graph_planner.py`. It must reject unknown node types before conversion, fill positions when missing, generate edge ids, copy source `asset_id`, and return a normal `PipelineDefinition`.

- [ ] **Step 4: Verify draft tests pass**

Run: `cd backend && python3 -m pytest tests/autoflow/test_graph_planner.py -v`
Expected: PASS.

### Task 3: Policy Validator and Policy Repair

**Files:**
- Create: `backend/app/autoflow/pipeline_policy.py`
- Test: `backend/tests/autoflow/test_pipeline_policy.py`

- [ ] **Step 1: Write failing policy tests**

Add tests for these rules: `owned_only` blocks external search/download nodes, `preview_only` removes upload nodes as an auto repair, `private_upload` clamps upload privacy to private, `unlisted_upload` clamps public privacy to unlisted, and public publishing remains review-gated.

Run: `cd backend && python3 -m pytest tests/autoflow/test_pipeline_policy.py -v`
Expected: FAIL because the policy module does not exist.

- [ ] **Step 2: Implement policy result models and validator**

Add `PipelinePolicyIssue`, `PipelinePolicyResult`, and `validate_pipeline_policy()`. Return a repaired definition when a safe repair is possible; otherwise return `valid=False` with hard errors.

- [ ] **Step 3: Verify policy tests pass**

Run: `cd backend && python3 -m pytest tests/autoflow/test_pipeline_policy.py -v`
Expected: PASS.

### Task 4: AutoFlow AI Graph Planning Path

**Files:**
- Modify: `backend/app/autoflow/graph_planner.py`
- Modify: `backend/app/autoflow/service.py`
- Modify: `backend/app/api/autoflow.py`
- Test: `backend/tests/autoflow/test_autoflow_api.py`
- Test: `backend/tests/autoflow/test_graph_planner.py`

- [ ] **Step 1: Write failing service/API tests**

Add tests for `POST /api/v1/autoflow/plan/graph` and `POST /api/v1/autoflow/plan` with `planning_mode=ai_graph`. Tests should cover a direct draft supplied in `constraints.pipeline_draft`, the dog/cat vertical timeline deterministic planner, policy repairs in plan validation, and fallback to existing template planning when graph planning cannot produce a valid draft.

Run: `cd backend && python3 -m pytest tests/autoflow/test_graph_planner.py tests/autoflow/test_autoflow_api.py -v`
Expected: FAIL because the graph planning path is not wired.

- [ ] **Step 2: Implement graph planner service**

Add `AutoFlowGraphPlanner` with a provider seam. The default provider first reads `request.constraints["pipeline_draft"]`, then recognizes dog/cat top-bottom sequential prompts, then reports unavailable so AutoFlow can fall back. Every output must be converted, structurally validated, policy validated/repaired, and captured in `GraphPlanningAttempt` records.

- [ ] **Step 3: Wire service and API**

Call graph planning when `planning_mode == "ai_graph"`. Add `/api/v1/autoflow/plan/graph`. Populate `template_id="ai_graph"`, `validation.graph_planning`, `validation.policy`, warnings, rights, review state, and existing persistence paths.

- [ ] **Step 4: Verify graph planning tests pass**

Run: `cd backend && python3 -m pytest tests/autoflow/test_graph_planner.py tests/autoflow/test_autoflow_api.py -v`
Expected: PASS.

### Task 5: Frontend Types and Planner Details

**Files:**
- Modify: `frontend/src/types/autoflow.ts`
- Modify: `frontend/src/api/autoflow.ts`
- Modify: `frontend/src/pages/AutoFlowPage.tsx`
- Modify: `frontend/src/pages/AutoFlowPage.css`

- [ ] **Step 1: Add frontend type support**

Add `AutoFlowPlanningMode`, planner contract types, draft/attempt types, request fields, and `createAutoFlowGraphPlan()`.

- [ ] **Step 2: Render graph planner details**

Show planning mode, graph attempts, policy repairs, planner assumptions, and risk flags when present in `plan.validation.graph_planning`.

- [ ] **Step 3: Verify frontend build**

Run: `cd frontend && npm run build`
Expected: PASS.

### Task 6: Full Verification

**Files:**
- No new files unless fixing issues found by verification.

- [ ] **Step 1: Run backend tests**

Run: `cd backend && python3 -m pytest`
Expected: PASS.

- [ ] **Step 2: Run optional backend quality gates**

Run:
```bash
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```
Expected: command output recorded; missing modules are acceptable only if the environment lacks them.

- [ ] **Step 3: Run frontend checks**

Run:
```bash
cd frontend && npm install
cd frontend && npm run build
cd frontend && npm run lint || true
```
Expected: build passes; lint output recorded.

- [ ] **Step 4: Run a live API smoke if services are available**

Use the running backend or start the local stack, then submit a graph plan prompt for dog/cat vertical timeline with private upload mode. Verify the returned plan is valid, uses `concat_vertical_timeline`, and has upload privacy private.
