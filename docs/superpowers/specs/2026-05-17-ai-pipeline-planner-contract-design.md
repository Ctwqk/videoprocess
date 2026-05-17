# AI Pipeline Planner Contract Design

Date: 2026-05-17

## Summary

VideoProcess should support an AI planner that can directly generate pipeline
`nodes` and `edges` from a user prompt, while keeping backend node contracts as
the only execution authority.

The selected design is **B+ direct graph planning**:

1. The backend exports a strict planner-grade capability manifest from the node
   registry.
2. The model generates a constrained `PipelineDraft` containing nodes, edges,
   configs, assumptions, and risk flags.
3. The backend validates the draft against schema, node contracts, graph rules,
   and policy rules.
4. Validation errors are returned to the model for bounded patch-based repair.
5. If repair still fails, the backend falls back to deterministic recipe or IR
   builders.
6. Only a fully validated plan can be persisted, reviewed, or executed.

This preserves model freedom to compose workflows while preventing arbitrary
node invention, unsafe platform actions, malformed graph edges, invalid params,
or policy bypass.

## Goals

- Let AI compose real pipeline graphs instead of being limited to a small set of
  hardcoded AutoFlow templates.
- Strictly define every node's inputs, outputs, params, dynamic ports, execution
  effects, policy requirements, and planner hints.
- Allow prompts such as "top half dog video first, bottom half cat video later"
  to map naturally to existing nodes such as `concat_vertical_timeline`.
- Keep existing `PipelineDefinition` compatibility.
- Keep `validate_pipeline()` as the structural validator and extend it where
  needed.
- Add a separate policy validator for rights, upload, privacy, external source,
  and review gates.
- Never let model output define or mutate backend node contracts.

## Non-Goals

- Do not implement unrestricted arbitrary code execution.
- Do not allow LLM output to define new node types, worker handlers, or platform
  permissions.
- Do not make public uploads automatic.
- Do not remove existing AutoFlow candidate/template behavior.
- Do not require real video generation nodes in this phase.

## Current State

The repo already has these relevant pieces:

- `backend/app/node_registry/base.py` defines `NodeTypeDefinition`,
  `PortDefinition`, and `ParamDefinition`.
- `backend/app/node_registry/builtin/` contains concrete nodes such as
  `smart_trim`, `concat_many`, `concat_timeline`, `concat_vertical_timeline`,
  `material_search`, `youtube_search`, `youtube_upload`, `transcode`, and
  `export`.
- `backend/app/autoflow/capability_manifest.py` exports existing registry data
  for AutoFlow.
- `backend/app/orchestrator/dag.py` contains `validate_pipeline()`, including
  node existence, port type, dynamic video input, required input, cycle, source
  media, and param checks.
- Existing AutoFlow builders can create deterministic pipelines from candidates
  and storyboard plans.

The gap is that the current manifest is UI/runtime oriented, not planner-grade.
It does not fully express side effects, rights risk, review requirements,
dynamic port limits, structured params, common connection patterns, or repair
metadata.

## Architecture

```text
User prompt
  -> AutoFlow AI planner request
  -> Backend exports planner-grade capability manifest
  -> LLM returns PipelineDraft
  -> Draft schema validation
  -> Node contract validation
  -> Graph validation
  -> Policy validation
  -> bounded model repair loop if needed
  -> deterministic fallback if repair fails
  -> AutoFlowPlan persisted with validation, warnings, rights, review state
  -> review / execute through existing flow
```

The model can choose nodes and edges directly, but the backend controls:

- which node types exist;
- which ports and params are legal;
- which media types may connect;
- which source and publish actions are allowed;
- whether the plan can execute automatically;
- when human review is required.

## Planner-Grade Node Contract

Existing node definitions remain the source of truth, but they should be
extended or adapted into a richer contract for planning.

### Contract Fields

Each node contract should expose:

- `type_name`: stable node type id, such as `smart_trim`.
- `display_name`, `description`, `category`, `worker_type`.
- `inputs`: named ports with type, required flag, description, and optional
  dynamic pattern.
- `outputs`: named ports with type and description.
- `params`: strict parameter schema.
- `media_contract`: actual media inference rules for nodes that expose
  `any_media`.
- `execution_contract`: pure transform, planner-only, network read, file write,
  platform upload, worker requirement.
- `policy_contract`: review requirements, privacy constraints, source policy
  constraints, rights risk.
- `planner_hints`: tags, common use cases, common upstream/downstream nodes,
  examples, and safe fallbacks.

### Port Contract

Port types should remain compatible with current `PortType`, with stronger
metadata:

- `video`
- `audio`
- `image`
- `subtitle`
- `any_media`
- `search_results`
- `url_value`
- `asset_value`

Dynamic inputs should be first-class:

```yaml
dynamic_inputs:
  pattern: video_{n}
  type: video
  min_count: 2
  max_count: 64
  ordered: true
```

`concat_many` and `concat_timeline` should use this dynamic contract instead of
pretending they have a fixed small set of ports. Existing legacy handles such as
`video_first` and `video_second` can remain accepted through compatibility
mapping where needed.

### Parameter Contract

Current params support `string`, `number`, `boolean`, `select`, and `file`.
Planner-grade params should support these additional logical types:

- `array`
- `object`
- `duration`
- `timestamp`
- `time_range`
- `string_list`
- `platform_payload`

Each param can define:

- required flag;
- default value;
- numeric min/max;
- select options;
- regex pattern;
- max length;
- item schema for arrays;
- object schema for objects;
- dependent params;
- mutually exclusive params;
- auto-fill policy.

The backend validator must enforce these rules. The manifest shown to the model
is generated from the same contract.

### Example: `concat_vertical_timeline`

```yaml
type_name: concat_vertical_timeline
inputs:
  video_first:
    type: video
    required: true
  video_second:
    type: video
    required: true
  image_top:
    type: image
    required: false
  image_bottom:
    type: image
    required: false
outputs:
  output:
    type: video
params:
  pane_width:
    type: number
    default: 640
    min: 160
    max: 2160
  pane_height:
    type: number
    default: 360
    min: 90
    max: 2160
  background_color:
    type: string
    default: black
  output_format:
    type: select
    default: mp4
    options: [mp4, mkv, webm]
execution_contract:
  effects: [media_transform]
  worker_type: ffmpeg
policy_contract:
  auto_executable: true
  requires_review: false
planner_hints:
  use_when:
    - two videos should occupy top and bottom panes sequentially
    - one pane plays while the other pane is held as a still image
  common_upstream: [smart_trim, trim, source, url_download]
  common_downstream: [transcode, export, youtube_upload]
```

## Model Output: PipelineDraft

The model should output a constrained draft, not arbitrary JSON.

```yaml
PipelineDraft:
  name: string
  description: string
  nodes: DraftNode[]
  edges: DraftEdge[]
  planner_notes: string[]
  assumptions: string[]
  risk_flags: string[]
```

`DraftNode`:

```yaml
id: string
type: string
label: string
config: object
asset_id: string | null
position: object | null
```

Rules:

- `type` must exist in the capability manifest.
- `config` must satisfy that node's param schema.
- `asset_id` is only meaningful for source-like nodes.
- Unknown top-level fields are rejected.
- Missing optional `position` is filled by backend layout.

`DraftEdge`:

```yaml
id: string | null
source: string
sourceHandle: string
target: string
targetHandle: string
```

Rules:

- `source` and `target` must reference draft nodes.
- Handles must exist or match an allowed dynamic input pattern.
- Edge ids may be generated by the backend when absent.

The backend converts a valid `PipelineDraft` to the existing
`PipelineDefinition` schema.

## Repair Loop

Validation failures should be returned to the model as structured errors.
Repair is patch-based, not whole-plan replacement by default.

```yaml
PipelineDraftPatch:
  add_nodes: DraftNode[]
  update_nodes: NodeUpdate[]
  remove_node_ids: string[]
  add_edges: DraftEdge[]
  remove_edge_ids: string[]
  replace_edges: EdgeReplacement[]
  notes: string[]
```

Repair flow:

1. Generate initial `PipelineDraft`.
2. Validate schema, graph, contracts, and policy.
3. If invalid, send only relevant manifest subset plus structured errors.
4. Ask the model for a patch.
5. Apply patch server-side.
6. Re-run full validation.
7. Stop after a small fixed limit, default 3 attempts.
8. If still invalid, fall back to deterministic recipe/IR builder or return a
   blocked plan with errors.

The repair loop must never skip backend validation.

## Policy Gate

Structural graph validation and behavior safety should be separate.

`validate_pipeline()` remains responsible for:

- node type existence;
- valid ports;
- media type compatibility;
- required inputs;
- dynamic input limits;
- duplicate inputs;
- cycles;
- parameter types and ranges;
- terminal node warnings.

Add `validate_pipeline_policy()` for:

- source policy;
- external network access;
- external media ingestion;
- material rights;
- platform upload;
- privacy;
- public publishing;
- review approval requirements.

### Policy Rules

`source_policy = owned_only`:

- allow `source`, `material_search`, and `input_asset_id` paths;
- block external search/download nodes such as `youtube_search` and
  `url_download`.

`source_policy = remix_with_review`:

- allow external search/download;
- require `review_required` before execution.

`publish_mode = preview_only`:

- disallow upload nodes, or auto-repair to export-only.

`publish_mode = private_upload`:

- allow upload nodes only with `privacy = private`.

`publish_mode = unlisted_upload`:

- allow upload nodes only with `privacy` in `private` or `unlisted`.

`publish_mode = public_after_review`:

- default upload privacy to `private`;
- require explicit `public_approved` before public publish.

Any external platform asset or public publish path requires human review. The
model can propose these paths, but it cannot approve them.

## Example Planning Result

Prompt:

```text
生成一个视频，上半部分是小狗，下半部分是小猫视频，上半部分先播放，下半部分后播放，上传到 YouTube
```

Expected direct graph shape:

```text
material_search_dog or youtube_search_dog
  -> source_dog or url_download_dog
  -> smart_trim_dog
  -> concat_vertical_timeline.video_first

material_search_cat or youtube_search_cat
  -> source_cat or url_download_cat
  -> smart_trim_cat
  -> concat_vertical_timeline.video_second

concat_vertical_timeline.output
  -> transcode
  -> export
  -> youtube_upload(private)
```

The exact source path depends on `source_policy` and available material
libraries. If `publish_mode` is `preview_only`, the upload node is omitted or
removed by repair.

## API Surface

Add or extend AutoFlow planning APIs without removing existing behavior:

- `POST /api/v1/autoflow/plan`
  - keeps existing candidate/template planning;
  - enables AI graph planning when requested by `planning_mode` or when prompt
    requires open graph composition.

- `POST /api/v1/autoflow/plan/graph`
  - optional explicit endpoint for direct graph planning;
  - returns draft, final pipeline, validation, policy result, repair attempts,
    warnings, and review state.

- `GET /api/v1/autoflow/capabilities`
  - returns planner-grade manifest.

Request additions:

```yaml
planning_mode:
  enum: [auto, template, storyboard, ai_graph]
  default: auto
max_repair_attempts:
  default: 3
allow_experimental_graph_planning:
  default: false
```

For the first implementation, direct graph planning should be behind an explicit
flag or `planning_mode = ai_graph` to keep existing AutoFlow behavior stable.

## Frontend Behavior

The AutoFlow page should show graph planning details when present:

- selected planning mode;
- chosen nodes;
- planner assumptions;
- repair attempts and validation errors;
- policy warnings;
- review-required reasons;
- final generated pipeline preview.

The node palette can continue to use the registry. As contracts become richer,
the UI can expose better port labels, param controls, dynamic input behavior,
and safety badges.

## Testing Plan

Backend unit tests:

- manifest exports all registered nodes with strict port and param contracts;
- dynamic input contracts validate `concat_many` and `concat_timeline`;
- invalid node types, ports, media types, and params are rejected;
- `PipelineDraft` converts to `PipelineDefinition` correctly;
- policy validator blocks external sources under `owned_only`;
- policy validator clamps or rejects unsafe upload privacy;
- repair patches are applied deterministically and revalidated.

AutoFlow tests:

- `planning_mode = ai_graph` calls the graph planner and persists a validated
  plan;
- invalid model output enters repair loop;
- repeated invalid repair falls back to deterministic builder or blocked plan;
- dog/cat vertical timeline prompt produces a valid graph using
  `concat_vertical_timeline`;
- all generated workflows pass `validate_pipeline()`.

Worker and integration tests:

- execute a small generated `concat_vertical_timeline` graph with synthetic
  dog/cat fixture videos;
- verify output artifact exists;
- verify upload plans default to private and require review.

Frontend checks:

- TypeScript build validates new planner response types;
- AutoFlow UI renders planner assumptions, repair attempts, policy warnings,
  and final graph without overlapping existing panels.

Required checks after implementation:

```bash
cd /home/taiwei/Constructure-repos/videoprocess/backend && python3 -m pytest
cd /home/taiwei/Constructure-repos/videoprocess/backend && python3 -m ruff check . || true
cd /home/taiwei/Constructure-repos/videoprocess/backend && python3 -m mypy app || true
cd /home/taiwei/Constructure-repos/videoprocess/frontend && npm install && npm run build && npm run lint || true
```

## Rollout Plan

Phase 1: Contract foundation

- Extend capability contract models.
- Export planner-grade manifest.
- Add dynamic input metadata.
- Add policy contract metadata.

Phase 2: Draft validation

- Add `PipelineDraft` and patch schemas.
- Convert valid drafts to `PipelineDefinition`.
- Strengthen `validate_pipeline()` for dynamic input bounds and structured
  params.
- Add `validate_pipeline_policy()`.

Phase 3: AI graph planner

- Add LLM prompt construction using manifest subsets.
- Parse strict JSON draft.
- Add bounded repair loop.
- Add fallback to deterministic builders.

Phase 4: UI and QA

- Show graph planner details in AutoFlow UI.
- Add tests and live smoke flows.
- Validate dog/cat vertical timeline prompt end to end.

## Open Decisions Resolved

- The model may directly generate nodes and edges.
- The model may read node contracts but cannot define or modify them.
- Backend validation is mandatory before persistence and execution.
- Public publishing is never automatic.
- Fallback to recipe/IR builders remains available when graph repair fails.
