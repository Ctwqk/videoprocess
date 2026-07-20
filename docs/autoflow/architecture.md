# AutoFlow Architecture

AutoFlow turns a user prompt into a validated, reviewable media workflow. The
planner is deliberately deterministic after intent parsing: templates and
capabilities define what can be built, and every generated `PipelineDefinition`
must pass `validate_pipeline()` before execution.

## Request Flow

1. `POST /api/v1/autoflow/plan` receives an `AutoFlowRequest`.
2. `RuleBasedIntentParser` converts the prompt and request settings into an
   `AutoFlowIntent`.
3. `TemplateLibrary` selects a known workflow template for the intent.
4. `MaterialSelector` chooses owned, licensed, or research candidates according
   to source policy. `SearchService` uses safe local stubs in the MVP.
5. `ClipRanker` deduplicates candidates and assigns explainable ranking scores.
6. `MetadataGenerator` creates titles, descriptions, tags, hashtags, and
   platform payloads.
7. `PipelineBuilder` maps template slots and candidates into a deterministic
   `PipelineDefinition`.
8. `validate_pipeline()` checks node types, ports, required params, and DAG
   shape. `AutoFlowRepairService` may apply bounded repairs and revalidate.
9. `RightsPolicy` evaluates candidates and requested publish mode.
10. The plan is returned with validation data, rights state, candidates,
   metadata, and `needs_review`.

Plan patch flows may update a reviewed plan before execution, but patches must
stay inside the same deterministic template/capability boundary and must be
revalidated with `validate_pipeline()`. Public approval is a separate human
decision: research candidates, external platform assets, or public upload modes
must remain blocked until review explicitly approves them.

## Core Modules

| Module | Responsibility |
| --- | --- |
| `backend/app/api/autoflow.py` | FastAPI routes for plans, approval, execution, templates, capabilities, and run lookup. |
| `backend/app/autoflow/service.py` | Orchestrates planning, approval, and execution. Stores Phase 1 plans/runs in memory. |
| `backend/app/autoflow/intent_parser.py` | Rule-based prompt parser for supported MVP intents. |
| `backend/app/autoflow/template_library.py` | Curated workflow templates. This is the planner control surface. |
| `backend/app/autoflow/material_selector.py` | Source-policy-aware candidate selection. |
| `backend/app/autoflow/search_service.py` | Deterministic local search adapters for MVP material/external candidates. |
| `backend/app/autoflow/clip_ranker.py` | Candidate dedupe and explainable scoring. |
| `backend/app/autoflow/capability_manifest.py` | Node registry projection with AutoFlow tags and suitable-use metadata. |
| `backend/app/autoflow/pipeline_builder.py` | Deterministic conversion from template, intent, candidates, and metadata to pipeline schema. |
| `backend/app/autoflow/validation_repair.py` | Bounded repair pass after validation failures. |
| `backend/app/autoflow/rights_policy.py` | Safety gate for source and publication policy. |
| `backend/app/autoflow/metrics_service.py` | In-memory MVP metrics import and template aggregation. |
| `backend/app/autoflow/trend_service.py` | Manual trend signals and opportunity scoring. |
| `backend/app/autoflow/content_strategy.py` | Ideas generated from trend suggestions and template performance. |
| `backend/app/schemas/autoflow.py` | Public request, plan, candidate, metadata, template, and run schemas. |

Production deployments should replace the MVP in-memory metrics/trends with
DB-backed metrics and trend APIs. Rankers and strategy helpers should treat
those values as optional context, never as authority to bypass template
validation, rights checks, or public approval.

## Execution Boundary

Planning must not require a live database, Redis, ffmpeg, or worker process.
`AutoFlowService.plan()` can run in tests with `db=None`.

Execution is separate:

- `AutoFlowExecuteRequest.execute=false` can be used for dry-run style clients.
- With `db=None`, execution returns an in-memory pending run and does not create
  a real pipeline/job.
- With a database and `execute=true`, the service creates a pipeline, creates a
  job, and defers or dispatches through the normal job runtime.
- `POST /api/v1/autoflow/execute` accepts an optional `idempotency_key`. Keyed
  execution requires the plan's current `approved_revision_hash`; the run
  reservation, pipeline, job, plan status, and schedule decision commit in one
  transaction. A replay returns the existing run and never schedules a second
  background start. Legacy callers that omit the key retain the prior behavior.
- An idempotency key is bound to one exact plan and approved revision. Reusing
  it for another plan or a newly approved revision fails closed.

## Safety Invariants

- Do not let LLM output directly define arbitrary nodes or edges.
- New generated workflows must use templates, capabilities, and deterministic
  builders.
- Default request safety is `source_policy=owned_only` and
  `publish_mode=preview_only`.
- Public publishing must require explicit review.
- Human and agent approvals authorize only the canonical execution revision
  recorded in `approved_revision_hash`; execution-relevant edits clear all
  approval tokens until the plan is reviewed again.
- External URL candidates may be used for research/preview, but require human
  review before publication.
- Upload nodes must default to `private` or `unlisted`.
- DB-backed metrics can influence ranking and content strategy, but they must
  not directly define arbitrary workflow graphs.

## Testing Strategy

Service-level e2e examples live in
`backend/tests/autoflow/test_e2e_examples.py`. They cover:

- Cat compilation from owned assets.
- Hot-topic explainer with external research candidates.
- Material-library remix with owned material defaults.

These tests assert intent selection, template selection, rights defaults,
candidate rights status, and independent `validate_pipeline()` success without
network, database, Redis, or ffmpeg dependencies.
