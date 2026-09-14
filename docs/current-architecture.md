# VideoProcess Current Architecture Contracts

> **Status:** Current contract summary
>
> **Applies to:** AutoFlow planning, ChannelOps production ownership, source rights, publication review, and feedback behavior as of 2026-09-14.

This page is the entry point for current behavior. Historical plans remain useful for design context, but their proposed phases, code sketches, and completion claims do not override these contracts.

## Runtime ownership

Go is the sole production owner of ChannelOps scheduling and queue execution. The `channelops-runner-go` service owns the production lifecycle and calls the Python FastAPI service for AutoFlow planning, persistence, and pipeline execution.

The legacy Python `channel_agent_runner.py` is a local/test compatibility runner. Its startup guard accepts only explicit `DEPLOY_MODE=local` or `DEPLOY_MODE=test` and exits before importing runtime code in missing, shared, or production modes. Its queue claim allowlist is limited to its implemented handlers:

`agent_tick`, `plan_task`, `execute_task`, `observe_job`, `publish_task`, `promote_publication`, `reconcile_publication`, `collect_metrics`, `account_health`, `send_alert`, and `cleanup_expired`.

Go-only kinds remain queued for a Go consumer. Operators must never run the Go and Python ChannelOps runners against the same queue.

## AutoFlow planning

AutoFlow accepts explicit `planning_mode` values: `auto`, `template`, `storyboard`, and `ai_graph`. Every plan records `validation.planning.requested_mode` and `effective_mode`, plus `provider_used`, `fallback_reason`, and `source` when applicable. A caller can therefore distinguish the requested path from the path that actually produced the plan.

Go forwards a versioned `planning_options` envelope. `PlanningOptions` v1 contains the planning mode, provider configuration, model, experimental graph opt-in, and repair limit. Explicit `false` and `0` values are preserved. Python validates the versioned envelope and rejects conflicts with duplicated top-level values.

Model-backed graph planning requires both the global `AUTOFLOW_AI_ENABLED` setting and per-request `allow_experimental_graph_planning=true`. A bounded deterministic graph rule may still satisfy a supported request without enabling a model provider. Model output is constrained by the capability manifest and compiled and validated as a `PipelineDefinition`; it does not define arbitrary executable graphs or assert source rights.

There is no implicit demo-material provider. If selection produces no usable candidates, AutoFlow persists a blocked plan with an empty graph and `material_status=no_material`. A storyboard with omitted optional shots may be `partial`; complete required coverage is `complete`. Missing required shots block formal upload plans; previews may expose a partial storyboard with explicit omissions. An executable plan must pass `validate_pipeline()`.

## Source rights and review

Candidate rights come from stored or provider-supplied facts: `rights_status`, `license`, `provenance`, `license_scope`, `license_source`, `rights_source`, and `evidence_ref` or `evidence_refs`. Conversion and materialization preserve these facts. They do not turn the presence of an asset ID or an arbitrary license string into permission.

Rights status uses restrictive precedence: `blocked` wins over `review_required`, `unknown`, and `allowed`. A blocked candidate blocks the plan. Unknown or unproven local rights require human review and never qualify for automatic upload. `owned_only`, `licensed_only`, and `public_domain_or_cc` use explicit license allowlists, including normalized known CC forms.

A generic input video is eligible when its stored asset metadata establishes `license=owned` and allowed rights. Original user footage is supported; generic AutoFlow does not require `provenance=generated`. The separate owned-inventory production profile remains stricter: it requires generated provenance, immutable inventory bindings, hashes, and its existing fenced evidence checks.

External platform candidates always require a human hold before upload, including private and unlisted uploads. Public publication also requires explicit approval. Default automated visibility remains `private` or `unlisted`.

## PDS fallback and publication authority

PDS fallback depends on the action:

| Action | Fallback when PDS is unavailable |
| --- | --- |
| `candidate_accept` | `allow` |
| `plan_approval` | `flag` for review |
| `publish` | `block` |
| `promote_publication` | `block` |

These fallbacks are audit facts, not substitutes for real PDS evidence in the fenced owned-inventory producer. External-source review and revision-bound plan approval remain independent publication gates.

## Feedback boundary

The system collects publication metrics, stores feedback snapshots, and can recompute learning-state recommendations. Those values are observational. Current candidate selection deliberately ignores learning context, so recommendations do not autonomously change selection, publishing, or promotion policy.

Active learning, bandit allocation, and automatic policy changes remain roadmap work. A richer structured-script authoring layer and execution of generated video assets are also future work; current generation fields are planning metadata and do not imply an implemented `video_generate` execution path.

## Operator references

- [`channelops-go-live-runner.md`](channelops-go-live-runner.md) — current Go runner and passive preflight runbook.
- [`../README.md`](../README.md) — repository overview and local stack entry points.

## Historical design references

| Document | Status | Applies to | Superseded by |
| --- | --- | --- | --- |
| [`videoprocess_autoflow_upgrade_plan.md`](../Design/videoprocess_autoflow_upgrade_plan.md) | Historical implementation plan | Original AutoFlow phased build-out | This page |
| [`videoprocess_channelops_live_agent_spec.md`](../Design/videoprocess_channelops_live_agent_spec.md) | Historical rollout specification | Earlier P0–F ChannelOps roadmap | This page |
| [`videoprocess_smart_trim_storyboard_codex_plan.md`](../Design/videoprocess_smart_trim_storyboard_codex_plan.md) | Historical implementation plan | Original Smart Trim and Storyboard design | This page |
