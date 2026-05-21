# ChannelOps Phase B/C/D Design

Date: 2026-05-21
Source spec: `Design/videoprocess_channelops_live_agent_spec.md`
Depends on: `docs/superpowers/specs/2026-05-21-channelops-go-live-agent-design.md`

## Goal

Advance ChannelOps from Phase A live-unlisted readiness into the next three automation phases:

- **Phase B:** candidate-level explainability and failure classification.
- **Phase C:** discovery signals separated from manual seeds.
- **Phase D:** feedback learning v1 as a read-only, auditable recommendation layer.

Phase B and Phase C are production runtime features. Phase D is implemented as a live data pipeline and API, but it does not change tick selection, cadence, privacy, public publishing, or account choice by default.

## Non-Goals

- No Phase E public auto-publishing.
- No Phase F bandit or portfolio learning.
- No public promotion from external platform assets without explicit human review.
- No arbitrary LLM-generated workflow graphs.
- No duplicate YouTube upload path. ChannelOps continues to drive AutoFlow and observe the existing upload/publish path.
- No removal of legacy Python ChannelOps APIs in this phase.

## Chosen Approach

Use a "complete B/C, observed D" rollout.

1. Phase B writes durable per-candidate audit rows and normalized failure categories.
2. Phase C moves trend discovery into a first-class `DiscoverySignal` entity and stops treating `trend_youtube` as manual seed data.
3. Phase D computes staged metrics, reward scores, and `LearningState` summaries, then exposes them through read-only API and decision-audit context. Learning influence remains disabled until a later gate explicitly turns it on.

This gives operators the data shape needed for later automation without pretending the system has enough samples for public publishing or bandit learning.

## Architecture Boundary

Go owns live ChannelOps runtime behavior:

- tick candidate generation
- guard evaluation
- candidate audit writes
- task creation
- queue handlers
- failure classification on runtime errors
- discovery-to-candidate conversion
- metrics-stage collection and learning aggregation jobs

Python remains the owner of:

- SQLAlchemy models and Alembic migrations
- FastAPI ChannelOps APIs
- dashboard-facing schemas
- existing Python trend ingesters unless they are later moved to Go
- AutoFlow internals

All new runtime behavior that belongs to the live loop should be implemented in Go where practical. Python changes should be limited to schema, migrations, API surfaces, and existing Python-only ingester code.

## Phase B: Explainability and Failure Classification

### DecisionAuditEntry

Add `decision_audit_entries`.

Required fields:

- `id`
- `tick_audit_id`
- `channel_profile_id`
- `candidate_id`
- `candidate_source`: `manual_seed`, `lane_seed`, `trend_youtube`, or future source key
- `topic_lane_id`
- `lane_format_id`
- `target_account_id`
- `score_json`
- `guard_results_json`
- `pds_decision_json`
- `learning_context_json`
- `selected`
- `rejection_reason`
- `created_task_id`
- `created_at`

Indexes:

- `(tick_audit_id)`
- `(channel_profile_id, created_at)`
- `(created_task_id)`
- `(candidate_source, created_at)`

Runtime rules:

- Every candidate considered during tick writes exactly one audit row.
- Guard-rejected candidates write `selected=false` and a structured rejection reason.
- Dry-run ticks still write candidate audit rows.
- Accepted candidates update `created_task_id` after task creation.
- `AgentTickAudit.decision_summary_json` remains a compact summary, not the only source of truth.

`guard_results_json` schema:

```json
[
  {
    "guard": "lane_cadence",
    "verdict": "allow",
    "reason": "2 scheduled/public publications in last 24h, limit 3"
  }
]
```

`learning_context_json` records any Phase D learning features available at selection time. While learning influence is disabled, this field is informational only.

### Failure Category

Add nullable `ProductionTask.failure_category`.

Allowed values:

- `auth`
- `quota`
- `upload`
- `render`
- `planning`
- `validation`
- `pds`
- `youtube_status`
- `metrics`
- `discovery`
- `learning`
- `other`

Rules:

- Any code path that sets `ProductionTask.state = failed` or equivalent held/failure state should also set `failure_category` when the cause is known.
- Classification should use explicit error types or handler context first, string matching second.
- Consecutive-upload guards should use `failure_category in ('auth', 'quota', 'upload', 'youtube_status')` instead of broad text matching where data exists.
- `other` is allowed for unknown failures but should stay below 10 percent in the seven-day failure report.

### APIs

Add or extend:

- `GET /api/v1/channel-agent/channels/{channel_id}/decisions`
  - filters: `tick_audit_id`, `candidate_source`, `selected`, `limit`, `offset`
  - returns `DecisionAuditEntry` rows scoped to the channel
- `GET /api/v1/channel-agent/tasks/{task_id}/audit`
  - returns task, publication, material usage rows, decision audit row, queue history if available
- `GET /api/v1/channel-agent/channels/{channel_id}/failures?days=7`
  - aggregates `ProductionTask.failure_category`, state, and representative reasons

All channel-scoped APIs must verify ownership by joining through `ChannelProfile` or related tables, not by trusting unscoped IDs alone.

## Phase C: DiscoverySignal

### Data Model

Add `discovery_signals`.

Required fields:

- `id`
- `channel_profile_id`
- `topic_lane_id`
- `source`: first supported value is `youtube_search`
- `source_url`
- `source_external_id`
- `title`
- `summary`
- `keywords_json`
- `observed_at`
- `expires_at`
- `trend_score`
- `novelty_score`
- `raw_json`
- `status`: `active`, `converted`, `expired`, `dismissed`
- `converted_task_id`
- `created_at`
- `updated_at`

Constraints and indexes:

- unique `(channel_profile_id, source, source_external_id)`
- `(channel_profile_id, topic_lane_id, observed_at)`
- `(channel_profile_id, status, expires_at)`

Add nullable `ProductionTask.discovery_signal_id`.

### Ingestion

`YouTubeTrendIngester` writes `DiscoverySignal`, not `ManualSeed`.

Rules:

- A repeated source item updates the existing row's `observed_at`, scores, raw payload, and status if appropriate.
- New rows default to `active`.
- Expired rows are not converted into task candidates.
- Per-lane active signal cap defaults to 50. Additional signals may be dropped or stored as `dismissed` with reason metadata.

Legacy data migration:

- Convert active `ManualSeed` rows where `source_policy = 'trend_youtube'` into `DiscoverySignal`.
- Preserve title/prompt text, lane, channel, URLs, and raw metadata where present.
- Mark migrated manual seeds as `exhausted` or retain them as inactive legacy rows so they no longer produce manual candidates.

### Discovery To Candidate

Tick loads active, unexpired discovery signals and creates `trend_youtube` candidates.

Candidate rules:

- `candidate_source = trend_youtube`
- `source_kind = trend_youtube` in task rationale
- `manual_override = false`
- `ProductionTask.discovery_signal_id` is populated for accepted candidates
- A converted signal moves to `status = converted` and stores `converted_task_id`

The same budget model used for manual and lane-driven candidates applies. Manual seeds remain higher priority, but guard-rejected manual candidates do not consume budget that could be filled by discovery or lane-driven candidates.

## Phase D: Feedback Learning v1

### Metrics Series

Extend `FeedbackSnapshot` to support staged metrics.

Add fields:

- `snapshot_stage`: `1h`, `6h`, `24h`, `72h`, `7d`
- `metrics_completeness_score`
- `available_fields_json`
- `reward_score`
- `reward_components_json`

Add a uniqueness rule equivalent to one row per `(publication_id, snapshot_stage)`.

Collection rules:

- `collect_metrics` updates an existing snapshot for the same `(publication_id, snapshot_stage)` instead of inserting duplicates.
- Missing fields remain missing. They must not be silently treated as zero for reward calculation.
- A snapshot with `metrics_completeness_score < 0.4` is stored but excluded from LearningState aggregation.

### Reward Formula

Use a partial-aware weighted reward.

Base components:

- retention or average view duration
- CTR when impressions exist
- view count normalized by channel median
- engagement rate from likes/comments/shares per view
- publish stability bonus when publication remained scheduled/public/unlisted without takedown or severe status

Only available components participate. Weights are renormalized over available components. The final reward is stored in `reward_score` with component evidence in `reward_components_json`.

### LearningState

Add `learning_states`.

Required fields:

- `id`
- `channel_profile_id`
- `dimension_type`: `topic_lane`, `lane_format`, `publish_window`, `template`, `source`
- `dimension_key`
- `window_days`
- `sample_count`
- `avg_reward`
- `confidence`
- `recommendation_json`
- `last_computed_at`
- `created_at`
- `updated_at`

Learning aggregation:

- Runs from metrics snapshots with completeness >= 0.4.
- Uses 7-day and 30-day windows.
- Produces recommendations such as "observe", "promote_more", "cool_down", or "insufficient_data".
- Requires `sample_count >= 10` for a non-observe recommendation.
- Writes no queue items and changes no production cadence in this phase.

### Learning APIs

Add:

- `GET /api/v1/channel-agent/channels/{channel_id}/learning`
  - returns LearningState grouped by dimension
- `POST /api/v1/channel-agent/channels/{channel_id}/learning/recompute`
  - operator-triggered recompute for local testing and backfill

Learning recompute is idempotent for `(channel_profile_id, dimension_type, dimension_key, window_days)`.

### Runtime Integration

Tick may read LearningState and include it in `DecisionAuditEntry.learning_context_json`.

LearningState must not affect:

- candidate ordering
- candidate rejection
- account choice
- privacy choice
- public promotion
- cadence

until a later design explicitly enables learning influence.

## Migration Plan

Use separate Alembic revisions after current revision `019_channelops_go_live_phase0.py`:

1. `020_channelops_decision_audit_failure_category.py`
   - `decision_audit_entries`
   - `production_tasks.failure_category`
2. `021_channelops_discovery_signals.py`
   - `discovery_signals`
   - `production_tasks.discovery_signal_id`
   - legacy `ManualSeed.source_policy = 'trend_youtube'` data migration
3. `022_channelops_feedback_learning.py`
   - staged feedback snapshot fields and uniqueness/indexes
   - `learning_states`

Each migration should have a matching downgrade for schema changes. Data migration downgrade may preserve migrated discovery rows rather than recreating old trend manual seeds.

## Testing Plan

Backend tests:

- migrations expose new models and indexes
- decisions API is channel scoped
- task audit API joins task/publication/material/audit evidence
- failure aggregation uses `failure_category`
- YouTube trend ingester writes `DiscoverySignal`
- legacy trend manual seeds migrate into `DiscoverySignal`
- learning API returns grouped LearningState

Go tests:

- dry-run tick writes `DecisionAuditEntry` rows without tasks
- accepted candidates write selected audit rows and backfill `created_task_id`
- guard-rejected candidates write structured rejection rows
- handler failures set expected `failure_category`
- discovery signals become `trend_youtube` candidates with `manual_override=false`
- converted discovery signals are marked converted
- metrics snapshots upsert per stage
- learning recompute writes stable LearningState and does not affect tick selection

Verification commands:

```bash
go test ./internal/channelops ./internal/config ./internal/store ./internal/orchestrator ./internal/worker/...
cd backend && python3 -m pytest
cd backend && python3 -m ruff check . || true
cd backend && python3 -m mypy app || true
```

If frontend files change:

```bash
cd frontend && npm install
cd frontend && npm run build
cd frontend && npm run lint || true
```

## Rollout

1. Apply migrations in order.
2. Deploy APIs and Go runner with learning influence disabled.
3. Run targeted B/C/D tests against local DB.
4. Run a local fake live flow that reaches measured state and writes staged metrics.
5. Confirm decisions, failures, discovery, and learning APIs return non-empty data in seeded fixtures.
6. Keep public publishing and bandit learning disabled.

## Acceptance Criteria

Phase B is accepted when:

- every tick candidate has a `DecisionAuditEntry`
- failed or held production tasks have a useful `failure_category` where cause is known
- task audit and failure APIs return channel-scoped, non-empty evidence in tests

Phase C is accepted when:

- YouTube trend ingestion no longer creates active `ManualSeed` rows
- trend candidates come from `DiscoverySignal`
- accepted trend tasks reference `discovery_signal_id`
- trend candidates do not receive manual material override

Phase D is accepted when:

- staged feedback snapshots are upserted idempotently
- reward scores are partial-aware
- LearningState recompute is idempotent
- LearningState is visible through API
- tick records learning context but does not let it affect selection

## Deferred Decisions

- Phase E public promotion gates and manual override workflow.
- Phase F epsilon-greedy or contextual bandit strategy.
- Cross-platform discovery sources beyond YouTube search.
- Whether `IdeaSeed` deserves its own table once one discovery signal can fan out into multiple ideas.
- Automatic application of learning recommendations to cadence, lane priority, or template choice.
