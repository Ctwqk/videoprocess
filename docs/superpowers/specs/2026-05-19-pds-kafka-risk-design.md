# PDS and Kafka Risk Pipeline Design

Date: 2026-05-19

Status: Approved section by section during brainstorming.

Source plans:

- `/home/taiwei/code/job-prep/PDS_PLAN.md`
- `/home/taiwei/code/job-prep/KAFKA_PLAN.md`

## Goal

Implement the Go policy decision service and Kafka-backed risk feature pipeline without
creating merge pressure with the recently merged Go VideoProcess sidecar work.

The first production path is Python ChannelOps in VideoProcess. The Go API and Go
ffmpeg worker sidecars remain out of scope for this first PDS/Kafka integration pass.

## Architecture

The system has four bounded parts:

1. VideoProcess Python ChannelOps writes actor/action events through an outbox and
   calls PDS for preflight decisions.
2. PDS evaluates allow/block/flag decisions, stores audit data, and emits decision
   events asynchronously.
3. Redpanda carries versioned risk events between services.
4. `vp-feature-aggregator` consumes VP action events and PDS decision events,
   builds actor feature windows, and exposes feature facts to PDS over HTTP.

Kafka is not the source of truth. VideoProcess Postgres remains authoritative for
VP business state. PDS audit storage remains authoritative for decisions. Kafka is
the asynchronous propagation layer used for features and observability.

## Repository Boundaries

### VideoProcess

Path: `/home/taiwei/.codex/worktrees/d1d5/videoprocess`

Responsibilities:

- Add a Python PDS client for ChannelOps.
- Add ChannelOps preflight checks before candidate acceptance and publication
  promotion.
- Add a Postgres-backed event outbox.
- Add an outbox relay process that publishes VP actor/action events to Kafka.
- Add tests for the PDS client, ChannelOps gate behavior, outbox writer, and relay.

Non-responsibilities:

- Do not move decision logic into VideoProcess.
- Do not make the Go sidecar the first PDS integration path.
- Do not make the feature aggregator part of the VideoProcess backend package.

### Policy Decision Service

Path: `/home/taiwei/Constructure-repos/policy-decision-service`

Responsibilities:

- Keep `/v1/decide` as the core decision API.
- Preserve the existing Go rule engine, YAML loader, CEL/rule support, and audit
  writer shape.
- Add feature provider support so rules can evaluate actor feature facts.
- Add asynchronous decision event publishing to Kafka.
- Add reload, metrics, gRPC, Kubernetes manifest, and focused Go tests.

Non-responsibilities:

- Do not consume Kafka in PDS in this first pass.
- Do not own feature aggregation or sliding-window state.

### VP Feature Aggregator

Path: `/home/taiwei/Constructure-repos/vp-feature-aggregator`

Responsibilities:

- Own versioned JSON schemas for `vp.actor.actions.v1` and `pds.decisions.v1`.
- Consume both topics.
- Maintain short-window actor counters in Redis and longer-window summaries in
  Postgres.
- Expose `GET /v1/features/{actor_id}` for PDS.

Non-responsibilities:

- Do not decide allow/block/flag.
- Do not mutate VideoProcess or PDS source-of-truth records.

## PDS Design

PDS keeps its current core objects:

- `engine.RuleEngine`
- `rules.LoadFile` and `rules.LoadBytes`
- `store.AuditWriter`
- `pds.decisions`

Decision evaluation gains a request-scoped `EvalState` object. `EvalState` contains
the actor, action, resource, request context, rate-limit facts, and optional feature
facts. CEL activation is built from `EvalState`, so rules can access stable fields
without depending on storage internals.

Rate limiting remains a local Redis-backed PDS rule capability. Kafka-derived actor
features are an additional signal, not a replacement for rate limiting.

Feature providers:

- `Provider.GetActorFeatures(ctx, actorID)` is the internal interface.
- `HTTPFeatureProvider` calls `vp-feature-aggregator`.
- `PostgresProfileProvider` remains available for local/static profile facts.
- `FallbackProvider` composes providers and fails open when feature lookup is
  unavailable.

Feature lookup timeout defaults to 100 ms. Timeout, 5xx, invalid JSON, and connection
errors fail open and record degraded-decision metadata and metrics.

Decision sinks:

- `AuditWriter` persists the authoritative audit record.
- `KafkaDecisionSink` publishes `pds.decisions.v1` asynchronously.
- `MultiDecisionSink` fans out to audit and Kafka.

Kafka publish failures must not block `/v1/decide`. The Kafka sink uses a bounded
queue, records dropped events with metrics, and relies on the audit database as the
durable decision source.

Operational endpoints:

- `POST /v1/admin/reload` reloads rule files.
- SIGHUP reloads rule files.
- Optional file watching can be added only after explicit reload works.
- Prometheus metrics cover decisions, rule results, PDS client errors, feature
  provider latency, audit writes, and Kafka sink queue/drop counts.
- gRPC shares the same engine and store path as HTTP.

## VideoProcess Integration Design

VideoProcess adds `backend/app/pds_client.py` with:

- `PDSClient.decide(...)`
- `NoopPDSClient`
- Configurable base URL, client id, enabled flag, and timeout.

Default configuration:

- `pds_enabled = False`
- `pds_base_url = "http://pds:8080"`
- `pds_client_id = "videoprocess-channel-agent"`
- `pds_timeout_seconds = 0.5`

PDS client behavior is fail-open for timeout, 5xx, connection errors, invalid JSON,
and disabled mode. Fail-open returns an allow decision with warning metadata such as
`pds_unavailable`. Explicit PDS block and flag decisions preserve rule details.

ChannelOps gate points:

- `_evaluate_candidate_guards()` checks PDS before accepting a candidate.
- `handle_promote_publication()` checks PDS before scheduling or public promotion.

Decision semantics:

- `allow`: continue current workflow.
- `block`: reject or hold the action with `pds_blocked` metadata.
- `flag`: first version treats the action as held/rejected with
  `pds_flagged_for_review` metadata.
- `pds_unavailable`: continue with warning metadata.

The outbox lives under `backend/app/events/`:

- `schemas.py`
- `outbox.py`
- `relay.py`
- `producer.py`

An Alembic migration adds `event_outbox` with:

- `id`
- `topic`
- `key`
- `payload`
- `created_at`
- `delivered_at`
- `attempt_count`
- `last_error`

The first VP events are limited to ChannelOps:

- candidate accepted
- candidate blocked
- candidate flagged
- publication promotion attempted
- publication promotion blocked
- publication scheduled

The relay runs as an independent process, for example
`backend/event_outbox_relay.py run`, and has its own compose service. It is separate
from `channel-agent-runner`.

## Kafka Contracts and Feature Aggregator

`vp-feature-aggregator` owns `schemas/` as the versioned contract source for:

- `vp.actor.actions.v1`
- `pds.decisions.v1`

VideoProcess produces only actor/action events. PDS produces only decision events.
The aggregator consumes both topics.

Consumer behavior:

- Validate event payloads against schema.
- Commit offsets only after successful processing.
- Send parse or schema failures to a DLQ topic.
- Deduplicate by `event_id` using Redis TTL.
- Keep processing idempotent enough to tolerate restart and retry.

Feature API:

`GET /v1/features/{actor_id}` returns the narrow feature set PDS rules need in the
first version:

- `publishes_5m`
- `publishes_1h`
- `publishes_24h`
- `blocks_24h`
- `flags_7d`
- `comment_burst_1m`
- `as_of`
- `from_cache`

Implementation shape:

- Short windows are Redis-backed bucket counters.
- Longer windows are flushed to Postgres summaries.
- The service can rebuild baseline state from Postgres after restart.
- The API is optimized for the PDS 100 ms feature lookup budget.

The aggregator provides feature facts only. All allow/block/flag judgment remains in
PDS rules.

## Deployment

Local development should use a compose override file:

- `docker-compose.pds-kafka.yml`

The override includes:

- `redpanda`
- `pds`
- `vp-feature-aggregator`
- `event-outbox-relay`

This keeps the main compose file and recently merged Go sidecar services stable
during the first implementation pass. Shared broker env vars may be added only where
needed.

Kubernetes ownership:

- PDS manifests live in the PDS repo.
- Redpanda and `vp-feature-aggregator` manifests live with VideoProcess/Constructure
  deployment assets, under the existing k8s Constructure area.
- Service names should stay stable: `pds`, `redpanda`, and
  `vp-feature-aggregator`.

## Testing and Verification

PDS:

- Go unit tests for rule evaluation, CEL activation, feature provider fallback,
  Kafka decision sink behavior, reload behavior, and metrics.
- Live curl smoke for health, readiness, metrics, and `/v1/decide`.

VideoProcess:

- Pytest for `PDSClient`.
- Pytest for ChannelOps allow/block/flag/unavailable behavior.
- Pytest for outbox writer and relay publishing behavior.
- Alembic migration verification.
- Required backend checks after changes:
  - `cd backend && python3 -m pytest`
  - `cd backend && python3 -m ruff check . || true`
  - `cd backend && python3 -m mypy app || true`

Feature aggregator:

- Pytest for schema validation.
- Pytest for window aggregation.
- Pytest for feature API responses.
- Consumer tests with fake Kafka events and fake stores.

End-to-end smoke:

1. VideoProcess writes an outbox event.
2. Relay publishes to Redpanda.
3. Aggregator consumes the event and updates features.
4. PDS fetches features and evaluates a CEL rule.
5. PDS stores the audit record and publishes `pds.decisions.v1`.
6. Aggregator consumes the decision event and updates decision-derived counters.

## Implementation Order

1. Finish PDS repo features and tests.
2. Create `vp-feature-aggregator` repo with schemas, consumer, stores, API, and tests.
3. Integrate VideoProcess Python ChannelOps with PDS client and outbox relay.
4. Add local compose override and smoke documentation.
5. Add Kubernetes manifests.
6. Run targeted tests and the local end-to-end smoke.

## Merge Risk Controls

Expected merge risk is low because the first path avoids the recently merged Go
VideoProcess sidecar implementation.

Known risk files in VideoProcess:

- `backend/app/config.py`
- `backend/app/channel_agent/service.py`
- Alembic migration directory
- compose files

Controls:

- Keep Go sidecar code out of the first integration pass.
- Use a compose override instead of broad main compose rewrites.
- Pick the next Alembic revision from the current repo state at implementation time.
- Keep Kafka schemas in the aggregator repo and import or copy only generated/validated
  client payload definitions when needed.
- Add tests at each service boundary before wiring the full smoke.
