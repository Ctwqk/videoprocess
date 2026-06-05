# VP Feature Aggregator

Real-time per-actor feature aggregator for the VideoProcess risk-control pipeline. This service is the Kafka consumer side of the closed loop:

```
videoprocess ──▶ vp.actor.actions.v1 ─┐
                                       │
policy-decision-service ──▶ pds.decisions.v1 ─┤
                                              │
                                              ▼
                              vp-feature-aggregator
                                              │
                                              ▼
                          GET /v1/features/{actor_id}
                                              ▲
                                              │
                          policy-decision-service (next decide)
```

It owns the versioned Kafka schemas in `schemas/`, validates incoming events with Pydantic, computes per-actor **sliding windows** (`publishes_5m`, `publishes_1h`, `publishes_24h`, `blocks_24h`, `flags_7d`, `comment_burst_1m`), and exposes them via a narrow read API tuned for PDS's 100 ms feature-lookup budget:

```text
GET /v1/features/{actor_id}
```

Related repositories:

- [`Ctwqk/policy-decision-service`](https://github.com/Ctwqk/policy-decision-service) — Go-based PDS that reads from this service and emits decision events into it.
- [`Ctwqk/videoprocess`](https://github.com/Ctwqk/videoprocess) — owning repository for this service and the multi-platform media workflow platform whose worker actions become the actor-action stream consumed here.

## Status And Scope

- ✅ Versioned Kafka schemas (`vp.actor.actions.v1`, `pds.decisions.v1`) with strict Pydantic validation.
- ✅ In-memory bucketed window aggregator with read-only feature API.
- ✅ Manual-commit aiokafka consumer with DLQ handling for malformed or invalid known-topic payloads.
- ✅ Lifecycle-supervised consumer task; `/readyz` reflects consumer health when enabled.
- ⏳ Durable Redis/Postgres persistence is scoped but not implemented. `AGG_REDIS_URL` and `PostgresSummaryStore` are explicit placeholders. Restarting the service clears live windows until Kafka replays events for the consumer group.

The current endpoint returns zero defaults for unknown actors and can serve in-memory publish, block, flag, and comment-burst feature windows. The Kafka consumer applies validated `vp.actor.actions.v1` and `pds.decisions.v1` events to the same in-memory store used by the API when `AGG_ENABLE_CONSUMER=true`.

## Operational Notes

- Feature state is in memory today. Restarting the service clears live windows until Kafka replays events for the consumer group.
- `/readyz` checks the consumer task only when `AGG_ENABLE_CONSUMER=true`; a failed, stopped, or missing consumer task returns `503`.
- Bad JSON or invalid known-topic payloads are written to `AGG_DEAD_LETTER_TOPIC`, defaulting to `risk.events.dlq.v1`.
- The Kafka consumer uses manual commits. Offsets are committed only after an event is applied, explicitly ignored, or successfully sent to the DLQ; store failures and DLQ publish failures leave the offset uncommitted for retry.

## Local Setup

```bash
python3 -m pip install -e '.[dev]'
uvicorn feature_aggregator.main:app --host 0.0.0.0 --port 8080
```

The consumer is disabled by default for local app startup and tests. To run the API and in-process consumer against Kafka:

```bash
AGG_ENABLE_CONSUMER=true \
AGG_KAFKA_BROKERS=localhost:9092 \
uvicorn feature_aggregator.main:app --host 0.0.0.0 --port 8080
```

## Consumer

`feature_aggregator.consumer.EventConsumer` validates Kafka values through the existing Pydantic schemas before applying them to a `FeatureStore`.

- `vp.actor.actions.v1`: applies publish/comment action windows.
- `pds.decisions.v1`: applies block/flag decision windows.
- `risk.events.dlq.v1`: receives original bytes for bad JSON or invalid known-topic payloads.
- Unknown topics are ignored and are not applied as known events.

`run_consumer(...)` uses `AIOKafkaConsumer` with manual commits. It commits only after a message is applied, explicitly ignored, or successfully sent to the DLQ. If DLQ publishing fails, the offset is left uncommitted so the message can be retried.

Relevant environment variables use the `AGG_` prefix:

- `AGG_ENABLE_CONSUMER`: `true` to run the in-process Kafka consumer; default `false`.
- `AGG_KAFKA_BROKERS`: Kafka/Redpanda bootstrap servers; default `redpanda:9092`.
- `AGG_KAFKA_GROUP_ID`: consumer group; default `vp-feature-aggregator`.
- `AGG_VP_ACTIONS_TOPIC`: default `vp.actor.actions.v1`.
- `AGG_PDS_DECISIONS_TOPIC`: default `pds.decisions.v1`.
- `AGG_DEAD_LETTER_TOPIC`: default `risk.events.dlq.v1`.
- `AGG_REDIS_URL`: reserved for later durable Redis-backed feature storage.

## Docker

Build the local image:

```bash
docker build -f deploy/Dockerfile -t vp-feature-aggregator:local .
```

Run API-only mode:

```bash
docker run --rm -p 8080:8080 vp-feature-aggregator:local
```

Run with the consumer enabled:

```bash
docker run --rm -p 8080:8080 \
  -e AGG_ENABLE_CONSUMER=true \
  -e AGG_KAFKA_BROKERS=host.docker.internal:9092 \
  vp-feature-aggregator:local
```

## Kubernetes

The k8s assets live in `/home/taiwei/k8s-Constructure/k8s-constructure/videoprocess`:

- `redpanda.yaml`: single-node Redpanda StatefulSet and Service.
- `vp-feature-aggregator.yaml`: aggregator Deployment and Service with `AGG_ENABLE_CONSUMER=true`.
- `event-outbox-relay.yaml`: relay Deployment and Service using `constructure-videoprocess-api:latest`; the relay Python entrypoint now exists in the VP repo as `backend/event_outbox_relay.py`.

Render them with:

```bash
kubectl kustomize /home/taiwei/k8s-Constructure/k8s-constructure/videoprocess
```

## Schemas

- `schemas/vp.actor.actions.v1.json`: VideoProcess channel-ops actor action events.
- `schemas/pds.decisions.v1.json`: PDS decision events.

## Tests

```bash
python3 -m pytest -q
python3 -m ruff check . || true
docker build -f deploy/Dockerfile -t vp-feature-aggregator:local .
```
