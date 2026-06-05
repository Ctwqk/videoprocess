# VideoProcess

Multi-platform media workflow platform with channel-agent orchestration, heterogeneous worker tiers, and an event-driven risk-control extension layer. The stack runs as ~16 containerized services locally via Docker Compose and deploys to Kubernetes in production.

## What This Repo Is

VideoProcess composes three concentric layers:

1. **Ingestion and processing** — FFmpeg transcode, vision inference, headless browser automation across YouTube and other platforms, and Faster-Whisper transcription utilities.
2. **Channel-ops orchestration** — a typed node registry, a DAG-based workflow orchestrator (`autoflow`), and LLM-driven channel agents that compose pipelines from registered node types and decouple capability declaration from execution.
3. **Risk-control extension** — a Postgres event outbox, a Kafka relay, and a fail-open pre-flight gate that calls the standalone [`policy-decision-service`](https://github.com/Ctwqk/policy-decision-service) and reads actor features from the in-repo `services/vp-feature-aggregator/` service before publication decisions.

The repository is a polyrepo-friendly monorepo: PDS remains a standalone repo, while the VP feature aggregator now lives under `services/vp-feature-aggregator/` and still communicates with PDS over HTTP and Kafka topics.

## Highlights

- **Dual-language backend**: Python FastAPI control plane (`backend/app/*`) for channel-ops, orchestration, and integration; Go services (`cmd/vp-api`, `cmd/vp-ffmpeg-worker`, `internal/*`) for low-latency HTTP and FFmpeg worker paths.
- **Heterogeneous worker tiers**: CPU FFmpeg transcode, GPU vision inference, headless browser automation, and LLM-driven channel agents, all coordinated through idempotent Redis queues with retry/backoff, dead-letter handling, and operator override paths.
- **Typed node registry + DAG orchestrator**: pipelines composed from registered node types, decoupling capability declaration from execution for pluggable ML, media-processing, and publishing steps.
- **Risk-control integration**: actor/action events emitted via a transactional Postgres outbox; a relay drains the outbox to Kafka topic `vp.actor.actions.v1` with exponential backoff and bounded-backlog metrics; the channel-ops service calls PDS for pre-flight decisions and treats unavailability as fail-open with explicit warning metadata.
- **Kubernetes-native deployment**: 20 production objects (7 Deployments, 9 Services, 3 StatefulSets, 1 ConfigMap) under a single namespace, extended by PDS, Redpanda, the feature aggregator, and the outbox relay manifests in the companion k8s repo.
- **Compose-first local development**: a base compose file plus a `docker-compose.pds-kafka.yml` override that adds Redpanda, PDS, the feature aggregator, and the relay without touching the main compose file.

## Tech Stack

- **Languages**: Python 3.12+, Go 1.25, TypeScript
- **Backend**: FastAPI, SQLAlchemy / asyncpg, Pydantic, chi (Go HTTP), prometheus-client, aiokafka
- **Storage**: PostgreSQL (runtime + outbox), Redis (queues + caches), MinIO (object storage)
- **Streaming**: Kafka via Redpanda (versioned topics `vp.actor.actions.v1`, `pds.decisions.v1`)
- **Frontend**: React + TypeScript + Vite
- **Ops**: Docker Compose, Kubernetes, Alembic migrations, pytest + Playwright

## Repository Layout

```text
backend/
├── app/
│   ├── api/                  # FastAPI routes
│   ├── autoflow/             # DAG-based workflow orchestrator
│   ├── channel_agent/        # LLM-driven channel agent + PDS gates
│   ├── events/               # Outbox writer, producer, relay
│   ├── node_registry/        # Typed node-type registry
│   ├── orchestrator/         # Pipeline orchestration
│   ├── pds_client.py         # Fail-open async PDS client
│   ├── services/             # Application services
│   ├── storage/, schemas/    # Persistence and event models
│   └── main.py, db.py
├── alembic/                  # Schema migrations (incl. 013_event_outbox)
├── event_outbox_relay.py     # Outbox → Kafka relay entry point
└── tests/                    # pytest suite (channel_agent, events, autoflow, ...)

cmd/
├── vp-api/                   # Go HTTP API sidecar
└── vp-ffmpeg-worker/         # Go FFmpeg worker

internal/                     # Go internal packages
├── httpapi/                  # chi-based HTTP server
├── orchestrator/             # Go pipeline orchestrator
├── pipeline/, worker/        # Worker dispatch
├── redisstream/, store/      # Redis stream + Postgres adapters
├── storage/                  # Object storage
├── config/, contracts/       # Config and shared types

frontend/                     # React + TS + Vite UI
services/
└── vp-feature-aggregator/     # Python Kafka consumer + actor feature API for PDS
docker-compose.yml            # Base local stack (~11 services)
docker-compose.pds-kafka.yml  # Risk-control extension override (+5 services)
docs/                         # Architecture, design specs, smoke runbooks
```

## Service Inventory

Base compose stack:

- `postgres`, `redis`, `minio`
- `api` (Python FastAPI), `api-go` (Go sidecar)
- `channel-agent-runner`, `ffmpeg-worker`, `ffmpeg-worker-go`, `vision-worker`
- `youtube-manager`, `platform-browser-manager`, `xiaohongshu-browser-manager`
- `frontend`

Risk-control override (`docker-compose.pds-kafka.yml`):

- `redpanda` (Kafka API), `pds` (Go decision service), `vp-feature-aggregator` (Python consumer + feature API), `event-outbox-relay` (Python)

## Quick Start

### Prerequisites

- Docker and Docker Compose
- (Optional) Go 1.25+ and Python 3.12+ for local builds outside containers

### Base stack

```bash
docker compose up -d --build
```

Then visit:

- API: `http://localhost:${API_PORT:-18080}/health`
- Frontend: `http://localhost:3001`
- MinIO Console: `http://localhost:9001`

### Risk-control stack (PDS + Kafka + aggregator)

```bash
docker compose -f docker-compose.yml -f docker-compose.pds-kafka.yml \
  up -d --build redpanda pds vp-feature-aggregator event-outbox-relay
```

See `docs/pds-kafka-smoke.md` for the full end-to-end smoke runbook (health, decision flow, feature lookup, decision audit).

## Kubernetes

Manifests live in the companion repository under `k8s-constructure/videoprocess/`:

- `videoprocess.yaml` — base stack (7 Deployments, 9 Services, 3 StatefulSets, 1 ConfigMap)
- `redpanda.yaml`, `vp-feature-aggregator.yaml`, `event-outbox-relay.yaml`, `pds.yaml` — risk-control extension
- `kustomization.yaml` — composes the namespace

```bash
kubectl kustomize /path/to/k8s-constructure/videoprocess | kubectl apply -f -
```

## Risk-Control Data Flow

```
VP worker (publish_video)
   │
   ├── (1) Postgres transaction: business write + outbox row
   │
   ├── (2) Synchronous POST /v1/decide → PDS
   │         (fail-open on timeout / 5xx)
   │
   └── (3) Outbox relay drains row → Kafka vp.actor.actions.v1

PDS                                    vp-feature-aggregator
 │ emits Decision events                 ▲
 ▼                                       │
Kafka pds.decisions.v1 ──────────────────┘
                                         │
                                         ▼
                                Sliding-window actor features
                                (5m / 1h / 24h / 7d)
                                         │
                                         ▼
                            GET /v1/features/{actor_id}
                                         ▲
                                         │
                                         PDS rules (next decide)
```

The loop is fail-open at every boundary: PDS client, feature provider, Kafka sink, and the outbox relay all degrade gracefully so the risk gate is never a single point of failure for the publishing path.

## Testing

```bash
# Python
cd backend && python3 -m pytest -q

# Go
go test ./...

# Lint (best-effort)
cd backend && python3 -m ruff check . || true
gofmt -l cmd internal | (! grep .)
```

## Related Repositories

- [`Ctwqk/policy-decision-service`](https://github.com/Ctwqk/policy-decision-service) — Go-based policy decision service (HTTP + gRPC, CEL rules, Aho-Corasick keywords, Kafka decision sink).
- [`Ctwqk/vp-feature-aggregator`](https://github.com/Ctwqk/vp-feature-aggregator) — historical source repository for the aggregator now vendored under `services/vp-feature-aggregator/`.

## Notes

- This repository is a monorepo of related services rather than a single small app; each subdirectory often has its own focused README.
- The legacy `voice_chat_bot/`, `TextToAudio/`, and `FasterWhisper/` directories are kept for historical media-processing experiments and are not on the critical path of the channel-ops / risk-control flow described above.
- The compose override pattern keeps the base stack and the risk-control extension independently composable so the recently merged Go sidecar work and the new PDS/Kafka integration do not collide.
