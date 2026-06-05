# AGENTS.md

## Project

VP Feature Aggregator is a narrow FastAPI service for VideoProcess actor feature aggregation.

## Boundaries

- This service owns versioned Kafka event schemas, actor feature windows, and the feature-read API used by PDS.
- This service lives inside the VideoProcess repo but must remain independently buildable and testable.
- Do not move allow/block/flag decision logic into this service.
- Do not mutate VideoProcess or PDS source-of-truth records from this service.

## Required Checks

Run these when files change:

```bash
python3 -m pytest -q
python3 -m ruff check . || true
```
