# AGENTS.md

## Project

VideoProcess is a FastAPI + React media workflow platform.

## Backend

- Python package root: `backend/`
- API modules: `backend/app/api/`
- Services: `backend/app/services/`
- Node registry: `backend/app/node_registry/`
- Worker handlers: `backend/worker/handlers/`

## Frontend

- React + TypeScript + Vite root: `frontend/`
- Pages: `frontend/src/pages/`
- API client: `frontend/src/api/`
- Editor components: `frontend/src/components/editor/`

## Required Checks

Run the following when files change.

### Backend

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

### Frontend

```bash
cd frontend
npm install
npm run build
npm run lint || true
```

## Rules

- Do not remove existing APIs unless explicitly requested.
- Add tests for all new services.
- Keep generated pipeline definitions compatible with `backend/app/schemas/pipeline.py`.
- All AutoFlow-generated workflows must pass `validate_pipeline()`.
- Default publication privacy must be `private` or `unlisted`.
- Do not let LLM output directly define arbitrary workflow graphs; use capabilities, templates, and deterministic builders.
- External platform assets must not be publicly published without explicit human review.
