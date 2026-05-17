# AutoFlow Codex Task Guide

Use this guide when adding or reviewing AutoFlow work. Keep changes scoped to
the assigned branch and files, especially when multiple workers are active.

## Before Editing

1. Read `AGENTS.md`.
2. Read the relevant section of
   `docs/superpowers/plans/2026-05-16-videoprocess-autoflow-mode-c.md`.
3. Check `git status --short --branch`.
4. Inspect the existing service, schemas, tests, and API routes before assuming
   behavior is missing.

## Common Task Types

| Task | Primary files | Required checks |
| --- | --- | --- |
| Add or change planning behavior | `backend/app/autoflow/*`, `backend/tests/autoflow/*` | `cd backend && python3 -m pytest -q tests/autoflow` |
| Add a template | `template_library.py`, `pipeline_builder.py`, schema/docs/tests | Template tests plus pipeline validation tests |
| Add a node type or worker handler | `backend/app/node_registry/builtin/*`, `backend/worker/handlers/*` | Node registration tests and worker tests |
| Add API surface | `backend/app/api/autoflow.py`, frontend API/types if needed | API tests without live external services |
| Add docs or demos | `docs/autoflow/*`, `scripts/autoflow_demo_*.py` | Targeted docs/e2e tests and script dry review |

## Planning Rules

- Start from user intent, but build only from approved templates.
- Do not accept arbitrary workflow graphs from LLM output.
- Keep every generated pipeline compatible with
  `backend/app/schemas/pipeline.py`.
- Always validate with `validate_pipeline()` after building or repairing.
- Keep default safety settings conservative:
  `source_policy=owned_only`, `publish_mode=preview_only`.
- Do not remove existing APIs unless explicitly requested.
- If a task touches plan patch behavior, keep patches deterministic, schema
  compatible, and revalidated with `validate_pipeline()`.
- Treat DB-backed metrics and trend APIs as optional ranking/strategy context;
  they must not bypass templates, rights checks, or human public approval.

## Test Pattern

For new backend services, write tests first. Prefer service-level tests that do
not require a live server, database, Redis, network, or ffmpeg unless the task
specifically requires integration with those systems.

Useful checks:

```bash
cd backend
python3 -m pytest -q tests/autoflow
python3 -m ruff check . || true
python3 -m mypy app || true
```

For frontend changes:

```bash
cd frontend
npm install
npm run build
npm run lint || true
```

## Demo Scripts

The scripts under `scripts/autoflow_demo_*.py` are HTTP clients for a running
FastAPI server. They intentionally do not import backend internals. Use them for
manual smoke checks after starting the API:

```bash
python3 scripts/autoflow_demo_cat_compilation.py --base-url http://127.0.0.1:8000
python3 scripts/autoflow_demo_hot_topic.py --base-url http://127.0.0.1:8000
python3 scripts/autoflow_demo_material_remix.py --base-url http://127.0.0.1:8000
```

Each script exits non-zero if the API returns an invalid plan, the selected
intent/template does not match the scenario, or the expected safety state is
missing.

Demo summaries should keep the review gate visible: public approval is required
before public upload, plan patch flows must revalidate patched plans, and
DB-backed metrics are production context rather than demo-script inputs.

## Review Checklist

- Intent and template are deterministic for the prompt class.
- Candidate source and rights state match the source policy.
- `plan.validation.valid` is true and independent validation passes in tests.
- `needs_review` is true for external/research candidates and public publishing.
- Upload privacy is `private` or `unlisted`.
- New docs match actual code paths and endpoint names.
- Branch stays scoped to the assigned files.
