# Worker Admission and Compose Safety Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent underconfigured production Python `ffmpeg` workers from joining production Redis streams, while keeping local worker development explicitly opt-in.

**Architecture:** Add a small pure Python admission module that validates worker settings before any Redis consumer group or DB engine is created. Wire the worker entrypoint to run that validation first, and move the local Compose Python `ffmpeg-worker` behind a `local-python-worker` profile.

**Tech Stack:** Python 3.12, Pydantic settings, pytest, Docker Compose YAML.

## Global Constraints

- Production queue consumer means `DEPLOY_MODE` is `shared` or `production`, or `REDIS_URL` points to a non-localhost host.
- For production `WORKER_TYPE=ffmpeg`, require `STORAGE_BACKEND=minio`.
- For production `WORKER_TYPE=ffmpeg`, require non-empty `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and `MINIO_BUCKET`.
- Reject production MinIO endpoints on `localhost`, `127.0.0.1`, `0.0.0.0`, or `::1`.
- Require explicit `WORKER_HOST` for production queue consumers.
- Do not add migrations, worker registration tables, Redis ACLs, upload-operation idempotency, or staged metrics in this update.
- Keep local development supported when Redis is local and deploy mode is not production/shared.

---

## File Structure

- Create `backend/app/services/worker_admission.py`: pure validation helpers and a process-level `enforce_worker_admission_from_env()` entrypoint.
- Create `backend/tests/worker/test_worker_admission.py`: unit tests for local, remote, shared-mode, and unsafe MinIO cases.
- Modify `backend/worker/main.py`: defer DB engine creation until after admission and call the guard before Redis/DB setup.
- Modify `docker-compose.yml`: add `profiles: ["local-python-worker"]` to `ffmpeg-worker` and default its `DEPLOY_MODE` to `local`.
- Create `tests/test_compose_worker_profiles.py`: repository-level Compose safety test.

---

### Task 1: Worker Admission Validation

**Files:**
- Create: `backend/app/services/worker_admission.py`
- Test: `backend/tests/worker/test_worker_admission.py`

**Interfaces:**
- Produces: `WorkerAdmissionDecision(allowed: bool, reasons: tuple[str, ...])`
- Produces: `validate_worker_admission(env: Mapping[str, str]) -> WorkerAdmissionDecision`
- Produces: `enforce_worker_admission_from_env(env: Mapping[str, str] | None = None) -> None`

- [ ] **Step 1: Write the failing tests**

Create `backend/tests/worker/test_worker_admission.py`:

```python
from __future__ import annotations

import pytest

from app.services.worker_admission import (
    WorkerAdmissionError,
    enforce_worker_admission_from_env,
    validate_worker_admission,
)


def test_local_worker_with_local_storage_is_allowed() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://localhost:6379/0",
            "WORKER_TYPE": "ffmpeg",
            "STORAGE_BACKEND": "local",
        }
    )

    assert decision.allowed is True
    assert decision.reasons == ()


def test_remote_ffmpeg_worker_requires_minio_storage() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "local",
        }
    )

    assert decision.allowed is False
    assert "production ffmpeg workers require STORAGE_BACKEND=minio" in decision.reasons


def test_remote_ffmpeg_worker_rejects_localhost_minio_endpoint() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "localhost:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is False
    assert "production MinIO endpoint must not point at localhost:9000" in decision.reasons


def test_remote_ffmpeg_worker_with_minio_and_explicit_host_is_allowed() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "local",
            "REDIS_URL": "redis://10.0.0.150:6380/0",
            "WORKER_TYPE": "ffmpeg",
            "WORKER_HOST": "150-gpu",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "10.0.0.150:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is True
    assert decision.reasons == ()


def test_shared_mode_requires_explicit_worker_host_even_with_local_redis() -> None:
    decision = validate_worker_admission(
        {
            "DEPLOY_MODE": "shared",
            "REDIS_URL": "redis://localhost:6379/0",
            "WORKER_TYPE": "ffmpeg",
            "STORAGE_BACKEND": "minio",
            "MINIO_ENDPOINT": "10.0.0.150:9000",
            "MINIO_ACCESS_KEY": "minioadmin",
            "MINIO_SECRET_KEY": "minioadmin",
            "MINIO_BUCKET": "videoprocess",
        }
    )

    assert decision.allowed is False
    assert "production workers require explicit WORKER_HOST" in decision.reasons


def test_enforce_raises_with_all_denial_reasons() -> None:
    with pytest.raises(WorkerAdmissionError) as exc:
        enforce_worker_admission_from_env(
            {
                "DEPLOY_MODE": "shared",
                "REDIS_URL": "redis://10.0.0.150:6380/0",
                "WORKER_TYPE": "ffmpeg",
                "STORAGE_BACKEND": "local",
            }
        )

    message = str(exc.value)
    assert "production workers require explicit WORKER_HOST" in message
    assert "production ffmpeg workers require STORAGE_BACKEND=minio" in message
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_worker_admission.py -q
```

Expected: FAIL because `app.services.worker_admission` does not exist.

- [ ] **Step 3: Implement worker admission module**

Create `backend/app/services/worker_admission.py`:

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlparse


LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "0.0.0.0", "::1"}
PRODUCTION_DEPLOY_MODES = {"shared", "production"}


class WorkerAdmissionError(RuntimeError):
    """Raised when a worker is not allowed to join production queues."""


@dataclass(frozen=True)
class WorkerAdmissionDecision:
    allowed: bool
    reasons: tuple[str, ...] = ()


def validate_worker_admission(env: Mapping[str, str]) -> WorkerAdmissionDecision:
    deploy_mode = _env_value(env, "DEPLOY_MODE", "shared").lower()
    redis_url = _env_value(env, "REDIS_URL", "redis://localhost:6379/0")
    worker_type = _env_value(env, "WORKER_TYPE", "ffmpeg").lower()
    storage_backend = _env_value(env, "STORAGE_BACKEND", "local").lower()
    reasons: list[str] = []

    if not _is_production_queue_consumer(deploy_mode=deploy_mode, redis_url=redis_url):
        return WorkerAdmissionDecision(allowed=True)

    if not _env_value(env, "WORKER_HOST", ""):
        reasons.append("production workers require explicit WORKER_HOST")

    if worker_type == "ffmpeg":
        if storage_backend != "minio":
            reasons.append("production ffmpeg workers require STORAGE_BACKEND=minio")

        for key in ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET"):
            if not _env_value(env, key, ""):
                reasons.append(f"production ffmpeg workers require {key}")

        minio_endpoint = _env_value(env, "MINIO_ENDPOINT", "")
        minio_host = _host_from_endpoint(minio_endpoint)
        if minio_host in LOCAL_HOSTS:
            reasons.append(f"production MinIO endpoint must not point at {minio_endpoint}")

    return WorkerAdmissionDecision(allowed=not reasons, reasons=tuple(reasons))


def enforce_worker_admission_from_env(env: Mapping[str, str] | None = None) -> None:
    decision = validate_worker_admission(os.environ if env is None else env)
    if not decision.allowed:
        raise WorkerAdmissionError("; ".join(decision.reasons))


def _env_value(env: Mapping[str, str], key: str, default: str) -> str:
    value = env.get(key, default)
    return str(value).strip()


def _is_production_queue_consumer(*, deploy_mode: str, redis_url: str) -> bool:
    if deploy_mode in PRODUCTION_DEPLOY_MODES:
        return True
    redis_host = urlparse(redis_url).hostname or ""
    return redis_host.lower() not in LOCAL_HOSTS


def _host_from_endpoint(endpoint: str) -> str:
    if not endpoint:
        return ""
    parsed = urlparse(endpoint if "://" in endpoint else f"//{endpoint}")
    return (parsed.hostname or endpoint.split(":", 1)[0]).lower().strip("[]")
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_worker_admission.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add backend/app/services/worker_admission.py backend/tests/worker/test_worker_admission.py
git commit -m "feat: add worker admission guard"
```

---

### Task 2: Worker Entrypoint and Compose Safety

**Files:**
- Modify: `backend/worker/main.py`
- Modify: `docker-compose.yml`
- Create: `tests/test_compose_worker_profiles.py`

**Interfaces:**
- Consumes: `enforce_worker_admission_from_env(env: Mapping[str, str] | None = None) -> None`
- Produces: `get_worker_session() -> async_sessionmaker`

- [ ] **Step 1: Write the failing Compose safety test**

Create `tests/test_compose_worker_profiles.py`:

```python
from __future__ import annotations

from pathlib import Path

import yaml


def test_python_ffmpeg_worker_is_local_profile_only() -> None:
    compose = yaml.safe_load(Path("docker-compose.yml").read_text())
    service = compose["services"]["ffmpeg-worker"]

    assert "local-python-worker" in service.get("profiles", [])
    assert service["environment"]["DEPLOY_MODE"] == "${DEPLOY_MODE:-local}"
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
python3 -m pytest tests/test_compose_worker_profiles.py -q
```

Expected: FAIL because `ffmpeg-worker` has no `local-python-worker` profile and defaults to `DEPLOY_MODE=shared`.

- [ ] **Step 3: Wire admission before Redis/DB setup**

Modify the top of `backend/worker/main.py` so DB engine state is initialized
after admission:

```python
from app.config import settings
from app.models.artifact import Artifact, ArtifactKind
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.services.worker_admission import WorkerAdmissionError, enforce_worker_admission_from_env
from app.storage.manager import get_storage
from worker.handlers import HANDLER_MAP
from worker.handlers.base import CancelledError
```

Replace the module-level DB engine/session creation with lazy helpers:

```python
engine_db = None
worker_session = None


def configure_worker_database() -> None:
    global engine_db, worker_session
    if engine_db is not None and worker_session is not None:
        return
    engine_db = create_async_engine(
        settings.database_url,
        echo=False,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    worker_session = async_sessionmaker(engine_db, expire_on_commit=False)


def get_worker_session() -> async_sessionmaker:
    if worker_session is None:
        configure_worker_database()
    assert worker_session is not None
    return worker_session
```

In DB call sites, replace `worker_session()` with `get_worker_session()()`.
For example:

```python
async with get_worker_session()() as db:
    ne = await db.get(NodeExecution, uuid.UUID(node_execution_id))
```

Modify `main()` to enforce admission before `_redis()`:

```python
async def main() -> None:
    """Main worker loop: consume tasks from Redis Stream."""
    try:
        enforce_worker_admission_from_env()
    except WorkerAdmissionError as exc:
        logger.critical("Worker admission denied: %s", exc)
        raise SystemExit(2) from exc

    configure_worker_database()
    r = _redis()
```

Modify final cleanup to handle the lazy engine:

```python
    finally:
        await r.aclose()
        if engine_db is not None:
            await engine_db.dispose()
```

- [ ] **Step 4: Move local Python worker behind an explicit profile**

In `docker-compose.yml`, edit the `ffmpeg-worker` service:

```yaml
  ffmpeg-worker:
    profiles: ["local-python-worker"]
    build:
      context: ./backend
      dockerfile: Dockerfile.worker
    container_name: vp_ffmpeg_worker_1
    restart: unless-stopped
    environment:
      DEPLOY_MODE: ${DEPLOY_MODE:-local}
```

Leave the rest of the service unchanged.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_worker_admission.py -q
cd ..
python3 -m pytest tests/test_compose_worker_profiles.py -q
```

Expected: both PASS.

- [ ] **Step 6: Run required backend checks**

Run:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: pytest PASS. Ruff and mypy findings may be existing project debt because AGENTS.md allows them with `|| true`; record their output in the completion note.

- [ ] **Step 7: Commit**

Run:

```bash
git add backend/worker/main.py docker-compose.yml tests/test_compose_worker_profiles.py
git commit -m "fix: gate production python workers before queue join"
```

---

## Plan Self-Review

- Spec coverage: Task 1 covers admission semantics and local-development behavior. Task 2 covers early worker entrypoint enforcement and Compose profile safety.
- Placeholder scan: no placeholder tasks remain; each test and code step contains concrete content.
- Type consistency: `validate_worker_admission`, `enforce_worker_admission_from_env`, `WorkerAdmissionDecision`, and `WorkerAdmissionError` names are consistent across tasks.
