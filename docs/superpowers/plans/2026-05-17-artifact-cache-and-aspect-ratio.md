# Artifact Cache And Aspect Ratio Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe deterministic intermediate artifact caching and make `concat_many` output sizing aspect-ratio aware.

**Architecture:** Introduce an `IntermediateArtifactCache` ORM table plus a focused orchestrator cache service that computes stable keys, validates cached artifacts, and upserts cache entries after successful node completion. Wire `JobEngine` so cache hits mark node executions succeeded before Redis dispatch, while cache misses and cache failures preserve existing execution behavior. Extend `concat_many` registry/worker sizing so explicit `width`/`height` stay authoritative and `aspect_ratio` fills the gap for standalone or planner-created nodes.

**Tech Stack:** FastAPI backend package, SQLAlchemy async ORM, Alembic, Redis stream orchestrator, pytest.

---

## File Map

- Create `backend/app/orchestrator/artifact_cache.py`: deterministic node allowlist, cache key builder, lookup, hit recording, and cache write service.
- Modify `backend/app/models/artifact.py`: add `IntermediateArtifactCache` model.
- Modify `backend/app/models/__init__.py`: export `IntermediateArtifactCache`.
- Add `backend/alembic/versions/008_intermediate_artifact_cache.py`: create cache table and indexes.
- Modify `backend/app/orchestrator/engine.py`: consult cache before dispatch and write cache after completion.
- Modify `backend/app/node_registry/builtin/concat_many.py`: add `aspect_ratio` param.
- Modify `backend/worker/handlers/concat_many.py`: infer dimensions from `aspect_ratio` and `_input_artifact_meta`.
- Modify `backend/app/autoflow/pipeline_builder.py`: include `aspect_ratio` in `concat_many` fallback config.
- Modify tests under `backend/tests/orchestrator`, `backend/tests/autoflow`, and `backend/tests/worker`.

## Task 1: Cache Model, Migration, And Key Service

**Files:**
- Create: `backend/app/orchestrator/artifact_cache.py`
- Add: `backend/alembic/versions/008_intermediate_artifact_cache.py`
- Modify: `backend/app/models/artifact.py`
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/tests/autoflow/test_schemas_models.py`
- Test: `backend/tests/orchestrator/test_artifact_cache.py`

- [ ] **Step 1: Write failing cache service tests**

Create `backend/tests/orchestrator/test_artifact_cache.py` with tests for stable config hashing, input/media-info invalidation, allowlist behavior, `disable_cache`, missing cache artifacts, and hit-count update.

Core assertions:

```python
assert service.cache_key("trim", {"duration": 5, "start_time": "0"}, [input_artifact]) == service.cache_key("trim", {"start_time": "0", "duration": 5}, [input_artifact])
assert service.cache_key("trim", {"duration": 5}, [input_artifact]) != service.cache_key("trim", {"duration": 6}, [input_artifact])
assert service.is_cache_eligible("trim", {"duration": 5}, ["input"]) is True
assert service.is_cache_eligible("youtube_upload", {}, ["input"]) is False
assert service.is_cache_eligible("trim", {"disable_cache": True}, ["input"]) is False
```

Add async tests with in-memory SQLite creating `Artifact.__table__` and `IntermediateArtifactCache.__table__`:

```python
hit = await service.lookup(db, node_type="trim", node_config={"duration": 5}, input_artifacts={"input": artifact})
assert hit is None
await service.store(db, node_type="trim", node_config={"duration": 5}, input_artifacts={"input": artifact}, output_artifact=output_artifact, node_id="trim_1", job_id=uuid.uuid4())
hit = await service.lookup(db, node_type="trim", node_config={"duration": 5}, input_artifacts={"input": artifact})
assert hit is not None
assert hit.output_artifact_id == output_artifact.id
await service.record_hit(db, hit)
assert hit.hit_count == 1
```

- [ ] **Step 2: Write failing model/migration assertions**

Update `backend/tests/autoflow/test_schemas_models.py` to assert:

```python
from app.models import IntermediateArtifactCache as ImportedCache
from app.models.artifact import IntermediateArtifactCache

assert ImportedCache is IntermediateArtifactCache
assert IntermediateArtifactCache.__tablename__ == "intermediate_artifact_cache"
assert isinstance(IntermediateArtifactCache.__table__.c.metadata_json.type, postgresql.JSON)
assert Path("alembic/versions/008_intermediate_artifact_cache.py").exists()
assert 'revision: str = "008"' in cache_migration_text
assert 'down_revision: Union[str, None] = "007"' in cache_migration_text
assert "intermediate_artifact_cache" in cache_migration_text
```

- [ ] **Step 3: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/orchestrator/test_artifact_cache.py tests/autoflow/test_schemas_models.py -q
```

Expected: fails because `IntermediateArtifactCache` and `artifact_cache.py` do not exist.

- [ ] **Step 4: Implement model, migration, and service**

Add `IntermediateArtifactCache` with columns:

```python
cache_key: str unique
node_type: str indexed
node_config_hash: str
input_signature_hash: str
output_artifact_id: UUID foreign key artifacts.id on delete cascade
created_at: server_default func.now()
last_used_at: server_default func.now()
hit_count: int default 0
metadata_json: dict default {}
```

Create Alembic revision `008_intermediate_artifact_cache.py` with matching table and indexes.

Create `IntermediateArtifactCacheService`:

```python
DETERMINISTIC_NODE_TYPES = {...}
TRANSIENT_CONFIG_KEYS = {"disable_cache", "cache_key", "retry_count", "worker_id", "queued_at", "started_at", "completed_at"}

def is_cache_eligible(node_type: str, node_config: dict, input_handles: Iterable[str]) -> bool
def cache_key(node_type: str, node_config: dict, input_artifacts: Mapping[str, Artifact]) -> str
async def lookup(db, *, node_type, node_config, input_artifacts) -> IntermediateArtifactCache | None
async def store(db, *, node_type, node_config, input_artifacts, output_artifact, node_id, job_id) -> None
async def record_hit(db, entry) -> None
```

Use sorted JSON for hashes, preserve input handle order with a natural sort that orders `video_2` before `video_10`, exclude config keys starting with `_`, and ignore transient keys.

- [ ] **Step 5: Verify green**

Run:

```bash
cd backend
python3 -m pytest tests/orchestrator/test_artifact_cache.py tests/autoflow/test_schemas_models.py -q
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```bash
git add backend/app/orchestrator/artifact_cache.py backend/app/models/artifact.py backend/app/models/__init__.py backend/alembic/versions/008_intermediate_artifact_cache.py backend/tests/orchestrator/test_artifact_cache.py backend/tests/autoflow/test_schemas_models.py
git commit -m "feat: add deterministic artifact cache service"
```

## Task 2: Orchestrator Cache Hit And Write Wiring

**Files:**
- Modify: `backend/app/orchestrator/engine.py`
- Test: `backend/tests/orchestrator/test_engine_artifact_cache.py`

- [ ] **Step 1: Write failing orchestrator tests**

Create `backend/tests/orchestrator/test_engine_artifact_cache.py` with fake Redis and in-memory DB tests:

```python
async def test_cache_hit_marks_node_succeeded_without_redis_dispatch(...):
    # Seed source and trim node executions, input artifact, output artifact, and cache entry.
    # Call JobEngine()._dispatch_ready_nodes(db, job, {"trim_1": ["source_1"]})
    # Assert trim status SUCCEEDED, output_artifact_id reused, redis xadd not called.

async def test_cache_miss_dispatches_redis_task(...):
    # Same graph without cache entry.
    # Assert trim status QUEUED and redis xadd called once.

async def test_node_completion_writes_cache_for_allowlisted_node(...):
    # Simulate completed trim node with input and output artifacts.
    # Call cache write helper or on_node_completed with patched session.
    # Assert one IntermediateArtifactCache row exists.

async def test_non_allowlisted_node_completion_does_not_write_cache(...):
    # Use youtube_upload node.
    # Assert no cache row exists.
```

- [ ] **Step 2: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/orchestrator/test_engine_artifact_cache.py -q
```

Expected: tests fail because engine does not consult or write cache.

- [ ] **Step 3: Implement cache lookup before dispatch**

In `_dispatch_ready_nodes()`:

1. Resolve `input_artifacts` as today.
2. Load `Artifact` objects for those ids.
3. Call cache service lookup.
4. On hit, set node execution `SUCCEEDED`, set `output_artifact_id`, set `progress=100`, set timestamps, record hit, commit, and skip Redis dispatch.
5. On miss or exception, dispatch normally.

Keep cache failures non-fatal and logged.

- [ ] **Step 4: Implement cache write after completion**

In `on_node_completed()` after persisting the output artifact id:

1. Reconstruct input artifact map from `ne.input_artifact_ids` and pipeline edges.
2. Load output artifact.
3. Call cache service store for eligible nodes.
4. Swallow/log cache write exceptions.

- [ ] **Step 5: Verify green**

Run:

```bash
cd backend
python3 -m pytest tests/orchestrator/test_engine_artifact_cache.py tests/orchestrator/test_engine_delivery_semantics.py tests/orchestrator/test_engine_retry.py -q
```

Expected: all selected orchestrator tests pass.

- [ ] **Step 6: Commit**

```bash
git add backend/app/orchestrator/engine.py backend/tests/orchestrator/test_engine_artifact_cache.py
git commit -m "feat: reuse cached intermediate artifacts"
```

## Task 3: concat_many Aspect Ratio Support

**Files:**
- Modify: `backend/app/node_registry/builtin/concat_many.py`
- Modify: `backend/worker/handlers/concat_many.py`
- Modify: `backend/app/autoflow/pipeline_builder.py`
- Test: `backend/tests/worker/test_concat_many_handler.py`
- Test: `backend/tests/autoflow/test_pipeline_builder.py`
- Test: `backend/tests/autoflow/test_node_registration.py`

- [ ] **Step 1: Write failing worker tests**

Append tests to `backend/tests/worker/test_concat_many_handler.py`:

```python
async def test_concat_many_infers_16x9_dimensions_from_aspect_ratio(monkeypatch):
    await ConcatManyHandler().execute({"aspect_ratio": "16:9"}, {"video_1": "a.mp4", "video_2": "b.mp4"}, "out.mp4")
    filter_complex = captured["args"][captured["args"].index("-filter_complex") + 1]
    assert "scale=1920:1080" in filter_complex
    assert "pad=1920:1080" in filter_complex

async def test_concat_many_explicit_dimensions_override_aspect_ratio(monkeypatch):
    await ConcatManyHandler().execute({"aspect_ratio": "16:9", "width": 720, "height": 1280}, {"video_1": "a.mp4", "video_2": "b.mp4"}, "out.mp4")
    assert "scale=720:1280" in filter_complex

async def test_concat_many_auto_aspect_ratio_uses_first_input_metadata(monkeypatch):
    await ConcatManyHandler().execute({"aspect_ratio": "auto", "_input_artifact_meta": {"video_1": {"width": 1920, "height": 1080}}}, {"video_1": "a.mp4", "video_2": "b.mp4"}, "out.mp4")
    assert "scale=1920:1080" in filter_complex
```

- [ ] **Step 2: Write failing registry/builder tests**

Update node registration tests to assert `concat_many` has an `aspect_ratio` param with `["9:16", "16:9", "1:1", "auto"]`.

Update pipeline builder tests to assert concat fallback config includes `aspect_ratio`.

- [ ] **Step 3: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_concat_many_handler.py tests/autoflow/test_node_registration.py tests/autoflow/test_pipeline_builder.py -q
```

Expected: tests fail because `aspect_ratio` is not implemented for `concat_many`.

- [ ] **Step 4: Implement aspect-ratio sizing**

In registry, add `ParamDefinition(name="aspect_ratio", param_type="select", default="9:16", options=["9:16", "16:9", "1:1", "auto"])`.

In worker, replace direct defaults:

```python
width = int(node_config.get("width") or 1080)
height = int(node_config.get("height") or 1920)
```

with helper:

```python
width, height = _target_dimensions(node_config)
```

Helper behavior:

- explicit width and height win;
- `16:9` gives `(1920, 1080)`;
- `1:1` gives `(1080, 1080)`;
- `auto` reads first selected input metadata from `_input_artifact_meta`;
- fallback is `(1080, 1920)`.

In pipeline builder concat fallback config, add `"aspect_ratio": intent.aspect_ratio`.

- [ ] **Step 5: Verify green**

Run:

```bash
cd backend
python3 -m pytest tests/worker/test_concat_many_handler.py tests/autoflow/test_node_registration.py tests/autoflow/test_pipeline_builder.py -q
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```bash
git add backend/app/node_registry/builtin/concat_many.py backend/worker/handlers/concat_many.py backend/app/autoflow/pipeline_builder.py backend/tests/worker/test_concat_many_handler.py backend/tests/autoflow/test_node_registration.py backend/tests/autoflow/test_pipeline_builder.py
git commit -m "feat: make concat many aspect ratio aware"
```

## Task 4: Full Verification

**Files:**
- No new files expected.

- [ ] **Step 1: Run targeted backend tests**

```bash
cd backend
python3 -m pytest tests/orchestrator/test_artifact_cache.py tests/orchestrator/test_engine_artifact_cache.py tests/orchestrator/test_engine_delivery_semantics.py tests/orchestrator/test_engine_retry.py tests/worker/test_concat_many_handler.py tests/autoflow/test_node_registration.py tests/autoflow/test_pipeline_builder.py tests/autoflow/test_schemas_models.py -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run required backend checks**

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: pytest passes. If ruff or mypy are not installed in this environment, capture the exact module-not-found output.

- [ ] **Step 3: Run hygiene checks**

```bash
git diff --check
git status --short
```

Expected: whitespace clean and no uncommitted implementation changes.
