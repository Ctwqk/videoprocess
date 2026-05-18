# AutoFlow Content Quality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make AutoFlow clip ranking, metadata, and storyboard pacing material-aware, platform-aware, and fallback-safe.

**Architecture:** Add focused AutoFlow services for platform profiles, embedding relevance, recent clip usage, and material-aware metadata. Existing ranker/generator APIs remain stable, with optional inputs for AI-enhanced scores and warnings. All network-backed behavior has deterministic fallback and tests use fakes, not live infra.

**Tech Stack:** FastAPI app package, Pydantic schemas, SQLAlchemy async ORM, Alembic migrations, pytest.

---

## File Map

- Create `backend/app/autoflow/platform_profiles.py`: owns platform pacing and merge rules.
- Create `backend/app/autoflow/embedding_relevance.py`: computes semantic relevance from configured embedding service or deterministic fallback.
- Create `backend/app/autoflow/recent_usage.py`: reads/writes `autoflow_used_clips`.
- Modify `backend/app/autoflow/clip_ranker.py`: accepts semantic scores, recent-use ids, and richer visual signals.
- Modify `backend/app/autoflow/metadata_generator.py`: uses clip facts, optional LLM client, grounded fallback metadata.
- Modify `backend/app/autoflow/storyboard_generator.py`: fits durations through `PlatformProfile`.
- Modify `backend/app/autoflow/content_strategy.py`: attaches platform profile summary to ideas.
- Modify `backend/app/autoflow/service.py`: loads recent ids, requests embedding relevance, wires metadata generator, records used clips.
- Modify `backend/app/config.py`: adds optional AutoFlow AI config.
- Modify `backend/app/models/autoflow.py` and `backend/app/models/__init__.py`: adds `AutoFlowUsedClip`.
- Add `backend/alembic/versions/007_autoflow_used_clips.py`: creates recent-use table.
- Modify tests under `backend/tests/autoflow/`: add red/green coverage for ranking, metadata, profiles, storyboard, models, and service wiring.

## Task 1: Platform Profiles And Storyboard Pacing

**Files:**
- Create: `backend/app/autoflow/platform_profiles.py`
- Modify: `backend/app/autoflow/storyboard_generator.py`
- Modify: `backend/app/autoflow/content_strategy.py`
- Test: `backend/tests/autoflow/test_platform_profiles.py`
- Test: `backend/tests/autoflow/test_storyboard_generator.py`
- Test: `backend/tests/autoflow/test_content_strategy.py`

- [ ] **Step 1: Write failing platform profile tests**

Add `backend/tests/autoflow/test_platform_profiles.py`:

```python
from __future__ import annotations

from app.autoflow.platform_profiles import PlatformProfileService


def test_platform_profile_merges_short_form_constraints():
    profile = PlatformProfileService().for_platforms(["youtube", "douyin", "bilibili"])

    assert profile.platform_key == "merged"
    assert profile.max_shot_seconds == 2.0
    assert profile.title_max_chars <= 40
    assert profile.motion_preference == 1.0
    assert "9:16" in profile.preferred_aspect_ratios
    assert profile.pacing_curve == "front_loaded"


def test_unknown_platform_uses_generic_profile():
    profile = PlatformProfileService().for_platforms(["unknown-platform"])

    assert profile.platform_key == "generic"
    assert profile.min_shot_seconds == 1.5
    assert profile.max_shot_seconds == 3.5
```

- [ ] **Step 2: Write failing storyboard pacing test**

Append to `backend/tests/autoflow/test_storyboard_generator.py`:

```python
def test_storyboard_fit_uses_short_video_hook_and_clamps():
    request = AutoFlowStoryboardRequest(
        prompt="我要一个 8 秒小猫视频，竖屏，可爱快节奏。",
        target_duration=8,
        aspect_ratio="9:16",
        target_platforms=["douyin"],
        source_strategy="input_video",
        allow_video_generation=False,
        min_shots=5,
        max_shots=5,
    )

    storyboard = StoryboardGenerator().generate(request).storyboard
    durations = [shot.target_duration for shot in storyboard.shots]

    assert sum(durations) == 8
    assert durations[0] == 1.0
    assert all(0.5 <= duration <= 2.0 for duration in durations)
    assert storyboard.extra["platform_profile"]["platform_key"] == "douyin"
```

- [ ] **Step 3: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_platform_profiles.py tests/autoflow/test_storyboard_generator.py -q
```

Expected: fails because `platform_profiles.py` does not exist and storyboard has no profile-aware duration fitting.

- [ ] **Step 4: Implement platform profiles and duration fit**

Create `platform_profiles.py` with a frozen dataclass `PlatformProfile`, constants for `generic`, `douyin`, `tiktok`, `youtube`, `youtube_shorts`, `bilibili`, and `PlatformProfileService.for_platforms()`.

Update `storyboard_generator.py` so `generate()` builds `platform_profile = PlatformProfileService().for_platforms(request.target_platforms)`, calls `_fit_durations(shots, target_duration, platform_profile)`, and stores `platform_profile.model_dump()` equivalent data in `storyboard.extra["platform_profile"]`.

Replace `_fit_durations()` with a profile-aware function:

```python
def _fit_durations(shots: list[ShotSpec], target_duration: float, profile: PlatformProfile) -> tuple[list[ShotSpec], list[str]]:
    if not shots:
        return [], []
    total = round(float(target_duration or len(shots) * profile.max_shot_seconds), 3)
    count = len(shots)
    min_total = profile.min_shot_seconds * count
    max_total = profile.max_shot_seconds * count
    relaxed = total < min_total or total > max_total
    hook = min(max(profile.hook_seconds, profile.min_shot_seconds), profile.max_shot_seconds)
    if count == 1:
        durations = [total]
    else:
        remaining = max(0.0, total - hook)
        weights = _pacing_weights(count - 1, profile.pacing_curve)
        weight_sum = sum(weights) or 1.0
        durations = [hook, *[remaining * weight / weight_sum for weight in weights]]
    if not relaxed:
        durations = [_clamp_duration(value, profile) for value in durations]
        durations = _redistribute_to_total(durations, total, profile)
    else:
        durations = _redistribute_relaxed(durations, total)
    updated = [
        shot.model_copy(
            update={
                "target_duration": round(durations[index], 3),
                "min_duration": profile.min_shot_seconds,
                "max_duration": max(profile.max_shot_seconds, round(durations[index], 3)),
            }
        )
        for index, shot in enumerate(shots)
    ]
    warnings = ["platform_pacing_relaxed"] if relaxed else []
    return updated, warnings
```

- [ ] **Step 5: Update content strategy profile summary**

Add `platform_profile` to generated idea dictionaries:

```python
profile = PlatformProfileService().for_platforms(target_platforms)
idea["platform_profile"] = profile.to_dict()
```

- [ ] **Step 6: Verify green**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_platform_profiles.py tests/autoflow/test_storyboard_generator.py tests/autoflow/test_content_strategy.py -q
```

Expected: all selected tests pass.

## Task 2: Ranker Visual Signals And Embedding Fallback

**Files:**
- Create: `backend/app/autoflow/embedding_relevance.py`
- Modify: `backend/app/autoflow/clip_ranker.py`
- Test: `backend/tests/autoflow/test_clip_ranker.py`
- Test: `backend/tests/autoflow/test_embedding_relevance.py`

- [ ] **Step 1: Write failing ranker tests**

Append to `test_clip_ranker.py`:

```python
def test_ranker_uses_semantic_relevance_scores_when_available():
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate("weak", title="generic office clip", asset_id="asset-weak", metadata={"duration": 5}),
            candidate("semantic", title="playful animal clip", asset_id="asset-semantic", metadata={"duration": 5}),
        ],
        semantic_relevance_scores={"asset-weak": 0.05, "asset-semantic": 0.98},
    )

    assert [item.id for item in ranked] == ["semantic", "weak"]
    assert ranked[0].score_breakdown["semantic_relevance"] == 0.98


def test_ranker_penalizes_recently_used_asset_ids():
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate("fresh", title="小猫 fresh", asset_id="asset-fresh", metadata={"duration": 5}),
            candidate("recent", title="小猫 recent", asset_id="asset-recent", metadata={"duration": 5}),
        ],
        recent_used_asset_ids={"asset-recent"},
    )

    assert [item.id for item in ranked] == ["fresh", "recent"]
    assert ranked[1].score_breakdown["recent_used_penalty"] == 1.0


def test_ranker_uses_visual_face_scene_and_brightness_signals():
    ranked = ClipRanker().rank(
        intent(),
        [
            candidate("plain", title="小猫 plain", asset_id="asset-plain", metadata={"duration": 5}),
            candidate(
                "visual",
                title="小猫 visual",
                asset_id="asset-visual",
                metadata={
                    "duration": 5,
                    "visual": {
                        "motion_score": 0.8,
                        "face_present": True,
                        "scene_change_score": 0.9,
                        "brightness_score": 0.85,
                    },
                },
            ),
        ],
    )

    assert [item.id for item in ranked] == ["visual", "plain"]
    assert ranked[0].score_breakdown["face_present"] == 1.0
    assert ranked[0].score_breakdown["scene_change_diversity"] == 0.9
    assert ranked[0].score_breakdown["brightness_fit"] == 0.85
```

- [ ] **Step 2: Write failing embedding fallback tests**

Add `backend/tests/autoflow/test_embedding_relevance.py`:

```python
from __future__ import annotations

import pytest

from app.autoflow.embedding_relevance import EmbeddingRelevanceService
from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent


@pytest.mark.asyncio
async def test_embedding_relevance_falls_back_without_endpoint():
    service = EmbeddingRelevanceService(embedding_url="")
    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", keywords=["kitten"])
    candidates = [
        AutoFlowClipCandidate(id="a", title="office", source_type="asset", asset_id="asset-a"),
        AutoFlowClipCandidate(id="b", title="小猫 kitten jumps", source_type="asset", asset_id="asset-b"),
    ]

    result = await service.score(intent, candidates)

    assert result.scores["asset-b"] > result.scores["asset-a"]
    assert result.warnings == []


@pytest.mark.asyncio
async def test_embedding_relevance_records_warning_on_client_failure():
    async def failing_embedder(_texts: list[str]) -> list[list[float]]:
        raise RuntimeError("embedding down")

    service = EmbeddingRelevanceService(embedding_url="http://embedding.test", embedder=failing_embedder)
    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", keywords=["kitten"])
    candidates = [AutoFlowClipCandidate(id="a", title="小猫", source_type="asset", asset_id="asset-a")]

    result = await service.score(intent, candidates)

    assert result.scores["asset-a"] > 0
    assert "embedding_relevance_unavailable" in result.warnings
```

- [ ] **Step 3: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_clip_ranker.py tests/autoflow/test_embedding_relevance.py -q
```

Expected: fails because ranker signature and embedding service do not exist yet.

- [ ] **Step 4: Implement ranker optional inputs and visual signals**

Change `ClipRanker.rank()` signature:

```python
def rank(
    self,
    intent: AutoFlowIntent,
    candidates: Iterable[AutoFlowClipCandidate],
    historical_performance: dict[str, Any] | None = None,
    *,
    semantic_relevance_scores: dict[str, float] | None = None,
    recent_used_asset_ids: set[str] | None = None,
    platform_profile: PlatformProfile | None = None,
) -> list[AutoFlowClipCandidate]:
```

Use `_candidate_score_key(candidate)` to look up semantic scores by `asset_id`
or candidate id. Add breakdown keys `semantic_relevance`, `intent_fit`,
`face_present`, `scene_change_diversity`, `brightness_fit`, `platform_fit`, and
`recent_used_penalty`. Keep `topic_relevance` for backward-compatible tests but
make it equal to the deterministic fallback component when semantic scores are
not supplied.

- [ ] **Step 5: Implement embedding relevance service**

Create `EmbeddingRelevanceService` with:

```python
@dataclass
class RelevanceResult:
    scores: dict[str, float]
    warnings: list[str]
```

The service should build one intent text and one text per candidate, call an
optional injected async embedder when `embedding_url` is set, parse both
`embeddings` and `vectors` payload shapes, compute cosine similarity, and fall
back to deterministic token relevance on any exception.

- [ ] **Step 6: Verify green**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_clip_ranker.py tests/autoflow/test_embedding_relevance.py -q
```

Expected: all selected tests pass.

## Task 3: Recent Clip Usage Persistence

**Files:**
- Create: `backend/app/autoflow/recent_usage.py`
- Create: `backend/alembic/versions/007_autoflow_used_clips.py`
- Modify: `backend/app/models/autoflow.py`
- Modify: `backend/app/models/__init__.py`
- Modify: `backend/tests/autoflow/test_autoflow_api.py`
- Modify: `backend/tests/autoflow/test_schemas_models.py`
- Test: `backend/tests/autoflow/test_recent_usage.py`

- [ ] **Step 1: Write failing model and migration tests**

Update `test_schemas_models.py` to import `AutoFlowUsedClip`, assert its table
name is `autoflow_used_clips`, and assert migration `007_autoflow_used_clips.py`
exists with revision `007`, down revision `006`, and table name text.

- [ ] **Step 2: Write failing recent usage service tests**

Add `backend/tests/autoflow/test_recent_usage.py`:

```python
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.autoflow.recent_usage import RecentClipUsageStore
from app.models.autoflow import AutoFlowUsedClip
from app.schemas.autoflow import AutoFlowClipCandidate


@pytest.fixture
async def usage_db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(AutoFlowUsedClip.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


@pytest.mark.asyncio
async def test_recent_usage_reads_only_last_seven_days(usage_db_session):
    now = datetime.now(timezone.utc)
    usage_db_session.add_all(
        [
            AutoFlowUsedClip(run_id=uuid.uuid4(), asset_id="recent", selected_at=now - timedelta(days=2)),
            AutoFlowUsedClip(run_id=uuid.uuid4(), asset_id="old", selected_at=now - timedelta(days=9)),
        ]
    )
    await usage_db_session.commit()

    result = await RecentClipUsageStore(now=lambda: now).load_recent_asset_ids(usage_db_session)

    assert result == {"recent"}


@pytest.mark.asyncio
async def test_recent_usage_records_selected_asset_ids(usage_db_session):
    run_id = str(uuid.uuid4())
    candidates = [
        AutoFlowClipCandidate(
            id="c1",
            title="小猫",
            source_type="asset",
            asset_id="asset-1",
            metadata={"source_platform": "bilibili"},
        )
    ]

    await RecentClipUsageStore().record_selected_clips(usage_db_session, run_id=run_id, candidates=candidates)

    result = await RecentClipUsageStore().load_recent_asset_ids(usage_db_session)
    assert result == {"asset-1"}
```

- [ ] **Step 3: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_recent_usage.py tests/autoflow/test_schemas_models.py -q
```

Expected: fails because model, migration, and service do not exist.

- [ ] **Step 4: Implement model, migration, store**

Add `AutoFlowUsedClip` with UUID primary key, `run_id`, `asset_id`,
`source_platform`, `candidate_title`, `selected_at`, and `metadata_json`.
Create Alembic revision `007` with indexes on `(asset_id, selected_at)` and
`run_id`. Implement `RecentClipUsageStore.load_recent_asset_ids()` and
`record_selected_clips()`.

- [ ] **Step 5: Update DB test fixture**

In `test_autoflow_api.py`, include `AutoFlowUsedClip.__table__` when creating
the in-memory SQLite schema.

- [ ] **Step 6: Verify green**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_recent_usage.py tests/autoflow/test_schemas_models.py tests/autoflow/test_autoflow_api.py -q
```

Expected: selected tests pass.

## Task 4: Material-Aware Metadata With LLM Fallback

**Files:**
- Modify: `backend/app/autoflow/metadata_generator.py`
- Test: `backend/tests/autoflow/test_metadata_generator.py`

- [ ] **Step 1: Write failing metadata tests**

Replace and extend metadata tests with:

```python
class FakeMetadataClient:
    def generate(self, payload: dict) -> dict:
        return {
            "titles": ["小猫追玩具的高光合集", "猫咪玩具挑战"],
            "thumbnail_texts": ["小猫追玩具"],
            "tags": ["小猫", "追玩具", "可爱"],
            "rationale": "grounded in object labels",
        }


def test_metadata_generator_uses_clip_facts_for_title_candidates():
    intent = AutoFlowIntent(
        intent_type="animal_compilation",
        subject="小猫",
        target_platforms=["douyin"],
        keywords=["可爱"],
    )
    candidates = [
        AutoFlowClipCandidate(
            id="c1",
            title="小猫追玩具",
            source_type="asset",
            asset_id="asset-1",
            metadata={"visual": {"object_labels": ["玩具"], "dominant_action": "追玩具"}},
        )
    ]

    metadata = MetadataGenerator(llm_client=FakeMetadataClient()).generate(intent, candidates)

    assert metadata.selected_title == "小猫追玩具的高光合集"
    assert "小猫追玩具" in metadata.thumbnail_text_candidates
    assert "metadata_llm" not in " ".join(metadata.platform_payloads["douyin"].get("warnings", []))


def test_metadata_generator_fallback_avoids_unverifiable_last_seconds_claim():
    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", target_platforms=["douyin"])
    candidates = [AutoFlowClipCandidate(id="c1", title="小猫晒太阳", source_type="asset", asset_id="asset-1")]

    metadata = MetadataGenerator().generate(intent, candidates)

    combined = " ".join([*metadata.title_candidates, *metadata.thumbnail_text_candidates])
    assert "最后 2 秒" not in combined
    assert metadata.selected_title


def test_metadata_generator_rejects_ungrounded_thumbnail_text():
    class UngroundedClient:
        def generate(self, payload: dict) -> dict:
            return {"titles": ["小猫晒太阳"], "thumbnail_texts": ["最后反转"], "tags": ["小猫"]}

    intent = AutoFlowIntent(intent_type="animal_compilation", subject="小猫", target_platforms=["douyin"])
    candidates = [AutoFlowClipCandidate(id="c1", title="小猫晒太阳", source_type="asset", asset_id="asset-1")]

    metadata = MetadataGenerator(llm_client=UngroundedClient()).generate(intent, candidates)

    assert "最后反转" not in metadata.thumbnail_text_candidates
    assert "metadata_llm_ungrounded_claims_removed" in metadata.platform_payloads["douyin"]["warnings"]
```

- [ ] **Step 2: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_metadata_generator.py -q
```

Expected: fails because `llm_client` injection and grounding validation do not exist.

- [ ] **Step 3: Implement metadata grounding**

Update `MetadataGenerator.__init__(llm_client=None, platform_profiles=None)` and
keep `generate(intent, candidates)` stable. Build clip facts from candidate
title, description, tags, visual object labels, dominant action, source
platform, and duration. If a client exists, call `client.generate(payload)`,
validate titles and thumbnail texts against length and grounding, and record
warnings in each platform payload. If no valid LLM candidates remain, use
deterministic fallback titles derived from subject plus observed action/object
labels.

- [ ] **Step 4: Verify green**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_metadata_generator.py -q
```

Expected: metadata tests pass.

## Task 5: Service Wiring, Config, And Full Verification

**Files:**
- Modify: `backend/app/config.py`
- Modify: `backend/app/autoflow/service.py`
- Modify: `backend/tests/autoflow/test_autoflow_api.py`
- Modify: `backend/tests/autoflow/test_e2e_examples.py`

- [ ] **Step 1: Write failing service integration tests**

Add a test that creates a DB-backed plan with one previously used asset in
`AutoFlowUsedClip` and verifies the plan warning-free ranking puts a fresh
fixture/candidate ahead when candidates are otherwise similar. Add an execute
test that verifies `autoflow_used_clips` receives selected asset ids after a
persisted run is saved.

- [ ] **Step 2: Verify red**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_autoflow_api.py tests/autoflow/test_e2e_examples.py -q
```

Expected: new service integration assertions fail.

- [ ] **Step 3: Add config and wire services**

Add optional settings:

```python
autoflow_ai_enabled: bool = False
autoflow_llm_gateway_url: str = "http://127.0.0.1:8000"
autoflow_llm_source: str = "videoprocess"
autoflow_llm_profile: str = "generic_chat"
autoflow_embedding_url: str = ""
autoflow_qdrant_url: str = "http://127.0.0.1:6333"
autoflow_ai_timeout_seconds: float = 8.0
```

In `AutoFlowService.__init__`, create `EmbeddingRelevanceService()` and
`RecentClipUsageStore()`. In `plan()`, load recent ids when DB is available,
score embedding relevance, and pass both to `ClipRanker.rank()`. In `execute()`,
record selected candidates through `RecentClipUsageStore` after a run id is
known. Any recent-use or embedding failure adds the structured warning to plan
warnings or run artifacts without failing the user action.

- [ ] **Step 4: Verify targeted tests**

Run:

```bash
cd backend
python3 -m pytest tests/autoflow/test_clip_ranker.py tests/autoflow/test_embedding_relevance.py tests/autoflow/test_recent_usage.py tests/autoflow/test_metadata_generator.py tests/autoflow/test_platform_profiles.py tests/autoflow/test_content_strategy.py tests/autoflow/test_storyboard_generator.py tests/autoflow/test_autoflow_api.py -q
```

Expected: selected tests pass.

- [ ] **Step 5: Run required backend checks**

Run:

```bash
cd backend
python3 -m pytest
python3 -m ruff check . || true
python3 -m mypy app || true
```

Expected: pytest passes. Ruff and mypy may report module-not-installed in this environment; capture the exact output.

- [ ] **Step 6: Final hygiene**

Run:

```bash
git diff --check
git status --short
```

Expected: whitespace check clean; status shows only intended files.
