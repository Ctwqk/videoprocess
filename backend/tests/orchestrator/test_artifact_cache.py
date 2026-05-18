from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.artifact import Artifact, ArtifactKind, IntermediateArtifactCache
from app.orchestrator.artifact_cache import IntermediateArtifactCacheService


@pytest.fixture
async def cache_db_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Artifact.__table__.create)
        await conn.run_sync(IntermediateArtifactCache.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


def artifact(
    *,
    artifact_id: uuid.UUID | None = None,
    storage_path: str = "artifacts/input.mp4",
    file_size: int = 123,
    media_info: dict | None = None,
) -> Artifact:
    return Artifact(
        id=artifact_id or uuid.uuid4(),
        job_id=uuid.uuid4(),
        node_execution_id=uuid.uuid4(),
        kind=ArtifactKind.INTERMEDIATE,
        filename=storage_path.rsplit("/", 1)[-1],
        mime_type="video/mp4",
        file_size=file_size,
        storage_backend="local",
        storage_path=storage_path,
        media_info=media_info or {"width": 1080, "height": 1920},
    )


def test_cache_key_is_stable_for_config_order_and_changes_for_inputs():
    service = IntermediateArtifactCacheService()
    input_artifact = artifact()
    changed_media_artifact = artifact(
        artifact_id=input_artifact.id,
        storage_path=input_artifact.storage_path,
        file_size=input_artifact.file_size or 0,
        media_info={"width": 1920, "height": 1080},
    )

    first_key = service.cache_key("trim", {"duration": 5, "start_time": "0"}, {"input": input_artifact})
    second_key = service.cache_key("trim", {"start_time": "0", "duration": 5}, {"input": input_artifact})
    changed_config_key = service.cache_key("trim", {"duration": 6, "start_time": "0"}, {"input": input_artifact})
    changed_media_key = service.cache_key("trim", {"duration": 5, "start_time": "0"}, {"input": changed_media_artifact})

    assert first_key == second_key
    assert first_key != changed_config_key
    assert first_key != changed_media_key


def test_cache_eligibility_uses_allowlist_inputs_and_disable_flag():
    service = IntermediateArtifactCacheService()

    assert service.is_cache_eligible("trim", {"duration": 5}, ["input"]) is True
    assert service.is_cache_eligible("youtube_upload", {}, ["input"]) is False
    assert service.is_cache_eligible("trim", {"disable_cache": True}, ["input"]) is False
    assert service.is_cache_eligible("trim", {"duration": 5}, []) is False


@pytest.mark.asyncio
async def test_cache_lookup_store_and_record_hit(cache_db_session):
    service = IntermediateArtifactCacheService()
    input_artifact = artifact(storage_path="artifacts/input.mp4")
    output_artifact = artifact(storage_path="artifacts/output.mp4")
    cache_db_session.add_all([input_artifact, output_artifact])
    await cache_db_session.commit()

    miss = await service.lookup(
        cache_db_session,
        node_type="trim",
        node_config={"duration": 5},
        input_artifacts={"input": input_artifact},
    )
    assert miss is None

    await service.store(
        cache_db_session,
        node_type="trim",
        node_config={"duration": 5},
        input_artifacts={"input": input_artifact},
        output_artifact=output_artifact,
        node_id="trim_1",
        job_id=uuid.uuid4(),
    )
    hit = await service.lookup(
        cache_db_session,
        node_type="trim",
        node_config={"duration": 5},
        input_artifacts={"input": input_artifact},
    )

    assert hit is not None
    assert hit.output_artifact_id == output_artifact.id
    await service.record_hit(cache_db_session, hit)
    assert hit.hit_count == 1


@pytest.mark.asyncio
async def test_cache_entry_with_missing_output_artifact_is_miss(cache_db_session):
    service = IntermediateArtifactCacheService()
    input_artifact = artifact(storage_path="artifacts/input.mp4")
    cache_db_session.add(input_artifact)
    await cache_db_session.commit()
    cache_key = service.cache_key("trim", {"duration": 5}, {"input": input_artifact})
    cache_db_session.add(
        IntermediateArtifactCache(
            cache_key=cache_key,
            node_type="trim",
            node_config_hash="config",
            input_signature_hash="input",
            output_artifact_id=uuid.uuid4(),
            metadata_json={},
        )
    )
    await cache_db_session.commit()

    hit = await service.lookup(
        cache_db_session,
        node_type="trim",
        node_config={"duration": 5},
        input_artifacts={"input": input_artifact},
    )
    remaining = (await cache_db_session.execute(select(IntermediateArtifactCache))).scalars().all()

    assert hit is None
    assert remaining == []
