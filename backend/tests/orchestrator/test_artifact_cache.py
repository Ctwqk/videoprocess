from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.artifact import Artifact, ArtifactKind, IntermediateArtifactCache
from app.orchestrator.artifact_cache import IntermediateArtifactCacheService
from app.services.external_url_identity import normalize_external_media_url


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
    assert service.is_cache_eligible(
        "url_download",
        {"url": "https://example.test/video", "format": "best"},
        [],
    ) is True
    assert service.is_cache_eligible(
        "url_download",
        {
            "url": "https://example.test/video",
            "format": "best",
            "disable_cache": True,
        },
        [],
    ) is False


@pytest.mark.asyncio
async def test_url_download_cache_reuses_zero_input_artifact_across_jobs(
    cache_db_session,
) -> None:
    service = IntermediateArtifactCacheService()
    output_artifact = artifact(storage_path="artifacts/job-1/download.mp4")
    cache_db_session.add(output_artifact)
    await cache_db_session.commit()
    config = {
        "url": "https://example.test/video?b=2&a=1",
        "format": "1080p",
    }

    await service.store(
        cache_db_session,
        node_type="url_download",
        node_config=config,
        input_artifacts={},
        output_artifact=output_artifact,
        node_id="download-1",
        job_id=uuid.uuid4(),
    )
    await cache_db_session.commit()

    hit = await service.lookup(
        cache_db_session,
        node_type="url_download",
        node_config=config,
        input_artifacts={},
    )

    assert hit is not None
    assert hit.output_artifact_id == output_artifact.id


def test_url_download_cache_key_uses_normalized_external_url() -> None:
    service = IntermediateArtifactCacheService()

    short_url = service.cache_key(
        "url_download",
        {
            "url": "https://youtu.be/abc123?utm_source=ignored",
            "format": "best",
        },
        {},
    )
    watch_url = service.cache_key(
        "url_download",
        {
            "url": "https://www.youtube.com/watch?feature=share&v=abc123",
            "format": "best",
        },
        {},
    )

    assert short_url == watch_url


def test_external_url_normalization_does_not_alias_unrelated_hosts() -> None:
    assert normalize_external_media_url(
        "https://notyoutube.com/watch?v=abc123"
    ) == "https://notyoutube.com/watch?v=abc123"
    assert normalize_external_media_url(
        "https://example.test/video/0123456789abcdef01234567"
    ) == "https://example.test/video/0123456789abcdef01234567"
    assert normalize_external_media_url(
        "https://example.test/path/BV1abc"
    ) == "https://example.test/path/BV1abc"


@pytest.mark.asyncio
async def test_cache_snapshot_survives_source_artifact_deletion(
    cache_db_session,
) -> None:
    service = IntermediateArtifactCacheService()
    source = artifact(storage_path="download-cache/youtube/abc123.mp4")
    cache_db_session.add(source)
    await cache_db_session.flush()
    await service.store(
        cache_db_session,
        node_type="url_download",
        node_config={"url": "https://youtu.be/abc123"},
        input_artifacts={},
        output_artifact=source,
        node_id="download",
        job_id=source.job_id,
    )
    await cache_db_session.delete(source)
    await cache_db_session.flush()

    hit = await service.lookup(
        cache_db_session,
        node_type="url_download",
        node_config={"url": "https://www.youtube.com/watch?v=abc123"},
        input_artifacts={},
    )

    assert hit is not None
    assert hit.storage_backend == "local"
    assert hit.storage_path == "download-cache/youtube/abc123.mp4"
    assert hit.output_artifact_id is None


@pytest.mark.asyncio
async def test_materialized_cache_hit_survives_later_source_deletion(
    cache_db_session,
) -> None:
    service = IntermediateArtifactCacheService()
    source = artifact(storage_path="staging/artifacts/source/output.mp4")
    cache_db_session.add(source)
    await cache_db_session.flush()
    await service.store(
        cache_db_session,
        node_type="url_download",
        node_config={"url": "https://youtu.be/abc123"},
        input_artifacts={},
        output_artifact=source,
        node_id="download-source",
        job_id=source.job_id,
    )
    hit = await service.lookup(
        cache_db_session,
        node_type="url_download",
        node_config={"url": "https://www.youtube.com/watch?v=abc123"},
        input_artifacts={},
    )
    assert hit is not None
    consumer_job_id = uuid.uuid4()
    materialized = await service.materialize_hit(
        cache_db_session,
        hit,
        job_id=consumer_job_id,
        node_execution_id=uuid.uuid4(),
    )
    await cache_db_session.delete(source)
    await cache_db_session.flush()
    hit = await service.lookup(
        cache_db_session,
        node_type="url_download",
        node_config={"url": "https://youtu.be/abc123"},
        input_artifacts={},
    )

    stored = await cache_db_session.get(Artifact, materialized.id)
    assert stored is not None
    assert hit is not None
    assert stored.job_id == consumer_job_id
    assert stored.storage_path == "staging/artifacts/source/output.mp4"
    assert hit.output_artifact_id is None


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
