from __future__ import annotations

import json
import stat
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.artifact import Artifact, IntermediateArtifactCache
from app.models.job import Job, NodeExecution
from app.services.staging_object_janitor import (
    STAGING_GRACE_SECONDS,
    StagingObjectJanitor,
    staging_janitor_ready,
)


@pytest.fixture
async def janitor_session_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'janitor.sqlite3'}"
    )
    async with engine.begin() as connection:
        for table in (
            Job.__table__,
            NodeExecution.__table__,
            Artifact.__table__,
            IntermediateArtifactCache.__table__,
        ):
            await connection.run_sync(table.create)
    yield async_sessionmaker(engine, expire_on_commit=False)
    await engine.dispose()


@pytest.mark.asyncio
async def test_janitor_deletes_only_old_unreferenced_claim_staging_objects(
    janitor_session_factory,
    tmp_path,
) -> None:
    now = datetime(2026, 7, 26, 12, tzinfo=timezone.utc)
    protected_path = (
        "staging/artifacts/11111111-1111-1111-1111-111111111111/"
        "22222222-2222-2222-2222-222222222222-a1b2c3d4e5f60708.mp4"
    )
    orphan_path = (
        "staging/artifacts/33333333-3333-3333-3333-333333333333/"
        "44444444-4444-4444-4444-444444444444-1020304050607080.mp4"
    )
    cached_path = (
        "staging/artifacts/77777777-7777-7777-7777-777777777777/"
        "88888888-8888-8888-8888-888888888888-aabbccddeeff0011.mp4"
    )
    fresh_path = (
        "staging/artifacts/55555555-5555-5555-5555-555555555555/"
        "66666666-6666-6666-6666-666666666666-1122334455667788.mp4"
    )
    invalid_path = "staging/artifacts/not-claim-owned.tmp"

    async with janitor_session_factory() as db:
        job = Job(
            id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
            pipeline_id=uuid.uuid4(),
            pipeline_snapshot={},
        )
        node = NodeExecution(
            id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
            job_id=job.id,
            node_id="node",
            node_type="vision",
        )
        db.add_all((job, node))
        await db.flush()
        db.add(
            Artifact(
                job_id=job.id,
                node_execution_id=node.id,
                filename="output.mp4",
                storage_backend="minio",
                storage_path=protected_path,
            )
        )
        db.add(
            IntermediateArtifactCache(
                cache_key="cache-owned-staging-object",
                node_type="url_download",
                node_config_hash="config",
                input_signature_hash="inputs",
                storage_backend="minio",
                storage_path=cached_path,
                filename="cached.mp4",
                metadata_json={},
            )
        )
        await db.commit()

    class Client:
        def __init__(self) -> None:
            self.removed: list[str] = []

        def list_objects(self, bucket, prefix, recursive):
            assert (bucket, prefix, recursive) == (
                "videoprocess",
                "staging/artifacts/",
                True,
            )
            old = now - timedelta(seconds=STAGING_GRACE_SECONDS + 1)
            fresh = now - timedelta(seconds=STAGING_GRACE_SECONDS - 1)
            return [
                SimpleNamespace(object_name=protected_path, last_modified=old),
                SimpleNamespace(object_name=cached_path, last_modified=old),
                SimpleNamespace(object_name=orphan_path, last_modified=old),
                SimpleNamespace(object_name=fresh_path, last_modified=fresh),
                SimpleNamespace(object_name=invalid_path, last_modified=old),
            ]

        def remove_object(self, bucket, path):
            assert bucket == "videoprocess"
            self.removed.append(path)

    client = Client()
    status_file = tmp_path / "janitor-status.json"
    result = await StagingObjectJanitor(
        janitor_session_factory,
        client=client,
        bucket="videoprocess",
        status_file=status_file,
    ).run_once(now=now)

    assert client.removed == [orphan_path]
    assert result == {
        "scanned": 5,
        "deleted": 1,
        "protected": 2,
        "too_young": 1,
        "invalid": 1,
        "errors": 0,
    }
    assert staging_janitor_ready(
        status_file,
        now=now,
        max_age_seconds=900,
    )
    assert json.loads(status_file.read_text())["grace_seconds"] == (
        STAGING_GRACE_SECONDS
    )
    assert stat.S_IMODE(status_file.stat().st_mode) == 0o600


def test_staging_grace_exceeds_all_operation_retry_and_clock_skew_windows() -> None:
    assert STAGING_GRACE_SECONDS > 2 * (180 + 120 + 15 + 300)
