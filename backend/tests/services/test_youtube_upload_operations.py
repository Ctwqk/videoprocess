from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import CheckConstraint
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.artifact import Artifact
from app.models.channel_agent import ChannelProfile, ProductionTask, PublicationRecord
from app.models.job import Job, NodeExecution
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.services.youtube_upload_operations import (
    UploadOperationConflictError,
    UploadOperationContext,
    YouTubeUploadOperationStore,
)


MANAGER_TASK_ID = "a0b1c2d3-e4f5-4678-9abc-def012345678"
SECOND_MANAGER_TASK_ID = "12345678-90ab-4cde-8f01-23456789abcd"
INVALID_MANAGER_TASK_ID_CASES = (
    pytest.param(None, id="null"),
    pytest.param("", id="empty"),
    pytest.param("   ", id="spaces"),
    pytest.param("\t", id="tab"),
    pytest.param("\n", id="newline"),
    pytest.param("\t\n", id="control-whitespace"),
    pytest.param("manager-task-1", id="legacy-placeholder"),
    pytest.param("a0b1c2d3e4f546789abcdef012345678", id="compact"),
    pytest.param("{a0b1c2d3-e4f5-4678-9abc-def012345678}", id="braced"),
    pytest.param("A0B1C2D3-E4F5-4678-9ABC-DEF012345678", id="uppercase"),
    pytest.param("g0b1c2d3-e4f5-4678-9abc-def012345678", id="non-hex"),
    pytest.param("a0b1c2d3-e4f5-4678-9abc-def01234567-", id="extra-hyphen"),
    pytest.param(f"{MANAGER_TASK_ID}\n", id="valid-plus-newline"),
)


@pytest.fixture
async def operation_session_factory(tmp_path):
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'upload-operations.sqlite3'}",
        connect_args={"timeout": 10},
    )
    async with engine.begin() as conn:
        for table in (
            Job.__table__,
            NodeExecution.__table__,
            Artifact.__table__,
            ChannelProfile.__table__,
            ProductionTask.__table__,
            YouTubeUploadOperation.__table__,
        ):
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    yield session_factory
    await engine.dispose()


async def _context_for(
    db: AsyncSession,
    *,
    production_task: ProductionTask | None = None,
) -> UploadOperationContext:
    job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={})
    db.add(job)
    await db.flush()

    node = NodeExecution(
        job_id=job.id,
        node_id=f"youtube_upload_{uuid.uuid4().hex}",
        node_type="youtube_upload",
    )
    db.add(node)
    await db.flush()

    artifact = Artifact(
        job_id=job.id,
        node_execution_id=node.id,
        filename="canary.mp4",
        storage_path="artifacts/canary.mp4",
    )
    db.add(artifact)
    await db.flush()

    if production_task is None:
        channel = ChannelProfile(name=f"canary-{uuid.uuid4()}")
        db.add(channel)
        await db.flush()
        production_task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=uuid.uuid4(),
            prompt="Upload the owned canary video",
            job_id=job.id,
        )
        db.add(production_task)
    else:
        production_task.job_id = job.id
    await db.commit()

    return UploadOperationContext(
        job_id=job.id,
        node_execution_id=node.id,
        input_artifact_id=artifact.id,
        content_sha256="a" * 64,
        title="Owned canary",
        privacy="unlisted",
    )


@pytest.mark.asyncio
async def test_claim_reserves_once_then_resumes_and_replays(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)

    claim = await store.claim(context)
    assert claim.action == "submit"
    assert claim.operation.status == "reserved"

    again = await store.claim(context)
    assert again.action == "block"
    assert again.operation.id == claim.operation.id

    submitted = await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)
    assert submitted.manager_task_id == MANAGER_TASK_ID
    assert (await store.claim(context)).action == "resume"

    receipt = {
        "video_id": "abcdefghijk",
        "url": "https://youtu.be/abcdefghijk",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["canary"],
        "quota_estimate": 1600,
        "access_token": "must-not-persist",
    }
    succeeded = await store.mark_succeeded(claim.operation.id, "abcdefghijk", receipt)
    assert succeeded.receipt_json == {
        "video_id": "abcdefghijk",
        "url": "https://youtu.be/abcdefghijk",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["canary"],
        "quota_estimate": 1600,
    }
    assert (await store.claim(context)).action == "replay"


@pytest.mark.asyncio
async def test_existing_reserved_uncertain_and_failed_operations_never_submit(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        reserved_context = await _context_for(db)

    reserved = await store.claim(reserved_context)
    assert (await store.claim(reserved_context)).action == "block"

    await store.mark_uncertain(reserved.operation.id, "upload response was ambiguous")
    assert (await store.claim(reserved_context)).action == "block"

    async with operation_session_factory() as db:
        failed_context = await _context_for(db)
    failed = await store.claim(failed_context)
    await store.mark_failed(failed.operation.id, "manager rejected upload")
    assert (await store.claim(failed_context)).action == "block"


@pytest.mark.asyncio
async def test_claim_rejects_second_node_for_the_same_production_task(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        production_task = await db.get(ProductionTask, claim.operation.production_task_id)
        assert production_task is not None
        second_context = await _context_for(db, production_task=production_task)

    with pytest.raises(
        UploadOperationConflictError,
        match="production task already has a YouTube upload operation",
    ):
        await store.claim(second_context)


@pytest.mark.asyncio
async def test_mark_succeeded_rejects_duplicate_platform_video_id(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        first_context = await _context_for(db)
    async with operation_session_factory() as db:
        second_context = await _context_for(db)
    first = await store.claim(first_context)
    second = await store.claim(second_context)

    await store.mark_submitted(first.operation.id, MANAGER_TASK_ID)
    await store.mark_succeeded(first.operation.id, "abcdefghijk", {"video_id": "abcdefghijk"})
    await store.mark_submitted(second.operation.id, SECOND_MANAGER_TASK_ID)

    with pytest.raises(
        UploadOperationConflictError,
        match="platform video id already belongs to a YouTube upload operation",
    ):
        await store.mark_succeeded(second.operation.id, "abcdefghijk", {"video_id": "abcdefghijk"})


@pytest.mark.asyncio
async def test_competing_successes_cannot_replace_the_winning_receipt(
    operation_session_factory,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)
    await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)

    start_barrier = asyncio.Barrier(2)

    async def transition(platform_video_id: str, title: str):
        await start_barrier.wait()
        return await store.mark_succeeded(
            claim.operation.id,
            platform_video_id,
            {"video_id": platform_video_id, "title": title},
        )

    results = await asyncio.gather(
        transition("video-win-a", "receipt-a"),
        transition("video-win-b", "receipt-b"),
        return_exceptions=True,
    )

    successes = [result for result in results if isinstance(result, YouTubeUploadOperation)]
    conflicts = [result for result in results if isinstance(result, UploadOperationConflictError)]
    assert len(successes) == 1
    assert len(conflicts) == 1
    async with operation_session_factory() as db:
        stored = await db.get(YouTubeUploadOperation, claim.operation.id)
    assert stored is not None
    assert stored.platform_video_id == successes[0].platform_video_id
    assert stored.receipt_json == successes[0].receipt_json


@pytest.mark.asyncio
async def test_terminal_transition_does_not_replace_an_existing_failure(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    failed = await store.mark_failed(claim.operation.id, "first conclusive failure")
    repeated = await store.mark_failed(claim.operation.id, "later failure must not replace evidence")
    assert repeated.status == "failed"
    assert repeated.error_message == failed.error_message == "first conclusive failure"
    with pytest.raises(ValueError, match="cannot mark failed operation uncertain"):
        await store.mark_uncertain(claim.operation.id, "ambiguous after failure")


def test_operation_model_requires_manager_task_for_submitted_and_succeeded_states():
    checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in YouTubeUploadOperation.__table__.constraints
        if isinstance(constraint, CheckConstraint)
    }
    manager_check = checks["ck_youtube_upload_operations_manager_task"]
    assert manager_check.startswith("(manager_task_id IS NULL OR (")
    assert (
        ")) AND (status NOT IN ('submitted', 'succeeded') OR manager_task_id IS NOT NULL)"
        in manager_check
    )
    assert "length(manager_task_id) = 36" in manager_check
    assert "manager_task_id = lower(manager_task_id)" in manager_check
    assert "length(replace(manager_task_id, '-', '')) = 32" in manager_check
    for position in (9, 14, 19, 24):
        assert f"substr(manager_task_id, {position}, 1) = '-'" in manager_check
    for character in "0123456789abcdef":
        assert f", '{character}', '')" in manager_check


@pytest.mark.parametrize(
    "manager_task_id",
    INVALID_MANAGER_TASK_ID_CASES,
)
@pytest.mark.asyncio
async def test_database_rejects_submitted_operation_without_canonical_manager_uuid(
    operation_session_factory,
    manager_task_id,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation is not None
        operation.status = "submitted"
        operation.manager_task_id = manager_task_id
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()


@pytest.mark.parametrize(
    "manager_task_id",
    [
        pytest.param("\t", id="tab"),
        pytest.param("\n", id="newline"),
        pytest.param("manager-task-1", id="malformed"),
    ],
)
@pytest.mark.asyncio
async def test_database_rejects_noncanonical_manager_uuid_before_submission(
    operation_session_factory,
    manager_task_id,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation is not None
        operation.manager_task_id = manager_task_id
        with pytest.raises(IntegrityError):
            await db.commit()
        await db.rollback()


@pytest.mark.asyncio
async def test_database_accepts_submitted_operation_with_canonical_manager_uuid(
    operation_session_factory,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    async with operation_session_factory() as db:
        operation = await db.get(YouTubeUploadOperation, claim.operation.id)
        assert operation is not None
        operation.status = "submitted"
        operation.manager_task_id = MANAGER_TASK_ID
        await db.commit()

    async with operation_session_factory() as db:
        stored = await db.get(YouTubeUploadOperation, claim.operation.id)
    assert stored is not None
    assert stored.manager_task_id == MANAGER_TASK_ID


@pytest.mark.parametrize("manager_task_id", INVALID_MANAGER_TASK_ID_CASES)
@pytest.mark.asyncio
async def test_mark_submitted_rejects_noncanonical_manager_task(
    operation_session_factory,
    manager_task_id,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)

    with pytest.raises(ValueError, match="manager task id"):
        await store.mark_submitted(claim.operation.id, manager_task_id)


@pytest.mark.parametrize("status", ["submitted", "succeeded"])
@pytest.mark.parametrize("manager_task_id", INVALID_MANAGER_TASK_ID_CASES)
def test_noncanonical_manager_durable_state_fails_closed(status, manager_task_id):
    operation = YouTubeUploadOperation(status=status, manager_task_id=manager_task_id)
    assert YouTubeUploadOperationStore._action_for(operation) == "block"


@pytest.mark.parametrize(
    ("quota_estimate", "expected"),
    [
        pytest.param({"units": 1600}, None, id="nested-object"),
        pytest.param([1600], None, id="nested-array"),
        pytest.param(float("nan"), None, id="nan"),
        pytest.param(float("inf"), None, id="infinity"),
        pytest.param(True, None, id="boolean"),
        pytest.param("1600", 1600.0, id="numeric-string"),
        pytest.param(1600, 1600, id="integer"),
        pytest.param(1600.5, 1600.5, id="float"),
    ],
)
@pytest.mark.asyncio
async def test_receipt_quota_estimate_is_a_finite_numeric_scalar_or_none(
    operation_session_factory,
    quota_estimate,
    expected,
):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)
    await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)

    succeeded = await store.mark_succeeded(
        claim.operation.id,
        "abcdefghijk",
        {"video_id": "abcdefghijk", "quota_estimate": quota_estimate},
    )
    assert succeeded.receipt_json["quota_estimate"] == expected


@pytest.mark.asyncio
async def test_receipt_never_stringifies_nested_or_non_string_values(operation_session_factory):
    store = YouTubeUploadOperationStore(operation_session_factory)
    async with operation_session_factory() as db:
        context = await _context_for(db)
    claim = await store.claim(context)
    await store.mark_submitted(claim.operation.id, MANAGER_TASK_ID)

    secrets = {
        "RECEIPT_VIDEO_TOKEN",
        "RECEIPT_URL_TOKEN",
        "RECEIPT_VIDEO_URL_TOKEN",
        "RECEIPT_TITLE_TOKEN",
        "RECEIPT_PRIVACY_TOKEN",
        "RECEIPT_TAG_DICT_TOKEN",
        "RECEIPT_TAG_LIST_TOKEN",
        "RECEIPT_QUOTA_TOKEN",
        "RECEIPT_IGNORED_TOKEN",
    }
    succeeded = await store.mark_succeeded(
        claim.operation.id,
        "safe-video-id",
        {
            "video_id": {"access_token": "RECEIPT_VIDEO_TOKEN"},
            "url": {"access_token": "RECEIPT_URL_TOKEN"},
            "video_url": [{"refresh_token": "RECEIPT_VIDEO_URL_TOKEN"}],
            "title": {"token": "RECEIPT_TITLE_TOKEN"},
            "privacy": {"token": "RECEIPT_PRIVACY_TOKEN"},
            "tags": [
                "safe-tag",
                {"access_token": "RECEIPT_TAG_DICT_TOKEN"},
                ["RECEIPT_TAG_LIST_TOKEN"],
                True,
                1600,
            ],
            "quota_estimate": {"access_token": "RECEIPT_QUOTA_TOKEN"},
            "ignored": {"access_token": "RECEIPT_IGNORED_TOKEN"},
        },
    )

    assert succeeded.receipt_json == {
        "video_id": "safe-video-id",
        "url": "",
        "title": "Owned canary",
        "privacy": "unlisted",
        "tags": ["safe-tag"],
        "quota_estimate": None,
    }
    serialized = json.dumps(succeeded.receipt_json, sort_keys=True)
    assert all(secret not in serialized for secret in secrets)


def test_publication_record_orm_has_migration_unique_indexes():
    indexes = {index.name: index for index in PublicationRecord.__table__.indexes}

    production_task = indexes["ux_publication_records_production_task"]
    assert production_task.unique is True
    assert [column.name for column in production_task.columns] == ["production_task_id"]

    platform_content = indexes["ux_publication_records_platform_content"]
    assert platform_content.unique is True
    assert [column.name for column in platform_content.columns] == ["platform", "platform_content_id"]


def test_alembic_upgrade_head_renders_offline_postgresql_sql():
    backend_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["DATABASE_URL"] = "postgresql+asyncpg://offline:offline@127.0.0.1:5432/offline"
    completed = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head", "--sql"],
        cwd=backend_root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "DO $$" in completed.stdout
    assert "RAISE EXCEPTION 'cannot add ux_publication_records_production_task" in completed.stdout
    assert "RAISE EXCEPTION 'cannot add ux_publication_records_platform_content" in completed.stdout

    widening = "ALTER TABLE alembic_version ALTER COLUMN version_num TYPE VARCHAR(128)"
    revision_update = (
        "UPDATE alembic_version SET version_num='020_channelops_decision_audit_failure_category'"
    )
    assert widening in completed.stdout
    assert completed.stdout.index(widening) < completed.stdout.index(revision_update)
    assert "CONSTRAINT ck_youtube_upload_operations_manager_task CHECK" in completed.stdout
    assert "(manager_task_id IS NULL OR (" in completed.stdout
    assert (
        ")) AND (status NOT IN ('submitted', 'succeeded') OR manager_task_id IS NOT NULL)"
        in completed.stdout
    )
    assert "length(manager_task_id) = 36" in completed.stdout
    assert "manager_task_id = lower(manager_task_id)" in completed.stdout
    assert "length(replace(manager_task_id, '-', '')) = 32" in completed.stdout
    for position in (9, 14, 19, 24):
        assert f"substr(manager_task_id, {position}, 1) = '-'" in completed.stdout
    for character in "0123456789abcdef":
        assert f", '{character}', '')" in completed.stdout
