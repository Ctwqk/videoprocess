from __future__ import annotations

import asyncio
import os
import uuid

import pytest
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.api.channel_agent import HumanReviewRequest, promote_publication, release_human_review
from app.models.autoflow import AutoFlowPlan
from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
)
from app.services.channelops_quarantine import QUARANTINE_REASON, quarantine_channelops_backlog


POSTGRES_URL = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL", "")


pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(not POSTGRES_URL, reason="set CHANNEL_OPS_POSTGRES_TEST_URL for PostgreSQL race tests"),
]


def _review_plan() -> AutoFlowPlan:
    return AutoFlowPlan(
        prompt="Review this external asset plan",
        request_json={
            "prompt": "Review this external asset plan",
            "target_platforms": ["youtube_shorts"],
            "duration_sec": 30,
            "aspect_ratio": "9:16",
            "source_policy": "remix_with_review",
            "publish_mode": "private_upload",
            "material_library_ids": [],
            "user_constraints": {},
        },
        intent_json={
            "intent_type": "generic_video",
            "subject": "external review",
            "style": "documentary",
            "duration_sec": 30,
            "aspect_ratio": "9:16",
            "target_platforms": ["youtube_shorts"],
            "source_policy": "remix_with_review",
            "publish_mode": "private_upload",
            "keywords": [],
            "negative_keywords": [],
            "needs_voiceover": False,
            "needs_subtitles": True,
            "needs_bgm": False,
            "user_confirmation_questions": [],
        },
        template_id="material_library_remix",
        pipeline_definition={"nodes": [], "edges": [], "viewport": {"x": 0, "y": 0, "zoom": 1}},
        candidates_json=[],
        metadata_json={},
        rights_json={
            "status": "review_required",
            "reasons": ["human review required"],
            "allowed_publish_modes": ["private_upload", "unlisted_upload"],
            "execute_allowed": True,
            "publish_allowed": True,
        },
        validation_json={"valid": True, "errors": [], "warnings": [], "repairs": []},
        status="review_required",
    )


@pytest.fixture
async def postgres_race_db():
    engine = create_async_engine(POSTGRES_URL, pool_size=8, max_overflow=0)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE channel_ops_queue_items, publication_records, production_tasks, "
                "publishing_accounts, autoflow_runs, autoflow_plans RESTART IDENTITY CASCADE"
            )
        )
    try:
        yield engine, factory
    finally:
        await engine.dispose()


async def _wait_until_lock_wait(engine, query_fragment: str, operation: asyncio.Task) -> None:
    deadline = asyncio.get_running_loop().time() + 5
    while asyncio.get_running_loop().time() < deadline:
        if operation.done():
            result = await operation
            pytest.fail(
                f"operation completed before reaching the expected PostgreSQL lock for {query_fragment}: {result!r}"
            )
        async with engine.connect() as conn:
            waiting = await conn.scalar(
                text(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND query ILIKE '%' || :fragment || '%'"
                    ")"
                ),
                {"fragment": query_fragment},
            )
        if waiting:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"operation did not reach the expected PostgreSQL lock for {query_fragment}")


async def _cancel_operations(blocker: AsyncSession, *operations: asyncio.Task) -> None:
    await blocker.rollback()
    for operation in operations:
        if not operation.done():
            operation.cancel()
    await asyncio.gather(*operations, return_exceptions=True)


async def _seed_review_release(factory) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with factory() as db:
        channel = ChannelProfile(name="release race", enabled=True, dry_run=False)
        db.add(channel)
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="release",
            credential_ref="youtube/release",
            default_privacy="unlisted",
        )
        plan = _review_plan()
        db.add_all([account, plan])
        await db.flush()
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            source="trend_youtube",
            prompt="review release race",
            uses_external_assets=True,
            approval_mode="human",
            autoflow_plan_id=plan.id,
            state="held",
            blocked_by_guard="human_approval_required",
            channel_config_snapshot_json={},
        )
        db.add(task)
        await db.commit()
        return channel.id, task.id, plan.id


async def _seed_promotion(factory) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with factory() as db:
        channel = ChannelProfile(name="promotion race", enabled=True, dry_run=False)
        db.add(channel)
        await db.flush()
        account = PublishingAccount(
            channel_profile_id=channel.id,
            account_label="promotion",
            credential_ref="youtube/promotion",
            default_privacy="unlisted",
        )
        db.add(account)
        await db.flush()
        task = ProductionTask(
            channel_profile_id=channel.id,
            target_account_id=account.id,
            prompt="promotion race",
            state="uploaded_private",
            channel_config_snapshot_json={},
        )
        db.add(task)
        await db.flush()
        publication = PublicationRecord(
            production_task_id=task.id,
            account_id=account.id,
            platform_content_id=f"race-{uuid.uuid4()}",
            title="promotion race",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="uploaded",
            compliance_disposition="owned",
        )
        db.add(publication)
        await db.commit()
        return channel.id, task.id, publication.id


async def _assert_quarantined(factory, task_id: uuid.UUID, *, queue_kind: str) -> None:
    async with factory() as db:
        task = await db.get(ProductionTask, task_id)
        assert task is not None
        assert task.state == "held"
        assert task.blocked_by_guard == QUARANTINE_REASON
        rows = (
            await db.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == queue_kind))
        ).scalars().all()
        assert all(row.status == "dead_lettered" for row in rows)


async def test_review_release_first_commits_atomically_then_quarantine_holds_it(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, plan_id = await _seed_review_release(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(select(AutoFlowPlan).where(AutoFlowPlan.id == plan_id).with_for_update())
        operator = asyncio.create_task(
            release_human_review(
                str(task_id),
                HumanReviewRequest(human_actor="reviewer"),
                db=operator_db,
            )
        )
        quarantine = None
        try:
            await _wait_until_lock_wait(engine, "autoflow_plans", operator)
            quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
            await _wait_until_lock_wait(engine, "channel_profiles", quarantine)
            await blocker.commit()
            result = await operator
            await quarantine
        finally:
            await _cancel_operations(blocker, operator, *(operation for operation in [quarantine] if operation))

    assert result["state"] == "planning"
    await _assert_quarantined(factory, task_id, queue_kind="execute_task")


async def test_quarantine_first_makes_review_release_conflict_without_enqueue(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, _plan_id = await _seed_review_release(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(select(ProductionTask).where(ProductionTask.id == task_id).with_for_update())
        quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
        operator = None
        try:
            await _wait_until_lock_wait(engine, "production_tasks", quarantine)
            operator = asyncio.create_task(
                release_human_review(
                    str(task_id),
                    HumanReviewRequest(human_actor="reviewer"),
                    db=operator_db,
                )
            )
            await _wait_until_lock_wait(engine, "channel_profiles", operator)
            await blocker.commit()
            await quarantine
            with pytest.raises(HTTPException) as exc_info:
                await operator
        finally:
            await _cancel_operations(blocker, quarantine, *(operation for operation in [operator] if operation))

    assert exc_info.value.status_code == 409
    async with factory() as db:
        task = await db.get(ProductionTask, task_id)
        assert task is not None
        assert task.human_review_evidence_json == {}
        assert await db.scalar(select(ChannelOpsQueueItem.id).where(ChannelOpsQueueItem.kind == "execute_task")) is None


async def test_manual_promotion_first_commits_then_quarantine_dead_letters_it(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, publication_id = await _seed_promotion(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(
            select(PublicationRecord).where(PublicationRecord.id == publication_id).with_for_update()
        )
        operator = asyncio.create_task(
            promote_publication(
                str(publication_id),
                HumanReviewRequest(human_actor="reviewer"),
                db=operator_db,
            )
        )
        quarantine = None
        try:
            await _wait_until_lock_wait(engine, "publication_records", operator)
            quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
            await _wait_until_lock_wait(engine, "channel_profiles", quarantine)
            await blocker.commit()
            result = await operator
            await quarantine
        finally:
            await _cancel_operations(blocker, operator, *(operation for operation in [quarantine] if operation))

    assert result.kind == "promote_publication"
    await _assert_quarantined(factory, task_id, queue_kind="promote_publication")


async def test_quarantine_first_makes_manual_promotion_conflict_without_evidence(postgres_race_db):
    engine, factory = postgres_race_db
    channel_id, task_id, publication_id = await _seed_promotion(factory)
    async with factory() as blocker, factory() as operator_db, factory() as quarantine_db:
        await blocker.execute(select(ProductionTask).where(ProductionTask.id == task_id).with_for_update())
        quarantine = asyncio.create_task(quarantine_channelops_backlog(quarantine_db, channel_id, apply=True))
        operator = None
        try:
            await _wait_until_lock_wait(engine, "production_tasks", quarantine)
            operator = asyncio.create_task(
                promote_publication(
                    str(publication_id),
                    HumanReviewRequest(human_actor="reviewer"),
                    db=operator_db,
                )
            )
            await _wait_until_lock_wait(engine, "channel_profiles", operator)
            await blocker.commit()
            await quarantine
            with pytest.raises(HTTPException) as exc_info:
                await operator
        finally:
            await _cancel_operations(blocker, quarantine, *(operation for operation in [operator] if operation))

    assert exc_info.value.status_code == 409
    async with factory() as db:
        task = await db.get(ProductionTask, task_id)
        assert task is not None
        assert task.human_review_evidence_json == {}
        assert (
            await db.scalar(select(ChannelOpsQueueItem.id).where(ChannelOpsQueueItem.kind == "promote_publication"))
            is None
        )
