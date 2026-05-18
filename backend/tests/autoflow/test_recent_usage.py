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
