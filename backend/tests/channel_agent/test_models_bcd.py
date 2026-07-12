from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from app.models.channel_agent import (
    AgentTickAudit,
    ChannelProfile,
    DecisionAuditEntry,
    DiscoverySignal,
    FeedbackSnapshot,
    LearningState,
    ManualSeed,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)
from app.schemas.channel_agent import LaneFormatCreate, PublishingAccountCreate


MIGRATION_021 = (
    Path(__file__).resolve().parents[2] / "alembic/versions/021_channelops_discovery_signals.py"
)


def test_new_publishing_defaults_are_private() -> None:
    assert PublishingAccountCreate(account_label="canary").default_privacy == "private"
    assert LaneFormatCreate().default_publish_visibility == "private"


def test_explicit_legacy_public_values_remain_accepted() -> None:
    assert PublishingAccountCreate(
        account_label="legacy",
        default_privacy="public",
    ).default_privacy == "public"
    assert LaneFormatCreate(default_publish_visibility="public").default_publish_visibility == "public"


async def _create_tables(*tables):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in tables:
            await conn.run_sync(table.create)
    await engine.dispose()


@pytest.mark.asyncio
async def test_bcd_models_create_in_sqlite():
    await _create_tables(
        ChannelProfile.__table__,
        TopicLane.__table__,
        PublishingAccount.__table__,
        AgentTickAudit.__table__,
        ManualSeed.__table__,
        ProductionTask.__table__,
        PublicationRecord.__table__,
        FeedbackSnapshot.__table__,
        DecisionAuditEntry.__table__,
        DiscoverySignal.__table__,
        LearningState.__table__,
    )


def test_trend_seed_migration_preserves_ingester_metadata():
    source = MIGRATION_021.read_text()

    assert "constraints_json->>'source_video_id'" in source
    assert "constraints_json->>'expires_at'" in source
    assert "expires_at = COALESCE(EXCLUDED.expires_at, discovery_signals.expires_at)" in source
    assert "'view_count'" in source
    assert "'raw_constraints'" in source


def test_trend_seed_migration_downgrade_reactivates_legacy_seed_ids():
    source = MIGRATION_021.read_text()

    assert "raw_json->>'legacy_manual_seed_id'" in source
    assert "SET status = 'active'" in source
