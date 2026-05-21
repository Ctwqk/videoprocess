from __future__ import annotations

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
