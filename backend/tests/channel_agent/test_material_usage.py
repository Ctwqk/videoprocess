from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.material_usage import (
    extract_material_references,
    recent_usage_flags,
    segment_signature,
)
from app.models.channel_agent import MaterialUsageLedger


@pytest.fixture
async def usage_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(MaterialUsageLedger.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


def test_extract_material_references_from_nested_payloads():
    refs = extract_material_references(
        plan_payload={
            "candidates": [
                {
                    "material_id": "mat-1",
                    "asset_id": "11111111-1111-1111-1111-111111111111",
                    "start_sec": 1.5,
                    "end_sec": 4.0,
                }
            ]
        },
        run_payload={"artifacts": {"selected": [{"material_id": "mat-2", "start_ms": 0, "end_ms": 1200}]}},
        upload_metadata={"material_refs": [{"material_id": "mat-1", "start_ms": 1500, "end_ms": 4000}]},
    )

    assert [ref.material_id for ref in refs] == ["mat-1", "mat-2"]
    assert refs[0].asset_id == "11111111-1111-1111-1111-111111111111"
    assert refs[0].start_ms == 1500
    assert refs[0].end_ms == 4000
    assert refs[0].segment_signature == segment_signature("mat-1", 1500, 4000)


@pytest.mark.asyncio
async def test_recent_usage_flags_same_lane_same_account_and_sibling_account(usage_session):
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    usage_session.add_all(
        [
            MaterialUsageLedger(
                material_id="mat-1",
                channel_profile_id=uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"),
                topic_lane_id=uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
                publishing_account_id=uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc"),
                segment_signature=segment_signature("mat-1", 0, 1000),
                used_at=now - timedelta(days=1),
            ),
            MaterialUsageLedger(
                material_id="mat-2",
                channel_profile_id=uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"),
                topic_lane_id=uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
                publishing_account_id=uuid.UUID("dddddddd-4444-4444-8444-dddddddddddd"),
                segment_signature=segment_signature("mat-2", 0, 1000),
                used_at=now - timedelta(days=10),
            ),
        ]
    )
    await usage_session.commit()

    refs = extract_material_references(
        plan_payload={},
        run_payload={},
        upload_metadata={
            "material_refs": [
                {"material_id": "mat-1", "start_ms": 0, "end_ms": 1000},
                {"material_id": "mat-2", "start_ms": 0, "end_ms": 1000},
            ]
        },
    )

    result = await recent_usage_flags(
        usage_session,
        channel_id="aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa",
        lane_id="bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb",
        account_id="cccccccc-3333-4333-8333-cccccccccccc",
        references=refs,
        now=now,
    )

    assert result.repetition_rejected is True
    assert result.cross_account_rejected is True
    assert {hit["guard"] for hit in result.hits} == {"repetition_rejected", "cross_account_rejected"}
