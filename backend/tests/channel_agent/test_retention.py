from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.retention import cleanup_expired
from app.models.channel_agent import (
    AgentTickAudit, CandidateFeatureSnapshot, ChannelOpsQueueItem, FeedbackSnapshot,
    PublicationMetricSchedule, PublicationRecord,
)
from app.models.youtube_upload_operation import YouTubeUploadOperation


@pytest.fixture
async def retention_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(ChannelOpsQueueItem.__table__.create)
        await conn.run_sync(AgentTickAudit.__table__.create)
        await conn.run_sync(CandidateFeatureSnapshot.__table__.create)
        await conn.run_sync(FeedbackSnapshot.__table__.create)
        await conn.run_sync(PublicationRecord.__table__.create)
        await conn.run_sync(PublicationMetricSchedule.__table__.create)
        await conn.run_sync(YouTubeUploadOperation.__table__.create)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    await engine.dispose()


@pytest.mark.asyncio
async def test_cleanup_expired_deletes_old_terminal_rows_and_preserves_recent(retention_session):
    now = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    old_queue = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key="old",
        status="succeeded",
        run_after=now - timedelta(days=40),
        created_at=(now - timedelta(days=40)).replace(tzinfo=None),
    )
    recent_queue = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key="recent",
        status="succeeded",
        run_after=now - timedelta(days=1),
        created_at=(now - timedelta(days=1)).replace(tzinfo=None),
    )
    running_old_queue = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key="running-old",
        status="running",
        run_after=now - timedelta(days=40),
        created_at=(now - timedelta(days=40)).replace(tzinfo=None),
    )
    old_audit = AgentTickAudit(
        channel_profile_id=uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"),
        tick_id="old",
        started_at=now - timedelta(days=100),
    )
    recent_audit = AgentTickAudit(
        channel_profile_id=uuid.UUID("aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa"),
        tick_id="recent",
        started_at=now - timedelta(days=10),
    )
    protected_audits = [
        AgentTickAudit(
            channel_profile_id=old_audit.channel_profile_id,
            tick_id=status,
            started_at=now - timedelta(days=100),
            replay_status=status,
        )
        for status in ("snapshot_complete", "snapshot_pending")
    ]
    old_feedback = FeedbackSnapshot(
        publication_id=uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
        snapshot_stage="24h",
        collected_at=now - timedelta(days=400),
    )
    recent_feedback = FeedbackSnapshot(
        publication_id=uuid.UUID("bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb"),
        snapshot_stage="7d",
        collected_at=now - timedelta(days=10),
    )
    retention_session.add_all(
        [old_queue, recent_queue, running_old_queue, old_audit, recent_audit, old_feedback, recent_feedback,
         *protected_audits]
    )
    await retention_session.commit()
    snapshot = CandidateFeatureSnapshot(
        tick_audit_id=protected_audits[0].id,
        candidate_id="retained",
        candidate_source="manual_seed",
        source_kind="manual_seed",
        policy_version_id=uuid.UUID("cccccccc-3333-4333-8333-cccccccccccc"),
        feature_schema_version="channelops-candidate-v1",
        feature_as_of=protected_audits[0].started_at,
        raw_features_json={"prompt": "original"},
        missing_feature_mask_json={},
        source_record_refs_json={},
        candidate_set_hash="a" * 64,
        feature_hash="b" * 64,
    )
    retention_session.add(snapshot)
    await retention_session.commit()

    result = await cleanup_expired(
        retention_session,
        now=now,
        queue_retention_days=30,
        audit_retention_days=90,
        feedback_retention_days=365,
    )

    queue_count = await retention_session.scalar(select(func.count()).select_from(ChannelOpsQueueItem))
    audit_count = await retention_session.scalar(select(func.count()).select_from(AgentTickAudit))
    feedback_count = await retention_session.scalar(select(func.count()).select_from(FeedbackSnapshot))
    assert result.deleted_queue_items == 1
    assert result.deleted_audits == 1
    assert result.deleted_feedback == 1
    assert queue_count == 2
    assert audit_count == 3
    assert set((await retention_session.scalars(select(AgentTickAudit.tick_id))).all()) == {
        "recent", "snapshot_complete", "snapshot_pending",
    }
    assert feedback_count == 1
    assert await retention_session.scalar(select(func.count()).select_from(CandidateFeatureSnapshot)) == 1
    assert (await retention_session.get(CandidateFeatureSnapshot, snapshot.id)).raw_features_json == {
        "prompt": "original",
    }


@pytest.mark.parametrize("anchor", ["youtube", "YouTube", "reserved", "submitted", "succeeded", "failed", "unknown"])
async def test_cleanup_preserves_publication_evidence_component(retention_session, anchor):
    now = datetime(2026, 10, 18, tzinfo=timezone.utc)
    old = now - timedelta(days=400)
    task_id, pub_id, metric_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    publication = PublicationRecord(
        id=pub_id, production_task_id=task_id,
        platform=anchor if anchor in {"youtube", "YouTube"} else "other",
        account_id=uuid.uuid4(), compliance_disposition="owned_generated",
    )
    metric = PublicationMetricSchedule(
        id=metric_id, publication_id=pub_id, snapshot_stage="24h",
        effective_start_at=old, due_at=old, grace_until=old,
    )
    retention_session.add_all([publication, metric])
    if anchor not in {"youtube", "YouTube"}:
        retention_session.add(YouTubeUploadOperation(
            production_task_id=task_id, job_id=uuid.uuid4(), node_execution_id=uuid.uuid4(),
            input_artifact_id=uuid.uuid4(), content_sha256="a" * 64, title="fixture",
            privacy="unlisted", status=anchor, manager_task_id=str(uuid.uuid4()),
        ))

    def queue(name, payload, *, parent=None, status="succeeded"):
        return ChannelOpsQueueItem(
            id=uuid.uuid4(), kind=name, idempotency_key=name, payload_json=payload,
            parent_queue_item_id=parent, status=status, run_after=old,
            created_at=old.replace(tzinfo=None), updated_at=old.replace(tzinfo=None),
        )

    ancestor = queue("ancestor", {})
    publish = queue("publish_task", {"production_task_id": str(task_id)}, parent=ancestor.id)
    automatic = queue("automatic", {"publication_id": str(pub_id)}, parent=publish.id, status="cancelled")
    manual = queue("manual", {"publication_id": str(pub_id)})
    reconcile = queue("reconcile_publication", {}, parent=manual.id)
    metric_queue = queue("collect_metrics", {"metric_schedule_id": str(metric_id)}, parent=manual.id)
    retry = queue("retry", {"publication_id": "drifted"}, parent=metric_queue.id, status="dead_lettered")
    cycle = queue("cycle", [], parent=retry.id)
    # A cycle reachable from a metric anchor must terminate without dropping rows.
    manual.parent_queue_item_id = cycle.id
    sibling = queue("sibling", None, parent=ancestor.id)
    protected = [ancestor, publish, automatic, manual, reconcile, metric_queue, retry, cycle, sibling]
    unrelated = queue("send_alert", {"production_task_id": "not-a-uuid"})
    other_pub_id = uuid.uuid4()
    other_publication = PublicationRecord(
        id=other_pub_id, production_task_id=uuid.uuid4(), platform="vimeo",
        account_id=uuid.uuid4(), compliance_disposition="owned_generated",
    )
    unrelated_publication = queue("other_publication", {"publication_id": str(other_pub_id)})
    protected_feedback = FeedbackSnapshot(publication_id=pub_id, collected_at=old)
    unrelated_feedback = FeedbackSnapshot(publication_id=other_pub_id, collected_at=old)
    retention_session.add_all([
        *protected, unrelated, other_publication, unrelated_publication,
        protected_feedback, unrelated_feedback,
    ])
    await retention_session.commit()

    result = await cleanup_expired(
        retention_session, now=now, queue_retention_days=30,
        audit_retention_days=90, feedback_retention_days=365,
    )

    remaining = set((await retention_session.scalars(select(ChannelOpsQueueItem.id))).all())
    assert remaining == {row.id for row in protected}
    assert result.deleted_queue_items == 2
    assert result.deleted_feedback == 1
    assert set((await retention_session.scalars(select(FeedbackSnapshot.id))).all()) == {protected_feedback.id}


async def test_cleanup_preserves_upload_only_task_chain(retention_session):
    now = datetime(2026, 10, 18, tzinfo=timezone.utc)
    old = now - timedelta(days=40)
    task_id = uuid.uuid4()
    retention_session.add(YouTubeUploadOperation(
        production_task_id=task_id, job_id=uuid.uuid4(), node_execution_id=uuid.uuid4(),
        input_artifact_id=uuid.uuid4(), content_sha256="a" * 64, title="fixture",
        privacy="unlisted", status="reserved",
    ))
    row = ChannelOpsQueueItem(
        kind="publish_task", idempotency_key="upload-only", payload_json={"production_task_id": str(task_id)},
        status="succeeded", run_after=old,
        created_at=old.replace(tzinfo=None), updated_at=old.replace(tzinfo=None),
    )
    retention_session.add(row)
    await retention_session.commit()
    result = await cleanup_expired(
        retention_session, now=now, queue_retention_days=30,
        audit_retention_days=90, feedback_retention_days=365,
    )
    assert result.deleted_queue_items == 0
    assert (await retention_session.scalars(select(ChannelOpsQueueItem.id))).all() == [row.id]
