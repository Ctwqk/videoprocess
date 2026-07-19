from __future__ import annotations

import json
import uuid
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models.channel_agent import (
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    LaneFormatMatrix,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TopicLane,
)
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.services.channelops_soak_guard import (
    ALLOWED_EXTERNAL_CONDITIONS,
    SoakGuardAssessment,
    SoakGuardPolicy,
    assess_channelops_soak,
)


NOW = datetime(2026, 7, 19, 18, 0, tzinfo=timezone.utc)
STARTED_AT = NOW - timedelta(hours=72)
ROW_CREATED_AT = STARTED_AT + timedelta(minutes=5)
TABLES = (
    ChannelProfile.__table__,
    TopicLane.__table__,
    PublishingAccount.__table__,
    LaneFormatMatrix.__table__,
    ChannelOpsQueueItem.__table__,
    ProductionTask.__table__,
    PublicationRecord.__table__,
    FeedbackSnapshot.__table__,
    YouTubeUploadOperation.__table__,
)


@pytest.fixture
async def soak_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        for table in TABLES:
            await connection.run_sync(table.create)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


def _policy(channel_id: uuid.UUID, **overrides) -> SoakGuardPolicy:
    values = {"channel_id": channel_id, "started_at": STARTED_AT, **overrides}
    return SoakGuardPolicy(**values)


def _task(channel_id: uuid.UUID, account_id: uuid.UUID, **overrides) -> ProductionTask:
    values = {
        "channel_profile_id": channel_id,
        "target_account_id": account_id,
        "prompt": "sensitive prompt must never enter metrics",
        "state": "measured",
        "state_updated_at": ROW_CREATED_AT,
        "created_at": ROW_CREATED_AT,
        "updated_at": ROW_CREATED_AT,
    }
    values.update(overrides)
    return ProductionTask(**values)


def _publication(task: ProductionTask, **overrides) -> PublicationRecord:
    values = {
        "production_task_id": task.id,
        "account_id": task.target_account_id,
        "platform_content_id": f"video-{uuid.uuid4()}",
        "permalink": "https://secret.example/video",
        "title": "sensitive publication title",
        "desired_privacy": "unlisted",
        "current_privacy": "unlisted",
        "publish_status": "published",
        "compliance_disposition": "owned",
        "uploaded_at": NOW - timedelta(hours=2),
        "created_at": ROW_CREATED_AT,
        "updated_at": ROW_CREATED_AT,
    }
    values.update(overrides)
    return PublicationRecord(**values)


def _operation(task: ProductionTask, **overrides) -> YouTubeUploadOperation:
    values = {
        "production_task_id": task.id,
        "job_id": uuid.uuid4(),
        "node_execution_id": uuid.uuid4(),
        "input_artifact_id": uuid.uuid4(),
        "content_sha256": "a" * 64,
        "title": "sensitive upload title",
        "privacy": "unlisted",
        "status": "succeeded",
        "manager_task_id": str(uuid.uuid4()),
        "platform_video_id": f"video-{uuid.uuid4()}",
        "error_message": "postgresql://user:secret@database/internal",
        "created_at": ROW_CREATED_AT,
        "updated_at": ROW_CREATED_AT,
        "completed_at": NOW - timedelta(hours=2),
    }
    values.update(overrides)
    return YouTubeUploadOperation(**values)


async def _seed_graph(session):
    channel = ChannelProfile(
        name="soak channel",
        enabled=True,
        dry_run=False,
        created_at=ROW_CREATED_AT,
        updated_at=ROW_CREATED_AT,
    )
    session.add(channel)
    await session.flush()

    account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="unlisted account",
        credential_ref="environment-secret-ref",
        default_privacy="unlisted",
        external_asset_auto_publish=False,
        enabled=True,
        created_at=ROW_CREATED_AT,
        updated_at=ROW_CREATED_AT,
    )
    lane = TopicLane(
        channel_profile_id=channel.id,
        name="soak lane",
        enabled=True,
        created_at=ROW_CREATED_AT,
        updated_at=ROW_CREATED_AT,
    )
    session.add_all([account, lane])
    await session.flush()

    lane_format = LaneFormatMatrix(
        topic_lane_id=lane.id,
        format_key="short",
        enabled=True,
        default_publish_visibility="unlisted",
        created_at=ROW_CREATED_AT,
        updated_at=ROW_CREATED_AT,
    )
    task = _task(channel.id, account.id)
    session.add_all([lane_format, task])
    await session.flush()

    publication = _publication(task)
    operation = _operation(task)
    queue = ChannelOpsQueueItem(
        kind="observe_job",
        idempotency_key=f"soak-{uuid.uuid4()}",
        channel_profile_id=channel.id,
        status="succeeded",
        last_error="sensitive queue error payload",
        created_at=ROW_CREATED_AT,
        updated_at=ROW_CREATED_AT,
    )
    session.add_all([publication, operation, queue])
    await session.flush()
    feedback = FeedbackSnapshot(
        publication_id=publication.id,
        snapshot_stage="24h",
        collected_at=NOW - timedelta(hours=1),
        raw_json={"secret": "sensitive feedback payload"},
    )
    session.add(feedback)
    await session.commit()
    return {
        "channel": channel,
        "account": account,
        "lane": lane,
        "lane_format": lane_format,
        "task": task,
        "publication": publication,
        "operation": operation,
        "queue": queue,
        "feedback": feedback,
    }


@pytest.mark.asyncio
async def test_healthy_channel_returns_immutable_assessment_with_count_metrics(soak_session):
    rows = await _seed_graph(soak_session)

    assessment = await assess_channelops_soak(
        soak_session,
        _policy(rows["channel"].id),
        now=NOW,
    )

    assert assessment.critical_codes == ()
    assert assessment.healthy is True
    assert assessment.metrics["channel_id"] == str(rows["channel"].id)
    assert assessment.metrics["publication_count"] == 1
    with pytest.raises(FrozenInstanceError):
        assessment.critical_codes = ("service_unhealthy",)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "expected_code"),
    [
        ("default_privacy", "public", "unsafe_account_privacy"),
        (
            "external_asset_auto_publish",
            True,
            "external_asset_auto_publish_enabled",
        ),
    ],
)
async def test_unsafe_account_configuration_is_critical(
    soak_session,
    field,
    value,
    expected_code,
):
    rows = await _seed_graph(soak_session)
    setattr(rows["account"], field, value)
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert expected_code in assessment.critical_codes


@pytest.mark.asyncio
async def test_unsafe_lane_privacy_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    rows["lane_format"].default_publish_visibility = "public"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "unsafe_lane_privacy" in assessment.critical_codes


@pytest.mark.asyncio
async def test_unsafe_publication_privacy_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    rows["publication"].current_privacy = "public"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "unsafe_publication_privacy" in assessment.critical_codes


@pytest.mark.asyncio
async def test_uncertain_upload_operation_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    rows["operation"].status = "uncertain"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "ambiguous_upload_operation" in assessment.critical_codes


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["reserved", "submitted"])
async def test_stale_in_flight_upload_operation_is_critical(soak_session, status):
    rows = await _seed_graph(soak_session)
    rows["operation"].status = status
    rows["operation"].request_attempted_at = NOW - timedelta(minutes=46)
    rows["operation"].updated_at = NOW - timedelta(minutes=46)
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "stale_upload_operation" in assessment.critical_codes


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["failed", "dead_lettered"])
async def test_channelops_queue_failure_is_critical(soak_session, status):
    rows = await _seed_graph(soak_session)
    rows["queue"].status = status
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "channelops_queue_failure" in assessment.critical_codes


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["failed", "held"])
async def test_failed_or_held_production_task_is_critical(soak_session, state):
    rows = await _seed_graph(soak_session)
    rows["task"].state = state
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "production_task_failure" in assessment.critical_codes


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["uploaded_private", "scheduled", "published", "measured"])
async def test_external_asset_progress_requires_human_approval(soak_session, state):
    rows = await _seed_graph(soak_session)
    rows["task"].uses_external_assets = True
    rows["task"].approval_mode = "agent"
    rows["task"].state = state
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "external_asset_human_approval_missing" in assessment.critical_codes


@pytest.mark.asyncio
async def test_publication_cadence_above_policy_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    second_task = _task(rows["channel"].id, rows["account"].id, state="published")
    soak_session.add(second_task)
    await soak_session.flush()
    second_publication = _publication(second_task, uploaded_at=NOW - timedelta(hours=1))
    soak_session.add(second_publication)
    await soak_session.flush()
    soak_session.add(
        FeedbackSnapshot(
            publication_id=second_publication.id,
            snapshot_stage="24h",
            collected_at=NOW,
        )
    )
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "publication_cadence_exceeded" in assessment.critical_codes
    assert assessment.metrics["publication_last_24h_count"] == 2


@pytest.mark.asyncio
async def test_feedback_missing_after_grace_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    await soak_session.delete(rows["feedback"])
    rows["publication"].uploaded_at = NOW - timedelta(hours=31)
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "feedback_missing_after_grace" in assessment.critical_codes


@pytest.mark.asyncio
async def test_allowed_external_conditions_are_sorted_and_deduplicated(soak_session):
    rows = await _seed_graph(soak_session)

    assessment = await assess_channelops_soak(
        soak_session,
        _policy(rows["channel"].id),
        external_conditions=("service_unhealthy", "redis_group_missing", "service_unhealthy"),
        now=NOW,
    )

    assert assessment.critical_codes == ("redis_group_missing", "service_unhealthy")
    assert assessment.metrics["external_condition_count"] == 2
    assert "service_unhealthy" in ALLOWED_EXTERNAL_CONDITIONS


@pytest.mark.asyncio
async def test_unknown_external_condition_is_rejected_before_database_access(soak_session):
    with pytest.raises(ValueError, match="external condition"):
        await assess_channelops_soak(
            soak_session,
            _policy(uuid.uuid4()),
            external_conditions=("database_url=postgresql://secret",),
            now=NOW,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("missing", "channel_missing"),
        ("disabled", "channel_disabled"),
        ("dry_run", "channel_dry_run"),
        ("halted", "channel_halted"),
    ],
)
async def test_invalid_channel_state_is_critical(soak_session, mutation, expected_code):
    rows = await _seed_graph(soak_session)
    channel_id = rows["channel"].id
    if mutation == "missing":
        channel_id = uuid.uuid4()
    elif mutation == "disabled":
        rows["channel"].enabled = False
    elif mutation == "dry_run":
        rows["channel"].dry_run = True
    else:
        rows["channel"].halted_at = NOW - timedelta(minutes=1)
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(channel_id), now=NOW)

    assert expected_code in assessment.critical_codes


@pytest.mark.asyncio
async def test_assessment_is_channel_scoped_and_metrics_never_expose_payloads(soak_session):
    rows = await _seed_graph(soak_session)
    other = ChannelProfile(name="other", enabled=True, dry_run=False)
    soak_session.add(other)
    await soak_session.flush()
    other_account = PublishingAccount(
        channel_profile_id=other.id,
        account_label="other",
        default_privacy="public",
        external_asset_auto_publish=True,
        enabled=True,
    )
    soak_session.add(other_account)
    await soak_session.flush()
    other_task = _task(other.id, other_account.id, state="failed")
    soak_session.add(other_task)
    await soak_session.flush()
    soak_session.add(_operation(other_task, status="uncertain"))
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert assessment.critical_codes == ()
    encoded = json.dumps(dict(assessment.metrics), sort_keys=True)
    for sensitive in (
        "sensitive",
        "secret",
        "https://",
        "postgresql://",
        str(rows["account"].id),
        str(rows["task"].id),
        str(rows["publication"].id),
        str(other.id),
    ):
        assert sensitive not in encoded
    assert all(
        key == "channel_id" or isinstance(value, int)
        for key, value in assessment.metrics.items()
    )


def test_policy_and_assessment_are_frozen_and_policy_normalizes_utc():
    channel_id = uuid.uuid4()
    policy = SoakGuardPolicy(channel_id=channel_id, started_at=STARTED_AT.replace(tzinfo=None))
    assessment = SoakGuardAssessment(critical_codes=(), metrics={"channel_id": str(channel_id)})

    assert policy.started_at == STARTED_AT
    with pytest.raises(FrozenInstanceError):
        policy.max_publications_per_24h = 2
    with pytest.raises(FrozenInstanceError):
        assessment.metrics = {}
