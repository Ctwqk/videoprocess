from __future__ import annotations

import json
import os
import uuid
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

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
from app.models.autoflow import AutoFlowPlan
from app.autoflow.service import autoflow_service
from app.schemas.autoflow import AutoFlowPlanPatch
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
NAIVE_ROW_CREATED_AT = ROW_CREATED_AT.replace(tzinfo=None)
TABLES = (
    AutoFlowPlan.__table__,
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


def _review_plan(*, review_approved_at: datetime | None = None, status: str = "review_approved") -> AutoFlowPlan:
    return AutoFlowPlan(
        prompt="external review",
        request_json={
            "prompt": "external review",
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
        rights_json={"status": "review_required", "reasons": [], "allowed_publish_modes": ["private_upload"]},
        validation_json={"valid": True, "errors": [], "warnings": [], "repairs": []},
        status=status,
        review_approved_at=review_approved_at,
    )


def _pre_upload_evidence(task: ProductionTask, plan: AutoFlowPlan, *, token: datetime | None = None, plan_id=None):
    resolved_token = token or plan.review_approved_at
    return {
        "pre_upload": {
            "kind": "human_review",
            "scope": "external_asset_pre_upload",
            "human_actor": "operator@example.com",
            "reviewed_at": resolved_token.isoformat() if resolved_token else "",
            "autoflow_plan_id": str(plan_id or plan.id),
            "plan_review_approved_at": resolved_token.isoformat() if resolved_token else "",
            "review_notes": "reviewed",
        }
    }


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
        "created_at": NAIVE_ROW_CREATED_AT,
        "updated_at": NAIVE_ROW_CREATED_AT,
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
        "created_at": NAIVE_ROW_CREATED_AT,
        "updated_at": NAIVE_ROW_CREATED_AT,
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
        "created_at": NAIVE_ROW_CREATED_AT,
        "updated_at": NAIVE_ROW_CREATED_AT,
        "completed_at": NOW - timedelta(hours=2),
    }
    values.update(overrides)
    return YouTubeUploadOperation(**values)


async def _seed_graph(session, *, include_operation: bool = True):
    channel = ChannelProfile(
        name="soak channel",
        enabled=True,
        dry_run=False,
        created_at=NAIVE_ROW_CREATED_AT,
        updated_at=NAIVE_ROW_CREATED_AT,
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
        created_at=NAIVE_ROW_CREATED_AT,
        updated_at=NAIVE_ROW_CREATED_AT,
    )
    lane = TopicLane(
        channel_profile_id=channel.id,
        name="soak lane",
        enabled=True,
        created_at=NAIVE_ROW_CREATED_AT,
        updated_at=NAIVE_ROW_CREATED_AT,
    )
    session.add_all([account, lane])
    await session.flush()

    lane_format = LaneFormatMatrix(
        topic_lane_id=lane.id,
        format_key="short",
        enabled=True,
        default_publish_visibility="unlisted",
        created_at=NAIVE_ROW_CREATED_AT,
        updated_at=NAIVE_ROW_CREATED_AT,
    )
    task = _task(channel.id, account.id)
    session.add_all([lane_format, task])
    await session.flush()

    publication = _publication(task)
    operation = _operation(task) if include_operation else None
    queue = ChannelOpsQueueItem(
        kind="observe_job",
        idempotency_key=f"soak-{uuid.uuid4()}",
        channel_profile_id=channel.id,
        status="succeeded",
        last_error="sensitive queue error payload",
        created_at=NAIVE_ROW_CREATED_AT,
        updated_at=NAIVE_ROW_CREATED_AT,
    )
    session.add_all(
        [row for row in (publication, operation, queue) if row is not None]
    )
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
async def test_desired_public_current_private_publication_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    rows["publication"].desired_privacy = "public"
    rows["publication"].current_privacy = "private"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "unsafe_publication_privacy" in assessment.critical_codes
    assert assessment.metrics["unsafe_publication_privacy_count"] == 1
    assert "public" not in assessment.metrics.values()


@pytest.mark.asyncio
async def test_public_upload_operation_privacy_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    rows["operation"].privacy = "public"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "unsafe_upload_operation_privacy" in assessment.critical_codes
    assert assessment.metrics["unsafe_upload_operation_privacy_count"] == 1
    assert "public" not in assessment.metrics.values()


@pytest.mark.asyncio
async def test_uncertain_upload_operation_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    rows["operation"].status = "uncertain"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "ambiguous_upload_operation" in assessment.critical_codes


@pytest.mark.asyncio
async def test_failed_upload_operation_is_critical(soak_session):
    rows = await _seed_graph(soak_session)
    rows["operation"].status = "failed"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "failed_upload_operation" in assessment.critical_codes
    assert assessment.metrics["failed_upload_operation_count"] == 1


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
async def test_snapshot_external_asset_planning_requires_durable_human_evidence(soak_session):
    rows = await _seed_graph(soak_session)
    rows["task"].uses_external_assets = False
    rows["task"].source_platforms_json = []
    rows["task"].channel_config_snapshot_json = {
        "lane_format": {"source_platforms_json": ["youtube"]}
    }
    rows["task"].state = "planning"
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "external_asset_human_approval_missing" in assessment.critical_codes


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence_case", ["missing", "agent_only", "stale", "mismatched", "rejected"])
async def test_external_asset_planning_rejects_invalid_durable_human_evidence(soak_session, evidence_case):
    rows = await _seed_graph(soak_session)
    approved_at = NOW - timedelta(hours=1)
    plan = _review_plan(review_approved_at=approved_at)
    soak_session.add(plan)
    await soak_session.flush()
    task = rows["task"]
    task.uses_external_assets = True
    task.approval_mode = "human"
    task.autoflow_plan_id = plan.id
    task.state = "planning"
    if evidence_case == "agent_only":
        task.agent_approval_evidence_json = {"approved_by": "channel_agent"}
    elif evidence_case == "stale":
        task.human_review_evidence_json = _pre_upload_evidence(
            task,
            plan,
            token=approved_at - timedelta(seconds=1),
        )
    elif evidence_case == "mismatched":
        task.human_review_evidence_json = _pre_upload_evidence(task, plan, plan_id=uuid.uuid4())
    elif evidence_case == "rejected":
        task.human_review_evidence_json = _pre_upload_evidence(task, plan)
        plan.status = "rejected"
        plan.review_approved_at = None
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "external_asset_human_approval_missing" in assessment.critical_codes


@pytest.mark.asyncio
async def test_external_asset_planning_accepts_current_human_review_evidence(soak_session):
    rows = await _seed_graph(soak_session)
    approved_at = NOW - timedelta(hours=1)
    plan = _review_plan(review_approved_at=approved_at)
    soak_session.add(plan)
    await soak_session.flush()
    task = rows["task"]
    task.uses_external_assets = True
    task.approval_mode = "human"
    task.autoflow_plan_id = plan.id
    task.state = "planning"
    task.human_review_evidence_json = _pre_upload_evidence(task, plan)
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert "external_asset_human_approval_missing" not in assessment.critical_codes


@pytest.mark.asyncio
async def test_approval_relevant_plan_patch_invalidates_task_human_review_evidence(soak_session):
    rows = await _seed_graph(soak_session)
    approved_at = NOW - timedelta(hours=1)
    plan = _review_plan(review_approved_at=approved_at)
    soak_session.add(plan)
    await soak_session.flush()
    task = rows["task"]
    task.uses_external_assets = True
    task.autoflow_plan_id = plan.id
    task.state = "planning"
    task.human_review_evidence_json = _pre_upload_evidence(task, plan)
    await soak_session.commit()

    patched = await autoflow_service.patch_plan(
        str(plan.id),
        AutoFlowPlanPatch(
            metadata={"description": "changed after review"},
            rebuild_definition=False,
            validate=False,
            evaluate_rights=False,
        ),
        soak_session,
    )
    assert patched is not None
    assert patched.review_approved_at is None

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
async def test_unknown_external_condition_is_rejected_before_database_access():
    db = AsyncMock()

    with pytest.raises(ValueError, match="external condition"):
        await assess_channelops_soak(
            db,
            _policy(uuid.uuid4()),
            external_conditions=("database_url=postgresql://secret",),
            now=NOW,
        )

    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_started_at_exactly_five_minutes_in_future_is_accepted(soak_session):
    rows = await _seed_graph(soak_session)

    assessment = await assess_channelops_soak(
        soak_session,
        _policy(rows["channel"].id, started_at=NOW + timedelta(seconds=300)),
        now=NOW,
    )

    assert assessment.metrics["channel_count"] == 1


@pytest.mark.asyncio
async def test_started_at_over_five_minutes_in_future_is_rejected_before_database_access():
    db = AsyncMock()

    with pytest.raises(ValueError, match="300 seconds"):
        await assess_channelops_soak(
            db,
            _policy(uuid.uuid4(), started_at=NOW + timedelta(seconds=301)),
            now=NOW,
        )

    db.execute.assert_not_awaited()


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
    other_lane = TopicLane(channel_profile_id=other.id, name="other lane", enabled=True)
    soak_session.add_all([other_account, other_lane])
    await soak_session.flush()
    other_lane_format = LaneFormatMatrix(
        topic_lane_id=other_lane.id,
        format_key="other-short",
        enabled=True,
        default_publish_visibility="public",
    )
    other_task = _task(other.id, other_account.id, state="failed")
    soak_session.add_all([other_lane_format, other_task])
    await soak_session.flush()
    other_publication = _publication(
        other_task,
        desired_privacy="public",
        current_privacy="public",
    )
    soak_session.add(other_publication)
    await soak_session.flush()
    soak_session.add_all(
        [
            _operation(other_task, status="uncertain", privacy="public"),
            FeedbackSnapshot(
                publication_id=other_publication.id,
                snapshot_stage="24h",
                collected_at=NOW,
                raw_json={"secret": "foreign feedback payload"},
            ),
            ChannelOpsQueueItem(
                kind="observe_job",
                idempotency_key=f"foreign-{uuid.uuid4()}",
                channel_profile_id=other.id,
                status="dead_lettered",
                last_error="foreign queue failure payload",
            ),
        ]
    )
    await soak_session.commit()

    assessment = await assess_channelops_soak(soak_session, _policy(rows["channel"].id), now=NOW)

    assert assessment.critical_codes == ()
    assert assessment.metrics["enabled_account_count"] == 1
    assert assessment.metrics["enabled_lane_format_count"] == 1
    assert assessment.metrics["publication_count"] == 1
    assert assessment.metrics["feedback_snapshot_count"] == 1
    assert assessment.metrics["upload_operation_count"] == 1
    assert assessment.metrics["channelops_queue_item_count"] == 1
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


@pytest.mark.asyncio
async def test_postgresql_assessment_accepts_mixed_timestamp_column_contracts():
    """Set CHANNEL_OPS_POSTGRES_TEST_URL to a migrated asyncpg test database."""
    database_url = os.getenv("CHANNEL_OPS_POSTGRES_TEST_URL")
    if not database_url:
        pytest.skip("CHANNEL_OPS_POSTGRES_TEST_URL is not set")
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    channel_id = None
    try:
        async with factory() as session:
            rows = await _seed_graph(session, include_operation=False)
            channel_id = rows["channel"].id

            assessment = await assess_channelops_soak(
                session,
                _policy(channel_id),
                now=NOW,
            )

            assert assessment.metrics["channel_count"] == 1
            assert assessment.metrics["production_task_count"] == 1
    finally:
        if channel_id is not None:
            async with factory() as cleanup_session:
                channel = await cleanup_session.get(ChannelProfile, channel_id)
                if channel is not None:
                    await cleanup_session.delete(channel)
                    await cleanup_session.commit()
        await engine.dispose()
