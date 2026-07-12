from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.channel_agent.clock import FakeClock
from app.channel_agent.clients import (
    AutoFlowExecutionObservation,
    AutoFlowJobObservation,
    FakeAutoFlowClient,
    FakeMiniMaxClient,
    FakeYouTubeClient,
)
from app.channel_agent.material_usage import segment_signature
from app.channel_agent.queue import ChannelOpsQueueService
from app.channel_agent.service import ChannelAgentService
from app.events.outbox import event_outbox_table
from app.models.channel_agent import (
    AgentTickAudit,
    ChannelOpsQueueItem,
    ChannelProfile,
    FeedbackSnapshot,
    LaneFormatMatrix,
    ManualSeed,
    MaterialUsageLedger,
    ProductionTask,
    PublicationRecord,
    PublishingAccount,
    TakedownEvent,
    TopicLane,
)
from app.models.autoflow import AutoFlowPlan, AutoFlowRun
from app.pds_client import PDSDecision, PDSDecisionRequest
from app.schemas.autoflow import AutoFlowRequest


CHANNEL_AGENT_TABLES = (
    AutoFlowPlan.__table__,
    AutoFlowRun.__table__,
    ChannelProfile.__table__,
    TopicLane.__table__,
    PublishingAccount.__table__,
    LaneFormatMatrix.__table__,
    ChannelOpsQueueItem.__table__,
    AgentTickAudit.__table__,
    ManualSeed.__table__,
    ProductionTask.__table__,
    MaterialUsageLedger.__table__,
    PublicationRecord.__table__,
    TakedownEvent.__table__,
    FeedbackSnapshot.__table__,
    event_outbox_table,
)


@pytest.fixture
async def service_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for table in CHANNEL_AGENT_TABLES:
            await conn.run_sync(table.create)

    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    await engine.dispose()


async def _channel_graph(db, *, dry_run: bool = True, external_auto: bool = True):
    channel = ChannelProfile(name="Channel", language="zh", dry_run=dry_run)
    db.add(channel)
    await db.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="lane", keywords_json=["cartoon"])
    account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="main",
        platform_account_id="yt-1",
        credential_ref="youtube/main",
        external_asset_auto_publish=external_auto,
    )
    db.add_all([lane, account])
    await db.flush()
    lane_format = LaneFormatMatrix(
        topic_lane_id=lane.id,
        format_key="shorts_9x16",
        target_duration_sec=30,
        template_pool_json=["material_library_remix"],
    )
    db.add(lane_format)
    await db.commit()
    return channel, lane, account, lane_format


def _service(
    *,
    clock=None,
    autoflow=None,
    youtube=None,
    minimax=None,
    pds=None,
    event_outbox=None,
    pds_health_monitor_enabled: bool = False,
) -> ChannelAgentService:
    clock = clock or FakeClock(datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc))
    queue = ChannelOpsQueueService(clock=clock)
    return ChannelAgentService(
        queue=queue,
        clock=clock,
        autoflow_client=autoflow or FakeAutoFlowClient(),
        youtube_client=youtube or FakeYouTubeClient(),
        minimax_client=minimax or FakeMiniMaxClient(),
        pds_client=pds,
        event_outbox=event_outbox,
        pds_health_monitor_enabled=pds_health_monitor_enabled,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _publication_for_task(
    task: ProductionTask,
    account: PublishingAccount,
    *,
    publish_status: str,
    scheduled_publish_at: datetime | None = None,
    public_at: datetime | None = None,
    uploaded_at: datetime | None = None,
    title: str = "published",
) -> PublicationRecord:
    return PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id=f"yt-{uuid.uuid4()}",
        title=title,
        desired_privacy="unlisted",
        current_privacy="public" if publish_status == "public" else "private",
        publish_status=publish_status,
        scheduled_publish_at=scheduled_publish_at,
        public_at=public_at,
        uploaded_at=uploaded_at,
        compliance_disposition="assumed_fair_use",
    )


async def _promotion_item_graph(db, *, target_visibility: str = "unlisted"):
    channel, _lane, account, _lane_format = await _channel_graph(db, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="publish risky video",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    db.add(task)
    await db.flush()
    publication = _publication_for_task(task, account, publish_status="uploaded", title="risky")
    db.add(publication)
    await db.flush()
    item = ChannelOpsQueueItem(
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:{target_visibility}",
        payload_json={"publication_id": str(publication.id), "target_visibility": target_visibility},
    )
    db.add(item)
    await db.commit()
    return channel, task, publication, item


async def _publish_task_with_material(db, *, source: str):
    channel, lane, account, _lane_format = await _channel_graph(db, dry_run=False)
    material_id = "mat-final-guard"
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source=source,
        prompt="make a test short",
        title_seed="test",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    db.add(task)
    await db.commit()
    item = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key=f"publish_task:{task.id}",
        payload_json={
            "production_task_id": str(task.id),
            "youtube": {
                "video_id": f"yt-{source}",
                "material_refs": [
                    {
                        "material_id": material_id,
                        "start_ms": 0,
                        "end_ms": 1000,
                    }
                ],
            },
        },
    )
    db.add(item)
    await db.commit()
    return channel, lane, account, task, item, material_id


class FailedExecutionAutoFlowClient(FakeAutoFlowClient):
    async def execute_task(self, task, request):
        return AutoFlowExecutionObservation(
            run_id="",
            pipeline_id=None,
            job_id=None,
            status="failed",
            error_message="review approval is required before execution",
        )


class IncompleteExecutionAutoFlowClient(FakeAutoFlowClient):
    async def execute_task(self, task, request):
        return AutoFlowExecutionObservation(
            run_id=str(uuid.uuid4()),
            pipeline_id=str(uuid.uuid4()),
            job_id=None,
            status="running",
        )


class CountingExecuteAutoFlowClient(FakeAutoFlowClient):
    def __init__(self):
        super().__init__()
        self.execute_calls = 0

    async def execute_task(self, task, request):
        self.execute_calls += 1
        return await super().execute_task(task, request)


class ApprovalRecordingAutoFlowClient(FakeAutoFlowClient):
    def __init__(self):
        super().__init__()
        self.approvals: list[dict[str, object]] = []

    async def approve_plan(self, plan_id: str, *, approved_by: str, evidence: dict):
        self.approvals.append(
            {
                "plan_id": plan_id,
                "approved_by": approved_by,
                "evidence": dict(evidence),
            }
        )


class AlwaysRunningAutoFlowClient(FakeAutoFlowClient):
    async def observe_job(self, db, *, run_id: str, job_id: str):
        return AutoFlowJobObservation(
            run_id=run_id,
            pipeline_id=None,
            job_id=job_id,
            status="running",
        )


class FakePDSClient:
    def __init__(self, decision: PDSDecision | None = None) -> None:
        self.decision = decision or PDSDecision(decision_id="decision-allow", verdict="allow")
        self.requests: list[PDSDecisionRequest] = []

    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        self.requests.append(request)
        return self.decision


class SequencePDSClient:
    def __init__(self, decisions: list[PDSDecision]) -> None:
        self.decisions = list(decisions)
        self.requests: list[PDSDecisionRequest] = []

    async def decide(self, request: PDSDecisionRequest) -> PDSDecision:
        self.requests.append(request)
        if len(self.decisions) > 1:
            return self.decisions.pop(0)
        return self.decisions[0]


class RaisingEventOutbox:
    async def enqueue(self, *args, **kwargs) -> str:
        raise RuntimeError("outbox unavailable")


async def _outbox_payloads(db) -> list[dict]:
    result = await db.execute(
        select(event_outbox_table.c.payload).order_by(event_outbox_table.c.created_at.asc())
    )
    return [dict(row[0]) for row in result.all()]


def test_lane_prompt_template_is_structured():
    from app.channel_agent.lane_prompts import build_lane_prompt

    prompt = build_lane_prompt(
        lane_name="Tech News",
        lane_description="Daily AI product updates",
        keywords=["AI", "robots"],
        format_key="shorts_9x16",
        duration_sec=30,
        aspect_ratio="9:16",
    )

    assert 'Create a shorts_9x16 video for the "Tech News" topic.' in prompt
    assert "Theme: Daily AI product updates" in prompt
    assert "Keywords: AI, robots" in prompt
    assert "Target duration: 30s, aspect ratio 9:16." in prompt


def test_autoflow_request_uses_task_snapshot_configuration():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a tech short",
        title_seed="AI news",
        source_platforms_json=["bilibili", "youtube"],
        material_library_ids_json=["library-1"],
        uses_external_assets=True,
        channel_config_snapshot_json={
            "channel": {
                "id": "channel-1",
                "default_aspect_ratio": "16:9",
                "risk_policy_json": {
                    "source_strategy": "external_search",
                    "planning_mode": "ai_graph",
                },
            },
            "lane": {"id": "lane-1", "name": "Tech"},
            "lane_format": {
                "id": "format-1",
                "format_key": "long_16x9",
                "target_duration_sec": 60,
                "template_pool_json": ["news_remix"],
                "default_publish_visibility": "unlisted",
            },
            "manual_seed": {"constraints_json": {"tone": "calm"}},
        },
    )
    request = _service()._autoflow_request(task)

    assert request["duration_sec"] == 60
    assert request["aspect_ratio"] == "16:9"
    assert request["source_platforms"] == ["bilibili", "youtube"]
    assert request["source_strategy"] == "external_research"
    assert request["planning_mode"] == "ai_graph"
    assert request["constraints"]["template_pool_json"] == ["news_remix"]
    assert request["constraints"]["tone"] == "calm"
    assert request["publish_mode"] == "unlisted_upload"
    AutoFlowRequest.model_validate(request)


def test_autoflow_request_uses_owned_input_asset_profile():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="manual_seed",
        prompt="Create a canary",
        source_platforms_json=["youtube", "bilibili"],
        uses_external_assets=True,
        channel_config_snapshot_json={
            "channel": {
                "default_aspect_ratio": "16:9",
                "risk_policy_json": {
                    "source_strategy": "external_research",
                    "planning_mode": "storyboard",
                },
            },
            "lane_format": {"source_platforms_json": ["youtube"]},
            "manual_seed": {
                "constraints_json": {
                    "input_asset_id": "00000000-0000-0000-0000-000000000123",
                    "source_strategy": "input_video",
                    "planning_mode": "template",
                }
            },
        },
    )

    request = _service()._autoflow_request(task)

    assert request["input_asset_id"] == "00000000-0000-0000-0000-000000000123"
    assert request["source_platforms"] == []
    assert request["source_policy"] == "owned_only"
    assert request["source_strategy"] == "input_video"
    assert request["planning_mode"] == "template"
    assert "input_asset_id" not in request["constraints"]
    AutoFlowRequest.model_validate(request)


@pytest.mark.parametrize("input_asset_id", [None, {}, [], 123, "not-a-uuid"])
def test_autoflow_request_invalid_owned_input_asset_fails_closed(input_asset_id):
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="manual_seed",
        prompt="Create a canary",
        source_platforms_json=["youtube"],
        uses_external_assets=True,
        channel_config_snapshot_json={
            "channel": {"risk_policy_json": {"source_strategy": "external_research"}},
            "manual_seed": {"constraints_json": {"input_asset_id": input_asset_id}},
        },
    )

    request = _service()._autoflow_request(task)

    assert "input_asset_id" not in request
    assert "input_asset_id" not in request["constraints"]
    assert request["source_platforms"] == []
    assert request["source_policy"] == "owned_only"
    assert request["source_strategy"] == "input_video"
    assert request["planning_mode"] == "template"
    AutoFlowRequest.model_validate(request)


@pytest.mark.asyncio
async def test_task_from_candidate_uses_naive_timestamp_mixin_fields(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 18, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    task = _service(clock=clock)._task_from_candidate(
        channel,
        {
            "account": account,
            "lane": lane,
            "lane_format": lane_format,
            "seed": None,
            "source": "lane_seed",
            "title_seed": "timestamp smoke",
            "prompt": "timestamp smoke",
            "source_platforms_json": [],
            "material_library_ids_json": [],
        },
        created_at=clock.now(),
    )

    assert task.created_at.tzinfo is None
    assert task.updated_at.tzinfo is None
    assert task.state_updated_at.tzinfo is not None


def test_autoflow_request_falls_back_for_invalid_strategy_and_planning_mode():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a tech short",
        title_seed="AI news",
        source_platforms_json="youtube",
        material_library_ids_json="library-1",
        uses_external_assets=False,
        channel_config_snapshot_json={
            "channel": {
                "id": "channel-1",
                "default_aspect_ratio": "9:16",
                "risk_policy_json": {
                    "source_strategy": "unknown_source",
                    "planning_mode": "surprise_me",
                },
            },
            "lane_format": {
                "id": "format-1",
                "target_duration_sec": -5,
                "template_pool_json": "news_remix",
            },
            "manual_seed": {"constraints_json": "not-a-dict"},
        },
    )

    request = _service()._autoflow_request(task)

    assert request["source_strategy"] == "auto"
    assert request["planning_mode"] == "auto"
    assert request["source_platforms"] == ["youtube"]
    assert request["material_library_ids"] == ["library-1"]
    assert request["duration_sec"] == 30
    assert request["constraints"]["template_pool_json"] == ["news_remix"]
    AutoFlowRequest.model_validate(request)


def test_autoflow_request_uses_lane_format_platforms_for_source_policy():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a tech short",
        title_seed="AI news",
        source_platforms_json=[],
        uses_external_assets=False,
        channel_config_snapshot_json={
            "channel": {"id": "channel-1", "default_aspect_ratio": "9:16"},
            "lane_format": {
                "id": "format-1",
                "source_platforms_json": ["bilibili"],
                "target_duration_sec": 45,
            },
        },
    )

    request = _service()._autoflow_request(task)

    assert request["source_platforms"] == ["bilibili"]
    assert request["source_policy"] == "remix_with_review"
    AutoFlowRequest.model_validate(request)


def test_desired_privacy_falls_back_to_unlisted_not_public():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a short",
        channel_config_snapshot_json={"lane_format": {}},
    )
    account = PublishingAccount(
        channel_profile_id=uuid.uuid4(),
        account_label="main",
        platform_account_id="yt",
        credential_ref="youtube/main",
        default_privacy="public",
    )

    assert _service()._desired_privacy(task, account) == "unlisted"


def test_desired_privacy_preserves_private_account_default():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a short",
        channel_config_snapshot_json={"lane_format": {"default_publish_visibility": "public"}},
    )
    account = PublishingAccount(
        channel_profile_id=uuid.uuid4(),
        account_label="main",
        platform_account_id="yt",
        credential_ref="youtube/main",
        default_privacy="private",
    )

    assert _service()._desired_privacy(task, account) == "private"


def test_autoflow_publish_mode_uses_safe_account_snapshot_default():
    task = ProductionTask(
        channel_profile_id=uuid.uuid4(),
        target_account_id=uuid.uuid4(),
        source="lane_seed",
        prompt="Create a short",
        channel_config_snapshot_json={
            "account": {"default_privacy": "private"},
            "lane_format": {"default_publish_visibility": "public"},
        },
    )

    request = _service()._autoflow_request(task)

    assert request["publish_mode"] == "private_upload"
    AutoFlowRequest.model_validate(request)


@pytest.mark.asyncio
async def test_dry_run_tick_writes_audit_without_tasks(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=True)
    seed = ManualSeed(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        prompt="make a test short",
        title_seed="test short",
        source_platforms_json=["youtube", "bilibili"],
    )
    service_session.add(seed)
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.dry_run is True
    assert audit.ideas_discovered == 1
    assert audit.tasks_selected == 0
    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    assert tasks == []
    assert audit.decision_summary_json["per_lane_eligible_count"][str(lane.id)] == 1


@pytest.mark.asyncio
async def test_active_tick_creates_task_and_plan_queue_item(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="make a test short",
            title_seed="test short",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.state == "selected"
    assert set(task.score_breakdown_json) >= {
        "lane_weight",
        "material_fit",
        "freshness",
        "account_fit",
        "timing",
        "novelty",
        "repetition_risk",
        "compliance_risk",
        "total_score",
    }
    assert task.channel_config_snapshot_json["channel"]["dry_run"] is False
    queue_item = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "plan_task")
        )
    ).scalar_one()
    assert queue_item.idempotency_key == f"plan_task:{task.id}"
    assert queue_item.channel_profile_id == channel.id
    payloads = await _outbox_payloads(service_session)
    assert [payload["action_type"] for payload in payloads] == ["candidate_accepted"]
    assert payloads[0]["actor_id"] == str(account.id)
    assert payloads[0]["metadata"]["candidate_id"]
    assert payloads[0]["metadata"]["task_id"] == str(task.id)
    assert payloads[0]["metadata"]["score"] == 0.0
    assert payloads[0]["metadata"]["reason_codes"] == []
    assert payloads[0]["metadata"]["warning"] == "pds_disabled"


@pytest.mark.asyncio
async def test_lane_seed_task_defaults_to_agent_approval_mode(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)

    task = _service(clock=clock)._task_from_candidate(
        channel,
        {
            "candidate_id": "lane-candidate",
            "source": "lane_seed",
            "seed": None,
            "lane": lane,
            "lane_format": lane_format,
            "account": account,
            "prompt": "make a lane short",
            "title_seed": "lane short",
            "source_platforms_json": [],
            "material_library_ids_json": [],
        },
        created_at=clock.now(),
    )

    assert task.approval_mode == "agent"
    assert task.agent_approval_evidence_json == {}


@pytest.mark.asyncio
async def test_manual_seed_task_defaults_to_human_approval_mode(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    seed = ManualSeed(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        prompt="manual prompt",
        title_seed="manual",
    )
    service_session.add(seed)
    await service_session.flush()

    task = _service(clock=clock)._task_from_candidate(
        channel,
        {
            "candidate_id": "manual-candidate",
            "source": "manual_seed",
            "seed": seed,
            "lane": lane,
            "lane_format": lane_format,
            "account": account,
            "prompt": seed.prompt,
            "title_seed": seed.title_seed,
            "source_platforms_json": [],
            "material_library_ids_json": [],
        },
        created_at=clock.now(),
    )

    assert task.approval_mode == "human"
    assert task.agent_approval_evidence_json == {}


@pytest.mark.asyncio
async def test_handle_plan_task_agent_approves_lane_seed_when_pds_allows(service_session):
    channel, _lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="lane_seed",
        approval_mode="agent",
        prompt="make a lane short",
        state="selected",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    item = ChannelOpsQueueItem(
        kind="plan_task",
        idempotency_key=f"plan_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()
    autoflow = ApprovalRecordingAutoFlowClient()
    pds = FakePDSClient(
        PDSDecision(
            decision_id="plan-allow",
            verdict="allow",
            score=0.03,
            reasons=[{"code": "safe"}],
            rules_version="risk-v4",
        )
    )

    result = await _service(autoflow=autoflow, pds=pds).handle_plan_task(service_session, item)

    assert result.autoflow_plan_id is not None
    assert autoflow.approvals == [
        {
            "plan_id": str(result.autoflow_plan_id),
            "approved_by": "channel_agent",
            "evidence": {
                "decision_id": "plan-allow",
                "verdict": "allow",
                "score": 0.03,
                "rules_version": "risk-v4",
                "reason_codes": ["safe"],
            },
        }
    ]
    assert result.agent_approval_evidence_json["decision_id"] == "plan-allow"
    assert pds.requests[0].action_type == "plan_approval"


@pytest.mark.asyncio
async def test_handle_plan_task_does_not_agent_approve_manual_seed(service_session):
    channel, _lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        approval_mode="human",
        prompt="manual prompt",
        state="selected",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    item = ChannelOpsQueueItem(
        kind="plan_task",
        idempotency_key=f"plan_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()
    autoflow = ApprovalRecordingAutoFlowClient()
    pds = FakePDSClient(PDSDecision(decision_id="plan-allow", verdict="allow"))

    result = await _service(autoflow=autoflow, pds=pds).handle_plan_task(service_session, item)

    assert result.autoflow_plan_id is not None
    assert autoflow.approvals == []
    assert result.agent_approval_evidence_json == {}


@pytest.mark.asyncio
async def test_tick_rejects_candidate_when_pds_blocks(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane_format.enabled = False
    seed = ManualSeed(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        prompt="publish risky video",
        title_seed="risky",
        status="active",
    )
    service_session.add(seed)
    await service_session.commit()
    pds = FakePDSClient(
        PDSDecision(
            decision_id="decision-block",
            verdict="block",
            score=0.91,
            reasons=[{"code": "publishing_burst", "rule": "burst_publish_feature_flag"}],
            rules_version="risk-v1",
            metadata={"warning": "feature_provider_unavailable"},
        )
    )

    audit = await _service(pds=pds).tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    assert tasks == []
    assert audit.tasks_selected == 0
    assert audit.tasks_rejected == 1
    assert pds.requests
    assert pds.requests[0].action_type == "candidate_accept"
    assert pds.requests[0].actor_id == str(account.id)
    assert pds.requests[0].context["lane_id"] == str(lane.id)
    assert audit.guards_triggered_json[0]["guard"] == "pds_blocked"
    payloads = await _outbox_payloads(service_session)
    assert [payload["action_type"] for payload in payloads] == ["candidate_blocked"]
    assert payloads[0]["metadata"]["decision_id"] == "decision-block"
    assert payloads[0]["metadata"]["verdict"] == "block"
    assert payloads[0]["metadata"]["score"] == 0.91
    assert payloads[0]["metadata"]["rules_version"] == "risk-v1"
    assert payloads[0]["metadata"]["reason_codes"] == ["publishing_burst"]
    assert payloads[0]["metadata"]["warning"] == "feature_provider_unavailable"


@pytest.mark.asyncio
async def test_dry_run_tick_skips_candidate_pds_gate(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=True)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="dry run candidate",
            title_seed="dry run",
        )
    )
    await service_session.commit()
    pds = FakePDSClient(PDSDecision(decision_id="decision-block", verdict="block"))

    audit = await _service(pds=pds).tick(service_session, channel_id=channel.id)

    assert pds.requests == []
    assert audit.tasks_selected == 0
    assert audit.tasks_rejected == 0
    assert audit.decision_summary_json["rejected_candidates"] == []
    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    assert tasks == []


@pytest.mark.asyncio
async def test_lane_cadence_rejection_happens_before_pds(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    published_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="published",
        state="published",
        channel_config_snapshot_json={},
    )
    service_session.add(published_task)
    await service_session.flush()
    service_session.add(
        _publication_for_task(
            published_task,
            account,
            publish_status="scheduled",
            scheduled_publish_at=clock.now() - timedelta(hours=1),
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new candidate",
            title_seed="new",
        )
    )
    await service_session.commit()
    pds = FakePDSClient(PDSDecision(decision_id="decision-block", verdict="block"))

    audit = await _service(clock=clock, pds=pds).tick(service_session, channel_id=channel.id)

    assert pds.requests == []
    assert audit.tasks_selected == 0
    assert audit.tasks_rejected >= 1
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "lane_cadence"


@pytest.mark.asyncio
async def test_candidate_accepted_outbox_failure_rolls_back_task_and_plan_queue(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="make a test short",
            title_seed="test short",
        )
    )
    await service_session.commit()

    with pytest.raises(RuntimeError, match="outbox unavailable"):
        await _service(event_outbox=RaisingEventOutbox()).tick(service_session, channel_id=channel.id)
    await service_session.rollback()

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    plan_items = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "plan_task")
        )
    ).scalars().all()
    assert tasks == []
    assert plan_items == []


@pytest.mark.asyncio
async def test_active_tick_stores_lane_format_platforms_when_seed_has_none(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane_format.source_platforms_json = ["bilibili"]
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="make a test short",
            title_seed="test short",
            source_platforms_json=[],
        )
    )
    await service_session.commit()

    await _service().tick(service_session, channel_id=channel.id)

    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.source_platforms_json == ["bilibili"]
    assert task.uses_external_assets is True


@pytest.mark.asyncio
async def test_lane_driven_tick_creates_task_without_manual_seed(service_session):
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.description = "Daily AI updates"
    lane.keywords_json = ["AI"]
    lane_format.source_platforms_json = ["bilibili", "youtube"]
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.source == "lane_seed"
    assert task.manual_seed_id is None
    assert task.topic_lane_id == lane.id
    assert task.lane_format_id == lane_format.id
    assert task.source_platforms_json == ["bilibili", "youtube"]
    assert task.uses_external_assets is True
    assert "Daily AI updates" in task.prompt


@pytest.mark.asyncio
async def test_tick_excludes_lane_paused_until_future(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, _account, _lane_format = await _channel_graph(service_session, dry_run=False)
    lane.paused_until = clock.now() + timedelta(hours=1)
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    assert audit.tasks_selected == 0
    assert tasks == []
    assert queue_items == []


@pytest.mark.asyncio
async def test_targeted_manual_seed_rejects_paused_lane_without_unassigned_task(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    lane.enabled = False
    lane.paused_until = clock.now() + timedelta(hours=1)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="manual blocked by lane pause",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert audit.tasks_selected == 0
    assert tasks == []
    assert queue_items == []
    assert rejected[0]["guard"] == "lane_unavailable"
    assert rejected[0]["lane_id"] == str(lane.id)


@pytest.mark.asyncio
async def test_targeted_manual_seed_rejects_paused_account_without_fallback(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    account.enabled = False
    account.paused_until = clock.now() + timedelta(hours=1)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="manual blocked by account pause",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert audit.tasks_selected == 0
    assert tasks == []
    assert queue_items == []
    assert rejected[0]["guard"] in {"account_unavailable", "no_enabled_account"}


@pytest.mark.asyncio
async def test_targeted_manual_seed_rejects_cross_channel_account_without_task(service_session):
    first_channel, first_lane, _first_account, _lane_format = await _channel_graph(service_session, dry_run=False)
    _second_channel, _second_lane, second_account, _second_lane_format = await _channel_graph(
        service_session,
        dry_run=False,
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=first_channel.id,
            topic_lane_id=first_lane.id,
            target_account_id=second_account.id,
            prompt="manual blocked by cross-channel account",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=first_channel.id)

    tasks = (
        await service_session.execute(
            select(ProductionTask).where(ProductionTask.channel_profile_id == first_channel.id)
        )
    ).scalars().all()
    queue_items = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.channel_profile_id == first_channel.id)
        )
    ).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert audit.tasks_selected == 0
    assert tasks == []
    assert queue_items == []
    assert rejected[0]["guard"] == "account_unavailable"
    assert rejected[0]["account_id"] == str(second_account.id)


@pytest.mark.asyncio
async def test_manual_seed_consumes_first_then_lane_driven_fills_budget(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 3
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
        external_asset_auto_publish=True,
    )
    service_session.add(second_account)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=second_account.id,
            prompt="manual short",
            title_seed="manual",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)
    tasks = (
        await service_session.execute(select(ProductionTask).order_by(ProductionTask.created_at.asc()))
    ).scalars().all()

    assert audit.tasks_selected == 2
    assert [task.source for task in tasks] == ["manual_seed", "lane_seed"]
    assert {task.target_account_id for task in tasks} == {account.id, second_account.id}


@pytest.mark.asyncio
async def test_rejected_manual_seed_does_not_consume_lane_driven_budget(service_session):
    channel, lane, busy_account, _lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    free_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="free",
        platform_account_id="yt-free",
        credential_ref="youtube/free",
        external_asset_auto_publish=True,
    )
    service_session.add(free_account)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=busy_account.id,
            source="manual_seed",
            prompt="busy account task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=busy_account.id,
            prompt="manual that will be rejected",
            title_seed="manual rejected",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)
    created_tasks = (
        await service_session.execute(
            select(ProductionTask)
            .where(ProductionTask.channel_profile_id == channel.id)
            .where(ProductionTask.prompt != "busy account task")
        )
    ).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]

    assert audit.tasks_selected == 1
    assert [task.source for task in created_tasks] == ["lane_seed"]
    assert created_tasks[0].target_account_id == free_account.id
    assert rejected[0]["guard"] == "account_concurrency"


@pytest.mark.asyncio
async def test_dry_run_evaluates_candidates_without_creating_tasks_or_queue(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=True)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt="held task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="dry run candidate",
            title_seed="dry",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]

    assert len(tasks) == 1
    assert queue_items == []
    assert audit.candidates_scored >= 1
    assert rejected[0]["guard"] == "account_concurrency"


@pytest.mark.asyncio
async def test_dry_run_low_supply_records_guard_without_alert_queue(service_session):
    channel = ChannelProfile(name="Channel", language="zh", dry_run=True)
    service_session.add(channel)
    await service_session.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="empty lane")
    account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="main",
        platform_account_id="yt-1",
        credential_ref="youtube/main",
    )
    service_session.add_all([lane, account])
    await service_session.flush()
    for offset in (2, 1):
        service_session.add(
            AgentTickAudit(
                channel_profile_id=channel.id,
                tick_id=f"prior:{offset}",
                dry_run=True,
                started_at=datetime(2026, 5, 18, 9 - offset, 0, tzinfo=timezone.utc),
                finished_at=datetime(2026, 5, 18, 9 - offset, 1, tzinfo=timezone.utc),
                decision_summary_json={"per_lane_eligible_count": {str(lane.id): 0}},
            )
        )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    assert queue_items == []
    assert audit.guards_triggered_json == [{"guard": "material_supply_low", "lane_id": str(lane.id)}]


@pytest.mark.asyncio
async def test_untargeted_manual_seed_uses_free_account_before_lane_candidate(service_session):
    channel, lane, busy_account, _lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 2
    free_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="free",
        platform_account_id="yt-2",
        credential_ref="youtube/free",
        external_asset_auto_publish=True,
    )
    service_session.add(free_account)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=busy_account.id,
            source="lane_seed",
            prompt="busy task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    seed = ManualSeed(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        prompt="manual short",
        title_seed="manual",
    )
    service_session.add(seed)
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    created = (
        await service_session.execute(
            select(ProductionTask)
            .where(ProductionTask.prompt != "busy task")
            .order_by(ProductionTask.created_at.asc())
        )
    ).scalars().all()
    await service_session.refresh(seed)

    assert audit.tasks_selected == 1
    assert len(created) == 1
    assert created[0].source == "manual_seed"
    assert created[0].target_account_id == free_account.id
    assert created[0].manual_seed_id == seed.id
    assert seed.status == "exhausted"


@pytest.mark.asyncio
async def test_producing_task_blocks_same_account_candidate(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="lane_seed",
            prompt="producing task",
            state="producing",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="manual blocked by producing",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert len(tasks) == 1
    assert queue_items == []
    assert audit.tasks_selected == 0
    assert rejected[0]["guard"] == "account_concurrency"


@pytest.mark.asyncio
async def test_account_concurrency_guard_blocks_active_account(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt="held task",
            state="held",
            channel_config_snapshot_json={},
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new task",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.tasks_rejected >= 1
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "account_concurrency"


@pytest.mark.asyncio
async def test_consecutive_upload_failure_guard_uses_recent_window_and_alerts(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    for index, reason in enumerate(
        ["youtube upload failed", "ok", "quota exhausted", "thumbnail failed", "publish failed"]
    ):
        state = "failed" if index != 1 else "measured"
        task = ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt=f"task {index}",
            state=state,
            failure_reason=reason if state == "failed" else None,
            channel_config_snapshot_json={},
            created_at=clock.now() - timedelta(hours=5 - index),
        )
        service_session.add(task)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="blocked",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    rejected = audit.decision_summary_json["rejected_candidates"][0]
    assert rejected["guard"] == "consecutive_upload_failure"
    alert = (
        await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert"))
    ).scalar_one()
    assert "pause the account" in alert.payload_json["message"]
    assert str(account.id) in alert.payload_json["message"]


@pytest.mark.asyncio
async def test_consecutive_upload_failure_guard_allows_old_recent_window(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    for index, reason in enumerate(
        ["youtube upload failed", "quota exhausted", "thumbnail failed", "publish failed", "oauth failed"]
    ):
        task = ProductionTask(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            source="manual_seed",
            prompt=f"task {index}",
            state="failed",
            failure_reason=reason,
            channel_config_snapshot_json={},
            created_at=clock.now() - timedelta(hours=30 - index),
        )
        service_session.add(task)
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="allowed",
            title_seed="allowed",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    assert audit.decision_summary_json["rejected_candidates"] == []
    alerts = (await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert"))).scalars().all()
    assert alerts == []


@pytest.mark.asyncio
async def test_consecutive_upload_failure_guard_blocks_exactly_three_recent_upload_failures(
    service_session,
):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    for index, reason in enumerate(
        ["render crashed", "upload timed out", "script typo", "quota exhausted", "publish failed"]
    ):
        service_session.add(
            ProductionTask(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                target_account_id=account.id,
                source="manual_seed",
                prompt=f"task {index}",
                state="failed",
                failure_reason=reason,
                channel_config_snapshot_json={},
                created_at=clock.now() - timedelta(minutes=50 - index),
            )
        )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="blocked",
            title_seed="blocked",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    rejected = audit.decision_summary_json["rejected_candidates"][0]
    assert rejected["guard"] == "consecutive_upload_failure"


@pytest.mark.asyncio
async def test_consecutive_upload_failure_guard_ignores_non_upload_failures(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    for index, reason in enumerate(
        ["render crashed", "upload timed out", "script typo", "quota exhausted", "audio mix failed"]
    ):
        service_session.add(
            ProductionTask(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                target_account_id=account.id,
                source="manual_seed",
                prompt=f"task {index}",
                state="failed",
                failure_reason=reason,
                channel_config_snapshot_json={},
                created_at=clock.now() - timedelta(minutes=50 - index),
            )
        )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="allowed",
            title_seed="allowed",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    assert audit.decision_summary_json["rejected_candidates"] == []


@pytest.mark.asyncio
async def test_dry_run_consecutive_upload_failure_guard_records_rejection_without_alert(
    service_session,
):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=True)
    for index, reason in enumerate(["upload failed", "quota exhausted", "publish failed"]):
        service_session.add(
            ProductionTask(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                target_account_id=account.id,
                source="manual_seed",
                prompt=f"task {index}",
                state="failed",
                failure_reason=reason,
                channel_config_snapshot_json={},
                created_at=clock.now() - timedelta(minutes=30 - index),
            )
        )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="dry-run blocked",
            title_seed="dry",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"][0]
    assert audit.tasks_selected == 0
    assert rejected["guard"] == "consecutive_upload_failure"
    assert [item for item in queue_items if item.kind == "send_alert"] == []


@pytest.mark.asyncio
async def test_lane_cadence_guard_counts_publications_not_created_tasks(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    published_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="published",
        state="published",
        channel_config_snapshot_json={},
    )
    service_session.add(published_task)
    await service_session.flush()
    service_session.add(
        PublicationRecord(
            production_task_id=published_task.id,
            platform="youtube",
            account_id=account.id,
            platform_content_id="yt-1",
            title="published",
            desired_privacy="unlisted",
            current_privacy="private",
            publish_status="scheduled",
            scheduled_publish_at=clock.now() - timedelta(hours=1),
            compliance_disposition="assumed_fair_use",
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "lane_cadence"


@pytest.mark.asyncio
async def test_lane_cadence_guard_prefers_public_at_for_public_publications(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    published_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="published",
        state="published",
        channel_config_snapshot_json={},
    )
    service_session.add(published_task)
    await service_session.flush()
    service_session.add(
        _publication_for_task(
            published_task,
            account,
            publish_status="public",
            scheduled_publish_at=clock.now() - timedelta(hours=30),
            public_at=clock.now() - timedelta(hours=1),
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "lane_cadence"


@pytest.mark.asyncio
async def test_lane_cadence_guard_reserves_same_tick_lane_slots(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, first_account, _lane_format = await _channel_graph(
        service_session,
        dry_run=False,
    )
    lane.max_posts_per_day = 1
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
        external_asset_auto_publish=True,
    )
    service_session.add(second_account)
    service_session.add_all(
        [
            ManualSeed(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                target_account_id=first_account.id,
                prompt="first manual",
                title_seed="first",
            ),
            ManualSeed(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                target_account_id=second_account.id,
                prompt="second manual",
                title_seed="second",
            ),
        ]
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert audit.tasks_selected == 1
    assert len(tasks) == 1
    assert len(rejected) == 1
    assert rejected[0]["guard"] == "lane_cadence"


@pytest.mark.asyncio
async def test_lane_cadence_guard_counts_recent_scheduled_publication_for_cooldown(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.cooldown_after_post_minutes = 60
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
        external_asset_auto_publish=True,
    )
    service_session.add(second_account)
    published_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="scheduled",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(published_task)
    await service_session.flush()
    service_session.add(
        _publication_for_task(
            published_task,
            account,
            publish_status="scheduled",
            scheduled_publish_at=clock.now() - timedelta(minutes=10),
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=second_account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "lane_cadence"


@pytest.mark.asyncio
async def test_lane_cadence_guard_counts_future_scheduled_publication_for_cooldown(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.cooldown_after_post_minutes = 60
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
        external_asset_auto_publish=True,
    )
    service_session.add(second_account)
    published_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="scheduled",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(published_task)
    await service_session.flush()
    service_session.add(
        _publication_for_task(
            published_task,
            account,
            publish_status="scheduled",
            scheduled_publish_at=clock.now() + timedelta(minutes=10),
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=second_account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 0
    assert audit.decision_summary_json["rejected_candidates"][0]["guard"] == "lane_cadence"


@pytest.mark.asyncio
async def test_lane_cadence_guard_counts_future_scheduled_publication_in_streak(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 5
    lane.cooldown_after_post_minutes = 0
    lane.max_consecutive_streak = 1
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
        external_asset_auto_publish=True,
    )
    service_session.add(second_account)
    scheduled_task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        lane_format_id=lane_format.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="scheduled",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(scheduled_task)
    await service_session.flush()
    service_session.add(
        _publication_for_task(
            scheduled_task,
            account,
            publish_status="scheduled",
            scheduled_publish_at=clock.now() + timedelta(hours=1),
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=second_account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    rejected = audit.decision_summary_json["rejected_candidates"][0]
    assert audit.tasks_selected == 0
    assert rejected["guard"] == "lane_cadence"
    assert "publication streak" in rejected["reason"]
    assert "max_consecutive_streak is 1" in rejected["reason"]


@pytest.mark.asyncio
async def test_lane_cadence_guard_ignores_held_tasks_without_publications(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    lane.max_posts_per_day = 1
    busy_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="busy",
        platform_account_id="yt-2",
        credential_ref="youtube/busy",
        external_asset_auto_publish=True,
    )
    service_session.add(busy_account)
    await service_session.flush()
    for index in range(4):
        service_session.add(
            ProductionTask(
                channel_profile_id=channel.id,
                topic_lane_id=lane.id,
                lane_format_id=lane_format.id,
                target_account_id=busy_account.id,
                source="manual_seed",
                prompt=f"held {index}",
                state="held",
                channel_config_snapshot_json={},
                created_at=clock.now() - timedelta(hours=index),
            )
        )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="new",
            title_seed="new",
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    assert audit.tasks_selected == 1
    assert audit.decision_summary_json["rejected_candidates"] == []


@pytest.mark.asyncio
async def test_dry_run_without_enabled_account_audits_rejection(service_session):
    channel = ChannelProfile(name="Channel", language="zh", dry_run=True)
    service_session.add(channel)
    await service_session.flush()
    lane = TopicLane(channel_profile_id=channel.id, name="lane")
    service_session.add(lane)
    await service_session.flush()
    service_session.add(
        LaneFormatMatrix(
            topic_lane_id=lane.id,
            format_key="shorts_9x16",
            target_duration_sec=30,
            template_pool_json=["material_library_remix"],
        )
    )
    await service_session.commit()

    audit = await _service().tick(service_session, channel_id=channel.id)

    tasks = (await service_session.execute(select(ProductionTask))).scalars().all()
    queue_items = (await service_session.execute(select(ChannelOpsQueueItem))).scalars().all()
    rejected = audit.decision_summary_json["rejected_candidates"]
    assert tasks == []
    assert queue_items == []
    assert audit.tasks_rejected >= 1
    assert rejected[0]["guard"] == "no_enabled_account"


@pytest.mark.asyncio
async def test_manual_seed_candidate_ids_include_seed_id(service_session):
    channel, lane, first_account, lane_format = await _channel_graph(service_session, dry_run=True)
    second_account = PublishingAccount(
        channel_profile_id=channel.id,
        account_label="second",
        platform_account_id="yt-2",
        credential_ref="youtube/second",
    )
    first_seed = ManualSeed(channel_profile_id=channel.id, topic_lane_id=lane.id, prompt="first manual")
    second_seed = ManualSeed(channel_profile_id=channel.id, topic_lane_id=lane.id, prompt="second manual")
    service_session.add_all([second_account, first_seed, second_seed])
    await service_session.commit()

    candidates = await _service()._build_tick_candidates(
        service_session,
        channel=channel,
        lanes=[lane],
        accounts=[first_account, second_account],
        seeds=[first_seed, second_seed],
        lane_formats_by_lane={str(lane.id): [lane_format]},
        bucket="2026-05-18-09",
    )

    manual_ids = [candidate["candidate_id"] for candidate in candidates if candidate["source"] == "manual_seed"]
    assert manual_ids == [
        f"manual_seed:{first_seed.id}:lane:{lane.id}:format:{lane_format.id}:2026-05-18-09",
        f"manual_seed:{second_seed.id}:lane:{lane.id}:format:{lane_format.id}:2026-05-18-09",
    ]


@pytest.mark.asyncio
async def test_plan_task_holds_when_autoflow_omits_youtube_upload(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()

    item = ChannelOpsQueueItem(
        kind="plan_task",
        idempotency_key=f"plan_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(autoflow=FakeAutoFlowClient(include_upload=False)).handle_plan_task(service_session, item)
    await service_session.refresh(task)

    assert task.state == "held"
    assert task.blocked_by_guard == "missing_youtube_upload_node"


@pytest.mark.asyncio
async def test_plan_task_enqueues_execute_with_channel_scope(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()

    item = ChannelOpsQueueItem(
        kind="plan_task",
        idempotency_key=f"plan_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service().handle_plan_task(service_session, item)

    execute = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "execute_task")
        )
    ).scalar_one()
    assert execute.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_execute_task_failed_observation_marks_task_failed_without_observe(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="planning",
        autoflow_plan_id=uuid.uuid4(),
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="execute_task",
        idempotency_key=f"execute_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(autoflow=FailedExecutionAutoFlowClient()).handle_execute_task(service_session, item)
    await service_session.refresh(task)

    assert task.state == "failed"
    assert "review approval is required" in task.failure_reason
    observe_items = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "observe_job")
        )
    ).scalars().all()
    assert observe_items == []


@pytest.mark.asyncio
async def test_execute_task_incomplete_success_observation_marks_failed_without_observe(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="planning",
        autoflow_plan_id=uuid.uuid4(),
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="execute_task",
        idempotency_key=f"execute_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(autoflow=IncompleteExecutionAutoFlowClient()).handle_execute_task(service_session, item)
    await service_session.refresh(task)

    assert task.state == "failed"
    assert "job_id" in task.failure_reason
    observe_items = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "observe_job")
        )
    ).scalars().all()
    assert observe_items == []


@pytest.mark.asyncio
async def test_execute_task_reuses_existing_run_and_job_without_reexecuting(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    run_id = uuid.uuid4()
    job_id = uuid.uuid4()
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="planning",
        autoflow_plan_id=uuid.uuid4(),
        autoflow_run_id=run_id,
        job_id=job_id,
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="execute_task",
        idempotency_key=f"execute_task:{task.id}",
        payload_json={"production_task_id": str(task.id)},
    )
    service_session.add(item)
    await service_session.commit()
    client = CountingExecuteAutoFlowClient()

    await _service(autoflow=client).handle_execute_task(service_session, item)
    await service_session.refresh(task)

    assert client.execute_calls == 0
    assert task.state == "producing"
    observe = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "observe_job")
        )
    ).scalar_one()
    assert observe.idempotency_key == f"observe_job:{task.id}:{run_id}:{job_id}:0"
    assert observe.payload_json["run_id"] == str(run_id)
    assert observe.payload_json["job_id"] == str(job_id)


@pytest.mark.asyncio
async def test_observe_job_max_count_holds_without_requeue(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    run_id = uuid.uuid4()
    job_id = uuid.uuid4()
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="producing",
        autoflow_run_id=run_id,
        job_id=job_id,
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="observe_job",
        idempotency_key=f"observe_job:{task.id}:{run_id}:{job_id}:20",
        payload_json={
            "production_task_id": str(task.id),
            "run_id": str(run_id),
            "job_id": str(job_id),
            "observe_count": 20,
        },
    )
    service_session.add(item)
    await service_session.commit()

    await _service(autoflow=AlwaysRunningAutoFlowClient()).handle_observe_job(service_session, item)
    await service_session.refresh(task)

    assert task.state == "held"
    assert task.blocked_by_guard == "autoflow_observe_timeout"
    observe_items = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "observe_job")
        )
    ).scalars().all()
    assert observe_items == [item]


@pytest.mark.asyncio
async def test_observe_job_backoff_uses_observe_count_multiplier(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    run_id = uuid.uuid4()
    job_id = uuid.uuid4()
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="producing",
        autoflow_run_id=run_id,
        job_id=job_id,
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="observe_job",
        idempotency_key=f"observe_job:{task.id}:{run_id}:{job_id}:1",
        payload_json={
            "production_task_id": str(task.id),
            "run_id": str(run_id),
            "job_id": str(job_id),
            "observe_count": 1,
        },
    )
    service_session.add(item)
    await service_session.commit()

    await _service(clock=clock, autoflow=AlwaysRunningAutoFlowClient()).handle_observe_job(service_session, item)

    requeued = (
        await service_session.execute(
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.kind == "observe_job")
            .where(ChannelOpsQueueItem.id != item.id)
        )
    ).scalar_one()
    assert requeued.payload_json["observe_count"] == 2
    assert _as_utc(requeued.run_after) == clock.now() + timedelta(seconds=60)


@pytest.mark.asyncio
async def test_publish_task_observes_upload_and_auto_enqueues_promote(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key=f"publish_task:{task.id}",
        payload_json={"production_task_id": str(task.id), "youtube": {"video_id": "yt-video-1"}},
    )
    service_session.add(item)
    await service_session.commit()

    publication = await _service().handle_publish_task(service_session, item)

    assert publication.platform_content_id == "yt-video-1"
    assert publication.current_privacy == "private"
    assert publication.publish_status == "uploaded"
    promote = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "promote_publication")
        )
    ).scalar_one()
    assert str(publication.id) in promote.idempotency_key
    assert promote.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_publish_task_holds_when_external_platforms_come_from_snapshot(service_session):
    channel, lane, account, _lane_format = await _channel_graph(
        service_session,
        dry_run=False,
        external_auto=False,
    )
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        source_platforms_json=[],
        uses_external_assets=False,
        state="uploaded_private",
        channel_config_snapshot_json={"lane_format": {"source_platforms_json": ["bilibili"]}},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key=f"publish_task:{task.id}",
        payload_json={"production_task_id": str(task.id), "youtube": {"video_id": "yt-video-1"}},
    )
    service_session.add(item)
    await service_session.commit()

    publication = await _service().handle_publish_task(service_session, item)
    await service_session.refresh(task)

    assert publication is not None
    assert task.state == "held"
    assert task.blocked_by_guard == "external_asset_auto_publish_required"
    assert publication.publish_status == "held"


@pytest.mark.asyncio
async def test_publish_task_writes_material_usage_ledger_from_upload_metadata(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        title_seed="test",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key=f"publish_task:{task.id}",
        payload_json={
            "production_task_id": str(task.id),
            "youtube": {
                "video_id": "yt-video-1",
                "material_refs": [
                    {
                        "material_id": "mat-1",
                        "asset_id": str(uuid.uuid4()),
                        "start_ms": 1000,
                        "end_ms": 5000,
                    }
                ],
            },
        },
    )
    service_session.add(item)
    await service_session.commit()

    publication = await _service().handle_publish_task(service_session, item)

    ledger = (await service_session.execute(select(MaterialUsageLedger))).scalar_one()
    assert ledger.publication_id == publication.id
    assert ledger.material_id == "mat-1"
    assert ledger.topic_lane_id == lane.id
    assert ledger.publishing_account_id == account.id
    assert ledger.segment_signature


@pytest.mark.asyncio
async def test_publish_task_holds_lane_task_when_material_recently_used(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, task, item, material_id = await _publish_task_with_material(
        service_session,
        source="lane_seed",
    )
    service_session.add(
        MaterialUsageLedger(
            material_id=material_id,
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            publishing_account_id=account.id,
            segment_signature=segment_signature(material_id, 0, 1000),
            used_at=clock.now() - timedelta(minutes=5),
        )
    )
    await service_session.commit()

    publication = await _service(clock=clock).handle_publish_task(service_session, item)
    await service_session.refresh(task)
    publications = (await service_session.execute(select(PublicationRecord))).scalars().all()

    assert publication is None
    assert publications == []
    assert task.state == "held"
    assert task.blocked_by_guard == "repetition_rejected"
    assert "publish-time material usage guard" in task.failure_reason
    assert task.rationale_json["material_usage_guard"]["repetition_rejected"] is True


@pytest.mark.asyncio
async def test_publish_task_manual_seed_repetition_override_is_recorded(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, task, item, material_id = await _publish_task_with_material(
        service_session,
        source="manual_seed",
    )
    service_session.add(
        MaterialUsageLedger(
            material_id=material_id,
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            publishing_account_id=account.id,
            segment_signature=segment_signature(material_id, 0, 1000),
            used_at=clock.now() - timedelta(minutes=5),
        )
    )
    await service_session.commit()

    publication = await _service(clock=clock).handle_publish_task(service_session, item)
    await service_session.refresh(task)

    assert publication is not None
    assert publication.platform_content_id == "yt-manual_seed"
    assert task.rationale_json["material_usage_guard"]["manual_override"] is True
    assert task.rationale_json["material_usage_guard"]["repetition_rejected"] is True


@pytest.mark.asyncio
async def test_lane_candidate_rejects_recent_material_usage(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        MaterialUsageLedger(
            material_id="mat-1",
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            publishing_account_id=account.id,
            segment_signature="existing-segment",
            used_at=clock.now() - timedelta(days=1),
        )
    )
    await service_session.commit()
    candidate = {
        "candidate_id": "lane-candidate",
        "source": "lane_seed",
        "seed": None,
        "lane": lane,
        "lane_format": lane_format,
        "account": account,
        "prompt": "lane prompt",
        "title_seed": "lane",
        "constraints_json": {
            "material_refs": [
                {"material_id": "mat-1", "segment_signature": "existing-segment"},
            ]
        },
    }

    rejection = await _service(clock=clock)._evaluate_candidate_guards(
        service_session,
        candidate,
        {},
        {},
        enqueue_alerts=False,
        pds_enabled=False,
    )

    assert rejection is not None
    assert rejection["guard"] == "repetition_rejected"


@pytest.mark.asyncio
async def test_manual_seed_repetition_override_is_annotated(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    service_session.add(
        MaterialUsageLedger(
            material_id="mat-1",
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            publishing_account_id=account.id,
            segment_signature="existing-segment",
            used_at=clock.now() - timedelta(days=1),
        )
    )
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="manual repeat",
            title_seed="repeat",
            constraints_json={
                "material_refs": [
                    {"material_id": "mat-1", "segment_signature": "existing-segment"},
                ]
            },
        )
    )
    await service_session.commit()

    audit = await _service(clock=clock).tick(service_session, channel_id=channel.id)

    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert audit.tasks_selected == 1
    assert task.rationale_json["material_usage_guard"]["manual_override"] is True
    assert task.rationale_json["material_usage_guard"]["hits"][0]["guard"] == "repetition_rejected"


@pytest.mark.asyncio
async def test_pds_unavailable_decision_enqueues_outage_alert(service_session):
    _channel, task, _publication, item = await _promotion_item_graph(service_session)
    service = _service(
        pds=SequencePDSClient(
            [
                PDSDecision(
                    decision_id="",
                    verdict="block",
                    metadata={"warning": "pds_unavailable", "fail_policy": "block"},
                )
            ]
        ),
        pds_health_monitor_enabled=True,
    )

    await service.handle_promote_publication(service_session, item)
    await service_session.refresh(task)

    assert task.state == "held"
    alert = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalar_one()
    assert alert.payload_json["type"] == "pds_outage"
    assert alert.payload_json["resource_id"] == "service:pds"
    assert alert.payload_json["severity"] == "critical"
    assert alert.payload_json["details"]["action_type"] == "publish"
    assert alert.payload_json["details"]["warning"] == "pds_unavailable"


@pytest.mark.asyncio
async def test_pds_outage_alert_is_deduped_per_hour(service_session):
    _first_channel, _first_task, _first_publication, first_item = await _promotion_item_graph(service_session)
    _second_channel, _second_task, _second_publication, second_item = await _promotion_item_graph(service_session)
    service = _service(
        pds=SequencePDSClient(
            [
                PDSDecision(
                    decision_id="",
                    verdict="block",
                    metadata={"warning": "pds_unavailable", "fail_policy": "block"},
                )
            ]
        ),
        pds_health_monitor_enabled=True,
    )

    await service.handle_promote_publication(service_session, first_item)
    await service.handle_promote_publication(service_session, second_item)

    alerts = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalars().all()
    assert len(alerts) == 1
    assert alerts[0].payload_json["type"] == "pds_outage"


@pytest.mark.asyncio
async def test_promote_publication_schedules_youtube_publish_at(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id="yt-video-1",
        title="test",
        desired_privacy="public",
        current_privacy="private",
        publish_status="uploaded",
        compliance_disposition="assumed_fair_use",
    )
    service_session.add(publication)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:public:2026-05-18T10:00:00+00:00",
        payload_json={"publication_id": str(publication.id), "scheduled_at": "2026-05-18T10:00:00+00:00"},
    )
    service_session.add(item)
    await service_session.commit()
    youtube = FakeYouTubeClient()
    pds = FakePDSClient(
        PDSDecision(
            decision_id="decision-allow",
            verdict="allow",
            score=0.12,
            reasons=[{"code": "low_risk"}],
            rules_version="risk-v2",
            metadata={"warning": "feature_cache_miss"},
        )
    )

    await _service(youtube=youtube, pds=pds).handle_promote_publication(service_session, item)
    await service_session.refresh(publication)

    assert publication.publish_status == "scheduled"
    assert publication.scheduled_publish_at is not None
    assert youtube.scheduled[0]["video_id"] == "yt-video-1"
    assert pds.requests
    assert pds.requests[0].action_type == "publish"
    metrics_item = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "collect_metrics")
        )
    ).scalar_one()
    assert _as_utc(metrics_item.run_after) == datetime(2026, 5, 18, 11, 0, tzinfo=timezone.utc)
    payloads = await _outbox_payloads(service_session)
    assert [payload["action_type"] for payload in payloads] == [
        "publication_promotion_attempted",
        "publication_scheduled",
    ]
    assert payloads[1]["metadata"]["publication_id"] == str(publication.id)
    assert payloads[1]["metadata"]["decision_id"] == "decision-allow"
    assert payloads[1]["metadata"]["score"] == 0.12
    assert payloads[1]["metadata"]["rules_version"] == "risk-v2"
    assert payloads[1]["metadata"]["reason_codes"] == ["low_risk"]
    assert payloads[1]["metadata"]["warning"] == "feature_cache_miss"


@pytest.mark.asyncio
async def test_promote_publication_holds_when_pds_blocks(service_session):
    channel, _lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="publish risky video",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = _publication_for_task(task, account, publish_status="uploaded", title="risky")
    service_session.add(publication)
    await service_session.flush()
    item = ChannelOpsQueueItem(
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:unlisted",
        payload_json={"publication_id": str(publication.id), "target_visibility": "unlisted"},
    )
    service_session.add(item)
    await service_session.commit()
    pds = FakePDSClient(
        PDSDecision(
            decision_id="decision-block",
            verdict="block",
            score=0.94,
            reasons=[{"code": "burst"}],
            rules_version="risk-v3",
            metadata={"warning": "aggregator_stale"},
        )
    )
    youtube = FakeYouTubeClient()

    result = await _service(youtube=youtube, pds=pds).handle_promote_publication(service_session, item)
    await service_session.refresh(task)

    assert result.publish_status == "held"
    assert task.state == "held"
    assert "pds_blocked:decision-block" in list(result.warnings_json or [])
    assert youtube.scheduled == []
    assert pds.requests
    assert pds.requests[0].action_type == "publish"
    payloads = await _outbox_payloads(service_session)
    assert [payload["action_type"] for payload in payloads] == [
        "publication_promotion_attempted",
        "publication_promotion_blocked",
    ]
    assert payloads[1]["metadata"]["decision_id"] == "decision-block"
    assert payloads[1]["metadata"]["verdict"] == "block"
    assert payloads[1]["metadata"]["score"] == 0.94
    assert payloads[1]["metadata"]["rules_version"] == "risk-v3"
    assert payloads[1]["metadata"]["reason_codes"] == ["burst"]
    assert payloads[1]["metadata"]["warning"] == "aggregator_stale"


@pytest.mark.asyncio
async def test_handle_promote_publication_ignores_rejected_publication(service_session):
    channel, _lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="rejected",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id="yt-video-1",
        title="test",
        desired_privacy="unlisted",
        current_privacy="private",
        publish_status="rejected",
        compliance_disposition="assumed_fair_use",
    )
    service_session.add(publication)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="promote_publication",
        idempotency_key=f"promote_publication:{publication.id}:unlisted:2026-05-18T10:00:00+00:00",
        payload_json={
            "publication_id": str(publication.id),
            "scheduled_at": "2026-05-18T10:00:00+00:00",
            "target_visibility": "unlisted",
        },
    )
    service_session.add(item)
    await service_session.commit()
    youtube = FakeYouTubeClient()

    result = await _service(youtube=youtube).handle_promote_publication(service_session, item)
    await service_session.refresh(task)
    await service_session.refresh(publication)

    assert result.id == publication.id
    assert youtube.scheduled == []
    assert publication.publish_status == "rejected"
    assert task.state == "rejected"
    metrics_items = (
        await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "collect_metrics"))
    ).scalars().all()
    assert metrics_items == []


@pytest.mark.asyncio
async def test_reconcile_publication_updates_status_from_youtube(service_session):
    channel, _lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = _publication_for_task(task, account, publish_status="scheduled", title="scheduled")
    publication.platform_content_id = "yt-video-1"
    service_session.add(publication)
    await service_session.flush()
    item = ChannelOpsQueueItem(
        kind="reconcile_publication",
        idempotency_key=f"reconcile_publication:{publication.id}",
        payload_json={"publication_id": str(publication.id)},
    )
    service_session.add(item)
    await service_session.commit()
    youtube = FakeYouTubeClient(
        status_by_video={
            "yt-video-1": {
                "privacy": "private",
                "processing_state": "processed",
                "publish_status": "processed",
                "permalink": "https://youtu.be/yt-video-1",
            }
        }
    )

    result = await _service(youtube=youtube).handle_reconcile_publication(service_session, item)
    await service_session.refresh(task)

    assert result.current_privacy == "private"
    assert result.publish_status == "processed"
    assert result.permalink == "https://youtu.be/yt-video-1"
    assert task.failure_reason is None


@pytest.mark.asyncio
async def test_collect_metrics_without_metrics_requeues_without_snapshot_or_measured(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = _publication_for_task(
        task,
        account,
        publish_status="scheduled",
        scheduled_publish_at=clock.now() - timedelta(minutes=5),
    )
    service_session.add(publication)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key=f"collect_metrics:{publication.id}:poll:0",
        payload_json={"publication_id": str(publication.id)},
        channel_profile_id=channel.id,
    )
    service_session.add(item)
    await service_session.commit()

    snapshot = await _service(clock=clock).handle_collect_metrics(service_session, item)
    await service_session.refresh(task)

    assert snapshot is None
    assert task.state == "scheduled"
    snapshots = (await service_session.execute(select(FeedbackSnapshot))).scalars().all()
    assert snapshots == []
    requeued = (
        await service_session.execute(
            select(ChannelOpsQueueItem)
            .where(ChannelOpsQueueItem.kind == "collect_metrics")
            .where(ChannelOpsQueueItem.id != item.id)
        )
    ).scalar_one()
    assert requeued.payload_json == {
        "publication_id": str(publication.id),
        "metrics_poll_count": 1,
    }
    assert _as_utc(requeued.run_after) == clock.now() + timedelta(hours=1)
    assert requeued.channel_profile_id == channel.id
    assert requeued.parent_queue_item_id == item.id


@pytest.mark.asyncio
async def test_collect_metrics_fetches_metrics_from_youtube_when_payload_has_none(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id="yt-live-1",
        title="metrics",
        desired_privacy="private",
        current_privacy="private",
        publish_status="scheduled",
        scheduled_publish_at=clock.now() - timedelta(hours=1),
        compliance_disposition="assumed_fair_use",
    )
    service_session.add(publication)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key=f"collect_metrics:{publication.id}:poll:0",
        payload_json={"publication_id": str(publication.id), "metrics_poll_count": 0},
        channel_profile_id=channel.id,
    )
    service_session.add(item)
    await service_session.commit()
    youtube = FakeYouTubeClient(metrics_by_video={"yt-live-1": {"views": 321, "likes": 17, "comments": 4}})

    snapshot = await _service(clock=clock, youtube=youtube).handle_collect_metrics(service_session, item)
    await service_session.refresh(task)

    assert snapshot is not None
    assert snapshot.views == 321
    assert snapshot.likes == 17
    assert snapshot.comments == 4
    assert task.state == "measured"
    collect_items = (
        await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "collect_metrics"))
    ).scalars().all()
    assert collect_items == [item]


@pytest.mark.asyncio
async def test_collect_metrics_max_poll_count_holds_without_snapshot_or_requeue(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = _publication_for_task(
        task,
        account,
        publish_status="scheduled",
        scheduled_publish_at=clock.now() - timedelta(days=1),
    )
    service_session.add(publication)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key=f"collect_metrics:{publication.id}:poll:23",
        payload_json={"publication_id": str(publication.id), "metrics_poll_count": 23},
        channel_profile_id=channel.id,
    )
    service_session.add(item)
    await service_session.commit()

    snapshot = await _service(clock=clock).handle_collect_metrics(service_session, item)
    await service_session.refresh(task)

    assert snapshot is None
    assert task.state == "held"
    assert task.blocked_by_guard == "metrics_unavailable"
    snapshots = (await service_session.execute(select(FeedbackSnapshot))).scalars().all()
    assert snapshots == []
    collect_items = (
        await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "collect_metrics"))
    ).scalars().all()
    assert collect_items == [item]


@pytest.mark.asyncio
async def test_collect_metrics_ignores_rejected_publication_without_snapshot_or_requeue(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="rejected",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = _publication_for_task(
        task,
        account,
        publish_status="rejected",
        scheduled_publish_at=clock.now() - timedelta(days=1),
    )
    service_session.add(publication)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key=f"collect_metrics:{publication.id}:poll:0",
        payload_json={
            "publication_id": str(publication.id),
            "metrics_poll_count": 0,
            "metrics": {"views": 100, "likes": 10},
        },
        channel_profile_id=channel.id,
    )
    service_session.add(item)
    await service_session.commit()

    snapshot = await _service(clock=clock).handle_collect_metrics(service_session, item)
    await service_session.refresh(task)
    await service_session.refresh(publication)

    assert snapshot is None
    assert task.state == "rejected"
    assert publication.publish_status == "rejected"
    snapshots = (await service_session.execute(select(FeedbackSnapshot))).scalars().all()
    assert snapshots == []
    collect_items = (
        await service_session.execute(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "collect_metrics"))
    ).scalars().all()
    assert collect_items == [item]


@pytest.mark.asyncio
async def test_collect_metrics_updates_existing_publication_snapshot(service_session):
    clock = FakeClock(datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        topic_lane_id=lane.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="scheduled",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = _publication_for_task(
        task,
        account,
        publish_status="scheduled",
        scheduled_publish_at=clock.now() - timedelta(hours=1),
    )
    service_session.add(publication)
    await service_session.flush()
    existing = FeedbackSnapshot(
        publication_id=publication.id,
        collected_at=clock.now() - timedelta(hours=1),
        views=10,
        likes=1,
        raw_json={"publication_id": str(publication.id), "metrics": {"views": 10}},
    )
    service_session.add(existing)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="collect_metrics",
        idempotency_key=f"collect_metrics:{publication.id}:poll:1",
        payload_json={
            "publication_id": str(publication.id),
            "metrics_poll_count": 1,
            "metrics": {"views": 125, "likes": 12, "avg_view_duration_sec": 9.5},
        },
        channel_profile_id=channel.id,
    )
    service_session.add(item)
    await service_session.commit()

    snapshot = await _service(clock=clock).handle_collect_metrics(service_session, item)

    snapshots = (await service_session.execute(select(FeedbackSnapshot))).scalars().all()
    assert snapshot is not None
    assert snapshot.id == existing.id
    assert len(snapshots) == 1
    assert snapshots[0].views == 125
    assert snapshots[0].likes == 12
    assert snapshots[0].avg_view_duration_sec == 9.5
    assert _as_utc(snapshots[0].collected_at) == clock.now()


@pytest.mark.asyncio
async def test_quota_exhaustion_holds_publish_and_alerts(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="uploaded_private",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.commit()
    item = ChannelOpsQueueItem(
        kind="publish_task",
        idempotency_key=f"publish_task:{task.id}",
        payload_json={"production_task_id": str(task.id), "youtube": {"video_id": "yt-video-1"}},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(youtube=FakeYouTubeClient(quota_remaining_fraction=0.05)).handle_publish_task(
        service_session,
        item,
    )
    await service_session.refresh(task)

    assert task.state == "held"
    alert = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalar_one()
    assert alert.payload_json["type"] == "quota_below_20pct"
    assert alert.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_token_refresh_failure_pauses_account_and_alerts(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    item = ChannelOpsQueueItem(
        kind="account_health",
        idempotency_key=f"account_health:{account.id}:2026-05-18-09",
        payload_json={"account_id": str(account.id)},
    )
    service_session.add(item)
    await service_session.commit()

    await _service(youtube=FakeYouTubeClient(token_valid=False)).handle_account_health(service_session, item)
    await service_session.refresh(account)

    assert account.enabled is False
    assert account.last_token_check_status == "invalid"
    alert = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalar_one()
    assert alert.payload_json["type"] == "token_expiring_24h"
    assert alert.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_severe_takedown_pauses_account_and_alerts(service_session):
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    task = ProductionTask(
        channel_profile_id=channel.id,
        target_account_id=account.id,
        source="manual_seed",
        prompt="make a test short",
        state="published",
        channel_config_snapshot_json={},
    )
    service_session.add(task)
    await service_session.flush()
    publication = PublicationRecord(
        production_task_id=task.id,
        platform="youtube",
        account_id=account.id,
        platform_content_id="yt-video-1",
        title="test",
        desired_privacy="public",
        current_privacy="public",
        publish_status="public",
        compliance_disposition="known_risk_accepted",
    )
    service_session.add(publication)
    await service_session.commit()

    event = await _service().log_takedown_event(
        service_session,
        publication_id=publication.id,
        event_type="strike",
        severity="severe",
        raw_payload={"reason": "copyright strike"},
    )
    await service_session.refresh(account)

    assert event.severity == "severe"
    assert account.enabled is False
    alert = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "send_alert")
        )
    ).scalar_one()
    assert alert.payload_json["type"] == "takedown_event_logged"
    assert alert.channel_profile_id == channel.id


@pytest.mark.asyncio
async def test_queue_flow_reaches_scheduled_publication_and_metrics(service_session):
    clock = FakeClock(datetime(2026, 5, 18, 9, 0, tzinfo=timezone.utc))
    channel, lane, account, _lane_format = await _channel_graph(service_session, dry_run=False)
    account.external_asset_auto_publish = True
    service_session.add(
        ManualSeed(
            channel_profile_id=channel.id,
            topic_lane_id=lane.id,
            target_account_id=account.id,
            prompt="make a test short",
            title_seed="test short",
        )
    )
    await service_session.commit()
    queue = ChannelOpsQueueService(clock=clock)
    service = ChannelAgentService(
        queue=queue,
        clock=clock,
        autoflow_client=FakeAutoFlowClient(include_upload=True, youtube_video_id="yt-video-1"),
        youtube_client=FakeYouTubeClient(),
        minimax_client=FakeMiniMaxClient(),
        pds_client=FakePDSClient(PDSDecision(decision_id="allow", verdict="allow")),
    )

    await service.tick(service_session, channel_id=channel.id)
    plan_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_plan_task(service_session, plan_item)
    execute_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_execute_task(service_session, execute_item)
    observe_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_observe_job(service_session, observe_item)
    publish_item = await queue.claim_next(service_session, worker_id="test")
    publication = await service.handle_publish_task(service_session, publish_item)
    promote_item = await queue.claim_next(service_session, worker_id="test")
    await service.handle_promote_publication(service_session, promote_item)
    assert await queue.claim_next(service_session, worker_id="test") is None
    queued_metrics_item = (
        await service_session.execute(
            select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "collect_metrics")
        )
    ).scalar_one()
    assert _as_utc(queued_metrics_item.run_after) == clock.now() + timedelta(hours=2)
    queued_metrics_item.payload_json = {
        **queued_metrics_item.payload_json,
        "metrics": {
            "views": 120,
            "likes": 12,
            "comments": 3,
            "shares": 2,
            "avg_view_duration_sec": 14.5,
        },
    }
    await service_session.commit()
    clock.advance(timedelta(hours=2))
    reconcile_item = await queue.claim_next(service_session, worker_id="test")
    assert reconcile_item.kind == "reconcile_publication"
    await service.handle_reconcile_publication(service_session, reconcile_item)
    metrics_item = await queue.claim_next(service_session, worker_id="test")
    assert metrics_item.kind == "collect_metrics"
    snapshot = await service.handle_collect_metrics(service_session, metrics_item)

    await service_session.refresh(publication)
    task = (await service_session.execute(select(ProductionTask))).scalar_one()
    assert task.state == "measured"
    assert publication.publish_status == "scheduled"
    assert publication.platform_content_id == "yt-video-1"
    assert snapshot.publication_id == publication.id
    assert snapshot.views == 120
