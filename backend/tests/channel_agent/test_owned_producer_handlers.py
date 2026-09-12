from dataclasses import asdict, replace
from copy import deepcopy
import asyncio
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.channel_agent.clients import LocalAutoFlowClient, FakeYouTubeClient
from app.channel_agent.service import ChannelAgentService
from app.models.autoflow import AutoFlowPlan, AutoFlowRun, AutoFlowUsedClip
from app.models.channel_agent import ChannelOpsQueueItem, ChannelProfile, ProductionTask, PublicationRecord, MaterialUsageLedger, PublicationMetricSchedule, PublishingAccount
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.publication_promotion_operation import PublicationPromotionOperation
from app.models.youtube_upload_operation import YouTubeUploadOperation
from app.models.job import Job, NodeExecution
from app.models.artifact import Artifact
from app.services import owned_seed_inventory as inv
from tests.channel_agent.test_owned_inventory import inventory_env as inventory_env, owned_env as owned_env, tick
from tests.services.test_owned_producer_fence import decision


class Policy:
    def __init__(self, action=None):
        self.action, self.calls = action, []

    async def decide(self, request):
        self.calls.append(request)
        if self.action:
            await self.action()
        return decision()


@pytest.fixture
async def plan_queue(owned_env):
    env = owned_env
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            for model in (AutoFlowPlan, AutoFlowRun, AutoFlowUsedClip):
                await conn.run_sync(model.__table__.create)
    await tick(env, Policy())
    async with env.factory() as db:
        queue = (await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "plan_task"))).one()
        queue.status, queue.locked_by, queue.locked_at = "running", "owned-runner", await inv._now(db)
        env.queue_id = queue.id
        env.task_id = (await db.scalars(select(ProductionTask.id))).one()
        await db.commit()
    return env


async def test_owned_plan_pds_outside_transaction_then_atomic_queue_success(plan_queue):
    env = plan_queue
    async with env.factory() as db:
        async def outside():
            assert not db.in_transaction()
        policy = Policy(outside)
        service = ChannelAgentService(pds_client=policy, autoflow_client=LocalAutoFlowClient(session_factory=env.factory))
        task = await service.handle_plan_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        assert queue.status == "succeeded" and queue.locked_by is None and queue.locked_at is None
        evidence = task.agent_approval_evidence_json
        assert evidence["candidate_pds"] and evidence["candidate_pds_request"] and evidence["owned_inventory"]
        assert evidence["plan_pds"] == {"request": asdict(policy.calls[0]), "response": asdict(decision())}
        payload = task.rationale_json["autoflow_plan_payload"]
        execute = (await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "execute_task"))).one()
        assert all(execute.payload_json[k] == payload[k] for k in ("plan_id", "expected_approved_revision", "expected_approved_revision_hash"))
        assert execute.parent_queue_item_id == env.queue_id and task.state == "planning"


async def test_owned_plan_config_change_during_pds_holds_without_approval_or_execute(plan_queue):
    env = plan_queue
    async def revoke():
        async with env.factory() as other:
            (await other.get(OwnedSeedInventory, env.inventory_id)).state = "revoked"
            await other.commit()
    async with env.factory() as db:
        service = ChannelAgentService(pds_client=Policy(revoke), autoflow_client=LocalAutoFlowClient(session_factory=env.factory))
        task = await service.handle_plan_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert task.state == "held" and task.blocked_by_guard == "owned_inventory_producer_inactive"
        assert (await db.get(ChannelProfile, env.channel_id)).intake_paused_at is not None
        assert not list((await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "execute_task"))).all())
        plan = await db.get(AutoFlowPlan, task.autoflow_plan_id)
        assert plan.review_approved_at is None and plan.agent_approved_by is None


async def test_owned_plan_cancellation_never_finalizes_queue_or_holds(plan_queue):
    import asyncio
    env = plan_queue
    async def cancel():
        raise asyncio.CancelledError()
    async with env.factory() as db:
        service = ChannelAgentService(pds_client=Policy(cancel), autoflow_client=LocalAutoFlowClient(session_factory=env.factory))
        with pytest.raises(asyncio.CancelledError):
            await service.handle_plan_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        await db.rollback()
        task = await db.get(ProductionTask, env.task_id)
        assert task.state == "selected" and task.blocked_by_guard is None
        assert (await db.get(ChannelOpsQueueItem, env.queue_id)).status == "running"


@pytest.fixture
async def execute_queue(plan_queue):
    env = plan_queue
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            await conn.run_sync(MaterialUsageLedger.__table__.create)
        service = ChannelAgentService(pds_client=Policy(), autoflow_client=LocalAutoFlowClient(session_factory=env.factory))
        await service.handle_plan_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        queue = (await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "execute_task"))).one()
        queue.status, queue.locked_by, queue.locked_at, queue.attempt_count = "running", "owned-runner", await inv._now(db), 1
        env.queue_id = queue.id
        await db.commit()
    return env


async def stage_queue(env, kind, *, publication=False):
    async with env.factory() as db:
        prior = await db.get(ChannelOpsQueueItem, env.queue_id)
        prior.status, prior.locked_by, prior.locked_at = "succeeded", None, None
        task = await db.get(ProductionTask, env.task_id)
        payload = {"production_task_id": str(task.id)}
        if publication:
            now = await inv._now(db)
            pub = PublicationRecord(production_task_id=task.id, account_id=task.target_account_id,
                platform="youtube", platform_content_id="OWNEDVIDEO1", title=task.title_seed,
                description=task.prompt, current_privacy="unlisted", desired_privacy="unlisted",
                publish_status="uploaded", uploaded_at=now, compliance_disposition="assumed_fair_use")
            task.state = "uploaded_private"
            db.add(pub)
            await db.flush()
            env.publication_id = pub.id
            payload = {"publication_id": str(pub.id), "target_visibility": "unlisted", "scheduled_at": now.isoformat()}
        queue = ChannelOpsQueueItem(kind=kind, idempotency_key=f"{kind}:fixture", payload_json=payload,
            status="running", locked_by="owned-runner", locked_at=await inv._now(db), attempt_count=1,
            channel_profile_id=env.channel_id, parent_queue_item_id=prior.id)
        db.add(queue)
        await db.commit()
        env.queue_id = queue.id
    return env


async def test_owned_execute_passes_exact_internal_binding_outside_transaction(execute_queue):
    env = execute_queue
    async with env.factory() as db:
        item = await db.get(ChannelOpsQueueItem, env.queue_id)
        expected = {"production_task_id": str(env.task_id), "channelops_queue_item_id": str(item.id),
            "channelops_queue_locked_by": item.locked_by, "channelops_queue_locked_at": inv.utc(item.locked_at),
            "expected_approved_revision": item.payload_json["expected_approved_revision"],
            "expected_approved_revision_hash": item.payload_json["expected_approved_revision_hash"],
            "idempotency_key": item.idempotency_key}
        class Client:
            async def execute_task(self, task, request):
                assert not db.in_transaction()
                assert request["_channelops_execute"] == expected
                assert task.id == env.task_id and task.autoflow_plan_id
                raise asyncio.CancelledError()
        with pytest.raises(asyncio.CancelledError):
            await ChannelAgentService(autoflow_client=Client()).handle_execute_task(db, item)
        await db.rollback()
        assert (await db.get(ChannelOpsQueueItem, env.queue_id)).status == "running"


async def test_owned_publish_rejects_queue_video_without_durable_receipt(execute_queue):
    env = await stage_queue(execute_queue, "publish_task")
    async with env.factory() as db:
        item = await db.get(ChannelOpsQueueItem, env.queue_id)
        item.payload_json = {**item.payload_json, "youtube": {"video_id": "OWNEDVIDEO1"}}
        await db.commit()
        await ChannelAgentService().handle_publish_task(db, item)
        task = await db.get(ProductionTask, env.task_id)
        assert task.state == "held" and task.blocked_by_guard == "owned_inventory_receipt"
        assert not list((await db.scalars(select(PublicationRecord))).all())


async def test_owned_promotion_rechecks_revocation_after_real_pds(upload_queue):
    env = upload_queue
    async with env.factory() as db:
        await ChannelAgentService().handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
    await claim_stage(env, "promote_publication")
    calls = []
    async with env.factory() as db:
        async def revoke():
            assert not db.in_transaction()
            async with env.factory() as other:
                (await other.get(OwnedSeedInventory, env.inventory_id)).state = "revoked"
                await other.commit()
        class YouTube(FakeYouTubeClient):
            async def schedule_publish(self, **kwargs):
                calls.append(kwargs)
                return {}
        await ChannelAgentService(pds_client=Policy(revoke), youtube_client=YouTube()).handle_promote_publication(
            db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert not calls
        assert (await db.get(ProductionTask, env.task_id)).blocked_by_guard == "owned_inventory_producer_inactive"


async def test_owned_reconcile_expired_original_binding_completes_queue_before_hook(execute_queue, monkeypatch):
    from app.services import owned_inventory_feedback
    env = await stage_queue(execute_queue, "reconcile_publication", publication=True)
    calls = []
    async with env.factory() as db:
        row = await db.get(OwnedSeedInventory, env.inventory_id)
        row.expires_at = await inv._now(db) - timedelta(seconds=1)
        channel = await db.get(ChannelProfile, env.channel_id)
        channel.owned_seed_inventory_id = None
        channel.intake_paused_at, channel.intake_pause_reason = await inv._now(db), "owned_inventory_expired"
        await db.commit()
        async def finalize(session, channel_id, *, publication_id):
            assert session is db and db.in_transaction()
            queue = await db.get(ChannelOpsQueueItem, env.queue_id)
            assert queue.status == "succeeded" and queue.locked_by is None and queue.locked_at is None
            assert channel_id == env.channel_id and publication_id == env.publication_id
            calls.append(publication_id)
            return ()
        monkeypatch.setattr(owned_inventory_feedback, "finalize_owned_inventory_items", finalize)
        class YouTube(FakeYouTubeClient):
            async def fetch_status(self, **kwargs):
                assert not db.in_transaction()
                return {"video_id": "OWNEDVIDEO1", "privacy": "unlisted", "publish_status": "scheduled"}
        pub = await ChannelAgentService(youtube_client=YouTube()).handle_reconcile_publication(
            db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert pub.current_privacy == "unlisted" and calls == [env.publication_id]
        assert db.info["owned_tick_lease"].id == env.queue_id


@pytest.fixture
async def upload_queue(execute_queue, monkeypatch):
    from tests.channel_agent import test_owned_inventory as fixtures
    env = await stage_queue(execute_queue, "publish_task")
    monkeypatch.setattr(fixtures, "SQLITE_TABLES", fixtures.SQLITE_TABLES | {"artifacts"})
    async with env.factory() as db:
        async with db.bind.begin() as conn:
            await conn.run_sync(Artifact.__table__.create)
        now = await inv._now(db)
        task = await db.get(ProductionTask, env.task_id)
        job_id, node_id, input_id, output_id = [uuid.uuid4() for _ in range(4)]
        task.job_id, task.pipeline_id, task.state = job_id, uuid.uuid4(), "producing"
        receipt = {"video_id": "OWNEDVIDEO1", "url": "https://www.youtube.com/watch?v=OWNEDVIDEO1",
            "title": task.title_seed, "privacy": "unlisted", "tags": [], "quota_estimate": 1600}
        db.add(Job(id=job_id, pipeline_id=task.pipeline_id, pipeline_snapshot={}, status="SUCCEEDED", completed_at=now))
        db.add(NodeExecution(id=node_id, job_id=job_id, node_id="upload", node_type="youtube_upload", status="SUCCEEDED",
            input_artifact_ids=[str(input_id)], output_artifact_id=output_id, completed_at=now))
        db.add(Artifact(id=output_id, job_id=job_id, node_execution_id=node_id, filename="youtube.json", storage_path="fixture/output",
            media_info={"youtube": receipt}))
        db.add(YouTubeUploadOperation(production_task_id=task.id, job_id=job_id, node_execution_id=node_id,
            input_artifact_id=input_id, content_sha256="a" * 64, title=task.title_seed, privacy="unlisted", status="succeeded",
            manager_task_id=str(uuid.uuid4()), platform_video_id="OWNEDVIDEO1", receipt_json=receipt,
            request_attempted_at=now, completed_at=now))
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        queue.payload_json = {**queue.payload_json, "youtube": receipt}
        for prior in (await db.scalars(select(ChannelOpsQueueItem))).all():
            prior.attempt_count = 1
        await db.commit()
    return env


async def claim_stage(env, kind):
    async with env.factory() as db:
        queue = (await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == kind))).one()
        queue.status, queue.locked_by, queue.locked_at, queue.attempt_count = "running", "owned-runner", await inv._now(db), 1
        await db.commit()
        env.queue_id = queue.id


@pytest.mark.parametrize("microseconds", [0, 123456])
async def test_owned_normal_publish_promote_reconcile_settles_real_rows(upload_queue, monkeypatch, microseconds):
    env = upload_queue
    async with env.factory() as db:
        now = (await inv._now(db)).replace(microsecond=microseconds)
    async def publication_clock(_db):
        return now
    monkeypatch.setattr(inv, "_now", publication_clock)
    async with env.factory() as db:
        service = ChannelAgentService(pds_client=Policy(), youtube_client=FakeYouTubeClient(
            status_by_video={"OWNEDVIDEO1": {"privacy": "unlisted", "publish_status": "scheduled"}}))
        pub = await service.handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert pub is not None and pub.current_privacy == "unlisted"
        pub_id = pub.id
    await claim_stage(env, "promote_publication")
    async with env.factory() as db:
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        due = inv.utc(queue.run_after)
    async def clock(_db):
        return due
    monkeypatch.setattr(inv, "_now", clock)
    async with env.factory() as db:
        pub = await service.handle_promote_publication(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert pub.publish_status == "scheduled"
        assert len(service.youtube_client.scheduled) == 1
        schedules = (await db.scalars(select(PublicationMetricSchedule))).all()
        assert {s.snapshot_stage for s in schedules} == {"1h", "6h", "24h", "72h", "7d"}
        assert all(s.status == "pending" and s.attempt_count == 0 for s in schedules)
        assert (await db.scalars(select(PublicationPromotionOperation.status))).one() == "finalized"
        task = await db.get(ProductionTask, env.task_id)
        operation = (await db.scalars(select(PublicationPromotionOperation))).one()
        assert operation.decision_json == asdict(decision())
        assert task.agent_approval_evidence_json["promotion_pds"] == {
            "request": {"actor_id": str(pub.account_id), "action_type": "publish", "platform": "youtube",
                "content": {"title": pub.title, "description": pub.description or ""},
                "context": {"publication_id": str(pub.id), "production_task_id": str(task.id),
                    "target_visibility": "unlisted", "owned_inventory": task.agent_approval_evidence_json["owned_inventory"]}},
            "response": asdict(decision()),
        }
    await claim_stage(env, "reconcile_publication")
    async with env.factory() as db:
        await service.handle_reconcile_publication(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        member = (await db.scalars(select(OwnedSeedInventoryItem).where(OwnedSeedInventoryItem.production_task_id == env.task_id))).one()
        assert member.state == "completed", (await db.get(OwnedSeedInventory, env.inventory_id)).hold_reason
        assert (await db.get(PublicationRecord, pub_id)).public_at is None
        assert not (await db.get(ProductionTask, env.task_id)).blocked_by_guard


@pytest.mark.parametrize("state", ["submitting", "uncertain", "confirmed"])
async def test_owned_promotion_resume_after_expiry_never_posts(execute_queue, state):
    env = await stage_queue(execute_queue, "promote_publication", publication=True)
    async with env.factory() as db:
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        row = await db.get(OwnedSeedInventory, env.inventory_id)
        row.state = "revoked"
        scheduled = inv.utc(datetime.fromisoformat(queue.payload_json["scheduled_at"]))
        db.add(PublicationPromotionOperation(publication_id=env.publication_id, production_task_id=env.task_id,
            queue_item_id=env.queue_id, platform_video_id="OWNEDVIDEO1", target_privacy="unlisted", scheduled_at=scheduled,
            attempt_key=str(uuid.uuid4()), status=state, request_attempted_at=scheduled,
            observed_privacy="unlisted" if state == "confirmed" else None,
            observed_publish_status="scheduled" if state == "confirmed" else None))
        await db.commit()
        class YouTube(FakeYouTubeClient):
            async def schedule_publish(self, **kwargs):
                pytest.fail("an attempted operation must never POST again")
            async def fetch_status(self, **kwargs):
                assert not db.in_transaction()
                return {"video_id": "OWNEDVIDEO1", "privacy": "unlisted", "publish_status": "scheduled"}
        policy = Policy()
        pub = await ChannelAgentService(pds_client=policy, youtube_client=YouTube()).handle_promote_publication(db, queue)
        assert pub.publish_status == "scheduled" and not policy.calls
        assert (await db.get(OwnedSeedInventory, env.inventory_id)).state == "revoked"


async def test_owned_reconcile_lost_lease_cannot_finish_or_settle(execute_queue):
    from app.channel_agent.owned_producer import QueueAuthorityLost
    env = await stage_queue(execute_queue, "reconcile_publication", publication=True)
    async with env.factory() as db:
        class YouTube(FakeYouTubeClient):
            async def fetch_status(self, **kwargs):
                assert not db.in_transaction()
                async with env.factory() as other:
                    (await other.get(ChannelOpsQueueItem, env.queue_id)).locked_by = "replacement"
                    await other.commit()
                return {"privacy": "unlisted", "publish_status": "scheduled"}
        with pytest.raises(QueueAuthorityLost):
            await ChannelAgentService(youtube_client=YouTube()).handle_reconcile_publication(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert (await db.get(ChannelOpsQueueItem, env.queue_id)).status == "running"
        assert (await db.get(ProductionTask, env.task_id)).state == "uploaded_private"


@pytest.mark.parametrize("kind", ["execute_task", "publish_task", "promote_publication", "reconcile_publication"])
async def test_ordinary_manual_without_item_still_enters_history_fence(execute_queue, monkeypatch, kind):
    from app.channel_agent import owned_producer
    env = execute_queue if kind == "execute_task" else await stage_queue(execute_queue, kind, publication="publication" in kind)
    calls = []
    async def deny(db, lease, task_id):
        calls.append((lease.kind, task_id))
        raise inv.OwnedInventoryError("owned_history_unclassified")
    monkeypatch.setattr(owned_producer, "phase", deny)
    async with env.factory() as db:
        member = (await db.scalars(select(OwnedSeedInventoryItem).where(OwnedSeedInventoryItem.production_task_id == env.task_id))).one()
        member.production_task_id, member.state, member.consumed_at = None, "unused", None
        (await db.get(ChannelProfile, env.channel_id)).owned_seed_inventory_id = None
        task = await db.get(ProductionTask, env.task_id)
        task.agent_approval_evidence_json, task.channel_config_snapshot_json = {}, {}
        task.approval_mode = "human"
        await db.commit()
        await getattr(ChannelAgentService(), f"handle_{kind}")(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert calls == [(kind, env.task_id)]
        assert (await db.get(ProductionTask, env.task_id)).blocked_by_guard == "owned_history_unclassified"


async def test_owned_promotion_rejects_publication_without_upload_proof(execute_queue):
    env = await stage_queue(execute_queue, "promote_publication", publication=True)
    youtube, policy = FakeYouTubeClient(), Policy()
    async with env.factory() as db:
        await ChannelAgentService(pds_client=policy, youtube_client=youtube).handle_promote_publication(
            db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert not policy.calls and not youtube.scheduled
        assert (await db.get(ProductionTask, env.task_id)).blocked_by_guard == "owned_inventory_receipt"


@pytest.mark.parametrize("halt", [False, True])
async def test_owned_metrics_intake_pause_allows_but_emergency_halt_denies(execute_queue, halt):
    env = await stage_queue(execute_queue, "collect_metrics", publication=True)
    calls = []
    async with env.factory() as db:
        now = await inv._now(db)
        channel = await db.get(ChannelProfile, env.channel_id)
        channel.intake_paused_at, channel.intake_pause_reason = now, "owned_inventory_expired"
        if halt:
            channel.halted_at = now
        pub = await db.get(PublicationRecord, env.publication_id)
        pub.scheduled_publish_at = now - timedelta(hours=1)
        pub.publish_status = "scheduled"
        schedule = PublicationMetricSchedule(publication_id=pub.id, snapshot_stage="1h",
            effective_start_at=now - timedelta(hours=1), due_at=now, grace_until=now + timedelta(hours=2))
        db.add(schedule)
        await db.flush()
        schedule_id = schedule.id
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        queue.payload_json = {"publication_id": str(pub.id), "metric_schedule_id": str(schedule.id),
            "snapshot_stage": "1h", "metrics_poll_count": 0}
        queue.idempotency_key = f"collect_metrics:{pub.id}:stage:1h:attempt:0"
        await db.commit()
        class YouTube(FakeYouTubeClient):
            async def fetch_metrics(self, **kwargs):
                calls.append(kwargs)
                assert not db.in_transaction()
                return {"views": 9, "likes": 2}
        result = await ChannelAgentService(youtube_client=YouTube()).handle_collect_metrics(db, queue)
        if halt:
            assert result is None and not calls
            assert (await db.get(PublicationMetricSchedule, schedule_id)).status == "pending"
        else:
            assert result.snapshot_stage == "1h" and result.views == 9 and len(calls) == 1
            assert (await db.get(PublicationMetricSchedule, schedule_id)).status == "succeeded"
            assert (await db.get(ChannelProfile, env.channel_id)).intake_paused_at is not None


async def test_owned_known_job_observation_is_fenced_after_expiry(execute_queue):
    from app.channel_agent.clients import AutoFlowJobObservation
    env = await stage_queue(execute_queue, "observe_job")
    async with env.factory() as db:
        task = await db.get(ProductionTask, env.task_id)
        run_id, job_id, pipeline_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        task.autoflow_run_id, task.job_id, task.pipeline_id, task.state = run_id, job_id, pipeline_id, "producing"
        db.add(AutoFlowRun(id=run_id, plan_id=task.autoflow_plan_id, job_id=job_id, pipeline_id=pipeline_id))
        (await db.get(OwnedSeedInventory, env.inventory_id)).state = "revoked"
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        queue.payload_json = {"production_task_id": str(task.id), "run_id": str(run_id), "job_id": str(job_id), "observe_count": 0}
        await db.commit()
        class Client:
            async def observe_job(self, session, **kwargs):
                assert not db.in_transaction()
                assert kwargs == {"run_id": str(run_id), "job_id": str(job_id)}
                return AutoFlowJobObservation(run_id=str(run_id), job_id=str(job_id), pipeline_id=str(pipeline_id), status="running")
        result = await ChannelAgentService(autoflow_client=Client()).handle_observe_job(db, queue)
        assert result.state == "producing"
        assert (await db.get(ChannelOpsQueueItem, env.queue_id)).status == "succeeded"
        assert len((await db.scalars(select(ChannelOpsQueueItem).where(ChannelOpsQueueItem.kind == "observe_job"))).all()) == 2


@pytest.mark.parametrize("stage", ["plan", "execute"])
async def test_owned_response_loss_uses_runner_cleanup_and_fresh_attempt_two(plan_queue, monkeypatch, stage):
    import importlib
    from app.channel_agent import runner as runner_module
    from app.channel_agent.clock import FakeClock
    from app.channel_agent.queue import ChannelOpsQueueService
    from app.channel_agent.runner import ChannelAgentRunner
    from app.models.job import JobStatus

    env = plan_queue
    calls, starts = [], []
    autoflow_module = importlib.import_module("app.autoflow.service")
    async def pipeline(db, data, *, commit=True):
        assert commit is False
        return SimpleNamespace(id=uuid.uuid4())
    async def job(db, pipeline_id, *, commit=True):
        assert commit is False
        row = Job(pipeline_id=pipeline_id, pipeline_snapshot={}, status=JobStatus.PENDING)
        db.add(row)
        await db.flush()
        return row
    async def start(job_ids):
        starts.extend(job_ids)
    monkeypatch.setattr(autoflow_module, "create_pipeline", pipeline)
    monkeypatch.setattr(autoflow_module, "create_job", job)
    monkeypatch.setattr(autoflow_module, "start_jobs_background", start)
    class LossClient(LocalAutoFlowClient):
        async def plan_task(self, task, request):
            result = await super().plan_task(task, request)
            if stage == "plan":
                calls.append(result.plan_id)
                raise ConnectionError("plan response lost")
            return result
        async def execute_task(self, task, request):
            result = await super().execute_task(task, request)
            calls.append(result.run_id)
            assert result.run_id, result.error_message
            raise ConnectionError("execute response lost")
    service = ChannelAgentService(pds_client=Policy(), autoflow_client=LossClient(session_factory=env.factory))
    async with env.factory() as db:
        if stage == "execute":
            await service.handle_plan_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
            env.queue_id = (await db.scalars(select(ChannelOpsQueueItem.id).where(ChannelOpsQueueItem.kind == "execute_task"))).one()
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        queue.status, queue.attempt_count, queue.locked_by, queue.locked_at = "queued", 0, None, None
        queue.run_after = await inv._now(db)
        clock = FakeClock(queue.run_after)
        await db.commit()
    async def now(_db):
        return clock.now()
    monkeypatch.setattr(inv, "_now", now)
    monkeypatch.setattr(runner_module, "async_session", env.factory)
    runner = ChannelAgentRunner()
    runner.queue = ChannelOpsQueueService(clock=clock)
    runner.service, service.queue, service.clock = service, runner.queue, clock
    assert await runner.run_once(run_scheduler_when_idle=False)
    async with env.factory() as db:
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        assert queue.status == "queued" and queue.attempt_count == 1 and queue.last_error == f"{stage} response lost"
        assert queue.locked_by is None and queue.locked_at is None
        original = await db.get(ProductionTask, env.task_id)
        plan_id, run_id, job_id = original.autoflow_plan_id, original.autoflow_run_id, original.job_id
        clock.current = inv.utc(queue.run_after)
        if stage == "execute":
            (await db.get(OwnedSeedInventory, env.inventory_id)).state = "revoked"
            await db.commit()
    assert await runner.run_once(run_scheduler_when_idle=False)
    async with env.factory() as db:
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        assert queue.status == "succeeded" and queue.attempt_count == 2 and queue.last_error is None
        assert queue.locked_by is None and queue.locked_at is None
        task = await db.get(ProductionTask, env.task_id)
        assert task.autoflow_plan_id == plan_id and task.autoflow_run_id == run_id and task.job_id == job_id
        assert len((await db.scalars(select(AutoFlowPlan))).all()) == 1 and len(calls) == 1
        assert len((await db.scalars(select(AutoFlowRun))).all()) == (stage == "execute")
        assert len(starts) == (stage == "execute")
        # Result recovery is not a clean-history or next-item authorization.
        from app.services import owned_seed_inventory_history as history
        row = await db.get(OwnedSeedInventory, env.inventory_id)
        snapshot = await history.load_owned_history_evidence(db, platform_channel_id=row.platform_channel_id)
        rows = snapshot.rows.as_dict()
        facts = history._task_history(rows, next(t for t in rows["production_tasks"] if t["id"] == str(task.id)))
        member = next(i for i in rows["owned_seed_inventory_items"] if i.get("production_task_id") == str(task.id))
        with pytest.raises(history.OwnedHistoryError, match="owned_inventory_queue_failed"):
            history._normal_history(facts, member, clock.now())
        from app.services.owned_inventory_feedback import finalize_owned_inventory_items
        assert await finalize_owned_inventory_items(db, env.channel_id) == ()
        await db.commit()
        assert (await db.get(OwnedSeedInventory, env.inventory_id)).state == ("revoked" if stage == "execute" else "held")
        assert (await db.get(ChannelProfile, env.channel_id)).intake_paused_at is not None
        assert (await db.get(ChannelOpsQueueItem, env.queue_id)).attempt_count == 2
        assert (await db.get(OwnedSeedInventoryItem, uuid.UUID(member["id"]))).state == "reserved"


@pytest.mark.parametrize("fault", ["both_missing", "plan_missing", "pds_missing", "unapproved"])
async def test_owned_first_promotion_requires_real_bound_approved_plan_and_pds(upload_queue, fault):
    env = upload_queue
    async with env.factory() as db:
        await ChannelAgentService().handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
    await claim_stage(env, "promote_publication")
    async with env.factory() as db:
        task = await db.get(ProductionTask, env.task_id)
        if fault in {"both_missing", "pds_missing"}:
            evidence = dict(task.agent_approval_evidence_json)
            evidence.pop("plan_pds")
            task.agent_approval_evidence_json = evidence
        if fault in {"both_missing", "plan_missing"}:
            task.autoflow_plan_id = None
        if fault == "unapproved":
            plan = await db.get(AutoFlowPlan, task.autoflow_plan_id)
            plan.approved_revision_hash, plan.approved_revision = None, None
            plan.agent_approved_by, plan.review_approved_at = None, None
        await db.commit()
        youtube, policy = FakeYouTubeClient(), Policy()
        await ChannelAgentService(pds_client=policy, youtube_client=youtube).handle_promote_publication(
            db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert not policy.calls and not youtube.scheduled
        assert not (await db.scalars(select(PublicationPromotionOperation))).all()
        assert (await db.get(ProductionTask, env.task_id)).state == "held"


@pytest.mark.parametrize("fault", ["revoked", "expired", "lease", "config", "attempt", "promotion_pds", "other_evidence"])
async def test_owned_final_post_reenters_after_attempt_commit(upload_queue, monkeypatch, fault):
    from app.channel_agent.owned_producer import QueueAuthorityLost
    env = upload_queue
    async with env.factory() as db:
        await ChannelAgentService().handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
    await claim_stage(env, "promote_publication")
    youtube = FakeYouTubeClient()
    async with env.factory() as db:
        commit = db.commit
        changed = False
        async def intervene():
            nonlocal changed
            await commit()
            if changed:
                return
            async with env.factory() as other:
                operation = (await other.scalars(select(PublicationPromotionOperation))).one_or_none()
                if operation is None or operation.status != "submitting":
                    return
                changed = True
                if fault == "revoked":
                    (await other.get(OwnedSeedInventory, env.inventory_id)).state = "revoked"
                elif fault == "expired":
                    row = await other.get(OwnedSeedInventory, env.inventory_id)
                    after = inv.utc(row.expires_at)
                    async def later(_db):
                        return after
                    monkeypatch.setattr(inv, "_now", later)
                elif fault == "lease":
                    (await other.get(ChannelOpsQueueItem, env.queue_id)).locked_by = "replacement"
                elif fault == "config":
                    task = await other.get(ProductionTask, env.task_id)
                    (await other.get(PublishingAccount, task.target_account_id)).default_privacy = "private"
                elif fault in {"promotion_pds", "other_evidence"}:
                    task = await other.get(ProductionTask, env.task_id)
                    evidence = deepcopy(task.agent_approval_evidence_json)
                    evidence[fault] = {"drift": True}
                    task.agent_approval_evidence_json = evidence
                else:
                    operation.attempt_key = "changed-attempt"
                await other.commit()
        monkeypatch.setattr(db, "commit", intervene)
        service = ChannelAgentService(pds_client=Policy(), youtube_client=youtube)
        if fault == "lease":
            with pytest.raises(QueueAuthorityLost):
                await service.handle_promote_publication(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        else:
            await service.handle_promote_publication(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert changed and not youtube.scheduled
        assert len((await db.scalars(select(PublicationPromotionOperation))).all()) == 1


@pytest.mark.parametrize("verdict", ["block", "flag"])
async def test_owned_promotion_rejection_retains_exact_request_response(upload_queue, verdict):
    env = upload_queue
    async with env.factory() as db:
        await ChannelAgentService().handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
    await claim_stage(env, "promote_publication")
    response = replace(decision(), verdict=verdict)
    class Reject(Policy):
        async def decide(self, request):
            self.calls.append(request)
            return response
    policy, youtube = Reject(), FakeYouTubeClient()
    async with env.factory() as db:
        plan_evidence = deepcopy((await db.get(ProductionTask, env.task_id)).agent_approval_evidence_json["plan_pds"])
        await ChannelAgentService(pds_client=policy, youtube_client=youtube).handle_promote_publication(
            db, await db.get(ChannelOpsQueueItem, env.queue_id))
        task = await db.get(ProductionTask, env.task_id)
        assert task.state == "held" and not youtube.scheduled
        assert not (await db.scalars(select(PublicationPromotionOperation))).all()
        assert task.agent_approval_evidence_json["promotion_pds"] == {
            "request": asdict(policy.calls[0]), "response": asdict(response)}
        assert task.agent_approval_evidence_json["plan_pds"] == plan_evidence
        assert (await db.get(ChannelProfile, env.channel_id)).intake_paused_at is not None
        assert (await db.get(OwnedSeedInventory, env.inventory_id)).state == "held"


@pytest.mark.parametrize("fault", ["lease", "transport"])
async def test_owned_promotion_cannot_persist_unobserved_or_stale_lease_evidence(upload_queue, fault):
    from app.channel_agent.owned_producer import QueueAuthorityLost
    env = upload_queue
    async with env.factory() as db:
        await ChannelAgentService().handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
    await claim_stage(env, "promote_publication")
    class Reject(Policy):
        async def decide(self, request):
            if fault == "transport":
                raise ConnectionError("no response")
            async with env.factory() as other:
                (await other.get(ChannelOpsQueueItem, env.queue_id)).locked_by = "replacement"
                await other.commit()
            return replace(decision(), verdict="block")
    youtube = FakeYouTubeClient()
    async with env.factory() as db:
        service = ChannelAgentService(pds_client=Reject(), youtube_client=youtube)
        if fault == "lease":
            with pytest.raises(QueueAuthorityLost):
                await service.handle_promote_publication(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        else:
            await service.handle_promote_publication(db, await db.get(ChannelOpsQueueItem, env.queue_id))
        task = await db.get(ProductionTask, env.task_id)
        assert "promotion_pds" not in task.agent_approval_evidence_json
        assert task.state == ("uploaded_private" if fault == "lease" else "held")
        assert not youtube.scheduled and not (await db.scalars(select(PublicationPromotionOperation))).all()


async def test_owned_plan_rejection_retains_exact_request_response(plan_queue):
    env = plan_queue
    response = replace(decision(), verdict="block")
    class Reject(Policy):
        async def decide(self, request):
            self.calls.append(request)
            return response
    policy = Reject()
    async with env.factory() as db:
        task = await ChannelAgentService(pds_client=policy,
            autoflow_client=LocalAutoFlowClient(session_factory=env.factory)).handle_plan_task(
                db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert task.state == "held"
        assert task.agent_approval_evidence_json["plan_pds"] == {
            "request": asdict(policy.calls[0]), "response": asdict(response)}
        plan = await db.get(AutoFlowPlan, task.autoflow_plan_id)
        assert not plan.review_approved_at and not plan.agent_approved_by


@pytest.mark.parametrize("fault", [None, "actor", "context", "response"])
async def test_owned_reserved_promotion_reuses_exact_policy_or_refuses_drift(upload_queue, fault):
    env = upload_queue
    async with env.factory() as db:
        await ChannelAgentService().handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
    await claim_stage(env, "promote_publication")
    async with env.factory() as db:
        task = await db.get(ProductionTask, env.task_id)
        pub = (await db.scalars(select(PublicationRecord))).one()
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        envelope = {"request": {"actor_id": str(pub.account_id), "action_type": "publish", "platform": "youtube",
            "content": {"title": pub.title, "description": pub.description or ""},
            "context": {"publication_id": str(pub.id), "production_task_id": str(task.id),
                "target_visibility": "unlisted", "owned_inventory": task.agent_approval_evidence_json["owned_inventory"]}},
            "response": asdict(decision())}
        if fault == "actor":
            envelope["request"]["actor_id"] = str(uuid.uuid4())
        elif fault == "context":
            envelope["request"]["context"]["production_task_id"] = str(uuid.uuid4())
        elif fault == "response":
            envelope["response"]["decision_id"] = "different-decision"
        task.agent_approval_evidence_json = {**task.agent_approval_evidence_json, "promotion_pds": envelope}
        db.add(PublicationPromotionOperation(publication_id=pub.id, production_task_id=task.id,
            queue_item_id=queue.id, platform_video_id=pub.platform_content_id, target_privacy="unlisted",
            scheduled_at=inv.utc(queue.run_after), attempt_key=str(uuid.uuid4()), status="reserved",
            decision_json=asdict(decision())))
        await db.commit()
        policy, youtube = Policy(), FakeYouTubeClient()
        await ChannelAgentService(pds_client=policy, youtube_client=youtube).handle_promote_publication(db, queue)
        assert not policy.calls
        assert len(youtube.scheduled) == (fault is None)
        assert (await db.get(ProductionTask, env.task_id)).state == ("scheduled" if fault is None else "held")


async def test_owned_post_holds_final_schedule_transaction(upload_queue):
    env = upload_queue
    async with env.factory() as db:
        await ChannelAgentService().handle_publish_task(db, await db.get(ChannelOpsQueueItem, env.queue_id))
    await claim_stage(env, "promote_publication")
    calls = []
    async with env.factory() as db:
        class YouTube(FakeYouTubeClient):
            async def schedule_publish(self, **kwargs):
                calls.append(kwargs)
                assert db.in_transaction()
                operation = (await db.scalars(select(PublicationPromotionOperation))).one()
                assert operation.status == "submitting" and operation.request_attempted_at is not None
                return {"accepted": True}
        await ChannelAgentService(pds_client=Policy(), youtube_client=YouTube()).handle_promote_publication(
            db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert len(calls) == 1
        assert (await db.scalars(select(PublicationPromotionOperation.status))).one() == "finalized"


async def test_owned_reconcile_hook_failure_rolls_back_queue_and_publication(execute_queue, monkeypatch):
    from app.services import owned_inventory_feedback
    env = await stage_queue(execute_queue, "reconcile_publication", publication=True)
    async def fail(db, channel_id, *, publication_id):
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        assert queue.status == "succeeded" and queue.locked_by is None and queue.locked_at is None
        raise RuntimeError("finalizer interrupted")
    monkeypatch.setattr(owned_inventory_feedback, "finalize_owned_inventory_items", fail)
    async with env.factory() as db:
        youtube = FakeYouTubeClient(status_by_video={"OWNEDVIDEO1": {
            "privacy": "unlisted", "publish_status": "scheduled", "url": "observed-url"}})
        with pytest.raises(RuntimeError, match="finalizer interrupted"):
            await ChannelAgentService(youtube_client=youtube).handle_reconcile_publication(
                db, await db.get(ChannelOpsQueueItem, env.queue_id))
        queue = await db.get(ChannelOpsQueueItem, env.queue_id)
        assert queue.status == "running" and queue.locked_by == "owned-runner" and queue.locked_at is not None
        assert (await db.get(PublicationRecord, env.publication_id)).permalink is None
        assert (await db.scalars(select(OwnedSeedInventoryItem.state).where(
            OwnedSeedInventoryItem.production_task_id == env.task_id))).one() == "reserved"


@pytest.mark.parametrize("status", [{}, {"privacy": "private", "publish_status": "scheduled"},
    {"privacy": "unlisted", "publish_status": "failed"},
    {"privacy": "unlisted", "publish_status": "scheduled", "video_id": "WRONGVIDEO1"}])
async def test_owned_unconfirmed_reconcile_never_calls_completion_hook(execute_queue, monkeypatch, status):
    from app.services import owned_inventory_feedback
    env = await stage_queue(execute_queue, "reconcile_publication", publication=True)
    async def forbidden(*args, **kwargs):
        pytest.fail("unconfirmed observation is not a successful reconciliation")
    monkeypatch.setattr(owned_inventory_feedback, "finalize_owned_inventory_items", forbidden)
    async with env.factory() as db:
        youtube = FakeYouTubeClient(status_by_video={"OWNEDVIDEO1": status})
        await ChannelAgentService(youtube_client=youtube).handle_reconcile_publication(
            db, await db.get(ChannelOpsQueueItem, env.queue_id))
        assert (await db.get(ProductionTask, env.task_id)).state == "held"
        assert (await db.scalars(select(OwnedSeedInventoryItem.state).where(
            OwnedSeedInventoryItem.production_task_id == env.task_id))).one() == "reserved"
