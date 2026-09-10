from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from app.models.artifact import Artifact, ArtifactKind, IntermediateArtifactCache
from app.models.asset import Asset
from app.models.channel_agent import ChannelProfile, ProductionTask
from app.models.job import Job, JobStatus, NodeExecution, NodeStatus
from app.models.registered_worker_event_receipt import WorkerTaskDispatch
from app.models.schedule import RuntimeSchedule
from app.orchestrator.artifact_cache import IntermediateArtifactCacheService
from app.orchestrator.dag import build_dependency_map
from app.orchestrator.engine import JobEngine
from app.schemas.pipeline import PipelineDefinition
from app.services.registered_worker_event_receipt import RegisteredWorkerEventReceiptService


class FakeRedis:
    def __init__(self):
        self.tasks = []
        self.markers = {}

    async def eval(self, _script, key_count, stream, marker, *fields):
        assert key_count == 2
        if marker not in self.markers:
            self.tasks.append((stream, dict(zip(fields[::2], fields[1::2], strict=True))))
            self.markers[marker] = f"{len(self.tasks)}-0"
        return self.markers[marker]

    async def get(self, marker):
        return self.markers.get(marker)

    async def aclose(self):
        pass


@pytest.fixture
async def launch_env(monkeypatch):
    database = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        json_serializer=lambda value: json.dumps(value, default=str),
    )
    async with database.begin() as connection:
        for table in (
            Asset.__table__, ChannelProfile.__table__, ProductionTask.__table__,
            RuntimeSchedule.__table__, Job.__table__, NodeExecution.__table__,
            Artifact.__table__, IntermediateArtifactCache.__table__, WorkerTaskDispatch.__table__,
        ):
            await connection.run_sync(table.create)
    factory = async_sessionmaker(database, expire_on_commit=False)
    redis = FakeRedis()
    monkeypatch.setattr("app.orchestrator.engine.async_session", factory)
    monkeypatch.setattr("app.orchestrator.engine._redis", lambda: redis)
    monkeypatch.setattr(
        "app.orchestrator.engine._worker_task_dispatches",
        RegisteredWorkerEventReceiptService(factory),
    )
    try:
        yield SimpleNamespace(factory=factory, redis=redis)
    finally:
        await database.dispose()


def _definition(asset_id, *, cached_chain=False):
    kinds = (
        {"source_1": "source", "cached_1": "trim", "cached_2": "transcode", "tail": "title_overlay"}
        if cached_chain else {
            "source_1": "source", "smart_trim_1": "smart_trim", "smart_trim_2": "smart_trim",
            "smart_trim_3": "smart_trim", "join": "concat_many", "encode": "transcode",
            "title": "title_overlay", "upload": "youtube_upload",
        }
    )
    pairs = (
        [("source_1", "cached_1"), ("cached_1", "cached_2"), ("cached_2", "tail")]
        if cached_chain else [
            *[("source_1", f"smart_trim_{index}") for index in range(1, 4)],
            *[(f"smart_trim_{index}", "join") for index in range(1, 4)],
            ("join", "encode"), ("encode", "title"), ("title", "upload"),
        ]
    )
    return {
        "nodes": [
            {
                "id": node_id, "type": kind, "position": {"x": index * 100, "y": 0},
                "data": {"label": node_id, "config": {"asset_id": str(asset_id)} if kind == "source" else {}},
            }
            for index, (node_id, kind) in enumerate(kinds.items())
        ],
        "edges": [
            {"id": f"edge-{index}", "source": source, "target": target,
             "sourceHandle": "output", "targetHandle": "input"}
            for index, (source, target) in enumerate(pairs)
        ],
    }


def _artifact(job_id, node_id, name):
    return Artifact(
        job_id=job_id, node_execution_id=node_id, kind=ArtifactKind.INTERMEDIATE,
        filename=name, mime_type="video/mp4", storage_backend="local",
        storage_path=f"artifacts/{name}", media_info={},
    )


async def _seed(env, *, resolved=True, cached_chain=False):
    async with env.factory() as db:
        asset = Asset(
            filename="owned.mp4", original_name="owned.mp4", mime_type="video/mp4",
            storage_path="assets/owned.mp4", media_info={"license": "owned", "provenance": "generated"},
        )
        channel = ChannelProfile(name="initial prefilter", enabled=True, dry_run=False)
        db.add_all([asset, channel])
        await db.flush()
        definition = _definition(asset.id, cached_chain=cached_chain)
        dependencies = build_dependency_map(PipelineDefinition.model_validate(definition))
        job = Job(
            pipeline_id=uuid.uuid4(), pipeline_snapshot=definition,
            status=JobStatus.RUNNING if resolved else JobStatus.PENDING,
            execution_plan={"dependencies": dependencies}, orchestrator_owner="python",
        )
        db.add(job)
        await db.flush()
        nodes = [
            NodeExecution(
                job_id=job.id, node_id=node["id"], node_type=node["type"],
                node_config=node["data"]["config"],
                status=NodeStatus.SUCCEEDED if resolved and node["type"] == "source" else NodeStatus.PENDING,
            )
            for node in definition["nodes"]
        ]
        task = ProductionTask(
            channel_profile_id=channel.id, target_account_id=uuid.uuid4(),
            prompt="owned input", state="producing", job_id=job.id,
        )
        db.add_all([*nodes, task, RuntimeSchedule(
            service_name="videoprocess", state="OPEN", guarded_job_id=job.id,
        )])
        await db.flush()
        if resolved:
            artifact = _artifact(job.id, nodes[0].id, "source.mp4")
            db.add(artifact)
            await db.flush()
            nodes[0].output_artifact_id = artifact.id
        await db.commit()
        return SimpleNamespace(
            job_id=job.id, channel_id=channel.id, task_id=task.id, dependencies=dependencies,
        )


async def _load(db, job_id):
    return (await db.execute(
        select(Job).where(Job.id == job_id).options(selectinload(Job.node_executions))
        .execution_options(populate_existing=True)
    )).scalar_one()


def _track_authority(monkeypatch, engine):
    calls = SimpleNamespace(initial=[], dispatch=[])
    initial = engine._lock_initial_launch_authority
    dispatch = engine._lock_dispatch_authority

    async def track_initial(db, job_id):
        calls.initial.append(job_id)
        return await initial(db, job_id)

    async def track_dispatch(db, job_id, node_id):
        calls.dispatch.append(node_id)
        return await dispatch(db, job_id, node_id)

    monkeypatch.setattr(engine, "_lock_initial_launch_authority", track_initial)
    monkeypatch.setattr(engine, "_lock_dispatch_authority", track_dispatch)
    return calls


async def test_real_start_only_rechecks_three_eligible_roots(launch_env, monkeypatch):
    seed = await _seed(launch_env, resolved=False)
    engine = JobEngine()
    calls = _track_authority(monkeypatch, engine)

    await engine.start_job(seed.job_id)

    assert len(calls.initial) == 6  # Three launch phases, then three eligible roots.
    assert len(calls.dispatch) == 3
    assert [payload["node_id"] for _, payload in launch_env.redis.tasks] == [
        "smart_trim_1", "smart_trim_2", "smart_trim_3",
    ]
    async with launch_env.factory() as db:
        job = await _load(db, seed.job_id)
        statuses = {node.node_id: node.status for node in job.node_executions}
        dispatches = list((await db.scalars(select(WorkerTaskDispatch))).all())
    assert job.status == JobStatus.RUNNING
    assert statuses["source_1"] == NodeStatus.SUCCEEDED
    assert all(statuses[name] == NodeStatus.PENDING for name in ("join", "encode", "title", "upload"))
    assert len(dispatches) == 3
    assert all(row.delivery_state == "delivered" for row in dispatches)


@pytest.mark.parametrize("status", [status for status in NodeStatus if status != NodeStatus.PENDING])
async def test_observed_nonpending_node_skips_full_initial_authority(launch_env, monkeypatch, status):
    seed = await _seed(launch_env)
    engine = JobEngine()
    calls = _track_authority(monkeypatch, engine)
    async with launch_env.factory() as db:
        job = await _load(db, seed.job_id)
        node = next(node for node in job.node_executions if node.node_id == "smart_trim_1")
        node.status = status
        await db.commit()
        await engine._dispatch_ready_nodes(
            db, job, {"smart_trim_1": ["source_1"]}, guard_initial_launch=True,
        )
    assert calls.initial == []
    assert calls.dispatch == []
    assert launch_env.redis.tasks == []


async def test_dependency_completion_is_dispatched_after_refresh(launch_env, monkeypatch):
    seed = await _seed(launch_env)
    engine = JobEngine()
    calls = _track_authority(monkeypatch, engine)
    async with launch_env.factory() as db:
        job = await _load(db, seed.job_id)
        source = next(node for node in job.node_executions if node.node_id == "source_1")
        source.status = NodeStatus.RUNNING
        await db.commit()
        await engine._dispatch_ready_nodes(
            db, job, {"smart_trim_1": ["source_1"]}, guard_initial_launch=True,
        )
        assert calls.initial == []
        assert launch_env.redis.tasks == []
        source.status = NodeStatus.SUCCEEDED
        await db.commit()
        job = await _load(db, seed.job_id)
        await engine._dispatch_ready_nodes(
            db, job, {"smart_trim_1": ["source_1"]}, guard_initial_launch=True,
        )
    assert len(calls.initial) == len(calls.dispatch) == 1
    assert [payload["node_id"] for _, payload in launch_env.redis.tasks] == ["smart_trim_1"]


@pytest.mark.parametrize(
    ("phase", "change"),
    [
        (phase, change)
        for phase in ("initial", "dispatch")
        for change in ("closed", "other_guard", "quarantined", "node_cancelled")
    ] + [("initial", "dependency_changed")],
)
async def test_observed_eligibility_never_replaces_fresh_authority(
    launch_env, monkeypatch, phase, change,
):
    seed = await _seed(launch_env)
    engine = JobEngine()
    calls = _track_authority(monkeypatch, engine)
    async with launch_env.factory() as db:
        job = await _load(db, seed.job_id)

        async def change_authority(_job_id, _node_id):
            schedule = await db.get(RuntimeSchedule, "videoprocess")
            if change == "closed":
                schedule.state = "CLOSED"
            elif change == "other_guard":
                schedule.guarded_job_id = uuid.uuid4()
            elif change == "quarantined":
                channel = await db.get(ChannelProfile, seed.channel_id)
                channel.halted_at = datetime.now(timezone.utc)
                task = await db.get(ProductionTask, seed.task_id)
                task.state = "held"
                job.status = JobStatus.CANCELLED
                for node in job.node_executions:
                    if node.status != NodeStatus.SUCCEEDED:
                        node.status = NodeStatus.CANCELLED
            else:
                name = "smart_trim_1" if change == "node_cancelled" else "source_1"
                node = next(node for node in job.node_executions if node.node_id == name)
                node.status = NodeStatus.CANCELLED if change == "node_cancelled" else NodeStatus.RUNNING
            await db.commit()

        hook = "_before_initial_node_launch_recheck" if phase == "initial" else "_before_node_dispatch_recheck"
        monkeypatch.setattr(engine, hook, change_authority)
        await engine._dispatch_ready_nodes(
            db, job, {"smart_trim_1": ["source_1"]}, guard_initial_launch=True,
        )
        assert list((await db.scalars(select(WorkerTaskDispatch))).all()) == []
    assert len(calls.initial) == 1
    assert len(calls.dispatch) == (1 if phase == "dispatch" else 0)
    assert launch_env.redis.tasks == []


async def test_cached_chain_makes_next_node_eligible_in_same_initial_pass(launch_env, monkeypatch):
    seed = await _seed(launch_env, cached_chain=True)
    engine = JobEngine()
    calls = _track_authority(monkeypatch, engine)
    async with launch_env.factory() as db:
        job = await _load(db, seed.job_id)
        nodes = {node.node_id: node for node in job.node_executions}
        input_artifact = await db.get(Artifact, nodes["source_1"].output_artifact_id)
        for name in ("cached_1", "cached_2"):
            output = _artifact(job.id, nodes[name].id, f"{name}.mp4")
            db.add(output)
            await db.flush()
            await IntermediateArtifactCacheService().store(
                db, node_type=nodes[name].node_type, node_config={},
                input_artifacts={"input": input_artifact}, output_artifact=output,
                node_id=name, job_id=job.id,
            )
            input_artifact = output
        await db.commit()
        await engine._dispatch_ready_nodes(db, job, seed.dependencies, guard_initial_launch=True)
        assert all(nodes[name].status == NodeStatus.SUCCEEDED for name in ("cached_1", "cached_2"))
        entries = list((await db.scalars(select(IntermediateArtifactCache))).all())
        assert [entry.hit_count for entry in entries] == [1, 1]
    assert len(calls.initial) == len(calls.dispatch) == 3
    assert [payload["node_id"] for _, payload in launch_env.redis.tasks] == ["tail"]
