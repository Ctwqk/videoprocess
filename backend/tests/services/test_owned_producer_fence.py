from dataclasses import asdict

import pytest

from app.pds_client import PDSDecision, PDSDecisionRequest
from app.services import owned_seed_inventory as inventory
from app.services import owned_seed_inventory_history as history
from tests.services.test_owned_seed_inventory_history import NOW, completed_rows, mapped_rows, retired_rows, snap, uid
from tests.channel_agent.test_owned_inventory import (
    inventory_env as inventory_env, owned_env as owned_env, Policy, tick, sqlite_history,
)


def decision():
    return PDSDecision(decision_id="decision-1", verdict="allow", rules_version="rules-1",
                       evaluated_rules=["owned-source", "unlisted"])


@pytest.mark.parametrize("change", ["decision", "rules", "evaluated", "fallback", "degraded", "noop", "block", "flag"])
def test_real_owned_pds_rejects_missing_or_degraded_authority(change):
    from app.services.owned_producer_fence import require_real_pds
    response = asdict(decision())
    if change == "decision":
        response["decision_id"] = ""
    elif change == "rules":
        response["rules_version"] = ""
    elif change == "evaluated":
        response["evaluated_rules"] = []
    elif change in {"block", "flag"}:
        response["verdict"] = change
    elif change == "fallback":
        response["metadata"] = {"warning": "pds_unavailable", "fail_policy": "allow"}
    else:
        response["metadata"] = {change: True}
    with pytest.raises(inventory.OwnedInventoryError, match="owned_inventory_pds"):
        require_real_pds(response)


def test_real_owned_pds_preserves_request_and_advisory():
    from app.services.owned_producer_fence import policy_evidence
    response = asdict(decision())
    response["metadata"] = {"warning": "advisory"}
    request = PDSDecisionRequest(actor_id="account", action_type="plan_approval", platform="youtube",
                                 context={"production_task_id": "task", "autoflow_plan_id": "plan"})
    evidence = policy_evidence(request, response)
    assert evidence == {"request": asdict(request), "response": response}
    response["metadata"]["warning"] = "mutated"
    assert evidence["response"]["metadata"]["warning"] == "advisory"


@pytest.mark.parametrize("verdict", ["block", "flag"])
def test_policy_evidence_preserves_denial_without_authorizing_it(verdict):
    from app.services.owned_producer_fence import policy_evidence, require_real_pds
    response = {**asdict(decision()), "verdict": verdict}
    request = PDSDecisionRequest(actor_id="account", action_type="publish", platform="youtube")
    assert policy_evidence(request, response) == {"request": asdict(request), "response": response}
    with pytest.raises(inventory.OwnedInventoryError, match="owned_inventory_pds_evidence"):
        require_real_pds(response)


def test_current_producer_exclusion_does_not_exclude_operation_classification():
    rows = completed_rows(own=True)
    task = rows["production_tasks"][0]
    rows["publishing_accounts"][0]["platform_account_id"] = ""
    result = history.assess_owned_producer_history(snap(rows), now=NOW, current_task_id=task["id"])
    assert result.block_reason == "owned_history_unclassified"


def test_current_producer_exclusion_does_not_exclude_other_orphan():
    rows = completed_rows(own=True)
    rows["youtube_upload_operations"].append({"id": uid(987), "production_task_id": uid(988)})
    result = history.assess_owned_producer_history(snap(rows), now=NOW, current_task_id=rows["production_tasks"][0]["id"])
    assert result.block_reason == "owned_history_orphan"


def test_current_producer_exclusion_only_removes_its_prior_effect_wait():
    rows = completed_rows(own=True, start=NOW)
    task_id = rows["production_tasks"][0]["id"]
    current = history.assess_owned_producer_history(snap(rows), now=NOW, current_task_id=task_id)
    prior = history.assess_owned_history(snap(rows), now=NOW)
    assert current.block_reason is None and current.wait_reason is None
    assert current.classifications == prior.classifications
    assert prior.wait_reason == "owned_inventory_cooldown"


def test_current_producer_exclusion_requires_an_exact_existing_task():
    rows = completed_rows(own=True)
    result = history.assess_owned_producer_history(snap(rows), now=NOW, current_task_id=uid(999))
    assert result.block_reason == "owned_inventory_producer_missing"


async def producer_snapshot(env):
    await tick(env, Policy())
    async with env.factory() as db:
        return await sqlite_history(db, platform_channel_id=env.scope["platform_channel_id"])


async def test_durable_item_not_caller_evidence_authorizes_producer(owned_env):
    from app.services.owned_producer_fence import producer_identity
    snapshot = await producer_snapshot(owned_env)
    task = snapshot.rows.as_dict()["production_tasks"][0]
    identity = producer_identity(snapshot, task["id"], now=snapshot.observed_at)
    assert identity.inventory_id == str(owned_env.inventory_id)
    assert identity.task_id == task["id"]


@pytest.mark.parametrize("drift", ["missing_item", "wrong_task_seed", "wrong_pointer", "expired", "held", "revoked", "privacy", "prompt", "snapshot_source"])
async def test_producer_reloads_durable_binding(owned_env, drift):
    from app.services.owned_producer_fence import producer_identity
    snapshot = await producer_snapshot(owned_env)
    rows = snapshot.rows.as_dict()
    task = rows["production_tasks"][0]
    if drift == "missing_item":
        rows["owned_seed_inventory_items"] = []
    elif drift == "wrong_task_seed":
        task["manual_seed_id"] = uid(999)
    elif drift == "wrong_pointer":
        rows["channel_profiles"][0]["owned_seed_inventory_id"] = None
    elif drift == "expired":
        rows["owned_seed_inventories"][0]["expires_at"] = snapshot.observed_at.isoformat()
    elif drift in {"held", "revoked"}:
        rows["owned_seed_inventories"][0]["state"] = drift
    elif drift == "prompt":
        task["prompt"] = "Different content"
    elif drift == "snapshot_source":
        task["channel_config_snapshot_json"]["manual_seed"]["constraints_json"]["input_asset_id"] = uid(999)
    else:
        rows["publishing_accounts"][0]["default_privacy"] = "private"
    changed = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id=snapshot.platform_channel_id,
                                                   observed_at=snapshot.observed_at)
    with pytest.raises(inventory.OwnedInventoryError):
        producer_identity(changed, task["id"], now=snapshot.observed_at)


async def test_candidate_pds_request_is_retained_exactly(owned_env):
    policy = Policy()
    await tick(owned_env, policy)
    async with owned_env.factory() as db:
        snapshot = await sqlite_history(db, platform_channel_id=owned_env.scope["platform_channel_id"])
    task = snapshot.rows.as_dict()["production_tasks"][0]
    assert task["agent_approval_evidence_json"]["candidate_pds_request"] == asdict(policy.calls[0])


async def test_ordinary_task_cannot_produce_in_occupied_canonical_scope(owned_env):
    from app.services.owned_producer_fence import producer_identity
    snapshot = await producer_snapshot(owned_env)
    rows = snapshot.rows.as_dict()
    original = rows["production_tasks"][0]
    ordinary = {**original, "id": uid(999), "manual_seed_id": None,
                "approval_mode": "human", "agent_approval_evidence_json": {}}
    rows["production_tasks"].append(ordinary)
    changed = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id=snapshot.platform_channel_id,
                                                   observed_at=snapshot.observed_at)
    with pytest.raises(inventory.OwnedInventoryError, match="owned_inventory_producer_binding"):
        producer_identity(changed, ordinary["id"], now=snapshot.observed_at)


@pytest.mark.parametrize("factory", [mapped_rows, retired_rows])
def test_historical_producers_remain_blocked_without_inventory_identity(factory):
    from app.services.owned_producer_fence import producer_identity
    rows = factory()
    if factory is retired_rows:
        rows, _ = rows
    task = rows["production_tasks"][0]
    task["agent_approval_evidence_json"] = {}
    task["channel_config_snapshot_json"] = {}
    with pytest.raises(inventory.OwnedInventoryError, match="historical_producer_pinned"):
        producer_identity(snap(rows), task["id"], now=NOW)


async def test_producer_history_keeps_full_current_classification(owned_env):
    from app.services.owned_producer_fence import assess_producer
    snapshot = await producer_snapshot(owned_env)
    task_id = snapshot.rows.as_dict()["production_tasks"][0]["id"]
    identity, assessment = assess_producer(snapshot, task_id, now=snapshot.observed_at)
    assert identity.inventory_id == str(owned_env.inventory_id) and assessment.block_reason is None


async def test_producer_history_rejects_new_global_orphan(owned_env):
    from app.services.owned_producer_fence import assess_producer
    snapshot = await producer_snapshot(owned_env)
    rows = snapshot.rows.as_dict()
    task_id = rows["production_tasks"][0]["id"]
    rows["youtube_upload_operations"].append({"id": uid(998), "production_task_id": uid(999)})
    changed = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id=snapshot.platform_channel_id,
                                                   observed_at=snapshot.observed_at)
    with pytest.raises(inventory.OwnedInventoryError, match="owned_history_orphan"):
        assess_producer(changed, task_id, now=snapshot.observed_at)


def owned_graph(asset_id):
    from app.autoflow.metadata_generator import MetadataGenerator
    from app.autoflow.pipeline_builder import PipelineBuilder
    from app.autoflow.template_library import TemplateLibrary
    from app.schemas.autoflow import AutoFlowClipCandidate, AutoFlowIntent
    intent = AutoFlowIntent(intent_type="animal_compilation", subject="Owned input", publish_mode="unlisted_upload")
    candidates = [AutoFlowClipCandidate(id="owned", title="Owned input", source_type="asset", asset_id=asset_id,
                                        start_sec=0, end_sec=10)]
    return PipelineBuilder().build(TemplateLibrary().get_template("animal_compilation_short"), intent, candidates,
                                   MetadataGenerator().generate(intent, candidates)).model_dump(mode="json")


@pytest.mark.parametrize("change", [None, "private", "other_asset", "second_source", "external", "extra_branch", "cycle"])
def test_owned_pipeline_requires_exact_connected_owned_source(change):
    from copy import deepcopy
    from app.services.owned_producer_fence import require_owned_pipeline
    graph = owned_graph(uid(123))
    source = next(n for n in graph["nodes"] if n["type"] == "source")
    upload = next(n for n in graph["nodes"] if n["type"] == "youtube_upload")
    if change == "private":
        upload["data"]["config"]["privacy"] = "private"
    elif change == "other_asset":
        source["data"]["asset_id"] = uid(124)
    elif change in {"second_source", "extra_branch"}:
        extra = deepcopy(source if change == "second_source" else graph["nodes"][1])
        extra["id"] = "detached"
        graph["nodes"].append(extra)
    elif change == "external":
        source["type"] = "url_download"
        source["data"]["config"] = {"url": "https://example.invalid/source.mp4"}
    elif change == "cycle":
        edge = deepcopy(graph["edges"][0])
        edge.update(id="cycle", source=upload["id"], target=source["id"])
        graph["edges"].append(edge)
    if change is None:
        require_owned_pipeline(graph, uid(123))
    else:
        with pytest.raises(inventory.OwnedInventoryError, match="owned_inventory_pipeline"):
            require_owned_pipeline(graph, uid(123))


async def test_locked_producer_rechecks_approval_and_leaves_fence_held(owned_env):
    from app.services.owned_producer_fence import lock_producer
    from app.models.owned_seed_inventory import OwnedSeedInventory
    from app.models.channel_agent import ProductionTask
    from sqlalchemy import select
    await tick(owned_env, Policy())
    async with owned_env.factory() as db:
        task = (await db.scalars(select(ProductionTask))).one()
        task_id = task.id
        authority = await lock_producer(db, task_id)
        assert authority.identity.task_id == str(task_id) and db.in_transaction()
        await db.rollback()
        row = await db.get(OwnedSeedInventory, owned_env.inventory_id)
        row.state = "revoked"
        await db.commit()
        with pytest.raises(inventory.OwnedInventoryError, match="producer_inactive"):
            await lock_producer(db, task_id)


async def test_locked_producer_rejects_pending_writes_before_any_rollback(owned_env):
    from app.services.owned_producer_fence import lock_producer
    from app.models.owned_seed_inventory import OwnedSeedInventory
    await tick(owned_env, Policy())
    async with owned_env.factory() as db:
        row = await db.get(OwnedSeedInventory, owned_env.inventory_id)
        row.hold_reason = "caller-pending-change"
        with pytest.raises(inventory.OwnedInventoryError, match="pending_session"):
            await lock_producer(db, uid(999))
        assert row in db.dirty and row.hold_reason == "caller-pending-change"


@pytest.mark.parametrize("boundary", ["reserve", "attempt", "attempt_without_context", "fence"])
async def test_local_upload_boundary_cannot_bypass_revoked_inventory(owned_env, boundary):
    from app.models.artifact import Artifact
    from app.models.channel_agent import ProductionTask
    from app.models.owned_seed_inventory import OwnedSeedInventory
    from app.models.youtube_upload_operation import YouTubeUploadOperation
    from app.services.youtube_upload_operations import YouTubeUploadOperationStore
    from tests.services.test_youtube_upload_operations import _context_for
    from sqlalchemy import select
    await tick(owned_env, Policy())
    async with owned_env.factory() as db:
        async with db.bind.begin() as connection:
            await connection.run_sync(Artifact.__table__.create)
        task = (await db.scalars(select(ProductionTask))).one()
        task.state = "producing"
        context = await _context_for(db, production_task=task)
        if boundary != "reserve":
            operation = YouTubeUploadOperation(production_task_id=task.id, job_id=context.job_id,
                node_execution_id=context.node_execution_id, input_artifact_id=context.input_artifact_id,
                content_sha256=context.content_sha256, title=context.title, privacy=context.privacy, status="reserved")
            db.add(operation)
            await db.flush()
            operation_id = operation.id
        row = await db.get(OwnedSeedInventory, owned_env.inventory_id)
        row.state = "revoked"
        await db.commit()
    store = YouTubeUploadOperationStore(owned_env.factory)
    with pytest.raises(inventory.OwnedInventoryError, match="producer_inactive"):
        if boundary == "reserve":
            await store.claim(context)
        elif boundary.startswith("attempt"):
            await store.mark_attempting(operation_id, context=context if boundary == "attempt" else None)
        else:
            async with store.submission_fence(context):
                pytest.fail("revoked producer reached POST fence")
    async with owned_env.factory() as db:
        operations = (await db.scalars(select(YouTubeUploadOperation))).all()
        assert len(operations) == (0 if boundary == "reserve" else 1)
        assert all(o.request_attempted_at is None and o.status == "reserved" for o in operations)
