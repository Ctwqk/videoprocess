"""Populated A2 history bindings and succession, on parent-owned scratch PG only."""
from __future__ import annotations

import copy
import json
import uuid
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import text, update
from sqlalchemy.exc import DBAPIError

from app.channel_agent import clients
from app.models.owned_seed_inventory import OwnedSeedInventory
from app.services import owned_seed_inventory as service
from app.services import owned_seed_inventory_history as history
from tests.migrations.owned_history_postgres import a2_pg as a2_pg, insert_record, succeeded_document
from tests.migrations.test_owned_history_seal_postgres import a2_env as a2_env, approve, seal


@pytest.fixture
async def a2_history(a2_env, monkeypatch):
    h = a2_env
    rows = succeeded_document(await h.case.owner.fetchval("SELECT clock_timestamp()"), metrics_retry=True)
    job = rows["jobs"][0]
    async with h.case.owner.transaction():
        await h.case.owner.execute("INSERT INTO pipelines(id,name,definition) VALUES($1,'A2 succeeded fixture',$2::json)",
            uuid.UUID(job["pipeline_id"]), json.dumps(job["pipeline_snapshot"]))
        for table in ("channel_profiles", "publishing_accounts", "jobs", "node_executions", "artifacts",
                      "production_tasks", "youtube_upload_operations", "publication_records",
                      "publication_metric_schedules", "feedback_snapshots", "channel_ops_queue_items"):
            for row in rows[table]:
                await insert_record(h.case.owner, table, row)
    calls = []
    controls = SimpleNamespace(channel=h.env.scope["platform_channel_id"])
    operation = rows["youtube_upload_operations"][0]

    def manager_get(request):
        calls.append((request.method, request.url.path))
        assert request.method == "GET"
        if request.url.path == f"/api/status/{operation['manager_task_id']}":
            return httpx.Response(200, json={"id": operation["manager_task_id"], "type": "upload", "status": "completed",
                "progress": 100, "result": {"video_id": operation["platform_video_id"], "url": operation["receipt_json"]["url"]}, "error": None})
        assert request.url.path == f"/api/videos/{operation['platform_video_id']}/status"
        return httpx.Response(200, json={"video_id": operation["platform_video_id"], "privacy": "unlisted",
            "upload_status": "processed", "processing_status": "succeeded", "title": "Synthetic owned video",
            "published_at": None, "made_for_kids": False, "public_stats_viewable": True,
            "raw": {"status": {}, "snippet": {"channelId": controls.channel}, "processingDetails": {}}})

    manager = clients.YouTubeManagerClient(base_url="http://synthetic-manager.invalid", transport=httpx.MockTransport(manager_get))
    monkeypatch.setattr(clients, "build_youtube_manager_client", lambda: manager)
    data = await h.env.data("populated-history")
    data.update(version=2, history_locators={**h.data["history_locators"], "operations": [{
        "operation_id": operation["id"], "legacy_account_id": rows["publishing_accounts"][0]["id"],
        "legacy_channel_profile_id": rows["channel_profiles"][0]["id"]}]})
    response = await h.env.client.post(h.env.url, json=data)
    assert response.status_code == 200, response.text
    yield SimpleNamespace(**{**vars(h), "data": data, "result": response.json()}, history_rows=rows,
                          manager_calls=calls, manager_controls=controls)


async def assessment(h):
    async with h.case.sessions() as db:
        snapshot = await history.load_owned_history_evidence(db, platform_channel_id=h.env.scope["platform_channel_id"])
        sources = service._retirement_sources(snapshot, requested=False)
        await db.rollback()
        observations = await service._observe_retirement(sources, observed_at=snapshot.observed_at)
        fresh = await history.load_owned_history_evidence(db, platform_channel_id=snapshot.platform_channel_id)
        return history.assess_owned_history(history.OwnedHistorySnapshot.from_rows(fresh.rows.as_dict(),
            platform_channel_id=fresh.platform_channel_id, observed_at=fresh.observed_at,
            redis_observations=observations[1]), now=fresh.observed_at)


async def test_actual_history_only_binding_survives_native_metric_settlement_and_real_successor(a2_history, monkeypatch):
    h = a2_history
    original = copy.deepcopy(h.result)
    binding = original["manifest"]["legacy_history"]["bindings"][0]
    assert binding["use"] == "history_only" and binding["canonical_platform_channel_id"] == h.env.scope["platform_channel_id"]
    assert len(h.manager_calls) == 2
    approved = await approve(h)
    assert approved.status_code == 200, approved.text
    pending = await assessment(h)
    assert pending.block_reason is None and pending.wait_reason == "owned_inventory_metrics_pending"
    assert {c.classification for c in pending.classifications} == {"history_only", "retired_unassigned_preupload"}
    metric = next(m for m in h.history_rows["publication_metric_schedules"] if m["snapshot_stage"] == "24h")
    retry = next(q for q in h.history_rows["channel_ops_queue_items"] if q["payload_json"].get("metrics_poll_count") == 1)
    # Apply the native successful retry shape under real constraints/triggers.
    # No clock override, queue deadline change, production handler or external I/O.
    async with h.case.owner.transaction():
        await h.case.owner.execute("UPDATE channel_ops_queue_items SET status='succeeded',attempt_count=1 WHERE id=$1", uuid.UUID(retry["id"]))
        await h.case.owner.execute("""UPDATE publication_metric_schedules SET status='succeeded',attempt_count=2,
            completed_at=statement_timestamp(),last_attempt_at=statement_timestamp(),last_error_code=NULL WHERE id=$1""", uuid.UUID(metric["id"]))
        feedback = {**h.history_rows["feedback_snapshots"][0], "id": str(uuid.uuid4()), "snapshot_stage": "24h",
                    "collected_at": (await h.case.owner.fetchval("SELECT clock_timestamp()")).isoformat()}
        await insert_record(h.case.owner, "feedback_snapshots", feedback)
    settled = await assessment(h)
    assert settled.block_reason is None and settled.wait_reason is None
    assert settled.stable_history_sha256 == pending.stable_history_sha256
    async with h.case.sessions() as db:
        await db.execute(update(OwnedSeedInventory).where(OwnedSeedInventory.id == uuid.UUID(original["id"])).values(
            state="revoked", revoked_at=await db.scalar(text("SELECT clock_timestamp()")), revoked_by="fixture:operator",
            succession_released_at=await db.scalar(text("SELECT clock_timestamp()"))))
        closeout = await service.closeout(db, await db.get(OwnedSeedInventory, uuid.UUID(original["id"])))
        assert closeout["status"] == "ready"
        await db.commit()
    monkeypatch.setattr(service.settings, "owned_seed_inventory_operator_subject", "successor-observer")
    data = await h.env.data("real-successor")
    data.update(version=2, history_locators=h.data["history_locators"])
    response = await h.env.client.post(h.env.url, json=data)
    assert response.status_code == 200, response.text
    successor = SimpleNamespace(**{**vars(h), "data": data, "result": response.json()})
    assert successor.result["id"] != original["id"]
    assert successor.result["manifest"]["legacy_history"] == original["manifest"]["legacy_history"]
    denied = await approve(successor)
    assert denied.status_code == 409 and denied.json()["detail"] == "owned_inventory_platform_slot_occupied"
    async with h.case.sessions() as db:
        # Bind lineage in the same first approval transition, as the native v1
        # succession path does. Public v2 succession remains unsupported.
        row = await db.get(OwnedSeedInventory, uuid.UUID(successor.result["id"]))
        await service._requalify_v2_draft(db, row, "a2-fixture")
        await db.execute(update(OwnedSeedInventory).where(OwnedSeedInventory.id == uuid.UUID(successor.result["id"])).values(
            approved_at=await db.scalar(text("SELECT clock_timestamp()")), approved_by="a2-fixture",
            approval_reference="fixture:successor", state="approved",
            predecessor_inventory_id=uuid.UUID(original["id"]), predecessor_closeout_sha256=closeout["sha256"]))
        await db.commit()
        predecessor = await db.get(OwnedSeedInventory, uuid.UUID(original["id"]))
        child = await db.get(OwnedSeedInventory, uuid.UUID(successor.result["id"]))
        assert predecessor.manifest_json == original["manifest"] and predecessor.manifest_sha256 == original["manifest_sha256"]
        assert child.predecessor_inventory_id == predecessor.id
        assert await db.scalar(text("SELECT owned_seed_inventory_id FROM channel_profiles WHERE id=:id"), {"id": h.env.channel_id}) is None
    assert (await assessment(h)).block_reason is None
    assert all(method == "GET" for method, _ in h.manager_calls)
    divergent = copy.deepcopy(successor.result["manifest"])
    divergent["legacy_history"]["bindings"][0]["canonical_platform_channel_id"] = "UC" + "z" * 22
    async with h.case.sessions() as db:
        with pytest.raises(DBAPIError, match="owned_inventory_manifest_immutable"):
            await db.execute(update(OwnedSeedInventory).where(OwnedSeedInventory.id == uuid.UUID(successor.result["id"])).values(
                manifest_json=divergent, manifest_sha256=service.sha256(divergent)))
        await db.rollback()
    assert (await assessment(h)).block_reason is None


async def test_actual_successor_rejects_fresh_manager_uc_conflict_without_rewriting_parent(a2_history):
    h = a2_history
    async with h.case.sessions() as db:
        await seal(h, db)
        await db.commit()
    h.manager_controls.channel = "UC" + "z" * 22
    data = await h.env.data("conflicting-successor")
    data.update(version=2, history_locators=h.data["history_locators"])
    before = await h.case.owner.fetchval("SELECT count(*) FROM owned_seed_inventories")
    response = await h.env.client.post(h.env.url, json=data)
    assert response.status_code == 409
    assert await h.case.owner.fetchval("SELECT count(*) FROM owned_seed_inventories") == before
    reread = await h.env.client.get(f"{h.env.url}/{h.result['id']}")
    assert reread.json()["manifest"] == h.result["manifest"]


@pytest.mark.parametrize("action", ["patch", "resume", "rebind", "identity_drift"])
async def test_actual_populated_history_binding_permanently_denies_producer_changes(a2_history, action):
    h = a2_history
    async with h.case.sessions() as db:
        await seal(h, db)
        await db.commit()
        await db.execute(update(OwnedSeedInventory).where(OwnedSeedInventory.id == uuid.UUID(h.result["id"])).values(
            state="revoked", revoked_at=await db.scalar(text("SELECT clock_timestamp()")), revoked_by="fixture:operator",
            succession_released_at=await db.scalar(text("SELECT clock_timestamp()"))))
        await db.commit()
    old = h.data["history_locators"]["operations"][0]
    base = "/api/v1/channel-agent"
    if action == "patch":
        response = await h.env.client.patch(f"{base}/channels/{old['legacy_channel_profile_id']}/accounts/{old['legacy_account_id']}", json={"enabled": True})
        assert response.status_code == 409 and response.json()["detail"] == "owned_inventory_historical_producer_pinned"
    elif action == "resume":
        response = await h.env.client.post(f"{base}/accounts/{old['legacy_account_id']}/resume")
        assert response.status_code == 409 and response.json()["detail"] == "owned_inventory_historical_producer_pinned"
    else:
        sql = ("UPDATE production_tasks SET target_account_id=:value WHERE id=:id" if action == "rebind" else
               "UPDATE publishing_accounts SET platform_account_id=:value WHERE id=:id")
        async with h.case.sessions() as db:
            with pytest.raises(DBAPIError, match="owned_history_sealed"):
                await db.execute(text(sql), {"id": uuid.UUID(h.history_rows["production_tasks"][0]["id"] if action == "rebind" else old["legacy_account_id"]),
                    "value": uuid.UUID(h.env.scope["target_account_id"]) if action == "rebind" else "UC" + "z" * 22})
            await db.rollback()
    reread = await h.env.client.get(f"{h.env.url}/{h.result['id']}")
    assert reread.json()["manifest"] == h.result["manifest"]
