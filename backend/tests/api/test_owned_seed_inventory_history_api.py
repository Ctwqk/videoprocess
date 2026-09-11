from __future__ import annotations

import copy
import hashlib
import json
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select

from app.channel_agent import clients
from app.models.channel_agent import ChannelProfile, PublishingAccount, ManualSeed
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.schedule import RuntimeSchedule
from app.schemas import channel_agent as schemas
from app.services import owned_seed_inventory as service
from app.services import owned_seed_inventory_history as history
from test_owned_seed_inventory import approval, inventory_env as inventory_env


def locator():
    return {"operation_id": str(uuid.UUID(int=1)), "legacy_account_id": str(uuid.UUID(int=2)),
            "legacy_channel_profile_id": str(uuid.UUID(int=3))}


async def test_v2_request_carries_only_typed_locators_and_preserves_v1_rejection(inventory_env):
    data = await inventory_env.data()
    data.update(version=2, history_locators={"operations": [locator()]})
    parsed = schemas.OwnedSeedInventoryCreateV2.model_validate(data)
    assert parsed.history_locators.operations[0].operation_id == locator()["operation_id"]
    assert parsed.version == 2
    with pytest.raises(ValidationError):
        schemas.OwnedSeedInventoryCreate.model_validate(data)


@pytest.mark.parametrize("bad", ["subject", "endpoint", "report", "facts", "safe", "legacy_history", "duplicate",
                                 "unsorted", "uuid", "version_bool", "version_float", "unknown_retired"])
async def test_v2_request_rejects_caller_authority_and_ambiguous_locators(inventory_env, bad):
    data = await inventory_env.data()
    history = {"operations": [locator()]}
    data.update(version=2, history_locators=history)
    if bad in {"subject", "endpoint", "report", "facts", "safe"}:
        field = {"subject": "server_subject", "endpoint": "manager_url", "report": "report_path",
                 "facts": "sanitized_facts", "safe": "safe"}[bad]
        history["operations"][0][field] = "caller-sentinel"
    elif bad == "legacy_history":
        data["legacy_history"] = {"bindings": []}
    elif bad == "duplicate":
        history["operations"].append(copy.deepcopy(locator()))
    elif bad == "unsorted":
        history["operations"].insert(0, {**locator(), "operation_id": str(uuid.UUID(int=10))})
    elif bad == "uuid":
        history["operations"][0]["operation_id"] = "not-a-uuid"
    elif bad == "unknown_retired":
        history["retired_unassigned_preupload"] = locator()
    else:
        data["version"] = True if bad == "version_bool" else 2.0
    with pytest.raises(ValidationError):
        schemas.OwnedSeedInventoryCreateV2.model_validate(data)


@pytest.fixture
async def history_env(inventory_env, monkeypatch):
    env = inventory_env
    fixture = json.loads((Path(__file__).parents[1] / "fixtures/owned_seed_inventory_history/history_only.json").read_text())
    identities = {fixture["rows"]["publishing_accounts"][0]["id"]: "bbbbbbbb-0000-0000-0000-000000000003",
                  fixture["rows"]["channel_profiles"][0]["id"]: "aaaaaaaa-0000-0000-0000-000000000002"}
    def remap(value):
        if isinstance(value, dict):
            return {key: remap(item) for key, item in value.items()}
        if isinstance(value, list):
            return [remap(item) for item in value]
        return identities.get(value, value) if isinstance(value, str) else value
    rows = remap(fixture["rows"])
    rows["owned_seed_inventories"] = []
    old_account = rows["publishing_accounts"][0]
    old_channel = rows["channel_profiles"][0]
    observed_at = datetime.fromisoformat(fixture["now"])
    async with env.factory() as db:
        connection = await db.connection()
        await connection.run_sync(lambda sync: RuntimeSchedule.__table__.create(sync, checkfirst=True))
        db.add(RuntimeSchedule(service_name="videoprocess", state="CLOSED"))
        db.add(ChannelProfile(id=uuid.UUID(old_channel["id"]), name="Historical channel", dry_run=False))
        db.add(PublishingAccount(id=uuid.UUID(old_account["id"]), channel_profile_id=uuid.UUID(old_channel["id"]),
                                account_label="Historical account", platform_account_id="", default_privacy="unlisted"))
        await db.commit()
    active_transactions = []

    async def snapshot(db, *, platform_channel_id):
        active_transactions[:] = [db]
        current = copy.deepcopy(rows)
        for model in (OwnedSeedInventory, OwnedSeedInventoryItem, ChannelProfile, PublishingAccount, RuntimeSchedule):
            values = list((await db.scalars(select(model))).all())
            name = model.__tablename__
            key = "service_name" if name == "runtime_schedules" else "id"
            combined = {r[key]: r for r in current[name]}
            for value in values:
                row = json.loads(service.canonical({c.name: getattr(value, c.name) for c in model.__table__.columns}))
                combined[row[key]] = row
            current[name] = list(combined.values())
        return history.OwnedHistorySnapshot.from_rows(current, platform_channel_id=platform_channel_id, observed_at=controls.observed_at)

    monkeypatch.setattr(history, "load_owned_history_evidence", snapshot)
    async def now(_db):
        return controls.observed_at
    monkeypatch.setattr(service, "_now", now)
    requests = []
    controls = SimpleNamespace(on_get=None, actual_channel=env.scope["platform_channel_id"], task_status="completed", observed_at=observed_at)

    async def handler(request):
        assert active_transactions and not active_transactions[0].in_transaction()
        requests.append((request.method, request.url.path))
        assert request.method == "GET"
        if controls.on_get:
            await controls.on_get()
        op = rows["youtube_upload_operations"][0]
        if request.url.path == f"/api/status/{op['manager_task_id']}":
            return httpx.Response(200, json={"id": op["manager_task_id"], "type": "upload", "status": controls.task_status,
                "progress": 100, "result": {"video_id": op["platform_video_id"], "url": op["receipt_json"]["url"]}, "error": None})
        assert request.url.path == f"/api/videos/{op['platform_video_id']}/status"
        return httpx.Response(200, json={"video_id": op["platform_video_id"], "privacy": "unlisted", "upload_status": "processed",
            "processing_status": "succeeded", "title": "Synthetic owned video", "published_at": None,
            "made_for_kids": False, "public_stats_viewable": True,
            "raw": {"status": {}, "snippet": {"channelId": controls.actual_channel}, "processingDetails": {}}})

    manager = clients.YouTubeManagerClient(base_url="http://configured-manager", transport=httpx.MockTransport(handler))
    monkeypatch.setattr(clients, "build_youtube_manager_client", lambda: manager)
    data = await env.data("history")
    data.update(version=2, history_locators={"operations": [{"operation_id": op["id"],
        "legacy_account_id": old_account["id"], "legacy_channel_profile_id": old_channel["id"]}
        for op in rows["youtube_upload_operations"]]})
    return SimpleNamespace(env=env, rows=rows, data=data, requests=requests, controls=controls, observed_at=observed_at)


async def test_v2_draft_is_server_qualified_immutable_and_never_producer_authority(history_env):
    h = history_env
    response = await h.env.client.post(h.env.url, json=h.data)
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["state"] == "draft" and result["approved_at"] is None
    binding = result["manifest"]["legacy_history"]["bindings"][0]
    assert binding["canonical_platform_channel_id"] == h.env.scope["platform_channel_id"]
    assert binding["use"] == "history_only" and binding["qualification"]["server_subject"] == "test-operator"
    assert binding["qualification"]["manager_endpoint_identity"] == "sha256:" + hashlib.sha256(b"http://configured-manager").hexdigest()
    assert len(binding["qualification"]["sanitized_facts"]) == len(h.data["history_locators"]["operations"])
    assert "configured-manager" not in response.text
    assert "credential_ref" not in response.text
    assert len(h.requests) == 2
    repeated = await h.env.client.post(h.env.url, json=h.data)
    assert repeated.status_code == 200 and repeated.json()["manifest_sha256"] == result["manifest_sha256"]
    assert len(h.requests) == 2
    h.controls.observed_at = h.controls.observed_at.replace(microsecond=1)
    denied = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result))
    assert denied.status_code == 409 and denied.json()["detail"] == "owned_inventory_v2_activation_disabled"
    assert len(h.requests) == 4
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(result["id"]))
        assert row.manifest_sha256 == result["manifest_sha256"] and row.approved_at is None
        channel = await db.get(ChannelProfile, h.env.channel_id)
        assert channel.owned_seed_inventory_id is None
        assert {s.status for s in (await db.scalars(select(ManualSeed))).all()} == {"inventory_pending"}


@pytest.mark.parametrize("bad", ["submitted", "actual_uc", "foreign_task", "drift", "new_operation"])
async def test_v2_qualification_rejects_unknown_or_changing_effects_without_draft(history_env, bad):
    h = history_env
    if bad == "submitted":
        h.rows["youtube_upload_operations"][0]["status"] = "submitted"
    elif bad == "actual_uc":
        h.controls.actual_channel = "UC" + "z" * 22
    elif bad == "foreign_task":
        h.rows["production_tasks"][0]["target_account_id"] = str(uuid.UUID(int=99))
    else:
        async def change():
            h.controls.on_get = None
            if bad == "drift":
                h.rows["youtube_upload_operations"][0]["title"] = "drift"
            else:
                h.rows["youtube_upload_operations"].append({**h.rows["youtube_upload_operations"][0], "id": str(uuid.UUID(int=999))})
        h.controls.on_get = change
    response = await h.env.client.post(h.env.url, json=h.data)
    assert response.status_code == 409, response.text
    async with h.env.factory() as db:
        assert not (await db.scalars(select(OwnedSeedInventory))).all()
        assert not (await db.scalars(select(ManualSeed))).all()
    if bad in {"submitted", "foreign_task"}:
        assert h.requests == []


@pytest.mark.parametrize("bad", ["actual_uc", "operation_hash", "submitted"])
async def test_v2_approval_requalifies_without_rewriting_changed_draft(history_env, bad):
    h = history_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    result = created.json()
    if bad == "actual_uc":
        h.controls.actual_channel = "UC" + "z" * 22
    else:
        key, value = ("content_sha256", "e" * 64) if bad == "operation_hash" else ("status", "submitted")
        h.rows["youtube_upload_operations"][0][key] = value
    response = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result))
    assert response.status_code == 409 and response.json()["detail"] != "owned_inventory_v2_activation_disabled"
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(result["id"]))
        assert row.manifest_json == result["manifest"] and row.manifest_sha256 == result["manifest_sha256"]
        assert row.approved_at is None and row.state == "draft"


async def test_v2_replay_cannot_change_locator_scope_or_requalify_existing_digest(history_env):
    h = history_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    h.data["history_locators"]["operations"][0]["operation_id"] = str(uuid.UUID(int=999))
    response = await h.env.client.post(h.env.url, json=h.data)
    assert response.status_code == 409 and response.json()["detail"] == "owned_inventory_idempotency_conflict"
    assert len(h.requests) == 2


async def test_v2_missing_configured_client_is_static_and_cannot_create_draft(history_env, monkeypatch):
    h = history_env
    def unavailable():
        raise RuntimeError("private-sentinel")
    monkeypatch.setattr(clients, "build_youtube_manager_client", unavailable)
    response = await h.env.client.post(h.env.url, json=h.data)
    assert response.status_code == 409 and "private-sentinel" not in response.text
    async with h.env.factory() as db:
        assert not (await db.scalars(select(OwnedSeedInventory))).all()
    assert h.requests == []


async def test_v2_ambiguous_global_manager_identity_blocks_before_http(history_env):
    h = history_env
    h.rows["youtube_upload_operations"].append({**h.rows["youtube_upload_operations"][0],
        "id": str(uuid.UUID(int=999)), "production_task_id": str(uuid.UUID(int=998))})
    response = await h.env.client.post(h.env.url, json=h.data)
    assert response.status_code == 409
    assert h.requests == []
