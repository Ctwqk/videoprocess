from __future__ import annotations

import copy
import hashlib
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.channel_agent import clients
from app.models.channel_agent import ChannelProfile, PublishingAccount, ManualSeed
from app.models.owned_seed_inventory import OwnedSeedInventory, OwnedSeedInventoryItem
from app.models.schedule import RuntimeSchedule
from app.models.asset import Asset
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
    for job in rows["jobs"]:
        job.setdefault("parent_job_id", None)
    rows["owned_seed_inventories"] = []
    old_account = rows["publishing_accounts"][0]
    old_channel = rows["channel_profiles"][0]
    observed_at = datetime.fromisoformat(fixture["now"])
    async with env.factory() as db:
        connection = await db.connection()
        await connection.run_sync(lambda sync: RuntimeSchedule.__table__.create(sync, checkfirst=True))
        db.add(ChannelProfile(id=uuid.UUID(old_channel["id"]), name="Historical channel", dry_run=False))
        db.add(PublishingAccount(id=uuid.UUID(old_account["id"]), channel_profile_id=uuid.UUID(old_channel["id"]),
                                account_label="Historical account", platform_account_id="", default_privacy="unlisted"))
        await db.commit()
    active_transactions = []

    async def snapshot(db, *, platform_channel_id):
        active_transactions[:] = [db]
        controls.db = db
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


async def test_v2_draft_is_server_qualified_and_approval_atomically_publishes_authority(history_env):
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
    approved = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result))
    assert approved.status_code == 200, approved.text
    assert approved.json()["state"] == "approved"
    assert len(h.requests) == 4
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(result["id"]))
        assert row.manifest_json == result["manifest"] and row.manifest_sha256 == result["manifest_sha256"]
        assert row.approved_at is not None and row.approved_by == "test-operator"
        assert row.approval_reference == approval(result)["approval_reference"]
        channel = await db.get(ChannelProfile, h.env.channel_id)
        assert channel.owned_seed_inventory_id == row.id and channel.tick_interval_minutes == 1
        assert channel.intake_paused_at is not None
        assert (await db.get(RuntimeSchedule, "videoprocess")).state == "CLOSED"
        assert {s.status for s in (await db.scalars(select(ManualSeed))).all()} == {"active"}
        for entry in h.data["entries"]:
            asset = await db.get(Asset, uuid.UUID(entry["asset_id"]))
            assert asset.media_info == {"duration": 30, "license": "owned", "provenance": "generated"}
        for item in (await db.scalars(select(OwnedSeedInventoryItem))).all():
            assert item.state == "unused" and item.production_task_id is None and item.consumed_at is None


@pytest.mark.parametrize("successor", [False, True])
async def test_new_authorized_observer_preserves_original_history_envelope(history_env, monkeypatch, successor):
    h = history_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    original = created.json()
    if successor:
        # Historical successor fixture; activating a v2 successor remains excluded.
        async with h.env.factory() as db:
            row = await db.get(OwnedSeedInventory, uuid.UUID(original["id"]))
            row.approved_at, row.approved_by, row.approval_reference = h.observed_at, "test-operator", "fixture:approved"
            row.state, row.revoked_at, row.revoked_by = "revoked", h.observed_at, "test-operator"
            await db.commit()
    monkeypatch.setattr(service.settings, "owned_seed_inventory_operator_subject", "rotated-operator")
    h.controls.observed_at += timedelta(seconds=1)
    if successor:
        data = await h.env.data("successor")
        data.update(version=2, history_locators=h.data["history_locators"])
        response = await h.env.client.post(h.env.url, json=data)
        assert response.status_code == 200, response.text
        assert response.json()["manifest"]["legacy_history"]["bindings"] == original["manifest"]["legacy_history"]["bindings"]
    else:
        response = await h.env.client.post(f"{h.env.url}/{original['id']}/approve", json=approval(original))
        assert response.status_code == 200, response.text
    reread = await h.env.client.get(f"{h.env.url}/{original['id']}")
    assert reread.json()["manifest"] == original["manifest"]
    assert reread.json()["manifest_sha256"] == original["manifest_sha256"]
    assert len(h.requests) == 4


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


@pytest.mark.parametrize("bad", ["actual_uc", "operation_hash", "submitted", "manager", "video", "receipt", "account", "endpoint"])
async def test_v2_approval_requalifies_without_rewriting_changed_draft(history_env, monkeypatch, bad):
    h = history_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    result = created.json()
    monkeypatch.setattr(service.settings, "owned_seed_inventory_operator_subject", "rotated-operator")
    if bad == "actual_uc":
        h.controls.actual_channel = "UC" + "z" * 22
    elif bad == "endpoint":
        clients.build_youtube_manager_client().base_url = "http://other-configured-manager"
    elif bad == "account":
        async with h.env.factory() as db:
            row = await db.get(PublishingAccount, uuid.UUID(h.data["history_locators"]["operations"][0]["legacy_account_id"]))
            row.credential_ref = "changed-test-reference"
            await db.commit()
    elif bad == "receipt":
        h.rows["youtube_upload_operations"][0]["receipt_json"]["extra"] = "changed"
    else:
        key, value = {"operation_hash": ("content_sha256", "e" * 64), "submitted": ("status", "submitted"),
                      "manager": ("manager_task_id", "changed-task"), "video": ("platform_video_id", "changed_vid")}[bad]
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


@pytest.mark.parametrize("phase", ["create", "approve"])
@pytest.mark.parametrize("seconds", [-1, 61])
async def test_v2_platform_reads_keep_the_original_database_freshness_bound(history_env, phase, seconds):
    h = history_env
    result = None
    if phase == "approve":
        created = await h.env.client.post(h.env.url, json=h.data)
        assert created.status_code == 200
        result = created.json()
    async def elapsed():
        h.controls.on_get = None
        h.controls.observed_at += timedelta(seconds=seconds)
    h.controls.on_get = elapsed
    response = await h.env.client.post(h.env.url if result is None else f"{h.env.url}/{result['id']}/approve",
        json=h.data if result is None else approval(result))
    assert response.status_code == 409 and response.json()["detail"] == "owned_inventory_history_observation_stale"
    async with h.env.factory() as db:
        rows = (await db.scalars(select(OwnedSeedInventory))).all()
        assert len(rows) == (0 if result is None else 1)
        assert all(row.approved_at is None for row in rows)


@pytest.fixture
async def retirement_env(history_env, monkeypatch):
    h = history_env
    fixture = json.loads((Path(__file__).parents[1] / "fixtures/owned_seed_inventory_history/retired_unassigned.json").read_text())
    source = fixture["rows"]["assets"][0]
    old_id, new_id = source["id"], str(uuid.uuid4())
    fixture = json.loads(json.dumps(fixture).replace(old_id, new_id))
    rows = fixture["rows"]
    rows["owned_seed_inventories"] = []
    for table, records in rows.items():
        h.rows[table].extend(records)
    channel = rows["channel_profiles"][0]
    account = rows["publishing_accounts"][0]
    asset = rows["assets"][0]
    async with h.env.factory() as db:
        db.add(ChannelProfile(id=uuid.UUID(channel["id"]), name="Retired channel", enabled=False,
            halted_at=datetime.fromisoformat(channel["halted_at"]), halt_reason=channel["halt_reason"],
            intake_paused_at=datetime.fromisoformat(channel["intake_paused_at"])))
        db.add(PublishingAccount(id=uuid.UUID(account["id"]), channel_profile_id=uuid.UUID(channel["id"]),
            account_label="Retired account", platform_account_id="", default_privacy="unlisted"))
        db.add(Asset(id=uuid.UUID(asset["id"]), filename=asset["filename"], original_name=asset["original_name"],
            mime_type=asset["mime_type"], file_size=asset["file_size"], storage_backend=asset["storage_backend"],
            storage_path=asset["storage_path"], media_info=asset["media_info"]))
        await db.commit()
    h.env.storage.blobs[asset["storage_path"]] = b"a" * asset["file_size"]
    h.data["history_locators"]["retired_unassigned_preupload"] = {"operation_id": history.RETIRED_TUPLE[0],
        "legacy_account_id": account["id"], "legacy_channel_profile_id": channel["id"]}
    markers = {"vp:worker-task-dispatch:" + row["dispatch_key"]: row["redis_message_id"] for row in rows["worker_task_dispatches"]}
    calls = []
    h.controls.redis_pending = False
    class Redis:
        async def get(self, key):
            assert not h.controls.db.in_transaction()
            calls.append(("get", key))
            return markers[key]
        async def xpending_range(self, *args):
            assert not h.controls.db.in_transaction()
            calls.append(("pending", args))
            return [{}] if h.controls.redis_pending else []
        async def aclose(self):
            calls.append(("close",))
    monkeypatch.setattr(service, "_history_redis", lambda: Redis())
    h.redis_calls = calls
    return h


async def test_v2_retirement_approval_rejects_pending_then_preserves_exact_certificate(retirement_env):
    h = retirement_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    result = created.json()
    certificate = result["manifest"]["legacy_history"]["retired_unassigned_preupload"]
    assert certificate["retained_facts"]["account"]["platform_account_id"] == ""
    assert h.redis_calls[-1] == ("close",)
    h.controls.observed_at = h.controls.observed_at.replace(microsecond=1)
    h.controls.redis_pending = True
    changed = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result))
    assert changed.status_code == 409 and changed.json()["detail"] == "owned_inventory_retirement_pending"
    reread = await h.env.client.get(f"{h.env.url}/{result['id']}")
    assert reread.json()["manifest_sha256"] == result["manifest_sha256"] and reread.json()["approved_at"] is None
    h.controls.redis_pending = False
    approved = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result))
    assert approved.status_code == 200, approved.text
    assert approved.json()["manifest"] == result["manifest"]
    assert approved.json()["approved_at"] is not None


async def test_v2_complete_retained_certificate_read_requires_existing_operator_auth(retirement_env):
    h = retirement_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    response = await h.env.client.get(f"{h.env.url}/{created.json()['id']}", headers={"Authorization": ""})
    assert response.status_code == 403 and "retained_facts" not in response.text


@pytest.mark.parametrize("action", ["patch_identity", "patch_enabled", "resume_account", "resume_channel", "pause_account", "channel_enabled", "new_account", "dry_run", "halt"])
async def test_approved_history_only_producers_cannot_be_rebound_or_reactivated(history_env, action):
    h = history_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    # Explicit prequalified historical fixture authority.
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(created.json()["id"]))
        row.approved_at, row.approved_by, row.approval_reference, row.state = h.observed_at, "test-operator", "fixture:approved", "revoked"
        await db.commit()
    locator = h.data["history_locators"]["operations"][0]
    base = "/api/v1/channel-agent"
    if action.startswith("patch"):
        payload = {"platform_account_id": "UC" + "z" * 22} if action == "patch_identity" else {"enabled": True}
        response = await h.env.client.patch(f"{base}/channels/{locator['legacy_channel_profile_id']}/accounts/{locator['legacy_account_id']}", json=payload)
    elif action == "resume_channel":
        response = await h.env.client.post(f"{base}/channels/{locator['legacy_channel_profile_id']}/resume")
    elif action == "channel_enabled":
        response = await h.env.client.patch(f"{base}/channels/{locator['legacy_channel_profile_id']}", json={"enabled": True})
    elif action == "new_account":
        response = await h.env.client.post(f"{base}/channels/{locator['legacy_channel_profile_id']}/accounts", json={"account_label": "new"})
    elif action == "dry_run":
        response = await h.env.client.patch(f"{base}/channels/{locator['legacy_channel_profile_id']}/dry-run", json={"dry_run": False})
    elif action == "halt":
        response = await h.env.client.post(f"{base}/channels/{locator['legacy_channel_profile_id']}/halt", json={"reason": "changed"})
    else:
        command = "pause" if action == "pause_account" else "resume"
        response = await h.env.client.post(f"{base}/accounts/{locator['legacy_account_id']}/{command}", json={})
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "owned_inventory_historical_producer_pinned"


async def test_retired_source_asset_stays_pinned_after_revocation(retirement_env):
    h = retirement_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    result = created.json()
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(result["id"]))
        row.approved_at, row.approved_by, row.approval_reference, row.state = h.observed_at, "test-operator", "fixture:approved", "revoked"
        await db.commit()
    source = result["manifest"]["legacy_history"]["retired_unassigned_preupload"]["retained_facts"]["source_assets"][0]["asset"]
    response = await h.env.client.delete(f"/api/v1/assets/{source['id']}")
    assert response.status_code == 409 and response.json()["detail"] == "owned_inventory_asset_pinned"
    async with h.env.factory() as db:
        assert await db.get(Asset, uuid.UUID(source["id"])) is not None


@pytest.mark.parametrize("terminal", [False, True])
async def test_v2_exact_approval_replay_is_observation_not_reactivation(history_env, terminal):
    h = history_env
    result = (await h.env.client.post(h.env.url, json=h.data)).json()
    url = f"{h.env.url}/{result['id']}"
    first = await h.env.client.post(url + "/approve", json=approval(result))
    assert first.status_code == 200, first.text
    if terminal:
        response = await h.env.client.post(url + "/revoke", json={
            "manifest_sha256": result["manifest_sha256"], "reason": "operator stop"})
        assert response.status_code == 200, response.text
    # Replay must work after the initial pause has changed, without new qualification.
    async with h.env.factory() as db:
        channel = await db.get(ChannelProfile, h.env.channel_id)
        if not terminal:
            channel.intake_paused_at = None
        await db.commit()
    before = len(h.requests), len(h.env.storage.reads)
    h.controls.actual_channel = "UC" + "z" * 22
    repeated = await h.env.client.post(url + "/approve", json=approval(result))
    assert repeated.status_code == 200, repeated.text
    assert repeated.json()["approved_at"] == first.json()["approved_at"]
    assert repeated.json()["state"] == ("revoked" if terminal else "approved")
    assert (len(h.requests), len(h.env.storage.reads)) == before
    async with h.env.factory() as db:
        assert {s.status for s in (await db.scalars(select(ManualSeed))).all()} == (
            {"inventory_revoked"} if terminal else {"active"})


@pytest.mark.parametrize("conflict", ["reference", "subject", "digest", "successor"])
async def test_v2_approval_replay_rejects_conflicting_authority(history_env, monkeypatch, conflict):
    h = history_env
    result = (await h.env.client.post(h.env.url, json=h.data)).json()
    url = f"{h.env.url}/{result['id']}/approve"
    first = await h.env.client.post(url, json=approval(result))
    assert first.status_code == 200, first.text
    data = approval(result)
    if conflict == "subject":
        monkeypatch.setattr(service.settings, "owned_seed_inventory_operator_subject", "another-operator")
    elif conflict == "reference":
        data["approval_reference"] = "different reference"
    elif conflict == "digest":
        data["manifest_sha256"] = "f" * 64
    else:
        data.update(predecessor_inventory_id=str(uuid.uuid4()), predecessor_closeout_sha256="a" * 64)
    before = len(h.requests)
    response = await h.env.client.post(url, json=data)
    assert response.status_code == 409, response.text
    assert len(h.requests) == before
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(result["id"]))
        assert row.approval_reference == approval(result)["approval_reference"]
        assert row.approved_by == "test-operator" and row.manifest_json == result["manifest"]


async def test_v2_finalization_failure_rolls_back_every_approval_write(history_env, monkeypatch):
    h = history_env
    result = (await h.env.client.post(h.env.url, json=h.data)).json()
    original = AsyncSession.commit
    attempted = []
    async def fail_approval(db):
        if any(isinstance(row, OwnedSeedInventory) and row.approved_at is not None for row in db.dirty):
            await db.flush()
            attempted.append(True)
            raise service.OwnedInventoryError("synthetic_commit_failure")
        await original(db)
    monkeypatch.setattr(AsyncSession, "commit", fail_approval)
    response = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result))
    assert response.status_code == 409 and attempted == [True], response.text
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(result["id"]))
        assert row.state == "draft" and row.approved_at is None and row.manifest_json == result["manifest"]
        channel = await db.get(ChannelProfile, h.env.channel_id)
        assert channel.owned_seed_inventory_id is None and channel.tick_interval_minutes == 60
        assert {s.status for s in (await db.scalars(select(ManualSeed))).all()} == {"inventory_pending"}
        for entry in h.data["entries"]:
            assert (await db.get(Asset, uuid.UUID(entry["asset_id"]))).media_info == {"duration": 30}


async def test_v2_draft_cannot_request_successor_authority(history_env):
    h = history_env
    result = (await h.env.client.post(h.env.url, json=h.data)).json()
    before = len(h.requests)
    response = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result,
        predecessor_inventory_id=str(uuid.uuid4()), predecessor_closeout_sha256="a" * 64))
    assert response.status_code == 409
    assert response.json()["detail"] == "owned_inventory_v2_successor_unsupported"
    assert len(h.requests) == before


@pytest.mark.parametrize("same", [True, False])
async def test_v2_approval_committed_after_initial_draft_read_is_replayed_or_conflicted(history_env, same):
    h = history_env
    result = (await h.env.client.post(h.env.url, json=h.data)).json()
    url = f"{h.env.url}/{result['id']}/approve"
    winner = []
    async def concurrent_approval():
        h.controls.on_get = None
        response = await h.env.client.post(url, json=approval(result))
        assert response.status_code == 200, response.text
        winner.append(response.json())
    h.controls.on_get = concurrent_approval
    data = approval(result)
    if not same:
        data["approval_reference"] = "losing reference"
    response = await h.env.client.post(url, json=data)
    assert response.status_code == (200 if same else 409), response.text
    assert len(winner) == 1
    async with h.env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(result["id"]))
        assert row.approval_reference == approval(result)["approval_reference"]
        assert service.utc(row.approved_at) == service.utc(datetime.fromisoformat(winner[0]["approved_at"]))


async def test_v2_final_lock_delay_cannot_extend_native_observation_freshness(history_env, monkeypatch):
    h = history_env
    result = (await h.env.client.post(h.env.url, json=h.data)).json()
    original = service._requalify_v2_draft
    async def delayed(*args, **kwargs):
        qualified = await original(*args, **kwargs)
        h.controls.observed_at += timedelta(seconds=61)
        return qualified
    monkeypatch.setattr(service, "_requalify_v2_draft", delayed)
    response = await h.env.client.post(f"{h.env.url}/{result['id']}/approve", json=approval(result))
    assert response.status_code == 409, response.text
    assert response.json()["detail"] == "owned_inventory_history_observation_stale"
