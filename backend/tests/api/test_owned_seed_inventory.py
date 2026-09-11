from __future__ import annotations

import copy
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api.assets import router as assets_router
from app.api.channel_agent import router
from app.config import settings
from app.db import get_db
from app.models import Base
from app.models.asset import Asset
from app.models.channel_agent import (
    ChannelProfile, LaneFormatMatrix, ManualSeed, PublishingAccount, TopicLane,
)


TOKEN = "inventory-test-capability-never-persist-this"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=True, allow_nan=False).encode()).hexdigest()


class FakeStorage:
    def __init__(self):
        self.blobs = {}
        self.reads = []
        self.deletions = []
        self.on_read = None

    async def read_bounded(self, path, max_bytes):
        self.reads.append((path, max_bytes))
        if self.on_read:
            await self.on_read(path)
        content = self.blobs[path]
        if len(content) > max_bytes:
            raise ValueError("oversized")
        return content

    async def delete(self, path):
        self.deletions.append(path)
        self.blobs.pop(path, None)


@pytest.fixture
async def inventory_env(monkeypatch, request):
    database_url = getattr(request, "param", "sqlite+aiosqlite:///:memory:")
    if database_url != "sqlite+aiosqlite:///:memory:":
        parsed = make_url(database_url)
        assert parsed.drivername == "postgresql+asyncpg"
        assert parsed.database and parsed.database.startswith("vp_owned_inventory_test_")
        assert os.environ.get("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM") == parsed.database
    engine = create_async_engine(database_url)
    names = (
        "assets", "channel_profiles", "topic_lanes", "publishing_accounts", "lane_format_matrix",
        "manual_seeds", "jobs", "node_executions", "production_tasks", "channel_ops_queue_items",
        "youtube_upload_operations", "publication_records", "publication_promotion_operations",
        "owned_seed_inventories", "owned_seed_inventory_items",
    )
    async with engine.begin() as connection:
        if database_url.startswith("sqlite"):
            await connection.run_sync(lambda sync: Base.metadata.create_all(
                sync, tables=[Base.metadata.tables[name] for name in names if name in Base.metadata.tables],
            ))
        else:
            assert (await connection.scalar(text("SELECT version_num FROM alembic_version"))) == "037_owned_seed_inventory"
    factory = async_sessionmaker(engine, expire_on_commit=False)
    storage = FakeStorage()
    monkeypatch.setattr("app.storage.manager.get_storage", lambda *a, **kw: storage)
    monkeypatch.setattr("app.services.asset_service.get_storage", lambda *a, **kw: storage)
    monkeypatch.setitem(settings.__dict__, "owned_seed_inventory_enabled", True)
    monkeypatch.setitem(settings.__dict__, "owned_seed_inventory_operator_token", SecretStr(TOKEN))
    monkeypatch.setitem(settings.__dict__, "owned_seed_inventory_operator_subject", "test-operator")
    async with factory() as db:
        channel = ChannelProfile(name="finite owned", enabled=True, dry_run=False,
                                 intake_paused_at=datetime.now(timezone.utc))
        db.add(channel)
        await db.flush()
        lane = TopicLane(channel_profile_id=channel.id, name="owned lane")
        platform_channel = "UC" + uuid.uuid4().hex[:22]
        account = PublishingAccount(channel_profile_id=channel.id, account_label="owned",
                                    platform_account_id=platform_channel, default_privacy="unlisted",
                                    credential_ref="private-credential-reference")
        db.add_all([lane, account])
        await db.flush()
        lane_format = LaneFormatMatrix(topic_lane_id=lane.id, format_key="owned",
                                       source_platforms_json=[], default_publish_visibility="unlisted")
        db.add(lane_format)
        await db.commit()
        scope = dict(topic_lane_id=str(lane.id), lane_format_id=str(lane_format.id),
                     target_account_id=str(account.id), platform_channel_id=platform_channel)
        channel_id = channel.id

    async def request_data(label="first"):
        entries = []
        async with factory() as db:
            for ordinal in range(7):
                content = f"owned-generated-{label}-{ordinal}".encode()
                path = f"assets/{uuid.uuid4()}.mp4"
                storage.blobs[path] = content
                asset = Asset(filename="owned.mp4", original_name="owned.mp4", mime_type="video/mp4",
                              file_size=len(content), storage_backend="local", storage_path=path,
                              media_info={"duration": 30})
                db.add(asset)
                await db.flush()
                entries.append(dict(asset_id=str(asset.id), expected_content_sha256=hashlib.sha256(content).hexdigest(),
                                    provenance_evidence=dict(rights="owned", provenance="generated",
                                                             evidence_reference=f"owned-record:{label}:{ordinal}",
                                                             evidence_sha256="b" * 64,
                                                             attestation="I attest this video is owned and generated."),
                                    prompt=f"Owned item {ordinal}", title_seed=f"Owned {ordinal}"))
            await db.commit()
        starts = datetime.now(timezone.utc) + timedelta(hours=1)
        return dict(client_request_id=str(uuid.uuid4()), **scope, starts_at=starts.isoformat(),
                    expires_at=(starts + timedelta(days=7)).isoformat(), privacy="unlisted",
                    max_admissions=7, minimum_interval_seconds=86400, entries=entries)

    app = FastAPI()
    app.include_router(router)
    app.include_router(assets_router)

    async def session_override():
        async with factory() as db:
            yield db

    app.dependency_overrides[get_db] = session_override
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test",
                           headers={"Authorization": f"Bearer {TOKEN}"}) as client:
        yield SimpleNamespace(client=client, factory=factory, storage=storage, data=request_data,
                              channel_id=channel_id, scope=scope,
                              url=f"/api/v1/channel-agent/channels/{channel_id}/owned-seed-inventories")
    await engine.dispose()


async def draft(env, label="first"):
    data = await env.data(label)
    response = await env.client.post(env.url, json=data)
    assert response.status_code == 200, response.text
    return data, response.json()


def approval(row, **extra):
    return dict(manifest_sha256=row["manifest_sha256"], approval_reference="review:owned-seven",
                tick_interval_minutes=1, **extra)


async def test_draft_approval_revoke_are_atomic_idempotent_and_secret_free(inventory_env):
    env = inventory_env
    data, row = await draft(env)
    assert row["state"] == "draft"
    assert digest(row["manifest"]) == row["manifest_sha256"]
    repeat = await env.client.post(env.url, json=data)
    assert repeat.json()["id"] == row["id"]
    async with env.factory() as db:
        seeds = list((await db.scalars(select(ManualSeed))).all())
        assert len(seeds) == 7
        assert {seed.status for seed in seeds} == {"inventory_pending"}
        assert (await db.get(ChannelProfile, env.channel_id)).tick_interval_minutes == 60
        for item in data["entries"]:
            assert (await db.get(Asset, uuid.UUID(item["asset_id"]))).media_info == {"duration": 30}
    url = f"{env.url}/{row['id']}"
    result = await env.client.post(url + "/approve", json=approval(row))
    assert result.status_code == 200, result.text
    approved = result.json()
    assert approved["state"] == "approved"
    assert approved["approved_by"] == "test-operator"
    assert approved["manifest_sha256"] == row["manifest_sha256"]
    again = await env.client.post(url + "/approve", json=approval(row))
    assert again.json()["approved_at"] == approved["approved_at"]
    async with env.factory() as db:
        channel = await db.get(ChannelProfile, env.channel_id)
        assert str(channel.owned_seed_inventory_id) == row["id"]
        assert channel.tick_interval_minutes == 1
        assert channel.intake_paused_at is not None
        assert {seed.status for seed in (await db.scalars(select(ManualSeed))).all()} == {"active"}
        for item in data["entries"]:
            asset = await db.get(Asset, uuid.UUID(item["asset_id"]))
            assert asset.media_info == {"duration": 30, "license": "owned", "provenance": "generated"}
    body = {"manifest_sha256": row["manifest_sha256"], "reason": "operator closeout"}
    revoked = await env.client.post(url + "/revoke", json=body)
    assert revoked.status_code == 200
    assert revoked.json()["state"] == "revoked"
    assert (await env.client.post(url + "/revoke", json=body)).json() == revoked.json()
    public_read = await env.client.get(url, headers={"Authorization": ""})
    assert public_read.status_code == 200
    assert public_read.json()["closeout"]["status"] == "ready"
    assert TOKEN not in public_read.text
    assert "private-credential-reference" not in public_read.text


@pytest.mark.parametrize("mode", ["disabled", "unset", "wrong", "missing", "subject"])
async def test_mutations_require_configured_capability(inventory_env, monkeypatch, mode):
    env = inventory_env
    data = await env.data()
    headers = {}
    if mode == "disabled":
        monkeypatch.setitem(settings.__dict__, "owned_seed_inventory_enabled", False)
    elif mode == "unset":
        monkeypatch.setitem(settings.__dict__, "owned_seed_inventory_operator_token", SecretStr(""))
    elif mode == "subject":
        monkeypatch.setitem(settings.__dict__, "owned_seed_inventory_operator_subject", "")
    else:
        headers["Authorization"] = "Bearer wrong" if mode == "wrong" else ""
    response = await env.client.post(env.url, json=data, headers=headers)
    assert response.status_code == 403
    assert TOKEN not in response.text
    assert env.storage.reads == []


@pytest.mark.parametrize("mutation", ["six", "eight", "duplicate_id", "duplicate_hash", "public", "private",
                                       "interval", "count", "naive", "wrong_window", "actor", "external",
                                       "empty_evidence", "bad_hash", "noncanonical_id"])
async def test_strict_draft_contract_rejects_unsafe_input(inventory_env, mutation):
    env = inventory_env
    data = await env.data()
    if mutation == "six":
        data["entries"].pop()
    elif mutation == "eight":
        data["entries"].append(copy.deepcopy(data["entries"][0]))
    elif mutation == "duplicate_id":
        data["entries"][1]["asset_id"] = data["entries"][0]["asset_id"]
    elif mutation == "duplicate_hash":
        data["entries"][1]["expected_content_sha256"] = data["entries"][0]["expected_content_sha256"]
    elif mutation in {"public", "private"}:
        data["privacy"] = mutation
    elif mutation == "interval":
        data["minimum_interval_seconds"] = 1
    elif mutation == "count":
        data["max_admissions"] = 8
    elif mutation == "naive":
        data["starts_at"] = "2030-01-01T00:00:00"
    elif mutation == "wrong_window":
        data["expires_at"] = data["starts_at"]
    elif mutation == "actor":
        data["approved_by"] = "untrusted actor"
    elif mutation == "external":
        data["entries"][0]["provenance_evidence"]["rights"] = "external"
    elif mutation == "empty_evidence":
        data["entries"][0]["provenance_evidence"]["attestation"] = " "
    elif mutation == "bad_hash":
        data["entries"][0]["expected_content_sha256"] = "x" * 64
    else:
        data["entries"][0]["asset_id"] = data["entries"][0]["asset_id"].upper()
    response = await env.client.post(env.url, json=data)
    assert response.status_code == 422
    assert env.storage.reads == []


@pytest.mark.parametrize("mutation", ["wrong_lane", "wrong_account", "wrong_format", "wrong_platform",
                                       "bytes", "size", "not_video", "unsafe_path"])
async def test_scope_and_actual_bytes_fail_closed_without_draft(inventory_env, mutation):
    env = inventory_env
    data = await env.data()
    if mutation.startswith("wrong_"):
        field = {"wrong_lane": "topic_lane_id", "wrong_account": "target_account_id",
                 "wrong_format": "lane_format_id", "wrong_platform": "platform_channel_id"}[mutation]
        data[field] = "UC" + "z" * 22 if mutation == "wrong_platform" else str(uuid.uuid4())
    else:
        async with env.factory() as db:
            asset = await db.get(Asset, uuid.UUID(data["entries"][0]["asset_id"]))
            if mutation == "bytes":
                env.storage.blobs[asset.storage_path] = b"replacement"
            elif mutation == "size":
                asset.file_size = 100 * 1024 * 1024
            elif mutation == "not_video":
                asset.mime_type = "text/plain"
            else:
                asset.storage_path = "../secret"
            await db.commit()
    response = await env.client.post(env.url, json=data)
    assert response.status_code == 409, response.text
    async with env.factory() as db:
        assert list((await db.scalars(select(ManualSeed))).all()) == []


@pytest.mark.parametrize("mutation", ["manifest", "bytes", "seed", "account", "descriptor", "actor", "interval"])
async def test_approve_rechecks_immutable_binding(inventory_env, mutation):
    env = inventory_env
    data, row = await draft(env)
    body = approval(row)
    if mutation == "manifest":
        body["manifest_sha256"] = "0" * 64
    elif mutation == "actor":
        body["approved_by"] = "untrusted"
    elif mutation == "interval":
        body["tick_interval_minutes"] = 60
    else:
        async with env.factory() as db:
            asset = await db.get(Asset, uuid.UUID(data["entries"][0]["asset_id"]))
            if mutation == "bytes":
                env.storage.blobs[asset.storage_path] = b"changed"
            elif mutation == "descriptor":
                asset.storage_path = "assets/changed.mp4"
            elif mutation == "seed":
                seed = (await db.scalars(select(ManualSeed))).first()
                seed.prompt = "new prompt"
            else:
                account = await db.get(PublishingAccount, uuid.UUID(env.scope["target_account_id"]))
                account.default_privacy = "public"
            await db.commit()
    response = await env.client.post(f"{env.url}/{row['id']}/approve", json=body)
    assert response.status_code == (422 if mutation in {"actor", "interval"} else 409)
    async with env.factory() as db:
        assert (await db.get(ChannelProfile, env.channel_id)).owned_seed_inventory_id is None


async def test_approved_asset_delete_is_denied_before_blob_delete(inventory_env):
    env = inventory_env
    data, row = await draft(env)
    assert (await env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row))).status_code == 200
    response = await env.client.delete(f"/api/v1/assets/{data['entries'][0]['asset_id']}")
    assert response.status_code == 409
    assert env.storage.deletions == []


async def test_explicit_successor_requires_terminal_zero_work_closeout(inventory_env):
    env = inventory_env
    _, old = await draft(env)
    old_url = f"{env.url}/{old['id']}"
    assert (await env.client.post(old_url + "/approve", json=approval(old))).status_code == 200
    _, new = await draft(env, "second")
    new_url = f"{env.url}/{new['id']}/approve"
    assert (await env.client.post(new_url, json=approval(new))).status_code == 409
    revoked = await env.client.post(old_url + "/revoke", json={"manifest_sha256": old["manifest_sha256"], "reason": "unused closeout"})
    closeout = revoked.json()["closeout"]
    assert closeout["status"] == "ready"
    bad = approval(new, predecessor_inventory_id=old["id"], predecessor_closeout_sha256="0" * 64)
    assert (await env.client.post(new_url, json=bad)).status_code == 409
    good = approval(new, predecessor_inventory_id=old["id"], predecessor_closeout_sha256=closeout["sha256"])
    response = await env.client.post(new_url, json=good)
    assert response.status_code == 200, response.text
    async with env.factory() as db:
        assert str((await db.get(ChannelProfile, env.channel_id)).owned_seed_inventory_id) == new["id"]
    history = (await env.client.get(old_url)).json()
    assert history["succession_released_at"] is not None
    assert history["manifest_sha256"] == old["manifest_sha256"]


@pytest.mark.parametrize("field", ["owned_seed_inventory_id", "tick_interval_minutes"])
async def test_generic_channel_patch_cannot_set_inventory_authority(inventory_env, field):
    env = inventory_env
    response = await env.client.patch(f"/api/v1/channel-agent/channels/{env.channel_id}",
                                      json={field: str(uuid.uuid4()) if field.endswith("id") else 1})
    assert response.status_code == 400


async def test_account_alias_cannot_bypass_occupied_platform_slot(inventory_env):
    env = inventory_env
    _, row = await draft(env)
    assert (await env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row))).status_code == 200
    async with env.factory() as db:
        other = ChannelProfile(name="other")
        db.add(other)
        await db.commit()
    response = await env.client.post(f"/api/v1/channel-agent/channels/{other.id}/accounts", json={
        "account_label": "alias", "platform_account_id": env.scope["platform_channel_id"], "default_privacy": "unlisted",
    })
    assert response.status_code == 409


@pytest.mark.parametrize("state", ["held", "expired", "exhausted"])
async def test_terminal_label_without_audited_closeout_cannot_release_slot(inventory_env, state):
    from app.models.owned_seed_inventory import OwnedSeedInventory

    env = inventory_env
    _, old = await draft(env)
    url = f"{env.url}/{old['id']}"
    assert (await env.client.post(url + "/approve", json=approval(old))).status_code == 200
    async with env.factory() as db:
        row = await db.get(OwnedSeedInventory, uuid.UUID(old["id"]))
        row.state = state
        await db.commit()
    observed = (await env.client.get(url)).json()
    assert observed["closeout"]["status"] == "unresolved"


@pytest.mark.parametrize("changed", ["asset", "config"])
async def test_hash_observation_is_rechecked_after_storage_read(inventory_env, changed):
    env = inventory_env
    data = await env.data()
    changed_once = False

    async def mutate(_path):
        nonlocal changed_once
        if changed_once:
            return
        changed_once = True
        async with env.factory() as db:
            if changed == "asset":
                row = await db.get(Asset, uuid.UUID(data["entries"][0]["asset_id"]))
                row.file_size += 1
            else:
                row = await db.get(LaneFormatMatrix, uuid.UUID(env.scope["lane_format_id"]))
                row.target_duration_sec += 1
            await db.commit()

    env.storage.on_read = mutate
    response = await env.client.post(env.url, json=data)
    assert response.status_code == 409
    async with env.factory() as db:
        assert list((await db.scalars(select(ManualSeed))).all()) == []


async def test_storage_exception_never_exposes_provider_error(inventory_env):
    env = inventory_env
    data = await env.data()

    async def broken(_path):
        raise RuntimeError("private-provider-credential")

    env.storage.on_read = broken
    response = await env.client.post(env.url, json=data)
    assert response.status_code == 409
    assert "private-provider-credential" not in response.text


async def test_approved_account_identity_cannot_be_patched(inventory_env):
    env = inventory_env
    _, row = await draft(env)
    assert (await env.client.post(f"{env.url}/{row['id']}/approve", json=approval(row))).status_code == 200
    response = await env.client.patch(f"/api/v1/channel-agent/channels/{env.channel_id}/accounts/{env.scope['target_account_id']}",
                                      json={"platform_account_id": "UC" + "z" * 22})
    assert response.status_code == 409


@pytest.mark.parametrize("risk", ["cancelled_job_live_node", "cancelled_queue_lease"])
async def test_unused_closeout_rejects_orphaned_runtime_risk(inventory_env, risk):
    from app.models.channel_agent import ChannelOpsQueueItem
    from app.models.job import Job, JobStatus, NodeExecution, NodeStatus

    env = inventory_env
    _, row = await draft(env)
    url = f"{env.url}/{row['id']}"
    assert (await env.client.post(url + "/approve", json=approval(row))).status_code == 200
    assert (await env.client.post(url + "/revoke", json={"manifest_sha256": row["manifest_sha256"], "reason": "unused closeout"})).status_code == 200
    async with env.factory() as db:
        if risk == "cancelled_job_live_node":
            job = Job(pipeline_id=uuid.uuid4(), pipeline_snapshot={}, status=JobStatus.CANCELLED)
            db.add(job)
            await db.flush()
            db.add(NodeExecution(job_id=job.id, node_id="stale", node_type="transcode", status=NodeStatus.RUNNING))
        else:
            db.add(ChannelOpsQueueItem(kind="plan_task", channel_profile_id=env.channel_id,
                                      idempotency_key=str(uuid.uuid4()), payload_json={}, status="cancelled",
                                      locked_at=datetime.now(timezone.utc), locked_by="old-runner"))
        await db.commit()
    result = await env.client.get(url)
    assert result.status_code == 200
    assert result.json()["closeout"]["status"] == "unresolved"
