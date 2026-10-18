"""Native draft/approval remains the sole issuer of retention certificates."""

import copy
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from app.models.channel_agent import ChannelProfile

from test_owned_seed_inventory_history_api import history_env as history_env
from test_owned_seed_inventory import inventory_env as inventory_env, approval


@pytest.fixture
async def retained_env(history_env):
    env = history_env
    f = json.loads(
        (
            Path(__file__).parents[1]
            / "fixtures/owned_seed_inventory_retention_gap.json"
        ).read_text()
    )
    remapping = {
        f["rows"]["publishing_accounts"][0]["id"]: env.data["history_locators"][
            "operations"
        ][0]["legacy_account_id"],
        f["rows"]["channel_profiles"][0]["id"]: env.data["history_locators"][
            "operations"
        ][0]["legacy_channel_profile_id"],
    }

    def remap(value):
        if isinstance(value, dict):
            return {k: remap(v) for k, v in value.items()}
        if isinstance(value, list):
            return [remap(v) for v in value]
        return remapping.get(value, value) if isinstance(value, str) else value

    rows = remap(f["rows"])
    rows["owned_seed_inventories"] = []
    env.rows.clear()
    env.rows.update(rows)
    async with env.env.factory() as db:
        channel = await db.get(
            ChannelProfile, uuid.UUID(rows["channel_profiles"][0]["id"])
        )
        channel.enabled = False
        await db.commit()
    env.controls.observed_at = datetime.fromisoformat(f["now"])
    env.data.update(
        starts_at=f["now"],
        expires_at=(env.controls.observed_at + timedelta(days=7)).isoformat(),
    )
    return env


async def test_native_retained_draft_refuses_active_legacy_producer(retained_env):
    h = retained_env
    async with h.env.factory() as db:
        channel = await db.get(
            ChannelProfile, uuid.UUID(h.rows["channel_profiles"][0]["id"])
        )
        channel.enabled = True
        await db.commit()
    response = await h.env.client.post(h.env.url, json=h.data)
    assert response.status_code == 409
    assert h.requests == []


async def test_native_retained_draft_and_approval_preserve_original_certificate(
    retained_env,
):
    h = retained_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    result = created.json()
    section = result["manifest"]["legacy_history"]
    assert section["version"] == 2 and len(section["retained_terminal_uploads"]) == 1
    certificate = copy.deepcopy(section["retained_terminal_uploads"][0])
    assert certificate["server_subject"] == "test-operator" and len(h.requests) == 2
    assert len(certificate["retained_facts"]["queues"]) == 1
    h.controls.observed_at += timedelta(seconds=1)
    approved = await h.env.client.post(
        f"{h.env.url}/{result['id']}/approve", json=approval(result)
    )
    assert approved.status_code == 200, approved.text
    assert approved.json()["manifest"]["legacy_history"][
        "retained_terminal_uploads"
    ] == [certificate]
    assert (
        approved.json()["manifest_sha256"] == result["manifest_sha256"]
        and len(h.requests) == 4
    )
    replay = await h.env.client.post(
        f"{h.env.url}/{result['id']}/approve", json=approval(result)
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["manifest"] == result["manifest"] and len(h.requests) == 4


@pytest.mark.parametrize(
    "fault", ["wrong_google_owner", "changed_retained_fact", "changed_during_google"]
)
async def test_retained_approval_requalifies_current_owner_and_frozen_primary_facts(
    retained_env, fault
):
    h = retained_env
    created = await h.env.client.post(h.env.url, json=h.data)
    assert created.status_code == 200, created.text
    result = created.json()

    def change():
        h.rows["feedback_snapshots"][0]["views"] = 123

    if fault == "wrong_google_owner":
        h.controls.actual_channel = "UC" + "z" * 22
    elif fault == "changed_retained_fact":
        change()
    else:

        async def drift():
            h.controls.on_get = None
            change()

        h.controls.on_get = drift
    approved = await h.env.client.post(
        f"{h.env.url}/{result['id']}/approve", json=approval(result)
    )
    assert approved.status_code == 409
