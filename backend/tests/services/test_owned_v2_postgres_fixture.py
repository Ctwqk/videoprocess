"""Offline checks of the exact V2 PG fixture setup and barrier observation point."""
import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel_agent import ChannelProfile
from app.services import owned_seed_inventory_history as history
from tests.api import test_owned_seed_inventory_v2_postgres as pg
from tests.api.test_owned_seed_inventory import inventory_env as inventory_env
from tests.services.test_owned_seed_inventory_history import completed_rows


@pytest.mark.parametrize("guarded", [False, True])
async def test_v2_fixture_closes_only_unguarded_synthetic_schedule(monkeypatch, guarded):
    class Owner:
        state = "OPEN"
        async def fetchval(self, statement):
            return "042_owned_producer_fence" if "alembic_version" in statement else self.state
        async def fetchrow(self, statement):
            assert "runtime_schedules" in statement
            return {"state": self.state, "guarded_job_id": "existing-guard" if guarded else None}
        async def execute(self, statement):
            assert "UPDATE runtime_schedules" in statement and "guarded_job_id IS NULL" in statement
            assert not guarded
            self.state = "CLOSED"
    owner = Owner()
    async def no_migration(target, revision):
        assert target == "synthetic-child" and revision == "042_owned_producer_fence"
    monkeypatch.setattr(pg, "migrate", no_migration)
    h = SimpleNamespace(case=SimpleNamespace(owner=owner, target="synthetic-child"))
    if guarded:
        with pytest.raises(AssertionError):
            await pg.v2_pg.__wrapped__(h)
    else:
        assert await pg.v2_pg.__wrapped__(h) is h
        assert owner.state == "CLOSED"


def test_pg_drift_fixture_changes_a_real_history_account_pin():
    account = completed_rows()["publishing_accounts"][0]
    changed = {**account, pg.HISTORY_DRIFT_FIELD: "changed during unlocked IO"}
    assert history.account_descriptor_sha256(changed) != history.account_descriptor_sha256(account)


async def test_pg_writer_signal_precedes_the_actual_first_account_patch_lock(inventory_env, monkeypatch):
    entered, pids = asyncio.Event(), {}
    original_scalars = AsyncSession.scalars
    original_scalar = AsyncSession.scalar
    class StopAtLock(Exception):
        pass
    async def scalar(db, statement, *args, **kwargs):
        if str(statement) == "SELECT pg_backend_pid()":
            return 123
        return await original_scalar(db, statement, *args, **kwargs)
    async def block(db, statement, *args, **kwargs):
        if getattr(statement, "_for_update_arg", None) is not None and any(
            d.get("entity") is ChannelProfile for d in statement.column_descriptions
        ):
            assert entered.is_set() and pids == {"writer": 123}
            raise StopAtLock
        return await original_scalars(db, statement, *args, **kwargs)
    monkeypatch.setattr(AsyncSession, "scalar", scalar)
    monkeypatch.setattr(AsyncSession, "scalars", block)
    pg.signal_account_writer(monkeypatch, pids, entered)
    env = inventory_env
    with pytest.raises(StopAtLock):
        await env.client.patch(f"/api/v1/channel-agent/channels/{env.channel_id}/accounts/{env.scope['target_account_id']}",
                               json={"account_label": "blocked writer"})
