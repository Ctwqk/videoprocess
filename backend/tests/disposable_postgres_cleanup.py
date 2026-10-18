"""Narrow cleanup exception for two explicitly disposable PG16 test fixtures.

Never import this helper from application code. Local runs must confirm the
named scratch database and its independently observed PostgreSQL system ID via
CHANNEL_OPS_POSTGRES_TEST_CONFIRM / CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID.
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping

from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine


_TRUNCATES = {
    "autoflow": (
        "TRUNCATE TABLE public.autoflow_used_clips, public.autoflow_runs, public.node_executions, public.jobs, "
        "public.pipelines, public.runtime_schedules, public.channel_ops_queue_items, public.production_tasks, "
        "public.channel_profiles, public.autoflow_plans CASCADE"
    ),
    "quarantine": (
        "TRUNCATE public.channel_ops_queue_items, public.publication_records, public.production_tasks, "
        "public.publishing_accounts, public.autoflow_runs, public.autoflow_plans, public.node_executions, public.jobs, "
        "public.pipelines, public.runtime_schedules RESTART IDENTITY CASCADE"
    ),
}
_AUDIT = "public.owned_inventory_recoveries"
_TRIGGER = "owned_inventory_recovery_audit_no_truncate"
_LOCK_KEY = 7116646839327985488


def checked_target(url: URL, env: Mapping[str, str]) -> str | None:
    """Reject ambient application URLs before opening a database connection."""
    repository_ci = env.get("GITHUB_ACTIONS") == "true" and env.get("GITHUB_REPOSITORY") == "Ctwqk/videoprocess"
    if (url.drivername != "postgresql+asyncpg" or url.host != "127.0.0.1"
        or url.username != "postgres" or not url.password or url.query
        or url.port is None or not 1024 <= url.port <= 65535):
        raise ValueError("requires an explicitly disposable PostgreSQL target")
    if repository_ci and url.port == 5432 and url.database == "postgres" and url.password == "postgres":
        return None
    system_id = env.get("CHANNEL_OPS_POSTGRES_TEST_SYSTEM_ID", "")
    if (url.port == 5432 and not repository_ci
        or not re.fullmatch(r"vp_owned_inventory_test_[a-z0-9_]+", url.database or "")
        or env.get("CHANNEL_OPS_POSTGRES_TEST_CONFIRM") != url.database
        or not re.fullmatch(r"[0-9]{10,20}", system_id)):
        raise ValueError("requires an explicitly confirmed disposable PostgreSQL database and system ID")
    return system_id


async def cleanup_disposable_postgres(engine: AsyncEngine, plan: str) -> None:
    """Own the transaction so any error also rolls back the trigger exception."""
    statement = _TRUNCATES[plan]  # Only the two reviewed fixture statements.
    system_id = checked_target(engine.url, os.environ)
    async with engine.begin() as conn:
        await conn.execute(text("SET LOCAL lock_timeout='5s'"))
        await conn.execute(text("SET LOCAL statement_timeout='10s'"))
        identity = (await conn.execute(text(
            "SELECT current_database(), session_user, current_setting('server_version_num')::int, "
            "system_identifier::text, pg_is_in_recovery() FROM pg_control_system()"
        ))).one()
        if (identity[0] != engine.url.database or identity[1] != engine.url.username
            or not 160000 <= identity[2] < 170000 or identity[4]
            or system_id is not None and identity[3] != system_id):
            raise ValueError("actual disposable PostgreSQL identity does not match authorization")
        if not await conn.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": _LOCK_KEY}):
            raise ValueError("another disposable PostgreSQL cleanup is active")
        # Block audit inserts before checking emptiness. Keep this lock until
        # the exact trigger has been re-enabled and the transaction commits.
        await conn.execute(text(f"LOCK TABLE {_AUDIT}, public.owned_inventory_recovery_permits IN ACCESS EXCLUSIVE MODE"))
        trigger = (await conn.execute(text(
            "SELECT tgenabled::text, tgtype, tgfoid='public.vp_owned_inventory_recovery_audit_immutable()'::regprocedure "
            "FROM pg_trigger WHERE tgrelid='public.owned_inventory_recoveries'::regclass "
            "AND tgname=:name AND NOT tgisinternal"
        ), {"name": _TRIGGER})).one_or_none()
        if trigger is None or tuple(trigger) != ("O", 34, True):
            raise ValueError("expected enabled recovery audit TRUNCATE guard is missing or changed")
        if await conn.scalar(text(
            f"SELECT EXISTS(SELECT 1 FROM {_AUDIT}) OR EXISTS(SELECT 1 FROM public.owned_inventory_recovery_permits)"
        )):
            raise ValueError("refusing cleanup of recovery evidence")
        await conn.execute(text(f"ALTER TABLE {_AUDIT} DISABLE TRIGGER {_TRIGGER}"))
        await conn.execute(text(statement))
        await conn.execute(text(f"ALTER TABLE {_AUDIT} ENABLE TRIGGER {_TRIGGER}"))
        if await conn.scalar(text(
            "SELECT tgenabled::text FROM pg_trigger WHERE tgrelid='public.owned_inventory_recoveries'::regclass AND tgname=:name"
        ), {"name": _TRIGGER}) != "O":
            raise ValueError("recovery audit TRUNCATE guard was not restored")
