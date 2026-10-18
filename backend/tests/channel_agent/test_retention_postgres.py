"""Explicit disposable PG16 qualification; only private random schemas are changed."""
from __future__ import annotations

import os
import re
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateTable

from app.models.channel_agent import (
    AgentTickAudit, CandidateFeatureSnapshot, ChannelOpsQueueItem, FeedbackSnapshot,
    PublicationMetricSchedule, PublicationRecord,
)
from app.models.youtube_upload_operation import YouTubeUploadOperation
from tests.channel_agent.test_retention import (  # noqa: F401 - run identical behavior on PG
    test_cleanup_expired_deletes_old_terminal_rows_and_preserves_recent as test_pg_unrelated_retention,
    test_cleanup_preserves_publication_evidence_component as test_pg_evidence_component,
    test_cleanup_preserves_upload_only_task_chain as test_pg_upload_only,
)


@pytest.mark.parametrize("actions,ci_url,accepted", [
    ("true", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres", True),
    ("", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres", False),
    ("true", "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres", False),
    ("true", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/production", False),
    ("true", "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres?host=elsewhere", False),
])
def test_retention_fixture_accepts_only_exact_ci_service(actions, ci_url, accepted):
    values = {"GITHUB_ACTIONS": actions, "CHANNEL_OPS_POSTGRES_TEST_URL": ci_url}
    if accepted:
        url, confirmation, system = disposable_target(values)
        assert url.database == confirmation == "postgres" and system is None
    else:
        with pytest.raises(ValueError, match="^explicit disposable"):
            disposable_target(values)


def test_retention_fixture_ignores_ambient_database_url():
    assert disposable_target({"DATABASE_URL": "postgresql://owner@production/db"}) is None


def disposable_target(environment):
    raw = environment.get("RETENTION_DISPOSABLE_POSTGRES_URL", "")
    if not raw:
        ci_url = environment.get("CHANNEL_OPS_POSTGRES_TEST_URL", "")
        if not ci_url:
            return None
        # Only the exact repository-owned GitHub Actions service is implicit.
        if environment.get("GITHUB_ACTIONS") != "true" or ci_url != "postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/postgres":
            raise ValueError("explicit disposable retention database required")
        return make_url(ci_url), "postgres", None
    url = make_url(raw)
    confirmation = environment.get("RETENTION_DISPOSABLE_POSTGRES_CONFIRM", "")
    system = environment.get("RETENTION_DISPOSABLE_POSTGRES_SYSTEM_ID", "")
    if not (
        url.drivername in {"postgresql", "postgresql+asyncpg"}
        and url.host == "127.0.0.1" and url.port not in {None, 5432}
        and not url.query and url.username and url.password
        and re.fullmatch(r"vp_retention_test_[a-z0-9_]+", url.database or "")
        and confirmation == url.database and re.fullmatch(r"[0-9]+", system)
    ):
        raise ValueError("explicit disposable database/cluster confirmation required")
    return url, confirmation, system


@pytest.fixture
async def retention_session():
    target = disposable_target(os.environ)
    if target is None:
        pytest.skip("explicit disposable retention PostgreSQL required")
    url, confirmation, system = target
    schema = "retention_" + uuid.uuid4().hex
    engine = create_async_engine(
        url.set(drivername="postgresql+asyncpg"),
        connect_args={"server_settings": {"search_path": schema, "statement_timeout": "5000"}},
    )
    created = False
    try:
        async with engine.begin() as conn:
            identity = (await conn.execute(text(
                "SELECT current_database(), current_setting('server_version_num'), "
                "system_identifier::text FROM pg_control_system()"
            ))).one()
            assert identity[0] == confirmation and 160000 <= int(identity[1]) < 170000
            assert system is None or identity[2] == system
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            created = True
            # Use actual model columns/checks and PostgreSQL JSON/UUID types.
            # Unrelated FK parents are omitted; this test owns no public tables.
            for model in (
                ChannelOpsQueueItem, AgentTickAudit, CandidateFeatureSnapshot,
                FeedbackSnapshot, PublicationRecord, PublicationMetricSchedule,
                YouTubeUploadOperation,
            ):
                await conn.execute(CreateTable(model.__table__, include_foreign_key_constraints=[]))
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            yield session
    finally:
        if created:
            async with engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await engine.dispose()
