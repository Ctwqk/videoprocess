"""Exercise staging SQL offline against the deployed column/default contract."""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import DefaultClause, event, func, inspect, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.registered_worker_event_receipt import WorkerTaskDispatch
from app.services.registered_worker_event_receipt import (
    RegisteredWorkerEventError,
    canonical_redis_payload_sha256,
    stage_worker_task_dispatch,
)
from app.services.worker_control_role_cli import ORCHESTRATOR_INSERT_COLUMNS


@pytest.fixture
async def staging_db(monkeypatch):
    # Select the production branch without connecting to PostgreSQL. The actual
    # SQL still executes on SQLite; only the deployed server default is mirrored.
    class PostgreSQLBranchSession(AsyncSession):
        def get_bind(self, *args, **kwargs):
            return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    monkeypatch.setattr(
        WorkerTaskDispatch.__table__.c.resolution_state,
        "server_default",
        DefaultClause(text("'unresolved'")),
    )
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(WorkerTaskDispatch.__table__.create)
    allowed = set(ORCHESTRATOR_INSERT_COLUMNS["worker_task_dispatches"])
    assert allowed == {
        "id", "origin_receipt_id", "dispatch_key", "job_id", "node_execution_id",
        "redis_stream", "consumer_group", "payload_sha256", "payload_json", "delivery_state",
    }
    inserts = []

    def guard_columns(connection, cursor, statement, parameters, context, executemany):
        if not statement.startswith("INSERT INTO worker_task_dispatches"):
            return
        columns = set(re.search(r"INSERT INTO worker_task_dispatches \(([^)]+)\)", statement)[1].split(", "))
        if not columns <= allowed:
            raise PermissionError(f"dispatch INSERT columns outside grant: {sorted(columns - allowed)}")
        compiled_pg = str(context.compiled.statement.compile(dialect=postgresql.dialect()))
        pg_columns = set(re.search(r"INSERT INTO worker_task_dispatches \(([^)]+)\)", compiled_pg)[1].split(", "))
        assert pg_columns == columns
        inserts.append((statement, compiled_pg, columns))

    event.listen(engine.sync_engine, "before_cursor_execute", guard_columns)
    try:
        yield async_sessionmaker(engine, class_=PostgreSQLBranchSession, expire_on_commit=False), inserts
    finally:
        await engine.dispose()


def _arguments(origin_receipt_id):
    job_id, node_id = uuid.uuid4(), uuid.uuid4()
    return {
        "origin_receipt_id": origin_receipt_id, "job_id": job_id, "node_execution_id": node_id,
        "redis_stream": " vp:tasks:vision ", "consumer_group": " vision-workers ",
        "payload": {"job_id": str(job_id), "node_execution_id": str(node_id), "config": "{}"},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", [None, "completion", "failure"])
async def test_staging_inserts_only_granted_columns_and_hydrates_server_state(staging_db, origin):
    factory, inserts = staging_db
    args = _arguments(None if origin is None else uuid.uuid4())
    async with factory() as db:
        dispatch = await stage_worker_task_dispatch(db, **args)
        assert inspect(dispatch).persistent and dispatch not in db.new
        assert dispatch.origin_receipt_id == args["origin_receipt_id"]
        assert dispatch.job_id == args["job_id"] and dispatch.node_execution_id == args["node_execution_id"]
        assert isinstance(dispatch.id, uuid.UUID) and isinstance(dispatch.dispatch_key, uuid.UUID)
        assert dispatch.redis_stream == "vp:tasks:vision" and dispatch.consumer_group == "vision-workers"
        assert dispatch.payload_json == {**args["payload"], "dispatch_key": str(dispatch.dispatch_key)}
        assert dispatch.payload_sha256 == canonical_redis_payload_sha256(dispatch.payload_json)
        assert dispatch.delivery_state == "pending" and dispatch.resolution_state == "unresolved"
        assert isinstance(dispatch.created_at, datetime)
        assert all(getattr(dispatch, field) is None for field in (
            "delivery_attempted_at", "delivery_error", "redis_message_id", "delivered_at", "acknowledged_at", "cancelled_at",
        ))
        await db.flush()
        assert len(inserts) == 1
        assert inserts[0][2] == set(ORCHESTRATOR_INSERT_COLUMNS["worker_task_dispatches"])
        await db.commit()
    async with factory() as db:
        stored = await db.get(WorkerTaskDispatch, dispatch.id)
        assert stored.payload_json == dispatch.payload_json and stored.resolution_state == "unresolved"


@pytest.mark.asyncio
async def test_structured_staging_rolls_back_without_hidden_commit(staging_db):
    factory, inserts = staging_db
    async with factory() as db:
        await stage_worker_task_dispatch(db, **_arguments(uuid.uuid4()))
        await db.rollback()
    async with factory() as db:
        assert await db.scalar(select(func.count()).select_from(WorkerTaskDispatch)) == 0
    assert len(inserts) == 1


@pytest.mark.asyncio
async def test_structured_staging_keeps_duplicate_initial_guard(staging_db):
    factory, inserts = staging_db
    args = _arguments(None)
    async with factory() as db:
        first = await stage_worker_task_dispatch(db, **args)
        with pytest.raises(RegisteredWorkerEventError, match="unresolved initial dispatch"):
            await stage_worker_task_dispatch(db, **args)
        assert await db.get(WorkerTaskDispatch, first.id) is first
        assert len(inserts) == 1
