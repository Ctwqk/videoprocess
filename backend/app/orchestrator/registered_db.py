"""Restricted database lifetime for registered receipt and dispatch transactions."""

from __future__ import annotations

import asyncio
import json
import os

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.services.worker_control_role_cli import (
    ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS,
    ORCHESTRATOR_ENTITY_COLUMNS,
    ORCHESTRATOR_INSERT_COLUMNS,
    ORCHESTRATOR_UPDATE_COLUMNS,
    ROLE_FUNCTIONS,
    role_names_for_generation,
)
from app.services.worker_role_cli_common import load_database_url_file


class RegisteredDatabaseError(RuntimeError):
    """Sanitized registered-runtime configuration or readiness failure."""


_QUALIFY = text("""
WITH principals AS (
    SELECT oid, rolname, rolcanlogin, rolinherit,
           rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls AS unsafe
    FROM pg_catalog.pg_roles WHERE rolname IN (:principal, :stable)
), forbidden AS (
    SELECT relation.oid FROM pg_catalog.pg_class relation
    JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace
    WHERE namespace.nspname='public' AND relation.relname IN (
        'worker_admission_grants', 'worker_registrations',
        'worker_redis_marker_cleanup_authorizations', 'worker_redis_continuity_status',
        'worker_redis_continuity_expectations', 'worker_redis_marker_repair_audits'
    )
)
SELECT session_user=:principal AND current_user=session_user
    AND current_database()=:database
    AND (SELECT count(*)=2 FROM principals)
    AND NOT EXISTS (SELECT 1 FROM principals WHERE unsafe
        OR rolcanlogin IS DISTINCT FROM (rolname=:principal)
        OR rolinherit IS DISTINCT FROM (rolname=:principal))
    AND pg_catalog.pg_has_role(session_user, :stable, 'USAGE')
    AND NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members m JOIN principals p ON p.oid=m.member
        WHERE p.rolname=:stable OR m.roleid<>pg_catalog.to_regrole(:stable)
            OR m.admin_option OR NOT m.set_option OR NOT m.inherit_option
    )
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relowner IN (SELECT oid FROM principals))
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_proc WHERE proowner IN (SELECT oid FROM principals))
    AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspowner IN (SELECT oid FROM principals))
    AND (SELECT count(*)=6 FROM forbidden)
    AND NOT EXISTS (SELECT 1 FROM forbidden WHERE
        pg_catalog.has_table_privilege(session_user, oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE')
        OR pg_catalog.has_any_column_privilege(session_user, oid, 'SELECT,INSERT,UPDATE,REFERENCES'))
    AND NOT EXISTS (
        SELECT 1 FROM unnest(ARRAY['worker_id','worker_registration_id','worker_lease_epoch','started_at']) col
        WHERE pg_catalog.has_column_privilege(session_user, 'public.node_executions', col, 'UPDATE')
    )
    AND NOT pg_catalog.has_column_privilege(session_user,
        'public.channel_profiles', 'owned_seed_inventory_id', 'INSERT,UPDATE')
    AND NOT EXISTS (
        SELECT 1 FROM jsonb_to_recordset(CAST(:columns AS jsonb)) AS c(tbl text, col text, privilege text)
        WHERE NOT coalesce(pg_catalog.has_column_privilege(session_user,
            pg_catalog.to_regclass('public.' || c.tbl), c.col, c.privilege), false)
    )
    AND NOT EXISTS (
        SELECT 1 FROM jsonb_array_elements_text(CAST(:functions AS jsonb)) f(signature)
        WHERE NOT coalesce(pg_catalog.has_function_privilege(session_user,
            pg_catalog.to_regprocedure('public.' || f.signature), 'EXECUTE'), false)
    )
""")


class RegisteredDatabase:
    def __init__(self) -> None:
        self._engine: AsyncEngine | None = None
        self._sessions: async_sessionmaker[AsyncSession] | None = None
        self.generation: str | None = None
        self.principal: str | None = None

    @property
    def ready(self) -> bool:
        return self._sessions is not None

    def session(self) -> AsyncSession:
        if self._sessions is None:
            raise RegisteredDatabaseError("registered database unavailable")
        return self._sessions()

    async def start(self, target_database_url: str) -> None:
        if self._engine is not None:
            raise RegisteredDatabaseError("registered database already initialized")
        engine = None
        try:
            generation = os.environ.get("WORKER_ORCHESTRATOR_CONTROL_GENERATION", "")
            names = role_names_for_generation(generation)
            principal = names.versioned["orchestrator"]
            url = make_url(
                load_database_url_file("WORKER_ORCHESTRATOR_DATABASE_URL_FILE")
            )
            target = make_url(target_database_url)
            if (
                url.username != principal
                or not url.password
                or url.query
                or target.drivername not in {"postgresql", "postgresql+asyncpg"}
                or (url.host, url.port or 5432, url.database)
                != (target.host, target.port or 5432, target.database)
            ):
                raise RegisteredDatabaseError(
                    "registered database configuration invalid"
                )
            engine = create_async_engine(
                url.set(drivername="postgresql+asyncpg"),
                echo=False,
                hide_parameters=True,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=0,
                connect_args={"timeout": 5, "command_timeout": 10},
            )
            columns = [
                {"tbl": table, "col": column, "privilege": privilege}
                for privilege, mapping in (
                    ("SELECT", ORCHESTRATOR_AUTHORITY_SELECT_COLUMNS),
                    ("SELECT", ORCHESTRATOR_ENTITY_COLUMNS),
                    ("INSERT", ORCHESTRATOR_INSERT_COLUMNS),
                    ("UPDATE", ORCHESTRATOR_UPDATE_COLUMNS),
                )
                for table, allowed in mapping.items()
                for column in allowed
            ]
            async with asyncio.timeout(10), engine.connect() as connection:
                valid = await connection.scalar(
                    _QUALIFY,
                    {
                        "principal": principal,
                        "stable": names.stable["orchestrator"],
                        "database": url.database,
                        "columns": json.dumps(columns),
                        "functions": json.dumps(ROLE_FUNCTIONS["orchestrator"]),
                    },
                )
            if valid is not True:
                raise RegisteredDatabaseError(
                    "registered database qualification failed"
                )
            self._sessions = async_sessionmaker(engine, expire_on_commit=False)
            self._engine, self.generation, self.principal = (
                engine,
                generation,
                principal,
            )
        except BaseException as exc:
            if engine is not None:
                await engine.dispose()
            if isinstance(exc, asyncio.CancelledError):
                raise
            raise RegisteredDatabaseError(
                "registered database qualification failed"
            ) from None

    async def close(self) -> None:
        engine, self._engine = self._engine, None
        self._sessions = None
        self.generation = self.principal = None
        if engine is not None:
            await engine.dispose()


registered_database = RegisteredDatabase()


def registered_session() -> AsyncSession:
    return registered_database.session()
