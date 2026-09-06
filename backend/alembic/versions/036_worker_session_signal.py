"""Bound worker session retirement to an administrator-installed helper.

Revision ID: 036_worker_session_signal
Revises: 035_worker_creator_edges
"""

from __future__ import annotations

import runpy
from pathlib import Path

from alembic import op

from app.services.worker_session_signal_sql import (
    INSTALL_SQL,
    SCHEMA,
    SERVICES_SQL,
    VERIFY_SQL,
    canonical_role_sql,
)


revision = "036_worker_session_signal"
down_revision = "035_worker_creator_edges"
branch_labels = None
depends_on = None


def _previous_statements() -> list[str]:
    previous = runpy.run_path(
        str(Path(__file__).with_name("035_worker_creator_edges.py"))
    )
    return previous["_operator_statements"](preserve_creator=True)


def _retirement_sql(*, activate: bool) -> str:
    predicate = "" if activate else "AND grant_row.generation = p_generation"
    targets = f"""
        SELECT grant_row.generation FROM public.worker_admission_grants AS grant_row
        JOIN pg_catalog.pg_roles AS role ON role.rolname = grant_row.database_principal
        WHERE grant_row.service_name = p_service_name AND grant_row.state = 'revoked'
          AND grant_row.revoked_at IS NOT NULL {predicate}
        ORDER BY grant_row.generation
    """
    # Every target must pass before the first irreversible signal. Validation
    # pins the catalog identities/edges until this transaction ends.
    return f"""
    IF p_service_name IN {SERVICES_SQL} THEN
    FOR v_signal_generation IN {targets} LOOP
        PERFORM {SCHEMA}.validate_target(p_service_name, v_signal_generation);
    END LOOP;
    FOR v_signal_generation IN {targets} LOOP
        PERFORM {SCHEMA}.retire(p_service_name, v_signal_generation);
    END LOOP;
    ELSE
        -- Generic public-API clients keep only the existing wrapper owner's
        -- authority. This branch never obtains bootstrap signaling privileges.
        PERFORM pg_catalog.pg_terminate_backend(activity.pid)
        FROM public.worker_admission_grants AS grant_row
        JOIN pg_catalog.pg_roles AS role ON role.rolname = grant_row.database_principal
        JOIN pg_catalog.pg_stat_activity AS activity ON activity.usesysid = role.oid
        JOIN pg_catalog.pg_database AS database ON database.oid = activity.datid
        WHERE grant_row.service_name = p_service_name AND grant_row.state = 'revoked'
          AND grant_row.revoked_at IS NOT NULL {predicate}
          AND database.datname = pg_catalog.current_database()
          -- Restricted pg_stat_activity fields can be NULL for this owner;
          -- retain PostgreSQL's native permission failure instead of skipping.
          AND (activity.backend_type = 'client backend' OR activity.backend_type IS NULL)
          AND activity.pid <> pg_catalog.pg_backend_pid();
    END IF;
"""


def _operator_statements() -> list[str]:
    result = []
    for sql in _previous_statements():
        activate = "FUNCTION public.vp_worker_grant_activate(" in sql
        sql = sql.replace(
            "    v_membership record;",
            "    v_membership record;\n    v_signal_generation bigint;",
        )
        # Canonical business checks stay at the non-superuser boundary.
        marker = "    IF v_grant.state"
        position = sql.index(marker)
        sql = (
            sql[:position]
            + f"""
    IF p_service_name IN {SERVICES_SQL} AND EXISTS (
        SELECT 1 FROM public.worker_admission_grants AS grant_row
        WHERE grant_row.service_name = p_service_name AND (
            grant_row.database_principal IS DISTINCT FROM
                {canonical_role_sql("grant_row.service_name", "grant_row.generation")}
            OR (grant_row.state = 'revoked' AND grant_row.revoked_at IS NULL)
        )
    ) THEN
        RAISE EXCEPTION 'worker_signal_identity_invalid';
    END IF;
"""
            + sql[position:]
        )
        principal = "v_revoked_principal" if activate else "v_grant.database_principal"
        signal = f"""        PERFORM pg_catalog.pg_terminate_backend(activity.pid)
        FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.usename = {principal}
          AND activity.pid <> pg_catalog.pg_backend_pid();"""
        assert sql.count(signal) == (2 if activate else 1)
        sql = sql.replace(signal, "")
        returning = "RETURN v_grant.id;" if activate else "RETURN TRUE;"
        assert sql.count(returning) == (2 if activate else 1)
        sql = sql.replace(
            returning, _retirement_sql(activate=activate) + "    " + returning
        )
        result.append(sql)
    return result


def upgrade() -> None:
    # Fresh original-bootstrap-owned databases may install automatically.
    # All ordinary deploy owners must find the exact preinstalled boundary.
    op.execute(
        "DO $auto$ BEGIN IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles "
        "WHERE oid = 10 AND rolsuper AND rolname = current_user) THEN "
        "EXECUTE " + "'" + INSTALL_SQL.replace("'", "''") + "'; END IF; END $auto$;"
    )
    op.execute(VERIFY_SQL)
    for statement in _operator_statements():
        op.execute(statement)


def downgrade() -> None:
    for statement in _previous_statements():
        op.execute(statement)
    # Only the administrator may remove its private bootstrap objects.
