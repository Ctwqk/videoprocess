"""Preserve PG16 admin-only creator edges in worker operator functions.

Revision ID: 035_worker_creator_edges
Revises: 034_worker_registrations
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from alembic import op


revision = "035_worker_creator_edges"
down_revision = "034_worker_registrations"
branch_labels = None
depends_on = None


def _creator_edge_sql(principal: str) -> str:
    # current_user is the SECURITY DEFINER owner, not the operator login.
    return f"""(
              granted.rolname = {principal}
              AND member.rolname <> {principal}
              AND grantor.rolname <> {principal}
              AND member.rolname = current_user
              AND grantor.oid = 10 AND grantor.rolsuper
              AND membership.admin_option
              AND NOT membership.inherit_option
              AND NOT membership.set_option
              AND member.rolcanlogin AND member.rolinherit
              AND member.rolcreaterole AND NOT member.rolsuper
              AND NOT member.rolcreatedb AND NOT member.rolreplication
              AND NOT member.rolbypassrls
              AND NOT EXISTS (
                  SELECT 1 FROM pg_catalog.pg_auth_members AS parent
                  WHERE parent.member = member.oid
                    AND (parent.inherit_option OR parent.set_option)
              )
          )"""


def _operator_statements(*, preserve_creator: bool) -> list[str]:
    source = Path(__file__).with_name("034_worker_registrations.py")
    spec = importlib.util.spec_from_file_location("worker_operator_034", source)
    assert spec is not None and spec.loader is not None
    previous = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(previous)
    statements: list[str] = []
    previous.op = SimpleNamespace(execute=statements.append)
    if preserve_creator:
        original_revoke = previous._operator_revoke_membership_authority_sql
        original_require = previous._operator_require_runtime_principal_sql

        def revoke(principal: str) -> str:
            sql = original_revoke(principal)
            predicate = f"""WHERE granted.rolname = {principal}
           OR member.rolname = {principal}
           OR grantor.rolname = {principal}"""
            assert sql.count(predicate) == 1
            return sql.replace(
                predicate,
                f"""WHERE (granted.rolname = {principal}
           OR member.rolname = {principal}
           OR grantor.rolname = {principal})
          AND NOT {_creator_edge_sql(principal)}""",
                1,
            )

        def require(principal: str) -> str:
            sql = original_require(principal)
            predicate = """          AND NOT (
              granted.rolname = 'vp_worker_runtime'"""
            assert sql.count(predicate) == 1
            return sql.replace(
                predicate,
                f"          AND NOT {_creator_edge_sql(principal)}\n" + predicate,
                1,
            )

        previous._operator_revoke_membership_authority_sql = revoke
        previous._operator_require_runtime_principal_sql = require
    previous._create_operator_functions()
    result = []
    for sql in statements:
        if not sql.lstrip().startswith((
            "CREATE FUNCTION public.vp_worker_grant_activate(",
            "CREATE FUNCTION public.vp_worker_grant_revoke(",
        )):
            continue
        result.append(sql.replace("CREATE FUNCTION ", "CREATE OR REPLACE FUNCTION ", 1))
    assert len(result) == 2
    return result


def upgrade() -> None:
    # Replacing only bodies retains signatures, ownership and EXECUTE ACLs.
    for statement in _operator_statements(preserve_creator=True):
        op.execute(statement)


def downgrade() -> None:
    for statement in _operator_statements(preserve_creator=False):
        op.execute(statement)
