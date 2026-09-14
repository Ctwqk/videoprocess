"""Add a restricted guard for explicitly pinned historical consumer chains.

Revision ID: 045_registered_consumer_history
Revises: 044_policy_decision_snapshots
"""
from __future__ import annotations

from pathlib import Path
import runpy

from alembic import op


revision = "045_registered_consumer_history"
down_revision = "044_policy_decision_snapshots"
branch_labels = None
depends_on = None
SIGNATURE = "public.vp_registered_consumer_reconcile_history_guard(text,uuid[],uuid[])"


def replace_once(source: str, before: str, after: str) -> str:
    if source.count(before) != 1:
        raise RuntimeError("registered_reconcile_definition_changed")
    return source.replace(before, after, 1)


def edge_sql(old: str, newer: str) -> str:
    return f"""({old}.superseded_by = {newer}.id OR (
        {old}.superseded_by IS NULL
        AND {old}.status = 'revoked'
        AND {old}.revoke_reason = 'worker_redis_continuity_unready'
        AND {old}.grant_id = {newer}.grant_id
        AND {old}.service_name = {newer}.service_name
        AND {old}.lease_epoch + 1 = {newer}.lease_epoch
        AND {old}.registered_at < {newer}.registered_at
        AND {old}.registered_at <= {old}.revoked_at
        AND {old}.revoked_at <= {newer}.registered_at
    ))"""


def guard_sql() -> str:
    root = Path(__file__).parent
    original = runpy.run_path(str(root / "039_registered_consumer_guard.py"))
    terminal = runpy.run_path(str(root / "041_registered_consumer_terminal.py"))
    sql = replace_once(
        original["guard_sql"](),
        "CREATE FUNCTION public.vp_registered_consumer_reconcile_guard(",
        "CREATE FUNCTION public.vp_registered_consumer_reconcile_history_guard(",
    )
    sql = replace_once(sql, terminal["OLD_PREDICATE"], terminal["NEW_PREDICATE"])
    sql = replace_once(
        sql, "cardinality(p_predecessor) > 4", "cardinality(p_predecessor) > 256"
    )
    sql = replace_once(
        sql,
        """NOT registration.superseded_by = ANY(p_current)
                         OR registration.superseded_by IS NULL""",
        """NOT EXISTS (
            SELECT 1 FROM public.worker_registrations AS successor
            WHERE successor.id = ANY(v_ids) AND """
        + edge_sql("registration", "successor") + ")",
    )
    marker = "    IF EXISTS (SELECT 1 FROM public.jobs"
    chain_guard = """
    -- Every retired node has an increasing, pinned successor. With finite
    -- distinct IDs and no retired root, every chain reaches its current worker.
    IF EXISTS (
        SELECT registration.service_name
        FROM public.worker_registrations AS registration
        WHERE registration.id = ANY(p_predecessor)
        GROUP BY registration.service_name HAVING count(*) > 64
    ) OR EXISTS (
        SELECT 1 FROM public.worker_registrations AS old_row
        JOIN public.worker_admission_grants AS old_grant ON old_grant.id = old_row.grant_id
        JOIN public.worker_registrations AS next_row ON next_row.id = ANY(v_ids)
            AND """ + edge_sql("old_row", "next_row") + """
        JOIN public.worker_admission_grants AS next_grant ON next_grant.id = next_row.grant_id
        WHERE old_row.id = ANY(p_predecessor)
          AND (old_row.service_name IS DISTINCT FROM next_row.service_name
               OR old_row.worker_host IS DISTINCT FROM next_row.worker_host
               OR old_row.worker_type IS DISTINCT FROM next_row.worker_type
               OR old_row.worker_slot IS DISTINCT FROM next_row.worker_slot
               OR old_row.lease_epoch >= next_row.lease_epoch
               OR old_grant.generation > next_grant.generation
               OR (old_grant.generation = next_grant.generation AND (
                   old_row.grant_id IS DISTINCT FROM next_row.grant_id
                   OR old_row.lease_epoch + 1 <> next_row.lease_epoch
                   OR next_row.id = ANY(p_current)))
               OR old_row.registered_at >= next_row.registered_at)
    ) THEN RAISE EXCEPTION 'registered_reconcile_history_changed'; END IF;

"""
    return replace_once(sql, marker, chain_guard + marker)


def upgrade() -> None:
    op.execute(guard_sql())
    op.execute(f"REVOKE ALL ON FUNCTION {SIGNATURE} FROM PUBLIC")
    op.execute(f"""
DO $history_acl$ DECLARE v_grantee record; BEGIN
    FOR v_grantee IN
        SELECT DISTINCT role.rolname FROM pg_catalog.pg_proc AS function
        CROSS JOIN LATERAL pg_catalog.aclexplode(function.proacl) AS acl
        JOIN pg_catalog.pg_roles AS role ON role.oid = acl.grantee
        WHERE function.oid = '{SIGNATURE}'::regprocedure AND acl.grantee <> function.proowner
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION {SIGNATURE} FROM %I', v_grantee.rolname);
    END LOOP;
    IF pg_catalog.to_regrole('vp_worker_operator_runtime') IS NOT NULL THEN
        GRANT EXECUTE ON FUNCTION {SIGNATURE} TO vp_worker_operator_runtime;
    END IF;
END $history_acl$;
""")


def downgrade() -> None:
    op.execute(f"DROP FUNCTION {SIGNATURE}")
