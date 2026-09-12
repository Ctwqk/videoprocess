"""Increase complete history snapshot capacity, retaining terminal proof bounds.

Revision ID: 043_owned_history_snapshot_rows
Revises: 042_owned_producer_fence
"""
from __future__ import annotations

from pathlib import Path
import runpy

from alembic import op


revision = "043_owned_history_snapshot_rows"
down_revision = "042_owned_producer_fence"
branch_labels = None
depends_on = None


def _once(source: str, before: str, after: str) -> str:
    if source.count(before) != 1:
        raise RuntimeError("owned_history_capacity_definition_changed")
    return source.replace(before, after, 1)


def _bodies(*, downgrade: bool = False) -> dict[str, tuple[str, str]]:
    terminal = runpy.run_path(str(Path(__file__).with_name("041_registered_consumer_terminal.py")))
    producer = runpy.run_path(str(Path(__file__).with_name("042_owned_producer_fence.py")))
    statements = terminal["helper_statements"]()
    rows = producer["rows_sql"]().split("AS $rows$", 1)[1].split("$rows$;", 1)[0]
    quiescent = statements[3].split("AS $bounded$", 1)[1].split("$bounded$;", 1)[0]
    proof = statements[2].split("AS $proof$", 1)[1].split("$proof$;", 1)[0]
    changed = _once(proof, "v_att_count bigint;", "v_att_count bigint; v_delivery_count bigint := 0;")
    anchor = "    IF NOT o.node_execution_id = ANY(v_nodes) THEN RETURN false; END IF;"
    changed = _once(changed, anchor, "    IF cardinality(v_nodes) > 4096 THEN RETURN false; END IF;\n" + anchor)
    anchor = "    -- Reject cross-boundary edges in either direction instead of hiding them in joins."
    changed = _once(changed, anchor, """    IF cardinality(v_keys) > 4096 OR cardinality(v_atts) > 4096
        OR cardinality(v_receipts) > 4096 OR cardinality(v_emissions) > 4096
    THEN RETURN false; END IF;
""" + anchor)
    anchor = "    IF EXISTS (SELECT 1 FROM public.channel_ops_queue_items WHERE parent_queue_item_id = ANY(v_queues) AND NOT id = ANY(v_queues))"
    changed = _once(changed, anchor, "    IF cardinality(v_queues) > 4096 THEN RETURN false; END IF;\n" + anchor)
    anchor = "        SELECT * INTO STRICT r FROM public.registered_worker_event_receipts WHERE id = delivery.receipt_id;"
    changed = _once(changed, anchor, """        v_delivery_count := v_delivery_count + 1;
        IF v_delivery_count > 4096 THEN RETURN false; END IF;
""" + anchor)
    result = {
        "public.vp_owned_producer_rows()": (rows, rows.replace("LIMIT 4097", "LIMIT 8193").replace("> 4096", "> 8192")),
        "public.vp_registered_consumer_uploads_quiescent()": (quiescent, quiescent.replace("LIMIT 4097", "LIMIT 8193").replace("> 4096", "> 8192")),
        "public.vp_registered_consumer_terminal_upload(uuid,timestamp with time zone)": (proof, changed),
    }
    if rows.count("LIMIT 4097") != 27 or quiescent.count("LIMIT 4097") != 1:
        raise RuntimeError("owned_history_capacity_definition_changed")
    return {name: (after, before) for name, (before, after) in result.items()} if downgrade else result


def _replacement(signature: str, before: str, after: str) -> str:
    for tag in ("$capacity_before$", "$capacity_after$", "$capacity_replace$"):
        if tag in before or tag in after:
            raise RuntimeError("owned_history_capacity_definition_changed")
    # Keep the installed identity, ACL and complete attributes; check/replace on
    # the server so offline Alembic rendering and asyncpg each emit one command.
    return f"""
DO $capacity_replace$
DECLARE
    v_before text := $capacity_before${before}$capacity_before$;
    v_after text := $capacity_after${after}$capacity_after$;
    v_definition text; v_source text; v_security boolean; v_config text[];
BEGIN
    SELECT pg_catalog.pg_get_functiondef(p.oid), p.prosrc, p.prosecdef, p.proconfig
    INTO v_definition, v_source, v_security, v_config
    FROM pg_catalog.pg_proc p WHERE p.oid = pg_catalog.to_regprocedure('{signature}');
    IF NOT FOUND OR v_source IS DISTINCT FROM v_before OR v_security IS NOT FALSE
        OR NOT COALESCE('search_path=pg_catalog' = ANY(v_config), false)
        OR (length(v_definition) - length(replace(v_definition, v_before, '')))
            IS DISTINCT FROM length(v_before)
    THEN RAISE EXCEPTION 'owned_history_capacity_definition_changed'; END IF;
    EXECUTE replace(v_definition, v_before, v_after);
END;
$capacity_replace$;
"""


def upgrade() -> None:
    for signature, (before, after) in _bodies().items():
        op.execute(_replacement(signature, before, after))


def downgrade() -> None:
    for signature, (before, after) in reversed(_bodies(downgrade=True).items()):
        op.execute(_replacement(signature, before, after))
