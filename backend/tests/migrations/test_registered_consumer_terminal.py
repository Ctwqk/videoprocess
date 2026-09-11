"""Offline migration contracts; SQL execution is parent-only PostgreSQL work."""

from __future__ import annotations

import runpy
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory


ROOT = Path(__file__).parents[2]
PATH = ROOT / "alembic/versions/041_registered_consumer_terminal.py"
OLD = runpy.run_path(str(ROOT / "alembic/versions/039_registered_consumer_guard.py"))


def migration():
    assert PATH.is_file(), "terminal reservation proof migration is missing"
    return runpy.run_path(str(PATH))


def installed():
    sql = OLD["guard_sql"]().replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)
    body = sql.split("AS $function$", 1)[1].split("$function$;", 1)[0]
    return sql, body


def test_new_head_is_an_additive_child_without_rewriting_039():
    unit = migration()
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    assert ScriptDirectory.from_config(config).get_heads() == [
        "041_registered_consumer_terminal"
    ]
    assert unit["down_revision"] == "039_registered_consumer_guard"


def test_definition_replacement_changes_only_upload_predicate_and_roundtrips():
    unit = migration()
    original, body = installed()
    changed = unit["replace_definition"](original, body)
    expected = original.replace(
        "OR EXISTS (SELECT 1 FROM public.youtube_upload_operations\n"
        "                  WHERE status NOT IN ('succeeded','failed'))",
        "OR NOT public.vp_registered_consumer_uploads_quiescent()",
    )
    assert changed == expected and changed != original
    new_body = changed.split("AS $function$", 1)[1].split("$function$;", 1)[0]
    assert unit["replace_definition"](changed, new_body, downgrade=True) == original


@pytest.mark.parametrize(
    "fault", ["missing", "duplicate", "other_guard", "already_changed"]
)
def test_changed_installed_guard_refuses_before_emitting_replacement(fault):
    unit = migration()
    definition, source = installed()
    if fault == "missing":
        definition = "CREATE FUNCTION unrelated() RETURNS void"
    elif fault == "duplicate":
        definition += source
    elif fault == "other_guard":
        source = source.replace("'CLOSED'", "'OPEN'")
        definition = definition.replace("'CLOSED'", "'OPEN'")
    else:
        definition = unit["replace_definition"](definition, source)
        source = source.replace(unit["OLD_PREDICATE"], unit["NEW_PREDICATE"])
    with pytest.raises(RuntimeError, match="^registered_reconcile_definition_changed$"):
        unit["replace_definition"](definition, source)


def test_evidence_sql_uses_sentinels_and_byte_budget_without_returning_evidence():
    unit = migration()
    statements = unit["helper_sql"]()
    assert "LIMIT 4097" in statements
    assert "16777216" in statements and "> 4096" in statements
    assert "RETURNS boolean" in statements
    for forbidden in (
        "FOR UPDATE",
        "FOR SHARE",
        "pg_advisory",
        "c25b9c38",
        "retired_unassigned_preupload",
    ):
        assert forbidden not in statements
    for table in (
        "jobs",
        "node_executions",
        "youtube_upload_operations",
        "production_tasks",
        "artifacts",
        "assets",
        "publishing_accounts",
        "channel_profiles",
        "worker_task_dispatches",
        "worker_task_delivery_attestations",
        "worker_event_emissions",
        "registered_worker_event_receipts",
        "registered_worker_event_deliveries",
        "worker_registrations",
        "worker_admission_grants",
        "channel_ops_queue_items",
        "legacy_worker_event_resolutions",
        "worker_redis_marker_cleanup_authorizations",
        "worker_redis_marker_repair_audits",
        "publication_records",
        "publication_promotion_operations",
    ):
        assert table in statements


def test_no_internal_helper_is_added_to_operator_function_allowlist():
    from app.services.worker_control_role_cli import ROLE_FUNCTIONS

    unit = migration()
    exposed = {name for names in ROLE_FUNCTIONS.values() for name in names}
    for signature in unit["HELPERS"]:
        assert signature.removeprefix("public.") not in exposed


def test_migration_upgrade_and_downgrade_preserve_guard_acl_and_identity(monkeypatch):
    unit = migration()
    _, source = installed()
    calls = []

    def no_catalog_io():
        pytest.fail("offline rendering must defer installed-definition checks to SQL")

    monkeypatch.setattr(unit["op"], "get_bind", no_catalog_io)
    monkeypatch.setattr(unit["op"], "execute", calls.append)
    unit["upgrade"]()
    assert "pg_catalog.pg_get_functiondef" in calls[-1]
    assert "p.prosrc" in calls[-1] and source in calls[-1]
    assert "registered_reconcile_definition_changed" in calls[-1]
    assert "EXECUTE replace(v_definition, v_before, v_after)" in calls[-1]
    assert all(s.count("CREATE FUNCTION") <= 1 for s in calls)
    for statement in calls:
        if "CREATE FUNCTION" in statement:
            assert "REVOKE" not in statement
    assert all(
        "DROP FUNCTION public.vp_registered_consumer_reconcile_guard" not in s
        for s in calls
    )
    assert all("GRANT EXECUTE" not in s for s in calls)
    assert any("REVOKE ALL" in s for s in calls)
    calls.clear()
    unit["downgrade"]()
    assert "pg_catalog.pg_get_functiondef" in calls[0]
    assert "EXECUTE replace(v_definition, v_before, v_after)" in calls[0]
    assert all("vp_registered_consumer_reconcile_guard" not in s for s in calls[1:])


def test_terminal_fixture_is_schema_complete_and_preserves_real_retry_staging_order():
    from tests.migrations.test_registered_consumer_reconcile_postgres import seed_rows
    from tests.migrations.test_registered_consumer_terminal_postgres import (
        terminal_rows,
        with_defaults,
    )
    from tests.services.test_owned_seed_inventory_history import NOW
    from app.models.base import Base

    _, registrations, grants = seed_rows(NOW, "terminal-test")
    rows = terminal_rows(
        SimpleNamespace(registrations=registrations, grants=grants), NOW
    )
    for table, records in rows.items():
        for raw in records:
            row = with_defaults(table, raw)
            for column in Base.metadata.tables[table].columns:
                if not column.nullable and column.server_default is None:
                    assert row.get(column.name) is not None, (table, column.name)
    retry = next(d for d in rows["worker_task_dispatches"] if d["origin_receipt_id"])
    origin = next(
        r
        for r in rows["registered_worker_event_receipts"]
        if r["id"] == retry["origin_receipt_id"]
    )
    assert datetime.fromisoformat(origin["accepted_at"]) <= datetime.fromisoformat(
        retry["created_at"]
    )
    assert datetime.fromisoformat(retry["created_at"]) <= datetime.fromisoformat(
        origin["applied_at"]
    )


def test_negative_fixtures_reach_guard_instead_of_missing_required_columns():
    from tests.migrations.test_registered_consumer_reconcile_postgres import seed_rows
    from tests.migrations.test_registered_consumer_terminal_postgres import (
        FAULTS,
        corrupt,
        terminal_rows,
        with_defaults,
    )
    from tests.services.test_owned_seed_inventory_history import NOW
    from app.models.base import Base

    _, registrations, grants = seed_rows(NOW, "terminal-test")
    for fault in FAULTS:
        rows = terminal_rows(
            SimpleNamespace(registrations=registrations, grants=grants), NOW
        )
        corrupt(rows, fault, NOW)
        for table, records in rows.items():
            for raw in records:
                row = with_defaults(table, raw)
                assert set(row) <= set(Base.metadata.tables[table].columns.keys()), (
                    fault,
                    table,
                )
                for column in Base.metadata.tables[table].columns:
                    if not column.nullable and column.server_default is None:
                        assert row.get(column.name) is not None, (
                            fault,
                            table,
                            column.name,
                        )
