"""Offline migration contracts; SQL execution is parent-only PostgreSQL work."""

from __future__ import annotations

import json
import re
import runpy
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import (
    CheckConstraint,
    Enum,
    ForeignKeyConstraint,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
)


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
        "042_owned_producer_fence"
    ]
    assert unit["down_revision"] == "040_owned_history_seal"


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
                if isinstance(column.type, Enum) and row.get(column.name) is not None:
                    assert row[column.name] in column.type.enums, (table, column.name)
            if table == "artifacts":
                assert raw["kind"] == "intermediate"
            if table == "jobs":
                assert raw["orchestrator_owner"] == ""
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
                    if (
                        isinstance(column.type, Enum)
                        and row.get(column.name) is not None
                    ):
                        assert row[column.name] in column.type.enums, (
                            fault,
                            table,
                            column.name,
                        )


def test_fixture_satisfies_migration_checks_defaults_foreign_keys_and_unique_keys(
    monkeypatch,
):
    """Evaluate portable row predicates offline, not PostgreSQL or trigger behavior."""
    from app.models.base import Base
    from tests.migrations.test_registered_consumer_reconcile_postgres import seed_rows
    from tests.migrations.test_registered_consumer_terminal_postgres import (
        FAULTS,
        corrupt,
        terminal_rows,
        with_defaults,
    )
    from tests.services.test_owned_seed_inventory_history import NOW

    owner_migration = runpy.run_path(
        str(ROOT / "alembic/versions/018_go_orchestrator_owner.py")
    )
    checks, columns = {}, {}
    monkeypatch.setattr(
        owner_migration["op"],
        "add_column",
        lambda table, column: columns.update({column.name: column}),
    )
    monkeypatch.setattr(
        owner_migration["op"],
        "create_check_constraint",
        lambda name, table, sql: checks.update({table: (name, sql)}),
    )
    owner_migration["upgrade"]()
    assert str(columns["orchestrator_owner"].server_default.arg) == "python"

    native = MetaData()
    migration034 = runpy.run_path(
        str(ROOT / "alembic/versions/034_worker_registrations.py")
    )
    monkeypatch.setattr(
        migration034["op"],
        "create_table",
        lambda name, *args, **kwargs: Table(name, native, *args, **kwargs),
    )
    monkeypatch.setattr(
        migration034["op"],
        "create_index",
        lambda name, table, cols, **kwargs: Index(
            name, *(native.tables[table].c[col] for col in cols), **kwargs
        ),
    )
    monkeypatch.setattr(
        migration034["op"],
        "create_foreign_key",
        lambda name, table, remote, local, target, **kwargs: native.tables[
            table
        ].append_constraint(
            ForeignKeyConstraint(
                local, [f"{remote}.{col}" for col in target], name=name, **kwargs
            )
        ),
    )
    migration034["_create_registered_event_receipt_tables"]()
    assert len(native.tables) == 5

    def bind(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if value is None or isinstance(value, (str, int, float)):
            return value
        return str(value)

    _, registrations, grants = seed_rows(NOW, "terminal-test")
    errors = []
    with closing(sqlite3.connect(":memory:")) as sql:

        def predicate(expression, row, table):
            projection = ",".join(f'? AS "{key}"' for key in row)
            return sql.execute(
                f'SELECT ({expression}) FROM (SELECT {projection}) AS "{table}"',
                [bind(v) for v in row.values()],
            ).fetchone()[0]

        for fault in (None, *FAULTS):
            rows = terminal_rows(
                SimpleNamespace(registrations=registrations, grants=grants), NOW
            )
            if fault:
                corrupt(rows, fault, NOW)
            rows = {
                table: [with_defaults(table, row) for row in records]
                for table, records in rows.items()
            }
            external = {
                "worker_registrations": registrations,
                "worker_admission_grants": grants,
            }
            ids = {
                table: {row["id"] for row in records} for table, records in rows.items()
            }
            ids.update(
                {
                    table: {str(row.id) for row in records}
                    for table, records in external.items()
                }
            )
            for table, records in rows.items():
                schema = (
                    native.tables[table]
                    if table in native.tables
                    else Base.metadata.tables[table]
                )
                for row in records:
                    for column in schema.columns:
                        if not column.nullable and (
                            column.name in row or column.server_default is None
                        ):
                            if row.get(column.name) is None:
                                errors.append(
                                    (
                                        fault,
                                        table,
                                        column.name,
                                        "missing nonnull/default",
                                    )
                                )
                    for fk in schema.foreign_keys:
                        value = row.get(fk.parent.name)
                        if value is not None:
                            remote, key = fk.target_fullname.rsplit(".", 1)
                            assert key == "id"
                            if str(value) not in ids.get(remote, set()):
                                errors.append(
                                    (fault, table, fk.parent.name, "foreign key")
                                )
                    expressions = [
                        (
                            check.name,
                            str(
                                check.sqltext.compile(
                                    compile_kwargs={"literal_binds": True}
                                )
                            ),
                        )
                        for check in schema.constraints
                        if isinstance(check, CheckConstraint)
                    ]
                    if table in checks:
                        expressions.append(checks[table])
                    for name, expression in expressions:
                        if expression == "(payload_sha256 ~ '^[0-9a-f]{64}$') IS TRUE":
                            valid = (
                                re.fullmatch(r"[0-9a-f]{64}", row["payload_sha256"])
                                is not None
                            )
                        else:
                            valid = predicate(expression, row, table) == 1
                        if not valid:
                            errors.append((fault, table, name, "check"))
                keys = [
                    (tuple(c.name for c in key.columns), None)
                    for key in schema.constraints
                    if isinstance(key, (PrimaryKeyConstraint, UniqueConstraint))
                ]
                keys.extend(
                    (
                        tuple(c.name for c in index.columns),
                        index.dialect_options["postgresql"].get("where"),
                    )
                    for index in schema.indexes
                    if index.unique
                )
                for names, condition in keys:
                    seen = set()
                    for row in records:
                        values = tuple(bind(row.get(name)) for name in names)
                        if None in values or (
                            condition is not None
                            and predicate(str(condition), row, table) != 1
                        ):
                            continue
                        if values in seen:
                            errors.append((fault, table, names, "unique"))
                        seen.add(values)
    assert not errors, errors
