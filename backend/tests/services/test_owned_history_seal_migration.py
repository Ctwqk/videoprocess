from pathlib import Path
import runpy

import pytest


MIGRATION = Path(__file__).parents[2] / "alembic/versions/040_owned_history_seal.py"


def migration():
    return runpy.run_path(str(MIGRATION))


@pytest.mark.parametrize("remove", [False, True])
def test_catalogue_patch_emits_existing_definition_replacement_without_new_acl(monkeypatch, remove):
    m = migration()
    assert m["revision"] == "040_owned_history_seal" and m["down_revision"] == "038_owned_seed_inventory"
    emitted = []
    monkeypatch.setattr(m["op"], "execute", emitted.append)
    m["_fence_installed_functions"](remove=remove)
    assert len(emitted) == 1
    sql = emitted[0]
    for name, job_expression in m["ENTRY_JOBS"].items():
        assert name in sql and job_expression in sql
    assert "pg_catalog.pg_get_functiondef(p.oid)" in sql
    assert "EXECUTE replace(v_definition, v_source, v_changed)" in sql
    assert "v_changed := overlay(v_source" in sql
    assert f"IF {'TRUE' if remove else 'FALSE'} THEN" in sql
    assert "owned_history_function_definition_changed" in sql
    assert "owned_history_function_inventory_changed" in sql
    assert "GRANT " not in sql and "ALTER FUNCTION" not in sql
    assert "vp_release_registered_retry_claim" in m["ENTRY_JOBS"]
    assert "vp_acknowledge_proven_worker_task_dispatch" in m["ENTRY_JOBS"]


def test_migration_emits_named_manifest_referenced_backstops_without_new_role_grants(monkeypatch):
    m = migration()
    emitted = []
    monkeypatch.setattr(m["op"], "execute", emitted.append)
    monkeypatch.setattr(m["op"], "get_bind", lambda: pytest.fail("offline DDL must not query a database"))
    m["upgrade"]()
    sql = "\n".join(str(s) for s in emitted)
    for table in ("youtube_upload_operations", "production_tasks", "jobs", "node_executions", "artifacts", "assets",
                  "manual_seeds", "channel_profiles", "publishing_accounts", "worker_task_dispatches", "worker_event_emissions",
                  "registered_worker_event_receipts", "registered_worker_event_deliveries", "worker_task_delivery_attestations",
                  "worker_registrations", "worker_admission_grants", "channel_ops_queue_items", "runtime_schedules"):
        assert f"owned_history_seal_{table}" in sql
    assert "approved_at IS NOT NULL" in sql and "succession_released_at IS NULL" not in sql
    assert "SECURITY DEFINER" in sql and "SET search_path = pg_catalog" in sql
    assert "REVOKE ALL" in sql and "GRANT " not in sql
    assert "FOR UPDATE" in sql and "owned_history_sealed" in sql
    assert "pg_catalog.pg_get_functiondef" in sql and "owned_history_function_inventory_changed" in sql


def test_sql_entry_recompares_task_channel_membership_after_schedule_lock():
    sql = migration()["ENTRY_SQL"]
    assert "INTO v_references" in sql and "INTO v_fresh_references" in sql
    assert sql.index("INTO v_references") < sql.index("FROM public.runtime_schedules") < sql.index("INTO v_fresh_references")
    assert "v_references IS DISTINCT FROM v_fresh_references" in sql
