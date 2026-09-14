"""Offline migration contracts for the additive, bounded history guard."""
from pathlib import Path
import runpy

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory


ROOT = Path(__file__).parents[2]
PATH = ROOT / "alembic/versions/045_registered_consumer_history.py"
SIGNATURE = "public.vp_registered_consumer_reconcile_history_guard(text,uuid[],uuid[])"


def migration():
    assert PATH.is_file(), "bounded historical consumer guard is missing"
    return runpy.run_path(str(PATH))


def test_history_guard_is_additive_and_preserves_safety_predicates():
    unit = migration()
    assert unit["revision"] == "045_registered_consumer_history"
    assert unit["down_revision"] == "044_policy_decision_snapshots"
    sql = unit["guard_sql"]()
    assert "CREATE FUNCTION public.vp_registered_consumer_reconcile_history_guard(" in sql
    for fragment in (
        "SECURITY DEFINER SET search_path = pg_catalog",
        "v_principal text := session_user",
        "cardinality(p_current) <> 4",
        "cardinality(p_predecessor) > 256",
        "HAVING count(*) > 64",
        "successor.id = ANY(v_ids)",
        "old_row.lease_epoch >= next_row.lease_epoch",
        "old_grant.generation > next_grant.generation",
        "old_row.service_name IS DISTINCT FROM next_row.service_name",
        "old_row.worker_host IS DISTINCT FROM next_row.worker_host",
        "old_row.worker_slot IS DISTINCT FROM next_row.worker_slot",
        "FOR SHARE OF registration, grant_row",
        "'CLOSED'",
        "OR NOT public.vp_registered_consumer_uploads_quiescent()",
        "registered_reconcile_work_active",
    ):
        assert fragment in sql
    assert "CREATE OR REPLACE" not in sql
    assert "CREATE FUNCTION public.vp_registered_consumer_reconcile_guard(" not in sql


def test_history_guard_restart_exception_is_exact_and_read_only():
    sql = migration()["guard_sql"]()
    for fragment in (
        "registration.superseded_by IS NULL",
        "registration.revoke_reason = 'worker_redis_continuity_unready'",
        "registration.grant_id = successor.grant_id",
        "registration.lease_epoch + 1 = successor.lease_epoch",
        "registration.revoked_at <= successor.registered_at",
        "registration.registered_at <= registration.revoked_at",
        "next_row.id = ANY(p_current)",
    ):
        assert fragment in sql
    assert "UPDATE public.worker_registrations" not in sql


def test_history_guard_grants_only_operator_execute_and_drops_only_new_guard(monkeypatch):
    unit = migration()
    calls = []
    monkeypatch.setattr(unit["op"], "execute", calls.append)
    unit["upgrade"]()
    statements = "\n".join(calls)
    assert f"REVOKE ALL ON FUNCTION {SIGNATURE} FROM PUBLIC" in statements
    assert f"GRANT EXECUTE ON FUNCTION {SIGNATURE} TO vp_worker_operator_runtime" in statements
    assert "GRANT SELECT" not in statements
    assert "TO PUBLIC" not in statements
    calls.clear()
    unit["downgrade"]()
    assert calls == [f"DROP FUNCTION {SIGNATURE}"]


def test_history_guard_is_release_head_and_operator_allowlisted():
    from app.services.worker_control_role_cli import ROLE_FUNCTIONS
    from app.services.worker_deployment_cli import EXPECTED_MIGRATION_HEAD

    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "alembic"))
    assert ScriptDirectory.from_config(config).get_heads() == ["045_registered_consumer_history"]
    assert EXPECTED_MIGRATION_HEAD == "045_registered_consumer_history"
    short = SIGNATURE.removeprefix("public.")
    assert short in ROLE_FUNCTIONS["operator"]
    assert all(short not in values for role, values in ROLE_FUNCTIONS.items() if role != "operator")


def test_source_replacement_rejects_missing_or_duplicate_contract():
    unit = migration()
    for source in ("missing", "xx"):
        with pytest.raises(RuntimeError, match="registered_reconcile_definition_changed"):
            unit["replace_once"](source, "x", "replacement")
