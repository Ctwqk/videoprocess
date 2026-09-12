from pathlib import Path
import runpy
import re

import pytest


PATH = Path(__file__).parents[2] / "alembic/versions/042_owned_producer_fence.py"


def migration():
    return runpy.run_path(str(PATH))


def test_producer_migration_is_additive_and_offline_renderable(monkeypatch):
    m = migration()
    assert (m["revision"], m["down_revision"]) == ("042_owned_producer_fence", "041_registered_consumer_terminal")
    emitted = []
    monkeypatch.setattr(m["op"], "execute", emitted.append)
    monkeypatch.setattr(m["op"], "get_bind", lambda: pytest.fail("offline rendering cannot query the catalog"))
    m["upgrade"]()
    for signature, body in m["HELPERS"].items():
        index = emitted.index(body)
        assert body.count("CREATE FUNCTION ") == 1
        assert "REVOKE " not in body
        assert emitted[index + 1] == f"REVOKE ALL ON FUNCTION public.{signature} FROM PUBLIC"
    sql = "\n".join(emitted)
    assert "GRANT " not in sql and "SET search_path = pg_catalog" in sql
    assert "pg_get_functiondef" in sql and "owned_producer_definition_changed" in sql


def test_catalog_patch_only_changes_new_effects_and_lock_entry():
    m = migration()
    patches = m["PATCHES"]
    assert set(patches) == {"vp_reserve_worker_youtube_upload", "vp_transition_worker_youtube_upload", "vp_owned_history_job_entry"}
    reserve = patches["vp_reserve_worker_youtube_upload"]
    transition = patches["vp_transition_worker_youtube_upload"]
    assert len(reserve) == 1 and len(transition) == 2
    assert "request_attempted_at IS NOT NULL" in reserve[0][1]
    assert "status <> 'reserved'" in reserve[0][1]
    assert "v_now := pg_catalog.clock_timestamp()" in transition[0][1]
    assert all("vp_owned_producer_upload" in after for _before, after in transition)
    assert all("SET status" not in before and "SET status" not in after for before, after in transition)
    entry = patches["vp_owned_history_job_entry"]
    assert "pg_advisory_xact_lock" in entry[0][1] and "FOR UPDATE" in entry[0][1]


def test_current_history_exclusion_is_after_global_operation_classification():
    sql = migration()["HISTORY_SQL"]
    assert sql.index("owned_history_unclassified") < sql.index("t.id = p_task_id")
    assert "owned_history_orphan" in sql and "owned_history_retired_changed" in sql
    assert "INTERVAL '24 hours'" in sql
    assert "request_attempted_at" in sql and "completed_at" in sql


def test_python_compatible_canonicalizer_keeps_json_numbers_and_rejects_duplicate_keys():
    sql = migration()["CANONICAL_SQL"]
    assert "p_value json" in sql and "json_each(p_value)" in sql
    assert "count(DISTINCT key)" in sql
    assert "vp_registered_consumer_ascii_json" in sql
    assert "1e16" in sql and "0.0001" in sql and "COLLATE \"C\"" in sql


def test_patches_match_existing_functions_once_and_preserve_settlement_suffix(monkeypatch):
    m = migration()
    registered = runpy.run_path(str(PATH.with_name("034_worker_registrations.py")))
    statements = []
    monkeypatch.setattr(registered["op"], "execute", statements.append)
    registered["_create_worker_youtube_upload_functions"]()
    entry = runpy.run_path(str(PATH.with_name("040_owned_history_seal.py")))["ENTRY_SQL"]
    for name, before in zip(m["PATCHES"], [*statements, entry]):
        after = before
        for old, new in m["PATCHES"][name]:
            assert after.count(old) == 1
            after = after.replace(old, new, 1)
        if name == "vp_transition_worker_youtube_upload":
            suffix = "    ELSIF p_transition = 'submitted'"
            assert after.split(suffix, 1)[1] == before.split(suffix, 1)[1]
        for old, new in reversed(m["PATCHES"][name]):
            assert after.count(new) == 1
            after = after.replace(new, old, 1)
        assert after == before


def test_json_row_aliases_cannot_shadow_plpgsql_local_evidence():
    for body in migration()["HELPERS"].values():
        if "DECLARE" not in body:
            continue
        declaration = body.split("DECLARE", 1)[1].split("BEGIN", 1)[0]
        locals_ = set(re.findall(r"\b(\w+)\s+jsonb?\b", declaration))
        aliases = set(re.findall(r"jsonb?_array_elements\([^\n]*?\) (\w+)", body))
        assert not locals_ & aliases, locals_ & aliases


def test_upload_requires_input_origin_in_same_job_graph():
    sql = migration()["UPLOAD_SQL"]
    assert "value->>'id' = art->>'node_execution_id'" in sql
    assert "value->>'job_id' = p_job_id::text" in sql


def test_history_row_projection_preserves_a1_raw_postgresql_json_types():
    sql = migration()["rows_sql"]()
    assert "row_to_json(r)" in sql and "json_agg(" in sql
    assert "to_char" not in sql and "'.0'" not in sql
    assert "SELECT json_build_object(" in sql
    assert "lease_secret_sha256" in sql and "token_sha256" in sql


def test_graph_case_expressions_are_nested_inside_sql_parentheses():
    sql = migration()["GRAPH_SQL"]
    assert "<> (CASE WHEN n = source THEN 0 ELSE 1 END)" in sql
    assert "(CASE WHEN n = upload OR n = export THEN 0 WHEN n->>'id' = parent THEN 2 ELSE 1 END)" in sql


def test_d_fixture_explicit_head_preserves_api_fixture_historical_default():
    import inspect
    from tests.api.test_owned_seed_inventory import inventory_env
    source = inspect.getsource(inventory_env.__wrapped__)
    assert 'getattr(request, "expected_migration_head", "041_registered_consumer_terminal")' in source


def test_upload_ancestry_requires_each_exact_durable_artifact_link():
    sql = migration()["UPLOAD_SQL"]
    assert "owned_inventory_artifact_lineage" in sql
    assert "WITH RECURSIVE ancestors" in sql
    assert "part::jsonb->'input_artifact_ids' = jsonb_build_array(v_predecessor->>'output_artifact_id')" in sql
    assert "v_input->>'node_execution_id' = v_predecessor->>'id'" in sql
