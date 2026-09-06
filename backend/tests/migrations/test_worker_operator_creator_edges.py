from pathlib import Path
import runpy


MIGRATION = Path(__file__).resolve().parents[2] / "alembic/versions/035_worker_creator_edges.py"


def test_upgrade_only_replaces_existing_operator_function_bodies():
    migration = runpy.run_path(str(MIGRATION))
    statements = migration["_operator_statements"](preserve_creator=True)
    assert len(statements) == 2
    for statement in statements:
        assert statement.lstrip().startswith("CREATE OR REPLACE FUNCTION public.vp_worker_grant_")
        assert "SECURITY DEFINER\nSET search_path = pg_catalog" in statement
        assert "grantor.oid = 10" in statement
        assert "member.rolname = current_user" in statement
        assert "NOT membership.inherit_option" in statement
        assert "NOT membership.set_option" in statement
        assert "parent.inherit_option OR parent.set_option" in statement
        assert "worker_role_not_isolated" in statement
        assert "GRANTED BY %I CASCADE" in statement
    previous = migration["_operator_statements"](preserve_creator=False)
    assert len(previous) == 2
    assert all("grantor.oid = 10" not in sql for sql in previous)


def test_creator_exception_never_matches_runtime_or_untrusted_owner():
    migration = runpy.run_path(str(MIGRATION))
    predicate = migration["_creator_edge_sql"]("v_grant.database_principal")
    for guard in (
        "granted.rolname = v_grant.database_principal",
        "member.rolname <> v_grant.database_principal",
        "grantor.rolname <> v_grant.database_principal",
        "grantor.rolsuper", "membership.admin_option",
        "member.rolcanlogin", "member.rolinherit", "member.rolcreaterole",
        "NOT member.rolsuper", "NOT member.rolcreatedb",
        "NOT member.rolreplication", "NOT member.rolbypassrls",
    ):
        assert guard in predicate
