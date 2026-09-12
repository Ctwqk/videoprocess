import ast
from pathlib import Path
import runpy
import subprocess
import sys

from app.services.worker_session_signal_sql import (
    RETIRE_BODY,
    VALIDATE_BODY,
    bootstrap_sql,
)


def test_sql_export_is_one_atomic_install_and_never_connects():
    result = subprocess.run(
        [sys.executable, "-m", "app.services.worker_session_signal_sql"],
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout == bootstrap_sql()
    assert result.stdout.startswith("BEGIN;\nSET LOCAL search_path = pg_catalog;")
    assert result.stdout.endswith("COMMIT;\n")
    assert not result.stderr


def test_private_bodies_have_no_application_dependencies():
    for body in (VALIDATE_BODY, RETIRE_BODY):
        assert "public." not in body
        assert "worker_admission_grants" not in body
        assert "%ROWTYPE" not in body
        assert "EXECUTE " not in body
    assert "datid = @DATABASE_OID@ AND usesysid = v_target" in RETIRE_BODY
    assert "backend_start = v_backend.backend_start" in RETIRE_BODY


def test_new_revision_only_replaces_existing_wrappers_after_validation():
    path = (
        Path(__file__).resolve().parents[2]
        / "alembic/versions/036_worker_session_signal.py"
    )
    migration = runpy.run_path(str(path))
    assert migration["down_revision"] == "035_worker_creator_edges"
    statements = migration["_operator_statements"]()
    assert len(statements) == 2
    for sql in statements:
        assert sql.lstrip().startswith(
            "CREATE OR REPLACE FUNCTION public.vp_worker_grant_"
        )
        assert "existing wrapper owner's" in sql
        assert "This branch never obtains bootstrap signaling privileges" in sql
        assert "SECURITY DEFINER\nSET search_path = pg_catalog" in sql
        assert sql.index("worker_signal_identity_invalid") < sql.index(".retire(")
        assert sql.index(".validate_target(") < sql.index(".retire(")


def test_session_signal_operator_fixture_uses_its_migration_allowlist():
    backend = Path(__file__).resolve().parents[2]
    fixtures = backend / "tests/migrations"
    source = ast.parse((fixtures / "test_worker_session_signal_postgres.py").read_text())
    case = next(
        node for node in source.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "test_real_operator_stable_allowlist_supports_repeated_drain"
    )
    loop = next(node for node in ast.walk(case) if isinstance(node, ast.For))
    assert isinstance(loop.iter, ast.Name), "036 fixture must not iterate current ROLE_FUNCTIONS"
    signatures = next(
        node.value for node in source.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == loop.iter.id for target in node.targets)
    )
    migration = runpy.run_path(str(backend / "alembic/versions/034_worker_registrations.py"))
    expected = tuple(
        migration[name].removeprefix("public.")
        for name in (
            "GRANT_UPSERT_SIGNATURE", "GRANT_ACTIVATE_SIGNATURE", "GRANT_REVOKE_SIGNATURE",
            "REGISTRATION_REVOKE_SIGNATURE", "REGISTRATION_EXPIRE_SIGNATURE",
        )
    )
    assert ast.literal_eval(signatures) == expected
    owner_fixture = ast.parse((fixtures / "test_worker_operator_creator_edges_postgres.py").read_text())
    target_revision = next(
        node.value for node in owner_fixture.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "TARGET_REVISION" for target in node.targets)
    )
    assert ast.literal_eval(target_revision) == "036_worker_session_signal"
