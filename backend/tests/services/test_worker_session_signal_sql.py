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
