from pathlib import Path
import runpy
from types import SimpleNamespace

from app.services.worker_control_role_cli import (
    ORCHESTRATOR_UPDATE_COLUMNS,
    ROLE_FUNCTIONS,
)


MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "alembic/versions/037_registered_retry_release.py"
)
SIGNATURE = "vp_release_registered_retry_claim(uuid)"


def test_retry_release_is_additive_narrow_security_definer(monkeypatch):
    assert MIGRATION.exists(), "registered retry release migration is missing"
    namespace = runpy.run_path(str(MIGRATION))
    assert namespace["down_revision"] == "036_worker_session_signal"
    statements = []
    monkeypatch.setattr(
        namespace["upgrade"].__globals__["op"], "execute", statements.append
    )
    namespace["upgrade"]()
    sql = "\n".join(statements)
    assert "SECURITY DEFINER" in sql and "SET search_path = pg_catalog" in sql
    assert f"REVOKE ALL ON FUNCTION public.{SIGNATURE} FROM PUBLIC" in sql
    assert (
        "vp_orchestrator_control_runtime" in sql
        and "database_principal_privileged" in sql
    )
    assert "vp_observe_worker_task_delivery" in sql
    assert (
        "application_state = 'accepted'" in sql and "event_type = 'node_failed'" in sql
    )
    assert "delivery_attempted_at IS NULL" in sql and "delivery_error IS NULL" in sql
    assert "FOR UPDATE" in sql and "FOR SHARE" in sql
    updates = sql.split("UPDATE public.node_executions")
    assert len(updates) == 2
    assignments = updates[1].split("SET", 1)[1].split("WHERE", 1)[0]
    assert set(item.strip() for item in assignments.split(",")) == {
        "worker_id = NULL",
        "worker_registration_id = NULL",
        "worker_lease_epoch = NULL",
        "started_at = NULL",
    }
    assert "GRANT " not in sql and "CREATE TABLE" not in sql
    statements.clear()
    namespace["downgrade"]()
    assert statements == [f"DROP FUNCTION public.{SIGNATURE}"]


def test_only_orchestrator_gets_retry_release_without_column_grant():
    assert SIGNATURE in ROLE_FUNCTIONS["orchestrator"]
    for purpose, signatures in ROLE_FUNCTIONS.items():
        if purpose != "orchestrator":
            assert SIGNATURE not in signatures
    assert not {
        "worker_id",
        "worker_registration_id",
        "worker_lease_epoch",
        "started_at",
    }.intersection(ORCHESTRATOR_UPDATE_COLUMNS["node_executions"])


async def test_postgres_adapter_uses_function_and_refresh_not_orm_update():
    from app.services import registered_worker_retry
    import uuid

    node_id, receipt_id = uuid.uuid4(), uuid.uuid4()
    node = SimpleNamespace(id=node_id)
    calls = []

    class Session:
        def get_bind(self):
            return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        async def scalar(self, statement, parameters):
            calls.append((str(statement), parameters))
            return node_id

        async def get(self, model, key):
            assert key == node_id
            return node

        async def refresh(self, value, *, attribute_names):
            assert value is node
            calls.append(tuple(attribute_names))

    assert (
        await registered_worker_retry.release_registered_retry_claim(
            Session(), receipt_id
        )
        == node_id
    )
    assert calls == [
        (
            "SELECT public.vp_release_registered_retry_claim(:receipt_id)",
            {"receipt_id": receipt_id},
        ),
        ("worker_id", "worker_registration_id", "worker_lease_epoch", "started_at"),
    ]
