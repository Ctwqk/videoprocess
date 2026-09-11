import importlib.util
import io
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql

from app.models.owned_seed_inventory import OwnedSeedInventory


def test_inventory_migration_is_additive_and_preserves_unreleased_terminal_slots():
    path = Path(__file__).resolve().parents[2] / "alembic/versions/037_owned_seed_inventory.py"
    assert path.exists(), "inventory migration is missing"
    spec = importlib.util.spec_from_file_location("owned_inventory_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.down_revision == "036_worker_session_signal"
    output = io.StringIO()
    context = MigrationContext.configure(url="postgresql://", opts={"as_sql": True, "output_buffer": output})
    with Operations.context(context):
        module.upgrade()
    sql = output.getvalue()
    assert "CREATE TABLE owned_seed_inventories" in sql
    assert "CREATE TABLE owned_seed_inventory_items" in sql
    assert sql.count("WHERE approved_at IS NOT NULL AND succession_released_at IS NULL") == 3
    assert "expires_at = starts_at + interval '168 hours'" in sql
    assert "owned_inventory_immutable" in sql
    assert "owned_item_immutable" in sql
    assert "ON DELETE RESTRICT" in sql
    assert "ALTER TABLE channel_profiles ADD COLUMN owned_seed_inventory_id UUID" in sql
    assert "vp_transition_worker_youtube_upload" not in sql
    assert "GRANT" not in sql


def test_trigger_ddl_is_individually_executable_by_asyncpg(monkeypatch):
    path = Path(__file__).resolve().parents[2] / "alembic/versions/037_owned_seed_inventory.py"
    spec = importlib.util.spec_from_file_location("owned_inventory_migration_statements", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    statements = []
    monkeypatch.setattr(module.op, "execute", statements.append)
    context = MigrationContext.configure(url="postgresql://", opts={"as_sql": True, "output_buffer": io.StringIO()})
    with Operations.context(context):
        module.upgrade()
    assert len(statements) == 4
    assert all(statement.count("CREATE FUNCTION") + statement.count("CREATE TRIGGER") == 1 for statement in statements)


def test_model_has_approval_actor_and_exact_window_checks():
    sql = str(CreateTable(OwnedSeedInventory.__table__).compile(dialect=postgresql.dialect()))
    assert "ck_owned_inventory_approval_actor" in sql
    assert "ck_owned_inventory_exact_window" in sql
