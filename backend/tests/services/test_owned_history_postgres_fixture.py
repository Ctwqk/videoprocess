"""Offline checks for the parent-only A2 PostgreSQL fixture boundary."""
import pytest

from tests.migrations.owned_history_postgres import checked_url, seed_document
from tests.services.test_owned_seed_inventory_history import NOW
from app.services import owned_seed_inventory_history as history


@pytest.mark.parametrize("raw, confirmation", [
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_scratch", "wrong"),
    ("postgresql+asyncpg://test:synthetic@production.invalid:55465/vp_owned_inventory_test_a2_scratch", "vp_owned_inventory_test_a2_scratch"),
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:5432/vp_owned_inventory_test_a2_scratch", "vp_owned_inventory_test_a2_scratch"),
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/postgres", "postgres"),
    ("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_scratch?sslmode=disable", "vp_owned_inventory_test_a2_scratch"),
])
def test_fixture_rejects_ambient_or_unconfirmed_endpoint(raw, confirmation):
    with pytest.raises(ValueError, match="^explicit A2 disposable"):
        checked_url(raw, confirmation)


def test_fixture_requires_explicit_loopback_named_database():
    raw = "postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_scratch"
    assert checked_url(raw, "vp_owned_inventory_test_a2_scratch").port == 55465


@pytest.mark.parametrize("null_link", [False, True])
def test_pg_seed_retains_four_native_paths_with_model_complete_nonsecret_rows(null_link):
    rows, observations = seed_document(NOW, null_link=null_link)
    snapshot = history.OwnedHistorySnapshot.from_rows(rows, platform_channel_id="UC" + "a" * 22,
        observed_at=NOW, redis_observations=observations)
    from app.services import owned_seed_inventory as service
    sources = service._retirement_sources(snapshot, requested=True)
    import hashlib
    import uuid
    hashes = {uuid.UUID(s["id"]): hashlib.sha256(b"a" * 100).hexdigest() for s in sources["source_assets"]}
    document = service._qualified_retirement(snapshot, sources, (hashes, observations), "fixture", "fixture:only")
    cert = history.RetiredPreuploadCertificate.parse(document)
    assert cert.retained_facts.account.as_dict()["platform_account_id"] == ""
    assert len(document["terminal_graph"]["worker_task_dispatches"]) == 4
    assert "lease_secret_sha256" not in str(rows) and "token_sha256" not in str(rows)


def test_pg_seed_uses_actual_native_enum_labels():
    from sqlalchemy import Enum
    rows, _ = seed_document(NOW)
    for name, model in history.HISTORY_MODELS.items():
        for row in rows[name]:
            for column in model.__table__.columns:
                if isinstance(column.type, Enum) and row.get(column.name) is not None:
                    assert row[column.name] in column.type.enums, (name, column.name, row[column.name])


@pytest.mark.parametrize("exit_kind", ["close", "error"])
async def test_nested_inventory_fixture_disposes_pool_on_generator_exit(monkeypatch, exit_kind):
    from types import SimpleNamespace
    from sqlalchemy.ext.asyncio import AsyncEngine
    from tests.api.test_owned_seed_inventory import inventory_env

    disposed = []
    original = AsyncEngine.dispose

    async def observed(engine, *args, **kwargs):
        disposed.append(engine)
        await original(engine, *args, **kwargs)

    monkeypatch.setattr(AsyncEngine, "dispose", observed)
    generator = inventory_env.__wrapped__(monkeypatch, SimpleNamespace())
    engine = None
    try:
        env = await anext(generator)
        engine = env.factory.kw["bind"]
        if exit_kind == "close":
            await generator.aclose()
        else:
            with pytest.raises(ValueError, match="fixture exit"):
                await generator.athrow(ValueError("fixture exit"))
        assert engine in disposed
    finally:
        await generator.aclose()
        if engine is not None:
            await original(engine)


async def test_a2_adapter_enters_real_inventory_validation_with_exact_child_confirmation(monkeypatch):
    from types import SimpleNamespace
    from sqlalchemy.engine import make_url
    from tests.api import test_owned_seed_inventory as api_fixture
    from tests.migrations.test_owned_history_seal_postgres import a2_env

    target = make_url("postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_child")
    seen = []

    class BeforeConnect(Exception):
        pass

    def intercept(url):
        seen.append(make_url(url))
        raise BeforeConnect

    monkeypatch.setenv("OWNED_INVENTORY_DISPOSABLE_TEST_CONFIRM", "stale-anchor-confirmation")
    monkeypatch.setattr(api_fixture, "create_async_engine", intercept)
    generator = a2_env.__wrapped__(SimpleNamespace(target=target), monkeypatch)
    try:
        with pytest.raises(BeforeConnect):
            await anext(generator)
        assert seen == [target]
    finally:
        await generator.aclose()


async def test_a2_generated_children_use_the_validated_namespace_without_reusing_anchor(monkeypatch, tmp_path):
    from types import SimpleNamespace
    from tests.migrations import owned_history_postgres as fixtures

    raw = "postgresql+asyncpg://test:synthetic@127.0.0.1:55465/vp_owned_inventory_test_a2_anchor"
    anchor = checked_url(raw, "vp_owned_inventory_test_a2_anchor")
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_TEST_URL", raw)
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_CONFIRM", anchor.database)
    monkeypatch.setenv("OWNED_HISTORY_A2_POSTGRES_SYSTEM_ID", "1234567890123456789")
    names, closed = [], []

    class BeforeCreate(Exception):
        pass

    class Admin:
        async def fetchval(self, sql):
            return {"SHOW server_version_num": 160000,
                    "SELECT system_identifier FROM pg_control_system()": 1234567890123456789,
                    "SELECT version_num FROM public.alembic_version": fixtures.HEAD}[sql]

        async def execute(self, sql):
            assert sql.startswith('CREATE DATABASE "') and sql.endswith('"')
            names.append(sql.removeprefix('CREATE DATABASE "').removesuffix('"'))
            raise BeforeCreate

        async def close(self):
            closed.append(True)

    async def connect(url, **_kwargs):
        assert url == fixtures.dsn(anchor)
        return Admin()

    monkeypatch.setattr(fixtures.asyncpg, "connect", connect)
    for _ in range(2):
        generator = fixtures.a2_pg.__wrapped__(monkeypatch, tmp_path, SimpleNamespace(param=False))
        try:
            with pytest.raises(BeforeCreate):
                await anext(generator)
        finally:
            await generator.aclose()
    assert len(set(names)) == len(closed) == 2
    assert anchor.database not in names
    for name in names:
        checked_url(anchor.set(database=name).render_as_string(hide_password=False), name)


@pytest.mark.parametrize("metrics_retry", [False, True])
@pytest.mark.parametrize("microsecond", [0, 123456])
def test_succeeded_pg_fixture_has_complete_native_rows_and_real_history_classification(metrics_retry, microsecond):
    from sqlalchemy import Enum
    from tests.migrations import owned_history_postgres as fixtures

    now = NOW.replace(microsecond=microsecond)
    rows = fixtures.succeeded_document(now, metrics_retry=metrics_retry)
    for name, model in history.HISTORY_MODELS.items():
        for row in rows[name]:
            for column in model.__table__.columns:
                if not column.nullable:
                    assert row.get(column.name) is not None, (name, column.name)
                if isinstance(column.type, Enum) and row.get(column.name) is not None:
                    assert row[column.name] in column.type.enums
    account = rows["publishing_accounts"][0]
    assert account["platform_account_id"] == ""
    # Only the real API may qualify a legacy binding. For this offline schema
    # oracle classify the same rows directly under an explicit synthetic UC.
    account["platform_account_id"] = "UC" + "a" * 22
    result = history.assess_owned_history(history.OwnedHistorySnapshot.from_rows(
        rows, platform_channel_id=account["platform_account_id"], observed_at=now), now=now)
    assert result.block_reason is None, result.block_reason
    assert result.wait_reason == ("owned_inventory_metrics_pending" if metrics_retry else None)
    assert rows["owned_seed_inventories"] == []
